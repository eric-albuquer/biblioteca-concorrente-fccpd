"""Worker RabbitMQ com ack após commit, retry limitado e DLQ."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import threading
import uuid
from typing import Any, Callable
from uuid import UUID

import pika
from psycopg import Error as ErroBanco

from messaging.config import parametros_conexao
from messaging.topology import (
    COMMAND_EXCHANGE,
    COMMAND_QUEUE,
    COMMAND_ROUTING_KEY,
    declarar_topologia,
)
from services.comum import EntidadeNaoEncontrada, RegraDeNegocioError
from services.emprestimos import ServicoEmprestimos


LOGGER = logging.getLogger(__name__)
MAX_MESSAGE_BYTES = 64 * 1024


class MensagemInvalida(ValueError):
    pass


class FalhaTransitoria(RuntimeError):
    """Usada por testes/failpoints e integrações temporariamente indisponíveis."""


class Worker:
    def __init__(
        self,
        dsn: str | None = None,
        rabbitmq_url: str | None = None,
        *,
        worker_id: str | None = None,
        prefetch_count: int | None = None,
        max_retries: int | None = None,
        backoff_max: float = 30.0,
        processador: Callable[[str, str], Any] | None = None,
        exit_function: Callable[[int], Any] | None = None,
    ):
        self.dsn = dsn
        self.rabbitmq_url = rabbitmq_url
        self.worker_id = (
            worker_id
            or os.getenv("WORKER_ID")
            or os.getenv("HOSTNAME")
            or f"worker-{uuid.uuid4().hex[:8]}"
        )
        self.prefetch_count = (
            prefetch_count
            if prefetch_count is not None
            else int(os.getenv("WORKER_PREFETCH", "1"))
        )
        self.max_retries = (
            max_retries
            if max_retries is not None
            else int(os.getenv("WORKER_MAX_RETRIES", "3"))
        )
        self.backoff_max = backoff_max
        self.exit_function = exit_function or os._exit
        self.servico = ServicoEmprestimos(
            dsn,
            failpoint=self._acionar_failpoint,
        )
        self.processador = processador or self._processar_no_banco
        self.pronto = threading.Event()
        self._encerrando = threading.Event()
        self._connection: pika.BlockingConnection | None = None
        self._channel = None
        self.mensagens_processadas = 0
        self.mensagens_dlq = 0

    def iniciar(self) -> None:
        atraso = 1.0
        while not self._encerrando.is_set():
            try:
                self._consumir()
                atraso = 1.0
            except (pika.exceptions.AMQPError, OSError) as erro:
                if not self._encerrando.is_set():
                    LOGGER.warning(
                        "worker_id=%s RabbitMQ indisponível: %s",
                        self.worker_id,
                        erro,
                    )
                    self._encerrando.wait(atraso)
                    atraso = min(atraso * 2, self.backoff_max)
            finally:
                self.pronto.clear()
                self._fechar_conexao()

    def encerrar(self) -> None:
        self._encerrando.set()
        conexao = self._connection
        channel = self._channel
        if conexao is not None and conexao.is_open and channel is not None:
            try:
                conexao.add_callback_threadsafe(channel.stop_consuming)
            except pika.exceptions.AMQPError:
                pass

    def _consumir(self) -> None:
        self._connection = pika.BlockingConnection(
            parametros_conexao(self.rabbitmq_url)
        )
        self._channel = self._connection.channel()
        declarar_topologia(self._channel)
        self._channel.confirm_delivery()
        self._channel.basic_qos(prefetch_count=self.prefetch_count)
        self._channel.basic_consume(
            queue=COMMAND_QUEUE,
            on_message_callback=self._ao_receber,
            auto_ack=False,
        )
        self.pronto.set()
        LOGGER.info("worker_id=%s pronto", self.worker_id)
        self._channel.start_consuming()

    def _ao_receber(self, channel, method, properties, body: bytes) -> None:
        try:
            request_id, tipo = self._validar_mensagem(body, properties)
        except MensagemInvalida as erro:
            LOGGER.error(
                "worker_id=%s mensagem inválida: %s", self.worker_id, erro
            )
            self.mensagens_dlq += 1
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        try:
            self.processador(request_id, tipo)
        except (ErroBanco, FalhaTransitoria, OSError) as erro:
            LOGGER.warning(
                "worker_id=%s request_id=%s falha transitória: %s",
                self.worker_id,
                request_id,
                erro,
            )
            self._retentar_ou_descartar(channel, method, properties, body)
            return
        except (EntidadeNaoEncontrada, RegraDeNegocioError) as erro:
            LOGGER.error(
                "worker_id=%s request_id=%s mensagem inconsistente: %s",
                self.worker_id,
                request_id,
                erro,
            )
            self.mensagens_dlq += 1
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        except Exception as erro:
            LOGGER.exception(
                "worker_id=%s request_id=%s erro inesperado",
                self.worker_id,
                request_id,
            )
            self._retentar_ou_descartar(channel, method, properties, body)
            return

        self._acionar_failpoint("AFTER_COMMIT_BEFORE_ACK", request_id)

        # processador só retorna depois do commit da transação de domínio.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        self.mensagens_processadas += 1
        LOGGER.info(
            "worker_id=%s request_id=%s tipo=%s processado",
            self.worker_id,
            request_id,
            tipo,
        )

    def _processar_no_banco(self, request_id: str, tipo: str) -> None:
        self.servico.processar_comando(request_id, tipo_esperado=tipo)

    def _retentar_ou_descartar(
        self, channel, method, properties, body: bytes
    ) -> None:
        headers = dict(properties.headers or {})
        tentativas = int(headers.get("x-retry-count", 0))
        if tentativas >= self.max_retries:
            self.mensagens_dlq += 1
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        headers["x-retry-count"] = tentativas + 1
        try:
            confirmado = channel.basic_publish(
                exchange=COMMAND_EXCHANGE,
                routing_key=COMMAND_ROUTING_KEY,
                body=body,
                properties=pika.BasicProperties(
                    content_type=properties.content_type or "application/json",
                    content_encoding=properties.content_encoding or "utf-8",
                    delivery_mode=pika.DeliveryMode.Persistent,
                    message_id=properties.message_id,
                    type=properties.type,
                    correlation_id=properties.correlation_id,
                    headers=headers,
                ),
                mandatory=True,
            )
            if confirmado is False:
                raise FalhaTransitoria("RabbitMQ não confirmou o retry")
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except (pika.exceptions.AMQPError, OSError, FalhaTransitoria):
            LOGGER.exception(
                "worker_id=%s falhou ao republicar retry", self.worker_id
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    @staticmethod
    def _validar_mensagem(
        body: bytes, properties
    ) -> tuple[str, str]:
        if len(body) > MAX_MESSAGE_BYTES:
            raise MensagemInvalida("mensagem excede 64 KiB")
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as erro:
            raise MensagemInvalida("JSON/UTF-8 inválido") from erro
        if not isinstance(payload, dict):
            raise MensagemInvalida("payload deve ser objeto JSON")

        try:
            request_id = str(UUID(str(payload["request_id"])))
        except (KeyError, TypeError, ValueError, AttributeError) as erro:
            raise MensagemInvalida("request_id ausente ou inválido") from erro
        tipo = payload.get("tipo")
        if tipo not in {"PEDIR", "DEVOLVER"}:
            raise MensagemInvalida("tipo deve ser PEDIR ou DEVOLVER")
        if properties.message_id and properties.message_id != request_id:
            raise MensagemInvalida("message_id diverge de request_id")
        return request_id, tipo

    def _acionar_failpoint(self, nome: str, request_id: str) -> None:
        if os.getenv("ENABLE_FAILPOINTS") != "1":
            return
        if os.getenv("FAILPOINT") != nome:
            return
        alvo = os.getenv("FAILPOINT_REQUEST_ID")
        if alvo and alvo != request_id:
            return

        diretorio = Path(
            os.getenv("FAILPOINT_MARKER_DIR", "/tmp/biblioteca-failpoints")
        )
        marcador = diretorio / f"{nome}-{request_id}.executado"
        try:
            diretorio.mkdir(parents=True, exist_ok=True)
            marcador.touch(exist_ok=False)
        except FileExistsError:
            return
        except OSError:
            LOGGER.exception(
                "worker_id=%s não conseguiu criar marcador do failpoint",
                self.worker_id,
            )
            return

        codigo = 91 if nome == "BEFORE_COMMIT" else 92
        LOGGER.critical(
            "worker_id=%s request_id=%s FAILPOINT=%s exit_code=%s",
            self.worker_id,
            request_id,
            nome,
            codigo,
        )
        logging.shutdown()
        self.exit_function(codigo)

    def _fechar_conexao(self) -> None:
        if self._connection is not None and self._connection.is_open:
            try:
                self._connection.close()
            except pika.exceptions.AMQPError:
                pass
        self._connection = None
        self._channel = None

