"""Publica eventos confirmados da outbox no RabbitMQ."""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

import pika

from database.connection import conectar
from messaging.config import parametros_conexao
from messaging.topology import (
    COMMAND_EXCHANGE,
    COMMAND_ROUTING_KEY,
    declarar_topologia,
)
from scripts.sinais import instalar_desligamento


LOGGER = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(
        self,
        dsn: str | None = None,
        rabbitmq_url: str | None = None,
        *,
        batch_size: int = 50,
        poll_interval: float = 0.5,
        backoff_max: float = 30.0,
    ):
        self.dsn = dsn
        self.rabbitmq_url = rabbitmq_url
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.backoff_max = backoff_max
        self._connection: pika.BlockingConnection | None = None
        self._channel = None
        self._encerrando = threading.Event()

    def iniciar(self) -> None:
        atraso = 1.0
        try:
            while not self._encerrando.is_set():
                try:
                    publicados = self.publicar_lote()
                    atraso = 1.0
                    self._encerrando.wait(
                        self.poll_interval if publicados == 0 else 0
                    )
                except (pika.exceptions.AMQPError, OSError) as erro:
                    LOGGER.warning("RabbitMQ indisponível para publisher: %s", erro)
                    self._fechar_conexao()
                    self._encerrando.wait(atraso)
                    atraso = min(atraso * 2, self.backoff_max)
                except Exception:
                    LOGGER.exception("Falha inesperada no Outbox Publisher")
                    self._encerrando.wait(atraso)
                    atraso = min(atraso * 2, self.backoff_max)
        finally:
            self._fechar_conexao()

    def publicar_lote(self) -> int:
        eventos = self._buscar_pendentes()
        if not eventos:
            return 0

        try:
            channel = self._obter_channel()
        except (pika.exceptions.AMQPError, OSError) as erro:
            self._registrar_falha(eventos[0]["id"], erro)
            raise

        publicados = 0
        for evento in eventos:
            if self._encerrando.is_set():
                break
            try:
                confirmado = channel.basic_publish(
                    exchange=COMMAND_EXCHANGE,
                    routing_key=COMMAND_ROUTING_KEY,
                    body=json.dumps(
                        evento["payload"],
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8"),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        content_encoding="utf-8",
                        delivery_mode=pika.DeliveryMode.Persistent,
                        message_id=str(evento["request_id"]),
                        type=evento["event_type"],
                        timestamp=int(time.time()),
                    ),
                    mandatory=True,
                )
                if confirmado is False:
                    raise RuntimeError("RabbitMQ não confirmou a publicação")
                self._marcar_publicado(evento["id"])
                publicados += 1
            except (pika.exceptions.AMQPError, OSError, RuntimeError) as erro:
                self._registrar_falha(evento["id"], erro)
                self._fechar_conexao()
                raise
        return publicados

    def encerrar(self) -> None:
        self._encerrando.set()

    def fechar(self) -> None:
        """Fecha conexão de uso pontual; chamar na mesma thread da publicação."""
        self._fechar_conexao()

    def _buscar_pendentes(self) -> list[dict[str, Any]]:
        with conectar(self.dsn) as conexao:
            return conexao.execute(
                """
                SELECT id, request_id, event_type, payload, tentativas
                FROM outbox
                WHERE publicado_em IS NULL
                ORDER BY criado_em, id
                LIMIT %s
                """,
                (self.batch_size,),
            ).fetchall()

    def _marcar_publicado(self, evento_id: int) -> None:
        with conectar(self.dsn) as conexao:
            conexao.execute(
                """
                UPDATE outbox
                SET publicado_em = CURRENT_TIMESTAMP,
                    tentativas = tentativas + 1,
                    erro_publicacao = NULL
                WHERE id = %s AND publicado_em IS NULL
                """,
                (evento_id,),
            )

    def _registrar_falha(self, evento_id: int, erro: BaseException) -> None:
        descricao = str(erro).strip() or type(erro).__name__
        with conectar(self.dsn) as conexao:
            conexao.execute(
                """
                UPDATE outbox
                SET tentativas = tentativas + 1,
                    erro_publicacao = %s
                WHERE id = %s AND publicado_em IS NULL
                """,
                (descricao[:1000], evento_id),
            )

    def _obter_channel(self):
        if self._connection is None or self._connection.is_closed:
            self._connection = pika.BlockingConnection(
                parametros_conexao(self.rabbitmq_url)
            )
            self._channel = self._connection.channel()
            declarar_topologia(self._channel)
            self._channel.confirm_delivery()
        return self._channel

    def _fechar_conexao(self) -> None:
        if self._connection is not None and self._connection.is_open:
            try:
                self._connection.close()
            except pika.exceptions.AMQPError:
                pass
        self._connection = None
        self._channel = None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    publisher = OutboxPublisher()
    instalar_desligamento(publisher.encerrar)
    publisher.iniciar()

