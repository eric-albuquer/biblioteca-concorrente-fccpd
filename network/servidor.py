"""Gateway TCP V2: aceita comandos no PostgreSQL e consulta status."""

from __future__ import annotations

import logging
import socket
import threading
from typing import Any

from psycopg import Error as ErroBanco

from network.protocolo import (
    DEVOLVER,
    ERRO,
    PEDIR,
    RESPOSTA,
    STATUS,
    ErroProtocolo,
    enviar_mensagem,
    receber_mensagem,
    validar_comando,
)
from services.comum import ConflitoIdempotencia, EntidadeNaoEncontrada
from services.solicitacoes import ServicoSolicitacoes


HOST = "0.0.0.0"
PORTA = 9999
LOGGER = logging.getLogger(__name__)


def _resposta_publica(solicitacao: dict[str, Any]) -> dict[str, Any]:
    campos = (
        "request_id",
        "status",
        "tipo",
        "usuario_id",
        "livro_id",
        "emprestimo_id",
        "ordem_fila",
        "erro",
        "criado_em",
        "atualizado_em",
        "concluido_em",
    )
    return {campo: solicitacao.get(campo) for campo in campos}


class Servidor:
    """Servidor multithread; cada comando usa uma transação curta no banco."""

    def __init__(
        self,
        dsn: str | None = None,
        *,
        host: str = HOST,
        porta: int = PORTA,
    ):
        self.host = host
        self.porta = porta
        self.servico = ServicoSolicitacoes(dsn)
        self.socket: socket.socket | None = None
        self.pronto = threading.Event()
        self._encerrando = threading.Event()

    @property
    def endereco(self) -> tuple[str, int]:
        if self.socket is None:
            return self.host, self.porta
        host, porta = self.socket.getsockname()[:2]
        return str(host), int(porta)

    def iniciar(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self.host, self.porta))
        self.socket.listen()
        self.socket.settimeout(0.5)
        self.pronto.set()
        LOGGER.info("Gateway ouvindo em %s:%s", *self.endereco)

        try:
            while not self._encerrando.is_set():
                try:
                    cliente, endereco = self.socket.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if self._encerrando.is_set():
                        break
                    raise

                threading.Thread(
                    target=self.atender_cliente,
                    args=(cliente, endereco),
                    daemon=True,
                ).start()
        finally:
            self.pronto.clear()
            if self.socket is not None:
                self.socket.close()

    def encerrar(self) -> None:
        self._encerrando.set()
        if self.socket is not None:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.socket.close()

    def atender_cliente(self, cliente: socket.socket, endereco) -> None:
        with cliente:
            while not self._encerrando.is_set():
                try:
                    operacao, payload = receber_mensagem(cliente)
                    resposta = self._executar(operacao, payload)
                    enviar_mensagem(cliente, RESPOSTA, resposta)
                except ConnectionError:
                    return
                except ErroProtocolo as erro:
                    if not self._enviar_erro(cliente, erro.codigo, str(erro)):
                        return
                    if erro.fatal:
                        return
                except ConflitoIdempotencia as erro:
                    if not self._enviar_erro(
                        cliente, "REQUEST_ID_CONFLITANTE", str(erro)
                    ):
                        return
                except EntidadeNaoEncontrada as erro:
                    if not self._enviar_erro(
                        cliente, "NAO_ENCONTRADO", str(erro)
                    ):
                        return
                except ErroBanco:
                    LOGGER.exception("Falha de banco ao atender %s", endereco)
                    if not self._enviar_erro(
                        cliente,
                        "BANCO_INDISPONIVEL",
                        "Não foi possível registrar o comando de forma durável",
                    ):
                        return
                except Exception:
                    LOGGER.exception("Erro inesperado ao atender %s", endereco)
                    if not self._enviar_erro(
                        cliente,
                        "ERRO_INTERNO",
                        "Erro interno no Gateway",
                    ):
                        return

    def _executar(self, operacao: int, payload: dict[str, Any]) -> dict[str, Any]:
        comando = validar_comando(operacao, payload)
        if operacao == PEDIR:
            solicitacao = self.servico.aceitar_pedido(**comando)
        elif operacao == DEVOLVER:
            solicitacao = self.servico.aceitar_devolucao(**comando)
        elif operacao == STATUS:
            solicitacao = self.servico.consultar_status(**comando)
        else:  # protegido por validar_comando; mantém o fluxo explícito.
            raise ErroProtocolo(f"Operação desconhecida: {operacao}")
        return _resposta_publica(solicitacao)

    @staticmethod
    def _enviar_erro(
        cliente: socket.socket,
        codigo: str,
        mensagem: str,
    ) -> bool:
        try:
            enviar_mensagem(
                cliente,
                ERRO,
                {
                    "erro": codigo,
                    "mensagem": mensagem,
                },
            )
            return True
        except (ConnectionError, OSError):
            return False


Gateway = Servidor


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    Servidor().iniciar()
