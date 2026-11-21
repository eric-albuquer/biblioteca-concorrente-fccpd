"""Cliente TCP V2 com UUID idempotente, reconexão e consulta de status."""

from __future__ import annotations

import socket
import time
from typing import Any
from uuid import UUID, uuid4

from network.protocolo import (
    DEVOLVER,
    ERRO,
    PEDIR,
    RESPOSTA,
    STATUS,
    ErroProtocolo,
    enviar_mensagem,
    receber_mensagem,
)


PORTA = 9999
SERVIDOR_IP = "127.0.0.1"


class ErroRemoto(RuntimeError):
    def __init__(self, resposta: dict[str, Any]):
        self.codigo = str(resposta.get("erro", "ERRO_REMOTO"))
        self.resposta = resposta
        super().__init__(str(resposta.get("mensagem", self.codigo)))


class Cliente:
    def __init__(
        self,
        host: str = SERVIDOR_IP,
        porta: int = PORTA,
        *,
        timeout: float = 5.0,
        tentativas: int = 3,
        intervalo_retry: float = 0.1,
    ):
        if tentativas < 1:
            raise ValueError("tentativas deve ser pelo menos 1")
        self.host = host
        self.porta = porta
        self.timeout = timeout
        self.tentativas = tentativas
        self.intervalo_retry = intervalo_retry
        self.socket: socket.socket | None = None

    def pedir_livro(
        self,
        usuario_id: int,
        livro_id: int,
        request_id: UUID | str | None = None,
    ) -> dict[str, Any]:
        request_uuid = str(UUID(str(request_id))) if request_id else str(uuid4())
        return self._requisicao(
            PEDIR,
            {
                "request_id": request_uuid,
                "usuario_id": usuario_id,
                "livro_id": livro_id,
            },
        )

    def devolver_livro(
        self,
        usuario_id: int,
        emprestimo_id: int,
        request_id: UUID | str | None = None,
    ) -> dict[str, Any]:
        request_uuid = str(UUID(str(request_id))) if request_id else str(uuid4())
        return self._requisicao(
            DEVOLVER,
            {
                "request_id": request_uuid,
                "usuario_id": usuario_id,
                "emprestimo_id": emprestimo_id,
            },
        )

    def consultar_status(self, request_id: UUID | str) -> dict[str, Any]:
        return self._requisicao(
            STATUS,
            {"request_id": str(UUID(str(request_id)))},
        )

    def aguardar_conclusao(
        self,
        request_id: UUID | str,
        *,
        timeout: float = 30.0,
        intervalo: float = 0.2,
    ) -> dict[str, Any]:
        limite = time.monotonic() + timeout
        while True:
            resposta = self.consultar_status(request_id)
            if resposta["status"] in {"CONCLUIDO", "REJEITADO"}:
                return resposta
            if time.monotonic() >= limite:
                raise TimeoutError(
                    f"Solicitação {request_id} não concluiu em {timeout} segundos"
                )
            time.sleep(intervalo)

    def _requisicao(
        self, operacao: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        ultimo_erro: BaseException | None = None
        for tentativa in range(1, self.tentativas + 1):
            try:
                self._conectar()
                enviar_mensagem(self.socket, operacao, payload)
                operacao_resposta, resposta = receber_mensagem(self.socket)
                if operacao_resposta == ERRO:
                    raise ErroRemoto(resposta)
                if operacao_resposta != RESPOSTA:
                    raise ErroProtocolo(
                        f"Resposta inesperada do Gateway: {operacao_resposta}"
                    )
                return resposta
            except ErroRemoto:
                raise
            except (ConnectionError, OSError) as erro:
                ultimo_erro = erro
                self.desconectar()
                if tentativa < self.tentativas:
                    time.sleep(self.intervalo_retry * tentativa)

        raise ConnectionError(
            f"Não foi possível comunicar com {self.host}:{self.porta} "
            f"após {self.tentativas} tentativa(s)"
        ) from ultimo_erro

    def _conectar(self) -> None:
        if self.socket is not None:
            return
        sock = socket.create_connection((self.host, self.porta), self.timeout)
        sock.settimeout(self.timeout)
        self.socket = sock

    def desconectar(self) -> None:
        if self.socket is not None:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.socket.close()
            self.socket = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.desconectar()


if __name__ == "__main__":
    with Cliente() as cliente:
        aceite = cliente.pedir_livro(usuario_id=1, livro_id=1)
        print("Solicitação aceita:", aceite)
        print("Status atual:", cliente.consultar_status(aceite["request_id"]))
