from __future__ import annotations

import socket
import threading
from uuid import uuid4

from database.connection import conectar
from network.cliente import Cliente, ErroRemoto
from network.protocolo import (
    ERRO,
    PEDIR,
    RESPOSTA,
    enviar_mensagem,
    receber_mensagem,
)
from network.servidor import Servidor


def _iniciar_gateway(dsn: str) -> tuple[Servidor, threading.Thread, int]:
    servidor = Servidor(dsn, host="127.0.0.1", porta=0)
    thread = threading.Thread(target=servidor.iniciar, daemon=True)
    thread.start()
    assert servidor.pronto.wait(timeout=5), "Gateway não iniciou a tempo"
    return servidor, thread, servidor.endereco[1]


def _encerrar_gateway(servidor: Servidor, thread: threading.Thread) -> None:
    servidor.encerrar()
    thread.join(timeout=5)
    assert not thread.is_alive(), "Gateway não encerrou a tempo"


def test_gateway_aceita_sem_worker_e_status_sobrevive_ao_restart(banco_limpo):
    request_id = uuid4()
    servidor, thread, porta = _iniciar_gateway(banco_limpo)
    try:
        with Cliente(porta=porta) as cliente:
            aceite = cliente.pedir_livro(1, 1, request_id)
            repetido = cliente.pedir_livro(1, 1, request_id)
            status = cliente.consultar_status(request_id)
        assert aceite["status"] == status["status"] == "RECEBIDO"
        assert repetido["ordem_fila"] == aceite["ordem_fila"]
    finally:
        _encerrar_gateway(servidor, thread)

    servidor, thread, nova_porta = _iniciar_gateway(banco_limpo)
    try:
        with Cliente(porta=nova_porta) as cliente:
            recuperada = cliente.consultar_status(request_id)
        assert recuperada["request_id"] == str(request_id)
        assert recuperada["status"] == "RECEBIDO"
    finally:
        _encerrar_gateway(servidor, thread)

    with conectar(banco_limpo) as conexao:
        assert conexao.execute(
            "SELECT COUNT(*) AS total FROM outbox WHERE request_id = %s",
            (request_id,),
        ).fetchone()["total"] == 1


def test_gateway_retorna_erros_estruturados_e_mantem_conexao(banco_limpo):
    servidor, thread, porta = _iniciar_gateway(banco_limpo)
    try:
        with Cliente(porta=porta, tentativas=1) as cliente:
            try:
                cliente.pedir_livro(999, 1)
            except ErroRemoto as erro:
                assert erro.codigo == "NAO_ENCONTRADO"
            else:
                raise AssertionError("Usuário inexistente deveria gerar erro")

            aceite = cliente.pedir_livro(1, 1)
            assert aceite["status"] == "RECEBIDO"
    finally:
        _encerrar_gateway(servidor, thread)


def test_gateway_rejeita_payload_incompleto_com_erro_json(banco_limpo):
    servidor, thread, porta = _iniciar_gateway(banco_limpo)
    try:
        with socket.create_connection(("127.0.0.1", porta), timeout=5) as cliente:
            enviar_mensagem(
                cliente,
                PEDIR,
                {"request_id": str(uuid4()), "usuario_id": 1},
            )
            operacao, resposta = receber_mensagem(cliente)
        assert operacao == ERRO
        assert resposta["erro"] == "PROTOCOLO_INVALIDO"
    finally:
        _encerrar_gateway(servidor, thread)


def test_cliente_retry_reenvia_exatamente_o_mesmo_request_id():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    porta = listener.getsockname()[1]
    recebidos = []

    def servidor_controlado():
        try:
            conexao, _ = listener.accept()
            with conexao:
                recebidos.append(receber_mensagem(conexao))
                # Simula perda da conexão depois do envio e antes da resposta.

            conexao, _ = listener.accept()
            with conexao:
                operacao, payload = receber_mensagem(conexao)
                recebidos.append((operacao, payload))
                enviar_mensagem(
                    conexao,
                    RESPOSTA,
                    {
                        "request_id": payload["request_id"],
                        "status": "RECEBIDO",
                    },
                )
        finally:
            listener.close()

    thread = threading.Thread(target=servidor_controlado, daemon=True)
    thread.start()
    request_id = uuid4()

    with Cliente(
        porta=porta,
        tentativas=2,
        intervalo_retry=0,
    ) as cliente:
        resposta = cliente.pedir_livro(1, 1, request_id)

    thread.join(timeout=5)
    assert not thread.is_alive()
    assert resposta["request_id"] == str(request_id)
    assert recebidos[0] == recebidos[1]

