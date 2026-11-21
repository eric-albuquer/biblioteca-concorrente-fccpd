from __future__ import annotations

import json
import threading
import time
from uuid import uuid4

import pika
import pytest

from database.connection import conectar
from database.repository import RepositorioBiblioteca
from messaging.config import parametros_conexao
from messaging.consumer import FalhaTransitoria, Worker
from messaging.publisher import OutboxPublisher
from messaging.topology import (
    COMMAND_EXCHANGE,
    COMMAND_QUEUE,
    COMMAND_ROUTING_KEY,
    DEAD_LETTER_QUEUE,
    declarar_topologia,
)
from network.cliente import Cliente
from network.servidor import Servidor
from services.solicitacoes import ServicoSolicitacoes


def _esperar(condicao, *, timeout: float = 10.0):
    limite = time.monotonic() + timeout
    while time.monotonic() < limite:
        resultado = condicao()
        if resultado:
            return resultado
        time.sleep(0.05)
    raise TimeoutError("Condição não foi satisfeita a tempo")


def _quantidade_fila(rabbitmq_url: str, fila: str) -> int:
    conexao = pika.BlockingConnection(parametros_conexao(rabbitmq_url))
    try:
        resultado = conexao.channel().queue_declare(queue=fila, passive=True)
        return resultado.method.message_count
    finally:
        conexao.close()


def _iniciar_worker(worker: Worker) -> threading.Thread:
    thread = threading.Thread(target=worker.iniciar, daemon=True)
    thread.start()
    assert worker.pronto.wait(timeout=10), f"{worker.worker_id} não iniciou"
    return thread


def _encerrar_worker(worker: Worker, thread: threading.Thread) -> None:
    worker.encerrar()
    thread.join(timeout=10)
    assert not thread.is_alive(), f"{worker.worker_id} não encerrou"


def _iniciar_gateway(dsn: str) -> tuple[Servidor, threading.Thread, int]:
    gateway = Servidor(dsn, host="127.0.0.1", porta=0)
    thread = threading.Thread(target=gateway.iniciar, daemon=True)
    thread.start()
    assert gateway.pronto.wait(timeout=10), "Gateway não iniciou"
    return gateway, thread, gateway.endereco[1]


def _encerrar_gateway(gateway: Servidor, thread: threading.Thread) -> None:
    gateway.encerrar()
    thread.join(timeout=10)
    assert not thread.is_alive(), "Gateway não encerrou"


def test_publisher_confirma_mensagem_persistente_antes_de_marcar_outbox(
    banco_limpo, rabbitmq_limpo
):
    solicitacoes = ServicoSolicitacoes(banco_limpo)
    request_id = uuid4()
    solicitacoes.aceitar_pedido(request_id, 1, 1)
    publisher = OutboxPublisher(banco_limpo, rabbitmq_limpo)

    try:
        assert publisher.publicar_lote() == 1
    finally:
        publisher.fechar()

    with conectar(banco_limpo) as conexao:
        evento = conexao.execute(
            """
            SELECT publicado_em, tentativas, erro_publicacao
            FROM outbox WHERE request_id = %s
            """,
            (request_id,),
        ).fetchone()
    assert evento["publicado_em"] is not None
    assert evento["tentativas"] == 1
    assert evento["erro_publicacao"] is None
    assert _quantidade_fila(rabbitmq_limpo, COMMAND_QUEUE) == 1


def test_publisher_preserva_outbox_quando_broker_esta_indisponivel(
    banco_limpo, rabbitmq_limpo
):
    request_id = uuid4()
    ServicoSolicitacoes(banco_limpo).aceitar_pedido(request_id, 1, 1)
    indisponivel = OutboxPublisher(
        banco_limpo,
        "amqp://guest:guest@127.0.0.1:55999/%2F",
    )

    with pytest.raises((pika.exceptions.AMQPError, OSError)):
        indisponivel.publicar_lote()

    with conectar(banco_limpo) as conexao:
        pendente = conexao.execute(
            """
            SELECT publicado_em, tentativas, erro_publicacao
            FROM outbox WHERE request_id = %s
            """,
            (request_id,),
        ).fetchone()
    assert pendente["publicado_em"] is None
    assert pendente["tentativas"] == 1
    assert pendente["erro_publicacao"]

    recuperado = OutboxPublisher(banco_limpo, rabbitmq_limpo)
    try:
        assert recuperado.publicar_lote() == 1
    finally:
        recuperado.fechar()


def test_dois_workers_executam_ciclo_completo_e_transferencia_fifo(
    banco_limpo, rabbitmq_limpo
):
    with conectar(banco_limpo) as conexao:
        conexao.execute(
            "UPDATE livros SET capacidade = 1, disponiveis = 1 WHERE id = 1"
        )

    worker_a = Worker(
        banco_limpo, rabbitmq_limpo, worker_id="worker-a", prefetch_count=1
    )
    worker_b = Worker(
        banco_limpo, rabbitmq_limpo, worker_id="worker-b", prefetch_count=1
    )
    thread_a = _iniciar_worker(worker_a)
    thread_b = _iniciar_worker(worker_b)
    publisher = OutboxPublisher(banco_limpo, rabbitmq_limpo)
    repositorio = RepositorioBiblioteca(banco_limpo)
    gateway, thread_gateway, porta = _iniciar_gateway(banco_limpo)
    cliente = Cliente(porta=porta)

    pedido_1 = uuid4()
    pedido_2 = uuid4()
    try:
        cliente.pedir_livro(1, 1, pedido_1)
        cliente.pedir_livro(2, 1, pedido_2)
        assert publisher.publicar_lote() == 2

        _esperar(
            lambda: {
                cliente.consultar_status(pedido_1)["status"],
                cliente.consultar_status(pedido_2)["status"],
            }
            == {"CONCLUIDO", "AGUARDANDO"}
        )
        ativo = repositorio.listar_emprestimos_ativos(1)[0]
        assert ativo["usuario_id"] == 1

        devolucao_id = uuid4()
        cliente.devolver_livro(1, ativo["id"], devolucao_id)
        assert publisher.publicar_lote() == 1

        _esperar(
            lambda: cliente.consultar_status(devolucao_id)["status"]
            == "CONCLUIDO"
            and cliente.consultar_status(pedido_2)["status"]
            == "CONCLUIDO"
        )
        novo_ativo = repositorio.listar_emprestimos_ativos(1)
        assert len(novo_ativo) == 1
        assert novo_ativo[0]["usuario_id"] == 2
        assert repositorio.consultar_livro(1)["disponiveis"] == 0
        assert worker_a.mensagens_processadas > 0
        assert worker_b.mensagens_processadas > 0
    finally:
        cliente.desconectar()
        _encerrar_gateway(gateway, thread_gateway)
        publisher.fechar()
        _encerrar_worker(worker_a, thread_a)
        _encerrar_worker(worker_b, thread_b)


def test_mensagem_invalida_vai_diretamente_para_dlq(
    banco_limpo, rabbitmq_limpo
):
    worker = Worker(banco_limpo, rabbitmq_limpo, worker_id="worker-invalida")
    thread = _iniciar_worker(worker)
    conexao = pika.BlockingConnection(parametros_conexao(rabbitmq_limpo))
    try:
        channel = conexao.channel()
        declarar_topologia(channel)
        channel.confirm_delivery()
        channel.basic_publish(
            exchange=COMMAND_EXCHANGE,
            routing_key=COMMAND_ROUTING_KEY,
            body=b"nao-json",
            properties=pika.BasicProperties(delivery_mode=2),
            mandatory=True,
        )
        _esperar(lambda: _quantidade_fila(rabbitmq_limpo, DEAD_LETTER_QUEUE) == 1)
        assert worker.mensagens_dlq == 1
    finally:
        conexao.close()
        _encerrar_worker(worker, thread)


def test_mensagens_fora_de_ordem_e_duplicadas_preservam_fifo_e_idempotencia(
    banco_limpo, rabbitmq_limpo
):
    with conectar(banco_limpo) as conexao:
        conexao.execute(
            "UPDATE livros SET capacidade = 1, disponiveis = 1 WHERE id = 1"
        )

    solicitacoes = ServicoSolicitacoes(banco_limpo)
    pedido_1 = uuid4()
    pedido_2 = uuid4()
    solicitacoes.aceitar_pedido(pedido_1, 1, 1)
    solicitacoes.aceitar_pedido(pedido_2, 2, 1)
    with conectar(banco_limpo) as conexao:
        eventos = {
            str(linha["request_id"]): linha["payload"]
            for linha in conexao.execute(
                "SELECT request_id, payload FROM outbox"
            ).fetchall()
        }

    conexao = pika.BlockingConnection(parametros_conexao(rabbitmq_limpo))
    try:
        channel = conexao.channel()
        declarar_topologia(channel)
        channel.confirm_delivery()
        for request_id in (pedido_2, pedido_1, pedido_2):
            channel.basic_publish(
                exchange=COMMAND_EXCHANGE,
                routing_key=COMMAND_ROUTING_KEY,
                body=json.dumps(eventos[str(request_id)]).encode("utf-8"),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                    message_id=str(request_id),
                ),
                mandatory=True,
            )
    finally:
        conexao.close()

    worker = Worker(banco_limpo, rabbitmq_limpo, worker_id="worker-ordem")
    thread = _iniciar_worker(worker)
    try:
        _esperar(lambda: worker.mensagens_processadas == 3)
        assert solicitacoes.consultar_status(pedido_1)["status"] == "CONCLUIDO"
        assert solicitacoes.consultar_status(pedido_2)["status"] == "AGUARDANDO"
        ativos = RepositorioBiblioteca(banco_limpo).listar_emprestimos_ativos(1)
        assert len(ativos) == 1
        assert ativos[0]["borrow_request_id"] == pedido_1
    finally:
        _encerrar_worker(worker, thread)


def test_retry_limitado_encaminha_para_dlq(rabbitmq_limpo):
    chamadas = 0

    def sempre_falhar(request_id: str, tipo: str):
        nonlocal chamadas
        chamadas += 1
        raise FalhaTransitoria("falha controlada")

    worker = Worker(
        rabbitmq_url=rabbitmq_limpo,
        worker_id="worker-retry",
        max_retries=2,
        processador=sempre_falhar,
    )
    thread = _iniciar_worker(worker)
    request_id = str(uuid4())
    conexao = pika.BlockingConnection(parametros_conexao(rabbitmq_limpo))
    try:
        channel = conexao.channel()
        declarar_topologia(channel)
        channel.confirm_delivery()
        channel.basic_publish(
            exchange=COMMAND_EXCHANGE,
            routing_key=COMMAND_ROUTING_KEY,
            body=json.dumps(
                {"request_id": request_id, "tipo": "PEDIR"}
            ).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=request_id,
            ),
            mandatory=True,
        )
        _esperar(lambda: _quantidade_fila(rabbitmq_limpo, DEAD_LETTER_QUEUE) == 1)
        assert chamadas == 3
        assert worker.mensagens_dlq == 1
    finally:
        conexao.close()
        _encerrar_worker(worker, thread)

