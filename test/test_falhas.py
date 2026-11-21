from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from uuid import UUID, uuid4

import pika

from database.connection import conectar
from database.repository import RepositorioBiblioteca
from messaging.config import parametros_conexao
from messaging.consumer import Worker
from messaging.publisher import OutboxPublisher
from messaging.topology import COMMAND_QUEUE
from network.cliente import Cliente
from network.servidor import Servidor
from scripts.validar_invariantes import validar_estado
from services.emprestimos import ServicoEmprestimos
from services.solicitacoes import ServicoSolicitacoes


ROOT = Path(__file__).resolve().parent.parent


def _esperar(condicao, *, timeout: float = 15.0):
    limite = time.monotonic() + timeout
    ultimo = None
    while time.monotonic() < limite:
        ultimo = condicao()
        if ultimo:
            return ultimo
        time.sleep(0.05)
    raise TimeoutError(f"Condição não satisfeita; último valor: {ultimo!r}")


def _estado_fila(rabbitmq_url: str) -> tuple[int, int]:
    conexao = pika.BlockingConnection(parametros_conexao(rabbitmq_url))
    try:
        fila = conexao.channel().queue_declare(
            queue=COMMAND_QUEUE,
            passive=True,
        )
        return fila.method.message_count, fila.method.consumer_count
    finally:
        conexao.close()


def _publicar(dsn: str, rabbitmq_url: str) -> int:
    publisher = OutboxPublisher(dsn, rabbitmq_url)
    try:
        return publisher.publicar_lote()
    finally:
        publisher.fechar()


def _ambiente_worker(
    dsn: str,
    rabbitmq_url: str,
    marcador: Path,
    failpoint: str,
    request_id: UUID,
) -> dict[str, str]:
    ambiente = os.environ.copy()
    ambiente.update(
        {
            "DATABASE_URL": dsn,
            "RABBITMQ_URL": rabbitmq_url,
            "ENABLE_FAILPOINTS": "1",
            "FAILPOINT": failpoint,
            "FAILPOINT_REQUEST_ID": str(request_id),
            "FAILPOINT_MARKER_DIR": str(marcador),
            "WORKER_ID": f"teste-{failpoint.lower()}",
            "PYTHONUNBUFFERED": "1",
            "PYTHONUTF8": "1",
        }
    )
    return ambiente


def _iniciar_processo_worker(ambiente: dict[str, str]) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "workers.emprestimos"],
        cwd=ROOT,
        env=ambiente,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
    )


def _encerrar_processo(processo: subprocess.Popen) -> str:
    if processo.poll() is None:
        processo.terminate()
    try:
        saida, _ = processo.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        processo.kill()
        saida, _ = processo.communicate(timeout=5)
    return saida


def _iniciar_gateway(dsn: str) -> tuple[Servidor, threading.Thread, Cliente]:
    gateway = Servidor(dsn, host="127.0.0.1", porta=0)
    thread = threading.Thread(target=gateway.iniciar, daemon=True)
    thread.start()
    assert gateway.pronto.wait(timeout=10)
    return gateway, thread, Cliente(porta=gateway.endereco[1])


def _encerrar_gateway(
    gateway: Servidor, thread: threading.Thread, cliente: Cliente
) -> None:
    cliente.desconectar()
    gateway.encerrar()
    thread.join(timeout=10)
    assert not thread.is_alive()


def test_validador_detecta_e_reporta_invariante_global(banco_limpo):
    valido = validar_estado(banco_limpo, verificar_rabbit=False)
    assert valido["ok"] is True

    with conectar(banco_limpo) as conexao:
        conexao.execute("UPDATE livros SET disponiveis = 2 WHERE id = 1")

    invalido = validar_estado(banco_limpo, verificar_rabbit=False)
    assert invalido["ok"] is False
    assert any(
        "disponíveis + ativos" in violacao
        for violacao in invalido["violacoes"]
    )


def test_aguardando_e_request_id_sobrevivem_a_novas_instancias(banco_limpo):
    with conectar(banco_limpo) as conexao:
        conexao.execute(
            "UPDATE livros SET capacidade = 1, disponiveis = 1 WHERE id = 1"
        )

    aceite = ServicoSolicitacoes(banco_limpo)
    dominio = ServicoEmprestimos(banco_limpo)
    primeiro = uuid4()
    segundo = uuid4()
    aceite.aceitar_pedido(primeiro, 1, 1)
    aceite.aceitar_pedido(segundo, 2, 1)
    dominio.processar_comando(primeiro, "PEDIR")
    dominio.processar_comando(segundo, "PEDIR")

    nova_instancia = ServicoSolicitacoes(banco_limpo)
    assert nova_instancia.consultar_status(primeiro)["status"] == "CONCLUIDO"
    assert nova_instancia.consultar_status(segundo)["status"] == "AGUARDANDO"

    repetido = nova_instancia.aceitar_pedido(segundo, 2, 1)
    assert repetido["status"] == "AGUARDANDO"
    with conectar(banco_limpo) as conexao:
        assert conexao.execute(
            "SELECT COUNT(*) AS total FROM solicitacoes"
        ).fetchone()["total"] == 2
        assert conexao.execute(
            "SELECT COUNT(*) AS total FROM outbox"
        ).fetchone()["total"] == 2


def test_request_id_repetido_antes_e_depois_do_processamento(banco_limpo):
    request_id = uuid4()
    solicitacoes = ServicoSolicitacoes(banco_limpo)
    dominio = ServicoEmprestimos(banco_limpo)

    primeira = solicitacoes.aceitar_pedido(request_id, 1, 1)
    antes = solicitacoes.aceitar_pedido(request_id, 1, 1)
    dominio.processar_comando(request_id, "PEDIR")
    depois = solicitacoes.aceitar_pedido(request_id, 1, 1)
    dominio.processar_comando(request_id, "PEDIR")

    assert primeira["request_id"] == antes["request_id"] == depois["request_id"]
    assert antes["status"] == "RECEBIDO"
    assert depois["status"] == "CONCLUIDO"
    with conectar(banco_limpo) as conexao:
        assert conexao.execute(
            "SELECT COUNT(*) AS total FROM solicitacoes WHERE request_id = %s",
            (request_id,),
        ).fetchone()["total"] == 1
        assert conexao.execute(
            "SELECT COUNT(*) AS total FROM outbox WHERE request_id = %s",
            (request_id,),
        ).fetchone()["total"] == 1
        assert conexao.execute(
            "SELECT COUNT(*) AS total FROM emprestimos WHERE borrow_request_id = %s",
            (request_id,),
        ).fetchone()["total"] == 1


def test_workers_offline_processam_todos_os_pedidos_ao_retornar(
    banco_limpo, rabbitmq_limpo
):
    gateway, thread_gateway, cliente = _iniciar_gateway(banco_limpo)
    ids = [uuid4() for _ in range(5)]
    try:
        for indice, request_id in enumerate(ids):
            cliente.pedir_livro((indice % 4) + 1, 1, request_id)
        assert _publicar(banco_limpo, rabbitmq_limpo) == 5
        assert {
            cliente.consultar_status(request_id)["status"] for request_id in ids
        } == {"RECEBIDO"}

        workers = [
            Worker(banco_limpo, rabbitmq_limpo, worker_id=f"retorno-{indice}")
            for indice in range(2)
        ]
        threads = [
            threading.Thread(target=worker.iniciar, daemon=True)
            for worker in workers
        ]
        for thread in threads:
            thread.start()
        for worker in workers:
            assert worker.pronto.wait(timeout=10)
        try:
            _esperar(
                lambda: all(
                    cliente.consultar_status(request_id)["status"]
                    in {"CONCLUIDO", "AGUARDANDO"}
                    for request_id in ids
                )
                and _estado_fila(rabbitmq_limpo)[0] == 0
            )
            estados = [
                cliente.consultar_status(request_id)["status"]
                for request_id in ids
            ]
            assert estados.count("CONCLUIDO") == 3
            assert estados.count("AGUARDANDO") == 2
        finally:
            for worker in workers:
                worker.encerrar()
            for thread in threads:
                thread.join(timeout=10)
    finally:
        _encerrar_gateway(gateway, thread_gateway, cliente)


def test_queda_real_antes_do_commit_faz_rollback_e_redelivery(
    banco_limpo, rabbitmq_limpo, tmp_path
):
    request_id = uuid4()
    solicitacoes = ServicoSolicitacoes(banco_limpo)
    solicitacoes.aceitar_pedido(request_id, 1, 1)
    assert _publicar(banco_limpo, rabbitmq_limpo) == 1
    ambiente = _ambiente_worker(
        banco_limpo,
        rabbitmq_limpo,
        tmp_path,
        "BEFORE_COMMIT",
        request_id,
    )

    falho = _iniciar_processo_worker(ambiente)
    codigo = falho.wait(timeout=20)
    logs_falha, _ = falho.communicate(timeout=5)
    assert codigo == 91
    assert "FAILPOINT=BEFORE_COMMIT" in logs_falha
    assert solicitacoes.consultar_status(request_id)["status"] == "RECEBIDO"
    assert RepositorioBiblioteca(banco_limpo).listar_emprestimos_ativos(1) == []

    recuperacao = _iniciar_processo_worker(ambiente)
    try:
        _esperar(
            lambda: solicitacoes.consultar_status(request_id)["status"]
            == "CONCLUIDO"
            and _estado_fila(rabbitmq_limpo)[0] == 0
        )
    finally:
        logs_recuperacao = _encerrar_processo(recuperacao)
    assert _estado_fila(rabbitmq_limpo)[0] == 0
    assert "processado" in logs_recuperacao
    assert len(RepositorioBiblioteca(banco_limpo).listar_emprestimos_ativos(1)) == 1


def test_queda_real_depois_do_commit_redelivery_nao_duplica(
    banco_limpo, rabbitmq_limpo, tmp_path
):
    request_id = uuid4()
    solicitacoes = ServicoSolicitacoes(banco_limpo)
    solicitacoes.aceitar_pedido(request_id, 1, 1)
    assert _publicar(banco_limpo, rabbitmq_limpo) == 1
    ambiente = _ambiente_worker(
        banco_limpo,
        rabbitmq_limpo,
        tmp_path,
        "AFTER_COMMIT_BEFORE_ACK",
        request_id,
    )

    falho = _iniciar_processo_worker(ambiente)
    codigo = falho.wait(timeout=20)
    logs_falha, _ = falho.communicate(timeout=5)
    assert codigo == 92
    assert "FAILPOINT=AFTER_COMMIT_BEFORE_ACK" in logs_falha
    assert solicitacoes.consultar_status(request_id)["status"] == "CONCLUIDO"
    assert len(RepositorioBiblioteca(banco_limpo).listar_emprestimos_ativos(1)) == 1

    recuperacao = _iniciar_processo_worker(ambiente)
    try:
        _esperar(
            lambda: _estado_fila(rabbitmq_limpo)[0] == 0
            and _estado_fila(rabbitmq_limpo)[1] >= 1
        )
        time.sleep(0.5)
    finally:
        logs_recuperacao = _encerrar_processo(recuperacao)

    assert _estado_fila(rabbitmq_limpo)[0] == 0
    assert solicitacoes.consultar_status(request_id)["status"] == "CONCLUIDO"
    ativos = RepositorioBiblioteca(banco_limpo).listar_emprestimos_ativos(1)
    assert len(ativos) == 1
    assert ativos[0]["borrow_request_id"] == request_id
    assert "processado" in logs_recuperacao
