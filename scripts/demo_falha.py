"""Demonstra crashes reais do worker antes do commit e antes do ACK.

Requer PostgreSQL e RabbitMQ de teste, sem outros workers conectados. O script
sobe um Gateway TCP local, envia cada comando por ele e usa subprocessos reais
do worker. Os livros de demonstração ficam persistidos para auditoria.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
import time
from typing import Any
from uuid import UUID, uuid4

import pika

from database.connection import conectar, obter_dsn
from messaging.config import obter_rabbitmq_url, parametros_conexao
from messaging.publisher import OutboxPublisher
from messaging.topology import COMMAND_QUEUE, DEAD_LETTER_QUEUE, declarar_topologia
from network.cliente import Cliente
from network.servidor import Servidor
from scripts.validar_invariantes import validar_estado


ROOT = Path(__file__).resolve().parent.parent
FAILPOINTS = (
    ("BEFORE_COMMIT", 91),
    ("AFTER_COMMIT_BEFORE_ACK", 92),
)


def _esperar(condicao, *, timeout: float = 20.0) -> Any:
    limite = time.monotonic() + timeout
    ultimo = None
    while time.monotonic() < limite:
        ultimo = condicao()
        if ultimo:
            return ultimo
        time.sleep(0.05)
    raise TimeoutError(f"Condição não satisfeita; último valor: {ultimo!r}")


def _filas(rabbitmq_url: str) -> dict[str, int]:
    rabbit = pika.BlockingConnection(parametros_conexao(rabbitmq_url))
    try:
        channel = rabbit.channel()
        declarar_topologia(channel)
        comandos = channel.queue_declare(queue=COMMAND_QUEUE, passive=True).method
        dlq = channel.queue_declare(queue=DEAD_LETTER_QUEUE, passive=True).method
        return {
            "comandos_prontos": comandos.message_count,
            "consumidores": comandos.consumer_count,
            "dlq": dlq.message_count,
        }
    finally:
        rabbit.close()


def _criar_livro(dsn: str, failpoint: str) -> int:
    titulo = f"DEMO_ETAPA_5_{failpoint}_{uuid4().hex[:8]}"
    with conectar(dsn) as conexao:
        return conexao.execute(
            """
            INSERT INTO livros (
                titulo, autor, edicao, genero, ano_publicacao, preco,
                capacidade, disponiveis
            ) VALUES (%s, 'Failpoint controlado', 1, 'Teste', 2026, 0, 1, 1)
            RETURNING id
            """,
            (titulo,),
        ).fetchone()["id"]


def _estado(dsn: str, rabbitmq_url: str, livro_id: int, request_id: UUID) -> dict:
    with conectar(dsn) as conexao:
        livro = conexao.execute(
            "SELECT capacidade, disponiveis FROM livros WHERE id = %s",
            (livro_id,),
        ).fetchone()
        ativos = conexao.execute(
            """
            SELECT COUNT(*)::int AS total
            FROM emprestimos
            WHERE livro_id = %s AND devolvido_em IS NULL
            """,
            (livro_id,),
        ).fetchone()["total"]
        solicitacao = conexao.execute(
            """
            SELECT status, emprestimo_id, ordem_fila
            FROM solicitacoes WHERE request_id = %s
            """,
            (request_id,),
        ).fetchone()
        outbox = conexao.execute(
            """
            SELECT publicado_em IS NOT NULL AS publicada, tentativas
            FROM outbox WHERE request_id = %s
            """,
            (request_id,),
        ).fetchone()
    return {
        "capacidade": livro["capacidade"],
        "disponiveis": livro["disponiveis"],
        "emprestimos_ativos": ativos,
        "solicitacao": solicitacao,
        "outbox": outbox,
        "rabbitmq": _filas(rabbitmq_url),
    }


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
            "WORKER_ID": f"demo-{failpoint.lower()}",
            "PYTHONUNBUFFERED": "1",
            "PYTHONUTF8": "1",
        }
    )
    return ambiente


def _iniciar_worker(ambiente: dict[str, str]) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-m", "workers.emprestimos"],
        cwd=ROOT,
        env=ambiente,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
    )


def _encerrar_worker(processo: subprocess.Popen[str]) -> str:
    if processo.poll() is None:
        processo.terminate()
    try:
        saida, _ = processo.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        processo.kill()
        saida, _ = processo.communicate(timeout=5)
    return saida


def _executar_cenario(
    dsn: str,
    rabbitmq_url: str,
    cliente: Cliente,
    diretorio_marcadores: Path,
    failpoint: str,
    codigo_esperado: int,
) -> dict:
    livro_id = _criar_livro(dsn, failpoint)
    request_id = uuid4()
    estado_inicial = _estado(dsn, rabbitmq_url, livro_id, request_id)
    aceite = cliente.pedir_livro(1, livro_id, request_id)

    publisher = OutboxPublisher(dsn, rabbitmq_url)
    try:
        publicados = publisher.publicar_lote()
    finally:
        publisher.fechar()
    if publicados != 1:
        raise RuntimeError(f"Esperado 1 evento publicado; obtido {publicados}")

    ambiente = _ambiente_worker(
        dsn, rabbitmq_url, diretorio_marcadores, failpoint, request_id
    )
    worker_falho = _iniciar_worker(ambiente)
    codigo = worker_falho.wait(timeout=20)
    logs_falha, _ = worker_falho.communicate(timeout=5)
    if codigo != codigo_esperado:
        raise RuntimeError(
            f"{failpoint} terminou em {codigo}, esperado {codigo_esperado}:\n"
            f"{logs_falha}"
        )
    _esperar(lambda: _filas(rabbitmq_url)["comandos_prontos"] == 1)
    estado_apos_queda = _estado(dsn, rabbitmq_url, livro_id, request_id)

    worker_recuperacao = _iniciar_worker(ambiente)
    try:
        _esperar(
            lambda: cliente.consultar_status(request_id)["status"] == "CONCLUIDO"
            and _filas(rabbitmq_url)["comandos_prontos"] == 0
        )
        time.sleep(0.25)
    finally:
        logs_recuperacao = _encerrar_worker(worker_recuperacao)
    estado_final = _estado(dsn, rabbitmq_url, livro_id, request_id)

    if estado_final["emprestimos_ativos"] != 1:
        raise AssertionError("A recuperação não terminou com exatamente 1 empréstimo")
    if estado_final["disponiveis"] != 0:
        raise AssertionError("O estoque final não corresponde ao empréstimo ativo")
    if "processado" not in logs_recuperacao:
        raise AssertionError("O worker de recuperação não confirmou o reprocessamento")

    return {
        "resultado": "APROVADO",
        "failpoint": failpoint,
        "request_id": str(request_id),
        "livro_id": livro_id,
        "aceite_gateway": aceite,
        "estado_inicial": estado_inicial,
        "estado_apos_queda": estado_apos_queda,
        "estado_final": estado_final,
        "exit_code_controlado": codigo,
        "logs_worker_falho": logs_falha.strip().splitlines(),
        "logs_worker_recuperacao": logs_recuperacao.strip().splitlines(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL"))
    parser.add_argument("--rabbitmq-url", default=os.getenv("RABBITMQ_URL"))
    args = parser.parse_args()
    dsn = obter_dsn(args.database_url)
    rabbitmq_url = obter_rabbitmq_url(args.rabbitmq_url)

    filas = _filas(rabbitmq_url)
    if filas != {"comandos_prontos": 0, "consumidores": 0, "dlq": 0}:
        raise RuntimeError(
            "A demo requer filas vazias e nenhum worker conectado; "
            f"estado atual: {filas}"
        )
    with conectar(dsn) as conexao:
        pendentes = conexao.execute(
            "SELECT COUNT(*)::int AS total FROM outbox WHERE publicado_em IS NULL"
        ).fetchone()["total"]
        usuario = conexao.execute("SELECT id FROM usuarios WHERE id = 1").fetchone()
    if pendentes:
        raise RuntimeError(f"A demo requer outbox vazia; há {pendentes} evento(s)")
    if usuario is None:
        raise RuntimeError("Execute migrations/seed antes da demonstração")

    gateway = Servidor(dsn, host="127.0.0.1", porta=0)
    thread_gateway = threading.Thread(target=gateway.iniciar, daemon=True)
    thread_gateway.start()
    if not gateway.pronto.wait(timeout=10):
        raise RuntimeError("Gateway local não iniciou")

    relatorio: dict[str, Any] = {"cenarios": []}
    try:
        with Cliente(host="127.0.0.1", porta=gateway.endereco[1]) as cliente:
            with TemporaryDirectory(prefix="biblioteca-failpoints-") as temporario:
                marcador = Path(temporario)
                for failpoint, codigo in FAILPOINTS:
                    relatorio["cenarios"].append(
                        _executar_cenario(
                            dsn,
                            rabbitmq_url,
                            cliente,
                            marcador,
                            failpoint,
                            codigo,
                        )
                    )
        relatorio["invariantes"] = validar_estado(dsn, rabbitmq_url)
        relatorio["resultado"] = (
            "APROVADO" if relatorio["invariantes"]["ok"] else "REPROVADO"
        )
        print(json.dumps(relatorio, ensure_ascii=False, default=str, indent=2))
        raise SystemExit(0 if relatorio["resultado"] == "APROVADO" else 1)
    finally:
        gateway.encerrar()
        thread_gateway.join(timeout=10)


if __name__ == "__main__":
    main()
