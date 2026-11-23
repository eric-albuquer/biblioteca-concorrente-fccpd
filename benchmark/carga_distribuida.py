"""Carga ponta a ponta: Gateway, outbox, RabbitMQ e workers em processos."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from typing import Any
from uuid import UUID, uuid4

import pika

from benchmark.comum import AmostradorProcessos, resumir_latencias
from database.connection import conectar, executar_sql
from messaging.config import parametros_conexao
from messaging.publisher import OutboxPublisher
from messaging.topology import COMMAND_QUEUE, DEAD_LETTER_QUEUE, declarar_topologia
from network.cliente import Cliente
from network.servidor import Servidor


ROOT = Path(__file__).resolve().parent.parent


def _esperar(condicao, *, timeout: float = 30.0) -> Any:
    limite = time.monotonic() + timeout
    ultimo = None
    while time.monotonic() < limite:
        ultimo = condicao()
        if ultimo:
            return ultimo
        time.sleep(0.05)
    raise TimeoutError(f"Condição não satisfeita; último valor: {ultimo!r}")


def estado_rabbit(rabbitmq_url: str) -> dict[str, int]:
    rabbit = pika.BlockingConnection(parametros_conexao(rabbitmq_url))
    try:
        channel = rabbit.channel()
        declarar_topologia(channel)
        comandos = channel.queue_declare(queue=COMMAND_QUEUE, passive=True).method
        dlq = channel.queue_declare(queue=DEAD_LETTER_QUEUE, passive=True).method
        return {
            "mensagens": comandos.message_count,
            "consumidores": comandos.consumer_count,
            "dlq": dlq.message_count,
        }
    finally:
        rabbit.close()


def _limpar_rabbit(rabbitmq_url: str) -> None:
    rabbit = pika.BlockingConnection(parametros_conexao(rabbitmq_url))
    try:
        channel = rabbit.channel()
        declarar_topologia(channel)
        channel.queue_purge(COMMAND_QUEUE)
        channel.queue_purge(DEAD_LETTER_QUEUE)
    finally:
        rabbit.close()


def _preparar_base(dsn: str, volume: int, titulos: int) -> list[int]:
    with conectar(dsn, autocommit=True) as conexao:
        conexao.execute(
            """
            TRUNCATE TABLE outbox, emprestimos, solicitacoes, livros, usuarios
            RESTART IDENTITY CASCADE
            """
        )
    executar_sql(ROOT / "database" / "seed.sql", dsn)
    capacidade = math.ceil(volume / titulos) + 1
    ids = []
    with conectar(dsn) as conexao:
        for indice in range(titulos):
            livro = conexao.execute(
                """
                INSERT INTO livros (
                    titulo, autor, edicao, genero, ano_publicacao, preco,
                    capacidade, disponiveis
                ) VALUES (%s, 'Benchmark', 1, 'Carga', 2026, 0, %s, %s)
                RETURNING id
                """,
                (f"BENCHMARK_{indice + 1}_{uuid4().hex[:8]}", capacidade, capacidade),
            ).fetchone()
            ids.append(livro["id"])
    return ids


def _iniciar_worker(dsn: str, rabbitmq_url: str, indice: int) -> subprocess.Popen:
    ambiente = os.environ.copy()
    caminhos_python = [
        str(ROOT),
        *(caminho for caminho in sys.path if "site-packages" in caminho),
    ]
    if ambiente.get("PYTHONPATH"):
        caminhos_python.append(ambiente["PYTHONPATH"])
    ambiente.update(
        {
            "DATABASE_URL": dsn,
            "RABBITMQ_URL": rabbitmq_url,
            "WORKER_ID": f"benchmark-{indice}",
            "WORKER_PREFETCH": "1",
            "ENABLE_FAILPOINTS": "0",
            "PYTHONUNBUFFERED": "1",
            "PYTHONUTF8": "1",
            "PYTHONPATH": os.pathsep.join(caminhos_python),
        }
    )
    executavel = getattr(sys, "_base_executable", sys.executable)
    return subprocess.Popen(
        [executavel, "-m", "workers.emprestimos"],
        cwd=ROOT,
        env=ambiente,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _encerrar_processo(processo: subprocess.Popen) -> None:
    if processo.poll() is None:
        processo.terminate()
    try:
        processo.wait(timeout=10)
    except subprocess.TimeoutExpired:
        processo.kill()
        processo.wait(timeout=5)


def _enviar(
    porta: int,
    request_id: UUID,
    usuario_id: int,
    livro_id: int,
) -> tuple[str, float, float, float, str | None]:
    inicio = time.perf_counter()
    try:
        with Cliente(porta=porta, timeout=10) as cliente:
            resposta = cliente.pedir_livro(usuario_id, livro_id, request_id)
        fim = time.perf_counter()
        return str(request_id), (fim - inicio) * 1000, inicio, fim, resposta["status"]
    except Exception as erro:
        fim = time.perf_counter()
        return str(request_id), (fim - inicio) * 1000, inicio, fim, type(erro).__name__


def _observar_conclusoes(
    dsn: str,
    ids: list[str],
    aceites: dict[str, tuple[float, float]],
    *,
    timeout: float,
) -> tuple[dict[str, float], dict[str, str]]:
    pendentes = set(ids)
    conclusoes: dict[str, float] = {}
    estados: dict[str, str] = {}
    limite = time.monotonic() + timeout
    while pendentes and time.monotonic() < limite:
        with conectar(dsn) as conexao:
            linhas = conexao.execute(
                """
                SELECT request_id::text AS request_id, status
                FROM solicitacoes
                WHERE request_id = ANY(%s::uuid[])
                """,
                (list(pendentes),),
            ).fetchall()
        agora = time.perf_counter()
        for linha in linhas:
            request_id = linha["request_id"]
            estados[request_id] = linha["status"]
            if linha["status"] in {"CONCLUIDO", "REJEITADO"}:
                conclusoes[request_id] = agora
                pendentes.discard(request_id)
        if pendentes:
            time.sleep(0.01)
    for request_id in pendentes:
        estados.setdefault(request_id, "TIMEOUT")
    return conclusoes, estados


def executar_carga(
    dsn: str,
    rabbitmq_url: str,
    *,
    volume: int,
    concorrencia: int,
    titulos: int,
    workers: int,
    repeticao: int,
    timeout: float = 120.0,
) -> dict[str, Any]:
    if min(volume, concorrencia, titulos, workers) < 1:
        raise ValueError("volume, concorrência, títulos e workers devem ser positivos")
    try:
        _esperar(
            lambda: estado_rabbit(rabbitmq_url)["consumidores"] == 0,
            timeout=10,
        )
    except TimeoutError as erro:
        raise RuntimeError("Pare os workers externos antes do benchmark") from erro
    _limpar_rabbit(rabbitmq_url)
    livros = _preparar_base(dsn, volume, titulos)

    gateway = Servidor(dsn, host="127.0.0.1", porta=0)
    thread_gateway = threading.Thread(target=gateway.iniciar, daemon=True)
    publisher = OutboxPublisher(dsn, rabbitmq_url, poll_interval=0.01)
    thread_publisher = threading.Thread(target=publisher.iniciar, daemon=True)
    processos: list[subprocess.Popen] = []
    amostrador: AmostradorProcessos | None = None
    recursos: dict[str, float] = {
        "workers_cpu_s": 0.0,
        "workers_cpu_pct": 0.0,
        "workers_rss_pico_mb": 0.0,
    }

    try:
        thread_gateway.start()
        if not gateway.pronto.wait(timeout=10):
            raise RuntimeError("Gateway não iniciou")
        thread_publisher.start()
        processos = [
            _iniciar_worker(dsn, rabbitmq_url, indice)
            for indice in range(1, workers + 1)
        ]
        _esperar(
            lambda: estado_rabbit(rabbitmq_url)["consumidores"] == workers,
            timeout=30,
        )
        amostrador = AmostradorProcessos([processo.pid for processo in processos])
        amostrador.iniciar()

        request_ids = [uuid4() for _ in range(volume)]
        resultados = []
        inicio_total = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concorrencia) as executor:
            futuros = [
                executor.submit(
                    _enviar,
                    gateway.endereco[1],
                    request_id,
                    (indice % 4) + 1,
                    livros[indice % len(livros)],
                )
                for indice, request_id in enumerate(request_ids)
            ]
            for futuro in as_completed(futuros):
                resultados.append(futuro.result())
        fim_aceites = time.perf_counter()

        aceites = {
            request_id: (inicio, fim)
            for request_id, _, inicio, fim, status in resultados
            if status == "RECEBIDO"
        }
        conclusoes, estados = _observar_conclusoes(
            dsn,
            list(aceites),
            aceites,
            timeout=timeout,
        )
        fim_total = time.perf_counter()
        fila_ao_concluir = estado_rabbit(rabbitmq_url)["mensagens"]
        recursos = amostrador.encerrar()
        amostrador = None

        latencias_aceite = [linha[1] for linha in resultados]
        latencias_conclusao = [
            (conclusoes[request_id] - inicio) * 1000
            for request_id, (inicio, _) in aceites.items()
            if request_id in conclusoes
        ]
        esperas_fila = [
            (conclusoes[request_id] - fim) * 1000
            for request_id, (_, fim) in aceites.items()
            if request_id in conclusoes
        ]
        concluidos = sum(status == "CONCLUIDO" for status in estados.values())
        rejeitados = sum(status == "REJEITADO" for status in estados.values())
        erros_cliente = sum(linha[4] != "RECEBIDO" for linha in resultados)
        tempo_aceite = fim_aceites - inicio_total
        tempo_total = fim_total - inicio_total
        linha: dict[str, Any] = {
            "cenario": f"v{volume}_c{concorrencia}_t{titulos}",
            "volume": volume,
            "concorrencia": concorrencia,
            "titulos": titulos,
            "workers": workers,
            "repeticao": repeticao,
            "aceitos": len(aceites),
            "concluidos": concluidos,
            "rejeitados": rejeitados,
            "erros": erros_cliente + (len(aceites) - len(conclusoes)),
            "tempo_aceite_s": tempo_aceite,
            "tempo_total_s": tempo_total,
            "throughput_aceite_ops_s": len(aceites) / tempo_aceite,
            "throughput_conclusao_ops_s": concluidos / tempo_total,
            "fila_pronta_ao_concluir": fila_ao_concluir,
        }
        linha.update(resumir_latencias(latencias_aceite, "aceite"))
        linha.update(resumir_latencias(latencias_conclusao, "conclusao"))
        linha.update(resumir_latencias(esperas_fila, "fila"))
        linha.update(recursos)
        return linha
    finally:
        if amostrador is not None:
            amostrador.encerrar()
        publisher.encerrar()
        if thread_publisher.is_alive():
            thread_publisher.join(timeout=10)
        for processo in processos:
            _encerrar_processo(processo)
        if processos:
            try:
                _esperar(
                    lambda: estado_rabbit(rabbitmq_url)["consumidores"] == 0,
                    timeout=10,
                )
            except (TimeoutError, pika.exceptions.AMQPError, OSError):
                pass
        try:
            _limpar_rabbit(rabbitmq_url)
        except (pika.exceptions.AMQPError, OSError):
            pass
        gateway.encerrar()
        if thread_gateway.is_alive():
            thread_gateway.join(timeout=10)


def versoes_infraestrutura(dsn: str, rabbitmq_url: str) -> dict[str, Any]:
    with conectar(dsn) as conexao:
        postgres = conexao.execute("SELECT version() AS versao").fetchone()["versao"]
    rabbit = pika.BlockingConnection(parametros_conexao(rabbitmq_url))
    try:
        servidor = rabbit._impl.server_properties
        produto = servidor.get("product", servidor.get(b"product", "RabbitMQ"))
        versao = servidor.get("version", servidor.get(b"version", "desconhecida"))
        if isinstance(produto, bytes):
            produto = produto.decode()
        if isinstance(versao, bytes):
            versao = versao.decode()
    finally:
        rabbit.close()
    return {"postgresql": postgres, "rabbitmq": f"{produto} {versao}"}
