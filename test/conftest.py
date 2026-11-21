from __future__ import annotations

import os

import pika
import pytest

from database.connection import aplicar_schema, conectar, executar_sql
from messaging.config import parametros_conexao
from messaging.topology import COMMAND_QUEUE, DEAD_LETTER_QUEUE, declarar_topologia


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip(
            "TEST_DATABASE_URL não configurada; testes PostgreSQL exigem banco real"
        )
    aplicar_schema(dsn)
    return dsn


@pytest.fixture()
def banco_limpo(postgres_dsn: str) -> str:
    with conectar(postgres_dsn, autocommit=True) as conexao:
        conexao.execute(
            """
            TRUNCATE TABLE outbox, emprestimos, solicitacoes, livros, usuarios
            RESTART IDENTITY CASCADE
            """
        )
    executar_sql("database/seed.sql", postgres_dsn)
    return postgres_dsn


@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    url = os.getenv("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip(
            "TEST_RABBITMQ_URL não configurada; testes RabbitMQ exigem broker real"
        )
    return url


@pytest.fixture()
def rabbitmq_limpo(rabbitmq_url: str) -> str:
    conexao = pika.BlockingConnection(parametros_conexao(rabbitmq_url))
    try:
        channel = conexao.channel()
        declarar_topologia(channel)
        channel.queue_purge(COMMAND_QUEUE)
        channel.queue_purge(DEAD_LETTER_QUEUE)
    finally:
        conexao.close()

    yield rabbitmq_url

    conexao = pika.BlockingConnection(parametros_conexao(rabbitmq_url))
    try:
        channel = conexao.channel()
        declarar_topologia(channel)
        channel.queue_purge(COMMAND_QUEUE)
        channel.queue_purge(DEAD_LETTER_QUEUE)
    finally:
        conexao.close()

