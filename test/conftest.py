from __future__ import annotations

import os

import pytest

from database.connection import aplicar_schema, conectar, executar_sql


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

