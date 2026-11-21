"""Conexões e inicialização do PostgreSQL.

O módulo não abre conexão no import. Cada operação recebe explicitamente uma
DSN ou usa ``DATABASE_URL``, o que facilita processos separados e testes.
"""

from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
from typing import Iterator

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row


DATABASE_DIR = Path(__file__).resolve().parent


def obter_dsn(dsn: str | None = None) -> str:
    valor = dsn or os.getenv("DATABASE_URL")
    if not valor:
        raise RuntimeError(
            "DATABASE_URL não configurada. Informe uma DSN PostgreSQL, por "
            "exemplo: postgresql://biblioteca:senha@localhost:5432/biblioteca"
        )
    return valor


def conectar(dsn: str | None = None, *, autocommit: bool = False) -> Connection:
    """Cria uma conexão nova; conexões não são compartilhadas entre threads."""
    return psycopg.connect(
        obter_dsn(dsn),
        autocommit=autocommit,
        row_factory=dict_row,
    )


@contextmanager
def transacao(dsn: str | None = None) -> Iterator[Connection]:
    """Entrega uma conexão dentro de uma transação com commit/rollback seguro."""
    with conectar(dsn) as conexao:
        with conexao.transaction():
            yield conexao


def executar_sql(caminho: str | Path, dsn: str | None = None) -> None:
    """Executa um arquivo SQL local sem interpolar dados da aplicação."""
    sql = Path(caminho).read_text(encoding="utf-8")
    with conectar(dsn, autocommit=True) as conexao:
        conexao.execute(sql)


def aplicar_schema(dsn: str | None = None, *, incluir_seed: bool = False) -> None:
    executar_sql(DATABASE_DIR / "schema.sql", dsn)
    if incluir_seed:
        executar_sql(DATABASE_DIR / "seed.sql", dsn)

