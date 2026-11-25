"""Aplica migrations SQL de forma idempotente."""

from __future__ import annotations

import logging
from pathlib import Path

import psycopg

from .config import Settings


LOGGER = logging.getLogger("biblioteca.migrate")
MIGRATION_LOCK_ID = 2_026_112_501


def run_migrations(database_url: str, directory: Path | None = None) -> None:
    migrations = directory or Path(__file__).resolve().parents[2] / "migrations"
    files = sorted(migrations.glob("*.sql"))
    if not files:
        raise RuntimeError(f"Nenhuma migration encontrada em {migrations}.")

    with psycopg.connect(database_url) as connection:
        with connection.transaction():
            connection.execute(
                "SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK_ID,)
            )
            for migration in files:
                LOGGER.info("aplicando migration", extra={"file": migration.name})
                connection.execute(migration.read_text(encoding="utf-8"))


def main() -> None:
    logging.basicConfig(
        level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    run_migrations(Settings.from_env().database_url)
    LOGGER.info("migrations concluídas")


if __name__ == "__main__":
    main()
