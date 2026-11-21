"""Aplica schema e seed idempotentes, com retry limitado de conexão."""

from __future__ import annotations

import logging
import os
import time

from psycopg import Error as ErroBanco

from database.connection import aplicar_schema


LOGGER = logging.getLogger(__name__)


def main() -> None:
    tentativas = int(os.getenv("DB_INIT_MAX_ATTEMPTS", "30"))
    intervalo = float(os.getenv("DB_INIT_RETRY_SECONDS", "1"))

    for tentativa in range(1, tentativas + 1):
        try:
            aplicar_schema(incluir_seed=True)
            LOGGER.info("Schema e seed aplicados com sucesso")
            return
        except ErroBanco as erro:
            if tentativa == tentativas:
                raise
            LOGGER.warning(
                "Banco indisponível na tentativa %s/%s: %s",
                tentativa,
                tentativas,
                erro,
            )
            time.sleep(intervalo)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    main()

