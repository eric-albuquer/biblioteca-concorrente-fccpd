"""Entrypoint do worker de empréstimos/devoluções."""

import logging

from messaging.consumer import Worker
from scripts.sinais import instalar_desligamento


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    worker = Worker()
    instalar_desligamento(worker.encerrar)
    worker.iniciar()

