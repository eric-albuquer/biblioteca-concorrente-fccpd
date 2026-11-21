"""Entrypoint do worker de empréstimos/devoluções."""

import logging

from messaging.consumer import Worker


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    worker = Worker()
    try:
        worker.iniciar()
    except KeyboardInterrupt:
        worker.encerrar()

