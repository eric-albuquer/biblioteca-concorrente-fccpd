"""Instala handlers mínimos de SIGINT/SIGTERM para containers."""

from __future__ import annotations

import signal
from collections.abc import Callable


def instalar_desligamento(parar: Callable[[], None]) -> None:
    def handler(signum, frame):
        parar()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

