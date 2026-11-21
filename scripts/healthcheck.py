"""Healthchecks pequenos para os serviços da imagem."""

from __future__ import annotations

import socket
import sys

import pika

from database.connection import conectar
from messaging.config import parametros_conexao


def verificar_gateway() -> None:
    with socket.create_connection(("127.0.0.1", 9999), timeout=3):
        pass


def verificar_dependencias() -> None:
    with conectar() as conexao:
        assert conexao.execute("SELECT 1 AS ok").fetchone()["ok"] == 1
    rabbit = pika.BlockingConnection(parametros_conexao())
    rabbit.close()


if __name__ == "__main__":
    modo = sys.argv[1] if len(sys.argv) > 1 else "dependencies"
    if modo == "gateway":
        verificar_gateway()
    elif modo == "dependencies":
        verificar_dependencias()
    else:
        raise SystemExit(f"Healthcheck desconhecido: {modo}")

