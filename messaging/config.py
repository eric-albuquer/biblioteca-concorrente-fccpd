"""Configuração comum e conexão AMQP."""

from __future__ import annotations

import os

import pika


DEFAULT_RABBITMQ_URL = "amqp://guest:guest@127.0.0.1:5672/%2F"


def obter_rabbitmq_url(url: str | None = None) -> str:
    return url or os.getenv("RABBITMQ_URL", DEFAULT_RABBITMQ_URL)


def parametros_conexao(url: str | None = None) -> pika.URLParameters:
    parametros = pika.URLParameters(obter_rabbitmq_url(url))
    parametros.heartbeat = 30
    parametros.blocked_connection_timeout = 30
    parametros.socket_timeout = 5
    parametros.connection_attempts = 1
    return parametros

