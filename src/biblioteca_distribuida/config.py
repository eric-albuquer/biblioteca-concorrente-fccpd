"""Configuração central, obtida exclusivamente por variáveis de ambiente."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass


CATEGORIES = (
    "ficcao-cientifica",
    "romance",
    "aventura",
    "acao",
    "suspense",
    "terror",
)


@dataclass(frozen=True)
class Settings:
    database_url: str
    rabbitmq_url: str
    worker_name: str
    prefetch_count: int
    max_retries: int
    retry_delay_ms: int
    response_queue_expires_ms: int
    response_message_ttl_ms: int
    enable_fault_injection: bool

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.getenv(
                "DATABASE_URL",
                "postgresql://biblioteca:biblioteca@localhost:15432/biblioteca",
            ),
            rabbitmq_url=os.getenv(
                "RABBITMQ_URL",
                "amqp://biblioteca:biblioteca@localhost:5672/%2F",
            ),
            worker_name=os.getenv("WORKER_NAME", socket.gethostname()),
            prefetch_count=max(1, int(os.getenv("PREFETCH_COUNT", "1"))),
            max_retries=max(0, int(os.getenv("MAX_RETRIES", "3"))),
            retry_delay_ms=max(100, int(os.getenv("RETRY_DELAY_MS", "3000"))),
            response_queue_expires_ms=max(
                60_000, int(os.getenv("RESPONSE_QUEUE_EXPIRES_MS", "86400000"))
            ),
            response_message_ttl_ms=max(
                60_000, int(os.getenv("RESPONSE_MESSAGE_TTL_MS", "604800000"))
            ),
            enable_fault_injection=os.getenv("ENABLE_FAULT_INJECTION", "false")
            .strip()
            .lower()
            in {"1", "true", "yes"},
        )
