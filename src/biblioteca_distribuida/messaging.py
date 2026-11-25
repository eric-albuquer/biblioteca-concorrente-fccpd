"""Topologia RabbitMQ compartilhada por produtores e consumidores."""

from __future__ import annotations

import json
import re
import time
from datetime import UTC, datetime
from typing import Any

import pika

from .config import CATEGORIES, Settings


COMMAND_EXCHANGE = "biblioteca.commands"
RESPONSE_EXCHANGE = "biblioteca.responses"
RETRY_EXCHANGE = "biblioteca.retry"
DEAD_EXCHANGE = "biblioteca.dead"
DEAD_QUEUE = "biblioteca.dead"


def command_queue(category: str) -> str:
    return f"biblioteca.commands.{category}"


def retry_queue(category: str) -> str:
    return f"biblioteca.retry.{category}"


def response_queue(client_id: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_.-]", "-", client_id)
    return f"biblioteca.responses.{safe}"


def connection_parameters(url: str) -> pika.URLParameters:
    parameters = pika.URLParameters(url)
    parameters.heartbeat = 30
    parameters.blocked_connection_timeout = 30
    parameters.connection_attempts = 1
    parameters.socket_timeout = 10
    return parameters


def connect_with_retry(url: str, *, attempts: int = 30) -> pika.BlockingConnection:
    delay = 1.0
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            return pika.BlockingConnection(connection_parameters(url))
        except pika.exceptions.AMQPConnectionError as error:
            last_error = error
            time.sleep(delay)
            delay = min(delay * 1.5, 5.0)
    raise RuntimeError("RabbitMQ indisponível após várias tentativas.") from last_error


def declare_topology(channel: pika.channel.Channel, settings: Settings) -> None:
    channel.exchange_declare(
        exchange=COMMAND_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        exchange=RESPONSE_EXCHANGE, exchange_type="direct", durable=True
    )
    channel.exchange_declare(
        exchange=RETRY_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        exchange=DEAD_EXCHANGE, exchange_type="topic", durable=True
    )

    channel.queue_declare(queue=DEAD_QUEUE, durable=True)
    channel.queue_bind(queue=DEAD_QUEUE, exchange=DEAD_EXCHANGE, routing_key="#")

    for category in CATEGORIES:
        commands = command_queue(category)
        retry = retry_queue(category)
        channel.queue_declare(queue=commands, durable=True)
        channel.queue_bind(
            queue=commands,
            exchange=COMMAND_EXCHANGE,
            routing_key=f"{category}.*",
        )
        channel.queue_declare(
            queue=retry,
            durable=True,
            arguments={
                "x-message-ttl": settings.retry_delay_ms,
                "x-dead-letter-exchange": COMMAND_EXCHANGE,
            },
        )
        channel.queue_bind(
            queue=retry,
            exchange=RETRY_EXCHANGE,
            routing_key=f"{category}.*",
        )


def declare_response_queue(
    channel: pika.channel.Channel, settings: Settings, client_id: str
) -> str:
    queue = response_queue(client_id)
    channel.queue_declare(
        queue=queue,
        durable=True,
        arguments={
            "x-expires": settings.response_queue_expires_ms,
            "x-message-ttl": settings.response_message_ttl_ms,
        },
    )
    channel.queue_bind(
        queue=queue, exchange=RESPONSE_EXCHANGE, routing_key=client_id
    )
    return queue


def persistent_properties(
    *,
    correlation_id: str,
    message_id: str,
    headers: dict[str, Any] | None = None,
) -> pika.BasicProperties:
    return pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        delivery_mode=pika.DeliveryMode.Persistent,
        correlation_id=correlation_id,
        message_id=message_id,
        timestamp=int(datetime.now(UTC).timestamp()),
        headers=headers or {},
    )


def publish_json(
    channel: pika.channel.Channel,
    *,
    exchange: str,
    routing_key: str,
    payload: dict[str, Any],
    correlation_id: str,
    message_id: str,
    headers: dict[str, Any] | None = None,
) -> None:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    publish_bytes(
        channel,
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=persistent_properties(
            correlation_id=correlation_id,
            message_id=message_id,
            headers=headers,
        ),
    )


def publish_bytes(
    channel: pika.channel.Channel,
    *,
    exchange: str,
    routing_key: str,
    body: bytes,
    properties: pika.BasicProperties,
) -> None:
    accepted = channel.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=properties,
        mandatory=True,
    )
    if accepted is False:
        raise pika.exceptions.NackError([])
