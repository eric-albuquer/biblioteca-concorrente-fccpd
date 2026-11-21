"""Nomes e declaração idempotente da topologia AMQP."""

from __future__ import annotations


COMMAND_EXCHANGE = "biblioteca"
COMMAND_QUEUE = "biblioteca.comandos"
COMMAND_ROUTING_KEY = "biblioteca.comandos"

DEAD_LETTER_EXCHANGE = "biblioteca.dlx"
DEAD_LETTER_QUEUE = "biblioteca.comandos.dlq"
DEAD_LETTER_ROUTING_KEY = "biblioteca.comandos.dlq"


def declarar_topologia(channel) -> None:
    channel.exchange_declare(
        exchange=COMMAND_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    channel.exchange_declare(
        exchange=DEAD_LETTER_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )
    channel.queue_declare(
        queue=DEAD_LETTER_QUEUE,
        durable=True,
    )
    channel.queue_bind(
        queue=DEAD_LETTER_QUEUE,
        exchange=DEAD_LETTER_EXCHANGE,
        routing_key=DEAD_LETTER_ROUTING_KEY,
    )
    channel.queue_declare(
        queue=COMMAND_QUEUE,
        durable=True,
        arguments={
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE,
            "x-dead-letter-routing-key": DEAD_LETTER_ROUTING_KEY,
        },
    )
    channel.queue_bind(
        queue=COMMAND_QUEUE,
        exchange=COMMAND_EXCHANGE,
        routing_key=COMMAND_ROUTING_KEY,
    )

