"""Demonstra fila de resposta durável, consumidor lento e DLQ."""

from __future__ import annotations

import json
import time
from uuid import uuid4

from biblioteca_distribuida.client import LibraryClient
from biblioteca_distribuida.config import Settings
from biblioteca_distribuida.messaging import (
    COMMAND_EXCHANGE,
    DEAD_QUEUE,
    connect_with_retry,
    declare_topology,
    persistent_properties,
    publish_bytes,
)


BOOK_ID = "66666666-6666-6666-6666-666666666666"
CATEGORY = "terror"


def disconnected_and_slow_client() -> None:
    client_id = f"resilience-{uuid4().hex[:8]}"
    producer = LibraryClient(client_id)
    request_id = producer.borrow(BOOK_ID, CATEGORY)
    producer.close()

    # O worker pode responder enquanto o consumidor está desconectado.
    time.sleep(2)
    reconnected = LibraryClient(client_id)
    try:
        borrowed = reconnected.wait_for(request_id, timeout=30)
        if borrowed["status"] != "EMPRESTADO":
            raise RuntimeError(f"Resposta após reconexão inesperada: {borrowed}")

        return_id = reconnected.return_loan(borrowed["data"]["loan_id"], CATEGORY)
        # Consumidor deliberadamente lento: a resposta aguarda na fila durável.
        time.sleep(2)
        returned = reconnected.wait_for(return_id, timeout=30)
        if returned["status"] != "DEVOLVIDO":
            raise RuntimeError(f"Resposta do consumidor lento inesperada: {returned}")
        print(json.dumps(returned, ensure_ascii=False, indent=2))
    finally:
        reconnected.close()


def malformed_message_goes_to_dlq() -> None:
    settings = Settings.from_env()
    connection = connect_with_retry(settings.rabbitmq_url)
    try:
        channel = connection.channel()
        channel.confirm_delivery()
        declare_topology(channel, settings)
        channel.queue_purge(DEAD_QUEUE)
        message_id = str(uuid4())
        publish_bytes(
            channel,
            exchange=COMMAND_EXCHANGE,
            routing_key="terror.pedir",
            body=b"{json-invalido",
            properties=persistent_properties(
                correlation_id=message_id,
                message_id=message_id,
            ),
        )

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            method, properties, body = channel.basic_get(DEAD_QUEUE, auto_ack=False)
            if method is not None:
                channel.basic_ack(method.delivery_tag)
                if properties.message_id != message_id or body != b"{json-invalido":
                    raise RuntimeError("A DLQ recebeu uma mensagem diferente da enviada.")
                print(
                    "DLQ OK:",
                    properties.message_id,
                    (properties.headers or {}).get("x-error"),
                )
                return
            time.sleep(0.1)
        raise TimeoutError("Mensagem inválida não chegou à DLQ.")
    finally:
        connection.close()


def main() -> None:
    disconnected_and_slow_client()
    malformed_message_goes_to_dlq()
    print("MESSAGING RESILIENCE DEMO: OK")


if __name__ == "__main__":
    main()
