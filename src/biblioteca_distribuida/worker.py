"""Consumer RabbitMQ e produtor de respostas."""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any

import pika

from .config import CATEGORIES, Settings
from .messages import Command, MessageValidationError
from .messaging import (
    DEAD_EXCHANGE,
    RESPONSE_EXCHANGE,
    RETRY_EXCHANGE,
    command_queue,
    connect_with_retry,
    declare_topology,
    persistent_properties,
    publish_bytes,
    publish_json,
)
from .service import LoanService, PermanentProcessingError


LOGGER = logging.getLogger("biblioteca.worker")


class Worker:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.service = LoanService(settings.database_url)

    def close(self) -> None:
        self.service.close()

    def run_forever(self) -> None:
        while True:
            try:
                self._consume_once()
            except KeyboardInterrupt:
                return
            except Exception:
                LOGGER.exception(
                    "conexão do worker interrompida worker=%s; reconectando",
                    self.settings.worker_name,
                )
                time.sleep(2)

    def _consume_once(self) -> None:
        connection = connect_with_retry(self.settings.rabbitmq_url)
        try:
            channel = connection.channel()
            channel.confirm_delivery()
            declare_topology(channel, self.settings)
            channel.basic_qos(prefetch_count=self.settings.prefetch_count)
            for category in CATEGORIES:
                channel.basic_consume(
                    queue=command_queue(category),
                    on_message_callback=self._on_message,
                    auto_ack=False,
                )
            LOGGER.info(
                "worker pronto worker=%s categorias=%s prefetch=%s",
                self.settings.worker_name,
                ",".join(CATEGORIES),
                self.settings.prefetch_count,
            )
            channel.start_consuming()
        finally:
            if connection.is_open:
                connection.close()

    def _on_message(self, channel, method, properties, body: bytes) -> None:
        request_id = properties.correlation_id or properties.message_id or "desconhecido"
        try:
            command = Command.from_json(body)
            request_id = command.request_id
            if method.routing_key != command.routing_key():
                raise MessageValidationError(
                    "Routing key não corresponde à categoria/ação da mensagem."
                )

            outgoing = self.service.process(command)
            headers = properties.headers or {}
            if (
                self.settings.enable_fault_injection
                and headers.get("x-test-crash-after-commit")
                and not method.redelivered
            ):
                LOGGER.error(
                    "falha injetada após commit worker=%s request_id=%s",
                    self.settings.worker_name,
                    request_id,
                )
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(70)

            for message in outgoing:
                publish_json(
                    channel,
                    exchange=RESPONSE_EXCHANGE,
                    routing_key=message.routing_key,
                    payload=message.payload,
                    correlation_id=message.payload["request_id"],
                    message_id=message.payload["event_id"],
                )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            LOGGER.info(
                "comando confirmado worker=%s request_id=%s redelivered=%s respostas=%s",
                self.settings.worker_name,
                request_id,
                method.redelivered,
                len(outgoing),
            )
        except (MessageValidationError, PermanentProcessingError) as error:
            LOGGER.warning(
                "mensagem permanente enviada à DLQ worker=%s request_id=%s erro=%s",
                self.settings.worker_name,
                request_id,
                error,
            )
            self._dead_letter_or_requeue(
                channel, method, properties, body, error=str(error)
            )
        except Exception as error:
            LOGGER.exception(
                "falha transitória worker=%s request_id=%s",
                self.settings.worker_name,
                request_id,
            )
            self._retry_or_dead_letter(channel, method, properties, body, error)

    def _copy_properties(
        self, properties, headers: dict[str, Any]
    ) -> pika.BasicProperties:
        return persistent_properties(
            correlation_id=properties.correlation_id
            or properties.message_id
            or "desconhecido",
            message_id=properties.message_id
            or properties.correlation_id
            or "desconhecido",
            headers=headers,
        )

    def _dead_letter_or_requeue(
        self, channel, method, properties, body: bytes, *, error: str
    ) -> None:
        headers = dict(properties.headers or {})
        headers.update(
            {
                "x-error": error[:500],
                "x-original-routing-key": method.routing_key,
                "x-worker": self.settings.worker_name,
            }
        )
        try:
            publish_bytes(
                channel,
                exchange=DEAD_EXCHANGE,
                routing_key=method.routing_key,
                body=body,
                properties=self._copy_properties(properties, headers),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            LOGGER.exception("não foi possível publicar na DLQ; requeue")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _retry_or_dead_letter(
        self, channel, method, properties, body: bytes, error: Exception
    ) -> None:
        headers = dict(properties.headers or {})
        retry_count = int(headers.get("x-retry-count", 0)) + 1
        headers.update(
            {
                "x-retry-count": retry_count,
                "x-last-error": str(error)[:500],
                "x-original-routing-key": method.routing_key,
                "x-worker": self.settings.worker_name,
            }
        )
        if retry_count > self.settings.max_retries:
            self._dead_letter_or_requeue(
                channel,
                method,
                self._copy_properties(properties, headers),
                body,
                error=f"limite de retry excedido: {error}",
            )
            return

        try:
            publish_bytes(
                channel,
                exchange=RETRY_EXCHANGE,
                routing_key=method.routing_key,
                body=body,
                properties=self._copy_properties(properties, headers),
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            LOGGER.warning(
                "retry agendado worker=%s tentativa=%s routing_key=%s",
                self.settings.worker_name,
                retry_count,
                method.routing_key,
            )
        except Exception:
            LOGGER.exception("não foi possível publicar retry; requeue imediato")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    worker = Worker(Settings.from_env())
    try:
        worker.run_forever()
    finally:
        worker.close()


if __name__ == "__main__":
    main()
