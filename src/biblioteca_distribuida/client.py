"""Cliente produtor de comandos e consumidor de respostas persistentes."""

from __future__ import annotations

import argparse
import json
import time
from collections import deque
from typing import Any
from uuid import uuid4

import pika

from .config import CATEGORIES, Settings
from .messages import Action, Command, utc_now_iso
from .messaging import (
    COMMAND_EXCHANGE,
    connect_with_retry,
    declare_response_queue,
    declare_topology,
    publish_json,
)


CATALOG = {
    "11111111-1111-1111-1111-111111111111": ("Duna", "ficcao-cientifica"),
    "22222222-2222-2222-2222-222222222222": (
        "Orgulho e Preconceito",
        "romance",
    ),
    "33333333-3333-3333-3333-333333333333": (
        "A Ilha do Tesouro",
        "aventura",
    ),
    "44444444-4444-4444-4444-444444444444": ("Jogos Vorazes", "acao"),
    "55555555-5555-5555-5555-555555555555": (
        "Garota Exemplar",
        "suspense",
    ),
    "66666666-6666-6666-6666-666666666666": ("It", "terror"),
}


class LibraryClient:
    def __init__(self, client_id: str, settings: Settings | None = None):
        if not client_id.strip():
            raise ValueError("client_id não pode ser vazio.")
        self.client_id = client_id.strip()
        self.settings = settings or Settings.from_env()
        self.connection = connect_with_retry(self.settings.rabbitmq_url)
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()
        declare_topology(self.channel, self.settings)
        self.queue = declare_response_queue(
            self.channel, self.settings, self.client_id
        )
        self._events: deque[dict[str, Any]] = deque()
        self._seen_event_ids: set[str] = set()
        self.channel.basic_consume(
            queue=self.queue,
            on_message_callback=self._on_response,
            auto_ack=False,
        )

    def close(self) -> None:
        if self.connection.is_open:
            self.connection.close()

    def _on_response(self, channel, method, properties, body: bytes) -> None:
        try:
            event = json.loads(body)
            event_id = event.get("event_id")
            if event_id and event_id not in self._seen_event_ids:
                self._seen_event_ids.add(event_id)
                self._events.append(event)
        finally:
            channel.basic_ack(delivery_tag=method.delivery_tag)

    def _publish(
        self,
        command: Command,
        *,
        headers: dict[str, Any] | None = None,
    ) -> str:
        publish_json(
            self.channel,
            exchange=COMMAND_EXCHANGE,
            routing_key=command.routing_key(),
            payload=command.to_dict(),
            correlation_id=command.request_id,
            message_id=command.request_id,
            headers=headers,
        )
        return command.request_id

    def borrow(
        self,
        book_id: str,
        category: str,
        *,
        request_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> str:
        request_id = request_id or str(uuid4())
        return self._publish(
            Command(
                request_id=request_id,
                client_id=self.client_id,
                action=Action.BORROW,
                category=category,
                reply_routing_key=self.client_id,
                sent_at=utc_now_iso(),
                book_id=book_id,
            ),
            headers=headers,
        )

    def return_loan(
        self,
        loan_id: str,
        category: str,
        *,
        request_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> str:
        request_id = request_id or str(uuid4())
        return self._publish(
            Command(
                request_id=request_id,
                client_id=self.client_id,
                action=Action.RETURN,
                category=category,
                reply_routing_key=self.client_id,
                sent_at=utc_now_iso(),
                loan_id=loan_id,
            ),
            headers=headers,
        )

    def receive(self, timeout: float = 30.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._events:
                return self._events.popleft()
            self.connection.process_data_events(time_limit=0.25)
        raise TimeoutError("Nenhuma resposta recebida no tempo limite.")

    def wait_for(self, request_id: str, timeout: float = 30.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        parked: list[dict[str, Any]] = []
        try:
            while time.monotonic() < deadline:
                event = self.receive(timeout=min(1.0, deadline - time.monotonic()))
                if event.get("request_id") == request_id:
                    return event
                parked.append(event)
        except TimeoutError:
            pass
        finally:
            self._events.extendleft(reversed(parked))
        raise TimeoutError(f"Resposta de {request_id} não chegou no tempo limite.")


def _print_catalog() -> None:
    for book_id, (title, category) in CATALOG.items():
        print(f"{book_id}  {category:20}  {title}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Cliente da biblioteca distribuída")
    parser.add_argument("--client-id", default=f"cliente-{uuid4().hex[:8]}")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("catalog")
    borrow = subparsers.add_parser("borrow")
    borrow.add_argument("book_id")
    borrow.add_argument("category", choices=CATEGORIES)
    return_parser = subparsers.add_parser("return")
    return_parser.add_argument("loan_id")
    return_parser.add_argument("category", choices=CATEGORIES)
    listen = subparsers.add_parser("listen")
    listen.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()

    if args.command == "catalog":
        _print_catalog()
        return

    client = LibraryClient(args.client_id)
    try:
        if args.command == "borrow":
            request_id = client.borrow(args.book_id, args.category)
            print(json.dumps(client.wait_for(request_id), ensure_ascii=False, indent=2))
        elif args.command == "return":
            request_id = client.return_loan(args.loan_id, args.category)
            print(json.dumps(client.wait_for(request_id), ensure_ascii=False, indent=2))
        else:
            print(json.dumps(client.receive(args.timeout), ensure_ascii=False, indent=2))
    finally:
        client.close()


if __name__ == "__main__":
    main()
