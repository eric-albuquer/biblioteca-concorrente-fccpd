"""Contrato JSON versionado usado entre clientes e workers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, NAMESPACE_URL, uuid5

from .config import CATEGORIES


SCHEMA_VERSION = 1


class MessageValidationError(ValueError):
    """Mensagem não segue o contrato público."""


class Action(StrEnum):
    BORROW = "PEDIR"
    RETURN = "DEVOLVER"


class ResponseStatus(StrEnum):
    BORROWED = "EMPRESTADO"
    WAITING = "NA_FILA"
    RETURNED = "DEVOLVIDO"
    LOAN_GRANTED = "EMPRESTIMO_LIBERADO"
    ERROR = "ERRO"


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _required_text(data: dict[str, Any], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value.strip():
        raise MessageValidationError(f"Campo '{field}' deve ser texto não vazio.")
    return value.strip()


def _optional_uuid(data: dict[str, Any], field: str) -> str | None:
    value = data.get(field)
    if value is None:
        return None
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError) as error:
        raise MessageValidationError(f"Campo '{field}' deve ser UUID válido.") from error


@dataclass(frozen=True)
class Command:
    request_id: str
    client_id: str
    action: Action
    category: str
    reply_routing_key: str
    sent_at: str
    book_id: str | None = None
    loan_id: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Command":
        if data.get("schema_version") != SCHEMA_VERSION:
            raise MessageValidationError(
                f"schema_version deve ser {SCHEMA_VERSION}."
            )

        request_id = _optional_uuid(data, "request_id")
        if request_id is None:
            raise MessageValidationError("Campo 'request_id' é obrigatório.")

        try:
            action = Action(_required_text(data, "action"))
        except ValueError as error:
            raise MessageValidationError("Ação deve ser PEDIR ou DEVOLVER.") from error

        category = _required_text(data, "category")
        if category not in CATEGORIES:
            raise MessageValidationError(
                f"Categoria inválida. Use uma de: {', '.join(CATEGORIES)}."
            )

        book_id = _optional_uuid(data, "book_id")
        loan_id = _optional_uuid(data, "loan_id")
        if action is Action.BORROW and book_id is None:
            raise MessageValidationError("PEDIR exige 'book_id'.")
        if action is Action.RETURN and loan_id is None:
            raise MessageValidationError("DEVOLVER exige 'loan_id'.")

        return cls(
            request_id=request_id,
            client_id=_required_text(data, "client_id"),
            action=action,
            category=category,
            reply_routing_key=_required_text(data, "reply_routing_key"),
            sent_at=_required_text(data, "sent_at"),
            book_id=book_id,
            loan_id=loan_id,
        )

    @classmethod
    def from_json(cls, body: bytes | str) -> "Command":
        try:
            data = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as error:
            raise MessageValidationError("Corpo deve ser JSON UTF-8 válido.") from error
        if not isinstance(data, dict):
            raise MessageValidationError("Corpo JSON deve ser um objeto.")
        return cls.from_dict(data)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "request_id": self.request_id,
            "client_id": self.client_id,
            "action": self.action.value,
            "category": self.category,
            "reply_routing_key": self.reply_routing_key,
            "sent_at": self.sent_at,
            "book_id": self.book_id,
            "loan_id": self.loan_id,
        }

    def routing_key(self) -> str:
        return f"{self.category}.{self.action.value.lower()}"


@dataclass(frozen=True)
class OutgoingMessage:
    routing_key: str
    payload: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OutgoingMessage":
        return cls(routing_key=data["routing_key"], payload=data["payload"])

    def to_dict(self) -> dict[str, Any]:
        return {"routing_key": self.routing_key, "payload": self.payload}


def response_message(
    command: Command,
    status: ResponseStatus,
    *,
    data: dict[str, Any] | None = None,
    error: str | None = None,
    routing_key: str | None = None,
    event_request_id: str | None = None,
) -> OutgoingMessage:
    request_id = event_request_id or command.request_id
    target = routing_key or command.reply_routing_key
    event_id = str(uuid5(NAMESPACE_URL, f"{request_id}:{status.value}:{target}"))
    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "request_id": request_id,
        "client_id": target,
        "status": status.value,
        "data": data or {},
        "error": error,
        "occurred_at": utc_now_iso(),
    }
    return OutgoingMessage(routing_key=target, payload=payload)
