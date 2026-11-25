"""Regras transacionais do serviço de empréstimos."""

from __future__ import annotations

import logging
from typing import Any
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .messages import (
    Action,
    Command,
    OutgoingMessage,
    ResponseStatus,
    response_message,
)


LOGGER = logging.getLogger("biblioteca.service")


class PermanentProcessingError(RuntimeError):
    """Erro permanente que deve ir para DLQ."""


class DomainError(RuntimeError):
    """Regra de negócio inválida, devolvida ao cliente sem retry."""


class LoanService:
    def __init__(self, database_url: str, *, min_pool_size: int = 1, max_pool_size: int = 5):
        self.pool = ConnectionPool(
            conninfo=database_url,
            min_size=min_pool_size,
            max_size=max_pool_size,
            kwargs={"row_factory": dict_row},
            open=True,
        )

    def close(self) -> None:
        self.pool.close()

    def process(self, command: Command) -> list[OutgoingMessage]:
        with self.pool.connection() as connection:
            with connection.transaction():
                claimed = connection.execute(
                    """
                    INSERT INTO processed_requests (
                        request_id, client_id, action, outbound_messages
                    ) VALUES (%s, %s, %s, NULL)
                    ON CONFLICT (request_id) DO NOTHING
                    RETURNING request_id
                    """,
                    (command.request_id, command.client_id, command.action.value),
                ).fetchone()

                if claimed is None:
                    previous = connection.execute(
                        """
                        SELECT client_id, action, outbound_messages
                        FROM processed_requests
                        WHERE request_id = %s
                        """,
                        (command.request_id,),
                    ).fetchone()
                    if previous is None or previous["outbound_messages"] is None:
                        raise RuntimeError("Requisição idempotente ainda sem resultado.")
                    if (
                        previous["client_id"] != command.client_id
                        or previous["action"] != command.action.value
                    ):
                        raise PermanentProcessingError(
                            "request_id reutilizado com cliente ou ação diferente."
                        )
                    return [
                        OutgoingMessage.from_dict(item)
                        for item in previous["outbound_messages"]
                    ]

                try:
                    if command.action is Action.BORROW:
                        outgoing = self._borrow(connection, command)
                    else:
                        outgoing = self._return(connection, command)
                except DomainError as error:
                    outgoing = [
                        response_message(
                            command, ResponseStatus.ERROR, error=str(error)
                        )
                    ]

                serialized = [message.to_dict() for message in outgoing]
                connection.execute(
                    """
                    UPDATE processed_requests
                    SET outbound_messages = %s::jsonb, processed_at = NOW()
                    WHERE request_id = %s
                    """,
                    (Jsonb(serialized), command.request_id),
                )
                return outgoing

    @staticmethod
    def _locked_book(connection, book_id: str | None) -> dict[str, Any]:
        if book_id is None:
            raise DomainError("Livro não informado.")
        book = connection.execute(
            """
            SELECT id, title, author, category, total_copies, available_copies
            FROM books
            WHERE id = %s
            FOR UPDATE
            """,
            (book_id,),
        ).fetchone()
        if book is None:
            raise DomainError("Livro não encontrado.")
        return book

    @staticmethod
    def _validate_category(book: dict[str, Any], command: Command) -> None:
        if book["category"] != command.category:
            raise DomainError("Categoria informada não corresponde ao livro.")

    def _borrow(self, connection, command: Command) -> list[OutgoingMessage]:
        book = self._locked_book(connection, command.book_id)
        self._validate_category(book, command)

        active = connection.execute(
            """
            SELECT id FROM loans
            WHERE client_id = %s AND book_id = %s AND status = 'ACTIVE'
            """,
            (command.client_id, book["id"]),
        ).fetchone()
        if active is not None:
            raise DomainError(
                f"Cliente já possui empréstimo ativo deste livro: {active['id']}."
            )

        waiting = connection.execute(
            """
            SELECT id FROM waitlist
            WHERE client_id = %s AND book_id = %s AND status = 'WAITING'
            """,
            (command.client_id, book["id"]),
        ).fetchone()
        if waiting is not None:
            raise DomainError(
                f"Cliente já está na fila deste livro: posição registrada {waiting['id']}."
            )

        if book["available_copies"] > 0:
            loan_id = str(uuid4())
            connection.execute(
                """
                UPDATE books
                SET available_copies = available_copies - 1
                WHERE id = %s
                """,
                (book["id"],),
            )
            connection.execute(
                """
                INSERT INTO loans (
                    id, client_id, book_id, status, borrow_request_id
                ) VALUES (%s, %s, %s, 'ACTIVE', %s)
                """,
                (loan_id, command.client_id, book["id"], command.request_id),
            )
            return [
                response_message(
                    command,
                    ResponseStatus.BORROWED,
                    data={
                        "loan_id": loan_id,
                        "book_id": str(book["id"]),
                        "title": book["title"],
                        "author": book["author"],
                        "category": book["category"],
                    },
                )
            ]

        wait = connection.execute(
            """
            INSERT INTO waitlist (
                request_id, client_id, reply_routing_key, book_id, status
            ) VALUES (%s, %s, %s, %s, 'WAITING')
            RETURNING id
            """,
            (
                command.request_id,
                command.client_id,
                command.reply_routing_key,
                book["id"],
            ),
        ).fetchone()
        position = connection.execute(
            """
            SELECT COUNT(*) AS position
            FROM waitlist
            WHERE book_id = %s AND status = 'WAITING' AND id <= %s
            """,
            (book["id"], wait["id"]),
        ).fetchone()["position"]
        return [
            response_message(
                command,
                ResponseStatus.WAITING,
                data={
                    "waitlist_id": wait["id"],
                    "position": position,
                    "book_id": str(book["id"]),
                    "title": book["title"],
                    "category": book["category"],
                },
            )
        ]

    def _return(self, connection, command: Command) -> list[OutgoingMessage]:
        loan_reference = connection.execute(
            "SELECT book_id FROM loans WHERE id = %s",
            (command.loan_id,),
        ).fetchone()
        if loan_reference is None:
            raise DomainError("Empréstimo não encontrado.")

        book = self._locked_book(connection, str(loan_reference["book_id"]))
        self._validate_category(book, command)
        loan = connection.execute(
            """
            SELECT id, client_id, status
            FROM loans
            WHERE id = %s
            FOR UPDATE
            """,
            (command.loan_id,),
        ).fetchone()
        if loan["client_id"] != command.client_id:
            raise DomainError("Cliente não possui este empréstimo.")
        if loan["status"] != "ACTIVE":
            raise DomainError("Empréstimo já foi devolvido.")

        connection.execute(
            """
            UPDATE loans
            SET status = 'RETURNED', returned_at = NOW(), return_request_id = %s
            WHERE id = %s
            """,
            (command.request_id, command.loan_id),
        )

        outgoing = [
            response_message(
                command,
                ResponseStatus.RETURNED,
                data={
                    "loan_id": command.loan_id,
                    "book_id": str(book["id"]),
                    "title": book["title"],
                    "category": book["category"],
                },
            )
        ]

        waiter = connection.execute(
            """
            SELECT id, request_id, client_id, reply_routing_key
            FROM waitlist
            WHERE book_id = %s AND status = 'WAITING'
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """,
            (book["id"],),
        ).fetchone()

        if waiter is None:
            connection.execute(
                """
                UPDATE books
                SET available_copies = available_copies + 1
                WHERE id = %s
                """,
                (book["id"],),
            )
            return outgoing

        granted_loan_id = str(uuid4())
        connection.execute(
            """
            INSERT INTO loans (
                id, client_id, book_id, status, borrow_request_id
            ) VALUES (%s, %s, %s, 'ACTIVE', %s)
            """,
            (
                granted_loan_id,
                waiter["client_id"],
                book["id"],
                waiter["request_id"],
            ),
        )
        connection.execute(
            """
            UPDATE waitlist
            SET status = 'GRANTED', granted_at = NOW(), granted_loan_id = %s
            WHERE id = %s
            """,
            (granted_loan_id, waiter["id"]),
        )

        wait_command = Command(
            request_id=str(waiter["request_id"]),
            client_id=waiter["client_id"],
            action=Action.BORROW,
            category=book["category"],
            reply_routing_key=waiter["reply_routing_key"],
            sent_at="persisted-waitlist",
            book_id=str(book["id"]),
        )
        granted_message = response_message(
            wait_command,
            ResponseStatus.LOAN_GRANTED,
            data={
                "loan_id": granted_loan_id,
                "book_id": str(book["id"]),
                "title": book["title"],
                "author": book["author"],
                "category": book["category"],
            },
        )
        outgoing.append(granted_message)
        connection.execute(
            """
            UPDATE processed_requests
            SET outbound_messages = %s::jsonb, processed_at = NOW()
            WHERE request_id = %s
            """,
            (Jsonb([granted_message.to_dict()]), waiter["request_id"]),
        )
        return outgoing
