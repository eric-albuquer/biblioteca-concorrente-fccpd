from __future__ import annotations

import os
import threading
import unittest
from uuid import uuid4

import psycopg

from biblioteca_distribuida.messages import Action, Command, utc_now_iso
from biblioteca_distribuida.migrate import run_migrations
from biblioteca_distribuida.service import LoanService


DATABASE_URL = os.getenv("TEST_DATABASE_URL")
DUNA_ID = "11111111-1111-1111-1111-111111111111"


@unittest.skipUnless(DATABASE_URL, "TEST_DATABASE_URL não configurada")
class LoanServiceIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        run_migrations(DATABASE_URL)

    def setUp(self):
        with psycopg.connect(DATABASE_URL) as connection:
            connection.execute("TRUNCATE waitlist, loans, processed_requests RESTART IDENTITY")
            connection.execute(
                "UPDATE books SET total_copies = 2, available_copies = 2"
            )
        self.service = LoanService(DATABASE_URL)

    def tearDown(self):
        self.service.close()

    def command(
        self,
        client_id: str,
        action: Action,
        *,
        request_id: str | None = None,
        book_id: str | None = None,
        loan_id: str | None = None,
    ) -> Command:
        return Command(
            request_id=request_id or str(uuid4()),
            client_id=client_id,
            action=action,
            category="ficcao-cientifica",
            reply_routing_key=client_id,
            sent_at=utc_now_iso(),
            book_id=book_id,
            loan_id=loan_id,
        )

    def test_idempotency_and_possession_protect_inventory(self):
        request_id = str(uuid4())
        borrow = self.command(
            "cliente-a", Action.BORROW, request_id=request_id, book_id=DUNA_ID
        )
        first = self.service.process(borrow)
        repeated = self.service.process(borrow)
        self.assertEqual(first[0].payload["event_id"], repeated[0].payload["event_id"])
        loan_id = first[0].payload["data"]["loan_id"]

        wrong_owner = self.service.process(
            self.command("cliente-b", Action.RETURN, loan_id=loan_id)
        )
        self.assertEqual("ERRO", wrong_owner[0].payload["status"])

        returned = self.service.process(
            self.command("cliente-a", Action.RETURN, loan_id=loan_id)
        )
        self.assertEqual("DEVOLVIDO", returned[0].payload["status"])

        duplicate = self.service.process(
            self.command("cliente-a", Action.RETURN, loan_id=loan_id)
        )
        self.assertEqual("ERRO", duplicate[0].payload["status"])
        with psycopg.connect(DATABASE_URL) as connection:
            available, total = connection.execute(
                "SELECT available_copies, total_copies FROM books WHERE id = %s",
                (DUNA_ID,),
            ).fetchone()
        self.assertEqual(total, available)

    def test_waitlist_is_fifo_and_persisted(self):
        first = self.service.process(
            self.command("cliente-a", Action.BORROW, book_id=DUNA_ID)
        )
        second = self.service.process(
            self.command("cliente-b", Action.BORROW, book_id=DUNA_ID)
        )
        waiting = self.service.process(
            self.command("cliente-c", Action.BORROW, book_id=DUNA_ID)
        )
        self.assertEqual("NA_FILA", waiting[0].payload["status"])

        result = self.service.process(
            self.command(
                "cliente-a",
                Action.RETURN,
                loan_id=first[0].payload["data"]["loan_id"],
            )
        )
        self.assertEqual(["DEVOLVIDO", "EMPRESTIMO_LIBERADO"], [
            item.payload["status"] for item in result
        ])
        self.assertEqual("cliente-c", result[1].routing_key)

        self.service.process(
            self.command(
                "cliente-b",
                Action.RETURN,
                loan_id=second[0].payload["data"]["loan_id"],
            )
        )
        self.service.process(
            self.command(
                "cliente-c",
                Action.RETURN,
                loan_id=result[1].payload["data"]["loan_id"],
            )
        )

    def test_simultaneous_borrow_serializes_the_last_copy(self):
        with psycopg.connect(DATABASE_URL) as connection:
            connection.execute(
                """
                UPDATE books
                SET total_copies = 1, available_copies = 1
                WHERE id = %s
                """,
                (DUNA_ID,),
            )

        barrier = threading.Barrier(2)
        results = []
        errors = []

        def borrow(client_id: str) -> None:
            service = LoanService(DATABASE_URL)
            try:
                command = self.command(client_id, Action.BORROW, book_id=DUNA_ID)
                barrier.wait(timeout=5)
                results.append(service.process(command)[0].payload["status"])
            except Exception as error:  # pragma: no cover - diagnostic path
                errors.append(error)
            finally:
                service.close()

        threads = [
            threading.Thread(target=borrow, args=("cliente-a",)),
            threading.Thread(target=borrow, args=("cliente-b",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertFalse(errors)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertCountEqual(["EMPRESTADO", "NA_FILA"], results)
        with psycopg.connect(DATABASE_URL) as connection:
            available, active, waiting = connection.execute(
                """
                SELECT
                    b.available_copies,
                    COUNT(DISTINCT l.id) FILTER (WHERE l.status = 'ACTIVE'),
                    COUNT(DISTINCT w.id) FILTER (WHERE w.status = 'WAITING')
                FROM books b
                LEFT JOIN loans l ON l.book_id = b.id
                LEFT JOIN waitlist w ON w.book_id = b.id
                WHERE b.id = %s
                GROUP BY b.id
                """,
                (DUNA_ID,),
            ).fetchone()
        self.assertEqual((0, 1, 1), (available, active, waiting))


if __name__ == "__main__":
    unittest.main()
