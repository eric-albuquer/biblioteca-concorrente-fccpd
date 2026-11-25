"""Injeta queda após commit e comprova redelivery/idempotência."""

from __future__ import annotations

import json
from uuid import uuid4

from biblioteca_distribuida.client import LibraryClient


BOOK_ID = "33333333-3333-3333-3333-333333333333"
CATEGORY = "aventura"


def main() -> None:
    client = LibraryClient(f"failure-{uuid4().hex[:8]}")
    try:
        request_id = client.borrow(
            BOOK_ID,
            CATEGORY,
            headers={"x-test-crash-after-commit": True},
        )
        response = client.wait_for(request_id, timeout=90)
        print(json.dumps(response, ensure_ascii=False, indent=2))
        if response["status"] != "EMPRESTADO":
            raise RuntimeError(f"Recuperação inesperada: {response}")

        return_request = client.return_loan(response["data"]["loan_id"], CATEGORY)
        returned = client.wait_for(return_request, timeout=60)
        if returned["status"] != "DEVOLVIDO":
            raise RuntimeError(f"Limpeza inesperada: {returned}")
        print("FAILURE DEMO: OK — pedido recuperado sem duplicar o empréstimo")
    finally:
        client.close()


if __name__ == "__main__":
    main()
