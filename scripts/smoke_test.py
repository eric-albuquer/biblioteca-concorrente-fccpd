"""Fluxo ponta a ponta: empréstimo, devolução e devolução duplicada."""

from __future__ import annotations

import json
from uuid import uuid4

from biblioteca_distribuida.client import LibraryClient


BOOK_ID = "22222222-2222-2222-2222-222222222222"
CATEGORY = "romance"


def main() -> None:
    client = LibraryClient(f"smoke-{uuid4().hex[:8]}")
    try:
        borrow_request = client.borrow(BOOK_ID, CATEGORY)
        borrowed = client.wait_for(borrow_request, timeout=60)
        print(json.dumps(borrowed, ensure_ascii=False, indent=2))
        if borrowed["status"] != "EMPRESTADO":
            raise RuntimeError(f"Empréstimo inesperado: {borrowed}")

        loan_id = borrowed["data"]["loan_id"]
        return_request = client.return_loan(loan_id, CATEGORY)
        returned = client.wait_for(return_request, timeout=60)
        print(json.dumps(returned, ensure_ascii=False, indent=2))
        if returned["status"] != "DEVOLVIDO":
            raise RuntimeError(f"Devolução inesperada: {returned}")

        duplicate_request = client.return_loan(loan_id, CATEGORY)
        duplicate = client.wait_for(duplicate_request, timeout=60)
        print(json.dumps(duplicate, ensure_ascii=False, indent=2))
        if duplicate["status"] != "ERRO":
            raise RuntimeError("Devolução duplicada deveria ser rejeitada.")
        print("SMOKE TEST: OK")
    finally:
        client.close()


if __name__ == "__main__":
    main()
