import json
import unittest
from uuid import uuid4

from biblioteca_distribuida.messages import (
    Action,
    Command,
    MessageValidationError,
    ResponseStatus,
    response_message,
)


class CommandTests(unittest.TestCase):
    def valid_borrow(self):
        return {
            "schema_version": 1,
            "request_id": str(uuid4()),
            "client_id": "cliente-a",
            "action": "PEDIR",
            "category": "ficcao-cientifica",
            "reply_routing_key": "cliente-a",
            "sent_at": "2026-11-25T12:00:00+00:00",
            "book_id": str(uuid4()),
            "loan_id": None,
        }

    def test_borrow_validates_and_builds_routing_key(self):
        command = Command.from_json(json.dumps(self.valid_borrow()))
        self.assertEqual(Action.BORROW, command.action)
        self.assertEqual("ficcao-cientifica.pedir", command.routing_key())

    def test_unknown_category_is_rejected(self):
        data = self.valid_borrow()
        data["category"] = "categoria-infinita"
        with self.assertRaisesRegex(MessageValidationError, "Categoria inválida"):
            Command.from_dict(data)

    def test_return_requires_loan_id(self):
        data = self.valid_borrow()
        data.update({"action": "DEVOLVER", "book_id": None, "loan_id": None})
        with self.assertRaisesRegex(MessageValidationError, "loan_id"):
            Command.from_dict(data)

    def test_event_id_is_deterministic_for_redelivery(self):
        command = Command.from_dict(self.valid_borrow())
        first = response_message(command, ResponseStatus.BORROWED)
        second = response_message(command, ResponseStatus.BORROWED)
        self.assertEqual(
            first.payload["event_id"], second.payload["event_id"]
        )


if __name__ == "__main__":
    unittest.main()
