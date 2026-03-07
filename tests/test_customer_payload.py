from __future__ import annotations

import unittest

from froxlor_migrator.migrate import Migrator


class CustomerPayloadTests(unittest.TestCase):
    def test_allowed_id_lists_support_json_and_scalar(self) -> None:
        migrator = object.__new__(Migrator)
        payload = migrator._customer_payload({
            "email": "user@example.test",
            "allowed_phpconfigs": "[2, 5]",
            "allowed_mysqlserver": "3",
        })

        self.assertEqual([2, 5], payload["allowed_phpconfigs"])
        self.assertEqual([3], payload["allowed_mysqlserver"])

    def test_allowed_id_lists_fall_back_for_empty_values(self) -> None:
        migrator = object.__new__(Migrator)
        payload = migrator._customer_payload({"email": "user@example.test", "allowed_phpconfigs": ""})

        self.assertEqual([1], payload["allowed_phpconfigs"])
        self.assertEqual([0], payload["allowed_mysqlserver"])


if __name__ == "__main__":
    unittest.main()
