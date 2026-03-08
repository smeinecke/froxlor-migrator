from __future__ import annotations

import unittest

from froxlor_migrator.migrate import Migrator


class MailboxScopeTests(unittest.TestCase):
    def test_ensure_mailboxes_lists_existing_by_target_customer(self) -> None:
        class TargetStub:
            def __init__(self) -> None:
                self.customer_ids: list[int | None] = []

            def list_emails(self, customerid=None, loginname=None):  # noqa: ANN001, ARG002
                self.customer_ids.append(customerid)
                return []

        target = TargetStub()
        migrator = object.__new__(Migrator)
        migrator.target = target  # type: ignore[assignment]

        migrated = migrator._ensure_mailboxes(77, [])

        self.assertEqual([], migrated)
        self.assertEqual([77], target.customer_ids)


if __name__ == "__main__":
    unittest.main()
