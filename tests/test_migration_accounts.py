from __future__ import annotations

import unittest
from types import SimpleNamespace

from froxlor_migrator.migration.accounts import MigratorAccountOps
from froxlor_migrator.migration.types import MigrationError


class StubTarget:
    def __init__(self) -> None:
        self._email_forwarders: list[dict[str, object]] = []
        self._email_senders: list[dict[str, object]] = []
        self._ftps: list[dict[str, object]] = []
        self._ssh_keys: list[dict[str, object]] = []
        self._data_dumps: list[dict[str, object]] = []
        self.calls: list[tuple[str, dict[str, object]]] = []

    def list_email_forwarders(self, customerid: int) -> list[dict[str, object]]:
        return self._email_forwarders

    def list_email_senders(self, customerid: int) -> list[dict[str, object]]:
        return self._email_senders

    def list_ftps(self, customerid: int) -> list[dict[str, object]]:
        return self._ftps

    def list_ssh_keys(self, customerid: int) -> list[dict[str, object]]:
        return self._ssh_keys

    def list_data_dumps(self, customerid: int) -> list[dict[str, object]]:
        return self._data_dumps

    def call(self, command: str, payload: dict[str, object]) -> None:
        self.calls.append((command, payload))


class StubOps(MigratorAccountOps):
    def __init__(self, target: StubTarget) -> None:
        self.target = target


class MigratorAccountOpsTests(unittest.TestCase):
    def test_ensure_email_forwarders_adds_missing(self) -> None:
        target = StubTarget()
        target._email_forwarders = [{"email": "a@x", "destination": "b@x"}]
        ops = StubOps(target)

        ops._ensure_email_forwarders(1, [{"email": "a@x", "destination": "b@x"}, {"email": "c@x", "destination": "d@x"}])
        self.assertIn(("EmailForwarders.add", {"emailaddr": "c@x", "destination": "d@x", "customerid": 1}), target.calls)

    def test_ensure_email_sender_aliases_adds_missing(self) -> None:
        target = StubTarget()
        target._email_senders = [{"email": "a@x", "allowed_sender": "z"}]
        ops = StubOps(target)

        ops._ensure_email_sender_aliases(1, [{"email": "a@x", "allowed_sender": "z"}, {"email": "b@x", "allowed_sender": "y"}])
        self.assertIn(("EmailSender.add", {"emailaddr": "b@x", "allowed_sender": "y", "customerid": 1}), target.calls)

    def test_ensure_ftp_accounts_updates_and_adds(self) -> None:
        target = StubTarget()
        target._ftps = [{"id": 5, "username": "u", "path": "old", "homedir": "/customer/u"}]
        ops = StubOps(target)

        ops._ensure_ftp_accounts(1, [{"username": "u", "path": "new"}, {"username": "v@d", "homedir": "/customer/v"}], "customer")
        # Expect an update call and an add call
        commands = [c for c, _ in target.calls]
        self.assertIn("Ftps.update", commands)
        self.assertIn("Ftps.add", commands)

    def test_ensure_ssh_keys_raises_when_user_missing(self) -> None:
        target = StubTarget()
        target._ftps = []
        ops = StubOps(target)
        with self.assertRaises(MigrationError):
            ops._ensure_ssh_keys(1, [{"username": "u", "ssh_pubkey": "key"}])

    def test_ensure_data_dumps_handles_405_gracefully(self) -> None:
        class FailingTarget(StubTarget):
            def call(self, command: str, payload: dict[str, object]) -> None:
                raise Exception("HTTP 405")

        target = FailingTarget()
        ops = StubOps(target)
        # Should not raise
        ops._ensure_data_dumps(1, [{"path": "/p"}])

    def test_ensure_dir_options_updates_and_adds(self) -> None:
        target = StubTarget()
        target._dir_options = [{"id": 1, "path": "/a"}]

        def list_dir_options(customerid: int):
            return target._dir_options

        target.list_dir_options = list_dir_options
        ops = StubOps(target)
        ops._relative_customer_path = lambda path, login: path
        ops._ensure_dir_options(1, [{"path": "/a", "options_indexes": 1}, {"path": "/b", "options_cgi": 1}], "customer")

        commands = [c for c, _ in target.calls]
        self.assertIn("DirOptions.update", commands)
        self.assertIn("DirOptions.add", commands)

    def test_ensure_dir_protections_updates_and_adds(self) -> None:
        target = StubTarget()
        target._dir_protections = [{"id": 1, "path": "/a", "username": "u"}]

        def list_dir_protections(customerid: int):
            return target._dir_protections

        target.list_dir_protections = list_dir_protections
        ops = StubOps(target)
        ops._relative_customer_path = lambda path, login: path
        ops._ensure_dir_protections(1, [{"path": "/a", "username": "u"}, {"path": "/b", "username": "v"}], "customer")

        commands = [c for c, _ in target.calls]
        self.assertIn("DirProtections.update", commands)
        self.assertIn("DirProtections.add", commands)

    def test_ensure_mailboxes_adds_and_verifies(self) -> None:
        target = StubTarget()
        target._emails = []

        def list_emails(customerid: int):
            return target._emails

        def call(command: str, payload: dict[str, object]) -> None:
            target.calls.append((command, payload))
            if command == "Emails.add":
                target._emails.append({"email": payload["email_part"] + "@" + payload["domain"]})

        target.list_emails = list_emails
        target.call = call

        ops = StubOps(target)
        ops.config = SimpleNamespace(behavior=SimpleNamespace(mailbox_exists="update"))
        target._emails = [
            {"email": "a@x", "spam_tag_level": 7, "rewrite_subject": 1, "spam_kill_level": 14, "bypass_spam": 0, "policy_greylist": 1, "iscatchall": 0}
        ]
        ops._mailbox_address = lambda mailbox: mailbox.get("email")

        mailboxes = [
            {"email": "a@x", "spam_tag_level": 7, "rewrite_subject": 1, "spam_kill_level": 14, "bypass_spam": 0, "policy_greylist": 1, "iscatchall": 0}
        ]
        transferable = ops._ensure_mailboxes(1, mailboxes)
        self.assertEqual(["a@x"], transferable)

    def test_ensure_dir_protections_rebuild_uses_target_login(self) -> None:
        # Regression: after each add/update the dedup map is rebuilt; it must
        # normalize target paths with the *target* login, otherwise renamed
        # target customers get duplicate DirProtections.add calls.
        from froxlor_migrator.migration.core import MigratorCore

        target = StubTarget()
        target._dir_protections = [
            {"id": 1, "path": "/var/www/dstuser/web/secret", "username": "bob"},
            {"id": 2, "path": "/var/www/dstuser/web/secret2", "username": "alice"},
        ]
        target.list_dir_protections = lambda customerid: target._dir_protections

        ops = StubOps(target)
        ops._relative_customer_path = lambda path, login: MigratorCore._relative_customer_path(ops, path, login)

        ops._ensure_dir_protections(
            1,
            [
                {"path": "/var/www/srcuser/web/secret", "username": "bob"},
                {"path": "/var/www/srcuser/web/secret2", "username": "alice"},
            ],
            "srcuser",
            "dstuser",
        )

        adds = [c for c, _ in target.calls if c == "DirProtections.add"]
        updates = [c for c, _ in target.calls if c == "DirProtections.update"]
        self.assertEqual([], adds)
        self.assertEqual(2, len(updates))

    def test_ensure_mailboxes_adds_missing_account_on_update(self) -> None:
        # The source mailbox has a mail account; the existing target mailbox
        # object does not. EmailAccounts.update would fail, so a missing
        # account must be created instead.
        target = StubTarget()
        target._emails = [
            {
                "email": "a@x",
                "popaccountid": 0,
                "spam_tag_level": 7,
                "rewrite_subject": 1,
                "spam_kill_level": 14,
                "bypass_spam": 0,
                "policy_greylist": 1,
                "iscatchall": 0,
            }
        ]
        target.list_emails = lambda customerid: target._emails

        ops = StubOps(target)
        ops.config = SimpleNamespace(behavior=SimpleNamespace(mailbox_exists="update"))
        ops._mailbox_address = lambda mailbox: mailbox.get("email")

        transferable = ops._ensure_mailboxes(1, [{"email": "a@x", "popaccountid": 1}])

        commands = [c for c, _ in target.calls]
        self.assertIn("EmailAccounts.add", commands)
        self.assertIn("EmailAccounts.update", commands)
        self.assertEqual(["a@x"], transferable)


if __name__ == "__main__":
    unittest.main()


class MailboxDuplicateRowTests(unittest.TestCase):
    def test_duplicate_mailbox_rows_do_not_crash(self) -> None:
        # The same mailbox appearing twice must not KeyError on the stale
        # existing_rows map after the first add.
        target = StubTarget()
        target._emails = []

        def list_emails(customerid: int):
            return target._emails

        def call(command: str, payload: dict[str, object]) -> None:
            target.calls.append((command, payload))
            if command == "Emails.add":
                target._emails.append({
                    "email": payload["email_part"] + "@" + payload["domain"],
                    "popaccountid": 1,
                    "spam_tag_level": payload["spam_tag_level"],
                    "rewrite_subject": payload["rewrite_subject"],
                    "spam_kill_level": payload["spam_kill_level"],
                    "bypass_spam": payload["bypass_spam"],
                    "policy_greylist": payload["policy_greylist"],
                    "iscatchall": payload["iscatchall"],
                })

        target.list_emails = list_emails
        target.call = call

        ops = StubOps(target)
        ops.config = SimpleNamespace(behavior=SimpleNamespace(mailbox_exists="update"))
        ops._mailbox_address = lambda mailbox: mailbox.get("email")

        row = {"email": "dup@x", "popaccountid": 1, "spam_tag_level": 7, "rewrite_subject": 1, "spam_kill_level": 14,
               "bypass_spam": 0, "policy_greylist": 1, "iscatchall": 0}
        transferable = ops._ensure_mailboxes(1, [row, dict(row)])
        self.assertEqual(["dup@x"], transferable)

    def test_skip_policy_does_not_transfer_forward_only_target(self) -> None:
        target = StubTarget()
        target._emails = [{"email": "fwd@x", "popaccountid": 0}]
        target.list_emails = lambda customerid: target._emails

        ops = StubOps(target)
        ops.config = SimpleNamespace(behavior=SimpleNamespace(mailbox_exists="skip"))
        ops._mailbox_address = lambda mailbox: mailbox.get("email")

        row = {"email": "fwd@x", "popaccountid": 1}
        transferable = ops._ensure_mailboxes(1, [row])
        self.assertEqual([], transferable)
        self.assertEqual([], target.calls)
