from __future__ import annotations

import unittest
from types import SimpleNamespace

from froxlor_migrator.migration.executor import Migrator
from froxlor_migrator.migration.types import MigrationContext, MigrationError, Selection


class DummyRunner:
    def __init__(self):
        self.dry_run = False
        self.debug_events: list[tuple[str, dict[str, object]]] = []
        self.transferred_files: list[tuple[str, str]] = []
        self.transferred_mailboxes: list[str] = []

    def debug_event(self, message: str, **payload: object) -> None:
        self.debug_events.append((message, payload))

    def transfer_files(self, source: str, dest: str) -> None:
        self.transferred_files.append((source, dest))

    def transfer_mailbox(self, mailbox: str) -> None:
        self.transferred_mailboxes.append(mailbox)


class DummyMigrator(Migrator):
    def __init__(self):
        config = SimpleNamespace(
            mysql=SimpleNamespace(target_panel_database="froxlor"),
            commands=SimpleNamespace(ssh="ssh", mysql="mysql", mysqldump="mysqldump"),
            ssh=SimpleNamespace(strict_host_key_checking=False, port=22, user="root", host="localhost"),
            paths=SimpleNamespace(source_web_root="/var/www", source_transfer_root="/var/www/transfer", target_web_root="/var/www"),
            behavior=SimpleNamespace(domain_exists="skip"),
        )
        source = SimpleNamespace(test_connection=lambda: None)
        target = SimpleNamespace(test_connection=lambda: None, list_mysqls=lambda: [])
        self.runner = DummyRunner()
        super().__init__(config, source, target, self.runner)

        # Replace complex operations with no-ops to keep execution focused on flow.
        self._ensure_target_customer = lambda customer, target_customer=None: 42
        self._build_ip_value_mapping = lambda domains, ip_mapping: {}
        self._ensure_domains = lambda *args, **kwargs: None
        self._sync_domain_redirects = lambda *args, **kwargs: None
        self._ensure_subdomains = lambda *args, **kwargs: None
        self._migrate_domain_certificates = lambda *args, **kwargs: None
        self._ensure_ftp_accounts = lambda *args, **kwargs: None
        self._ensure_ssh_keys = lambda *args, **kwargs: None
        self._ensure_data_dumps = lambda *args, **kwargs: None
        self._ensure_dir_options = lambda *args, **kwargs: None
        self._ensure_dir_protections = lambda *args, **kwargs: None
        self._sync_target_mysql_prefix_setting = lambda: None
        self._create_database_on_target = lambda target_customer_id, source_db, known_before: source_db.get("databasename", "")
        self._transfer_database_with_defaults = lambda *args, **kwargs: None
        self._sync_database_login_hashes = lambda *args, **kwargs: None
        self._ensure_mailboxes = lambda target_customer_id, mailboxes: [m["email"] for m in mailboxes]
        self._ensure_email_forwarders = lambda *args, **kwargs: None
        self._ensure_email_sender_aliases = lambda *args, **kwargs: None
        self._sync_password_hashes = lambda *args, **kwargs: None
        self._enable_letsencrypt_after_dns = lambda domains: (_ for _ in ()).throw(MigrationError("fail"))

    def preflight(self, selection: Selection) -> None:
        # keep preflight simple for test coverage
        return


class MigratorExecuteTests(unittest.TestCase):
    def test_execute_dry_run_returns_context(self) -> None:
        migrator = DummyMigrator()
        migrator.runner.dry_run = True
        selection = Selection(
            customer={"loginname": "foo"},
            target_customer={"customerid": 123},
            domains=[],
            subdomains=[],
            databases=[],
            mailboxes=[],
            email_forwarders=[],
            email_senders=[],
            ftp_accounts=[],
            ssh_keys=[],
            data_dumps=[],
            dir_protections=[],
            dir_options=[],
            domain_zones=[],
            include_files=False,
            include_databases=False,
            include_mail=False,
            include_subdomains=False,
            validate_database_names=False,
            php_setting_map={},
            ip_mapping={},
        )

        ctx = migrator.execute(selection)
        self.assertIsInstance(ctx, MigrationContext)
        self.assertEqual(123, ctx.target_customer_id)

    def test_execute_runs_full_flow_and_records_progress(self) -> None:
        migrator = DummyMigrator()
        migrator.runner.dry_run = False
        migrator.target.list_mysqls = lambda: [{"databasename": "db1"}]

        selection = Selection(
            customer={"loginname": "foo"},
            target_customer=None,
            domains=[{"domain": "example.com"}],
            subdomains=[],
            databases=[{"databasename": "db1"}],
            mailboxes=[{"email": "x@x"}],
            email_forwarders=[],
            email_senders=[],
            ftp_accounts=[],
            ssh_keys=[],
            data_dumps=[],
            dir_protections=[],
            dir_options=[],
            domain_zones=[],
            include_files=True,
            include_databases=True,
            include_mail=True,
            include_subdomains=False,
            validate_database_names=False,
            php_setting_map={},
            ip_mapping={},
            include_certificates=True,
            include_domain_zones=False,
            include_password_sync=True,
            include_forwarders=False,
            include_letsencrypt_flags=True,
            include_sender_aliases=False,
        )

        # Track progress status calls
        statuses: list[str] = []
        migrator.set_progress_callback(lambda step, total, status: statuses.append(status))

        ctx = migrator.execute(selection)

        self.assertEqual(42, ctx.target_customer_id)
        self.assertIn("Running preflight checks", statuses[0])
        self.assertIn("Mailbox content transferred", statuses)
        self.assertTrue(migrator.runner.transferred_files)
        self.assertTrue(migrator.runner.transferred_mailboxes)


if __name__ == "__main__":
    unittest.main()
