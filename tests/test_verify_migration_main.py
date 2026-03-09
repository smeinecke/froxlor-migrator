from __future__ import annotations

import sys
import unittest

from froxlor_migrator import verify_migration


class StubClient:
    def __init__(self, customers: list[dict[str, object]], php_settings: list[dict[str, object]], domains: list[dict[str, object]]):
        self._customers = customers
        self._php_settings = php_settings
        self._domains = domains

    def list_customers(self) -> list[dict[str, object]]:
        return self._customers

    def list_php_settings(self) -> list[dict[str, object]]:
        return self._php_settings

    def list_domains(self, **kwargs):
        return self._domains

    def list_subdomains(self, **kwargs):
        return []

    def list_emails(self, **kwargs):
        return []

    def list_ftps(self, **kwargs):
        return []

    def list_dir_protections(self, **kwargs):
        return []

    def list_dir_options(self, **kwargs):
        return []

    def list_ssh_keys(self, **kwargs):
        return []

    def list_data_dumps(self, **kwargs):
        return []

    def list_email_forwarders(self, **kwargs):
        return []

    def list_email_senders(self, **kwargs):
        return []

    def list_domain_zones(self, **kwargs):
        return []

    def listing(self, command: str):
        return []


class VerifyMigrationMainTests(unittest.TestCase):
    def test_main_succeeds_when_source_and_target_match(self) -> None:
        # Replace config loader and FroxlorClient used in main() to avoid external dependencies
        class DummyConfig:
            class Api:
                api_url = ""
                api_key = ""
                api_secret = ""
                timeout_seconds = 30

            source = Api()
            target = Api()

            class Paths:
                source_web_root = "/var/www"
                source_transfer_root = "/var/www"
                target_web_root = "/var/www"

            paths = Paths()

            class Mysql:
                target_panel_database = "froxlor"

            mysql = Mysql()

        source_customers = [{"customerid": 1, "loginname": "alice"}]
        target_customers = [{"customerid": 1, "loginname": "alice"}]
        php_settings = [{"id": 1, "description": "php", "binary": "php"}]
        domains = [{"domain": "example.com", "documentroot": "/var/www/example.com"}]

        verify_migration.load_config = lambda path: DummyConfig()
        verify_migration.FroxlorClient = lambda api_url, api_key, api_secret, timeout: StubClient(
            source_customers if api_url == "https://s" else target_customers, php_settings, domains
        )
        verify_migration._load_redirect_map_source = lambda config, customer_id: {}
        verify_migration._load_redirect_map_target = lambda config, customer_id: {}

        sys_argv = sys.argv
        try:
            sys.argv = ["verify_migration", "--config", "config.toml", "--customer", "alice"]
            result = verify_migration.main()
            self.assertEqual(0, result)
        finally:
            sys.argv = sys_argv


if __name__ == "__main__":
    unittest.main()
