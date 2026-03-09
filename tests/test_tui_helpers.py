from __future__ import annotations

import unittest
from argparse import Namespace

from froxlor_migrator import tui as tui_module


class TuiHelperTests(unittest.TestCase):
    def test_split_and_dedupe_helpers(self) -> None:
        self.assertEqual(["a", "b"], tui_module._split_csv(" a , b "))
        self.assertEqual([], tui_module._split_csv(None))
        self.assertEqual(["A", "b"], tui_module._dedupe_keep_order(["A", "", "b", "A"]))

    def test_parse_mapping_accepts_multiple_formats(self) -> None:
        mapping = tui_module._parse_mapping_arg("source=>target,one=two", "php map")
        self.assertEqual({"source": "target", "one": "two"}, mapping)
        with self.assertRaises(ValueError):
            tui_module._parse_mapping_arg("invalid", "php map")

    def test_build_value_index_and_resolve_named_mapping(self) -> None:
        rows = [
            {"id": 1, "aliases": ["alpha", "a"]},
            {"id": 2, "aliases": ["beta"]},
        ]
        index = tui_module._build_value_index(rows, lambda row: row["id"], lambda row: row["aliases"])
        self.assertEqual({"alpha": {1}, "a": {1}, "beta": {2}}, index)

        mapping = tui_module._resolve_named_mapping(
            {"alpha": "first", "beta": "second"},
            rows,
            lambda row: row["id"],
            lambda row: row["aliases"],
            [
                {"id": 10, "aliases": ["first"]},
                {"id": 20, "aliases": ["second"]},
            ],
            lambda row: row["id"],
            lambda row: row["aliases"],
            "mapping",
        )
        self.assertEqual({1: 10, 2: 20}, mapping)

        with self.assertRaises(ValueError):
            tui_module._resolve_named_mapping(
                {"unknown": "first"},
                rows,
                lambda row: row["id"],
                lambda row: row["aliases"],
                [],
                lambda row: row["id"],
                lambda row: row["aliases"],
                "mapping",
            )

    def test_build_php_and_ip_mapping_tokens(self) -> None:
        php_tokens = tui_module._build_php_mapping_tokens(
            {1: 3},
            [{"id": 1, "description": "PHP 8", "binary": "/usr/bin/php8"}],
            [{"id": 3, "description": "PHP 8", "binary": "/usr/bin/php8"}],
        )
        self.assertEqual({"php 8|/usr/bin/php8": "php 8|/usr/bin/php8"}, php_tokens)

        ip_tokens = tui_module._build_ip_mapping_tokens(
            {1: 2},
            [{"id": 1, "ip": "192.0.2.10", "port": 80, "ssl": 0}],
            [{"id": 2, "ip": "192.0.2.20", "port": 80, "ssl": 1}],
        )
        self.assertEqual({"192.0.2.10:80:0": "192.0.2.20:80:1"}, ip_tokens)

    def test_select_rows_by_tokens_supports_special_keywords(self) -> None:
        rows = [
            {"value": "one"},
            {"value": "two"},
        ]

        def selector(row: dict) -> list[str]:
            return [row["value"]]

        self.assertEqual(rows, tui_module._select_rows_by_tokens(rows, None, selector, "values"))
        self.assertEqual([], tui_module._select_rows_by_tokens(rows, "none", selector, "values"))
        self.assertEqual(rows, tui_module._select_rows_by_tokens(rows, "all", selector, "values"))
        self.assertEqual([rows[1]], tui_module._select_rows_by_tokens(rows, "two", selector, "values"))
        with self.assertRaises(ValueError):
            tui_module._select_rows_by_tokens(rows, "missing", selector, "values")

    def test_build_replay_command_includes_flags(self) -> None:
        args = Namespace(
            config="config.toml",
            apply=True,
            skip_subdomains=False,
            skip_database_name_validation=True,
            skip_certificates=True,
            skip_dns_zones=False,
            skip_password_sync=False,
            skip_forwarders=True,
            skip_sender_aliases=False,
        )
        selected_customer = {"customerid": 42, "loginname": "cust"}
        target_customer = {"customerid": 84, "loginname": "target"}
        command = tui_module._build_replay_command(
            args,
            selected_customer,
            target_customer,
            migrate_whole_customer=False,
            selected_domains=[{"domain": "example.com"}],
            selected_subdomains=[{"domain": "sub.example.com"}],
            selected_databases=[{"databasename": "db1"}],
            selected_mailboxes=[{"email": "user@example.com"}],
            selected_ftps=[{"username": "ftpuser"}],
            php_mapping_tokens={"php8": "php8"},
            ip_mapping_tokens={"192.0.2.1:80:0": "192.0.2.2:80:0"},
            include_files=True,
            include_databases=False,
            include_mail=True,
            include_certificates=False,
            include_domain_zones=True,
            include_password_sync=False,
            include_forwarders=False,
            include_sender_aliases=True,
            debug=True,
        )
        self.assertIn("--apply", command)
        self.assertIn("--domain-only", command)
        self.assertIn("--skip-database-name-validation", command)
        self.assertIn("--skip-certificates", command)
        self.assertIn("--skip-forwarders", command)
        self.assertIn("--php-map", command)
        self.assertIn("--domains example.com", command)

    def test_view_helpers_produce_expected_dicts(self) -> None:
        self.assertEqual(
            [{"id": 1, "login": "user", "name": "Company", "email": "e", "_raw": {"customerid": 1, "loginname": "user", "company": "Company", "email": "e"}}],
            tui_module._customer_view([{"customerid": 1, "loginname": "user", "company": "Company", "email": "e"}]),
        )
        self.assertEqual(
            [{"domain": "d", "docroot": "/tmp", "ssl": 1, "php": 2, "_raw": {"domain": "d", "documentroot": "/tmp", "sslenabled": 1, "phpsettingid": 2}}],
            tui_module._domain_view([{"domain": "d", "documentroot": "/tmp", "sslenabled": 1, "phpsettingid": 2}]),
        )
        self.assertEqual(
            [{"dbname": "db", "description": "desc", "server": "srv", "_raw": {"databasename": "db", "description": "desc", "mysql_server": "srv"}}],
            tui_module._db_view([{"databasename": "db", "description": "desc", "mysql_server": "srv"}]),
        )
        self.assertEqual(
            [{"domain": "d", "path": "/p", "ssl": 0, "_raw": {"domain": "d", "path": "/p", "sslenabled": 0}}],
            tui_module._subdomain_view([{"domain": "d", "path": "/p", "sslenabled": 0}]),
        )
        self.assertEqual(
            [{"username": "u", "path": "/p", "login": 1, "_raw": {"username": "u", "path": "/p", "login_enabled": 1}}],
            tui_module._ftp_view([{"username": "u", "path": "/p", "login_enabled": 1}]),
        )
        self.assertEqual(
            [{"email": "a@b.com", "domain": "b.com", "_raw": {"email": "a@b.com"}}],
            tui_module._mail_view([{"email": "a@b.com"}], {"b.com"}),
        )
        self.assertEqual(
            [{"id": 1, "description": "d", "binary": "b", "_raw": {"id": 1, "description": "d", "binary": "b"}}],
            tui_module._php_settings_view([{"id": 1, "description": "d", "binary": "b"}]),
        )
        self.assertEqual(
            [{"id": 1, "ip": "1.2.3.4", "port": 123, "ssl": 1, "_raw": {"id": 1, "ip": "1.2.3.4", "port": 123, "ssl": 1}}],
            tui_module._ip_view([{"id": 1, "ip": "1.2.3.4", "port": 123, "ssl": 1}]),
        )
        self.assertTrue(tui_module._domain_in_source_root({"documentroot": "/var/www/ex"}, "/var/www"))

    def test_build_ip_map_non_interactive_returns_mapping_and_rows(self) -> None:
        class StubTarget:
            def listing(self, command: str):
                if command == "IpsAndPorts.listing":
                    return [{"id": 10, "ip": "192.0.2.10", "port": 80, "ssl": 0}]
                return []

        mapping, source_rows, target_rows = tui_module._build_ip_map(
            selected_domains=[{"ipsandports": [{"id": 1, "ip": "192.0.2.1", "port": 80, "ssl": 0}]}],
            target=StubTarget(),
            preset_mapping={"192.0.2.1:80:0": "192.0.2.10:80:0"},
            non_interactive=True,
        )
        self.assertEqual({1: 10}, mapping)
        self.assertEqual(1, len(source_rows))
        self.assertEqual(1, len(target_rows))

    def test_choose_rows_interactive_and_empty(self) -> None:
        # Simulate user entering invalid selection then valid selection
        from unittest.mock import patch

        rows = [{"id": 1, "login": "x", "name": "n", "email": "e"}]
        with patch("froxlor_migrator.tui.Prompt.ask", side_effect=["invalid", "1"]):
            with patch("froxlor_migrator.tui.console.print"):
                selected = tui_module._choose_rows("Title", rows, [("id", "ID")], multi=False, allow_empty=False)
                self.assertEqual([rows[0]], selected)

        # allow_empty path
        with patch("froxlor_migrator.tui.Prompt.ask", return_value="new"):
            with patch("froxlor_migrator.tui.console.print"):
                self.assertEqual([], tui_module._choose_rows("Title", rows, [("id", "ID")], multi=False, allow_empty=True))

    def test_build_php_setting_map_interactive(self) -> None:
        from unittest.mock import patch

        selected_domains = [{"phpsettingid": 1}]
        source_settings = [{"id": 1, "description": "PHP", "binary": "php"}]
        target_settings = [{"id": 1, "description": "PHP", "binary": "php"}]

        with patch("froxlor_migrator.tui.Prompt.ask", return_value="1"):
            with patch("froxlor_migrator.tui.console.print"):
                mapping, rows = tui_module._build_php_setting_map(
                    selected_domains,
                    source_settings,
                    target_settings,
                    preset_mapping=None,
                    non_interactive=False,
                )
        self.assertEqual({1: 1}, mapping)
        self.assertEqual(1, len(rows))


if __name__ == "__main__":
    unittest.main()
