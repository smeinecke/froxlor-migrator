from __future__ import annotations

import unittest

from froxlor_migrator.util import relative_customer_path


class RelativeCustomerPathTests(unittest.TestCase):
    def test_relative_customer_path_returns_string_and_strips_customer_prefix(self) -> None:
        self.assertEqual("", relative_customer_path("", "custalpha"))
        self.assertEqual("secure", relative_customer_path("/var/customers/webs/custalpha/secure", "custalpha"))
        self.assertEqual("logs", relative_customer_path("custalpha/logs", "custalpha"))

    def test_relative_customer_path_preserves_nested_login_component(self) -> None:
        self.assertEqual(
            "custalpha/logs",
            relative_customer_path("/var/customers/webs/custalpha/custalpha/logs", "custalpha"),
        )

    def test_relative_customer_path_without_login_marker(self) -> None:
        self.assertEqual("srv/special", relative_customer_path("/srv/special", "custalpha"))
        self.assertEqual("subdir", relative_customer_path("subdir", "custalpha"))
        self.assertEqual("subdir", relative_customer_path("subdir", ""))


if __name__ == "__main__":
    unittest.main()
