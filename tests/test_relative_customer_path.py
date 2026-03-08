from __future__ import annotations

import unittest

from froxlor_migrator.migrate import Migrator


class RelativeCustomerPathTests(unittest.TestCase):
    def test_relative_customer_path_returns_string_and_strips_customer_prefix(self) -> None:
        migrator = object.__new__(Migrator)

        self.assertEqual("", migrator._relative_customer_path("", "custalpha"))
        self.assertEqual("secure", migrator._relative_customer_path("/var/customers/webs/custalpha/secure", "custalpha"))
        self.assertEqual("logs", migrator._relative_customer_path("custalpha/logs", "custalpha"))


if __name__ == "__main__":
    unittest.main()
