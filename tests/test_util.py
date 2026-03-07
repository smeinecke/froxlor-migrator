from __future__ import annotations

import unittest

from froxlor_migrator.util import as_bool


class UtilTests(unittest.TestCase):
    def test_as_bool_parses_yes_no_variants(self) -> None:
        self.assertTrue(as_bool("Y"))
        self.assertTrue(as_bool("yes"))
        self.assertTrue(as_bool("1"))
        self.assertFalse(as_bool("N"))
        self.assertFalse(as_bool("no"))
        self.assertFalse(as_bool("0"))

    def test_as_bool_uses_default_for_unknown_string(self) -> None:
        self.assertTrue(as_bool("unexpected", default=True))
        self.assertFalse(as_bool("unexpected", default=False))


if __name__ == "__main__":
    unittest.main()
