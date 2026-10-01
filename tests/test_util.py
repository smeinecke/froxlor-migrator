from __future__ import annotations

import string
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from froxlor_migrator.util import (
    as_bool,
    as_int,
    ensure_dir,
    parse_multi_select,
    pick,
    random_password,
    resolve_subdomain_parts,
    slugify,
)


class UtilTests(unittest.TestCase):
    def test_pick_returns_first_present_value(self) -> None:
        row = {"primary": "value", "secondary": "other"}
        self.assertEqual("value", pick(row, "missing", "primary", "secondary"))
        self.assertEqual("fallback", pick(row, "missing", default="fallback"))

    def test_as_int_uses_default_on_invalid_values(self) -> None:
        self.assertEqual(42, as_int("42"))
        self.assertEqual(-1, as_int("oops", default=-1))

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

    def test_random_password_uses_expected_alphabet(self) -> None:
        password = random_password(16)
        allowed = set(string.ascii_letters + string.digits + "-_")
        self.assertEqual(16, len(password))
        self.assertTrue(all(char in allowed for char in password))

    def test_slugify_handles_empty_result(self) -> None:
        with patch("froxlor_migrator.util.random.randint", return_value=1234):
            self.assertEqual("migration-1234", slugify("!!!"))
        self.assertEqual("hello-world", slugify("Hello World"))

    def test_parse_multi_select_supports_ranges_and_all(self) -> None:
        self.assertEqual([0, 1, 2, 3], parse_multi_select("all", max_index=4))
        self.assertEqual([0, 2, 3], parse_multi_select("1,3-4", max_index=5))
        self.assertEqual([], parse_multi_select("none", max_index=5))

    def test_parse_multi_select_skips_out_of_range_entries(self) -> None:
        self.assertEqual([0], parse_multi_select("0,1,99", max_index=1))

    def test_ensure_dir_creates_path_and_returns_path_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "manifests" / "nested"
            result = ensure_dir(target)
            self.assertTrue(target.exists())
            self.assertEqual(target, result)

    def test_resolve_subdomain_parts_simple(self) -> None:
        result = resolve_subdomain_parts("sub.example.com", "", {"example.com"})
        self.assertEqual(("sub", "example.com"), result)

    def test_resolve_subdomain_parts_multi_level_uses_longest_known_suffix(self) -> None:
        result = resolve_subdomain_parts("a.b.example.com", "", {"example.com"})
        self.assertEqual(("a.b", "example.com"), result)

    def test_resolve_subdomain_parts_prefers_known_hint(self) -> None:
        result = resolve_subdomain_parts("a.b.example.com", "b.example.com", {"b.example.com"})
        self.assertEqual(("a", "b.example.com"), result)

    def test_resolve_subdomain_parts_hint_falls_back_to_suffix_walk(self) -> None:
        # Froxlor may report the intermediate subdomain as parentdomain; when
        # that is not a known main domain, the longest known suffix wins.
        result = resolve_subdomain_parts("a.b.example.com", "b.example.com", {"example.com"})
        self.assertEqual(("a.b", "example.com"), result)

    def test_resolve_subdomain_parts_returns_none_when_no_parent(self) -> None:
        self.assertIsNone(resolve_subdomain_parts("a.b.other.com", "", {"example.com"}))
        self.assertIsNone(resolve_subdomain_parts("example.com", "", {"example.com"}))


if __name__ == "__main__":
    unittest.main()


class ResolveSubdomainPartsHintMismatchTests(unittest.TestCase):
    def test_hint_in_known_domains_but_not_a_suffix_of_name(self) -> None:
        # The hint must be an actual suffix of the subdomain name; a stale or
        # inconsistent parentdomain field must not produce a garbage label.
        self.assertIsNone(resolve_subdomain_parts("sub.other.com", "example.com", {"example.com"}))

    def test_hint_matches_name_exactly_returns_none(self) -> None:
        self.assertIsNone(resolve_subdomain_parts("example.com", "example.com", {"example.com"}))
