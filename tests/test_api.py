from __future__ import annotations

import unittest
from typing import Any

from froxlor_migrator.api import FroxlorApiError, FroxlorClient


class StubClient(FroxlorClient):
    def __init__(self) -> None:
        super().__init__(api_url="https://example.invalid", api_key="k", api_secret="s")
        self._responses: list[Any] = []
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def queue(self, *responses: Any) -> None:
        self._responses.extend(responses)

    def call(self, command: str, params: dict[str, Any] | None = None) -> Any:
        self.calls.append((command, dict(params or {})))
        if not self._responses:
            raise AssertionError(f"No queued response left for {command}")
        return self._responses.pop(0)


class ApiClientTests(unittest.TestCase):
    def test_listing_paginates_until_count(self) -> None:
        client = StubClient()
        client.queue(
            {"list": [{"id": 1}, {"id": 2}], "count": 3},
            {"list": [{"id": 3}], "count": 3},
        )

        rows = client.listing("Domains.listing")

        self.assertEqual([{"id": 1}, {"id": 2}, {"id": 3}], rows)
        self.assertEqual(
            [
                ("Domains.listing", {"sql_limit": 500, "sql_offset": 0}),
                ("Domains.listing", {"sql_limit": 500, "sql_offset": 2}),
            ],
            client.calls,
        )

    def test_filter_customer_rows_respects_id_and_login(self) -> None:
        client = StubClient()
        rows = [
            {"customerid": 10, "loginname": "alpha"},
            {"customerid": 10, "loginname": "beta"},
            {"customerid": 11, "loginname": "alpha"},
        ]

        filtered = client._filter_customer_rows(rows, customerid=10, loginname="alpha")

        self.assertEqual([{"customerid": 10, "loginname": "alpha"}], filtered)

    def test_list_email_forwarders_normalizes_payload(self) -> None:
        client = StubClient()
        client.queue({
            "list": [
                {"address": "Alice@Example.com"},
                {"address": "Target@One.Tld", "email": "Alice@Example.com"},
                {"destination": "Other@Two.Tld"},
            ]
        })

        rows = client.list_email_forwarders(emailaddr="Alice@Example.com")

        self.assertEqual(2, len(rows))
        self.assertEqual("alice@example.com", rows[0]["emailaddr"])
        self.assertEqual("target@one.tld", rows[0]["destination"])
        self.assertEqual("other@two.tld", rows[1]["destination"])

    def test_list_dir_protections_and_options_use_customer_filter(self) -> None:
        client = StubClient()
        client.queue(
            {
                "list": [
                    {"customerid": 10, "path": "a", "username": "u1"},
                    {"customerid": 11, "path": "b", "username": "u2"},
                ],
                "count": 2,
            },
            {
                "list": [
                    {"customerid": 10, "path": "a"},
                    {"customerid": 11, "path": "b"},
                ],
                "count": 2,
            },
        )

        protections = client.list_dir_protections(customerid=10)
        options = client.list_dir_options(customerid=10)

        self.assertEqual([{"customerid": 10, "path": "a", "username": "u1"}], protections)
        self.assertEqual([{"customerid": 10, "path": "a"}], options)

    def test_list_ssh_keys_uses_customer_filter(self) -> None:
        client = StubClient()
        client.queue({
            "list": [
                {"customerid": 9, "username": "u9", "ssh_pubkey": "k9"},
                {"customerid": 10, "username": "u10", "ssh_pubkey": "k10"},
            ],
            "count": 2,
        })

        rows = client.list_ssh_keys(customerid=10)

        self.assertEqual([{"customerid": 10, "username": "u10", "ssh_pubkey": "k10"}], rows)

    def test_list_data_dumps_returns_empty_on_access_error(self) -> None:
        class FailingStub(StubClient):
            def call(self, command: str, params: dict[str, Any] | None = None) -> Any:
                if command == "DataDump.listing":
                    raise FroxlorApiError("HTTP 405")
                return super().call(command, params)

        client = FailingStub()

        self.assertEqual([], client.list_data_dumps(customerid=10))


if __name__ == "__main__":
    unittest.main()
