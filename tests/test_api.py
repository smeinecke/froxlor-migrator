from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import patch

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

    def test_auth_header_is_base64_encoded(self) -> None:
        client = FroxlorClient(api_url="https://example.invalid", api_key="k", api_secret="s")
        # "k:s" base64 encoded is "azpz"
        self.assertEqual("azpz", client._auth_header())

    def test_call_retries_on_request_exception(self) -> None:
        client = FroxlorClient(api_url="https://example.invalid", api_key="k", api_secret="s")

        class DummyResponse:
            def __init__(self, status_code: int, json_data: dict[str, Any], text: str = ""):
                self.status_code = status_code
                self._json_data = json_data
                self.text = text

            def json(self):
                return self._json_data

        from requests.exceptions import RequestException

        calls = {"count": 0}

        def fake_post(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 1:
                raise RequestException("network")
            return DummyResponse(200, {"data": {"ok": True}})

        with patch("froxlor_migrator.api.requests.post", side_effect=fake_post):
            data = client.call("cmd")
            self.assertEqual({"ok": True}, data)

    def test_call_raises_on_http_error(self) -> None:
        client = FroxlorClient(api_url="https://example.invalid", api_key="k", api_secret="s")

        class DummyResponse:
            status_code = 500
            text = "err"

            def json(self):
                return {}

        with patch("froxlor_migrator.api.requests.post", return_value=DummyResponse()):
            with self.assertRaises(FroxlorApiError):
                client.call("cmd")

    def test_call_raises_on_invalid_json(self) -> None:
        client = FroxlorClient(api_url="https://example.invalid", api_key="k", api_secret="s")

        from requests.exceptions import JSONDecodeError

        class DummyResponse:
            status_code = 200
            text = "not json"

            def json(self):
                raise JSONDecodeError("msg", "doc", 0)

        with patch("froxlor_migrator.api.requests.post", return_value=DummyResponse()):
            with self.assertRaises(FroxlorApiError):
                client.call("cmd")

    def test_call_raises_on_api_semantic_error(self) -> None:
        client = FroxlorClient(api_url="https://example.invalid", api_key="k", api_secret="s")

        class DummyResponse:
            status_code = 200

            def json(self):
                return {"status": 500, "status_message": "Boom"}

        with patch("froxlor_migrator.api.requests.post", return_value=DummyResponse()):
            with self.assertRaises(FroxlorApiError) as cm:
                client.call("cmd")
            self.assertIn("Boom", str(cm.exception))

    def test_rows_from_payload_handles_none_and_non_list(self) -> None:
        client = StubClient()
        self.assertEqual([], client._rows_from_payload(None))
        self.assertEqual([{"a": 1}], client._rows_from_payload({"a": 1}))

    def test_listing_handles_list_payload_directly(self) -> None:
        client = StubClient()
        client.queue([{"id": 1}, {"id": 2}])
        self.assertEqual([{"id": 1}, {"id": 2}], client.listing("Test.listing"))

    def test_list_domain_zones_returns_empty_on_error(self) -> None:
        class FailingStub(StubClient):
            def call(self, command: str, params: dict[str, Any] | None = None) -> Any:
                if command == "DomainZones.listing":
                    raise FroxlorApiError("boom")
                return super().call(command, params)

        client = FailingStub()
        self.assertEqual([], client.list_domain_zones(domainname="example.com"))

    def test_list_email_forwarders_uses_mailbox_list_when_no_filters(self) -> None:
        client = StubClient()
        # first call is list_emails
        client.queue(
            {"list": [{"email_full": "a@example.com", "customerid": 1}]},
            # then call to EmailForwarders.listing (must include customerid to pass filtering)
            {"list": [{"destination": "other@example.com", "customerid": 1}]},
        )

        rows = client.list_email_forwarders(customerid=1)
        self.assertEqual(1, len(rows))
        self.assertEqual("a@example.com", rows[0]["emailaddr"])

    def test_list_email_senders_uses_mailbox_list_when_no_filters(self) -> None:
        client = StubClient()
        client.queue(
            {"list": [{"email_full": "a@example.com", "customerid": 1}]},
            {"list": [{"sender": "other@example.com", "customerid": 1}]},
        )

        rows = client.list_email_senders(customerid=1)
        self.assertEqual(1, len(rows))

    def test_call_sends_expected_http_body(self) -> None:
        client = FroxlorClient(api_url="https://example.invalid", api_key="k", api_secret="s")

        class DummyResponse:
            status_code = 200
            text = "{}"

            def json(self):
                return {"data": {"ok": True}}

        with patch("froxlor_migrator.api.requests.post", return_value=DummyResponse()) as mock_post:
            client.call("Cmd", {"a": 1})
            args, kwargs = mock_post.call_args
            self.assertEqual(args[0], "https://example.invalid")
            self.assertEqual(kwargs["timeout"], 30)
            self.assertIn("Authorization", kwargs["headers"])
            self.assertIn("Content-Type", kwargs["headers"])
            self.assertEqual(kwargs["data"], '{"command": "Cmd", "params": {"a": 1}}')

    def test_call_retries_twice_then_fails(self) -> None:
        client = FroxlorClient(api_url="https://example.invalid", api_key="k", api_secret="s")

        from requests.exceptions import RequestException

        def fake_post(*args, **kwargs):
            raise RequestException("boom")

        with patch("froxlor_migrator.api.requests.post", side_effect=fake_post) as mock_post:
            with self.assertRaises(FroxlorApiError):
                client.call("cmd")
            self.assertEqual(mock_post.call_count, 2)

    def test_test_connection_calls_list_functions(self) -> None:
        client = FroxlorClient(api_url="https://example.invalid", api_key="k", api_secret="s")
        called: list[str] = []

        def fake_call(command: str, params=None):
            called.append(command)
            return None

        client.call = fake_call  # type: ignore[assignment]
        client.test_connection()
        self.assertEqual(["Froxlor.listFunctions"], called)

    def test_listing_returns_empty_for_dict_without_list(self) -> None:
        client = StubClient()
        client.queue({"foo": "bar"})
        self.assertEqual([], client.listing("something"))

    def test_list_customers_and_domains_applies_filters(self) -> None:
        client = StubClient()
        client.queue({"list": [{"customerid": 1, "loginname": "bob"}]})
        self.assertEqual([{"customerid": 1, "loginname": "bob"}], client.list_customers())

        client.queue({"list": [{"customerid": 1, "loginname": "bob"}, {"customerid": 2, "loginname": "alice"}]})
        self.assertEqual([{"customerid": 1, "loginname": "bob"}], client.list_domains(customerid=1, loginname="bob"))

    def test_list_mysqls_and_emails_and_other_resources(self) -> None:
        client = StubClient()
        client.queue({"list": [{"customerid": 1, "loginname": "bob"}]})
        self.assertEqual([{"customerid": 1, "loginname": "bob"}], client.list_mysqls(customerid=1))

        client.queue({"list": [{"customerid": 1, "loginname": "bob"}]})
        self.assertEqual([{"customerid": 1, "loginname": "bob"}], client.list_emails(customerid=1))

        client.queue({"list": [{"id": 1}]})
        self.assertEqual([{"id": 1}], client.list_php_settings())

        client.queue({"list": [{"customerid": 1, "loginname": "bob"}]})
        self.assertEqual([{"customerid": 1, "loginname": "bob"}], client.list_subdomains(customerid=1))

        client.queue({"list": [{"customerid": 1, "loginname": "bob"}]})
        self.assertEqual([{"customerid": 1, "loginname": "bob"}], client.list_ftps(customerid=1))

    def test_list_email_forwarders_and_senders_by_emailaddr(self) -> None:
        client = StubClient()
        client.queue({"list": [{"destination": "other@example.com"}]})
        self.assertEqual(
            [{"destination": "other@example.com", "emailaddr": "a@example.com", "email": "a@example.com"}],
            client.list_email_forwarders(emailaddr="a@example.com"),
        )

        client.queue({"list": [{"sender": "other@example.com"}]})
        self.assertEqual(
            [{"sender": "other@example.com"}],
            client.list_email_senders(emailaddr="a@example.com"),
        )

    def test_list_email_forwarders_returns_empty_on_error(self) -> None:
        class FailingStub(StubClient):
            def call(self, command: str, params: dict[str, Any] | None = None) -> Any:
                if command == "EmailForwarders.listing":
                    raise FroxlorApiError("boom")
                return super().call(command, params)

        client = FailingStub()
        self.assertEqual([], client.list_email_forwarders(emailaddr="a@example.com"))

    def test_list_email_senders_returns_empty_on_error(self) -> None:
        class FailingStub(StubClient):
            def call(self, command: str, params: dict[str, Any] | None = None) -> Any:
                if command == "EmailSender.listing":
                    raise FroxlorApiError("boom")
                return super().call(command, params)

        client = FailingStub()
        self.assertEqual([], client.list_email_senders(emailaddr="a@example.com"))


if __name__ == "__main__":
    unittest.main()
