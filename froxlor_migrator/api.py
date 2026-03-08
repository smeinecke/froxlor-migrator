from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from typing import Any

import requests
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError
from requests.exceptions import RequestException


class FroxlorApiError(RuntimeError):
    pass


@dataclass
class FroxlorClient:
    api_url: str
    api_key: str
    api_secret: str
    timeout_seconds: int = 30

    def _auth_header(self) -> str:
        raw = f"{self.api_key}:{self.api_secret}".encode()
        return base64.b64encode(raw).decode("ascii")

    def call(self, command: str, params: dict[str, Any] | None = None) -> Any:
        body: dict[str, Any] = {"command": command}
        if params:
            body["params"] = params

        try:
            response = requests.post(
                self.api_url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Basic {self._auth_header()}",
                },
                data=json.dumps(body),
                timeout=self.timeout_seconds,
            )
        except RequestException as exc:
            # Network-level failures can be transient; retry once.
            time.sleep(0.5)
            try:
                response = requests.post(
                    self.api_url,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Basic {self._auth_header()}",
                    },
                    data=json.dumps(body),
                    timeout=self.timeout_seconds,
                )
            except RequestException as exc2:
                raise FroxlorApiError(f"API {command} request failed: {exc2}") from exc2

        if response.status_code >= 400:
            raise FroxlorApiError(f"API {command} failed with HTTP {response.status_code}: {response.text[:400]}")

        try:
            data = response.json()
        except RequestsJSONDecodeError as exc:
            snippet = response.text[:400]
            raise FroxlorApiError(
                f"API {command} returned non-JSON response (HTTP {response.status_code}): {snippet!r}"
            ) from exc

        if data.get("status") and int(data.get("status", 200)) >= 400:
            raise FroxlorApiError(f"API {command} failed: {data.get('status_message', 'unknown error')}")
        return data.get("data")

    def test_connection(self) -> None:
        self.call("Froxlor.listFunctions")

    def listing(self, command: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        merged = dict(params or {})
        merged.setdefault("sql_limit", 500)
        merged.setdefault("sql_offset", 0)

        results: list[dict[str, Any]] = []
        while True:
            data = self.call(command, merged)
            if isinstance(data, dict) and "list" in data:
                items = data.get("list") or []
                count = int(data.get("count", len(items)))
            elif isinstance(data, list):
                items = data
                count = len(items)
            else:
                items = []
                count = 0

            results.extend(items)
            if not items or len(results) >= count:
                break
            merged["sql_offset"] = int(merged.get("sql_offset", 0)) + len(items)

        return results

    def list_customers(self) -> list[dict[str, Any]]:
        return self.listing("Customers.listing")

    def list_domains(self, customerid: int | None = None, loginname: str | None = None) -> list[dict[str, Any]]:
        return self._filter_customer_rows(self.listing("Domains.listing"), customerid, loginname)

    def list_mysqls(self, customerid: int | None = None, loginname: str | None = None) -> list[dict[str, Any]]:
        return self._filter_customer_rows(self.listing("Mysqls.listing"), customerid, loginname)

    def list_emails(self, customerid: int | None = None, loginname: str | None = None) -> list[dict[str, Any]]:
        return self._filter_customer_rows(self.listing("Emails.listing"), customerid, loginname)

    def list_php_settings(self) -> list[dict[str, Any]]:
        return self.listing("PhpSettings.listing")

    def list_subdomains(self, customerid: int | None = None, loginname: str | None = None) -> list[dict[str, Any]]:
        return self._filter_customer_rows(self.listing("SubDomains.listing"), customerid, loginname)

    def list_ftps(self, customerid: int | None = None, loginname: str | None = None) -> list[dict[str, Any]]:
        return self._filter_customer_rows(self.listing("Ftps.listing"), customerid, loginname)

    def list_dir_protections(self, customerid: int | None = None, loginname: str | None = None) -> list[dict[str, Any]]:
        return self._filter_customer_rows(self.listing("DirProtections.listing"), customerid, loginname)

    def list_dir_options(self, customerid: int | None = None, loginname: str | None = None) -> list[dict[str, Any]]:
        return self._filter_customer_rows(self.listing("DirOptions.listing"), customerid, loginname)

    def list_ssh_keys(self, customerid: int | None = None, loginname: str | None = None) -> list[dict[str, Any]]:
        return self._filter_customer_rows(self.listing("SshKeys.listing"), customerid, loginname)

    def list_data_dumps(self, customerid: int | None = None, loginname: str | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if customerid is not None:
            params["customerid"] = customerid
        if loginname:
            params["loginname"] = loginname
        try:
            return self.listing("DataDump.listing", params)
        except FroxlorApiError:
            return []

    def list_email_forwarders(
        self,
        customerid: int | None = None,
        loginname: str | None = None,
        emailaddr: str | None = None,
        email_id: int | None = None,
    ) -> list[dict[str, Any]]:
        if emailaddr or email_id:
            params: dict[str, Any] = {}
            if emailaddr:
                params["emailaddr"] = emailaddr
            if email_id:
                params["id"] = email_id
            try:
                raw_rows = self._rows_from_payload(self.call("EmailForwarders.listing", params))
            except FroxlorApiError:
                return []
            mailbox_email = (emailaddr or "").strip().lower()
            rows: list[dict[str, Any]] = []
            for item in raw_rows:
                destination = str(item.get("destination") or item.get("address") or "").strip().lower()
                if not destination or (mailbox_email and destination == mailbox_email):
                    continue
                rows.append({
                    **item,
                    "emailaddr": mailbox_email or str(item.get("email") or item.get("emailaddr") or "").strip().lower(),
                    "email": mailbox_email or str(item.get("email") or item.get("emailaddr") or "").strip().lower(),
                    "destination": destination,
                })
            return rows

        rows: list[dict[str, Any]] = []
        for mailbox in self.list_emails(customerid=customerid, loginname=loginname):
            mailbox_email = str(mailbox.get("email_full") or mailbox.get("email") or mailbox.get("emailaddr") or "").strip()
            if not mailbox_email:
                continue
            try:
                chunk = self._rows_from_payload(self.call("EmailForwarders.listing", {"emailaddr": mailbox_email}))
            except FroxlorApiError:
                chunk = []
            for item in chunk:
                destination = str(item.get("destination") or item.get("address") or "").strip().lower()
                if not destination or destination == mailbox_email.lower():
                    continue
                rows.append({
                    **item,
                    "emailaddr": mailbox_email.lower(),
                    "email": mailbox_email.lower(),
                    "destination": destination,
                })
        return self._filter_customer_rows(rows, customerid, loginname)

    def list_email_senders(
        self,
        customerid: int | None = None,
        loginname: str | None = None,
        emailaddr: str | None = None,
        email_id: int | None = None,
    ) -> list[dict[str, Any]]:
        if emailaddr or email_id:
            params: dict[str, Any] = {}
            if emailaddr:
                params["emailaddr"] = emailaddr
            if email_id:
                params["id"] = email_id
            try:
                return self._rows_from_payload(self.call("EmailSender.listing", params))
            except FroxlorApiError:
                return []

        rows: list[dict[str, Any]] = []
        for mailbox in self.list_emails(customerid=customerid, loginname=loginname):
            mailbox_email = str(mailbox.get("email_full") or mailbox.get("email") or mailbox.get("emailaddr") or "").strip()
            if not mailbox_email:
                continue
            try:
                chunk = self._rows_from_payload(self.call("EmailSender.listing", {"emailaddr": mailbox_email}))
            except FroxlorApiError:
                chunk = []
            rows.extend(chunk)
        return self._filter_customer_rows(rows, customerid, loginname)

    def list_domain_zones(self, domainname: str | None = None, domain_id: int | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if domainname:
            params["domainname"] = domainname
        if domain_id is not None:
            params["id"] = domain_id
        try:
            return self.listing("DomainZones.listing", params)
        except FroxlorApiError:
            return []

    def _filter_customer_rows(
        self,
        rows: list[dict[str, Any]],
        customerid: int | None,
        loginname: str | None,
    ) -> list[dict[str, Any]]:
        if customerid is None and not loginname:
            return rows

        wanted_login = (loginname or "").strip().lower()
        filtered: list[dict[str, Any]] = []
        for row in rows:
            row_customer_id = row.get("customerid")
            row_login = str(row.get("loginname", "")).strip().lower()
            if customerid is not None and int(row_customer_id or 0) != int(customerid):
                continue
            if wanted_login and row_login and row_login != wanted_login:
                continue
            filtered.append(row)
        return filtered

    def _rows_from_payload(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            if "list" in payload:
                return list(payload.get("list") or [])
            return [payload]
        if isinstance(payload, list):
            return payload
        return []
