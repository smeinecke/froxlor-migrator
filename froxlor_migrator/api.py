from __future__ import annotations

import base64
import json
import logging
import time
from dataclasses import dataclass
from typing import Any

import requests
from requests.exceptions import JSONDecodeError as RequestsJSONDecodeError
from requests.exceptions import RequestException

from .util import as_int


class FroxlorApiError(RuntimeError):
    pass


logger = logging.getLogger(__name__)


_SENSITIVE_PARAM_MARKERS = ("password", "passwd", "secret", "ssl_key", "private_key", "privatekey", "data_2fa", "token", "apikey", "api_key")


def _is_idempotent_command(command: str) -> bool:
    lowered = command.lower()
    return lowered.endswith((".listing", ".list", ".get"))


def _redact_params(params: Any) -> Any:
    if isinstance(params, dict):
        redacted: dict[str, Any] = {}
        for key, value in params.items():
            lowered = str(key).lower()
            if any(marker in lowered for marker in _SENSITIVE_PARAM_MARKERS):
                redacted[key] = "***"
            elif isinstance(value, dict):
                redacted[key] = _redact_params(value)
            else:
                redacted[key] = value
        return redacted
    return params


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

        logger.debug(
            "Froxlor API call: command=%s url=%s params=%s",
            command,
            self.api_url,
            _redact_params(params or {}),
        )

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
            if not _is_idempotent_command(command):
                logger.debug("Froxlor API request failed (mutating command, not retried): command=%s error=%s", command, exc)
                raise FroxlorApiError(f"API {command} request failed: {exc}") from exc
            logger.debug("Froxlor API request failed, retrying once: command=%s error=%s", command, exc)
            # Network-level failures can be transient; retry once for reads only.
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
                logger.debug("Froxlor API retry failed: command=%s error=%s", command, exc2)
                raise FroxlorApiError(f"API {command} request failed: {exc2}") from exc2

        logger.debug("Froxlor API response: command=%s http_status=%s", command, response.status_code)

        if response.status_code >= 400:
            logger.debug(
                "Froxlor API HTTP error: command=%s status=%s body=%s",
                command,
                response.status_code,
                response.text[:400],
            )
            raise FroxlorApiError(f"API {command} failed with HTTP {response.status_code}: {response.text[:400]}")

        try:
            data = response.json()
        except RequestsJSONDecodeError as exc:
            snippet = response.text[:400]
            logger.debug("Froxlor API non-JSON response: command=%s body=%s", command, snippet)
            raise FroxlorApiError(f"API {command} returned non-JSON response (HTTP {response.status_code}): {snippet!r}") from exc

        if data.get("status") and as_int(data.get("status"), default=200) >= 400:
            logger.debug(
                "Froxlor API semantic error: command=%s status=%s message=%s",
                command,
                data.get("status"),
                data.get("status_message", "unknown error"),
            )
            raise FroxlorApiError(f"API {command} failed: {data.get('status_message', 'unknown error')}")
        logger.debug("Froxlor API call succeeded: command=%s", command)
        return data.get("data")

    def test_connection(self) -> None:
        self.call("Froxlor.listFunctions")

    def listing(self, command: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        merged = dict(params or {})
        merged.setdefault("sql_limit", 500)
        merged.setdefault("sql_offset", 0)

        results: list[dict[str, Any]] = []
        previous_items: list[dict[str, Any]] | None = None
        limit = as_int(merged["sql_limit"], default=500)
        while True:
            data = self.call(command, merged)
            if isinstance(data, dict) and "list" in data:
                items = data.get("list") or []
            elif isinstance(data, list):
                items = data
            else:
                items = []

            results.extend(items)
            # Some endpoints return `count` as the page size, others as the
            # total — treat a short page as the end instead of trusting count.
            if not items or len(items) < limit:
                break
            merged["sql_offset"] = int(merged.get("sql_offset", 0)) + len(items)
            if items == previous_items:
                # Endpoint ignored sql_offset and returned the same page —
                # stop rather than loop forever.
                logger.warning("API %s returned an identical page; stopping pagination", command)
                results = results[: -len(items)]
                break
            previous_items = items

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

    def list_data_dumps(self, customerid: int | None = None, loginname: str | None = None, strict: bool = False) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if customerid is not None:
            params["customerid"] = customerid
        if loginname:
            params["loginname"] = loginname
        try:
            return self.listing("DataDump.listing", params)
        except FroxlorApiError as exc:
            if strict:
                raise
            logger.warning("DataDump.listing failed; data dumps will not be migrated: %s", exc)
            return []

    def _forwarder_rows_from_payload(self, payload: Any, mailbox_email: str) -> list[dict[str, Any]]:
        """Normalize EmailForwarders.listing rows: lowercase destinations,
        drop empty/self-referencing rows, and stamp the owning mailbox."""
        rows: list[dict[str, Any]] = []
        mailbox_lower = mailbox_email.lower()
        for item in self._rows_from_payload(payload):
            destination = str(item.get("destination") or item.get("address") or "").strip().lower()
            if not destination or (mailbox_lower and destination == mailbox_lower):
                continue
            owner = mailbox_lower or str(item.get("email") or item.get("emailaddr") or "").strip().lower()
            rows.append({**item, "emailaddr": owner, "email": owner, "destination": destination})
        return rows

    def list_email_forwarders(
        self,
        customerid: int | None = None,
        loginname: str | None = None,
        emailaddr: str | None = None,
        email_id: int | None = None,
        strict: bool = False,
    ) -> list[dict[str, Any]]:
        if emailaddr or email_id:
            params: dict[str, Any] = {}
            if emailaddr:
                params["emailaddr"] = emailaddr
            if email_id:
                params["id"] = email_id
            try:
                payload = self.call("EmailForwarders.listing", params)
            except FroxlorApiError as exc:
                if strict:
                    raise
                logger.warning("EmailForwarders.listing failed for %s: %s", emailaddr or email_id, exc)
                return []
            return self._forwarder_rows_from_payload(payload, (emailaddr or "").strip().lower())

        rows: list[dict[str, Any]] = []
        for mailbox in self.list_emails(customerid=customerid, loginname=loginname):
            mailbox_email = str(mailbox.get("email_full") or mailbox.get("email") or mailbox.get("emailaddr") or "").strip()
            if not mailbox_email:
                continue
            try:
                payload = self.call("EmailForwarders.listing", {"emailaddr": mailbox_email})
            except FroxlorApiError as exc:
                if strict:
                    raise
                logger.warning("EmailForwarders.listing failed for mailbox %s: %s", mailbox_email, exc)
                continue
            rows.extend(self._forwarder_rows_from_payload(payload, mailbox_email))
        return self._filter_customer_rows(rows, customerid, loginname)

    def _sender_rows_from_payload(self, payload: Any, mailbox_email: str) -> list[dict[str, Any]]:
        """Normalize EmailSender.listing rows: lowercase the allowed sender,
        drop empty rows, and stamp the owning mailbox."""
        rows: list[dict[str, Any]] = []
        for item in self._rows_from_payload(payload):
            allowed_sender = str(item.get("allowed_sender") or item.get("sender") or "").strip().lower()
            if not allowed_sender:
                continue
            rows.append({
                **item,
                "emailaddr": str(item.get("emailaddr") or item.get("email") or mailbox_email).strip().lower(),
                "email": str(item.get("email") or item.get("emailaddr") or mailbox_email).strip().lower(),
                "allowed_sender": allowed_sender,
            })
        return rows

    def list_email_senders(
        self,
        customerid: int | None = None,
        loginname: str | None = None,
        emailaddr: str | None = None,
        email_id: int | None = None,
        strict: bool = False,
    ) -> list[dict[str, Any]]:
        if emailaddr or email_id:
            params: dict[str, Any] = {}
            if emailaddr:
                params["emailaddr"] = emailaddr
            if email_id:
                params["id"] = email_id
            try:
                return self._rows_from_payload(self.call("EmailSender.listing", params))
            except FroxlorApiError as exc:
                if strict:
                    raise
                logger.warning("EmailSender.listing failed for %s: %s", emailaddr or email_id, exc)
                return []

        rows: list[dict[str, Any]] = []
        for mailbox in self.list_emails(customerid=customerid, loginname=loginname):
            mailbox_email = str(mailbox.get("email_full") or mailbox.get("email") or mailbox.get("emailaddr") or "").strip()
            if not mailbox_email:
                continue
            try:
                payload = self.call("EmailSender.listing", {"emailaddr": mailbox_email})
            except FroxlorApiError as exc:
                if strict:
                    raise
                logger.warning("EmailSender.listing failed for mailbox %s: %s", mailbox_email, exc)
                continue
            rows.extend(self._sender_rows_from_payload(payload, mailbox_email))
        return self._filter_customer_rows(rows, customerid, loginname)

    def list_domain_zones(
        self,
        domainname: str | None = None,
        domain_id: int | None = None,
        strict: bool = False,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {}
        if domainname:
            params["domainname"] = domainname
        if domain_id is not None:
            params["id"] = domain_id
        try:
            return self.listing("DomainZones.listing", params)
        except FroxlorApiError as exc:
            if strict:
                raise
            logger.warning("DomainZones.listing failed for %s: %s", domainname or domain_id, exc)
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
            # Rows that do not expose the filtered field at all are kept: some
            # listings (e.g. per-mailbox forwarders/senders) lack customerid but
            # are already scoped to the queried customer.
            if customerid is not None and row_customer_id not in (None, ""):
                if as_int(row_customer_id, default=-1) != as_int(customerid, default=-2):
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
