#!/usr/bin/env python3
"""Ensure an extra IP:port entry exists on a Froxlor panel.

Used to give source and target distinct secondary IPs so the migration's
--ip-map / zone-content rewrite path can be exercised end to end.
"""
from __future__ import annotations

import argparse
import base64
import json
from typing import Any

import requests


class ApiError(RuntimeError):
    pass


class FroxlorApi:
    def __init__(self, api_url: str, api_key: str, api_secret: str, timeout: int = 30) -> None:
        self.api_url = api_url
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout = timeout

    def _auth(self) -> str:
        raw = f"{self.api_key}:{self.api_secret}".encode()
        return base64.b64encode(raw).decode("ascii")

    def call(self, command: str, params: dict[str, Any] | None = None) -> Any:
        payload: dict[str, Any] = {"command": command}
        if params:
            payload["params"] = params
        resp = requests.post(
            self.api_url,
            headers={
                "Authorization": f"Basic {self._auth()}",
                "Content-Type": "application/json",
            },
            data=json.dumps(payload),
            timeout=self.timeout,
        )
        if resp.status_code >= 400:
            raise ApiError(f"{command} HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        if int(data.get("status", 200)) >= 400:
            raise ApiError(f"{command} failed: {data.get('status_message', 'unknown error')}")
        return data.get("data")

    def listing(self, command: str) -> list[dict[str, Any]]:
        merged: dict[str, Any] = {"sql_limit": 500, "sql_offset": 0}
        rows: list[dict[str, Any]] = []
        while True:
            payload = self.call(command, merged)
            if isinstance(payload, dict):
                chunk = payload.get("list") or []
                count = int(payload.get("count", len(chunk)))
            else:
                chunk = payload or []
                count = len(chunk)
            rows.extend(chunk)
            if not chunk or len(rows) >= count:
                break
            merged["sql_offset"] = int(merged["sql_offset"]) + len(chunk)
        return rows


def to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def ensure_ip_port(api: FroxlorApi, ip: str, port: int, ssl: bool = False) -> int:
    for row in api.listing("IpsAndPorts.listing"):
        if (
            str(row.get("ip") or "").strip() == ip
            and to_int(row.get("port"), 0) == port
            and to_int(row.get("ssl"), 0) == int(ssl)
        ):
            return to_int(row.get("id"), 0)
    payload: dict[str, Any] = {"ip": ip, "port": port}
    if ssl:
        payload["ssl"] = True
    api.call("IpsAndPorts.add", payload)
    for row in api.listing("IpsAndPorts.listing"):
        if (
            str(row.get("ip") or "").strip() == ip
            and to_int(row.get("port"), 0) == port
            and to_int(row.get("ssl"), 0) == int(ssl)
        ):
            return to_int(row.get("id"), 0)
    raise ApiError(f"Could not ensure IP:port {ip}:{port}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--api-secret", required=True)
    parser.add_argument("--ip", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--ssl", action="store_true", help="Create an SSL ip:port row")
    args = parser.parse_args()

    api = FroxlorApi(args.api_url, args.api_key, args.api_secret)
    ip_id = ensure_ip_port(api, args.ip, args.port, ssl=args.ssl)
    print(f"{args.ip}:{args.port} (ssl={int(args.ssl)}) -> id {ip_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
