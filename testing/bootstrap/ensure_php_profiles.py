#!/usr/bin/env python3
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


def _fpm_daemon_payload(version_label: str) -> dict[str, Any]:
    version = version_label.replace("php", "", 1)
    return {
        "description": version_label,
        "reload_cmd": f"service {version_label}-fpm restart",
        "config_dir": f"/etc/php/{version}/fpm/pool.d/",
        "pm": "dynamic",
        "max_children": 5,
        "start_servers": 2,
        "min_spare_servers": 1,
        "max_spare_servers": 3,
        "max_requests": 0,
        "idle_timeout": 10,
        "limit_extensions": ".php",
    }


def _ensure_fpm_daemon(api: FroxlorApi, version_label: str) -> int:
    rows = api.listing("FpmDaemons.listing")
    payload = _fpm_daemon_payload(version_label)
    wanted_desc = version_label.strip().lower()
    existing = next(
        (row for row in rows if str(row.get("description") or "").strip().lower() == wanted_desc),
        None,
    )
    if existing:
        api.call("FpmDaemons.update", {"id": to_int(existing.get("id"), 0), **payload})
    else:
        try:
            api.call("FpmDaemons.add", payload)
        except ApiError as exc:
            message = str(exc).lower()
            if "already exists" not in message:
                raise
            refreshed = api.listing("FpmDaemons.listing")
            collision = next(
                (
                    row
                    for row in refreshed
                    if str(row.get("reload_cmd") or "").strip().lower() == str(payload["reload_cmd"]).lower()
                    or str(row.get("config_dir") or "").strip().lower() == str(payload["config_dir"]).lower()
                ),
                None,
            )
            if not collision:
                raise
            api.call("FpmDaemons.update", {"id": to_int(collision.get("id"), 0), **payload})

    refreshed = api.listing("FpmDaemons.listing")
    for row in refreshed:
        if str(row.get("description") or "").strip().lower() == wanted_desc:
            daemon_id = to_int(row.get("id"), 0)
            if daemon_id > 0:
                return daemon_id
    raise ApiError(f"Failed to ensure FPM daemon profile '{version_label}'")


def ensure_profiles(api: FroxlorApi, names: list[str]) -> list[int]:
    rows = api.listing("PhpSettings.listing")
    if not rows:
        raise ApiError("No PHP settings available in Froxlor")

    fpm_ids = {name.lower(): _ensure_fpm_daemon(api, name) for name in names}

    base = rows[0]
    base_ini = str(base.get("phpsettings") or "memory_limit = 256M\nmax_execution_time = 60")

    by_desc: dict[str, dict[str, Any]] = {}
    for row in rows:
        desc = str(row.get("description") or "").strip().lower()
        if desc:
            by_desc[desc] = row

    for name in names:
        existing = by_desc.get(name.lower())
        payload: dict[str, Any] = {
            "description": name,
            "phpsettings": base_ini,
            "fpmconfig": fpm_ids[name.lower()],
        }
        if existing:
            api.call("PhpSettings.update", {"id": to_int(existing.get("id"), 0), **payload})
        else:
            api.call("PhpSettings.add", payload)

    refreshed = api.listing("PhpSettings.listing")
    refreshed_by_desc = {str(row.get("description") or "").strip().lower(): row for row in refreshed}

    result: list[int] = []
    for name in names:
        row = refreshed_by_desc.get(name.lower())
        if not row:
            raise ApiError(f"Failed to ensure PHP setting profile '{name}'")
        result.append(to_int(row.get("id")))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Ensure named PHP setting profiles exist")
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--api-secret", required=True)
    parser.add_argument("--profile", action="append", default=[])
    args = parser.parse_args()

    api = FroxlorApi(args.api_url, args.api_key, args.api_secret)
    profile_names = args.profile or ["php8.3", "php8.4"]
    ids = ensure_profiles(api, profile_names)
    print("Ensured PHP profiles:")
    for name, profile_id in zip(profile_names, ids):
        print(f"- {name}: id={profile_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
