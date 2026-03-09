from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from cachetools import LRUCache, cached


def froxlor_userdata_paths() -> list[str]:
    return [
        "/var/www/froxlor/lib/userdata.conf",
        "/var/www/froxlor/lib/userdata.inc.php",
        "/var/customers/webs/froxlor/lib/userdata.conf",
        "/var/customers/webs/froxlor/lib/userdata.inc.php",
        "/data/customers/webs/froxlor/lib/userdata.conf",
        "/data/customers/webs/froxlor/lib/userdata.inc.php",
        "/var/www/html/lib/userdata.conf",
        "/var/www/html/lib/userdata.inc.php",
        "/var/www/html/froxlor/lib/userdata.conf",
        "/var/www/html/froxlor/lib/userdata.inc.php",
    ]


def extract_sql_root_credentials(content: str) -> dict[str, str] | None:
    return _extract_credentials(content, "sql_root")


def extract_sql_credentials(content: str) -> dict[str, str] | None:
    return _extract_credentials(content, "sql")


def _extract_credentials(content: str, section: str) -> dict[str, str] | None:
    pairs: dict[str, str] = {}

    if section == "sql_root":
        indexed_pairs: dict[str, dict[str, str]] = {}
        for index, key, raw_value in re.findall(
            r"\$sql_root\s*\[\s*(\d+)\s*\]\s*\[\s*['\"]([A-Za-z0-9_]+)['\"]\s*\]\s*=\s*['\"]((?:\\.|[^'\"])*)['\"]\s*;",
            content,
        ):
            indexed_pairs.setdefault(index, {})[key] = raw_value
        if indexed_pairs:
            candidates = [item for item in indexed_pairs.values() if item.get("user", "").strip()]
            if candidates:
                pairs = max(candidates, key=_credential_score)
    else:
        for key, raw_value in re.findall(r"\$sql\s*\[\s*['\"]([A-Za-z0-9_]+)['\"]\s*\]\s*=\s*['\"]((?:\\.|[^'\"])*)['\"]\s*;", content):
            pairs[key] = raw_value

    if not pairs:
        body = _extract_php_array_body(content, section)
        if body:
            if section == "sql_root":
                # New Froxlor format stores root entries under index keys (e.g. '0' => [...]).
                root_entry = _extract_first_sql_root_entry(body)
                if root_entry:
                    body = root_entry
            for key in ("host", "port", "socket", "user", "password"):
                value = _extract_php_array_value(body, key)
                if value is not None:
                    pairs[key] = value
        elif section == "sql_root":
            # Keep legacy best-effort fallback for sql_root only.
            pairs = dict(re.findall(r"['\"]([A-Za-z0-9_]+)['\"]\s*=>\s*['\"]((?:\\.|[^'\"])*)['\"]", content))

    user = pairs.get("user", "").encode("utf-8").decode("unicode_escape").strip()
    password = pairs.get("password", "").encode("utf-8").decode("unicode_escape")
    host = pairs.get("host", "").encode("utf-8").decode("unicode_escape").strip() or "localhost"
    if not user:
        return None

    result = {"user": user, "password": password, "host": host}
    if "port" in pairs and pairs["port"].strip():
        result["port"] = pairs["port"].strip()
    if "socket" in pairs and pairs["socket"].strip():
        result["socket"] = pairs["socket"].strip()
    return result


def load_local_sql_root_credentials(paths: list[str] | None = None) -> dict[str, str]:
    return _load_local_credentials(paths, extract_sql_root_credentials, "sql_root")


def load_local_sql_credentials(paths: list[str] | None = None) -> dict[str, str]:
    return _load_local_credentials(paths, extract_sql_credentials, "sql", section_kind="panel")


def _credential_score(creds: dict[str, str]) -> int:
    score = 0
    if creds.get("password", ""):
        score += 2
    if creds.get("socket", ""):
        score += 1
    if creds.get("port", ""):
        score += 1
    return score


def _extract_php_array_body(content: str, section: str) -> str:
    match = re.search(rf"\${re.escape(section)}\s*=\s*\[(.*?)\];", content, flags=re.DOTALL)
    return match.group(1) if match else ""


def _extract_first_sql_root_entry(body: str) -> str:
    match = re.search(r"['\"]\d+['\"]\s*=>\s*\[(.*?)\]\s*(?:,|$)", body, flags=re.DOTALL)
    return match.group(1) if match else body


def _extract_php_array_value(body: str, key: str) -> str | None:
    # Single-quoted scalar.
    single = re.search(rf"['\"]{re.escape(key)}['\"]\s*=>\s*'((?:\\.|[^'])*)'\s*,?", body)
    if single:
        return single.group(1).encode("utf-8").decode("unicode_escape")
    # Double-quoted scalar.
    double = re.search(rf"['\"]{re.escape(key)}['\"]\s*=>\s*\"((?:\\.|[^\"])*)\"\s*,?", body)
    if double:
        return double.group(1).encode("utf-8").decode("unicode_escape")
    # HEREDOC/NOWDOC scalar.
    heredoc = re.search(
        rf"['\"]{re.escape(key)}['\"]\s*=>\s*<<<['\"]?([A-Za-z_][A-Za-z0-9_]*)['\"]?\s*\n(.*?)\n\1\s*,?",
        body,
        flags=re.DOTALL,
    )
    if heredoc:
        return heredoc.group(2)
    return None


_read_file_cache = LRUCache(maxsize=32)


@cached(_read_file_cache)
def _read_userdata_file(path: str) -> str:
    return Path(path).read_text(encoding="utf-8", errors="ignore")


def _load_local_credentials(
    paths: list[str] | None,
    extractor,
    section_name: str,
    section_kind: str = "generic",
) -> dict[str, str]:
    candidates = paths or froxlor_userdata_paths()
    found: list[dict[str, str]] = []
    for raw_path in candidates:
        path = Path(raw_path)
        if not path.exists() or not path.is_file():
            continue
        content = _read_userdata_file(str(path))
        creds = extractor(content)
        if creds:
            found.append(creds)
    if found:
        if section_kind == "panel":
            with_password = [item for item in found if item.get("password", "")]
            if with_password:
                found = with_password
            # Prefer non-root panel users for source panel DB access.
            non_root = [item for item in found if item.get("user", "").strip().lower() != "root"]
            if non_root:
                found = non_root
        return max(found, key=_credential_score)
    joined = ", ".join(candidates)
    raise RuntimeError(f"Could not parse {section_name} credentials from local froxlor userdata files: {joined}")


def connect_kwargs_from_credentials(creds: dict[str, str]) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "user": creds.get("user", ""),
        "password": creds.get("password", ""),
    }
    socket_path = creds.get("socket", "").strip()
    host = creds.get("host", "").strip() or "localhost"
    port_text = creds.get("port", "").strip()
    if socket_path:
        kwargs["unix_socket"] = socket_path
    else:
        kwargs["host"] = host
        kwargs["port"] = int(port_text) if port_text else 3306
    return kwargs


def mysql_defaults_content(creds: dict[str, str]) -> str:
    lines = ["[client]"]
    for key in ("user", "password", "host", "port", "socket"):
        value = creds.get(key, "")
        if value:
            lines.append(f"{key}={value}")
    return "\n".join(lines) + "\n"
