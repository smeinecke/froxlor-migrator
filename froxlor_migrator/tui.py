from __future__ import annotations

import argparse
import logging
import shlex
from collections.abc import Callable
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.prompt import Confirm, Prompt
from rich.table import Table

from .api import FroxlorApiError, FroxlorClient
from .config import load_config
from .migrate import MigrationError, Migrator, Selection
from .transfer import TransferError, TransferRunner
from .util import as_int, parse_multi_select, pick, slugify

console = Console()


def _split_csv(raw: str | None) -> list[str]:
    if raw is None:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def _parse_mapping_arg(raw: str | None, arg_name: str) -> dict[str, str]:
    if raw is None or not raw.strip():
        return {}
    mapping: dict[str, str] = {}
    for entry in _split_csv(raw):
        if "=>" in entry:
            left, right = entry.split("=>", 1)
        elif "=" in entry:
            left, right = entry.split("=", 1)
        else:
            raise ValueError(f"Invalid {arg_name} value '{entry}': expected source=>target")
        source_token = left.strip().lower()
        target_token = right.strip().lower()
        if not source_token or not target_token:
            raise ValueError(f"Invalid {arg_name} value '{entry}': empty source/target token")
        mapping[source_token] = target_token
    return mapping


def _build_value_index(rows: list[dict], value_getter: Callable[[dict], int], alias_getter: Callable[[dict], list[str]]) -> dict[str, set[int]]:
    index: dict[str, set[int]] = {}
    for row in rows:
        value = value_getter(row)
        if value <= 0:
            continue
        for alias in alias_getter(row):
            key = alias.strip().lower()
            if not key:
                continue
            index.setdefault(key, set()).add(value)
    return index


def _resolve_named_mapping(
    raw_mapping: dict[str, str],
    source_rows: list[dict],
    source_value_getter: Callable[[dict], int],
    source_alias_getter: Callable[[dict], list[str]],
    target_rows: list[dict],
    target_value_getter: Callable[[dict], int],
    target_alias_getter: Callable[[dict], list[str]],
    mapping_label: str,
) -> dict[int, int]:
    if not raw_mapping:
        return {}
    source_index = _build_value_index(source_rows, source_value_getter, source_alias_getter)
    target_index = _build_value_index(target_rows, target_value_getter, target_alias_getter)
    resolved: dict[int, int] = {}
    for source_token, target_token in raw_mapping.items():
        source_matches = source_index.get(source_token, set())
        if not source_matches:
            raise ValueError(f"{mapping_label} contains unknown source token '{source_token}'")
        if len(source_matches) > 1:
            raise ValueError(f"{mapping_label} source token '{source_token}' is ambiguous")
        target_matches = target_index.get(target_token, set())
        if not target_matches:
            raise ValueError(f"{mapping_label} contains unknown target token '{target_token}'")
        if len(target_matches) > 1:
            raise ValueError(f"{mapping_label} target token '{target_token}' is ambiguous")
        resolved[next(iter(source_matches))] = next(iter(target_matches))
    return resolved


def _php_setting_aliases(row: dict) -> list[str]:
    setting_id = as_int(pick(row, "id", default=0))
    description = str(pick(row, "description", default="")).strip()
    binary = str(pick(row, "binary", default="")).strip()
    aliases = [
        str(setting_id) if setting_id > 0 else "",
        description,
        binary,
        f"{description}|{binary}" if description or binary else "",
    ]
    return aliases


def _ip_aliases(row: dict) -> list[str]:
    ip_id = as_int(pick(row, "id", default=0))
    ip = str(pick(row, "ip", default="")).strip()
    port = as_int(pick(row, "port", default=0))
    ssl = as_int(pick(row, "ssl", default=0))
    aliases = [
        str(ip_id) if ip_id > 0 else "",
        f"{ip}:{port}",
        f"{ip}:{port}:{ssl}",
    ]
    return aliases


def _build_php_mapping_tokens(resolved_map: dict[int, int], source_settings: list[dict], target_settings: list[dict]) -> dict[str, str]:
    if not resolved_map:
        return {}
    source_by_id = {as_int(pick(row, "id", default=0)): row for row in source_settings}
    target_by_id = {as_int(pick(row, "id", default=0)): row for row in target_settings}
    tokens: dict[str, str] = {}
    for source_id, target_id in sorted(resolved_map.items()):
        source_row = source_by_id.get(source_id)
        target_row = target_by_id.get(target_id)
        if source_row is None or target_row is None:
            continue
        source_desc = str(pick(source_row, "description", default="")).strip()
        source_bin = str(pick(source_row, "binary", default="")).strip()
        target_desc = str(pick(target_row, "description", default="")).strip()
        target_bin = str(pick(target_row, "binary", default="")).strip()
        source_token = f"{source_desc}|{source_bin}".strip("|").lower()
        target_token = f"{target_desc}|{target_bin}".strip("|").lower()
        if source_token and target_token:
            tokens[source_token] = target_token
    return tokens


def _build_ip_mapping_tokens(resolved_map: dict[int, int], source_rows: list[dict], target_rows: list[dict]) -> dict[str, str]:
    if not resolved_map:
        return {}
    source_by_id = {as_int(pick(row, "id", default=0)): row for row in source_rows}
    target_by_id = {as_int(pick(row, "id", default=0)): row for row in target_rows}
    tokens: dict[str, str] = {}
    for source_id, target_id in sorted(resolved_map.items()):
        source_row = source_by_id.get(source_id)
        target_row = target_by_id.get(target_id)
        if source_row is None or target_row is None:
            continue
        source_ip = str(pick(source_row, "ip", default="")).strip()
        source_port = as_int(pick(source_row, "port", default=0))
        source_ssl = as_int(pick(source_row, "ssl", default=0))
        target_ip = str(pick(target_row, "ip", default="")).strip()
        target_port = as_int(pick(target_row, "port", default=0))
        target_ssl = as_int(pick(target_row, "ssl", default=0))
        source_token = f"{source_ip}:{source_port}:{source_ssl}".lower()
        target_token = f"{target_ip}:{target_port}:{target_ssl}".lower()
        if source_ip and target_ip:
            tokens[source_token] = target_token
    return tokens


def _select_rows_by_tokens(
    rows: list[dict],
    selectors_raw: str | None,
    selector_values: Callable[[dict], list[str]],
    selector_label: str,
) -> list[dict]:
    if selectors_raw is None:
        return rows
    tokens = {token.lower() for token in _split_csv(selectors_raw)}
    if not tokens:
        return rows
    if "all" in tokens:
        return rows
    if "none" in tokens:
        return []

    selected: list[dict] = []
    unresolved = set(tokens)
    for row in rows:
        values = {value.strip().lower() for value in selector_values(row) if value and value.strip()}
        if values & tokens:
            selected.append(row)
            unresolved -= values & tokens
    if unresolved:
        missing = ", ".join(sorted(unresolved))
        raise ValueError(f"Unknown {selector_label} selector(s): {missing}")
    return selected


def _build_replay_command(
    args: argparse.Namespace,
    selected_customer: dict[str, Any],
    target_customer: dict[str, Any] | None,
    migrate_whole_customer: bool,
    selected_domains: list[dict],
    selected_subdomains: list[dict],
    selected_databases: list[dict],
    selected_mailboxes: list[dict],
    selected_ftps: list[dict],
    php_mapping_tokens: dict[str, str],
    ip_mapping_tokens: dict[str, str],
    include_files: bool,
    include_databases: bool,
    include_mail: bool,
    include_certificates: bool,
    include_domain_zones: bool,
    include_password_sync: bool,
    include_forwarders: bool,
    include_sender_aliases: bool,
    debug: bool,
) -> str:
    parts: list[str] = ["uv", "run", "python", "main.py", "--config", args.config, "--non-interactive", "--yes"]
    if args.apply:
        parts.append("--apply")
    if debug:
        parts.append("--debug")
    if migrate_whole_customer:
        parts.append("--whole-customer")
    else:
        parts.append("--domain-only")

    source_customer_id = as_int(pick(selected_customer, "customerid", "id", default=0))
    source_customer_login = str(pick(selected_customer, "loginname", "login", default="")).strip()
    parts.extend(["--source-customer", str(source_customer_id) if source_customer_id > 0 else source_customer_login])

    if not migrate_whole_customer:
        if target_customer is None:
            parts.extend(["--target-customer", "new"])
        else:
            target_customer_id = as_int(pick(target_customer, "customerid", "id", default=0))
            target_customer_login = str(pick(target_customer, "loginname", "login", default="")).strip()
            parts.extend(["--target-customer", str(target_customer_id) if target_customer_id > 0 else target_customer_login])

    domain_names = _dedupe_keep_order([str(pick(row, "domain", "domainname", default="")).strip() for row in selected_domains])
    subdomain_names = _dedupe_keep_order([str(pick(row, "domain", "domainname", default="")).strip() for row in selected_subdomains])
    database_names = _dedupe_keep_order([str(pick(row, "databasename", "dbname", default="")).strip() for row in selected_databases])
    mailbox_names = _dedupe_keep_order([str(pick(row, "email_full", "email", "emailaddr", default="")).strip() for row in selected_mailboxes])
    ftp_names = _dedupe_keep_order([str(pick(row, "username", "ftpuser", default="")).strip() for row in selected_ftps])

    parts.extend(["--domains", ",".join(domain_names) if domain_names else "none"])
    parts.extend(["--subdomains", ",".join(subdomain_names) if subdomain_names else "none"])
    parts.extend(["--databases", ",".join(database_names) if database_names else "none"])
    parts.extend(["--mailboxes", ",".join(mailbox_names) if mailbox_names else "none"])
    parts.extend(["--ftp-accounts", ",".join(ftp_names) if ftp_names else "none"])

    if php_mapping_tokens:
        php_map_value = ",".join(f"{source_token}=>{target_token}" for source_token, target_token in sorted(php_mapping_tokens.items()))
        parts.extend(["--php-map", php_map_value])
    if ip_mapping_tokens:
        ip_map_value = ",".join(f"{source_token}=>{target_token}" for source_token, target_token in sorted(ip_mapping_tokens.items()))
        parts.extend(["--ip-map", ip_map_value])

    parts.extend(["--include-files", "yes" if include_files else "no"])
    parts.extend(["--include-databases", "yes" if include_databases else "no"])
    parts.extend(["--include-mail", "yes" if include_mail else "no"])

    if args.skip_subdomains:
        parts.append("--skip-subdomains")
    if args.skip_database_name_validation:
        parts.append("--skip-database-name-validation")
    if not include_certificates:
        parts.append("--skip-certificates")
    if not include_domain_zones:
        parts.append("--skip-dns-zones")
    if not include_password_sync:
        parts.append("--skip-password-sync")
    if not include_forwarders:
        parts.append("--skip-forwarders")
    if not include_sender_aliases:
        parts.append("--skip-sender-aliases")

    return " ".join(shlex.quote(part) for part in parts)


def _render_table(title: str, rows: list[dict], cols: list[tuple[str, str]]) -> None:
    table = Table(title=title)
    table.add_column("#", justify="right")
    for _, label in cols:
        table.add_column(label)
    for idx, row in enumerate(rows, start=1):
        values = [str(idx)]
        for key, _ in cols:
            values.append(str(row.get(key, "")))
        table.add_row(*values)
    console.print(table)


def _choose_rows(title: str, rows: list[dict], cols: list[tuple[str, str]], multi: bool = False, allow_empty: bool = False) -> list[dict]:
    if not rows:
        return []
    _render_table(title, rows, cols)

    if multi:
        while True:
            default = "none" if allow_empty else "all"
            raw = Prompt.ask(f"Select entries (e.g. 1,2,5-7, {default})", default=default)
            if allow_empty and raw == "none":
                return []
            try:
                indices = parse_multi_select(raw, len(rows))
                return [rows[i] for i in indices]
            except ValueError:
                console.print("[red]Invalid selection[/red]")

    while True:
        if allow_empty:
            default = "new"
            prompt_text = f"Select entry number (or {default} to create new)"
            raw = Prompt.ask(prompt_text, default=default)
        else:
            prompt_text = "Select entry number"
            raw = Prompt.ask(prompt_text)

        if allow_empty and raw == "new":
            return []

        try:
            idx = int(raw)
            if 1 <= idx <= len(rows):
                return [rows[idx - 1]]
        except ValueError:
            pass
        console.print("[red]Invalid selection[/red]")


def _customer_view(customers: list[dict]) -> list[dict]:
    view = []
    for item in customers:
        view.append({
            "id": as_int(pick(item, "customerid", "id", default=0)),
            "login": pick(item, "loginname", "login", default=""),
            "name": pick(item, "name", "company", default=""),
            "email": pick(item, "email", default=""),
            "_raw": item,
        })
    return view


def _domain_view(domains: list[dict]) -> list[dict]:
    view = []
    for item in domains:
        view.append({
            "domain": pick(item, "domain", "domainname", default=""),
            "docroot": pick(item, "documentroot", default=""),
            "ssl": pick(item, "sslenabled", default=""),
            "php": as_int(pick(item, "phpsettingid", default=0)),
            "_raw": item,
        })
    return view


def _db_view(dbs: list[dict]) -> list[dict]:
    view = []
    for item in dbs:
        view.append({
            "dbname": pick(item, "databasename", "dbname", default=""),
            "description": pick(item, "description", default=""),
            "server": pick(item, "mysql_server", "dbserver", default=""),
            "_raw": item,
        })
    return view


def _subdomain_view(rows: list[dict]) -> list[dict]:
    view = []
    for item in rows:
        view.append({
            "domain": pick(item, "domain", "domainname", default=""),
            "path": pick(item, "path", "documentroot", default=""),
            "ssl": pick(item, "sslenabled", "ssl_enabled", default=""),
            "_raw": item,
        })
    return view


def _ftp_view(rows: list[dict]) -> list[dict]:
    view = []
    for item in rows:
        view.append({
            "username": pick(item, "username", "ftpuser", default=""),
            "path": pick(item, "path", default=""),
            "login": pick(item, "login_enabled", default=""),
            "_raw": item,
        })
    return view


def _mail_view(emails: list[dict], selected_domains: set[str]) -> list[dict]:
    view = []
    for item in emails:
        email = str(pick(item, "email_full", "email", "emailaddr", default="")).lower()
        domain = email.split("@", 1)[1] if "@" in email else ""
        if selected_domains and domain not in selected_domains:
            continue
        view.append({"email": email, "domain": domain, "_raw": item})
    return view


def _php_settings_view(settings: list[dict]) -> list[dict]:
    view = []
    for item in settings:
        view.append({
            "id": as_int(pick(item, "id", default=0)),
            "description": pick(item, "description", default=""),
            "binary": pick(item, "binary", default=""),
            "_raw": item,
        })
    return view


def _ip_view(ip_rows: list[dict]) -> list[dict]:
    view = []
    for item in ip_rows:
        view.append({
            "id": as_int(pick(item, "id", default=0)),
            "ip": str(pick(item, "ip", default="")),
            "port": as_int(pick(item, "port", default=0)),
            "ssl": as_int(pick(item, "ssl", default=0)),
            "_raw": item,
        })
    return view


def _domain_in_source_root(domain: dict, source_root: str) -> bool:
    docroot = str(pick(domain, "documentroot", default="")).strip()
    root = source_root.rstrip("/")
    return bool(docroot.startswith(root + "/") or docroot == root)


def _build_ip_map(
    selected_domains: list[dict],
    target: FroxlorClient,
    preset_mapping: dict[str, str] | None = None,
    non_interactive: bool = False,
) -> tuple[dict[int, int], list[dict], list[dict]]:
    source_ips: dict[int, dict] = {}
    for domain in selected_domains:
        for ip in pick(domain, "ipsandports", default=[]) or []:
            ip_id = as_int(pick(ip, "id", default=0))
            if ip_id > 0 and ip_id not in source_ips:
                source_ips[ip_id] = ip

    if not source_ips:
        return {}, [], []

    target_ips = target.listing("IpsAndPorts.listing")
    target_ip_rows = _ip_view(target_ips)
    if not target_ip_rows:
        console.print("[yellow]No target IPs available via API, using Froxlor defaults.[/yellow]")
        return {}, list(source_ips.values()), []

    _render_table(
        "Target IP/Port entries",
        target_ip_rows,
        cols=[("id", "ID"), ("ip", "IP"), ("port", "Port"), ("ssl", "SSL")],
    )

    source_ip_rows = list(source_ips.values())
    valid_target_ids = {row["id"] for row in target_ip_rows}
    mapping = _resolve_named_mapping(
        raw_mapping=preset_mapping or {},
        source_rows=source_ip_rows,
        source_value_getter=lambda row: as_int(pick(row, "id", default=0)),
        source_alias_getter=_ip_aliases,
        target_rows=target_ip_rows,
        target_value_getter=lambda row: as_int(pick(row, "id", default=0)),
        target_alias_getter=_ip_aliases,
        mapping_label="IP mapping",
    )

    if non_interactive:
        return mapping, source_ip_rows, target_ip_rows

    for source_id, source_ip in sorted(source_ips.items()):
        if source_id in mapping:
            continue
        source_label = f"{pick(source_ip, 'ip', default='?')}:{pick(source_ip, 'port', default='?')} ssl={pick(source_ip, 'ssl', default=0)}"
        raw = Prompt.ask(
            f"Map source IP id {source_id} ({source_label}) to target id (empty=use Froxlor default)",
            default="",
            show_default=False,
        ).strip()
        if not raw:
            continue
        try:
            target_id = int(raw)
        except ValueError:
            console.print(f"[yellow]Invalid IP id '{raw}', using Froxlor default for source id {source_id}[/yellow]")
            continue
        if target_id not in valid_target_ids:
            console.print(f"[yellow]Target IP id {target_id} not found, using Froxlor default for source id {source_id}[/yellow]")
            continue
        mapping[source_id] = target_id

    return mapping, source_ip_rows, target_ip_rows


def _build_php_setting_map(
    selected_domains: list[dict],
    source_settings: list[dict],
    target_settings: list[dict],
    preset_mapping: dict[str, str] | None = None,
    non_interactive: bool = False,
) -> tuple[dict[int, int], list[dict]]:
    source_ids = sorted({as_int(pick(item, "phpsettingid", default=0)) for item in selected_domains if as_int(pick(item, "phpsettingid", default=0)) > 0})
    if not source_ids:
        return {}, []

    target_rows = _php_settings_view(target_settings)
    if not target_rows:
        raise ValueError("No target PHP settings found")

    _render_table(
        "Target PHP settings",
        target_rows,
        cols=[("id", "ID"), ("description", "Description"), ("binary", "Binary")],
    )
    source_rows = [row for row in source_settings if as_int(pick(row, "id", default=0)) in source_ids]
    valid_target_ids = {row["id"] for row in target_rows}
    default_target_id = target_rows[0]["id"]
    mapping = _resolve_named_mapping(
        raw_mapping=preset_mapping or {},
        source_rows=source_rows,
        source_value_getter=lambda row: as_int(pick(row, "id", default=0)),
        source_alias_getter=_php_setting_aliases,
        target_rows=target_rows,
        target_value_getter=lambda row: as_int(pick(row, "id", default=0)),
        target_alias_getter=_php_setting_aliases,
        mapping_label="PHP mapping",
    )

    if non_interactive:
        for source_id in source_ids:
            if source_id in mapping:
                continue
            mapping[source_id] = source_id if source_id in valid_target_ids else default_target_id
        return mapping, source_rows

    for source_id in source_ids:
        if source_id in mapping:
            continue
        guessed = source_id if source_id in valid_target_ids else default_target_id
        while True:
            raw = Prompt.ask(f"Map source PHP setting id {source_id} to target id", default=str(guessed))
            try:
                chosen = int(raw)
            except ValueError:
                console.print("[red]Please enter a numeric target PHP setting id[/red]")
                continue
            if chosen not in valid_target_ids:
                console.print("[red]Target PHP setting id not found[/red]")
                continue
            mapping[source_id] = chosen
            break
    return mapping, source_rows


def run_app() -> None:
    parser = argparse.ArgumentParser(description="Froxlor full migration helper")
    parser.add_argument("--config", default="config.toml", help="Path to config TOML")
    parser.add_argument("--apply", action="store_true", help="Execute changes (default is dry-run)")
    parser.add_argument("--debug", action="store_true", help="Enable verbose manifest debug tracing")
    parser.add_argument("--non-interactive", action="store_true", help="Run without prompts; use defaults and CLI selections")
    parser.add_argument("--yes", action="store_true", help="Skip final confirmation prompt and start migration")
    parser.add_argument("--source-customer", help="Source customer selector (id, login, name, or email)")
    parser.add_argument("--target-customer", help="Target customer selector for domain-only mode (id, login, name, email, or 'new')")
    parser.add_argument("--domain-only", action="store_true", help="Disable whole-customer mode")
    parser.add_argument("--whole-customer", action="store_true", help="Enable whole-customer mode")
    parser.add_argument("--domains", help="Selected domains (comma-separated names, 'all', or 'none')")
    parser.add_argument("--subdomains", help="Selected subdomains (comma-separated names, 'all', or 'none')")
    parser.add_argument("--databases", help="Selected databases (comma-separated names, 'all', or 'none')")
    parser.add_argument("--mailboxes", help="Selected mailboxes (comma-separated addresses, 'all', or 'none')")
    parser.add_argument("--ftp-accounts", help="Selected FTP accounts (comma-separated usernames, 'all', or 'none')")
    parser.add_argument(
        "--php-map",
        help="Source to target PHP mapping (source=>target, comma-separated; use description|binary names)",
    )
    parser.add_argument(
        "--ip-map",
        help="Source to target IP mapping (source=>target, comma-separated; use ip:port:ssl names)",
    )
    parser.add_argument("--include-files", choices=["yes", "no"], help="Transfer website files")
    parser.add_argument("--include-databases", choices=["yes", "no"], help="Transfer database schema+data")
    parser.add_argument("--include-mail", choices=["yes", "no"], help="Transfer mailbox content via doveadm backup")
    parser.add_argument("--skip-subdomains", action="store_true", help="Skip subdomain creation/update on target")
    parser.add_argument("--skip-certificates", action="store_true", help="Skip certificate migration")
    parser.add_argument("--skip-dns-zones", action="store_true", help="Skip custom DNS zone migration")
    parser.add_argument("--skip-password-sync", action="store_true", help="Skip password hash synchronization")
    parser.add_argument("--skip-forwarders", action="store_true", help="Skip email forwarder migration")
    parser.add_argument("--skip-sender-aliases", action="store_true", help="Skip sender alias migration")
    parser.add_argument(
        "--skip-database-name-validation",
        action="store_true",
        help="Allow source/target database names to differ after Mysqls.add",
    )
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )

    try:
        config = load_config(args.config)
    except Exception as exc:
        console.print(f"[red]Config error:[/red] {exc}")
        return

    dry_run = not args.apply
    if args.apply:
        dry_run = False
    elif config.behavior.dry_run_default:
        dry_run = True

    source = FroxlorClient(
        api_url=config.source.api_url,
        api_key=config.source.api_key,
        api_secret=config.source.api_secret,
        timeout_seconds=config.source.timeout_seconds,
    )
    target = FroxlorClient(
        api_url=config.target.api_url,
        api_key=config.target.api_key,
        api_secret=config.target.api_secret,
        timeout_seconds=config.target.timeout_seconds,
    )

    console.print("[bold]Froxlor Migrator[/bold]")
    console.print(f"Mode: {'[yellow]dry-run[/yellow]' if dry_run else '[green]apply[/green]'}")
    if args.debug:
        console.print("Debug: [green]enabled[/green]")
    if args.domain_only and args.whole_customer:
        console.print("[red]Use only one of --domain-only or --whole-customer.[/red]")
        return

    try:
        php_mapping_arg = _parse_mapping_arg(args.php_map, "--php-map")
        ip_mapping_arg = _parse_mapping_arg(args.ip_map, "--ip-map")
    except ValueError as exc:
        console.print(f"[red]Argument error:[/red] {exc}")
        return

    try:
        customers = source.list_customers()
    except FroxlorApiError as exc:
        console.print(f"[red]API error while listing customers:[/red] {exc}")
        return

    customer_rows = _customer_view(customers)
    selected_customer: dict[str, Any] | None = None
    if args.source_customer:
        selected_rows = _select_rows_by_tokens(
            customer_rows,
            args.source_customer,
            lambda row: [str(row.get("id", "")), str(row.get("login", "")), str(row.get("name", "")), str(row.get("email", ""))],
            "source customer",
        )
        if len(selected_rows) != 1:
            console.print("[red]--source-customer must resolve to exactly one customer.[/red]")
            return
        selected_customer = selected_rows[0]["_raw"]
    elif args.non_interactive:
        if len(customer_rows) != 1:
            console.print("[red]Non-interactive mode requires --source-customer when multiple source customers exist.[/red]")
            return
        selected_customer = customer_rows[0]["_raw"]
    else:
        selected_customer_row = _choose_rows(
            "Source customers",
            customer_rows,
            cols=[("id", "ID"), ("login", "Login"), ("name", "Name"), ("email", "Email")],
            multi=False,
        )
        if not selected_customer_row:
            console.print("[yellow]No customer selected.[/yellow]")
            return
        selected_customer = selected_customer_row[0]["_raw"]
    if not selected_customer:
        console.print("[yellow]No customer selected.[/yellow]")
        return
    customer_id = as_int(pick(selected_customer, "customerid", "id", default=0))
    customer_login = str(pick(selected_customer, "loginname", "login", default=""))

    try:
        domains = source.list_domains(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        subdomains = source.list_subdomains(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        dbs = source.list_mysqls(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        emails = source.list_emails(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        ftps = source.list_ftps(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        forwarders = source.list_email_forwarders(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        sender_aliases = source.list_email_senders(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        dir_protections = source.list_dir_protections(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        dir_options = source.list_dir_options(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        ssh_keys = source.list_ssh_keys(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        data_dumps = source.list_data_dumps(customerid=customer_id if customer_id else None, loginname=customer_login or None)
        source_php_settings = source.list_php_settings()
        target_php_settings = target.list_php_settings()
    except FroxlorApiError as exc:
        console.print(f"[red]API discovery error:[/red] {exc}")
        return

    if args.whole_customer:
        migrate_whole_customer = True
    elif args.domain_only:
        migrate_whole_customer = False
    elif args.non_interactive:
        migrate_whole_customer = True
    else:
        migrate_whole_customer = Confirm.ask(
            "Migrate whole customer (all domains, webspace, databases, mailboxes and settings)",
            default=True,
        )

    # For domain-only migration, let user select target customer
    target_customer = None
    if not migrate_whole_customer:
        try:
            target_customers = target.list_customers()
            target_customer_rows = _customer_view(target_customers)

            if args.target_customer:
                if args.target_customer.strip().lower() == "new":
                    target_customer = None
                    console.print("[yellow]New customer will be created from source customer data.[/yellow]")
                else:
                    selected_target_rows = _select_rows_by_tokens(
                        target_customer_rows,
                        args.target_customer,
                        lambda row: [str(row.get("id", "")), str(row.get("login", "")), str(row.get("name", "")), str(row.get("email", ""))],
                        "target customer",
                    )
                    if len(selected_target_rows) != 1:
                        console.print("[red]--target-customer must resolve to exactly one customer or 'new'.[/red]")
                        return
                    target_customer = selected_target_rows[0]["_raw"]
                    console.print(f"[green]Using existing target customer: {pick(target_customer, 'loginname', 'login', default='unknown')}[/green]")
            elif target_customer_rows and not args.non_interactive:
                selected_target_row = _choose_rows(
                    "Target customers",
                    target_customer_rows,
                    cols=[("id", "ID"), ("login", "Login"), ("name", "Name"), ("email", "Email")],
                    multi=False,
                    allow_empty=True,
                )

                if selected_target_row:
                    target_customer = selected_target_row[0]["_raw"]
                    console.print(f"[green]Using existing target customer: {pick(target_customer, 'loginname', 'login', default='unknown')}[/green]")
                else:
                    console.print("[yellow]New customer will be created from source customer data.[/yellow]")
            else:
                console.print("[yellow]New customer will be created from source customer data.[/yellow]")
        except FroxlorApiError as exc:
            console.print(f"[red]API error while listing target customers:[/red] {exc}")
            return

    if migrate_whole_customer:
        selected_domains = [d for d in domains if _domain_in_source_root(d, config.paths.source_web_root)]
        skipped = len(domains) - len(selected_domains)
        if skipped > 0:
            console.print(f"[yellow]Skipped {skipped} domain(s) outside source_web_root {config.paths.source_web_root}[/yellow]")
        try:
            selected_domains = _select_rows_by_tokens(
                selected_domains,
                args.domains,
                lambda row: [str(pick(row, "domain", "domainname", default=""))],
                "domain",
            )
        except ValueError as exc:
            console.print(f"[red]Domain selection error:[/red] {exc}")
            return
    else:
        if args.domains is not None:
            try:
                selected_domains = _select_rows_by_tokens(
                    domains,
                    args.domains,
                    lambda row: [str(pick(row, "domain", "domainname", default=""))],
                    "domain",
                )
            except ValueError as exc:
                console.print(f"[red]Domain selection error:[/red] {exc}")
                return
        elif args.non_interactive:
            selected_domains = domains
        else:
            selected_domain_rows = _choose_rows(
                "Source domains",
                _domain_view(domains),
                cols=[
                    ("domain", "Domain"),
                    ("docroot", "Documentroot"),
                    ("php", "PHP setting"),
                    ("ssl", "SSL"),
                ],
                multi=True,
            )
            selected_domains = [x["_raw"] for x in selected_domain_rows]

    selected_domain_names = {str(pick(domain, "domain", "domainname", default="")).lower() for domain in selected_domains}
    selected_subdomains = [
        item for item in subdomains if str(pick(item, "domain", "domainname", default="")).split(".", 1)[-1].lower() in selected_domain_names
    ]

    if migrate_whole_customer:
        try:
            selected_databases = _select_rows_by_tokens(
                dbs,
                args.databases,
                lambda row: [str(pick(row, "databasename", "dbname", default=""))],
                "database",
            )
            selected_mailboxes = _select_rows_by_tokens(
                emails,
                args.mailboxes,
                lambda row: [str(pick(row, "email_full", "email", "emailaddr", default=""))],
                "mailbox",
            )
            selected_ftps = _select_rows_by_tokens(
                ftps,
                args.ftp_accounts,
                lambda row: [str(pick(row, "username", "ftpuser", default=""))],
                "FTP account",
            )
            selected_subdomains = _select_rows_by_tokens(
                selected_subdomains,
                args.subdomains,
                lambda row: [str(pick(row, "domain", "domainname", default=""))],
                "subdomain",
            )
        except ValueError as exc:
            console.print(f"[red]Selection error:[/red] {exc}")
            return
        selected_dir_protections = dir_protections
        selected_dir_options = dir_options
        selected_ssh_keys = ssh_keys
        selected_data_dumps = data_dumps
    else:
        if args.databases is not None:
            try:
                selected_databases = _select_rows_by_tokens(
                    dbs,
                    args.databases,
                    lambda row: [str(pick(row, "databasename", "dbname", default=""))],
                    "database",
                )
            except ValueError as exc:
                console.print(f"[red]Database selection error:[/red] {exc}")
                return
        elif dbs and not args.non_interactive:
            selected_db_rows = _choose_rows(
                "Customer databases (separate selection, optional - press Enter for none)",
                _db_view(dbs),
                cols=[("dbname", "DB Name"), ("description", "Description"), ("server", "Server")],
                multi=True,
                allow_empty=True,
            )
            selected_databases = [x["_raw"] for x in selected_db_rows]
        else:
            selected_databases = []
            if not dbs:
                console.print("[yellow]No databases found for this customer.[/yellow]")

        mailbox_candidates = _mail_view(emails, selected_domain_names)
        if args.mailboxes is not None:
            try:
                selected_mailbox_rows = _select_rows_by_tokens(
                    mailbox_candidates,
                    args.mailboxes,
                    lambda row: [str(row.get("email", ""))],
                    "mailbox",
                )
            except ValueError as exc:
                console.print(f"[red]Mailbox selection error:[/red] {exc}")
                return
        elif args.non_interactive:
            selected_mailbox_rows = mailbox_candidates
        else:
            selected_mailbox_rows = _choose_rows(
                "Mailboxes for selected domains",
                mailbox_candidates,
                cols=[("email", "Mailbox"), ("domain", "Domain")],
                multi=True,
            )
        selected_mailboxes = [x.get("_raw", x) for x in selected_mailbox_rows]

        if args.subdomains is not None:
            try:
                selected_subdomains = _select_rows_by_tokens(
                    selected_subdomains,
                    args.subdomains,
                    lambda row: [str(pick(row, "domain", "domainname", default=""))],
                    "subdomain",
                )
            except ValueError as exc:
                console.print(f"[red]Subdomain selection error:[/red] {exc}")
                return
        elif not args.non_interactive:
            selected_subdomain_rows = _choose_rows(
                "Subdomains",
                _subdomain_view(selected_subdomains),
                cols=[("domain", "Subdomain"), ("path", "Path"), ("ssl", "SSL")],
                multi=True,
            )
            selected_subdomains = [x["_raw"] for x in selected_subdomain_rows]

        if args.ftp_accounts is not None:
            try:
                selected_ftps = _select_rows_by_tokens(
                    ftps,
                    args.ftp_accounts,
                    lambda row: [str(pick(row, "username", "ftpuser", default=""))],
                    "FTP account",
                )
            except ValueError as exc:
                console.print(f"[red]FTP selection error:[/red] {exc}")
                return
        elif ftps and not args.non_interactive:
            selected_ftp_rows = _choose_rows(
                "FTP accounts (optional - press Enter for none)",
                _ftp_view(ftps),
                cols=[("username", "Username"), ("path", "Path"), ("login", "Login")],
                multi=True,
                allow_empty=True,
            )
            selected_ftps = [x["_raw"] for x in selected_ftp_rows]
        else:
            selected_ftps = []
            if not ftps:
                console.print("[yellow]No FTP accounts found for this customer.[/yellow]")
        selected_dir_protections = dir_protections
        selected_dir_options = dir_options
        selected_ssh_keys = ssh_keys
        selected_data_dumps = data_dumps

    mailbox_names = {str(pick(item, "email_full", "email", "emailaddr", default="")).strip().lower() for item in selected_mailboxes}
    selected_forwarders = [item for item in forwarders if str(pick(item, "email", "emailaddr", default="")).strip().lower() in mailbox_names]
    selected_sender_aliases = [item for item in sender_aliases if str(pick(item, "email", "emailaddr", default="")).strip().lower() in mailbox_names]

    include_certificates = not args.skip_certificates
    include_domain_zones = not args.skip_dns_zones
    include_password_sync = not args.skip_password_sync
    include_forwarders = not args.skip_forwarders
    include_sender_aliases = not args.skip_sender_aliases

    selected_domain_zones: list[dict] = []
    if include_domain_zones:
        for domain_name in sorted(selected_domain_names):
            try:
                selected_domain_zones.extend(source.list_domain_zones(domainname=domain_name))
            except FroxlorApiError:
                continue

    try:
        php_setting_map, source_selected_php_settings = _build_php_setting_map(
            selected_domains,
            source_php_settings,
            target_php_settings,
            preset_mapping=php_mapping_arg,
            non_interactive=args.non_interactive,
        )
        ip_mapping, source_ip_rows, target_ip_rows = _build_ip_map(
            selected_domains,
            target,
            preset_mapping=ip_mapping_arg,
            non_interactive=args.non_interactive,
        )
    except ValueError as exc:
        console.print(f"[red]Mapping/selection error:[/red] {exc}")
        return
    except FroxlorApiError as exc:
        console.print(f"[red]IP discovery/mapping error:[/red] {exc}")
        return

    if args.include_files is not None:
        include_files = args.include_files == "yes"
    elif args.non_interactive:
        include_files = True
    else:
        include_files = Confirm.ask("Include website files", default=True)

    if args.include_databases is not None:
        include_databases = args.include_databases == "yes"
    elif args.non_interactive:
        include_databases = True
    else:
        include_databases = Confirm.ask("Include database schema+data", default=True)

    if args.include_mail is not None:
        include_mail = args.include_mail == "yes"
    elif args.non_interactive:
        include_mail = True
    else:
        include_mail = Confirm.ask("Include mailbox content via doveadm backup", default=True)

    plan = Table(title="Migration plan")
    plan.add_column("Item")
    plan.add_column("Count", justify="right")
    plan.add_row("Customer", "1")
    plan.add_row("Domains", str(len(selected_domains)))
    plan.add_row("Subdomains", str(0 if args.skip_subdomains else len(selected_subdomains)))
    plan.add_row("Databases", str(len(selected_databases) if include_databases else 0))
    plan.add_row("Mailboxes", str(len(selected_mailboxes) if include_mail else 0))
    plan.add_row("Mail forwarders", str(len(selected_forwarders) if include_forwarders else 0))
    plan.add_row("Sender aliases", str(len(selected_sender_aliases) if include_sender_aliases else 0))
    plan.add_row("FTP accounts", str(len(selected_ftps)))
    plan.add_row("SSH keys", str(len(selected_ssh_keys)))
    plan.add_row("Data dumps", str(len(selected_data_dumps)))
    plan.add_row("Dir protections", str(len(selected_dir_protections)))
    plan.add_row("Dir options", str(len(selected_dir_options)))
    plan.add_row("Domain zone records", str(len(selected_domain_zones)))
    plan.add_row("Whole customer mode", "yes" if migrate_whole_customer else "no")
    plan.add_row("PHP mappings", str(len(php_setting_map)))
    plan.add_row("Mapped IP entries", str(len(ip_mapping)))
    plan.add_row("Certificates", "yes" if include_certificates else "no")
    plan.add_row("Domain zone sync", "yes" if include_domain_zones else "no")
    plan.add_row("Password hash sync", "yes" if include_password_sync else "no")
    plan.add_row("Forwarders", "yes" if include_forwarders else "no")
    plan.add_row("Sender aliases sync", "yes" if include_sender_aliases else "no")
    plan.add_row("Files transfer", "yes" if include_files else "no")
    plan.add_row("Validate DB names", "no" if args.skip_database_name_validation else "yes")
    plan.add_row("Debug tracing", "yes" if args.debug else "no")
    plan.add_row("Dry-run", "yes" if dry_run else "no")
    console.print(plan)

    php_mapping_tokens = _build_php_mapping_tokens(php_setting_map, source_selected_php_settings, target_php_settings)
    ip_mapping_tokens = _build_ip_mapping_tokens(ip_mapping, source_ip_rows, target_ip_rows)
    replay_command = _build_replay_command(
        args=args,
        selected_customer=selected_customer,
        target_customer=target_customer,
        migrate_whole_customer=migrate_whole_customer,
        selected_domains=selected_domains,
        selected_subdomains=selected_subdomains,
        selected_databases=selected_databases,
        selected_mailboxes=selected_mailboxes,
        selected_ftps=selected_ftps,
        php_mapping_tokens=php_mapping_tokens,
        ip_mapping_tokens=ip_mapping_tokens,
        include_files=include_files,
        include_databases=include_databases,
        include_mail=include_mail,
        include_certificates=include_certificates,
        include_domain_zones=include_domain_zones,
        include_password_sync=include_password_sync,
        include_forwarders=include_forwarders,
        include_sender_aliases=include_sender_aliases,
        debug=args.debug,
    )
    console.print("[bold]Replay command (same selection, non-interactive):[/bold]")
    console.print(replay_command)

    if not args.yes and not args.non_interactive:
        if not Confirm.ask("Start migration", default=True):
            console.print("[yellow]Cancelled.[/yellow]")
            return

    manifest_name = slugify(f"{pick(selected_customer, 'loginname', 'login', default='customer')}-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
    runner = TransferRunner(config=config, dry_run=dry_run, manifest_name=manifest_name, debug=args.debug)
    migrator = Migrator(config=config, source=source, target=target, runner=runner)

    selection = Selection(
        customer=selected_customer,
        target_customer=target_customer,
        domains=selected_domains,
        subdomains=selected_subdomains,
        databases=selected_databases,
        mailboxes=selected_mailboxes,
        email_forwarders=selected_forwarders,
        email_senders=selected_sender_aliases,
        ftp_accounts=selected_ftps,
        ssh_keys=selected_ssh_keys,
        data_dumps=selected_data_dumps,
        dir_protections=selected_dir_protections,
        dir_options=selected_dir_options,
        domain_zones=selected_domain_zones,
        include_files=include_files,
        include_databases=include_databases,
        include_mail=include_mail,
        include_subdomains=not args.skip_subdomains,
        validate_database_names=not args.skip_database_name_validation,
        php_setting_map=php_setting_map,
        ip_mapping=ip_mapping,
        include_certificates=include_certificates,
        include_domain_zones=include_domain_zones,
        include_password_sync=include_password_sync,
        include_forwarders=include_forwarders,
        include_sender_aliases=include_sender_aliases,
    )

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Starting migration", total=1)
            last_progress_line: str | None = None

            def _on_progress(step: int, total: int, status: str) -> None:
                nonlocal last_progress_line
                progress.update(task_id, total=max(total, 1), completed=step, description=f"[cyan]{status}[/cyan]")
                runner.progress_event(step, max(total, 1), status)
                if args.non_interactive:
                    line = f"Progress {step}/{max(total, 1)}: {status}"
                    if line != last_progress_line:
                        console.print(line)
                        last_progress_line = line

            migrator.set_progress_callback(_on_progress)
            context = migrator.execute(selection)
            progress.update(task_id, completed=progress.tasks[0].total, description="[green]Migration completed[/green]")
    except (MigrationError, FroxlorApiError, TransferError) as exc:
        console.print(f"[red]Migration failed:[/red] {exc}")
        console.print(f"Manifest: {runner.manifest_path}")
        return

    console.print("[green]Migration completed.[/green]")
    console.print(f"Target customer id: {context.target_customer_id}")
    if context.source_to_target_db:
        db_table = Table(title="Database mapping")
        db_table.add_column("Source DB")
        db_table.add_column("Target DB")
        for source_db, target_db in sorted(context.source_to_target_db.items()):
            db_table.add_row(source_db, target_db)
        console.print(db_table)
    console.print(f"Manifest: {runner.manifest_path}")
