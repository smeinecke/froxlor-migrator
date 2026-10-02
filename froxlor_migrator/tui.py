from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from .api import FroxlorApiError, FroxlorClient
from .config import load_config
from .migrate import MigrationError, Migrator, Selection
from .plan import (
    batch_summary_row,
    build_batch_replay_command,
    build_clients,
    build_ip_mapping_tokens,
    build_php_mapping_tokens,
    build_replay_command,
    build_selection,
    collect_ip_mapping_candidates,
    collect_php_mapping_candidates,
    customer_selector_token,
    customer_selector_values,
    customer_view,
    discover_customer_resources,
    fetch_domain_zones,
    filter_mapping_to_rows,
    ip_aliases,
    ip_view,
    parse_mapping_arg,
    php_setting_aliases,
    plan_rows,
    resolve_named_mapping,
    select_customer_resources,
    select_customers_by_tokens,
    select_rows_by_tokens,
    unmatched_tokens,
)
from .transfer import TransferError, TransferRunner
from .util import as_int, pick, slugify

console = Console()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Froxlor full migration helper")
    parser.add_argument("--config", default="config.toml", help="Path to config TOML")
    parser.add_argument("--apply", action="store_true", help="Execute changes (default is dry-run)")
    parser.add_argument("--debug", action="store_true", help="Enable verbose manifest debug tracing")
    parser.add_argument("--non-interactive", action="store_true", help="Run without prompts; use defaults and CLI selections")
    parser.add_argument("--yes", action="store_true", help="Skip final confirmation prompt and start migration")
    parser.add_argument(
        "--source-customer",
        help="Source customer selector (id, login, name, or email; comma-separated for batch)",
    )
    parser.add_argument("--all-customers", action="store_true", help="Migrate every source customer (batch mode)")
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
    return parser.parse_args()


def _load_config_or_exit(path: str):
    try:
        return load_config(path)
    except Exception as exc:
        console.print(f"[red]Config error:[/red] {exc}")
        raise SystemExit(1) from exc


def _resolve_dry_run(args: argparse.Namespace, config) -> bool:
    if args.apply:
        return False
    return config.behavior.dry_run_default


def _parse_mapping_args(args: argparse.Namespace) -> tuple[dict[str, str], dict[str, str]]:
    try:
        return parse_mapping_arg(args.php_map, "--php-map"), parse_mapping_arg(args.ip_map, "--ip-map")
    except ValueError as exc:
        console.print(f"[red]Argument error:[/red] {exc}")
        raise SystemExit(1) from exc


def _resolve_source_customers(args: argparse.Namespace, customer_rows: list[dict]) -> list[dict]:
    """Headless source-customer resolution: --all-customers, --source-customer
    (comma-separated allowed), else the single available customer, else error."""
    if args.all_customers:
        if args.source_customer:
            console.print("[red]Use only one of --all-customers or --source-customer.[/red]")
            raise SystemExit(1)
        if not customer_rows:
            console.print("[red]No source customers found.[/red]")
            raise SystemExit(1)
        return [row["_raw"] for row in customer_rows]

    if args.source_customer:
        try:
            selected_rows = select_customers_by_tokens(customer_rows, args.source_customer, customer_selector_values, "source customer")
        except ValueError as exc:
            console.print(f"[red]Source customer selection error:[/red] {exc}")
            raise SystemExit(1) from exc
        if not selected_rows:
            console.print("[red]--source-customer resolved to no customers.[/red]")
            raise SystemExit(1)
        return [row["_raw"] for row in selected_rows]

    if len(customer_rows) != 1:
        console.print("[red]Non-interactive mode requires --source-customer when multiple source customers exist.[/red]")
        raise SystemExit(1)
    return [customer_rows[0]["_raw"]]


def _resolve_mode(args: argparse.Namespace) -> bool:
    if args.whole_customer:
        return True
    if args.domain_only:
        return False
    return True


def _resolve_target_customer(args: argparse.Namespace, target: FroxlorClient) -> dict[str, Any] | None:
    if not args.target_customer:
        console.print("[yellow]New customer will be created from source customer data.[/yellow]")
        return None
    try:
        target_customer_rows = customer_view(target.list_customers())
    except FroxlorApiError as exc:
        console.print(f"[red]API error while listing target customers:[/red] {exc}")
        raise SystemExit(1) from exc

    if args.target_customer.strip().lower() == "new":
        console.print("[yellow]New customer will be created from source customer data.[/yellow]")
        return None
    try:
        selected_target_rows = select_rows_by_tokens(
            target_customer_rows,
            args.target_customer,
            customer_selector_values,
            "target customer",
        )
    except ValueError as exc:
        console.print(f"[red]Target customer selection error:[/red] {exc}")
        raise SystemExit(1) from exc
    if len(selected_target_rows) != 1:
        console.print("[red]--target-customer must resolve to exactly one customer or 'new'.[/red]")
        raise SystemExit(1)
    target_customer = selected_target_rows[0]["_raw"]
    console.print(f"[green]Using existing target customer: {pick(target_customer, 'loginname', 'login', default='unknown')}[/green]")
    return target_customer


def _resource_selectors(args: argparse.Namespace) -> dict[str, str | None]:
    return {
        "domains": args.domains,
        "subdomains": args.subdomains,
        "databases": args.databases,
        "mailboxes": args.mailboxes,
        "ftp_accounts": args.ftp_accounts,
    }


def _build_ip_map(
    selected_domains: list[dict],
    target: FroxlorClient,
    preset_mapping: dict[str, str] | None = None,
    matched: dict[str, set[str]] | None = None,
) -> tuple[dict[int, int], list[dict], list[dict]]:
    source_ip_rows = collect_ip_mapping_candidates(selected_domains)
    if not source_ip_rows:
        return {}, [], []

    id_getter = lambda row: as_int(pick(row, "id", default=0))  # noqa: E731
    raw_mapping = preset_mapping or {}
    if matched is not None:
        # Record applicability before the no-target-IPs early return — tokens
        # that match source rows are "matched" even when nothing can apply.
        raw_mapping, _absent = filter_mapping_to_rows(raw_mapping, source_ip_rows, id_getter, ip_aliases)
        matched.setdefault("IP mapping", set()).update(raw_mapping)

    target_ips = target.listing("IpsAndPorts.listing")
    target_ip_rows = ip_view(target_ips)
    if not target_ip_rows:
        console.print("[yellow]No target IPs available via API, using Froxlor defaults.[/yellow]")
        return {}, source_ip_rows, []
    try:
        mapping = resolve_named_mapping(
            raw_mapping=raw_mapping,
            source_rows=source_ip_rows,
            source_value_getter=id_getter,
            source_alias_getter=ip_aliases,
            target_rows=target_ip_rows,
            target_value_getter=id_getter,
            target_alias_getter=ip_aliases,
            mapping_label="IP mapping",
        )
    except ValueError as exc:
        console.print(f"[red]Mapping/selection error:[/red] {exc}")
        raise SystemExit(1) from exc

    return mapping, source_ip_rows, target_ip_rows


def _build_php_setting_map(
    selected_domains: list[dict],
    source_settings: list[dict],
    target_settings: list[dict],
    preset_mapping: dict[str, str] | None = None,
    matched: dict[str, set[str]] | None = None,
) -> tuple[dict[int, int], list[dict]]:
    source_ids, source_rows, target_rows, default_map = collect_php_mapping_candidates(selected_domains, source_settings, target_settings)
    if not source_ids:
        return {}, []
    id_getter = lambda row: as_int(pick(row, "id", default=0))  # noqa: E731
    raw_mapping = preset_mapping or {}
    if matched is not None:
        raw_mapping, _absent = filter_mapping_to_rows(raw_mapping, source_rows, id_getter, php_setting_aliases)
        matched.setdefault("PHP mapping", set()).update(raw_mapping)
    try:
        mapping = resolve_named_mapping(
            raw_mapping=raw_mapping,
            source_rows=source_rows,
            source_value_getter=id_getter,
            source_alias_getter=php_setting_aliases,
            target_rows=target_rows,
            target_value_getter=id_getter,
            target_alias_getter=php_setting_aliases,
            mapping_label="PHP mapping",
        )
    except ValueError as exc:
        console.print(f"[red]Mapping/selection error:[/red] {exc}")
        raise SystemExit(1) from exc

    for source_id in source_ids:
        if source_id not in mapping:
            mapping[source_id] = default_map[source_id]
    return mapping, source_rows


def _resolve_include_flag(cli_value: str | None) -> bool:
    if cli_value is not None:
        return cli_value == "yes"
    return True


def _print_migration_plan(rows: list[tuple[str, str]]) -> None:
    plan = Table(title="Migration plan")
    plan.add_column("Item")
    plan.add_column("Count", justify="right")
    for item, value in rows:
        plan.add_row(item, value)
    console.print(plan)


def _execute_migration(migrator: Migrator, runner: TransferRunner, selection: Selection):
    """Run one selection under a progress display. Exceptions propagate — the
    caller decides whether to exit (single) or record-and-continue (batch)."""
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
            line = f"Progress {step}/{max(total, 1)}: {status}"
            if line != last_progress_line:
                console.print(line)
                last_progress_line = line

        migrator.set_progress_callback(_on_progress)
        context = migrator.execute(selection)
        progress.update(task_id, completed=progress.tasks[0].total, description="[green]Migration completed[/green]")
    return context


def _print_migration_result(context, runner: TransferRunner) -> None:
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


@dataclass
class _PlannedCustomer:
    """Everything needed to execute one customer's migration."""

    customer: dict
    sel: dict[str, Any]
    selection: Selection
    php_setting_map: dict[int, int]
    ip_mapping: dict[int, int]
    source_php_rows: list[dict]
    source_ip_rows: list[dict]
    target_ip_rows: list[dict]


def _include_flags(args: argparse.Namespace) -> dict[str, bool]:
    return {
        "files": _resolve_include_flag(args.include_files),
        "databases": _resolve_include_flag(args.include_databases),
        "mail": _resolve_include_flag(args.include_mail),
        "certificates": not args.skip_certificates,
        "domain_zones": not args.skip_dns_zones,
        "password_sync": not args.skip_password_sync,
        "forwarders": not args.skip_forwarders,
        "sender_aliases": not args.skip_sender_aliases,
        "subdomains": not args.skip_subdomains,
        "validate_db_names": not args.skip_database_name_validation,
    }


def _plan_customer(
    args: argparse.Namespace,
    config,
    source: FroxlorClient,
    target: FroxlorClient,
    customer: dict,
    *,
    migrate_whole_customer: bool,
    target_customer: dict | None,
    php_mapping_arg: dict[str, str],
    ip_mapping_arg: dict[str, str],
    source_php_settings: list[dict],
    target_php_settings: list[dict],
    includes: dict[str, bool],
    matched: dict[str, set[str]] | None,
) -> _PlannedCustomer | None:
    """Discover + select + map + build the Selection for one customer.

    Returns ``None`` when the customer ends up with no selected domains in
    batch (tolerant) mode — in strict mode an empty/unmatched selection raises
    via SystemExit instead.
    """
    login = str(pick(customer, "loginname", "login", default=""))
    customer_id = as_int(pick(customer, "customerid", "id", default=0))

    try:
        resources = discover_customer_resources(source, customer_id, login)
    except FroxlorApiError as exc:
        console.print(f"[red]API discovery error for {login}:[/red] {exc}")
        raise SystemExit(1) from exc

    try:
        sel = select_customer_resources(
            resources,
            whole_customer=migrate_whole_customer,
            source_web_root=config.paths.source_web_root,
            selectors=_resource_selectors(args),
            strict=matched is None,
            matched=matched,
        )
    except ValueError as exc:
        console.print(f"[red]Selection error for {login}:[/red] {exc}")
        raise SystemExit(1) from exc
    if sel["skipped_out_root"]:
        console.print(f"[yellow]Skipped {sel['skipped_out_root']} domain(s) outside source_web_root {config.paths.source_web_root}[/yellow]")

    if not sel["domains"] and matched is not None:
        return None

    if includes["domain_zones"]:
        sel["domain_zones"], zone_errors = fetch_domain_zones(source, sel["domain_names"])
        for error in zone_errors:
            console.print(f"[yellow]Skipping DNS zone for {error}[/yellow]")
    else:
        sel["domain_zones"] = []

    try:
        php_setting_map, source_php_rows = _build_php_setting_map(
            sel["domains"] + sel["subdomains"],
            source_php_settings,
            target_php_settings,
            preset_mapping=php_mapping_arg,
            matched=matched,
        )
        ip_mapping, source_ip_rows, target_ip_rows = _build_ip_map(
            sel["domains"],
            target,
            preset_mapping=ip_mapping_arg,
            matched=matched,
        )
    except FroxlorApiError as exc:
        console.print(f"[red]IP discovery/mapping error for {login}:[/red] {exc}")
        raise SystemExit(1) from exc

    selection = build_selection(
        customer=customer,
        target_customer=target_customer,
        domains=sel["domains"],
        subdomains=sel["subdomains"],
        databases=sel["databases"],
        mailboxes=sel["mailboxes"],
        email_forwarders=sel["forwarders"],
        email_senders=sel["sender_aliases"],
        ftp_accounts=sel["ftps"],
        ssh_keys=sel["ssh_keys"],
        data_dumps=sel["data_dumps"],
        dir_protections=sel["dir_protections"],
        dir_options=sel["dir_options"],
        domain_zones=sel["domain_zones"],
        include_files=includes["files"],
        include_databases=includes["databases"],
        include_mail=includes["mail"],
        include_subdomains=includes["subdomains"],
        validate_database_names=includes["validate_db_names"],
        php_setting_map=php_setting_map,
        ip_mapping=ip_mapping,
        include_certificates=includes["certificates"],
        include_domain_zones=includes["domain_zones"],
        include_password_sync=includes["password_sync"],
        include_forwarders=includes["forwarders"],
        include_sender_aliases=includes["sender_aliases"],
    )
    return _PlannedCustomer(
        customer=customer,
        sel=sel,
        selection=selection,
        php_setting_map=php_setting_map,
        ip_mapping=ip_mapping,
        source_php_rows=source_php_rows,
        source_ip_rows=source_ip_rows,
        target_ip_rows=target_ip_rows,
    )


def _check_unmatched_tokens(args: argparse.Namespace, matched: dict[str, set[str]], php_mapping_arg: dict[str, str], ip_mapping_arg: dict[str, str]) -> None:
    """Fail the batch if a selector/mapping token matched no customer at all —
    per-customer misses are fine (tolerant), a global miss is a typo."""
    missing_parts = unmatched_tokens(
        {
            "domain": args.domains,
            "subdomain": args.subdomains,
            "database": args.databases,
            "mailbox": args.mailboxes,
            "FTP account": args.ftp_accounts,
        },
        matched,
        php_mapping=php_mapping_arg,
        ip_mapping=ip_mapping_arg,
    )
    if missing_parts:
        console.print(f"[red]Selector/mapping tokens matched no customer:[/red] {'; '.join(missing_parts)}")
        raise SystemExit(1)


def _print_batch_plan(planned: list[_PlannedCustomer], includes: dict[str, bool], migrate_whole_customer: bool, dry_run: bool) -> None:
    plan = Table(title=f"Batch migration plan — {len(planned)} customers")
    for column in ("Customer", "Domains", "Subdomains", "Databases", "Mailboxes", "FTP", "Zone recs"):
        plan.add_column(column)
    for item in planned:
        plan.add_row(*batch_summary_row(item.customer, item.sel))
    console.print(plan)
    flags = Table(title="Shared options")
    flags.add_column("Option")
    flags.add_column("Value")
    flags.add_row("Mode", "whole-customer" if migrate_whole_customer else "domain-only")
    for key, value in includes.items():
        flags.add_row(key, "yes" if value else "no")
    flags.add_row("Dry-run", "yes" if dry_run else "no")
    console.print(flags)


def _plan_all_customers(
    args: argparse.Namespace,
    config,
    source: FroxlorClient,
    target: FroxlorClient,
    selected_customers: list[dict],
    *,
    migrate_whole_customer: bool,
    target_customer: dict | None,
    php_mapping_arg: dict[str, str],
    ip_mapping_arg: dict[str, str],
    source_php_settings: list[dict],
    target_php_settings: list[dict],
    includes: dict[str, bool],
    batch: bool,
) -> list[_PlannedCustomer]:
    matched: dict[str, set[str]] | None = {} if batch else None
    planned: list[_PlannedCustomer] = []
    for customer in selected_customers:
        login = str(pick(customer, "loginname", "login", default=""))
        if batch:
            console.print(f"[bold]Planning {login}…[/bold]")
        item = _plan_customer(
            args,
            config,
            source,
            target,
            customer,
            migrate_whole_customer=migrate_whole_customer,
            target_customer=target_customer,
            php_mapping_arg=php_mapping_arg,
            ip_mapping_arg=ip_mapping_arg,
            source_php_settings=source_php_settings,
            target_php_settings=target_php_settings,
            includes=includes,
            matched=matched,
        )
        if item is None:
            console.print(f"[yellow]Skipping {login}: no matching domains.[/yellow]")
            continue
        planned.append(item)

    if matched is not None:
        _check_unmatched_tokens(args, matched, php_mapping_arg, ip_mapping_arg)
    if not planned:
        console.print("[red]Nothing to migrate: no customer produced a selection.[/red]")
        raise SystemExit(1)
    return planned


def _print_single_plan_and_replay(
    args: argparse.Namespace,
    item: _PlannedCustomer,
    *,
    migrate_whole_customer: bool,
    target_customer: dict | None,
    target_php_settings: list[dict],
    includes: dict[str, bool],
    dry_run: bool,
) -> None:
    _print_migration_plan(
        plan_rows(
            selected_domains=item.sel["domains"],
            selected_subdomains=item.sel["subdomains"],
            selected_databases=item.sel["databases"],
            selected_mailboxes=item.sel["mailboxes"],
            selected_forwarders=item.sel["forwarders"],
            selected_sender_aliases=item.sel["sender_aliases"],
            selected_ftps=item.sel["ftps"],
            selected_ssh_keys=item.sel["ssh_keys"],
            selected_data_dumps=item.sel["data_dumps"],
            selected_dir_protections=item.sel["dir_protections"],
            selected_dir_options=item.sel["dir_options"],
            selected_domain_zones=item.sel["domain_zones"],
            migrate_whole_customer=migrate_whole_customer,
            php_setting_map=item.php_setting_map,
            ip_mapping=item.ip_mapping,
            include_files=includes["files"],
            include_databases=includes["databases"],
            include_mail=includes["mail"],
            include_certificates=includes["certificates"],
            include_domain_zones=includes["domain_zones"],
            include_password_sync=includes["password_sync"],
            include_forwarders=includes["forwarders"],
            include_sender_aliases=includes["sender_aliases"],
            include_subdomains=includes["subdomains"],
            validate_database_names=includes["validate_db_names"],
            debug=args.debug,
            dry_run=dry_run,
        )
    )
    replay_command = build_replay_command(
        config_path=args.config,
        apply=args.apply,
        debug=args.debug,
        migrate_whole_customer=migrate_whole_customer,
        selected_customer=item.customer,
        target_customer=target_customer,
        selected_domains=item.sel["domains"],
        selected_subdomains=item.sel["subdomains"],
        selected_databases=item.sel["databases"],
        selected_mailboxes=item.sel["mailboxes"],
        selected_ftps=item.sel["ftps"],
        php_mapping_tokens=build_php_mapping_tokens(item.php_setting_map, item.source_php_rows, target_php_settings),
        ip_mapping_tokens=build_ip_mapping_tokens(item.ip_mapping, item.source_ip_rows, item.target_ip_rows),
        include_files=includes["files"],
        include_databases=includes["databases"],
        include_mail=includes["mail"],
        include_certificates=includes["certificates"],
        include_domain_zones=includes["domain_zones"],
        include_password_sync=includes["password_sync"],
        include_forwarders=includes["forwarders"],
        include_sender_aliases=includes["sender_aliases"],
        skip_subdomains=args.skip_subdomains,
        skip_database_name_validation=args.skip_database_name_validation,
    )
    console.print("[bold]Replay command (same selection, non-interactive):[/bold]")
    console.print(replay_command)


def _print_batch_plan_and_replay(
    args: argparse.Namespace,
    planned: list[_PlannedCustomer],
    *,
    migrate_whole_customer: bool,
    target_customer: dict | None,
    includes: dict[str, bool],
    dry_run: bool,
) -> None:
    _print_batch_plan(planned, includes, migrate_whole_customer, dry_run)
    replay_command = build_batch_replay_command(
        config_path=args.config,
        apply=args.apply,
        debug=args.debug,
        migrate_whole_customer=migrate_whole_customer,
        customer_tokens=[customer_selector_token(item.customer) for item in planned],
        all_customers=bool(args.all_customers),
        target_customer=target_customer,
        domains_arg=args.domains,
        subdomains_arg=args.subdomains,
        databases_arg=args.databases,
        mailboxes_arg=args.mailboxes,
        ftp_accounts_arg=args.ftp_accounts,
        php_map_arg=args.php_map,
        ip_map_arg=args.ip_map,
        include_files=includes["files"],
        include_databases=includes["databases"],
        include_mail=includes["mail"],
        include_certificates=includes["certificates"],
        include_domain_zones=includes["domain_zones"],
        include_password_sync=includes["password_sync"],
        include_forwarders=includes["forwarders"],
        include_sender_aliases=includes["sender_aliases"],
        skip_subdomains=args.skip_subdomains,
        skip_database_name_validation=args.skip_database_name_validation,
    )
    console.print("[bold]Replay command (same batch, non-interactive):[/bold]")
    console.print(replay_command)


def _execute_planned(
    args: argparse.Namespace,
    config,
    source: FroxlorClient,
    target: FroxlorClient,
    planned: list[_PlannedCustomer],
    *,
    batch: bool,
    dry_run: bool,
) -> None:
    results: list[tuple[str, str, Any, str]] = []  # (login, status, context, manifest)
    for index, item in enumerate(planned):
        login = str(pick(item.customer, "loginname", "login", default="customer"))
        if batch:
            console.print(f"[bold cyan]━━━ [{index + 1}/{len(planned)}] {login} ━━━[/bold cyan]")
        manifest_name = slugify(f"{login}-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{index}")
        runner = TransferRunner(config=config, dry_run=dry_run, manifest_name=manifest_name, debug=args.debug)
        migrator = Migrator(config=config, source=source, target=target, runner=runner)
        try:
            context = _execute_migration(migrator, runner, item.selection)
        except (MigrationError, FroxlorApiError, TransferError) as exc:
            console.print(f"[red]Migration failed for {login}:[/red] {exc}")
            console.print(f"Manifest: {runner.manifest_path}")
            if not batch:
                raise SystemExit(1) from exc
            results.append((login, f"failed: {exc}", None, str(runner.manifest_path)))
            continue
        except Exception as exc:  # noqa: BLE001 — batch must record + continue, not strand later customers
            console.print(f"[red]Unexpected failure for {login}:[/red] {exc}")
            console.print(f"Manifest: {runner.manifest_path}")
            if not batch:
                raise
            results.append((login, f"failed: {exc}", None, str(runner.manifest_path)))
            continue
        results.append((login, "ok", context, str(runner.manifest_path)))
        if not batch:
            _print_migration_result(context, runner)

    if not batch:
        return
    summary = Table(title="Batch result")
    summary.add_column("Customer")
    summary.add_column("Status")
    summary.add_column("Target id")
    summary.add_column("Manifest")
    for login, status, context, manifest in results:
        target_id = str(getattr(context, "target_customer_id", "") or "") if context else ""
        style = "green" if status == "ok" else "red"
        summary.add_row(login, f"[{style}]{status}[/{style}]", target_id, manifest)
    console.print(summary)
    if any(status != "ok" for _login, status, _ctx, _m in results):
        raise SystemExit(1)


def _run_headless(args: argparse.Namespace, config) -> None:
    dry_run = _resolve_dry_run(args, config)
    source, target = build_clients(config)

    console.print("[bold]Froxlor Migrator[/bold]")
    console.print(f"Mode: {'[yellow]dry-run[/yellow]' if dry_run else '[green]apply[/green]'}")
    if args.debug:
        console.print("Debug: [green]enabled[/green]")

    php_mapping_arg, ip_mapping_arg = _parse_mapping_args(args)

    try:
        customers = source.list_customers()
        source_php_settings = source.list_php_settings()
        target_php_settings = target.list_php_settings()
    except FroxlorApiError as exc:
        console.print(f"[red]API error:[/red] {exc}")
        raise SystemExit(1) from exc

    selected_customers = _resolve_source_customers(args, customer_view(customers))
    batch = len(selected_customers) > 1

    migrate_whole_customer = _resolve_mode(args)
    target_customer = None
    if not migrate_whole_customer:
        target_customer = _resolve_target_customer(args, target)

    includes = _include_flags(args)

    planned = _plan_all_customers(
        args,
        config,
        source,
        target,
        selected_customers,
        migrate_whole_customer=migrate_whole_customer,
        target_customer=target_customer,
        php_mapping_arg=php_mapping_arg,
        ip_mapping_arg=ip_mapping_arg,
        source_php_settings=source_php_settings,
        target_php_settings=target_php_settings,
        includes=includes,
        batch=batch,
    )

    if batch:
        _print_batch_plan_and_replay(
            args,
            planned,
            migrate_whole_customer=migrate_whole_customer,
            target_customer=target_customer,
            includes=includes,
            dry_run=dry_run,
        )
    else:
        _print_single_plan_and_replay(
            args,
            planned[0],
            migrate_whole_customer=migrate_whole_customer,
            target_customer=target_customer,
            target_php_settings=target_php_settings,
            includes=includes,
            dry_run=dry_run,
        )

    _execute_planned(args, config, source, target, planned, batch=batch, dry_run=dry_run)


def run_app() -> None:
    args = _parse_args()

    if args.debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )

    config = _load_config_or_exit(args.config)

    if args.domain_only and args.whole_customer:
        console.print("[red]Use only one of --domain-only or --whole-customer.[/red]")
        raise SystemExit(1)

    if args.non_interactive:
        _run_headless(args, config)
        return

    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        console.print("[red]Interactive wizard requires a TTY. Use --non-interactive for scripted runs.[/red]")
        raise SystemExit(2)

    from .wizard import run_wizard

    run_wizard(config=config, args=args)
