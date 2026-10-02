"""Textual-based interactive migration wizard.

This is the default interactive front-end (``froxlor-migrator`` on a TTY).
It shares its planning logic with the headless CLI via ``plan.py`` — the
screens only collect user decisions and turn them into a ``Selection``.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, cast

from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, VerticalScroll
from textual.screen import Screen
from textual.widgets import (
    Button,
    Checkbox,
    DataTable,
    Footer,
    Header,
    Label,
    LoadingIndicator,
    ProgressBar,
    RadioButton,
    RadioSet,
    RichLog,
    Select,
    SelectionList,
    Static,
    TabbedContent,
    TabPane,
    TextArea,
)
from textual.widgets.selection_list import Selection as Item
from textual.worker import Worker, WorkerState

from .api import FroxlorApiError, FroxlorClient
from .config import AppConfig
from .migrate import MigrationError, Migrator
from .migration.types import MigrationContext, ResourceRow, Selection
from .plan import (
    build_clients,
    build_ip_mapping_tokens,
    build_php_mapping_tokens,
    build_replay_command,
    build_selection,
    collect_ip_mapping_candidates,
    collect_php_mapping_candidates,
    customer_selector_values,
    customer_view,
    db_view,
    derive_scoped_resources,
    discover_customer_resources,
    domain_in_source_root,
    domain_view,
    fetch_domain_zones,
    ftp_view,
    ip_aliases,
    ip_view,
    mail_view,
    narrow_subdomains,
    parse_mapping_arg,
    php_setting_aliases,
    php_settings_view,
    plan_rows,
    resolve_named_mapping,
    select_rows_by_tokens,
    subdomain_view,
)
from .transfer import TransferRunner
from .util import as_int, domain_name, ftp_username, mailbox_address, pick, slugify

TARGET_NEW = "new"
IP_DEFAULT = 0


@dataclass
class WizardState:
    """All user decisions live here so back navigation can rebuild screens."""

    config: AppConfig
    args: argparse.Namespace
    dry_run: bool

    source: FroxlorClient | None = None
    target: FroxlorClient | None = None
    customers: list[dict] = field(default_factory=list)
    target_customers: list[dict] = field(default_factory=list)
    source_php_settings: list[dict] = field(default_factory=list)
    target_php_settings: list[dict] = field(default_factory=list)
    target_ip_rows: list[dict] = field(default_factory=list)

    customer: ResourceRow | None = None
    resources: dict[str, list[dict]] = field(default_factory=dict)
    whole_customer: bool = True
    target_customer: ResourceRow | None = None

    domains: list[ResourceRow] | None = None
    subdomains: list[ResourceRow] | None = None
    databases: list[ResourceRow] | None = None
    mailboxes: list[ResourceRow] | None = None
    ftps: list[ResourceRow] | None = None

    php_map: dict[int, int] = field(default_factory=dict)
    ip_map: dict[int, int] = field(default_factory=dict)

    includes: dict[str, bool] = field(default_factory=dict)
    domain_zones: list[ResourceRow] = field(default_factory=list)
    zone_errors: list[str] = field(default_factory=list)

    selection: Selection | None = None
    replay_command: str = ""
    summary_rows: list[tuple[str, str]] = field(default_factory=list)
    context: MigrationContext | None = None
    runner: TransferRunner | None = None

    def needs_mappings(self) -> bool:
        if self.php_map or self.ip_map:
            return True
        domains = self.domains or []
        if collect_ip_mapping_candidates(domains) and self.target_ip_rows:
            return True
        if not (self.source_php_settings and self.target_php_settings):
            return False
        try:
            source_ids, _, _, _ = collect_php_mapping_candidates(domains + (self.subdomains or []), self.source_php_settings, self.target_php_settings)
        except ValueError:
            return False
        return bool(source_ids)


def _yes_no(value: str | None) -> bool:
    return value != "no"


class MigratorWizardApp(App):
    ENABLE_COMMAND_PALETTE = False
    CSS = """
    #title { text-style: bold; margin-bottom: 1; }
    #subtitle { color: $text-muted; margin-bottom: 1; }
    .nav { height: auto; margin-top: 1; dock: bottom; }
    .error { color: $error; }
    .note { color: $warning; }
    SelectionList { max-height: 16; }
    DataTable { max-height: 20; }
    Select { margin-bottom: 1; }
    #log { height: 12; border: solid $primary; }
    #replay { height: 5; }
    """

    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit"),
        Binding("escape", "back", "Back"),
    ]

    def __init__(
        self,
        *,
        config: AppConfig,
        args: argparse.Namespace,
        clients: tuple[FroxlorClient, FroxlorClient] | None = None,
        migrator_cls: type[Migrator] = Migrator,
        runner_cls: type[TransferRunner] = TransferRunner,
    ) -> None:
        super().__init__()
        dry_run = config.behavior.dry_run_default and not args.apply
        self.state = WizardState(config=config, args=args, dry_run=dry_run)
        if clients is not None:
            self.state.source, self.state.target = clients
        self.migrator_cls = migrator_cls
        self.runner_cls = runner_cls
        self.state.whole_customer = not args.domain_only

    def on_mount(self) -> None:
        self.push_screen(ConnectScreen())

    def action_back(self) -> None:
        if len(self.screen_stack) > 1 and not getattr(self.screen, "block_back", False):
            self.pop_screen()

    def advance_past(self, screen: Screen) -> None:
        """Push the screen after `screen`, skipping steps that don't apply."""
        order: list[type[Screen]] = [
            ConnectScreen,
            CustomerScreen,
            ModeScreen,
            DomainsScreen,
            ResourcesScreen,
            MappingsScreen,
            OptionsScreen,
            PlanScreen,
        ]
        idx = order.index(type(screen)) + 1
        for cls in order[idx:]:
            if cls is MappingsScreen and not self.state.needs_mappings():
                continue
            self.push_screen(cls())
            return


class WizardScreen(Screen):
    """Base class typing ``self.app`` as the wizard app.

    ``stash()`` persists tentative widget state into ``state`` so a screen
    rebuilt after back navigation restores the user's earlier toggles.
    """

    BINDINGS = [Binding("escape", "back")]

    block_back = False

    @property
    def app(self) -> MigratorWizardApp:
        return cast(MigratorWizardApp, super().app)

    @property
    def state(self) -> WizardState:
        return self.app.state

    def stash(self) -> None:
        """Save uncommitted widget state; overridden by screens with toggles."""

    def action_back(self) -> None:
        if self.block_back:
            return
        self.stash()
        self.app.pop_screen()

    def advance(self) -> None:
        self.app.advance_past(self)


class ConnectScreen(WizardScreen):
    block_back = True  # first screen — nothing to go back to

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Froxlor Migrator", id="title")
        yield Label("Connecting to source and target panels…", id="subtitle")
        yield LoadingIndicator(id="spinner")
        yield Static("", id="connect-error", classes="error")
        yield Footer()
        with Horizontal(classes="nav"):
            yield Button("Retry", id="retry")
            yield Button("Quit", id="quit")

    def on_mount(self) -> None:
        self.query_one("#retry", Button).display = False
        self.run_worker(self._load, thread=True, exit_on_error=False)

    def _load(self) -> None:
        state = self.state
        if state.source is None or state.target is None:
            state.source, state.target = build_clients(state.config)
        source, target = state.source, state.target
        state.customers = source.list_customers()
        state.target_customers = target.list_customers()
        state.source_php_settings = source.list_php_settings()
        state.target_php_settings = target.list_php_settings()
        try:
            state.target_ip_rows = ip_view(target.listing("IpsAndPorts.listing"))
        except FroxlorApiError:
            state.target_ip_rows = []

    @on(Worker.StateChanged)
    def _done(self, event: Worker.StateChanged) -> None:
        if event.state == WorkerState.ERROR:
            self.query_one("#spinner", LoadingIndicator).display = False
            self.query_one("#connect-error", Static).update(f"Connection failed: {event.worker.error}")
            self.query_one("#retry", Button).display = True
        elif event.state == WorkerState.SUCCESS:
            self.advance()

    @on(Button.Pressed, "#retry")
    def _retry(self) -> None:
        self.query_one("#retry", Button).display = False
        self.query_one("#connect-error", Static).update("")
        self.query_one("#spinner", LoadingIndicator).display = True
        self.run_worker(self._load, thread=True, exit_on_error=False)

    @on(Button.Pressed, "#quit")
    def _quit(self) -> None:
        self.app.exit()


class CustomerScreen(WizardScreen):
    # Back would return to a stale ConnectScreen; quit and restart instead.
    block_back = True

    _keys: list[str]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Step 1 — Source customer", id="title")
        yield Label("Select the customer to migrate.", id="subtitle")
        table = DataTable(cursor_type="row", id="customer-table")
        table.add_columns("ID", "Login", "Name", "Email")
        self._keys = []
        for view in customer_view(self.state.customers):
            key = str(view["id"])
            self._keys.append(key)
            table.add_row(str(view["id"]), str(view["login"]), str(view["name"]), str(view["email"]), key=key)
        yield table
        yield Footer()
        with Horizontal(classes="nav"):
            yield Button("Next", id="next", variant="primary")
            yield Button("Quit", id="quit")

    def on_mount(self) -> None:
        token = self.state.args.source_customer
        if not token:
            return
        try:
            picked = select_rows_by_tokens(customer_view(self.state.customers), token, customer_selector_values, "source customer")
        except ValueError as exc:
            self.app.notify(str(exc), severity="error")
            return
        if len(picked) == 1:
            wanted = str(picked[0]["id"])
            if wanted in self._keys:
                self.query_one("#customer-table", DataTable).move_cursor(row=self._keys.index(wanted))

    @on(DataTable.RowSelected, "#customer-table")
    def _row_selected(self, event: DataTable.RowSelected) -> None:
        self._commit(str(event.row_key.value))

    @on(Button.Pressed, "#next")
    def _next(self) -> None:
        table = self.query_one("#customer-table", DataTable)
        if table.cursor_row >= len(self._keys):
            self.app.notify("Pick a customer first", severity="warning")
            return
        self._commit(self._keys[table.cursor_row])

    @on(Button.Pressed, "#quit")
    def _quit(self) -> None:
        self.app.exit()

    def _commit(self, customer_id: str) -> None:
        for view in customer_view(self.state.customers):
            if str(view["id"]) == customer_id:
                self.state.customer = view["_raw"]
                break
        if self.state.customer is None:
            self.app.notify("Unknown customer selected", severity="error")
            return
        self.advance()


class ModeScreen(WizardScreen):
    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Step 2 — Migration mode", id="title")
        yield Label("Whole customer migrates every resource; domain-only lets you pick.", id="subtitle")
        with RadioSet(id="mode"):
            yield RadioButton("Whole customer (all domains, files, databases, mailboxes, settings)", value=self.state.whole_customer)
            yield RadioButton("Domain-only (choose individual resources, may merge into an existing customer)", value=not self.state.whole_customer)
        yield Label("Target customer (domain-only):", id="target-label")
        yield Select(self._target_options(), id="target-customer", value=self._target_default(), allow_blank=False)
        yield Footer()
        with Horizontal(classes="nav"):
            yield Button("Next", id="next", variant="primary")
            yield Button("Back", id="back")

    def _target_options(self) -> list[tuple[str, str]]:
        options = [("Create new customer from source data", TARGET_NEW)]
        for view in customer_view(self.state.target_customers):
            label = f"{view['login']} — {view['name'] or view['email'] or view['id']}"
            options.append((label, str(view["id"])))
        return options

    def _target_default(self) -> str:
        state = self.state
        if state.target_customer is not None:
            return str(as_int(pick(state.target_customer, "customerid", "id", default=0)))
        if state.args.target_customer:
            token = state.args.target_customer.strip().lower()
            if token == TARGET_NEW:
                return TARGET_NEW
            try:
                picked = select_rows_by_tokens(customer_view(state.target_customers), token, customer_selector_values, "target customer")
            except ValueError:
                picked = []
            if len(picked) == 1:
                return str(picked[0]["id"])
        return TARGET_NEW

    def on_mount(self) -> None:
        self._sync_target_picker()

    @on(RadioSet.Changed, "#mode")
    def _mode_changed(self) -> None:
        self._sync_target_picker()

    def _sync_target_picker(self) -> None:
        domain_only = self.query_one("#mode", RadioSet).pressed_index == 1
        self.query_one("#target-customer", Select).display = domain_only
        self.query_one("#target-label", Label).display = domain_only

    def stash(self) -> None:
        state = self.state
        state.whole_customer = self.query_one("#mode", RadioSet).pressed_index != 1
        state.target_customer = None
        if not state.whole_customer:
            choice = self.query_one("#target-customer", Select).value
            for view in customer_view(state.target_customers):
                if str(view["id"]) == str(choice):
                    state.target_customer = view["_raw"]
                    break

    @on(Button.Pressed, "#next")
    def _next(self) -> None:
        self.stash()
        self.advance()

    @on(Button.Pressed, "#back")
    def _back(self) -> None:
        self.action_back()


class DomainsScreen(WizardScreen):
    _rows: list[dict]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Step 3 — Domains", id="title")
        yield Label("Select domains to migrate. Domains outside source_web_root are skipped automatically.", id="subtitle")
        yield LoadingIndicator(id="load-spinner")
        yield SelectionList(id="domain-list")
        yield Static("", id="domain-note", classes="note")
        yield Footer()
        with Horizontal(classes="nav"):
            yield Button("Next", id="next", variant="primary")
            yield Button("Back", id="back")

    def on_mount(self) -> None:
        if not self.state.resources:
            self.run_worker(self._load_resources, thread=True, exit_on_error=False)
        else:
            self._build_list()

    def _load_resources(self) -> None:
        state = self.state
        if state.source is None:
            raise FroxlorApiError("source client is not connected")
        if state.customer is None:
            raise FroxlorApiError("no customer selected")
        customer = state.customer
        customer_id = as_int(pick(customer, "customerid", "id", default=0))
        login = str(pick(customer, "loginname", "login", default=""))
        state.resources = discover_customer_resources(state.source, customer_id, login)

    @on(Worker.StateChanged)
    def _loaded(self, event: Worker.StateChanged) -> None:
        if event.state == WorkerState.ERROR:
            self.query_one("#load-spinner", LoadingIndicator).display = False
            self.query_one("#domain-note", Static).update(f"Could not load customer resources: {event.worker.error}")
        elif event.state == WorkerState.SUCCESS:
            self.query_one("#load-spinner", LoadingIndicator).display = False
            self._build_list()

    def _build_list(self) -> None:
        state = self.state
        self._rows = domain_view(state.resources.get("domains", []))
        widget = self.query_one("#domain-list", SelectionList)
        widget.clear_options()
        previously = {domain_name(d) for d in state.domains} if state.domains is not None else None
        skipped = 0
        for idx, row in enumerate(self._rows):
            if state.whole_customer and not domain_in_source_root(row["_raw"], state.config.paths.source_web_root):
                skipped += 1
                widget.add_option(Item(f"{row['domain']}  {row['docroot']}  (outside web root — skipped)", value=-1, disabled=True))
                continue
            label = f"{row['domain']}  {row['docroot']}  php={row['php']}  ssl={row['ssl']}"
            checked = previously is None or domain_name(row["_raw"]) in previously
            widget.add_option(Item(label, value=idx, initial_state=checked))
        if skipped:
            self.query_one("#domain-note", Static).update(f"{skipped} domain(s) outside source_web_root skipped.")

    def stash(self) -> None:
        if getattr(self, "_rows", None) is None:
            return
        widget = self.query_one("#domain-list", SelectionList)
        self.state.domains = [self._rows[i]["_raw"] for i in widget.selected if i >= 0]

    @on(Button.Pressed, "#next")
    def _next(self) -> None:
        self.stash()
        if not self.state.domains:
            self.app.notify("Select at least one domain", severity="warning")
            return
        self.advance()

    @on(Button.Pressed, "#back")
    def _back(self) -> None:
        self.action_back()


class ResourcesScreen(WizardScreen):
    _sub_rows: list[dict]
    _db_rows: list[dict]
    _mail_rows: list[dict]
    _ftp_rows: list[dict]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Step 4 — Resources", id="title")
        yield Label("Mail forwarders, sender aliases and SSH keys follow the selected mailboxes/FTP accounts automatically.", id="subtitle")
        with TabbedContent():
            with TabPane("Subdomains", id="tab-subdomains"):
                yield SelectionList(id="sub-list")
            with TabPane("Databases", id="tab-dbs"):
                yield SelectionList(id="db-list")
            with TabPane("Mailboxes", id="tab-mail"):
                yield SelectionList(id="mail-list")
            with TabPane("FTP accounts", id="tab-ftp"):
                yield SelectionList(id="ftp-list")
        yield Footer()
        with Horizontal(classes="nav"):
            yield Button("Next", id="next", variant="primary")
            yield Button("Back", id="back")

    def on_mount(self) -> None:
        state = self.state
        resources = state.resources
        selected_domain_names = {domain_name(d) for d in (state.domains or [])}
        narrowed = narrow_subdomains(resources.get("subdomains", []), selected_domain_names)
        self._sub_rows = subdomain_view(narrowed)
        self._db_rows = db_view(resources.get("dbs", []))
        if state.whole_customer:
            # Whole-customer migrates every mailbox, including ones whose
            # domain was skipped for file transfer — same as the headless path.
            mailbox_domain_names = set()
        else:
            mailbox_domain_names = selected_domain_names | {domain_name(r["_raw"]) for r in self._sub_rows}
        self._mail_rows = mail_view(resources.get("emails", []), mailbox_domain_names)
        self._ftp_rows = ftp_view(resources.get("ftps", []))

        self._fill(
            "#sub-list",
            self._sub_rows,
            state.subdomains,
            lambda r: f"{r['domain']}  ->  {r['path']}  ssl={r['ssl']}",
            lambda r: domain_name(r["_raw"]),
        )
        self._fill(
            "#db-list",
            self._db_rows,
            state.databases,
            lambda r: f"{r['dbname']}  {r['description']}  ({r['server']})",
            lambda r: str(pick(r["_raw"], "databasename", "dbname", default="")),
        )
        self._fill(
            "#mail-list",
            self._mail_rows,
            state.mailboxes,
            lambda r: str(r["email"]),
            lambda r: mailbox_address(r["_raw"]),
        )
        self._fill(
            "#ftp-list",
            self._ftp_rows,
            state.ftps,
            lambda r: f"{r['username']}  ->  {r['path']}",
            lambda r: ftp_username(r["_raw"]),
        )

    def _fill(self, widget_id: str, rows: list[dict], previous: list[dict] | None, label: Callable[[dict], str], key_of: Callable[[dict], str]) -> None:
        widget = self.query_one(widget_id, SelectionList)
        previous_keys = {key_of({"_raw": r}) for r in previous} if previous is not None else None
        for idx, row in enumerate(rows):
            checked = previous_keys is None or key_of(row) in previous_keys
            widget.add_option(Item(label(row), value=idx, initial_state=checked))

    def stash(self) -> None:
        state = self.state
        state.subdomains = [self._sub_rows[i]["_raw"] for i in self.query_one("#sub-list", SelectionList).selected]
        state.databases = [self._db_rows[i]["_raw"] for i in self.query_one("#db-list", SelectionList).selected]
        state.mailboxes = [self._mail_rows[i]["_raw"] for i in self.query_one("#mail-list", SelectionList).selected]
        state.ftps = [self._ftp_rows[i]["_raw"] for i in self.query_one("#ftp-list", SelectionList).selected]

    @on(Button.Pressed, "#next")
    def _next(self) -> None:
        self.stash()
        if not self._resolve_preset_mappings():
            return
        self.advance()

    def _resolve_preset_mappings(self) -> bool:
        """Apply --php-map/--ip-map CLI presets (if any) on top of defaults."""
        state = self.state
        try:
            php_arg = parse_mapping_arg(state.args.php_map, "--php-map")
            ip_arg = parse_mapping_arg(state.args.ip_map, "--ip-map")
        except ValueError as exc:
            self.app.notify(str(exc), severity="error")
            return False
        return self._resolve_php_preset(php_arg) and self._resolve_ip_preset(ip_arg)

    def _resolve_php_preset(self, php_arg: dict[str, str]) -> bool:
        state = self.state
        domains = (state.domains or []) + (state.subdomains or [])
        try:
            _, source_rows, target_rows, default_map = collect_php_mapping_candidates(domains, state.source_php_settings, state.target_php_settings)
        except ValueError:
            source_rows, target_rows, default_map = [], [], {}
        state.php_map = dict(default_map)
        if not php_arg or not source_rows:
            return True
        try:
            state.php_map.update(
                resolve_named_mapping(
                    php_arg,
                    source_rows,
                    lambda row: as_int(pick(row, "id", default=0)),
                    php_setting_aliases,
                    target_rows,
                    lambda row: as_int(pick(row, "id", default=0)),
                    php_setting_aliases,
                    "PHP mapping",
                )
            )
        except ValueError as exc:
            self.app.notify(str(exc), severity="error")
            return False
        return True

    def _resolve_ip_preset(self, ip_arg: dict[str, str]) -> bool:
        state = self.state
        source_ip_rows = collect_ip_mapping_candidates(state.domains or [])
        state.ip_map = {}
        if not ip_arg or not source_ip_rows or not state.target_ip_rows:
            return True
        try:
            state.ip_map = resolve_named_mapping(
                ip_arg,
                source_ip_rows,
                lambda row: as_int(pick(row, "id", default=0)),
                ip_aliases,
                state.target_ip_rows,
                lambda row: as_int(pick(row, "id", default=0)),
                ip_aliases,
                "IP mapping",
            )
        except ValueError as exc:
            self.app.notify(str(exc), severity="error")
            return False
        return True

    @on(Button.Pressed, "#back")
    def _back(self) -> None:
        self.action_back()


class MappingsScreen(WizardScreen):
    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Step 5 — PHP & IP mappings", id="title")
        yield Label("Map source PHP settings and IP:port bindings to target IDs.", id="subtitle")
        yield VerticalScroll(id="mapping-body")
        yield Footer()
        with Horizontal(classes="nav"):
            yield Button("Next", id="next", variant="primary")
            yield Button("Back", id="back")

    def on_mount(self) -> None:
        self._mount_php_selects()
        self._mount_ip_selects()

    def _mount_php_selects(self) -> None:
        state = self.state
        body = self.query_one("#mapping-body", VerticalScroll)
        domains = (state.domains or []) + (state.subdomains or [])
        try:
            source_ids, _, php_target_rows, php_default = collect_php_mapping_candidates(domains, state.source_php_settings, state.target_php_settings)
        except ValueError:
            source_ids, php_target_rows, php_default = [], [], {}
        if not source_ids or not php_target_rows:
            return
        options = [
            (f"{row['description']} ({row['binary']})" if row["description"] or row["binary"] else f"id {row['id']}", row["id"]) for row in php_target_rows
        ]
        for source_id in source_ids:
            body.mount(Label(f"Source PHP setting id {source_id} ->"))
            body.mount(Select(options, value=state.php_map.get(source_id, php_default.get(source_id)), id=f"php-{source_id}", allow_blank=False))

    def _mount_ip_selects(self) -> None:
        state = self.state
        source_ip_rows = collect_ip_mapping_candidates(state.domains or [])
        if not source_ip_rows or not state.target_ip_rows:
            return
        body = self.query_one("#mapping-body", VerticalScroll)
        options = [("Froxlor default (customer IP)", IP_DEFAULT)] + [(f"{row['ip']}:{row['port']} ssl={row['ssl']}", row["id"]) for row in state.target_ip_rows]
        for row in source_ip_rows:
            ip_id = as_int(pick(row, "id", default=0))
            label = f"{pick(row, 'ip', default='?')}:{pick(row, 'port', default='?')} ssl={pick(row, 'ssl', default=0)}"
            body.mount(Label(f"Source IP {label} (id {ip_id}) ->"))
            body.mount(Select(options, value=state.ip_map.get(ip_id, IP_DEFAULT), id=f"ip-{ip_id}", allow_blank=False))

    def stash(self) -> None:
        state = self.state
        for widget in self.query(Select):
            wid = widget.id or ""
            raw = widget.value
            if not isinstance(raw, (int, str)) or raw == "":
                continue
            if wid.startswith("php-"):
                state.php_map[int(wid[4:])] = int(raw)
            elif wid.startswith("ip-"):
                value = int(raw)
                if value > 0:
                    state.ip_map[int(wid[3:])] = value
                else:
                    state.ip_map.pop(int(wid[3:]), None)

    @on(Button.Pressed, "#next")
    def _next(self) -> None:
        self.stash()
        self.advance()

    @on(Button.Pressed, "#back")
    def _back(self) -> None:
        self.action_back()


_INCLUDE_OPTIONS = [
    ("files", "Transfer website files (docroots via tar)"),
    ("databases", "Transfer database schema + data"),
    ("mail", "Transfer mailbox content via doveadm backup"),
    ("subdomains", "Create/update subdomains on target"),
    ("certificates", "Migrate TLS certificates"),
    ("dns_zones", "Migrate custom DNS zone records"),
    ("password_sync", "Sync password hashes (customer, FTP, mailbox, htpasswd, DB logins)"),
    ("forwarders", "Migrate mail forwarders"),
    ("sender_aliases", "Migrate sender aliases"),
    ("validate_db_names", "Validate database names after creation"),
]


class OptionsScreen(WizardScreen):
    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Step 6 — Options", id="title")
        yield Label("Choose what gets transferred. Unchecked items are skipped entirely.", id="subtitle")
        yield VerticalScroll(id="options-body")
        yield Footer()
        with Horizontal(classes="nav"):
            yield Button("Next", id="next", variant="primary")
            yield Button("Back", id="back")

    def on_mount(self) -> None:
        state = self.state
        body = self.query_one("#options-body", VerticalScroll)
        initial = state.includes or self._initial_includes()
        for key, label in _INCLUDE_OPTIONS:
            body.mount(Checkbox(label, value=initial.get(key, True), id=f"opt-{key}"))
        body.mount(Checkbox("Dry run (plan only, no changes)", value=state.dry_run, id="opt-dry-run"))

    def _initial_includes(self) -> dict[str, bool]:
        args = self.state.args
        return {
            "files": _yes_no(args.include_files),
            "databases": _yes_no(args.include_databases),
            "mail": _yes_no(args.include_mail),
            "subdomains": not args.skip_subdomains,
            "certificates": not args.skip_certificates,
            "dns_zones": not args.skip_dns_zones,
            "password_sync": not args.skip_password_sync,
            "forwarders": not args.skip_forwarders,
            "sender_aliases": not args.skip_sender_aliases,
            "validate_db_names": not args.skip_database_name_validation,
        }

    def stash(self) -> None:
        state = self.state
        state.includes = {key: self.query_one(f"#opt-{key}", Checkbox).value for key, _ in _INCLUDE_OPTIONS}
        state.dry_run = self.query_one("#opt-dry-run", Checkbox).value

    @on(Button.Pressed, "#next")
    def _next(self) -> None:
        state = self.state
        self.stash()
        if state.includes.get("dns_zones", True):
            self.query_one("#next", Button).disabled = True
            self.run_worker(self._load_zones, thread=True, exit_on_error=False)
        else:
            state.domain_zones, state.zone_errors = [], []
            self._build_plan_and_continue()

    def _load_zones(self) -> None:
        state = self.state
        names = {domain_name(d) for d in (state.domains or [])}
        if state.source is None:
            raise FroxlorApiError("source client is not connected")
        state.domain_zones, state.zone_errors = fetch_domain_zones(state.source, names)

    @on(Worker.StateChanged)
    def _zones_done(self, event: Worker.StateChanged) -> None:
        self.query_one("#next", Button).disabled = False
        if event.state == WorkerState.ERROR:
            self.app.notify(f"Zone fetch failed: {event.worker.error}", severity="error")
        elif event.state == WorkerState.SUCCESS:
            self._build_plan_and_continue()

    def _build_plan_and_continue(self) -> None:
        state = self.state
        sel = self._collect_selection_parts()
        self._commit_selection(sel)
        state.replay_command = self._replay_command(sel)
        state.summary_rows = self._summary(sel)
        self.app.push_screen(PlanScreen())

    def _collect_selection_parts(self) -> dict[str, Any]:
        state = self.state
        scoped = derive_scoped_resources(state.resources, state.mailboxes or [], state.ftps or [])
        return {
            "domains": state.domains or [],
            "subdomains": state.subdomains or [],
            "databases": state.databases or [],
            "mailboxes": state.mailboxes or [],
            "ftps": state.ftps or [],
            "domain_names": {domain_name(d) for d in (state.domains or [])},
            **scoped,
        }

    def _commit_selection(self, sel: dict[str, Any]) -> None:
        state = self.state
        inc = state.includes
        state.selection = build_selection(
            customer=state.customer or {},
            target_customer=state.target_customer,
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
            domain_zones=state.domain_zones,
            include_files=inc["files"],
            include_databases=inc["databases"],
            include_mail=inc["mail"],
            include_subdomains=inc["subdomains"],
            validate_database_names=inc["validate_db_names"],
            php_setting_map=state.php_map,
            ip_mapping=state.ip_map,
            include_certificates=inc["certificates"],
            include_domain_zones=inc["dns_zones"],
            include_password_sync=inc["password_sync"],
            include_forwarders=inc["forwarders"],
            include_sender_aliases=inc["sender_aliases"],
        )

    def _replay_command(self, sel: dict[str, Any]) -> str:
        state = self.state
        inc = state.includes
        source_php_rows: list[dict] = []
        if sel["domains"] or sel["subdomains"]:
            try:
                _, source_php_rows, _, _ = collect_php_mapping_candidates(
                    sel["domains"] + sel["subdomains"], state.source_php_settings, state.target_php_settings
                )
            except ValueError:
                source_php_rows = []
        return build_replay_command(
            config_path=state.args.config,
            apply=not state.dry_run,
            debug=state.args.debug,
            migrate_whole_customer=state.whole_customer,
            selected_customer=state.customer or {},
            target_customer=state.target_customer,
            selected_domains=sel["domains"],
            selected_subdomains=sel["subdomains"],
            selected_databases=sel["databases"],
            selected_mailboxes=sel["mailboxes"],
            selected_ftps=sel["ftps"],
            php_mapping_tokens=build_php_mapping_tokens(state.php_map, source_php_rows, php_settings_view(state.target_php_settings)),
            ip_mapping_tokens=build_ip_mapping_tokens(state.ip_map, collect_ip_mapping_candidates(sel["domains"]), state.target_ip_rows),
            include_files=inc["files"],
            include_databases=inc["databases"],
            include_mail=inc["mail"],
            include_certificates=inc["certificates"],
            include_domain_zones=inc["dns_zones"],
            include_password_sync=inc["password_sync"],
            include_forwarders=inc["forwarders"],
            include_sender_aliases=inc["sender_aliases"],
            skip_subdomains=not inc["subdomains"],
            skip_database_name_validation=not inc["validate_db_names"],
        )

    def _summary(self, sel: dict[str, Any]) -> list[tuple[str, str]]:
        state = self.state
        inc = state.includes
        return plan_rows(
            selected_domains=sel["domains"],
            selected_subdomains=sel["subdomains"],
            selected_databases=sel["databases"],
            selected_mailboxes=sel["mailboxes"],
            selected_forwarders=sel["forwarders"],
            selected_sender_aliases=sel["sender_aliases"],
            selected_ftps=sel["ftps"],
            selected_ssh_keys=sel["ssh_keys"],
            selected_data_dumps=sel["data_dumps"],
            selected_dir_protections=sel["dir_protections"],
            selected_dir_options=sel["dir_options"],
            selected_domain_zones=state.domain_zones,
            migrate_whole_customer=state.whole_customer,
            php_setting_map=state.php_map,
            ip_mapping=state.ip_map,
            include_files=inc["files"],
            include_databases=inc["databases"],
            include_mail=inc["mail"],
            include_certificates=inc["certificates"],
            include_domain_zones=inc["dns_zones"],
            include_password_sync=inc["password_sync"],
            include_forwarders=inc["forwarders"],
            include_sender_aliases=inc["sender_aliases"],
            include_subdomains=inc["subdomains"],
            validate_database_names=inc["validate_db_names"],
            debug=state.args.debug,
            dry_run=state.dry_run,
        )

    @on(Button.Pressed, "#back")
    def _back(self) -> None:
        self.action_back()


class PlanScreen(WizardScreen):
    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Step 7 — Review & start", id="title")
        yield Label("Verify the plan, then start the migration.", id="subtitle")
        yield DataTable(id="plan-table")
        for error in self.state.zone_errors:
            yield Static(f"Zone skipped: {error}", classes="note")
        yield Label("Replay command (same selection, non-interactive):")
        yield TextArea(self.state.replay_command, read_only=True, id="replay")
        yield Footer()
        with Horizontal(classes="nav"):
            yield Button("Start migration", id="start", variant="success")
            yield Button("Back", id="back")

    def on_mount(self) -> None:
        table = self.query_one("#plan-table", DataTable)
        table.add_columns("Item", "Value")
        for item, value in self.state.summary_rows:
            table.add_row(item, value)
        if self.state.dry_run:
            self.query_one("#title", Label).update("Step 7 — Review & start (DRY RUN)")

    @on(Button.Pressed, "#start")
    def _start(self) -> None:
        self.app.push_screen(RunScreen())

    @on(Button.Pressed, "#back")
    def _back(self) -> None:
        self.action_back()


class RunScreen(WizardScreen):
    block_back = True
    _worker: Worker | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        yield Label("Running migration…", id="title")
        yield Label("DRY RUN — no changes will be written", id="subtitle", classes="note") if self.state.dry_run else Label("", id="subtitle")
        yield ProgressBar(total=100, id="progress")
        yield Static("Starting", id="status-line")
        yield RichLog(id="log", max_lines=2000)
        yield Static("", id="run-error", classes="error")
        yield Footer()

    def on_mount(self) -> None:
        state = self.state
        manifest_name = slugify(f"{pick(state.customer or {}, 'loginname', 'login', default='customer')}-{datetime.now().strftime('%Y%m%d-%H%M%S')}")
        state.runner = self.app.runner_cls(config=state.config, dry_run=state.dry_run, manifest_name=manifest_name, debug=state.args.debug)
        if state.source is None or state.target is None:
            self.query_one("#run-error", Static).update("Clients are not connected; restart the wizard.")
            self.block_back = False
            return
        migrator = self.app.migrator_cls(config=state.config, source=state.source, target=state.target, runner=state.runner)
        self._worker = self.run_worker(lambda: self._execute(migrator), thread=True, exit_on_error=False)

    def _execute(self, migrator: Migrator) -> MigrationContext:
        def _progress(step: int, total: int, status: str) -> None:
            self.app.call_from_thread(self._apply_progress, step, max(total, 1), status)
            if self.state.runner is not None:
                self.state.runner.progress_event(step, max(total, 1), status)

        migrator.set_progress_callback(_progress)
        if self.state.selection is None:
            raise MigrationError("no selection prepared")
        return migrator.execute(self.state.selection)

    def _apply_progress(self, step: int, total: int, status: str) -> None:
        self.query_one("#progress", ProgressBar).update(total=total, progress=step)
        self.query_one("#status-line", Static).update(status)
        self.query_one("#log", RichLog).write(f"[{step}/{total}] {status}")

    @on(Worker.StateChanged)
    def _done(self, event: Worker.StateChanged) -> None:
        if event.worker is not self._worker:
            return
        if event.state == WorkerState.SUCCESS:
            self.state.context = event.worker.result
            self.app.push_screen(ResultScreen())
        elif event.state == WorkerState.ERROR:
            manifest = getattr(self.state.runner, "manifest_path", "")
            self.query_one("#run-error", Static).update(f"Migration failed: {event.worker.error}\nManifest: {manifest}")
            self.query_one("#title", Label).update("Migration failed")
            self.block_back = False
        elif event.state == WorkerState.CANCELLED:
            self.query_one("#run-error", Static).update("Migration cancelled.")
            self.block_back = False


class ResultScreen(WizardScreen):
    block_back = True

    def compose(self) -> ComposeResult:
        yield Header()
        state = self.state
        yield Label("Migration completed", id="title")
        yield Static(f"Target customer id: {state.context.target_customer_id if state.context else 'n/a'}")
        yield DataTable(id="db-map")
        yield Static(f"Manifest: {getattr(state.runner, 'manifest_path', '')}")
        yield Label("Replay command:")
        yield TextArea(state.replay_command, read_only=True, id="replay")
        yield Label("Run `froxlor-migrator-verify` to check target parity.", id="subtitle")
        yield Footer()
        with Horizontal(classes="nav"):
            yield Button("Migrate another customer", id="again", variant="primary")
            yield Button("Quit", id="quit")

    def on_mount(self) -> None:
        table = self.query_one("#db-map", DataTable)
        db_map = self.state.context.source_to_target_db if self.state.context else {}
        if not db_map:
            table.display = False
            return
        table.add_columns("Source DB", "Target DB")
        for source_db, target_db in sorted(db_map.items()):
            table.add_row(source_db, target_db)

    @on(Button.Pressed, "#again")
    def _again(self) -> None:
        state = self.state
        state.customer = None
        state.resources = {}
        state.domains = state.subdomains = state.databases = state.mailboxes = state.ftps = None
        state.php_map = {}
        state.ip_map = {}
        state.selection = None
        state.context = None
        state.runner = None
        state.domain_zones = []
        state.zone_errors = []
        # Pop back to the still-suspended CustomerScreen; clients stay connected.
        while len(self.app.screen_stack) > 1 and not isinstance(self.app.screen, CustomerScreen):
            self.app.pop_screen()

    @on(Button.Pressed, "#quit")
    def _quit(self) -> None:
        self.app.exit()


def run_wizard(*, config: AppConfig, args: argparse.Namespace, clients: tuple[FroxlorClient, FroxlorClient] | None = None) -> None:
    app = MigratorWizardApp(config=config, args=args, clients=clients)
    app.run()
