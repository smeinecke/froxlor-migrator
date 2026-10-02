from __future__ import annotations

import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from froxlor_migrator.wizard import (
    ConnectScreen,
    CustomerScreen,
    DomainsScreen,
    MigratorWizardApp,
    ModeScreen,
    OptionsScreen,
    PlanScreen,
    ResourcesScreen,
    ResultScreen,
    RunScreen,
)


def make_args(**overrides: Any) -> SimpleNamespace:
    base = dict(
        config="config.toml",
        apply=False,
        debug=False,
        non_interactive=False,
        yes=False,
        all_customers=False,
        source_customer=None,
        target_customer=None,
        domain_only=False,
        whole_customer=False,
        domains=None,
        subdomains=None,
        databases=None,
        mailboxes=None,
        ftp_accounts=None,
        php_map=None,
        ip_map=None,
        include_files=None,
        include_databases=None,
        include_mail=None,
        skip_subdomains=False,
        skip_certificates=False,
        skip_dns_zones=False,
        skip_password_sync=False,
        skip_forwarders=False,
        skip_sender_aliases=False,
        skip_database_name_validation=False,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def make_config(**overrides: Any) -> SimpleNamespace:
    base = dict(
        source=SimpleNamespace(api_url="", api_key="", api_secret="", timeout_seconds=30),
        target=SimpleNamespace(api_url="", api_key="", api_secret="", timeout_seconds=30),
        paths=SimpleNamespace(source_web_root="/var/www", source_transfer_root="/var/www", target_web_root="/var/www"),
        behavior=SimpleNamespace(dry_run_default=True),
        commands=SimpleNamespace(ssh="ssh"),
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class DummyClient:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def list_customers(self) -> list[dict]:
        return [
            {"customerid": 1, "loginname": "alice", "name": "Alice A", "email": "alice@example.com"},
            {"customerid": 2, "loginname": "bob", "name": "Bob B", "email": "bob@example.com"},
        ]

    def list_domains(self, **kwargs: Any) -> list[dict]:
        return [
            {"domain": "example.com", "documentroot": "/var/www/example.com", "phpsettingid": 1, "sslenabled": 1},
            {"domain": "other.test", "documentroot": "/srv/outside", "phpsettingid": 0, "sslenabled": 0},
        ]

    def list_subdomains(self, **kwargs: Any) -> list[dict]:
        return [{"domain": "app.example.com", "parentdomain": "example.com", "path": "/app", "sslenabled": 0}]

    def list_mysqls(self, **kwargs: Any) -> list[dict]:
        return [{"databasename": "alice_db", "description": "main", "mysql_server": "srv"}]

    def list_emails(self, **kwargs: Any) -> list[dict]:
        return [
            {"email": "info@example.com"},
            # Domain outside web root: whole-customer must still migrate it.
            {"email": "orphan@other.test"},
        ]

    def list_ftps(self, **kwargs: Any) -> list[dict]:
        return [{"username": "aliceftp", "path": "/", "login_enabled": 1}]

    def list_email_forwarders(self, **kwargs: Any) -> list[dict]:
        return []

    def list_email_senders(self, **kwargs: Any) -> list[dict]:
        return []

    def list_dir_protections(self, **kwargs: Any) -> list[dict]:
        return []

    def list_dir_options(self, **kwargs: Any) -> list[dict]:
        return []

    def list_ssh_keys(self, **kwargs: Any) -> list[dict]:
        return []

    def list_data_dumps(self, **kwargs: Any) -> list[dict]:
        return []

    def list_php_settings(self) -> list[dict]:
        return [{"id": 1, "description": "PHP 8", "binary": "php8"}]

    def list_domain_zones(self, **kwargs: Any) -> list[dict]:
        return []

    def listing(self, command: str) -> list[dict]:
        return []


class DummyRunner:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.manifest_path = Path("/tmp/manifest.json")

    def progress_event(self, step: int, total: int, status: str) -> None:
        pass


class DummyMigrator:
    last_selection = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._callback = None

    def set_progress_callback(self, callback) -> None:
        self._callback = callback

    def execute(self, selection):
        DummyMigrator.last_selection = selection
        if self._callback:
            self._callback(1, 1, "done")
        return SimpleNamespace(target_customer_id=77, source_to_target_db={"alice_db": "alice_db"})


def make_app(**overrides: Any) -> MigratorWizardApp:
    return MigratorWizardApp(
        config=make_config(),
        args=make_args(**overrides.pop("args", {})),
        clients=(DummyClient(), DummyClient()),
        migrator_cls=DummyMigrator,
        runner_cls=DummyRunner,
    )


async def wait_for_screen(app: MigratorWizardApp, screen_type: type, pilot, timeout: float = 5.0) -> None:
    for _ in range(int(timeout / 0.02)):
        if isinstance(app.screen, screen_type):
            # Extra pauses so layout finishes before clicks resolve widget regions.
            await pilot.pause()
            await pilot.pause()
            return
        await pilot.pause(0.02)
    raise AssertionError(f"timed out waiting for {screen_type.__name__}; on {type(app.screen).__name__}")


async def drive_to_plan(pilot) -> MigratorWizardApp:
    """Click through customer/mode/domains/resources/options to PlanScreen."""
    app = pilot.app
    await wait_for_screen(app, CustomerScreen, pilot)
    app.screen.query_one("#customer-list").select(0)
    await pilot.click("#next")
    await wait_for_screen(app, ModeScreen, pilot)
    await pilot.click("#next")
    await wait_for_screen(app, DomainsScreen, pilot)
    for _ in range(250):
        if app.screen.query_one("#domain-list").option_count > 0:
            break
        await pilot.pause(0.02)
    await pilot.click("#next")
    await wait_for_screen(app, ResourcesScreen, pilot)
    await pilot.click("#next")
    # MappingsScreen may appear in between; OptionsScreen is next either way.
    for _ in range(250):
        if isinstance(app.screen, OptionsScreen):
            break
        if type(app.screen).__name__ == "MappingsScreen":
            await pilot.click("#next")
        await pilot.pause(0.02)
    await wait_for_screen(app, OptionsScreen, pilot)
    await pilot.click("#next")
    await wait_for_screen(app, PlanScreen, pilot)
    return app


class WizardFlowTests(unittest.TestCase):
    def test_happy_path_runs_to_result(self) -> None:
        async def scenario() -> None:
            DummyMigrator.last_selection = None
            app = make_app()
            async with app.run_test() as pilot:
                await drive_to_plan(pilot)
                self.assertIsInstance(app.screen, PlanScreen)
                await pilot.click("#start")
                await wait_for_screen(app, ResultScreen, pilot)
                # "Migrate another customer" resets state back to the picker
                await pilot.click("#again")
                await wait_for_screen(app, CustomerScreen, pilot)
            selection = DummyMigrator.last_selection
            self.assertIsNotNone(selection)
            self.assertEqual([d["domain"] for d in selection.domains], ["example.com"])
            self.assertTrue(selection.include_files)
            self.assertTrue(selection.include_mail)
            self.assertEqual(selection.php_setting_map, {1: 1})
            self.assertEqual(
                sorted(m["email"] for m in selection.mailboxes),
                ["info@example.com", "orphan@other.test"],
            )
            self.assertIn("froxlor-migrator", app.state.replay_command)
            self.assertIn("--non-interactive", app.state.replay_command)
            self.assertIsNone(app.state.customer)
            self.assertIsNone(app.state.domains)
            self.assertIsNone(app.state.selection)

        asyncio.run(scenario())

    def test_domain_only_with_existing_target_customer(self) -> None:
        async def scenario() -> None:
            DummyMigrator.last_selection = None
            app = make_app(args={"domain_only": True, "target_customer": "bob"})
            async with app.run_test() as pilot:
                await wait_for_screen(app, CustomerScreen, pilot)
                app.screen.query_one("#customer-list").select(0)
                await pilot.click("#next")
                await wait_for_screen(app, ModeScreen, pilot)
                # domain-only radio is pre-pressed via args; target select shows bob
                mode = app.screen.query_one("#mode")
                self.assertEqual(mode.pressed_index, 1)
                self.assertTrue(app.screen.query_one("#target-customer").display)
                await pilot.click("#next")
                await wait_for_screen(app, DomainsScreen, pilot)
                for _ in range(250):
                    if app.screen.query_one("#domain-list").option_count > 0:
                        break
                    await pilot.pause(0.02)
                await pilot.click("#next")
                await wait_for_screen(app, ResourcesScreen, pilot)
                await pilot.click("#next")
                for _ in range(250):
                    if isinstance(app.screen, OptionsScreen):
                        break
                    if type(app.screen).__name__ == "MappingsScreen":
                        await pilot.click("#next")
                    await pilot.pause(0.02)
                await wait_for_screen(app, OptionsScreen, pilot)
                await pilot.click("#next")
                await wait_for_screen(app, PlanScreen, pilot)
                await pilot.click("#start")
                await wait_for_screen(app, ResultScreen, pilot)
            selection = DummyMigrator.last_selection
            self.assertIsNotNone(selection)
            self.assertEqual(selection.target_customer["loginname"], "bob")
            self.assertFalse(app.state.whole_customer)
            self.assertIn("--domain-only", app.state.replay_command)
            self.assertIn("--target-customer", app.state.replay_command)

        asyncio.run(scenario())

    def test_back_navigation_preserves_domain_selection(self) -> None:
        async def scenario() -> None:
            app = make_app()
            async with app.run_test() as pilot:
                await wait_for_screen(app, CustomerScreen, pilot)
                app.screen.query_one("#customer-list").select(0)
                await pilot.click("#next")
                await wait_for_screen(app, ModeScreen, pilot)
                await pilot.click("#next")
                await wait_for_screen(app, DomainsScreen, pilot)
                for _ in range(250):
                    if app.screen.query_one("#domain-list").option_count > 0:
                        break
                    await pilot.pause(0.02)
                widget = app.screen.query_one("#domain-list")
                widget.deselect(0)
                # Go back to ModeScreen and forward again: deselection survives
                await pilot.press("escape")
                await wait_for_screen(app, ModeScreen, pilot)
                await pilot.click("#next")
                await wait_for_screen(app, DomainsScreen, pilot)
                widget = app.screen.query_one("#domain-list")
                self.assertFalse(widget.get_option_at_index(0).value in widget.selected)
                # whole-customer mode: other.test is outside web root -> disabled
                self.assertTrue(widget.get_option_at_index(1).disabled)
                # Re-select and advance: state.domains should hold only example.com
                widget.select(0)
                await pilot.click("#next")
                await wait_for_screen(app, ResourcesScreen, pilot)
                self.assertEqual([d["domain"] for d in app.state.domains], ["example.com"])

        asyncio.run(scenario())

    def test_connect_error_shows_retry(self) -> None:
        async def scenario() -> None:
            class FailingClient(DummyClient):
                def list_customers(self) -> list[dict]:
                    raise RuntimeError("api down")

            app = MigratorWizardApp(
                config=make_config(),
                args=make_args(),
                clients=(FailingClient(), FailingClient()),
                migrator_cls=DummyMigrator,
                runner_cls=DummyRunner,
            )
            async with app.run_test() as pilot:
                await wait_for_screen(app, ConnectScreen, pilot)
                for _ in range(250):
                    if app.screen.query_one("#retry").display:
                        break
                    await pilot.pause(0.02)
                self.assertIn("api down", str(app.screen.query_one("#connect-error").render()))
                self.assertTrue(app.screen.query_one("#retry", type(app.screen.query_one("#retry"))).display)

        asyncio.run(scenario())

    def test_batch_multi_select_runs_each_customer(self) -> None:
        async def scenario() -> None:
            executed: list[str] = []

            class RecordingMigrator(DummyMigrator):
                def execute(self, selection):
                    executed.append(selection.customer["loginname"])
                    return SimpleNamespace(target_customer_id=100 + len(executed), source_to_target_db={})

            app = MigratorWizardApp(
                config=make_config(),
                args=make_args(),
                clients=(DummyClient(), DummyClient()),
                migrator_cls=RecordingMigrator,
                runner_cls=DummyRunner,
            )
            async with app.run_test() as pilot:
                await wait_for_screen(app, CustomerScreen, pilot)
                widget = app.screen.query_one("#customer-list")
                widget.select(0)
                widget.select(1)
                await pilot.click("#next")
                await wait_for_screen(app, ModeScreen, pilot)
                await pilot.click("#next")
                # Batch skips Domains/Resources/Mappings -> straight to Options
                await wait_for_screen(app, OptionsScreen, pilot)
                await pilot.click("#next")
                await wait_for_screen(app, PlanScreen, pilot)
                self.assertTrue(app.state.is_batch)
                self.assertIn("--source-customer", app.state.replay_command)
                self.assertIn("1,2", app.state.replay_command)
                await pilot.click("#start")
                await wait_for_screen(app, ResultScreen, pilot)
                table = app.screen.query_one("#batch-results")
                self.assertEqual(table.row_count, 2)
                self.assertEqual(len(app.state.batch_results), 2)
                self.assertTrue(all(r["status"] == "ok" for r in app.state.batch_results))
            self.assertEqual(executed, ["alice", "bob"])

        asyncio.run(scenario())

    def test_batch_continues_past_failed_customer(self) -> None:
        async def scenario() -> None:
            executed: list[str] = []

            class FlakyMigrator(DummyMigrator):
                def execute(self, selection):
                    executed.append(selection.customer["loginname"])
                    if selection.customer["loginname"] == "alice":
                        raise RuntimeError("kaboom")
                    return SimpleNamespace(target_customer_id=42, source_to_target_db={})

            app = MigratorWizardApp(
                config=make_config(),
                args=make_args(),
                clients=(DummyClient(), DummyClient()),
                migrator_cls=FlakyMigrator,
                runner_cls=DummyRunner,
            )
            async with app.run_test() as pilot:
                await wait_for_screen(app, CustomerScreen, pilot)
                widget = app.screen.query_one("#customer-list")
                widget.select(0)
                widget.select(1)
                await pilot.click("#next")
                await wait_for_screen(app, ModeScreen, pilot)
                await pilot.click("#next")
                await wait_for_screen(app, OptionsScreen, pilot)
                await pilot.click("#next")
                await wait_for_screen(app, PlanScreen, pilot)
                await pilot.click("#start")
                await wait_for_screen(app, ResultScreen, pilot)
                statuses = {r["login"]: r["status"] for r in app.state.batch_results}
                self.assertEqual(statuses["bob"], "ok")
                self.assertTrue(str(statuses["alice"]).startswith("failed"))
            self.assertEqual(executed, ["alice", "bob"])

        asyncio.run(scenario())

    def test_run_error_shows_manifest_and_unlocks_back(self) -> None:
        async def scenario() -> None:
            class FailingMigrator(DummyMigrator):
                def execute(self, selection):
                    raise RuntimeError("kaboom")

            app = MigratorWizardApp(
                config=make_config(),
                args=make_args(),
                clients=(DummyClient(), DummyClient()),
                migrator_cls=FailingMigrator,
                runner_cls=DummyRunner,
            )
            async with app.run_test() as pilot:
                await drive_to_plan(pilot)
                await pilot.click("#start")
                await wait_for_screen(app, RunScreen, pilot)
                for _ in range(250):
                    if not app.screen.block_back:
                        break
                    await pilot.pause(0.02)
                self.assertIn("kaboom", str(app.screen.query_one("#run-error").render()))
                self.assertFalse(app.screen.block_back)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
