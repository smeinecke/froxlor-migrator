from __future__ import annotations

from argparse import Namespace

from froxlor_migrator.tui import _build_replay_command


def test_build_replay_command_includes_debug_flag() -> None:
    args = Namespace(
        config="config.toml",
        non_interactive=True,
        yes=True,
        apply=True,
        skip_subdomains=False,
        skip_database_name_validation=False,
    )
    command = _build_replay_command(
        args=args,
        selected_customer={"customerid": 3, "loginname": "alice"},
        target_customer={"customerid": 1, "loginname": "bob"},
        migrate_whole_customer=False,
        selected_domains=[{"domain": "example.test"}],
        selected_subdomains=[],
        selected_databases=[],
        selected_mailboxes=[],
        selected_ftps=[],
        php_mapping_tokens={},
        ip_mapping_tokens={},
        include_files=True,
        include_databases=True,
        include_mail=True,
        include_certificates=True,
        include_domain_zones=True,
        include_password_sync=True,
        include_forwarders=True,
        include_sender_aliases=True,
        debug=True,
    )
    assert "--debug" in command
