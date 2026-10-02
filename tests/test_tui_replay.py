from __future__ import annotations

from froxlor_migrator.plan import build_replay_command


def test_build_replay_command_includes_debug_flag() -> None:
    command = build_replay_command(
        config_path="config.toml",
        apply=True,
        debug=True,
        migrate_whole_customer=False,
        selected_customer={"customerid": 3, "loginname": "alice"},
        target_customer={"customerid": 1, "loginname": "bob"},
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
        skip_subdomains=False,
        skip_database_name_validation=False,
    )
    assert "--debug" in command
