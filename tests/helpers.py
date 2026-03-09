from __future__ import annotations

from froxlor_migrator.config import (
    ApiConfig,
    AppConfig,
    BehaviorConfig,
    CommandsConfig,
    MysqlConfig,
    OutputConfig,
    PathsConfig,
    SshConfig,
)


def sample_config(
    manifest_dir: str,
    *,
    ssh_user: str = "root",
    ssh_host: str = "example.invalid",
    ssh_port: int = 22,
    strict_host_key_checking: bool = True,
    commands: CommandsConfig | None = None,
    paths: PathsConfig | None = None,
    behavior: BehaviorConfig | None = None,
    output: OutputConfig | None = None,
) -> AppConfig:
    return AppConfig(
        source=ApiConfig(api_url="https://source.invalid/api.php", api_key="k", api_secret="s"),
        target=ApiConfig(api_url="https://target.invalid/api.php", api_key="k", api_secret="s"),
        ssh=SshConfig(host=ssh_host, user=ssh_user, port=ssh_port, strict_host_key_checking=strict_host_key_checking),
        paths=paths or PathsConfig(source_web_root="/src", source_transfer_root="/src", target_web_root="/dst"),
        mysql=MysqlConfig(source_panel_database="froxlor", target_panel_database="froxlor"),
        commands=commands or CommandsConfig(),
        behavior=behavior or BehaviorConfig(),
        output=output or OutputConfig(manifest_dir=manifest_dir),
    )
