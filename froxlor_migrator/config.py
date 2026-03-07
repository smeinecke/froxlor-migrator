from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - Python 3.10 fallback
    import tomli as tomllib


def _expand_env(value: str) -> str:
    if value.startswith("${") and value.endswith("}"):
        env_name = value[2:-1]
        return os.environ.get(env_name, "")
    return value


def _must(mapping: dict, key: str) -> str:
    value = mapping.get(key)
    if value is None:
        raise ValueError(f"Missing required config key: {key}")
    if isinstance(value, str):
        value = _expand_env(value)
    if value == "":
        raise ValueError(f"Empty required config key: {key}")
    return value


@dataclass(frozen=True)
class ApiConfig:
    api_url: str
    api_key: str
    api_secret: str
    timeout_seconds: int = 30


@dataclass(frozen=True)
class SshConfig:
    host: str
    user: str
    port: int = 22
    strict_host_key_checking: bool = True


@dataclass(frozen=True)
class PathsConfig:
    source_web_root: str
    source_transfer_root: str
    target_web_root: str


@dataclass(frozen=True)
class MysqlConfig:
    source_dump_args: list[str]
    target_import_args: list[str]
    source_panel_database: str = "froxlor"
    target_panel_database: str = "froxlor"


@dataclass(frozen=True)
class CommandsConfig:
    ssh: str = "ssh"
    sudo: str = "sudo"
    tar: str = "tar"
    mysqldump: str = "mysqldump"
    mysql: str = "mysql"
    doveadm: str = "doveadm"
    pzstd: str = "pzstd"
    pigz: str = "pigz"


@dataclass(frozen=True)
class BehaviorConfig:
    dry_run_default: bool = True
    domain_exists: str = "fail"
    database_exists: str = "fail"
    mailbox_exists: str = "skip"
    parallel: int = 1


@dataclass(frozen=True)
class OutputConfig:
    manifest_dir: str = "./manifests"


@dataclass(frozen=True)
class AppConfig:
    source: ApiConfig
    target: ApiConfig
    ssh: SshConfig
    paths: PathsConfig
    mysql: MysqlConfig
    commands: CommandsConfig
    behavior: BehaviorConfig
    output: OutputConfig


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path)
    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))

    source = raw.get("source", {})
    target = raw.get("target", {})
    ssh = raw.get("ssh", {})
    paths = raw.get("paths", {})
    mysql = raw.get("mysql", {})
    commands = raw.get("commands", {})
    behavior = raw.get("behavior", {})
    output = raw.get("output", {})

    source_cfg = ApiConfig(
        api_url=_must(source, "api_url"),
        api_key=_must(source, "api_key"),
        api_secret=_must(source, "api_secret"),
        timeout_seconds=int(source.get("timeout_seconds", 30)),
    )
    target_cfg = ApiConfig(
        api_url=_must(target, "api_url"),
        api_key=_must(target, "api_key"),
        api_secret=_must(target, "api_secret"),
        timeout_seconds=int(target.get("timeout_seconds", 30)),
    )
    ssh_cfg = SshConfig(
        host=_must(ssh, "host"),
        user=_must(ssh, "user"),
        port=int(ssh.get("port", 22)),
        strict_host_key_checking=bool(ssh.get("strict_host_key_checking", True)),
    )
    paths_cfg = PathsConfig(
        source_web_root=_must(paths, "source_web_root"),
        source_transfer_root=str(paths.get("source_transfer_root", _must(paths, "source_web_root"))),
        target_web_root=_must(paths, "target_web_root"),
    )
    mysql_cfg = MysqlConfig(
        source_dump_args=list(mysql.get("source_dump_args", [])),
        target_import_args=list(mysql.get("target_import_args", [])),
        source_panel_database=str(mysql.get("source_panel_database", "froxlor")),
        target_panel_database=str(mysql.get("target_panel_database", "froxlor")),
    )
    commands_cfg = CommandsConfig(
        ssh=str(commands.get("ssh", "ssh")),
        sudo=str(commands.get("sudo", "sudo")),
        tar=str(commands.get("tar", "tar")),
        mysqldump=str(commands.get("mysqldump", "mysqldump")),
        mysql=str(commands.get("mysql", "mysql")),
        doveadm=str(commands.get("doveadm", "doveadm")),
        pzstd=str(commands.get("pzstd", "pzstd")),
        pigz=str(commands.get("pigz", "pigz")),
    )
    behavior_cfg = BehaviorConfig(
        dry_run_default=bool(behavior.get("dry_run_default", True)),
        domain_exists=str(behavior.get("domain_exists", "fail")),
        database_exists=str(behavior.get("database_exists", "fail")),
        mailbox_exists=str(behavior.get("mailbox_exists", "skip")),
        parallel=max(1, int(behavior.get("parallel", 1))),
    )
    output_cfg = OutputConfig(manifest_dir=str(output.get("manifest_dir", "./manifests")))

    return AppConfig(
        source=source_cfg,
        target=target_cfg,
        ssh=ssh_cfg,
        paths=paths_cfg,
        mysql=mysql_cfg,
        commands=commands_cfg,
        behavior=behavior_cfg,
        output=output_cfg,
    )
