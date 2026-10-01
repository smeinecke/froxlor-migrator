"""Docker-compose integration test for the full Froxlor migration testbed.

Brings up the `testing/` compose stack (Froxlor source + target panels,
MariaDB backends), runs the real migration + verify + mail-content probe via
the bootstrap container, then tears the project down.

Run locally with:  make test-integration   (or: pytest -m integration)
Requires a working `docker compose` daemon. Skips otherwise.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import NamedTuple

import pytest

TESTING_DIR = Path(__file__).resolve().parent.parent / "testing"
ENV_FILE = TESTING_DIR / ".env"
ENV_EXAMPLE = TESTING_DIR / ".env.example"
BOOTSTRAP_TIMEOUT_SECONDS = 900

pytestmark = pytest.mark.integration


class ComposeTestbed(NamedTuple):
    project: str
    env: dict[str, str]


def _run(cmd: list[str], env: dict[str, str] | None = None, timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, check=False, env=env, cwd=TESTING_DIR, timeout=timeout)


def _docker_compose_available() -> bool:
    return shutil.which("docker") is not None and _run(["docker", "compose", "version"]).returncode == 0


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _build_env_text(project: str) -> str:
    """testing/.env contents for an isolated run: unique project + free ports."""
    lines = [
        ENV_EXAMPLE.read_text(encoding="utf-8"),
        f"COMPOSE_PROJECT_NAME={project}",
        "BOOTSTRAP_RUN_MIGRATION_VERIFY=1",
        f"SOURCE_HTTP_PORT={_free_port()}",
        f"TARGET_HTTP_PORT={_free_port()}",
        f"SOURCE_SSH_PORT={_free_port()}",
        f"TARGET_SSH_PORT={_free_port()}",
        f"SOURCE_DB_PORT={_free_port()}",
        f"TARGET_DB_PORT={_free_port()}",
    ]
    return "\n".join(lines)


@pytest.fixture(scope="module")
def testbed() -> Iterator[ComposeTestbed]:
    if not _docker_compose_available():
        pytest.skip("docker compose is not available.")

    project = f"froxlorint{uuid.uuid4().hex[:8]}"
    env = os.environ.copy()
    env["COMPOSE_PROJECT_NAME"] = project

    backup = ENV_FILE.read_bytes() if ENV_FILE.exists() else None
    ENV_FILE.write_text(_build_env_text(project), encoding="utf-8")
    try:
        yield ComposeTestbed(project=project, env=env)
    finally:
        _run(["docker", "compose", "down", "-v", "--remove-orphans"], env=env, timeout=120)
        if backup is None:
            ENV_FILE.unlink(missing_ok=True)
        else:
            ENV_FILE.write_bytes(backup)


def test_full_migration(testbed: ComposeTestbed) -> None:
    result = _run(
        ["docker", "compose", "--profile", "bootstrap", "run", "--rm", "bootstrap"],
        env=testbed.env,
        timeout=BOOTSTRAP_TIMEOUT_SECONDS,
    )
    if result.returncode != 0:
        logs = _run(["docker", "compose", "logs", "--no-color"], env=testbed.env, timeout=60)
        pytest.fail(
            "compose bootstrap failed "
            f"(rc={result.returncode}).\n--- bootstrap output ---\n{result.stdout}\n{result.stderr}\n"
            f"--- compose logs ---\n{logs.stdout}\n{logs.stderr}"
        )
