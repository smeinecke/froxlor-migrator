# CI/CD & tooling adoption

Moved here from docs/TODO.md — deferred tooling/CI improvements, to be handled after the code fixes in TODO.md.

The docker-compose Froxlor testbed in `testing/` covers real
source+target migration + `verify_migration` + a doveadm mail-content probe —
CI/tooling structure now follows llro's setup
(`.github/workflows/check.yml`, `mutation.yml`, `Makefile`,
`.pre-commit-config.yaml`).

## Done

- [x] **CI never ran the unit tests.** New `test` job runs
  `uv run --no-sync pytest tests -v -m "not integration"` — the 268-test
  suite is now exercised in CI, coverage artifact uploaded on the 3.10 leg.
- [x] **No Python version matrix.** CI now tests 3.10–3.13 via
  `uv sync --python ${{ matrix.python-version }}` + `uv run --no-sync`.
- [x] **CI was missing lint stages that existed locally** — `ruff format
  --check`, `vulture`, and `bandit` (configured in `[tool.bandit]` with
  documented skips for the inherent subprocess/paramiko/SQL-building
  patterns) all run in the `validate` job. Xenon is deliberately
  informational-only (`make xenon`) — see "Still open".
- [x] **No uv cache** → `actions/cache` on `~/.cache/uv` keyed by
  `pyproject.toml`/`uv.lock` in validate/test jobs.
- [x] **Integration job hardening** — `timeout-minutes: 25`,
  `needs: [validate, test]`, fixed `container_name`s removed and
  `COMPOSE_PROJECT_NAME` written into `testing/.env` per run so nested
  compose calls in the bootstrap container share the project and parallel
  runs can't collide.
- [x] **Compose testbed wrapped in pytest** — `tests/test_integration_compose.py`
  runs locally via `pytest -m integration` / `make test-integration`,
  allocates free ports, backs up a pre-existing `testing/.env`, tears the
  project down afterwards, and dumps compose logs on failure.
  `pytest-timeout` + `integration` marker added.
- [x] **Makefile uv run prefixes** — added `bandit`, `test-cov`,
  `test-integration` targets.
- [x] **`.pre-commit-config.yaml`** — ruff + ruff-format + pyright pre-push.
- [x] **`.github/dependabot.yml`** — uv (weekly, grouped minor/patch),
  github-actions, docker, docker-compose (monthly).
- [x] **Old action versions** — checkout v7, setup-uv v7, cache v6,
  upload-artifact v7; dropped setup-python (uv resolves interpreters) and
  buildx (compose v2 has BuildKit).
- [x] **pytest config gaps** — `--strict-markers`, `pytest-timeout` dev dep,
  `fail_under = 75` moved into `[tool.coverage.report]` so every pytest
  invocation enforces it.
- [x] **Weekly mutmut workflow** — `mutation.yml` Mondays 04:00 UTC +
  workflow_dispatch; incremental `mutants/` cache, results artifact.
  `[tool.mutmut]` configured with `--no-cov` + 60s per-mutant timeout and
  logging-only no-mutate patterns.

## Still open / deferred

- [x] **Xenon gating** — all rank-E/F blocks were refactored in round 7
  (scanner class, phase extraction, shared normalizers); every block now
  ranks ≤ B, so the gate runs at `-b C -m B -a B` (any new C-rank function
  fails). Enforced in `make validate` and the CI validate job.

## Verified while adopting

- `docker/setup-buildx-action` is unnecessary — compose v2 builds via
  BuildKit natively.
- Bandit medium findings (19× B608 SQL construction, B603 subprocess,
  B601 paramiko, B108 remote /tmp paths, try/except-pass) are inherent to
  a migration tool — covered by documented `[tool.bandit]` skips rather
  than individual nosec comments. The one actionable finding (B507
  AutoAddPolicy) is inline-nosec'd: it is the explicit
  `strict_host_key_checking = false` opt-out.
