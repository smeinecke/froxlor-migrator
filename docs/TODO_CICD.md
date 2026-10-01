# CI/CD & tooling adoption

Moved here from docs/TODO.md — deferred tooling/CI improvements, to be handled after the code fixes in TODO.md.

The docker-compose Froxlor testbed in `testing/` already covers real
source+target migration + `verify_migration` + a doveadm mail-content probe —
the gap is CI/tooling structure compared to llro's setup
(`.github/workflows/check.yml`, `mutation.yml`, `Makefile`,
`.pre-commit-config.yaml`).

- [ ] **CI never runs the unit tests.** `ci.yml` has lint-typecheck +
  integration-test jobs but no `pytest tests/` job — the 228-test suite is
  never executed in CI. Add a `test` job like llro's (uv sync →
  `pytest -m "not integration" --cov --cov-report=xml`, upload coverage
  artifact on one matrix leg).
- [ ] **No Python version matrix.** CI pins 3.10 while `requires-python` is
  `>=3.10` — test 3.10–3.13 (llro: `uv sync --python ${{ matrix... }}` +
  `uv run --no-sync pytest` to keep the resolved interpreter).
- [ ] **CI is missing lint stages that already exist locally** — `ruff format
  --check`, `vulture`, `xenon`, `bandit`, `radon` all have Makefile targets and
  dev deps but never run in CI. Bandit/xenon currently report pre-existing
  findings → either fix them first or gate with a documented baseline so the
  check is green on day one.
- [ ] **No uv cache** → add `actions/cache` on `~/.cache/uv` keyed by
  `pyproject.toml`/`uv.lock` (llro pattern).
- [ ] **Integration job hardening** — no `timeout-minutes` (llro uses 15), no
  `needs: [validate, test]` gating, and `testing/docker-compose.yml` uses fixed
  `container_name` values → parallel runs or a shared runner collide. Adopt
  llro's per-run `docker compose -p <project>` randomization.
- [ ] **Wrap the compose testbed in pytest** so it can run locally via
  `pytest -m integration` / `make test-integration` instead of only inside the
  workflow. Add `pytest-timeout` + `integration` marker (mirroring llro's
  `test_integration_compose.py` fixture pattern).
- [ ] **Makefile targets assume an activated venv** — prefix with `uv run`
  (llro does this everywhere); add missing `bandit`/`test-integration` targets.
- [ ] **No `.pre-commit-config.yaml`** — llro has ruff+pyright pre-push hooks.
- [ ] **No `.github/dependabot.yml`** — llro has one.
- [ ] **Old action versions** — checkout@v4, setup-uv@v5, setup-python@v5,
  upload-artifact@v4 → bump to current majors.
- [ ] **pytest config gaps** — no `--strict-markers`, no `pytest-timeout` dev
  dep, `fail_under` only in the Makefile (`--cov-fail-under=75`) rather than
  `[tool.coverage.report] fail_under` so every pytest invocation enforces it.
- [ ] **Optional: weekly mutmut workflow** (llro `mutation.yml`) — mutation
  testing is a good fit for a correctness-critical migration tool; too slow for
  the PR gate, run on schedule + workflow_dispatch with a results artifact.
