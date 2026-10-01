# Makefile

.PHONY: all format reformat-ruff check fix-ruff fix vulture complexity xenon bandit pyright typecheck test test-cov test-integration validate help

# Default target: runs format and check
all: validate test

# Format the code using ruff
format:
	uv run ruff format --check --diff .

reformat-ruff:
	uv run ruff format .

# Check the code using ruff
check:
	uv run ruff check .

fix-ruff:
	uv run ruff check . --fix

fix: reformat-ruff fix-ruff
	@echo "Updated code."

vulture:
	uv run vulture . --exclude .venv,migrations,tests,docs,testing --make-whitelist

complexity:
	uv run radon cc froxlor_migrator -a -nc

# Informational only — several long functions intentionally stay above the
# default B rank (see docs/TODO_CICD.md). Not part of `validate` or CI.
xenon:
	uv run xenon -b D -m B -a B froxlor_migrator

bandit:
	uv run bandit -c pyproject.toml -r froxlor_migrator

pyright:
	uv run pyright

typecheck: pyright

test:
	uv run pytest tests/ -m "not integration"

test-cov:
	uv run pytest tests/ -m "not integration" --cov-report=xml --cov-report=term-missing

# Docker-based integration test (builds the compose testbed; needs a docker daemon)
test-integration:
	uv run pytest tests/test_integration_compose.py -v -m integration --timeout=900 --no-cov

# Validate the code (format + check)
validate: format check complexity pyright vulture bandit
	@echo "Validation passed. Your code is ready to push."

# Help target
help:
	@echo "Available targets:"
	@echo "  all              - Run validation and unit tests (default)"
	@echo "  format           - Check code formatting with ruff"
	@echo "  reformat-ruff    - Format code with ruff"
	@echo "  check            - Run ruff linting"
	@echo "  fix-ruff         - Auto-fix ruff issues"
	@echo "  fix              - Run reformat-ruff and fix-ruff"
	@echo "  vulture          - Run dead code detection"
	@echo "  complexity       - Run complexity analysis (radon)"
	@echo "  xenon            - Run xenon complexity check (informational)"
	@echo "  bandit           - Run bandit security lint"
	@echo "  pyright          - Run type checking"
	@echo "  test             - Run unit tests"
	@echo "  test-cov         - Run unit tests with coverage report"
	@echo "  test-integration - Run docker-compose integration test"
	@echo "  validate         - Run all validation checks"
	@echo "  help             - Show this help message"
