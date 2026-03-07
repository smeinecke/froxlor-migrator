# Makefile

.PHONY: all format check validate test help

# Default target: runs format and check
all: validate test

# Format the code using ruff
format:
	ruff format --check --diff .

reformat-ruff:
	ruff format .

# Check the code using ruff
check:
	ruff check .

fix-ruff:
	ruff check . --fix

fix: reformat-ruff fix-ruff
	@echo "Updated code."

vulture:
	vulture . --exclude .venv,migrations,tests --make-whitelist

complexity:
	radon cc . -a -nc

xenon:
	xenon -b D -m B -a B .

pyright:
	pyright

test:
	pytest tests/ --cov-fail-under=85

# Validate the code (format + check)
validate: format check complexity pyright vulture
	@echo "Validation passed. Your code is ready to push."

# Help target
help:
	@echo "Available targets:"
	@echo "  all           - Run validation and unit tests (default)"
	@echo "  format        - Check code formatting with ruff"
	@echo "  reformat-ruff - Format code with ruff"
	@echo "  check         - Run ruff linting"
	@echo "  fix-ruff      - Auto-fix ruff issues"
	@echo "  fix           - Run reformat-ruff and fix-ruff"
	@echo "  vulture       - Run dead code detection"
	@echo "  complexity    - Run complexity analysis"
	@echo "  xenon         - Run xenon complexity check"
	@echo "  pyright       - Run type checking"
	@echo "  test          - Run all tests"
	@echo "  validate      - Run all validation checks"
	@echo "  help          - Show this help message"