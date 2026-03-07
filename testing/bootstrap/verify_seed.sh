#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Running post-bootstrap verification..."

if [[ ! -f "$ROOT_DIR/.env" ]]; then
	echo "Create $ROOT_DIR/.env first (copy from .env.example)."
	exit 1
fi

set -a
source "$ROOT_DIR/.env"
set +a

if [[ "${BOOTSTRAP_IN_DOCKER:-0}" == "1" ]]; then
	SOURCE_API_URL="${SOURCE_API_URL/127.0.0.1/host.docker.internal}"
	SOURCE_MYSQL_HOST="host.docker.internal"
	SOURCE_MYSQL_PORT="${SOURCE_DB_PORT:-33061}"
fi

SOURCE_MYSQL_HOST="${SOURCE_MYSQL_HOST:-127.0.0.1}"
SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-${SOURCE_DB_PORT:-33061}}"

cd "$ROOT_DIR"
SOURCE_API_URL="$SOURCE_API_URL" \
	SOURCE_MYSQL_HOST="$SOURCE_MYSQL_HOST" \
	SOURCE_MYSQL_PORT="$SOURCE_MYSQL_PORT" \
	uv run --no-project --with requests "$SCRIPT_DIR/verify_seed.py"

if [ $? -eq 0 ]; then
	echo "Verification completed successfully!"
else
	echo "Verification failed!"
	exit 1
fi
