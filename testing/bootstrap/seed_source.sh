#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

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

for _ in $(seq 1 30); do
	if curl -fsS "${SOURCE_API_URL%/api.php}/" >/dev/null 2>&1; then
		break
	fi
	sleep 2
done

HOST_UID="$(id -u)"
HOST_GID="$(id -g)"
docker run --rm -v "$ROOT_DIR/data/source:/mnt" testing-source-froxlor sh -lc "chown -R ${HOST_UID}:${HOST_GID} /mnt"
docker compose exec -T source-froxlor sh -lc "mkdir -p /data/customers && chown -R ${HOST_UID}:${HOST_GID} /data/customers"

SOURCE_API_URL="$SOURCE_API_URL" SOURCE_API_MYSQL_HOST="source-db" SOURCE_API_MYSQL_PORT="3306" SOURCE_MYSQL_HOST="${SOURCE_MYSQL_HOST:-127.0.0.1}" SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-${SOURCE_DB_PORT:-33061}}" SOURCE_CONTENT_ROOT="$ROOT_DIR/data/source/customers" uv run --no-project --with requests --with pymysql "$SCRIPT_DIR/seed_source.py"
