#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

if [[ ! -f "$ROOT_DIR/.env" ]]; then
	echo "Create $ROOT_DIR/.env first (copy from .env.example)."
	exit 1
fi

if [[ "${BOOTSTRAP_IN_DOCKER:-0}" == "1" ]]; then
	if [[ -z "${TESTING_BIND_ROOT:-}" ]]; then
		workspace_bind_source="$(docker inspect "$HOSTNAME" --format '{{range .Mounts}}{{if eq .Destination "/workspace"}}{{.Source}}{{end}}{{end}}' 2>/dev/null || true)"
		if [[ -n "$workspace_bind_source" ]]; then
			TESTING_BIND_ROOT="$workspace_bind_source/testing"
		fi
	fi
	if [[ -z "${TESTING_BIND_ROOT:-}" ]]; then
		echo "Could not determine TESTING_BIND_ROOT for nested docker compose usage." >&2
		echo "Set TESTING_BIND_ROOT to the host path of the testing directory and retry." >&2
		exit 1
	fi
	export TESTING_BIND_ROOT
else
	export TESTING_BIND_ROOT="${TESTING_BIND_ROOT:-$ROOT_DIR}"
fi

set -a
source "$ROOT_DIR/.env"
set +a

mkdir -p "$ROOT_DIR/ssh"
if [[ ! -f "$ROOT_DIR/ssh/id_ed25519" ]]; then
	ssh-keygen -q -t ed25519 -N "" -f "$ROOT_DIR/ssh/id_ed25519"
fi
if [[ -d "$ROOT_DIR/ssh/authorized_keys" ]]; then
	rm -rf "$ROOT_DIR/ssh/authorized_keys"
fi
cp "$ROOT_DIR/ssh/id_ed25519.pub" "$ROOT_DIR/ssh/authorized_keys"
chmod 600 "$ROOT_DIR/ssh/id_ed25519"
chmod 600 "$ROOT_DIR/ssh/authorized_keys"
chmod 644 "$ROOT_DIR/ssh/id_ed25519.pub"

docker compose down -v --remove-orphans

# Ensure stale/corrupt bind-mounted MariaDB files do not survive restarts.
docker run --rm -v "$ROOT_DIR/data/source/db:/mnt" alpine:3.21 sh -lc 'rm -rf /mnt/* /mnt/.[!.]* /mnt/..?* 2>/dev/null || true'
docker run --rm -v "$ROOT_DIR/data/target/db:/mnt" alpine:3.21 sh -lc 'rm -rf /mnt/* /mnt/.[!.]* /mnt/..?* 2>/dev/null || true'
docker run --rm -v "$ROOT_DIR/data/source/db:/mnt" alpine:3.21 sh -lc 'chmod -R 0777 /mnt'
docker run --rm -v "$ROOT_DIR/data/target/db:/mnt" alpine:3.21 sh -lc 'chmod -R 0777 /mnt'

# Start each bootstrap from clean customer content directories as well.
docker run --rm -v "$ROOT_DIR/data/source/customers:/mnt" alpine:3.21 sh -lc 'rm -rf /mnt/* /mnt/.[!.]* /mnt/..?* 2>/dev/null || true'
docker run --rm -v "$ROOT_DIR/data/target/customers:/mnt" alpine:3.21 sh -lc 'rm -rf /mnt/* /mnt/.[!.]* /mnt/..?* 2>/dev/null || true'

docker compose up -d --build --force-recreate source-db target-db source-froxlor target-froxlor
"$SCRIPT_DIR/install_wizard.sh"

# Enable allowed-sender aliases for mailbox testing/migration.
docker compose exec -T source-db sh -lc "MYSQL_PWD='${SOURCE_DB_ROOT_PASSWORD:-source-root}' mariadb -u'${SOURCE_DB_ROOT_USER:-root}' '${SOURCE_DB_NAME:-froxlor}' -e \"UPDATE panel_settings SET value='1' WHERE settinggroup='mail' AND varname='enable_allow_sender';\""
docker compose exec -T target-db sh -lc "MYSQL_PWD='${TARGET_DB_ROOT_PASSWORD:-target-root}' mariadb -u'${TARGET_DB_ROOT_USER:-root}' '${TARGET_DB_NAME:-froxlor}' -e \"UPDATE panel_settings SET value='1' WHERE settinggroup='mail' AND varname='enable_allow_sender';\""

"$SCRIPT_DIR/create_api_keys.sh"

set -a
source "$ROOT_DIR/.env"
set +a

refresh_froxlor_runtime() {
	local service="$1"
	docker compose exec -T "$service" sh -lc "/var/www/html/bin/froxlor-cli froxlor:cron --force >/dev/null 2>&1 || true"
	docker compose exec -T "$service" sh -lc "service php8.2-fpm restart >/dev/null 2>&1 || true"
	docker compose exec -T "$service" sh -lc "service php8.3-fpm restart >/dev/null 2>&1 || true"
	docker compose exec -T "$service" sh -lc "service php8.4-fpm restart >/dev/null 2>&1 || true"
	docker compose exec -T "$service" sh -lc "service dovecot restart >/dev/null 2>&1 || true"
	docker compose exec -T "$service" sh -lc "service postfix restart >/dev/null 2>&1 || postfix start >/dev/null 2>&1 || true"
}

wait_api() {
	local url="$1"
	for _ in $(seq 1 60); do
		local code
		code="$(curl -s -o /dev/null -w "%{http_code}" "$url" || true)"
		if [[ "$code" != "000" ]] && [[ "$code" -lt 500 ]]; then
			return 0
		fi
		sleep 2
	done
	echo "API endpoint not ready: $url" >&2
	exit 1
}

if [[ "${BOOTSTRAP_IN_DOCKER:-0}" == "1" ]]; then
	SOURCE_API_URL="${SOURCE_API_URL/127.0.0.1/host.docker.internal}"
	TARGET_API_URL="${TARGET_API_URL/127.0.0.1/host.docker.internal}"
fi

refresh_froxlor_runtime source-froxlor
refresh_froxlor_runtime target-froxlor
wait_api "${SOURCE_API_URL}"
wait_api "${TARGET_API_URL}"

uv run --no-project --with requests "$SCRIPT_DIR/ensure_php_profiles.py" \
	--api-url "${SOURCE_API_URL}" \
	--api-key "${SOURCE_API_KEY}" \
	--api-secret "${SOURCE_API_SECRET}" \
	--profile php8.3 \
	--profile php8.4

uv run --no-project --with requests "$SCRIPT_DIR/ensure_php_profiles.py" \
	--api-url "${TARGET_API_URL}" \
	--api-key "${TARGET_API_KEY}" \
	--api-secret "${TARGET_API_SECRET}" \
	--profile php8.3 \
	--profile php8.4

"$SCRIPT_DIR/seed_source.sh"
"$SCRIPT_DIR/verify_seed.sh"

if [[ "${BOOTSTRAP_RUN_MIGRATION_VERIFY:-1}" == "1" ]]; then
	"$SCRIPT_DIR/migrate_and_verify.sh"
fi

echo
echo "Server login info"
echo "-----------------"
echo "Source UI: http://127.0.0.1:${SOURCE_HTTP_PORT:-8081}"
echo "  Admin user: ${SOURCE_ADMIN_USER:-admin}"
echo "  Admin pass: ${SOURCE_ADMIN_PASSWORD:-admin123!}"
echo "Target UI: http://127.0.0.1:${TARGET_HTTP_PORT:-8082}"
echo "  Admin user: ${TARGET_ADMIN_USER:-admin}"
echo "  Admin pass: ${TARGET_ADMIN_PASSWORD:-admin124!}"
echo "Source SSH: 127.0.0.1:${SOURCE_SSH_PORT:-2221} (key: $ROOT_DIR/ssh/id_ed25519)"
echo "Target SSH: 127.0.0.1:${TARGET_SSH_PORT:-2222} (key: $ROOT_DIR/ssh/id_ed25519)"

echo "Full bootstrap completed"
