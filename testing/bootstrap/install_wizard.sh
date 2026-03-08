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

cd "$ROOT_DIR"

resolve_cli_path() {
	local service="$1"
	docker compose exec -T "$service" sh -lc '
for path in \
  /app/www/public/bin/froxlor-cli \
  /var/www/html/froxlor/bin/froxlor-cli \
  /var/www/html/bin/froxlor-cli \
  /config/www/froxlor/bin/froxlor-cli \
  /config/www/bin/froxlor-cli
do
  if [ -x "$path" ]; then
    printf "%s" "$path"
    exit 0
  fi
done
exit 1
'
}

fix_runtime_permissions() {
	local service="$1"
	docker compose exec -T "$service" sh -lc "chown -R www-data:www-data /var/www/html/lib /var/www/html/cache"
	docker compose exec -T "$service" sh -lc "chmod 640 /var/www/html/lib/userdata.inc.php 2>/dev/null || true"
}

configure_services() {
	local service="$1"
	local cli
	cli="$(resolve_cli_path "$service")"

	local distro
	distro="${FROXLOR_DISTRIBUTION:-trixie}"
	local webserver
	webserver="${FROXLOR_WEBSERVER:-apache24}"

	local tmp_dir
	tmp_dir="$(mktemp -d)"
	local service_json="$tmp_dir/${service}-services.json"

	python3 - "$service_json" "$distro" "$webserver" <<'PY'
import json
import sys

path = sys.argv[1]
distro = sys.argv[2]
webserver = sys.argv[3]
cfg = {
    "distro": distro,
    "http": webserver,
    "dns": "x",
    "smtp": "postfix_dovecot",
    "mail": "dovecot_postfix2",
    "antispam": "x",
    "ftp": "x",
    "system": ["cron", "libnssextrausers"],
}
with open(path, "w", encoding="utf-8") as f:
    json.dump(cfg, f)
PY

	docker compose cp "$service_json" "$service:/tmp/froxlor-services.json"
	docker compose exec -T "$service" sh -lc "$cli froxlor:config-services --apply=/tmp/froxlor-services.json --yes-to-all --no-interaction"
	docker compose exec -T "$service" sh -lc "service php8.2-fpm start >/dev/null 2>&1 || true"
	docker compose exec -T "$service" sh -lc "service dovecot start >/dev/null 2>&1 || true"
	docker compose exec -T "$service" sh -lc "service postfix start >/dev/null 2>&1 || postfix start >/dev/null 2>&1 || true"

	rm -rf "$tmp_dir"
	echo "[$service] service setup (postfix/dovecot/php-fpm) completed"
}

install_if_needed() {
	local service="$1"
	local db_host="$2"
	local db_root_user="$3"
	local db_root_pass="$4"
	local db_user="$5"
	local db_pass="$6"
	local db_name="$7"
	local admin_name="$8"
	local admin_user="$9"
	local admin_pass="${10}"
	local admin_email="${11}"
	local servername="${12}"
	local db_ready=0

	for _ in $(seq 1 60); do
		if docker compose exec -T "$service" sh -lc "MYSQL_PWD='$db_root_pass' mariadb -h '$db_host' -u '$db_root_user' -Nse 'SELECT 1' >/dev/null 2>&1"; then
			db_ready=1
			break
		fi
		sleep 2
	done
	if [[ "$db_ready" != "1" ]]; then
		echo "[$service] database host '$db_host' not reachable for install" >&2
		exit 1
	fi

	local cli
	cli="$(resolve_cli_path "$service")"

	local install_dir
	install_dir="$(docker compose exec -T "$service" sh -lc "dirname \"$cli\" | xargs dirname")"

	if docker compose exec -T "$service" sh -lc "[ -f \"$install_dir/lib/userdata.inc.php\" ]"; then
		echo "[$service] already installed, skipping wizard"
		fix_runtime_permissions "$service"
		return
	fi

	local tmp_dir
	tmp_dir="$(mktemp -d)"
	local example_json="$tmp_dir/${service}-example.json"
	local input_json="$tmp_dir/${service}-input.json"

	docker compose exec -T "$service" sh -lc "$cli froxlor:install --print-example-file" >"$example_json"

	python3 "$SCRIPT_DIR/build_install_input.py" \
		--example "$example_json" \
		--output "$input_json" \
		--db-host "$db_host" \
		--db-root-user "$db_root_user" \
		--db-root-pass "$db_root_pass" \
		--db-user "$db_user" \
		--db-pass "$db_pass" \
		--db-name "$db_name" \
		--admin-name "$admin_name" \
		--admin-user "$admin_user" \
		--admin-pass "$admin_pass" \
		--admin-email "$admin_email" \
		--servername "$servername" \
		--distribution "${FROXLOR_DISTRIBUTION:-}" \
		--webserver "${FROXLOR_WEBSERVER:-apache24}" \
		--webserver-backend "${FROXLOR_WEBSERVER_BACKEND:-php-fpm}" \
		--manual-config

	docker compose cp "$input_json" "$service:/tmp/froxlor-install.json"
	docker compose exec -T "$service" sh -lc "$cli froxlor:install /tmp/froxlor-install.json"
	fix_runtime_permissions "$service"

	rm -rf "$tmp_dir"
	echo "[$service] unattended install completed"
}

install_if_needed \
	source-froxlor \
	source-db \
	"${SOURCE_DB_ROOT_USER:-root}" \
	"${SOURCE_DB_ROOT_PASSWORD:-source-root}" \
	"${SOURCE_DB_USER:-froxlor}" \
	"${SOURCE_DB_PASSWORD:-froxlor}" \
	"${SOURCE_DB_NAME:-froxlor}" \
	"${SOURCE_ADMIN_NAME:-Source Admin}" \
	"${SOURCE_ADMIN_USER:-admin}" \
	"${SOURCE_ADMIN_PASSWORD:-admin123!}" \
	"${SOURCE_ADMIN_EMAIL:-admin-source@example.test}" \
	"${SOURCE_SERVERNAME:-source.example.test}"

install_if_needed \
	target-froxlor \
	target-db \
	"${TARGET_DB_ROOT_USER:-root}" \
	"${TARGET_DB_ROOT_PASSWORD:-target-root}" \
	"${TARGET_DB_USER:-froxlor}" \
	"${TARGET_DB_PASSWORD:-froxlor}" \
	"${TARGET_DB_NAME:-froxlor}" \
	"${TARGET_ADMIN_NAME:-Target Admin}" \
	"${TARGET_ADMIN_USER:-admin}" \
	"${TARGET_ADMIN_PASSWORD:-admin123!}" \
	"${TARGET_ADMIN_EMAIL:-admin-target@example.test}" \
	"${TARGET_SERVERNAME:-target.example.test}"

configure_services source-froxlor
configure_services target-froxlor
