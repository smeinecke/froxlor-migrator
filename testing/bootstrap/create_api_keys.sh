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

db_query() {
	local service="$1"
	local root_pass="$2"
	local sql="$3"
	docker compose exec -T "$service" sh -lc "MYSQL_PWD=$root_pass mariadb -uroot -Nse \"$sql\""
}

detect_tables() {
	local service="$1"
	local root_pass="$2"
	local db_name="$3"

	local api_table
	api_table="$(db_query "$service" "$root_pass" "SELECT table_name FROM information_schema.columns WHERE table_schema='${db_name}' GROUP BY table_name HAVING SUM(column_name='apikey')>0 AND SUM(column_name='secret')>0 AND SUM(column_name='adminid')>0 AND SUM(column_name='customerid')>0 LIMIT 1;")"

	local admin_table
	admin_table="$(db_query "$service" "$root_pass" "SELECT table_name FROM information_schema.columns WHERE table_schema='${db_name}' GROUP BY table_name HAVING SUM(column_name='adminid')>0 AND SUM(column_name='loginname')>0 LIMIT 1;")"

	local settings_table
	settings_table="$(db_query "$service" "$root_pass" "SELECT table_name FROM information_schema.columns WHERE table_schema='${db_name}' GROUP BY table_name HAVING SUM(column_name='settinggroup')>0 AND SUM(column_name='varname')>0 AND SUM(column_name='value')>0 LIMIT 1;")"

	if [[ -z "$api_table" || -z "$admin_table" || -z "$settings_table" ]]; then
		echo "Failed to detect required froxlor tables in $service/$db_name" >&2
		exit 1
	fi

	printf "%s;%s;%s\n" "$api_table" "$admin_table" "$settings_table"
}

create_key_for_admin() {
	local db_service="$1"
	local root_pass="$2"
	local db_name="$3"
	local admin_login="$4"

	IFS=';' read -r api_table admin_table settings_table <<<"$(detect_tables "$db_service" "$root_pass" "$db_name")"

	local admin_id
	admin_id="$(db_query "$db_service" "$root_pass" "SELECT adminid FROM ${db_name}.${admin_table} WHERE loginname='${admin_login}' LIMIT 1;")"
	if [[ -z "$admin_id" ]]; then
		echo "Could not find admin '${admin_login}' in $db_service" >&2
		exit 1
	fi

	local api_key api_secret
	api_key="$(
		python3 - <<'PY'
import secrets
print(secrets.token_hex(32))
PY
	)"
	api_secret="$(
		python3 - <<'PY'
import secrets
print(secrets.token_hex(64))
PY
	)"

	db_query "$db_service" "$root_pass" "UPDATE ${db_name}.${settings_table} SET value='1' WHERE settinggroup='api' AND varname='enabled';"
	db_query "$db_service" "$root_pass" "DELETE FROM ${db_name}.${api_table} WHERE adminid=${admin_id} AND customerid=0;"
	db_query "$db_service" "$root_pass" "INSERT INTO ${db_name}.${api_table} (apikey, secret, adminid, customerid, valid_until, allowed_from) VALUES ('${api_key}', '${api_secret}', ${admin_id}, 0, -1, '');"

	printf "%s;%s\n" "$api_key" "$api_secret"
}

set_mysql_access_host() {
	local db_service="$1"
	local root_pass="$2"
	local db_name="$3"
	local host_value="$4"
	db_query "$db_service" "$root_pass" "UPDATE ${db_name}.panel_settings SET value='${host_value}' WHERE settinggroup='system' AND varname='mysql_access_host';"
}

upsert_env_var() {
	local file="$1"
	local key="$2"
	local value="$3"
	if grep -qE "^${key}=" "$file"; then
		python3 - "$file" "$key" "$value" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
lines = path.read_text(encoding="utf-8").splitlines()
updated = []
for line in lines:
    if line.startswith(f"{key}="):
        updated.append(f"{key}={value}")
    else:
        updated.append(line)
path.write_text("\n".join(updated) + "\n", encoding="utf-8")
PY
	else
		printf "%s=%s\n" "$key" "$value" >>"$file"
	fi
}

IFS=';' read -r source_key source_secret <<<"$(create_key_for_admin \
	source-db \
	"${SOURCE_DB_ROOT_PASSWORD:-source-root}" \
	"${SOURCE_DB_NAME:-froxlor}" \
	"${SOURCE_ADMIN_USER:-admin}")"

IFS=';' read -r target_key target_secret <<<"$(create_key_for_admin \
	target-db \
	"${TARGET_DB_ROOT_PASSWORD:-target-root}" \
	"${TARGET_DB_NAME:-froxlor}" \
	"${TARGET_ADMIN_USER:-admin}")"

set_mysql_access_host \
	source-db \
	"${SOURCE_DB_ROOT_PASSWORD:-source-root}" \
	"${SOURCE_DB_NAME:-froxlor}" \
	"source-db"

set_mysql_access_host \
	target-db \
	"${TARGET_DB_ROOT_PASSWORD:-target-root}" \
	"${TARGET_DB_NAME:-froxlor}" \
	"target-db"

if [[ -z "$source_key" || -z "$source_secret" || -z "$target_key" || -z "$target_secret" ]]; then
	echo "Failed to generate API keys" >&2
	exit 1
fi

upsert_env_var "$ROOT_DIR/.env" "SOURCE_API_URL" "http://127.0.0.1:${SOURCE_HTTP_PORT:-8081}/api.php"
upsert_env_var "$ROOT_DIR/.env" "SOURCE_API_KEY" "$source_key"
upsert_env_var "$ROOT_DIR/.env" "SOURCE_API_SECRET" "$source_secret"

upsert_env_var "$ROOT_DIR/.env" "TARGET_API_URL" "http://127.0.0.1:${TARGET_HTTP_PORT:-8082}/api.php"
upsert_env_var "$ROOT_DIR/.env" "TARGET_API_KEY" "$target_key"
upsert_env_var "$ROOT_DIR/.env" "TARGET_API_SECRET" "$target_secret"

echo "API keys created and stored in testing/.env"
echo "SOURCE_API_KEY=$source_key"
echo "TARGET_API_KEY=$target_key"
