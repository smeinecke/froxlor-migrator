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
fi

cd "$ROOT_DIR"
SOURCE_API_URL="$SOURCE_API_URL" uv run --no-project --with requests "$SCRIPT_DIR/verify_seed.py"

if [ $? -eq 0 ]; then
	echo "Verification completed successfully!"
else
	echo "Verification failed!"
	exit 1
fi
