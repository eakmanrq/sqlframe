#!/usr/bin/env bash
set -euo pipefail

compose_file="tests/integration/trino/compose.yml"
# A run-unique project and an ephemeral host port keep concurrent checkouts
# from sharing, or tearing down, each other's server.
project="sqlframe-trino-$$-${RANDOM}"
compose() { docker compose -p "$project" -f "$compose_file" "$@"; }
cleanup() {
    local status=$?
    if ! compose down -v && [[ "$status" -eq 0 ]]; then
        status=1
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
compose up -d --wait
port="$(compose port trino 8080)"
port="${port##*:}"
SQLFRAME_TRINO_HOST=127.0.0.1 SQLFRAME_TRINO_PORT="$port" \
    uv run --extra dev --extra trino pytest -n 0 tests/integration/trino
