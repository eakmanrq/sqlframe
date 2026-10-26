#!/usr/bin/env bash
set -euo pipefail

compose_file="tests/integration/clickhouse/compose.yml"
# A run-unique project and an ephemeral host port keep concurrent checkouts
# from sharing, or tearing down, each other's server.
project="sqlframe-clickhouse-$$-${RANDOM}"
compose() { docker compose -p "$project" -f "$compose_file" "$@"; }
trap 'compose down -v' EXIT
compose up -d --wait
port="$(compose port clickhouse 8123)"
port="${port##*:}"
SQLFRAME_CLICKHOUSE_HOST=127.0.0.1 SQLFRAME_CLICKHOUSE_PORT="$port" \
  uv run --extra dev --extra clickhouse pytest -n 0 tests/integration/clickhouse
