#!/usr/bin/env bash
set -euo pipefail

compose_file="tests/integration/clickhouse/compose.yml"
trap 'docker compose -f "$compose_file" down -v' EXIT
docker compose -f "$compose_file" up -d --wait
SQLFRAME_CLICKHOUSE_HOST=127.0.0.1 uv run --extra dev --extra clickhouse pytest -n 0 tests/integration/clickhouse
