#!/usr/bin/env bash
set -euo pipefail

compose_file="tests/integration/trino/compose.yml"
trap 'docker compose -f "$compose_file" down -v' EXIT
docker compose -f "$compose_file" up -d --wait
SQLFRAME_TRINO_HOST=127.0.0.1 SQLFRAME_TRINO_PORT=18080 \
    uv run --extra dev --extra trino pytest -n 0 tests/integration/trino
