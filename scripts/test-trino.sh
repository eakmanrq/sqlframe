#!/usr/bin/env bash
set -euo pipefail
# Give tracked commands their own process groups, including their descendants.
set -m

compose_file="tests/integration/trino/compose.yml"
# A run-unique project and an ephemeral host port keep concurrent checkouts
# from sharing, or tearing down, each other's server.
project="sqlframe-trino-$$-${RANDOM}"
compose() { docker compose -p "$project" -f "$compose_file" "$@"; }
child_pid=""
port_file=""
run() {
    "$@" &
    child_pid=$!
    local status=0
    wait "$child_pid" || status=$?
    child_pid=""
    return "$status"
}
cleanup() {
    local status=$?
    trap '' INT TERM
    if [[ -n "$child_pid" ]]; then
        kill -TERM -- "-$child_pid" 2>/dev/null || true
        # Allow a short grace period before killing unresponsive descendants.
        for _ in 1 2 3 4 5; do
            kill -0 -- "-$child_pid" 2>/dev/null || break
            sleep 0.2
        done
        kill -KILL -- "-$child_pid" 2>/dev/null || true
        wait "$child_pid" 2>/dev/null || true
    fi
    if ! compose down -v && [[ "$status" -eq 0 ]]; then
        status=1
    fi
    if [[ -n "$port_file" ]]; then
        rm -f "$port_file"
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
port_file="$(mktemp)"
run compose up -d --wait
run compose port trino 8080 > "$port_file"
IFS= read -r port < "$port_file"
port="${port##*:}"
SQLFRAME_TRINO_HOST=127.0.0.1 SQLFRAME_TRINO_PORT="$port" \
    run uv run --extra dev --extra trino pytest -n 0 tests/integration/trino
