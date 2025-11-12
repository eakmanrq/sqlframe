# conftest.py
from __future__ import annotations

import time
from typing import Optional

import docker

# ----------------------------
# Helpers
# ----------------------------
# Module-level holders so hooks can share state
_container: Optional[docker.models.containers.Container] = None


def _is_controller(session_or_env) -> bool:
    # xdist workers set workerinput (session) or PYTEST_XDIST_WORKER (env)
    if hasattr(session_or_env, "config") and hasattr(session_or_env.config, "workerinput"):
        return False
    if isinstance(session_or_env, dict) and session_or_env.get("PYTEST_XDIST_WORKER"):
        return False
    return True


def _wait_for_container_log(container, timeout=30, poll_interval=1, ready_message="GizmoSQL server - started"):
    print(f"Waiting for GizmoSQL container to show '{ready_message}' in logs...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        # Get the logs from the container
        logs = container.logs().decode('utf-8')

        # Check if the ready message is in the logs
        if ready_message in logs:
            return True

        # Wait for the next poll
        time.sleep(poll_interval)

    raise TimeoutError(f"Container did not show '{ready_message}' in logs within {timeout} seconds.")


# Constants
GIZMOSQL_PORT = 31337
CONTAINER_NAME = "sqlframe-gizmosql-test"


def start_gizmosql_container():
    global _container

    client = docker.from_env()

    print(f"Starting GizmoSQL container with name '{CONTAINER_NAME}'...")
    _container = client.containers.run(
        image="gizmodata/gizmosql:latest",
        name=CONTAINER_NAME,
        detach=True,
        remove=True,
        tty=True,
        init=True,
        ports={f"{GIZMOSQL_PORT}/tcp": GIZMOSQL_PORT},
        environment={"GIZMOSQL_USERNAME": "gizmosql_username",
                     "GIZMOSQL_PASSWORD": "gizmosql_password",
                     "DATABASE_FILENAME": "data/sqlframe.db",
                     "TLS_ENABLED": "1",
                     "PRINT_QUERIES": "1",
                     "INIT_SQL_COMMANDS": "CALL dsdgen(sf=0.01);"
                     },
        stdout=True,
        stderr=True
    )

    # Wait for the container to be ready
    _wait_for_container_log(_container)

    print(f"GizmoSQL container successfully started.")


def stop_gizmosql_container():
    global _container

    print(f"Stopping GizmoSQL")
    _container.stop()
    print(f"GizmoSQL container successfully stopped.")


# ----------------------------
# Pytest session hooks
# ----------------------------
def pytest_sessionstart(session):
    """Start the GizmoSQL container once before tests."""
    if not _is_controller(session):
        return
    start_gizmosql_container()


def pytest_sessionfinish(session, exitstatus):
    """Stop the GizmoSQL container after all tests complete."""
    if not _is_controller(session):
        return
    stop_gizmosql_container()
