"""Secret-free readiness probe for the persistent gateway replay journal."""

import os
import socket
import sqlite3
import time
from pathlib import Path

from nexus_fosun_gateway.security.replay_schema import replay_schema_is_valid


def replay_store_is_ready(path: Path, *, now: float | None = None) -> bool:
    """Return false for a missing, malformed, or still-draining replay store."""
    if not path.is_absolute() or not path.is_file():
        return False
    try:
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=0.25)
        try:
            connection.execute("PRAGMA query_only = ON")
            if not replay_schema_is_valid(connection):
                return False
            rows = connection.execute(
                "SELECT key, value FROM gateway_replay_metadata ORDER BY key"
            ).fetchall()
        finally:
            connection.close()
    except (OSError, sqlite3.Error, TypeError, ValueError):
        return False
    if [row[0] for row in rows] != [
        "clock_floor_timestamp",
        "ready_after_timestamp",
    ]:
        return False
    try:
        clock_floor = int(rows[0][1])
        ready_after = int(rows[1][1])
    except (TypeError, ValueError):
        return False
    checked_at = time.time() if now is None else now
    return checked_at >= max(clock_floor, ready_after)


def gateway_process_is_ready(
    path: Path,
    *,
    now: float | None = None,
    port: int = 8081,
) -> bool:
    """Require journal readiness, a live owning PID, and the actual TCP listener."""
    if not replay_store_is_ready(path, now=now):
        return False
    marker_path = path.with_name(f"{path.name}.ready")
    try:
        marker = marker_path.read_text(encoding="ascii")
        if len(marker) > 32 or marker != marker.strip() + "\n":
            return False
        pid = int(marker.strip())
        if pid <= 0:
            return False
        os.kill(pid, 0)
        with socket.create_connection(("127.0.0.1", port), timeout=0.25):
            return True
    except (OSError, UnicodeError, ValueError):
        return False


def main() -> None:
    """Exit nonzero until a production replay journal is safe to serve."""
    if os.environ.get("NEXUS_ENV") != "production":
        raise SystemExit(0)
    raw_path = os.environ.get("FOSUN_GATEWAY_REPLAY_STORE_PATH")
    ready = raw_path is not None and gateway_process_is_ready(Path(raw_path))
    raise SystemExit(0 if ready else 1)


if __name__ == "__main__":
    main()
