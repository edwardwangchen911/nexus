"""Secret-free readiness probe for the persistent gateway replay journal."""

import fcntl
import os
import signal
import socket
import sqlite3
import time
from pathlib import Path

from nexus_fosun_gateway.security.hmac import (
    latch_activation_expiry_while_marker_locked,
    read_readiness_marker,
    readiness_marker_lock,
)
from nexus_fosun_gateway.security.replay_schema import replay_schema_is_valid
from nexus_fosun_gateway.settings import BROKER_LISTENER_PORT, MARKET_LISTENER_PORT


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
    port: int | None = None,
) -> bool:
    """Require journal readiness, a live owning PID, and the actual TCP listener."""
    if not replay_store_is_ready(path, now=now):
        return False
    marker_path = path.with_name(f"{path.name}.ready")
    try:
        with readiness_marker_lock(marker_path):
            marker = read_readiness_marker(marker_path)
            if marker is None:
                return False
            marker_kind, first_value, second_value = marker
            if marker_kind == "expired":
                try:
                    latch_activation_expiry_while_marker_locked(path, first_value)
                except RuntimeError:
                    pass
                return False
            if second_value is None:
                return False
            pid = first_value
            ready_until = second_value
            checked_at = time.time() if now is None else now
            if checked_at >= ready_until:
                try:
                    latch_activation_expiry_while_marker_locked(path, ready_until)
                except RuntimeError:
                    try:
                        # The marker guard prevents owner close() from deleting
                        # the last durable evidence before fail-stop completes.
                        os.kill(pid, 0)
                        if _owner_lease_is_held(path):
                            os.kill(pid, signal.SIGKILL)
                    except OSError:
                        pass
                return False
            os.kill(pid, 0)
            if not _owner_lease_is_held(path):
                return False
        ports = (BROKER_LISTENER_PORT, MARKET_LISTENER_PORT) if port is None else (port,)
        for listener_port in ports:
            with socket.create_connection(("127.0.0.1", listener_port), timeout=0.25):
                pass
        return True
    except (OSError, RuntimeError, UnicodeError, ValueError):
        return False


def _owner_lease_is_held(path: Path) -> bool:
    lock_path = path.with_name(f"{path.name}.lock")
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        lease_fd = os.open(lock_path, flags)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        except OSError:
            return False
        fcntl.flock(lease_fd, fcntl.LOCK_UN)
        return False
    finally:
        os.close(lease_fd)


def main() -> None:
    """Exit nonzero until a production replay journal is safe to serve."""
    if os.environ.get("NEXUS_ENV") != "production":
        raise SystemExit(0)
    raw_path = os.environ.get("FOSUN_GATEWAY_REPLAY_STORE_PATH")
    ready = raw_path is not None and gateway_process_is_ready(Path(raw_path))
    raise SystemExit(0 if ready else 1)


if __name__ == "__main__":
    main()
