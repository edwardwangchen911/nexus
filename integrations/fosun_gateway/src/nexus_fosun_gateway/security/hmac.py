"""Gateway-local HMAC v2 verification with principal-bound authorization."""

import asyncio
import base64
import fcntl
import hashlib
import hmac
import math
import os
import re
import secrets
import sqlite3
import stat
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from urllib.parse import parse_qsl, quote, urlsplit
from uuid import UUID

from fastapi import Request

from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.policy.operations import (
    BROKER_OPERATIONS,
    MARKET_OPERATIONS,
    GatewayOperation,
)
from nexus_fosun_gateway.security.replay_schema import (
    REPLAY_APPLICATION_ID,
    REPLAY_SCHEMA_VERSION,
    replay_schema_is_valid,
)
from nexus_fosun_gateway.settings import Environment, GatewaySettings

_SIGNATURE_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_MAX_HMAC_AGE_SECONDS = 60
_REPLAY_STARTUP_DRAIN_SECONDS = _MAX_HMAC_AGE_SECONDS + 1
_READY_MARKER_PATTERN = re.compile(r"^v1 ([1-9][0-9]*) ([1-9][0-9]*)\n$")
_EXPIRY_TOMBSTONE_PATTERN = re.compile(r"^v1-expired ([1-9][0-9]*)\n$")
_MAX_READINESS_MARKER_BYTES = 80


def read_readiness_marker(path: Path) -> tuple[str, int, int | None] | None:
    """Read one owner-only marker without following links."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        marker_fd = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError:
        raise RuntimeError("REPLAY_READINESS_MARKER_INVALID") from None
    try:
        metadata = os.fstat(marker_fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size > _MAX_READINESS_MARKER_BYTES
        ):
            raise RuntimeError("REPLAY_READINESS_MARKER_INVALID")
        marker = os.read(marker_fd, _MAX_READINESS_MARKER_BYTES + 1).decode("ascii")
    except (OSError, UnicodeError):
        raise RuntimeError("REPLAY_READINESS_MARKER_INVALID") from None
    finally:
        os.close(marker_fd)
    ready = _READY_MARKER_PATTERN.fullmatch(marker)
    if ready is not None:
        return ("ready", int(ready.group(1)), int(ready.group(2)))
    expired = _EXPIRY_TOMBSTONE_PATTERN.fullmatch(marker)
    if expired is not None:
        return ("expired", int(expired.group(1)), None)
    raise RuntimeError("REPLAY_READINESS_MARKER_INVALID")


def latch_activation_expiry(path: Path, expires_at_timestamp: int) -> int:
    """Persist an irreversible activation floor, retaining a tombstone on failure."""
    marker_path = path.with_name(f"{path.name}.ready")
    try:
        with _marker_file_lock(marker_path):
            return latch_activation_expiry_while_marker_locked(path, expires_at_timestamp)
    except OSError:
        raise RuntimeError("REPLAY_READINESS_MARKER_INVALID") from None


def latch_activation_expiry_while_marker_locked(
    path: Path,
    expires_at_timestamp: int,
) -> int:
    """Persist expiry while the caller holds the cross-process marker guard."""
    if not path.is_absolute() or expires_at_timestamp <= 0:
        raise RuntimeError("REPLAY_STORE_INVALID")
    marker_path = path.with_name(f"{path.name}.ready")
    try:
        _replace_marker_with_expiry_tombstone_unlocked(marker_path, expires_at_timestamp)
    except RuntimeError:
        # SQLite is an independent durable fail-closed channel. Continue so a
        # marker filesystem failure cannot preserve a previously ready state.
        pass
    try:
        connection = sqlite3.connect(path, timeout=0.25)
        try:
            connection.execute("PRAGMA trusted_schema = OFF")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("BEGIN IMMEDIATE")
            if not replay_schema_is_valid(connection):
                raise RuntimeError("REPLAY_STORE_INVALID")
            rows = dict(
                connection.execute(
                    "SELECT key, value FROM gateway_replay_metadata ORDER BY key"
                ).fetchall()
            )
            if set(rows) != {"clock_floor_timestamp", "ready_after_timestamp"}:
                raise RuntimeError("REPLAY_STORE_INVALID")
            clock_floor = max(int(rows["clock_floor_timestamp"]), expires_at_timestamp)
            cursor = connection.execute(
                "UPDATE gateway_replay_metadata SET value = ? WHERE key = 'clock_floor_timestamp'",
                (clock_floor,),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("REPLAY_STORE_INVALID")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
    except (OSError, sqlite3.Error, TypeError, ValueError) as error:
        # Keep either the expiry tombstone or the pre-existing ready marker.
        # The caller shuts down the listeners and marks an unclean restart for
        # manual review; deleting the last durable marker would permit a later
        # wall-clock rollback to re-admit the same activation evidence.
        raise RuntimeError("REPLAY_STORE_INVALID") from error
    try:
        _remove_expiry_tombstone_unlocked(marker_path, expires_at_timestamp)
    except RuntimeError:
        # The SQLite floor is already durable. Retaining the tombstone is a
        # safe crash-recovery state and the next constructor will merge it.
        pass
    return clock_floor


def _replace_marker_with_expiry_tombstone(path: Path, expires_at_timestamp: int) -> None:
    with _marker_file_lock(path):
        _replace_marker_with_expiry_tombstone_unlocked(path, expires_at_timestamp)


def _replace_marker_with_expiry_tombstone_unlocked(
    path: Path,
    expires_at_timestamp: int,
) -> None:
    payload = f"v1-expired {expires_at_timestamp}\n".encode("ascii")
    temporary_path: Path | None = None
    marker_fd: int | None = None
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        for _ in range(16):
            candidate = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp")
            try:
                marker_fd = os.open(candidate, flags, 0o600)
                temporary_path = candidate
                break
            except FileExistsError:
                continue
        if marker_fd is None or temporary_path is None:
            raise OSError("readiness tombstone collision")
        offset = 0
        while offset < len(payload):
            offset += os.write(marker_fd, payload[offset:])
        os.fsync(marker_fd)
        os.fchmod(marker_fd, 0o600)
        os.close(marker_fd)
        marker_fd = None
        os.replace(temporary_path, path)
        temporary_path = None
        _fsync_directory(path.parent)
    except OSError:
        raise RuntimeError("REPLAY_READINESS_MARKER_INVALID") from None
    finally:
        if marker_fd is not None:
            os.close(marker_fd)
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _remove_expiry_tombstone(path: Path, expires_at_timestamp: int) -> None:
    with _marker_file_lock(path):
        _remove_expiry_tombstone_unlocked(path, expires_at_timestamp)


def _remove_expiry_tombstone_unlocked(path: Path, expires_at_timestamp: int) -> None:
    try:
        marker = read_readiness_marker(path)
        if marker is None:
            return
        if marker != ("expired", expires_at_timestamp, None):
            return
        path.unlink()
        _fsync_directory(path.parent)
    except OSError:
        raise RuntimeError("REPLAY_READINESS_MARKER_INVALID") from None


def _remove_ready_marker(path: Path) -> None:
    try:
        with _marker_file_lock(path):
            marker = read_readiness_marker(path)
            if marker is None or marker[0] == "expired":
                return
            path.unlink()
            _fsync_directory(path.parent)
    except OSError:
        raise RuntimeError("REPLAY_READINESS_MARKER_INVALID") from None


@contextmanager
def _marker_file_lock(path: Path) -> Iterator[None]:
    lock_path = path.with_name(f"{path.name}.guard")
    flags = os.O_RDWR | os.O_CREAT
    flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    lock_fd = os.open(lock_path, flags, 0o600)
    os.fchmod(lock_fd, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        os.close(lock_fd)


@contextmanager
def readiness_marker_lock(path: Path) -> Iterator[None]:
    """Serialize a health probe with owner marker mutation and clean shutdown."""
    with _marker_file_lock(path):
        yield


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(path, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def canonical_hmac_v2(
    method: str,
    path_and_query: str,
    timestamp: int,
    nonce: str,
    actor_id: str,
    request_id: str,
    body: bytes,
) -> str:
    parsed = urlsplit(path_and_query)
    if not parsed.path.startswith("/") or not _valid_percent_escapes(path_and_query):
        raise ValueError("path_and_query must contain an absolute path")
    pairs = [
        (quote(key, safe="~-._"), quote(value, safe="~-._"))
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
    ]
    pairs.sort(key=lambda pair: (pair[0].encode("ascii"), pair[1].encode("ascii")))
    query = "&".join(f"{key}={value}" for key, value in pairs)
    normalized = parsed.path if not query else f"{parsed.path}?{query}"
    return "\n".join(
        (
            "hmac-v2",
            method.upper(),
            normalized,
            str(timestamp),
            nonce,
            actor_id,
            request_id,
            hashlib.sha256(body).hexdigest(),
        )
    )


@dataclass(frozen=True)
class VerifiedGatewayActor:
    actor_id: str
    request_id: UUID
    key_id: str
    allowed_operations: frozenset[GatewayOperation]


@dataclass(frozen=True)
class _VerificationIdentity:
    actor_id: str
    secret: str
    allowed_operations: frozenset[GatewayOperation]


class ReplayCache:
    """Bound replay state for the single-process gateway deployment."""

    def __init__(self, max_entries: int = 4096, ttl_seconds: int = 60) -> None:
        self._entries: OrderedDict[tuple[str, str], float] = OrderedDict()
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._lock = asyncio.Lock()
        self._principal_lanes: dict[str, asyncio.Lock] = {}
        self.ready_after_timestamp = 0

    async def reserve(
        self,
        key_id: str,
        nonce: str,
        now: float,
        *,
        expires_at: float | None = None,
        lane_id: str | None = None,
    ) -> bool:
        async with self._principal_lane(lane_id or key_id):
            async with self._lock:
                expired = [identity for identity, expiry in self._entries.items() if expiry < now]
                for identity in expired:
                    del self._entries[identity]
                identity = (key_id, nonce)
                if identity in self._entries:
                    return False
                if (
                    sum(entry_key_id == key_id for entry_key_id, _ in self._entries)
                    >= self._max_entries
                ):
                    return False
                self._entries[identity] = (
                    now + self._ttl_seconds if expires_at is None else expires_at
                )
                return True

    def _principal_lane(self, lane_id: str) -> asyncio.Lock:
        return self._principal_lanes.setdefault(lane_id, asyncio.Lock())

    def close(self) -> None:
        """The development/test cache owns no external resource."""

    def is_ready(self, now: int) -> bool:
        del now
        return True

    def retry_after_seconds(self, now: int) -> int:
        del now
        return 1

    def mark_serving(
        self,
        *,
        pid: int | None = None,
        ready_until_timestamp: int | None = None,
    ) -> None:
        del pid, ready_until_timestamp

    def mark_not_ready(self) -> None:
        """The in-memory fixture has no external readiness marker."""

    def clock_floor_timestamp(self, now: float) -> float:
        return now

    def mark_activation_expired(self, expires_at_timestamp: int) -> None:
        del expires_at_timestamp
        self.mark_not_ready()


class PersistentReplayCache:
    """Durable nonce journal plus a shared-volume singleton process lease."""

    def __init__(
        self,
        path: Path,
        *,
        initialized_at: int,
        max_entries: int = 4096,
    ) -> None:
        if not path.is_absolute() or max_entries <= 0:
            raise RuntimeError("REPLAY_STORE_INVALID")
        self._path = path
        self._marker_path = path.with_name(f"{path.name}.ready")
        self._max_entries = max_entries
        self._lock = asyncio.Lock()
        self._marker_state_lock = RLock()
        self._principal_lanes: dict[str, asyncio.Lock] = {}
        self._lease_fd: int | None = None
        self._preserve_marker_on_close = False
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            lock_path = path.with_name(f"{path.name}.lock")
            flags = os.O_RDWR | os.O_CREAT
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            lease_fd = os.open(lock_path, flags, 0o600)
            os.fchmod(lease_fd, 0o600)
            try:
                fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (BlockingIOError, OSError):
                os.close(lease_fd)
                raise RuntimeError("REPLAY_SINGLETON_LEASE_UNAVAILABLE") from None
            self._lease_fd = lease_fd
            with _marker_file_lock(self._marker_path):
                marker = read_readiness_marker(self._marker_path)
            if marker is not None and marker[0] == "ready":
                self._preserve_marker_on_close = True
                raise RuntimeError("REPLAY_UNCLEAN_SHUTDOWN_REVIEW_REQUIRED")
            activation_floor = marker[1] if marker is not None and marker[0] == "expired" else 0
            self.ready_after_timestamp = self._initialize(
                initialized_at,
                activation_floor=activation_floor,
            )
            if marker is not None:
                if marker[0] == "expired":
                    _remove_expiry_tombstone(self._marker_path, marker[1])
                else:
                    _remove_ready_marker(self._marker_path)
        except RuntimeError:
            self.close()
            raise
        except (OSError, sqlite3.Error, ValueError):
            self.close()
            raise RuntimeError("REPLAY_STORE_INVALID") from None

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=0.25)
        connection.execute("PRAGMA trusted_schema = OFF")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self, initialized_at: int, *, activation_floor: int = 0) -> int:
        is_new = not self._path.exists() or self._path.stat().st_size == 0
        with self._connect() as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if is_new:
                if version != 0:
                    raise RuntimeError("REPLAY_STORE_INVALID")
                connection.executescript(
                    f"""
                    PRAGMA application_id = {REPLAY_APPLICATION_ID};
                    CREATE TABLE gateway_replay_metadata (
                      key TEXT PRIMARY KEY,
                      value INTEGER NOT NULL
                    ) WITHOUT ROWID;
                    CREATE TABLE gateway_replay_nonces (
                      key_id TEXT NOT NULL,
                      nonce TEXT NOT NULL,
                      expires_at REAL NOT NULL,
                      PRIMARY KEY (key_id, nonce)
                    ) WITHOUT ROWID;
                    PRAGMA user_version = {REPLAY_SCHEMA_VERSION};
                    """
                )
                clock_floor = max(initialized_at, activation_floor)
                ready_after = max(
                    initialized_at + _REPLAY_STARTUP_DRAIN_SECONDS,
                    clock_floor,
                )
                connection.executemany(
                    "INSERT INTO gateway_replay_metadata (key, value) VALUES (?, ?)",
                    (
                        ("clock_floor_timestamp", clock_floor),
                        ("ready_after_timestamp", ready_after),
                    ),
                )
            elif version != REPLAY_SCHEMA_VERSION or not replay_schema_is_valid(connection):
                raise RuntimeError("REPLAY_STORE_INVALID")
            else:
                rows = dict(
                    connection.execute(
                        "SELECT key, value FROM gateway_replay_metadata ORDER BY key"
                    ).fetchall()
                )
                if set(rows) != {"clock_floor_timestamp", "ready_after_timestamp"}:
                    raise RuntimeError("REPLAY_STORE_INVALID")
                clock_floor = int(rows["clock_floor_timestamp"])
                clock_floor = max(clock_floor, initialized_at, activation_floor)
                ready_after = max(
                    initialized_at + _REPLAY_STARTUP_DRAIN_SECONDS,
                    clock_floor,
                )
                connection.executemany(
                    "UPDATE gateway_replay_metadata SET value = ? WHERE key = ?",
                    (
                        (clock_floor, "clock_floor_timestamp"),
                        (ready_after, "ready_after_timestamp"),
                    ),
                )
            if not replay_schema_is_valid(connection):
                raise RuntimeError("REPLAY_STORE_INVALID")
            clock_row = connection.execute(
                "SELECT value FROM gateway_replay_metadata WHERE key = 'clock_floor_timestamp'"
            ).fetchone()
            if clock_row is None:
                raise RuntimeError("REPLAY_STORE_INVALID")
            self._clock_floor_timestamp = int(clock_row[0])
        os.chmod(self._path, 0o600)
        return ready_after

    async def reserve(
        self,
        key_id: str,
        nonce: str,
        now: float,
        *,
        expires_at: float | None = None,
        lane_id: str | None = None,
    ) -> bool:
        expiry = now + _MAX_HMAC_AGE_SECONDS if expires_at is None else expires_at
        async with self._principal_lane(lane_id or key_id):
            async with self._lock:
                return self._reserve_sync(key_id, nonce, now, expiry)

    def _principal_lane(self, lane_id: str) -> asyncio.Lock:
        return self._principal_lanes.setdefault(lane_id, asyncio.Lock())

    def _reserve_sync(self, key_id: str, nonce: str, now: float, expiry: float) -> bool:
        try:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                floor_row = connection.execute(
                    "SELECT value FROM gateway_replay_metadata WHERE key = 'clock_floor_timestamp'"
                ).fetchone()
                if floor_row is None or now < int(floor_row[0]):
                    connection.rollback()
                    return False
                clock_floor = max(int(floor_row[0]), int(now))
                connection.execute(
                    "UPDATE gateway_replay_metadata SET value = ? "
                    "WHERE key = 'clock_floor_timestamp'",
                    (clock_floor,),
                )
                connection.execute(
                    "DELETE FROM gateway_replay_nonces WHERE expires_at < ?",
                    (now,),
                )
                count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM gateway_replay_nonces WHERE key_id = ?",
                        (key_id,),
                    ).fetchone()[0]
                )
                if count >= self._max_entries:
                    connection.commit()
                    self._clock_floor_timestamp = clock_floor
                    return False
                connection.execute(
                    "INSERT INTO gateway_replay_nonces "
                    "(key_id, nonce, expires_at) VALUES (?, ?, ?)",
                    (key_id, nonce, expiry),
                )
                connection.commit()
                self._clock_floor_timestamp = clock_floor
                return True
            except sqlite3.IntegrityError:
                connection.commit()
                self._clock_floor_timestamp = max(
                    self._clock_floor_timestamp,
                    int(now),
                )
                return False
            finally:
                connection.close()
        except sqlite3.Error:
            return False

    def is_ready(self, now: int) -> bool:
        return now >= max(self.ready_after_timestamp, self._clock_floor_timestamp)

    def clock_floor_timestamp(self, now: float) -> float:
        return max(now, self._clock_floor_timestamp)

    def retry_after_seconds(self, now: int) -> int:
        return max(
            1,
            max(self.ready_after_timestamp, self._clock_floor_timestamp) - now,
        )

    def mark_serving(
        self,
        *,
        pid: int | None = None,
        ready_until_timestamp: int | None = None,
    ) -> None:
        with self._marker_state_lock:
            marker_pid = os.getpid() if pid is None else pid
            if marker_pid <= 0 or ready_until_timestamp is None or ready_until_timestamp <= 0:
                raise RuntimeError("REPLAY_READINESS_MARKER_INVALID")
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            try:
                with _marker_file_lock(self._marker_path):
                    marker_fd = os.open(self._marker_path, flags, 0o600)
                    try:
                        os.write(
                            marker_fd,
                            f"v1 {marker_pid} {ready_until_timestamp}\n".encode("ascii"),
                        )
                        os.fsync(marker_fd)
                        os.fchmod(marker_fd, 0o600)
                    finally:
                        os.close(marker_fd)
                    _fsync_directory(self._marker_path.parent)
            except OSError:
                raise RuntimeError("REPLAY_READINESS_MARKER_INVALID") from None

    def mark_not_ready(self) -> None:
        with self._marker_state_lock:
            if self._preserve_marker_on_close:
                return
            try:
                _remove_ready_marker(self._marker_path)
            except (OSError, RuntimeError):
                raise RuntimeError("REPLAY_READINESS_MARKER_INVALID") from None

    def mark_activation_expired(self, expires_at_timestamp: int) -> None:
        with self._marker_state_lock:
            try:
                clock_floor = latch_activation_expiry(self._path, expires_at_timestamp)
            except BaseException:
                # Any interruption before both durable channels are known-good
                # makes marker cleanup unsafe. Preserve the prior ready marker
                # so restart requires explicit unclean-shutdown review.
                self._preserve_marker_on_close = True
                raise
            self._clock_floor_timestamp = max(self._clock_floor_timestamp, clock_floor)

    def close(self) -> None:
        lease_fd = self._lease_fd
        if lease_fd is None:
            return
        self._lease_fd = None
        try:
            with self._marker_state_lock:
                if not self._preserve_marker_on_close:
                    try:
                        _remove_ready_marker(self._marker_path)
                    except RuntimeError:
                        # Preserve malformed or expiry tombstones for fail-closed
                        # diagnosis/recovery rather than erasing durable evidence.
                        pass
            fcntl.flock(lease_fd, fcntl.LOCK_UN)
        finally:
            os.close(lease_fd)


class GatewayHmacVerifier:
    def __init__(
        self,
        settings: GatewaySettings,
        replay_cache: ReplayCache | None = None,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._settings = settings
        self._clock = clock or time.time
        if settings.environment is Environment.PRODUCTION:
            if replay_cache is not None or settings.replay_store_path is None:
                raise RuntimeError("PRODUCTION_REPLAY_STORE_REQUIRED")
            self._replay_cache: ReplayCache | PersistentReplayCache = PersistentReplayCache(
                settings.replay_store_path,
                initialized_at=int(self._clock()),
            )
        else:
            self._replay_cache = replay_cache or ReplayCache()
        self._ready_until_timestamp: int | None = None

    def bind_readiness_expiry(self, expires_at: datetime | None) -> None:
        """Bind the external health marker to the admitted capability lifetime."""
        if expires_at is None:
            if self._settings.environment is Environment.PRODUCTION:
                raise RuntimeError("REPLAY_READINESS_EXPIRY_REQUIRED")
            return
        if expires_at.tzinfo is None or expires_at.utcoffset() is None:
            raise RuntimeError("REPLAY_READINESS_EXPIRY_REQUIRED")
        timestamp = math.ceil(expires_at.timestamp())
        if timestamp <= 0:
            raise RuntimeError("REPLAY_READINESS_EXPIRY_REQUIRED")
        if self._ready_until_timestamp not in (None, timestamp):
            raise RuntimeError("REPLAY_READINESS_EXPIRY_CONFLICT")
        if self._replay_cache.clock_floor_timestamp(float("-inf")) >= timestamp:
            raise RuntimeError("REPLAY_READINESS_EXPIRY_REQUIRED")
        self._ready_until_timestamp = timestamp

    def is_ready(self) -> bool:
        return self._replay_cache.is_ready(int(self._clock()))

    def trusted_activation_checked_at(self) -> datetime:
        checked_at = self._replay_cache.clock_floor_timestamp(self._clock())
        return datetime.fromtimestamp(checked_at, tz=UTC)

    def mark_serving(self) -> None:
        if self._ready_until_timestamp is not None and (
            self._replay_cache.clock_floor_timestamp(float("-inf")) >= self._ready_until_timestamp
        ):
            raise RuntimeError("REPLAY_READINESS_EXPIRY_REQUIRED")
        self._replay_cache.mark_serving(
            ready_until_timestamp=self._ready_until_timestamp,
        )

    def mark_not_ready(self) -> None:
        self._replay_cache.mark_not_ready()

    def mark_activation_expired(self, expires_at: datetime) -> None:
        if expires_at.tzinfo is None or expires_at.utcoffset() is None:
            raise RuntimeError("REPLAY_READINESS_EXPIRY_REQUIRED")
        self._replay_cache.mark_activation_expired(math.ceil(expires_at.timestamp()))

    def close(self) -> None:
        self._replay_cache.close()

    async def verify(
        self,
        request: Request,
        body: bytes,
        *,
        operation: GatewayOperation | None = None,
        expected_actor_id: str | None = None,
    ) -> VerifiedGatewayActor:
        required = (
            "X-Nexus-Signature-Version",
            "X-Nexus-Key-Id",
            "X-Nexus-Timestamp",
            "X-Nexus-Nonce",
            "X-Nexus-Body-SHA256",
            "X-Nexus-Signature",
            "X-Nexus-Actor-Id",
            "X-Request-Id",
        )
        values = {name: request.headers.get(name) for name in required}
        if any(value is None or value == "" for value in values.values()):
            raise _unauthenticated()
        if values["X-Nexus-Signature-Version"] != "hmac-v2":
            raise _unauthenticated()
        timestamp = _parse_timestamp(values["X-Nexus-Timestamp"])
        now = int(self._clock())
        if not self._replay_cache.is_ready(now):
            raise _replay_draining(self._replay_cache.retry_after_seconds(now))
        if timestamp is None or timestamp > now or now - timestamp > _MAX_HMAC_AGE_SECONDS:
            raise _unauthenticated()
        nonce = _parse_uuid7(values["X-Nexus-Nonce"])
        request_id = _parse_uuid7(values["X-Request-Id"])
        if nonce is None or request_id is None:
            raise _unauthenticated()
        key_id = values["X-Nexus-Key-Id"]
        identity = self._verification_identity(key_id)
        if key_id is None or identity is None:
            raise _unauthenticated()
        actor_id = values["X-Nexus-Actor-Id"]
        if actor_id is None or not hmac.compare_digest(actor_id, identity.actor_id):
            raise _unauthenticated()
        supplied_hash = values["X-Nexus-Body-SHA256"]
        body_hash = hashlib.sha256(body).hexdigest()
        if supplied_hash is None or not hmac.compare_digest(supplied_hash, body_hash):
            raise _unauthenticated()
        signature = _decode_signature(values["X-Nexus-Signature"])
        if signature is None:
            raise _unauthenticated()
        path_and_query = _raw_path_and_query(request)
        if path_and_query is None:
            raise _unauthenticated()
        try:
            canonical = canonical_hmac_v2(
                request.method,
                path_and_query,
                timestamp,
                str(nonce),
                actor_id,
                str(request_id),
                body,
            )
        except ValueError:
            raise _unauthenticated() from None
        expected = hmac.new(identity.secret.encode(), canonical.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise _unauthenticated()
        if expected_actor_id is not None and not hmac.compare_digest(
            identity.actor_id,
            expected_actor_id,
        ):
            raise _unauthenticated()
        if operation is not None and operation not in identity.allowed_operations:
            raise _operation_not_allowed()
        if not await self._replay_cache.reserve(
            key_id,
            str(nonce),
            now,
            expires_at=timestamp + _MAX_HMAC_AGE_SECONDS,
            lane_id=identity.actor_id,
        ):
            raise _unauthenticated()
        return VerifiedGatewayActor(
            actor_id=identity.actor_id,
            request_id=request_id,
            key_id=key_id,
            allowed_operations=identity.allowed_operations,
        )

    def _verification_identity(self, key_id: str | None) -> _VerificationIdentity | None:
        settings = self._settings
        principals = (
            (
                "nexus-broker-worker",
                BROKER_OPERATIONS,
                settings.gateway_hmac_current_key_id,
                settings.gateway_hmac_current_secret,
                settings.gateway_hmac_previous_key_id,
                settings.gateway_hmac_previous_secret,
            ),
            (
                "nexus-provider-worker",
                MARKET_OPERATIONS,
                settings.market_gateway_hmac_current_key_id,
                settings.market_gateway_hmac_current_secret,
                settings.market_gateway_hmac_previous_key_id,
                settings.market_gateway_hmac_previous_secret,
            ),
        )
        for (
            actor_id,
            operations,
            current_key_id,
            current_secret,
            previous_key_id,
            previous_secret,
        ) in principals:
            if key_id == current_key_id and current_secret is not None:
                return _VerificationIdentity(
                    actor_id,
                    current_secret.get_secret_value(),
                    operations,
                )
            if key_id == previous_key_id and previous_secret is not None:
                return _VerificationIdentity(
                    actor_id,
                    previous_secret.get_secret_value(),
                    operations,
                )
        return None


def _parse_timestamp(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def _parse_uuid7(value: str | None) -> UUID | None:
    try:
        parsed = UUID(value) if value is not None else None
    except ValueError:
        return None
    return parsed if parsed is not None and parsed.version == 7 else None


def _decode_signature(value: str | None) -> bytes | None:
    if value is None or _SIGNATURE_PATTERN.fullmatch(value) is None:
        return None
    try:
        return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except ValueError:
        return None


def _raw_path_and_query(request: Request) -> str | None:
    raw_path = request.scope.get("raw_path")
    raw_query = request.scope.get("query_string")
    if not isinstance(raw_path, bytes) or not isinstance(raw_query, bytes):
        return None
    try:
        path = raw_path.decode("ascii")
        query = raw_query.decode("ascii")
    except UnicodeDecodeError:
        return None
    if not path.startswith("/") or not _valid_percent_escapes(path + query):
        return None
    return path if not query else f"{path}?{query}"


def _valid_percent_escapes(value: str) -> bool:
    index = 0
    while index < len(value):
        if value[index] == "%":
            if index + 2 >= len(value) or any(
                char not in "0123456789abcdefABCDEF" for char in value[index + 1 : index + 3]
            ):
                return False
            index += 3
        else:
            index += 1
    return True


def _unauthenticated() -> GatewayError:
    return GatewayError(
        "UNAUTHENTICATED",
        "Service authentication failed.",
        status_code=401,
        next_action="REAUTHENTICATE",
    )


def _replay_draining(seconds: int) -> GatewayError:
    return GatewayError(
        "GATEWAY_REPLAY_DRAINING",
        "Gateway authentication is not ready.",
        status_code=503,
        next_action="RETRY_WITH_NEW_SIGNATURE",
        retry_after_seconds=float(max(1, seconds)),
    )


def _operation_not_allowed() -> GatewayError:
    return GatewayError(
        "OPERATION_NOT_ALLOWED",
        "Gateway operation is not available to this caller.",
        status_code=404,
        next_action="USE_AUTHORIZED_OPERATION",
    )
