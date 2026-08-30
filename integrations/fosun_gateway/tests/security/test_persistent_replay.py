"""Persistent replay journal and singleton lease security tests."""

import asyncio
import inspect
import os
import socket
import sqlite3
import stat
import threading
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path

import nexus_fosun_gateway.readiness as readiness_module
import nexus_fosun_gateway.security.hmac as hmac_module
import pytest
from nexus_fosun_gateway.readiness import gateway_process_is_ready, replay_store_is_ready
from nexus_fosun_gateway.security.hmac import (
    GatewayHmacVerifier,
    PersistentReplayCache,
    ReplayCache,
)
from nexus_fosun_gateway.security.replay_schema import REPLAY_APPLICATION_ID
from nexus_fosun_gateway.settings import GatewaySettings


@pytest.mark.asyncio
async def test_persistent_nonce_survives_restart_and_wall_clock_rollback(
    tmp_path: Path,
) -> None:
    path = tmp_path / "replay.sqlite3"
    first = PersistentReplayCache(path, initialized_at=1_000)
    try:
        assert first.ready_after_timestamp == 1_061
        assert await first.reserve("key-1", "nonce-1", 1_061, expires_at=1_121)
    finally:
        first.close()

    restarted = PersistentReplayCache(path, initialized_at=900)
    try:
        assert restarted.ready_after_timestamp == 1_061
        assert not await restarted.reserve("key-1", "nonce-1", 900, expires_at=1_121)
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    finally:
        restarted.close()


@pytest.mark.asyncio
async def test_persistent_replay_capacity_is_independent_per_hmac_key(
    tmp_path: Path,
) -> None:
    """Provider nonce churn must not consume the Broker key's durable quota."""
    cache = PersistentReplayCache(
        tmp_path / "replay.sqlite3",
        initialized_at=1_000,
        max_entries=2,
    )
    try:
        assert await cache.reserve("broker-key", "broker-nonce-1", 1_061)
        assert await cache.reserve("broker-key", "broker-nonce-2", 1_061)
        assert await cache.reserve("market-key", "market-nonce-1", 1_061)
        assert not await cache.reserve("broker-key", "broker-nonce-3", 1_061)
    finally:
        cache.close()


@pytest.mark.asyncio
async def test_replay_principal_lane_keeps_market_backlog_behind_broker() -> None:
    """Only one market reservation may contend with Broker on the shared journal."""
    assert "lane_id" in inspect.signature(ReplayCache.reserve).parameters

    class RecordingLock:
        def __init__(self) -> None:
            self._lock = asyncio.Lock()
            self.first_entered = asyncio.Event()
            self.release_first = asyncio.Event()
            self.order: list[str] = []

        async def __aenter__(self) -> None:
            await self._lock.acquire()
            task = asyncio.current_task()
            self.order.append(task.get_name() if task is not None else "unknown")
            if len(self.order) == 1:
                self.first_entered.set()
                await self.release_first.wait()

        async def __aexit__(self, *args: object) -> None:
            del args
            self._lock.release()

    cache = ReplayCache()
    journal_lock = RecordingLock()
    cache._lock = journal_lock  # type: ignore[assignment]
    first_market = asyncio.create_task(
        cache.reserve(
            "market-key",
            "market-1",
            1_000,
            lane_id="nexus-provider-worker",
        ),
        name="market-1",
    )
    try:
        await asyncio.wait_for(journal_lock.first_entered.wait(), timeout=1)
        second_market = asyncio.create_task(
            cache.reserve(
                "market-key",
                "market-2",
                1_000,
                lane_id="nexus-provider-worker",
            ),
            name="market-2",
        )
        await asyncio.sleep(0)
        broker = asyncio.create_task(
            cache.reserve(
                "broker-key",
                "broker-1",
                1_000,
                lane_id="nexus-broker-worker",
            ),
            name="broker",
        )
        await asyncio.sleep(0)
        journal_lock.release_first.set()
        assert all(await asyncio.gather(first_market, second_market, broker))
    finally:
        journal_lock.release_first.set()

    assert journal_lock.order == ["market-1", "broker", "market-2"]


def test_persistent_replay_store_allows_only_one_live_gateway(tmp_path: Path) -> None:
    path = tmp_path / "replay.sqlite3"
    first = PersistentReplayCache(path, initialized_at=1_000)
    try:
        first.mark_serving(pid=os.getpid(), ready_until_timestamp=2_000)
        with pytest.raises(RuntimeError, match="REPLAY_SINGLETON_LEASE_UNAVAILABLE"):
            PersistentReplayCache(path, initialized_at=1_000)
        assert path.with_name("replay.sqlite3.ready").is_file()
    finally:
        first.close()


def test_existing_corrupt_replay_store_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "replay.sqlite3"
    path.write_bytes(b"not-a-sqlite-database")

    with pytest.raises(RuntimeError, match="REPLAY_STORE_INVALID"):
        PersistentReplayCache(path, initialized_at=1_000)


def test_readiness_uses_persisted_first_start_boundary(tmp_path: Path) -> None:
    path = tmp_path / "replay.sqlite3"
    cache = PersistentReplayCache(path, initialized_at=1_000)
    try:
        assert not replay_store_is_ready(path, now=1_060)
        assert replay_store_is_ready(path, now=1_061)
    finally:
        cache.close()


def test_process_readiness_requires_live_marker_and_listener(tmp_path: Path) -> None:
    path = tmp_path / "replay.sqlite3"
    cache = PersistentReplayCache(path, initialized_at=1_000)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = int(listener.getsockname()[1])
    try:
        assert not gateway_process_is_ready(path, now=1_061, port=port)
        cache.mark_serving(pid=os.getpid(), ready_until_timestamp=2_000)
        assert gateway_process_is_ready(path, now=1_061, port=port)
        cache.mark_not_ready()
        cache.mark_not_ready()
        assert not gateway_process_is_ready(path, now=1_061, port=port)
        cache.mark_serving(pid=os.getpid(), ready_until_timestamp=2_000)
        listener.close()
        assert not gateway_process_is_ready(path, now=1_061, port=port)
    finally:
        listener.close()
        cache.close()


def test_process_readiness_expires_and_irreversibly_removes_marker(tmp_path: Path) -> None:
    path = tmp_path / "replay.sqlite3"
    cache = PersistentReplayCache(path, initialized_at=1_000)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = int(listener.getsockname()[1])
    marker = path.with_name("replay.sqlite3.ready")
    try:
        cache.mark_serving(
            pid=os.getpid(),
            ready_until_timestamp=1_100,
        )
        assert gateway_process_is_ready(path, now=1_099, port=port)
        assert not gateway_process_is_ready(path, now=1_100, port=port)
        assert not marker.exists()
        assert not gateway_process_is_ready(path, now=1_099, port=port)
    finally:
        listener.close()
        cache.close()

    restarted = PersistentReplayCache(path, initialized_at=900)
    try:
        assert restarted.clock_floor_timestamp(900) >= 1_100
        assert not restarted.is_ready(1_099)
    finally:
        restarted.close()


def test_activation_expiry_floor_survives_restart_and_wall_clock_rollback(
    tmp_path: Path,
) -> None:
    path = tmp_path / "replay.sqlite3"
    first = PersistentReplayCache(path, initialized_at=1_000)
    try:
        first.mark_activation_expired(1_100)
        assert first.clock_floor_timestamp(900) == 1_100
    finally:
        first.close()

    restarted = PersistentReplayCache(path, initialized_at=900)
    try:
        assert restarted.clock_floor_timestamp(900) == 1_100
        assert not restarted.is_ready(1_099)
    finally:
        restarted.close()


def test_expiry_guard_failure_preserves_ready_marker_across_close_and_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "replay.sqlite3"
    marker = path.with_name("replay.sqlite3.ready")
    cache = PersistentReplayCache(path, initialized_at=1_000)
    cache.mark_serving(pid=os.getpid(), ready_until_timestamp=1_101)

    class _FailingMarkerLock:
        def __enter__(self) -> None:
            raise OSError("synthetic marker guard failure")

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(
        hmac_module,
        "_marker_file_lock",
        lambda _path: _FailingMarkerLock(),
    )
    with pytest.raises(RuntimeError, match="REPLAY_READINESS_MARKER_INVALID"):
        cache.mark_activation_expired(1_101)

    monkeypatch.undo()
    cache.close()
    assert marker.exists()
    with pytest.raises(RuntimeError, match="REPLAY_UNCLEAN_SHUTDOWN_REVIEW_REQUIRED"):
        PersistentReplayCache(path, initialized_at=900)


def test_expiry_tombstone_is_merged_after_database_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "replay.sqlite3"
    marker = path.with_name("replay.sqlite3.ready")
    cache = PersistentReplayCache(path, initialized_at=1_000)
    cache.mark_serving(pid=os.getpid(), ready_until_timestamp=1_101)
    real_connect = hmac_module.sqlite3.connect
    monkeypatch.setattr(
        hmac_module.sqlite3,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(sqlite3.OperationalError()),
    )
    try:
        with pytest.raises(RuntimeError, match="REPLAY_STORE_INVALID"):
            cache.mark_activation_expired(1_101)
        assert marker.read_text(encoding="ascii") == "v1-expired 1101\n"
    finally:
        cache.close()
        monkeypatch.setattr(hmac_module.sqlite3, "connect", real_connect)

    assert marker.exists()
    restarted = PersistentReplayCache(path, initialized_at=900)
    try:
        assert restarted.clock_floor_timestamp(900) == 1_101
        assert not marker.exists()
    finally:
        restarted.close()


def test_committed_expiry_floor_survives_cleanup_interruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "replay.sqlite3"
    marker = path.with_name("replay.sqlite3.ready")
    cache = PersistentReplayCache(path, initialized_at=1_000)
    cache.mark_serving(pid=os.getpid(), ready_until_timestamp=1_101)
    monkeypatch.setattr(
        hmac_module,
        "_remove_expiry_tombstone_unlocked",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("synthetic cleanup interruption")
        ),
    )
    cache.mark_activation_expired(1_101)
    cache.close()
    assert marker.read_text(encoding="ascii") == "v1-expired 1101\n"

    monkeypatch.undo()
    restarted = PersistentReplayCache(path, initialized_at=900)
    try:
        assert restarted.clock_floor_timestamp(900) == 1_101
        assert not marker.exists()
    finally:
        restarted.close()


def test_malformed_expiry_tombstone_rejects_gateway_restart(tmp_path: Path) -> None:
    path = tmp_path / "replay.sqlite3"
    first = PersistentReplayCache(path, initialized_at=1_000)
    first.close()
    marker = path.with_name("replay.sqlite3.ready")
    marker.write_text("v1-expired invalid\n", encoding="ascii")
    marker.chmod(0o600)

    with pytest.raises(RuntimeError, match="REPLAY_READINESS_MARKER_INVALID"):
        PersistentReplayCache(path, initialized_at=900)

    assert marker.exists()


def test_verifier_ceil_rounds_subsecond_activation_expiry() -> None:
    recorded: list[int] = []

    class RecordingReplayCache(ReplayCache):
        def mark_activation_expired(self, expires_at_timestamp: int) -> None:
            recorded.append(expires_at_timestamp)

    verifier = GatewayHmacVerifier(
        GatewaySettings.for_test(),
        replay_cache=RecordingReplayCache(),
    )

    verifier.mark_activation_expired(datetime.fromtimestamp(1_100.1, tz=UTC))

    assert recorded == [1_101]


def test_readiness_marker_ceil_rounds_subsecond_activation_expiry(
    tmp_path: Path,
) -> None:
    path = tmp_path / "replay.sqlite3"
    settings = GatewaySettings.for_test().model_copy(
        update={
            "environment": hmac_module.Environment.PRODUCTION,
            "replay_store_path": path,
        }
    )
    verifier = GatewayHmacVerifier(settings, clock=lambda: 1_000.0)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = int(listener.getsockname()[1])
    marker = path.with_name("replay.sqlite3.ready")
    try:
        verifier.bind_readiness_expiry(datetime.fromtimestamp(1_100.1, tz=UTC))
        verifier.mark_serving()

        assert marker.read_text(encoding="ascii") == f"v1 {os.getpid()} 1101\n"
        assert gateway_process_is_ready(path, now=1_100.9, port=port)
        assert not gateway_process_is_ready(path, now=1_101, port=port)
        assert not marker.exists()
    finally:
        listener.close()
        verifier.close()


def test_double_persistence_failure_preserves_ready_marker_and_blocks_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "replay.sqlite3"
    marker = path.with_name("replay.sqlite3.ready")
    cache = PersistentReplayCache(path, initialized_at=1_000)
    cache.mark_serving(pid=os.getpid(), ready_until_timestamp=1_101)
    monkeypatch.setattr(
        hmac_module,
        "_replace_marker_with_expiry_tombstone_unlocked",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("synthetic marker failure")),
    )
    monkeypatch.setattr(
        hmac_module.sqlite3,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(sqlite3.OperationalError()),
    )

    with pytest.raises(RuntimeError, match="REPLAY_STORE_INVALID"):
        cache.mark_activation_expired(1_101)
    cache.close()

    assert marker.read_text(encoding="ascii") == f"v1 {os.getpid()} 1101\n"
    monkeypatch.undo()
    with pytest.raises(RuntimeError, match="REPLAY_UNCLEAN_SHUTDOWN_REVIEW_REQUIRED"):
        PersistentReplayCache(path, initialized_at=900)
    assert marker.exists()


def test_marker_mutations_are_serialized_around_expiry_latch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "replay.sqlite3"
    cache = PersistentReplayCache(path, initialized_at=1_000)
    cache.mark_serving(pid=os.getpid(), ready_until_timestamp=1_101)
    entered = threading.Event()
    release = threading.Event()
    original_replace = hmac_module._replace_marker_with_expiry_tombstone_unlocked

    def paused_replace(marker_path: Path, expires_at_timestamp: int) -> None:
        entered.set()
        assert release.wait(timeout=1)
        original_replace(marker_path, expires_at_timestamp)

    monkeypatch.setattr(
        hmac_module,
        "_replace_marker_with_expiry_tombstone_unlocked",
        paused_replace,
    )
    expiry = threading.Thread(target=cache.mark_activation_expired, args=(1_101,))
    not_ready = threading.Thread(target=cache.mark_not_ready)
    expiry.start()
    assert entered.wait(timeout=1)
    not_ready.start()
    assert not_ready.is_alive()
    release.set()
    expiry.join(timeout=1)
    not_ready.join(timeout=1)

    try:
        assert not expiry.is_alive()
        assert not not_ready.is_alive()
        assert cache.clock_floor_timestamp(900) == 1_101
    finally:
        release.set()
        cache.close()


def test_unclean_ready_marker_requires_review_before_restart(tmp_path: Path) -> None:
    path = tmp_path / "replay.sqlite3"
    cache = PersistentReplayCache(path, initialized_at=1_000)
    cache.mark_serving(pid=os.getpid(), ready_until_timestamp=2_000)
    lease_fd = cache._lease_fd  # noqa: SLF001 - model a process crash without cleanup.
    assert lease_fd is not None
    cache._lease_fd = None  # noqa: SLF001
    os.close(lease_fd)

    with pytest.raises(RuntimeError, match="REPLAY_UNCLEAN_SHUTDOWN_REVIEW_REQUIRED"):
        PersistentReplayCache(path, initialized_at=1_100)


def test_cli_expiry_failure_terminates_the_marker_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "replay.sqlite3"
    cache = PersistentReplayCache(path, initialized_at=1_000)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = int(listener.getsockname()[1])
    signals: list[tuple[int, int]] = []
    try:
        cache.mark_serving(pid=os.getpid(), ready_until_timestamp=1_100)
        monkeypatch.setattr(
            readiness_module,
            "latch_activation_expiry_while_marker_locked",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("synthetic durable latch failure")
            ),
        )
        monkeypatch.setattr(
            readiness_module.os,
            "kill",
            lambda pid, sig: signals.append((pid, sig)),
        )

        assert not gateway_process_is_ready(path, now=1_100, port=port)
        assert signals == [
            (os.getpid(), 0),
            (os.getpid(), readiness_module.signal.SIGKILL),
        ]
        assert path.with_name("replay.sqlite3.ready").exists()

        lease_fd = cache._lease_fd  # noqa: SLF001 - simulate SIGKILL without cleanup.
        assert lease_fd is not None
        cache._lease_fd = None  # noqa: SLF001
        os.close(lease_fd)
        monkeypatch.undo()
        with pytest.raises(RuntimeError, match="REPLAY_UNCLEAN_SHUTDOWN_REVIEW_REQUIRED"):
            PersistentReplayCache(path, initialized_at=900)
    finally:
        listener.close()
        cache.close()


def test_cli_expiry_guard_prevents_concurrent_close_from_erasing_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "replay.sqlite3"
    marker = path.with_name("replay.sqlite3.ready")
    cache = PersistentReplayCache(path, initialized_at=1_000)
    cache.mark_serving(pid=os.getpid(), ready_until_timestamp=1_100)
    latch_entered = threading.Event()
    release_latch = threading.Event()
    killed = threading.Event()
    probe_results: list[bool] = []

    def failing_latch(_path: Path, _ready_until: int) -> int:
        latch_entered.set()
        assert release_latch.wait(timeout=1)
        raise RuntimeError("synthetic double persistence failure")

    def simulated_kill(_pid: int, signal_number: int) -> None:
        if signal_number == readiness_module.signal.SIGKILL:
            killed.set()

    def simulated_owner_remove(marker_path: Path) -> None:
        # The real kernel never resumes a SIGKILLed process. Acquiring the same
        # guard here models a close() already waiting at that exact boundary.
        with hmac_module.readiness_marker_lock(marker_path):
            assert killed.is_set()

    monkeypatch.setattr(
        readiness_module,
        "latch_activation_expiry_while_marker_locked",
        failing_latch,
    )
    monkeypatch.setattr(readiness_module.os, "kill", simulated_kill)
    monkeypatch.setattr(hmac_module, "_remove_ready_marker", simulated_owner_remove)

    probe = threading.Thread(
        target=lambda: probe_results.append(gateway_process_is_ready(path, now=1_100)),
        name="expiry-health-probe",
    )
    close = threading.Thread(target=cache.close, name="concurrent-owner-close")
    probe.start()
    assert latch_entered.wait(timeout=1)
    close.start()
    assert close.is_alive()
    release_latch.set()
    probe.join(timeout=1)
    close.join(timeout=1)

    assert probe_results == [False]
    assert killed.is_set()
    assert not probe.is_alive()
    assert not close.is_alive()
    assert marker.exists()

    monkeypatch.undo()
    with pytest.raises(RuntimeError, match="REPLAY_UNCLEAN_SHUTDOWN_REVIEW_REQUIRED"):
        PersistentReplayCache(path, initialized_at=900)


def test_default_process_readiness_requires_both_principal_listeners(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A healthy Broker port must not hide a failed Provider listener."""
    path = tmp_path / "replay.sqlite3"
    cache = PersistentReplayCache(path, initialized_at=1_000)
    attempted_ports: list[int] = []

    def connect(address: tuple[str, int], *, timeout: float):
        del timeout
        attempted_ports.append(address[1])
        if address[1] == 8082:
            raise OSError("synthetic market listener unavailable")
        return nullcontext()

    monkeypatch.setattr(socket, "create_connection", connect)
    try:
        cache.mark_serving(pid=os.getpid(), ready_until_timestamp=2_000)
        assert not gateway_process_is_ready(path, now=1_061)
    finally:
        cache.close()

    assert attempted_ports == [8081, 8082]


@pytest.mark.asyncio
async def test_clock_floor_survives_gc_and_blocks_rollback_replay(tmp_path: Path) -> None:
    path = tmp_path / "replay.sqlite3"
    first = PersistentReplayCache(path, initialized_at=1_000)
    try:
        assert await first.reserve("key-1", "nonce-1", 1_100, expires_at=1_160)
        assert await first.reserve("key-1", "nonce-2", 1_161, expires_at=1_221)
    finally:
        first.close()

    rolled_back = PersistentReplayCache(path, initialized_at=900)
    try:
        assert rolled_back.ready_after_timestamp >= 1_161
        assert not await rolled_back.reserve(
            "key-1",
            "nonce-1",
            1_100,
            expires_at=1_160,
        )
    finally:
        rolled_back.close()


def test_every_restart_reestablishes_drain_for_restored_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "replay.sqlite3"
    first = PersistentReplayCache(path, initialized_at=1_000)
    first.close()

    restored = PersistentReplayCache(path, initialized_at=1_200)
    try:
        assert restored.ready_after_timestamp == 1_261
    finally:
        restored.close()


@pytest.mark.parametrize("extra_schema", (False, True), ids=("missing-pk", "extra-table"))
def test_structurally_drifted_replay_store_fails_closed(
    tmp_path: Path,
    extra_schema: bool,
) -> None:
    path = tmp_path / "replay.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(
            f"""
            PRAGMA application_id = {REPLAY_APPLICATION_ID};
            PRAGMA user_version = 1;
            CREATE TABLE gateway_replay_metadata (
              key TEXT NOT NULL,
              value INTEGER NOT NULL
            );
            CREATE TABLE gateway_replay_nonces (
              key_id TEXT NOT NULL,
              nonce TEXT NOT NULL,
              expires_at REAL NOT NULL
            );
            INSERT INTO gateway_replay_metadata VALUES
              ('clock_floor_timestamp', 1000),
              ('ready_after_timestamp', 1061);
            """
        )
        if extra_schema:
            connection.execute("CREATE TABLE unexpected_table (value TEXT)")

    with pytest.raises(RuntimeError, match="REPLAY_STORE_INVALID"):
        PersistentReplayCache(path, initialized_at=1_100)
    assert not replay_store_is_ready(path, now=2_000)
