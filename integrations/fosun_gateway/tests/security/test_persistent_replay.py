"""Persistent replay journal and singleton lease security tests."""

import os
import socket
import sqlite3
import stat
from pathlib import Path

import pytest
from nexus_fosun_gateway.readiness import gateway_process_is_ready, replay_store_is_ready
from nexus_fosun_gateway.security.hmac import PersistentReplayCache
from nexus_fosun_gateway.security.replay_schema import REPLAY_APPLICATION_ID


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


def test_persistent_replay_store_allows_only_one_live_gateway(tmp_path: Path) -> None:
    path = tmp_path / "replay.sqlite3"
    first = PersistentReplayCache(path, initialized_at=1_000)
    try:
        first.mark_serving(pid=os.getpid())
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
        cache.mark_serving(pid=os.getpid())
        assert gateway_process_is_ready(path, now=1_061, port=port)
        listener.close()
        assert not gateway_process_is_ready(path, now=1_061, port=port)
    finally:
        listener.close()
        cache.close()


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
