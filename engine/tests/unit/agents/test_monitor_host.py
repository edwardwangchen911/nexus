"""Event-controlled monitor scheduling never blocks unrelated model work."""

import asyncio
from datetime import UTC, datetime, timedelta, timezone

import pytest
from nexus.monitoring import host

NOW = datetime(2026, 10, 26, 14, 31, 27, tzinfo=UTC)


@pytest.mark.asyncio
async def test_model_and_scan_can_progress_independently_without_overlap_or_catchup():
    model_started, model_release = asyncio.Event(), asyncio.Event()
    scan_started, scan_release, scan_done = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = 0

    async def model():
        model_started.set()
        await model_release.wait()

    async def scan():
        nonlocal calls
        calls += 1
        scan_started.set()
        await scan_release.wait()
        scan_done.set()
        return {"checked": 1}

    model_task = asyncio.create_task(model())
    monitor = host.PriceMonitorCoordinator(scan)
    try:
        await model_started.wait()
        assert monitor.kick(NOW) is True
        assert monitor.kick(NOW + timedelta(seconds=30)) is False
        await scan_started.wait()
        assert not model_task.done() and monitor.busy
        assert monitor.kick(NOW + timedelta(minutes=5)) is False
        scan_release.set()
        await scan_done.wait()
        assert not monitor.busy and not model_task.done()
        # The bucket skipped while busy is not retried after completion.
        assert monitor.kick(NOW + timedelta(minutes=6)) is False
        assert monitor.kick(NOW - timedelta(minutes=5)) is False
        assert monitor.kick(NOW + timedelta(minutes=15)) is True
        await monitor.close()
        assert calls == 2 and monitor.last_diagnostic is None
        assert monitor.kick(NOW + timedelta(hours=1)) is False
        assert not monitor.busy
    finally:
        scan_release.set()
        model_release.set()
        await monitor.close()
        await model_task


@pytest.mark.asyncio
async def test_scan_failure_is_safe_and_never_retried_in_same_bucket():
    finished = asyncio.Event()
    calls = 0

    async def scan():
        nonlocal calls
        calls += 1
        finished.set()
        raise RuntimeError("private-provider-secret")

    monitor = host.PriceMonitorCoordinator(scan)
    assert monitor.kick(NOW)
    await finished.wait()
    assert monitor.last_diagnostic == "MONITOR_SCAN_FAILED"
    assert monitor.kick(NOW + timedelta(minutes=1)) is False
    await monitor.close()
    assert calls == 1 and not monitor.busy


@pytest.mark.asyncio
async def test_total_timeout_cancels_and_collects_only_the_scan(monkeypatch):
    started, collected = asyncio.Event(), asyncio.Event()
    deadline = asyncio.timeout(None)
    requested = []

    def controlled_timeout(seconds):
        requested.append(seconds)
        return deadline

    monkeypatch.setattr(host, "timeout", controlled_timeout)

    async def scan():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            collected.set()

    monitor = host.PriceMonitorCoordinator(scan)
    assert monitor.kick(NOW)
    await started.wait()
    # Expire explicitly; no tiny real-time sleep defines correctness.
    deadline.reschedule(asyncio.get_running_loop().time())
    await collected.wait()
    await monitor.close()
    assert requested == [8.0]
    assert monitor.last_diagnostic == "MONITOR_SCAN_TIMEOUT"
    assert not monitor.busy


@pytest.mark.asyncio
async def test_close_waits_for_own_scan_and_is_idempotent():
    started, release = asyncio.Event(), asyncio.Event()

    async def scan():
        started.set()
        await release.wait()

    monitor = host.PriceMonitorCoordinator(scan)
    assert monitor.kick(NOW)
    await started.wait()
    closer = asyncio.create_task(monitor.close())
    # A competing model/background task remains independently awaitable.
    await asyncio.sleep(0)
    assert not closer.done()
    assert monitor.kick(NOW + timedelta(minutes=5)) is False
    release.set()
    await closer
    await monitor.close()
    assert not monitor.busy


@pytest.mark.asyncio
async def test_cancelled_close_collects_owned_task_without_orphans():
    started, collected = asyncio.Event(), asyncio.Event()

    async def scan():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            collected.set()

    monitor = host.PriceMonitorCoordinator(scan)
    assert monitor.kick(NOW)
    await started.wait()
    closer = asyncio.create_task(monitor.close())
    await asyncio.sleep(0)
    closer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closer
    assert collected.is_set() and not monitor.busy
    assert monitor.last_diagnostic == "MONITOR_SCAN_CANCELLED"
    await monitor.close()


@pytest.mark.asyncio
async def test_utc_bucket_normalization_and_invalid_clock():
    async def scan():
        return None

    monitor = host.PriceMonitorCoordinator(scan)
    with pytest.raises(ValueError):
        monitor.kick(NOW.replace(tzinfo=None))
    assert monitor.last_bucket is None
    assert monitor.kick(NOW.astimezone(timezone(timedelta(hours=8))))
    assert monitor.last_bucket == NOW.replace(minute=30, second=0, microsecond=0)
    await monitor.close()


@pytest.mark.parametrize("seconds", [0, -1, float("nan"), float("inf"), True, "8"])
def test_invalid_timeout_is_rejected(seconds):
    async def scan():
        pass

    with pytest.raises(ValueError, match="^MONITOR_SCAN_TIMEOUT_INVALID$"):
        host.PriceMonitorCoordinator(scan, timeout_seconds=seconds)
