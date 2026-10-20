"""Portfolio v2 keeps one absolute work/cleanup envelope and trustworthy usage."""

import asyncio
from types import SimpleNamespace

import pytest
from nexus.agents import codex_sdk
from nexus.agents.responses_budget import UsageSnapshot
from openai_codex.generated.v2_all import TurnStatus
from unit.agents.test_codex_sdk import _sdk


def envelope():
    now = asyncio.get_running_loop().time()
    return dict(
        lifecycle_version="portfolio_synthesis_job_v2",
        deadline_at=now + 80,
        cleanup_deadline_at=now + 87,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"lifecycle_version": "unknown"},
        {"deadline_at": None},
        {"cleanup_deadline_at": None},
        {"deadline_at": True},
        {"deadline_at": float("inf")},
        {"cleanup_deadline_at": float("nan")},
        {"lifecycle_version": None},
    ],
)
async def test_invalid_v2_never_constructs_client(monkeypatch, change):
    client, _, _, configs = _sdk(monkeypatch)
    with pytest.raises(ValueError, match="CODEX_PORTFOLIO_LIFECYCLE_INVALID"):
        await codex_sdk.invoke_codex_portfolio({}, **(envelope() | change))
    assert configs == []
    client.thread_start.assert_not_awaited()


@pytest.mark.asyncio
async def test_v2_records_usage_before_cleanup_failure(monkeypatch):
    client, _, _, _ = _sdk(monkeypatch)
    usages = []

    async def close():
        assert usages == [UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1)]
        raise RuntimeError("private-cleanup-canary")

    client.close.side_effect = close
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex_portfolio({}, **envelope(), on_usage=usages.append)
    assert caught.value.usage == usages[0]
    assert caught.value.phase == "client_close"
    assert "private" not in str(caught.value)


@pytest.mark.asyncio
async def test_v2_incomplete_response_retains_received_usage(monkeypatch):
    _, thread, result, _ = _sdk(monkeypatch)
    result.status = TurnStatus.failed
    usages = []
    with pytest.raises(codex_sdk.CodexInvocationError, match="CODEX_RESPONSE_INCOMPLETE") as caught:
        await codex_sdk.invoke_codex_portfolio({}, **envelope(), on_usage=usages.append)
    assert usages == [caught.value.usage]
    assert usages[0].input_tokens == 300
    thread.run.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [True, 1.5, -1])
async def test_v2_invalid_usage_is_not_trusted(monkeypatch, invalid):
    _, _, result, _ = _sdk(monkeypatch)
    result.usage.total.input_tokens = invalid
    usages = []
    with pytest.raises(codex_sdk.CodexInvocationError, match="CODEX_USAGE_INVALID"):
        await codex_sdk.invoke_codex_portfolio({}, **envelope(), on_usage=usages.append)
    assert usages == []


@pytest.mark.asyncio
async def test_v2_work_deadline_and_legacy_timeout_are_distinct(monkeypatch):
    _sdk(monkeypatch)
    observed = []
    real_timeout_at = asyncio.timeout_at

    def timer(at):
        observed.append(at)
        return real_timeout_at(at)

    monkeypatch.setattr(asyncio, "timeout_at", timer)
    limits = envelope()
    raw, usage = await codex_sdk.invoke_codex_portfolio({}, **limits)
    assert raw and usage.provider_calls == 1
    assert observed == [limits["deadline_at"]]
    await codex_sdk.invoke_codex_portfolio({})
    assert len(observed) == 1  # legacy still uses its own 45-second relative timer


@pytest.mark.asyncio
@pytest.mark.parametrize("responded", [False, True])
async def test_v2_repeated_parent_cancel_finishes_cleanup_then_propagates(monkeypatch, responded):
    client, thread, result, _ = _sdk(monkeypatch)
    run_started, close_started, release_close = asyncio.Event(), asyncio.Event(), asyncio.Event()
    usages = []
    run_finished = asyncio.Event()
    prior_tasks = asyncio.all_tasks()

    async def run(*args, **kwargs):
        run_started.set()
        if not responded:
            await release_close.wait()
        run_finished.set()
        if not responded:
            raise ConnectionError("closed before response")
        return result

    async def close():
        close_started.set()
        await release_close.wait()

    thread.run.side_effect = run
    client.close.side_effect = close
    task = asyncio.create_task(
        codex_sdk.invoke_codex_portfolio({}, **envelope(), on_usage=usages.append)
    )
    await run_started.wait()
    if not responded:
        task.cancel()
    await close_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release_close.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert run_finished.is_set()
    assert len(usages) == (1 if responded else 0)
    assert client.close.await_count == 1
    assert not (asyncio.all_tasks() - prior_tasks)


@pytest.mark.asyncio
async def test_v2_preparation_consumes_original_deadline(monkeypatch):
    client, thread, _, _ = _sdk(monkeypatch)
    loop = asyncio.get_running_loop()
    original_time = loop.time
    offset = [0.0]
    monkeypatch.setattr(loop, "time", lambda: original_time() + offset[0])
    limits = envelope()

    original_account = client.account

    async def account(**kwargs):
        offset[0] = 81
        return await original_account(**kwargs)

    client.account.side_effect = account
    with pytest.raises((TimeoutError, codex_sdk.CodexInvocationError)):
        await codex_sdk.invoke_codex_portfolio({}, **limits)
    thread.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_v2_cancel_keeps_cancellation_when_close_fails(monkeypatch):
    client, thread, _, _ = _sdk(monkeypatch)
    started, wake = asyncio.Event(), asyncio.Event()

    async def run(*args, **kwargs):
        started.set()
        await wake.wait()
        raise ConnectionError("closed")

    async def close():
        wake.set()
        raise RuntimeError("private-close-error")

    thread.run.side_effect, client.close.side_effect = run, close
    task = asyncio.create_task(codex_sdk.invoke_codex_portfolio({}, **envelope()))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_v2_stalled_close_kills_captured_child_and_wakes_thread_waiter(monkeypatch):
    import queue

    client, thread, _, _ = _sdk(monkeypatch)
    notifications = queue.Queue()
    started, killed = asyncio.Event(), asyncio.Event()
    consumed = asyncio.Event()

    class Process:
        def poll(self):
            return 0 if killed.is_set() else None

        def kill(self):
            killed.set()

    class Router:
        def fail_all(self, error):
            notifications.put(error)

    sync = SimpleNamespace(_proc=Process(), _router=Router())
    client._client._sync = sync

    async def run(*args, **kwargs):
        started.set()
        # A timeout bounds the test even if a regression strands the real thread.
        item = await asyncio.to_thread(notifications.get, True, 3)
        consumed.set()
        raise item

    async def close():
        sync._proc = None  # The real SDK clears this before waiting for its child.
        await killed.wait()

    original_wait = asyncio.wait
    waits = []

    async def expired_close_wait(tasks, *, timeout):
        waits.append(timeout)
        if len(waits) == 1:
            await asyncio.sleep(0)
            return set(), set(tasks)
        return await original_wait(tasks, timeout=timeout)

    thread.run.side_effect, client.close.side_effect = run, close
    monkeypatch.setattr(asyncio, "wait", expired_close_wait)
    task = asyncio.create_task(codex_sdk.invoke_codex_portfolio({}, **envelope()))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert killed.is_set() and consumed.is_set()
    assert waits[0] <= codex_sdk.CLIENT_CLOSE_TIMEOUT_SECONDS


@pytest.mark.asyncio
async def test_v2_graceful_close_timeout_is_failure_even_after_successful_kill(monkeypatch):
    client, _, _, _ = _sdk(monkeypatch)
    killed = asyncio.Event()
    usages = []

    class Process:
        def poll(self):
            return 0 if killed.is_set() else None

        def kill(self):
            killed.set()

    client._client._sync = SimpleNamespace(_proc=Process())

    async def close():
        await killed.wait()

    real_wait = asyncio.wait
    waits = []

    async def expire_close(tasks, *, timeout):
        waits.append(timeout)
        if len(waits) == 1:
            await asyncio.sleep(0)
            return set(), set(tasks)
        return await real_wait(tasks, timeout=timeout)

    client.close.side_effect = close
    monkeypatch.setattr(asyncio, "wait", expire_close)
    with pytest.raises(codex_sdk.CodexInvocationError, match="CODEX_TIMEOUT") as caught:
        await codex_sdk.invoke_codex_portfolio({}, **envelope(), on_usage=usages.append)
    assert killed.is_set() and caught.value.phase == "client_close"
    assert caught.value.usage == usages[0]


@pytest.mark.asyncio
async def test_v2_late_response_does_not_reset_work_or_cleanup_deadline(monkeypatch):
    client, thread, result, _ = _sdk(monkeypatch)
    loop = asyncio.get_running_loop()
    real_time = loop.time
    offset = [0.0]
    monkeypatch.setattr(loop, "time", lambda: real_time() + offset[0])
    limits = envelope()
    waits, usages = [], []
    real_wait = asyncio.wait

    async def late(*args, **kwargs):
        offset[0] = 86
        return result

    async def observe_wait(tasks, *, timeout):
        waits.append(timeout)
        return await real_wait(tasks, timeout=timeout)

    thread.run.side_effect = late
    monkeypatch.setattr(asyncio, "wait", observe_wait)
    with pytest.raises(TimeoutError):
        await codex_sdk.invoke_codex_portfolio({}, **limits, on_usage=usages.append)
    assert usages[0].input_tokens == 300
    assert 0 <= waits[0] < 1
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_v2_response_arriving_during_cancel_cleanup_still_records_usage(monkeypatch):
    client, thread, result, _ = _sdk(monkeypatch)
    started, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    usages = []

    async def run(*args, **kwargs):
        started.set()
        await release.wait()
        return result

    async def close():
        closing.set()
        await release.wait()

    thread.run.side_effect, client.close.side_effect = run, close
    task = asyncio.create_task(
        codex_sdk.invoke_codex_portfolio({}, **envelope(), on_usage=usages.append)
    )
    await started.wait()
    task.cancel()
    await closing.wait()
    assert usages == []
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert usages == [UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1)]


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["too_long", "past", "cleanup_long", "cleanup_equal"])
async def test_v2_rejects_expanded_or_expired_absolute_envelope(monkeypatch, kind):
    _, _, _, configs = _sdk(monkeypatch)
    now = asyncio.get_running_loop().time()
    work, cleanup = {
        "too_long": (now + 84, now + 91),
        "past": (now - 1, now + 6),
        "cleanup_long": (now + 80, now + 88),
        "cleanup_equal": (now + 80, now + 80),
    }[kind]
    with pytest.raises(ValueError, match="CODEX_PORTFOLIO_LIFECYCLE_INVALID"):
        await codex_sdk.invoke_codex_portfolio(
            {},
            lifecycle_version="portfolio_synthesis_job_v2",
            deadline_at=work,
            cleanup_deadline_at=cleanup,
        )
    assert configs == []
