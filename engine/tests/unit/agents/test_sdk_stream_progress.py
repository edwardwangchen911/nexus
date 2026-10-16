"""Observe delivered SDK notifications without consuming or retaining their data."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nexus.agents import sdk_stream_progress as progress


@pytest.mark.asyncio
async def test_observer_preserves_reader_identity_result_and_private_payload(monkeypatch):
    clock = [10.0]
    monkeypatch.setattr(progress, "perf_counter", lambda: clock[0])
    events = [
        SimpleNamespace(method="item/agentMessage/delta", payload="private-body-canary"),
        SimpleNamespace(method="private-method-canary", payload="private-account-canary"),
        SimpleNamespace(method="turn/completed", payload="private-terminal-canary"),
    ]
    reader = AsyncMock(side_effect=events)
    transport = SimpleNamespace(next_turn_notification=reader)
    client = SimpleNamespace(_client=transport)
    observer = progress.SdkStreamObserver()
    with progress.observe_sdk_notifications(client, observer):
        for i, event in enumerate(events):
            clock[0] = 11.0 + i
            assert await transport.next_turn_notification("private-turn-canary") is event
    assert transport.next_turn_notification is reader
    snapshot = observer.snapshot().model_dump(mode="json")
    assert snapshot == {
        "available": True,
        "notifications": 3,
        "progress_notifications": 1,
        "first_notification_ms": 1000,
        "last_notification_ms": 3000,
        "terminal_notification_seen": True,
        "reasoning_notifications": 0,
        "answer_notifications": 1,
    }
    assert "private" not in json.dumps(snapshot)
    assert reader.await_count == 3


@pytest.mark.asyncio
async def test_exception_and_late_delivery_do_not_change_observed_deadline(monkeypatch):
    clock = [10.0]
    monkeypatch.setattr(progress, "perf_counter", lambda: clock[0])
    event = SimpleNamespace(method="item/reasoning/textDelta", payload="private-canary")
    reader = AsyncMock(side_effect=[TimeoutError("private-failure"), event])
    transport = SimpleNamespace(next_turn_notification=reader)
    observer = progress.SdkStreamObserver()
    with pytest.raises(TimeoutError, match="private-failure"):
        with progress.observe_sdk_notifications(SimpleNamespace(_client=transport), observer):
            wrapped = transport.next_turn_notification
            await wrapped("turn")
    clock[0] = 100.0
    assert await wrapped("turn") is event
    assert transport.next_turn_notification is reader
    assert observer.snapshot().notifications == 0
    assert observer.snapshot().first_notification_ms is None


def test_unavailable_reader_is_unknown_not_observed_zero():
    observer = progress.SdkStreamObserver()
    with progress.observe_sdk_notifications(SimpleNamespace(), observer):
        pass
    assert not observer.snapshot().available
    assert observer.snapshot().first_notification_ms is None


def test_old_timing_remains_readable_and_progress_rejects_raw_fields():
    from nexus.agents.committee import SdkStreamProgress, StageInvocationTiming
    from pydantic import ValidationError

    assert StageInvocationTiming(elapsed_ms=0, phase_elapsed_ms={}).stream_progress is None
    old = SdkStreamProgress(available=True, progress_notifications=5)
    assert old.reasoning_notifications is None
    assert old.answer_notifications is None
    assert "answer_notifications" not in old.model_dump(mode="json")
    timing = StageInvocationTiming(elapsed_ms=0, phase_elapsed_ms={})
    assert timing.committee_remaining_ms is None
    assert timing.stage_preparation_ms is None
    assert "committee_remaining_ms" not in timing.model_dump(mode="json")
    with pytest.raises(ValidationError):
        SdkStreamProgress(available=True, raw_payload="private-canary")


@pytest.mark.asyncio
async def test_reasoning_and_answer_progress_are_distinct_without_reading_payload():
    class Notification:
        def __init__(self, method):
            self.method = method

        @property
        def payload(self):
            pytest.fail("observer accessed private payload")

    events = [
        Notification(method)
        for method in (
            "item/reasoning/textDelta",
            "item/reasoning/summaryTextDelta",
            "item/agentMessage/delta",
            "item/completed",
            "turn/completed",
        )
    ]
    transport = SimpleNamespace(next_turn_notification=AsyncMock(side_effect=events))
    observer = progress.SdkStreamObserver()
    with progress.observe_sdk_notifications(SimpleNamespace(_client=transport), observer):
        for event in events:
            assert await transport.next_turn_notification("private-turn") is event
    result = observer.snapshot()
    assert result.progress_notifications == 3
    assert result.reasoning_notifications == 2
    assert result.answer_notifications == 1
    assert result.terminal_notification_seen


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", [False, True, "local_deadline"])
async def test_normal_invocation_uses_pinned_sdk_collector_and_restores_reader(monkeypatch, fail):
    import asyncio
    from unittest.mock import Mock

    from nexus.agents import codex_sdk
    from openai_codex.api import AsyncThread, AsyncTurnHandle
    from openai_codex.generated.v2_all import (
        ItemCompletedNotification,
        ThreadTokenUsageUpdatedNotification,
        TurnCompletedNotification,
    )
    from openai_codex.models import Notification
    from unit.agents.test_codex_sdk import _batch, _sdk

    client, _, _, _ = _sdk(monkeypatch)
    client._ensure_initialized = AsyncMock()
    transport = client._client
    transport.register_turn_notifications = Mock()
    transport.unregister_turn_notifications = Mock()
    usage = {
        "input_tokens": 300,
        "output_tokens": 80,
        "cached_input_tokens": 0,
        "reasoning_output_tokens": 0,
        "total_tokens": 380,
    }
    events = [
        Notification(
            "item/completed",
            ItemCompletedNotification.model_validate(
                {
                    "thread_id": "thread",
                    "turn_id": "turn",
                    "completed_at_ms": 1,
                    "item": {"type": "agentMessage", "id": "message", "text": json.dumps(_batch())},
                }
            ),
        ),
        Notification(
            "thread/tokenUsage/updated",
            ThreadTokenUsageUpdatedNotification.model_validate(
                {
                    "thread_id": "thread",
                    "turn_id": "turn",
                    "token_usage": {"last": usage, "total": usage},
                }
            ),
        ),
        TimeoutError("private-upstream")
        if fail is True
        else Notification(
            "turn/completed",
            TurnCompletedNotification.model_validate(
                {
                    "thread_id": "thread",
                    "turn": {"id": "turn", "items": [], "status": "completed"},
                }
            ),
        ),
    ]
    reader = AsyncMock(side_effect=events)
    if fail == "local_deadline":
        closed = asyncio.Event()
        original_timeout_at = asyncio.timeout_at
        timers = []

        def track_timer(deadline):
            timer = original_timeout_at(deadline)
            timers.append(timer)
            return timer

        monkeypatch.setattr(asyncio, "timeout_at", track_timer)
        index = 0

        async def pending_reader(turn_id):
            nonlocal index
            event = events[index]
            index += 1
            if index == 3:
                # Trigger the real timer only once the real collector is
                # waiting. No fragile subsecond wall-clock window.
                timers[0].reschedule(asyncio.get_running_loop().time())
                await closed.wait()
            return event

        reader.side_effect = pending_reader
        client.close.side_effect = closed.set
    transport.next_turn_notification = reader
    turn_start = AsyncMock(return_value=AsyncTurnHandle(client, "thread", "turn"))
    monkeypatch.setattr(AsyncThread, "turn", turn_start)
    client.thread_start.return_value = AsyncThread(client, "thread")
    observer = progress.SdkStreamObserver()
    if fail:
        with pytest.raises(codex_sdk.CodexInvocationError) as caught:
            await codex_sdk.invoke_codex({}, timeout_seconds=45, stream_observer=observer)
        assert caught.value.kind == "timeout"
        assert caught.value.provider_call_started is True
        assert caught.value.usage is None
        assert caught.value.timeout_source == (
            "stage_deadline" if fail == "local_deadline" else "unknown"
        )
        assert "private" not in str(caught.value)
    else:
        body, measured = await codex_sdk.invoke_codex(
            {}, timeout_seconds=45, stream_observer=observer
        )
        assert json.loads(body) == _batch()
        assert measured.input_tokens == 300 and measured.output_tokens == 80
    assert observer.snapshot().notifications == (2 if fail else 3)
    assert observer.snapshot().terminal_notification_seen is (not fail)
    assert transport.next_turn_notification is reader
    turn_start.assert_awaited_once()
    client.close.assert_awaited_once()
    transport.register_turn_notifications.assert_called_once_with("turn")
    transport.unregister_turn_notifications.assert_called_once_with("turn")


@pytest.mark.asyncio
async def test_committee_failure_records_progress_without_settling_unknown_usage(monkeypatch):
    import asyncio

    from nexus.agents import codex_staged
    from nexus.agents.execution import _analyze_with_one_retry
    from unit.agents.test_codex_sdk import _authority, _sdk, _tools

    client, thread, _, _ = _sdk(monkeypatch)
    client._client.next_turn_notification = AsyncMock(
        return_value=SimpleNamespace(
            method="item/reasoning/textDelta", payload="private-model-canary"
        )
    )

    async def incomplete_turn(*args, **kwargs):
        await client._client.next_turn_notification("private-turn")
        raise TimeoutError("private-error-canary")

    thread.run.side_effect = incomplete_turn
    request, evidence, prompts = _authority()
    committee = codex_staged.StagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        stage_time_budget=True,
        deadline=asyncio.get_running_loop().time() + 120,
    )
    run = SimpleNamespace(retry_count=0)
    assert (
        await _analyze_with_one_retry(
            AsyncMock(),
            run,
            committee=committee,
            role="orchestrator",
            request=request,
            evidence_refs=evidence,
            prompt=prompts["orchestrator"],
        )
        is None
    )
    snapshot = committee.failure_diagnostic.model_dump(mode="json")
    assert snapshot["timing"]["stream_progress"]["progress_notifications"] == 1
    assert snapshot["timing"]["stream_progress"]["terminal_notification_seen"] is False
    assert "private" not in json.dumps(snapshot)
    assert committee.failure_reason == "model_usage_uncertain"
    assert run.retry_count == 0 and committee.usage.provider_calls == 1
    assert committee.usage.input_tokens == 0  # Unknown is not a reported zero usage.
    thread.run.assert_awaited_once()
    client.close.assert_awaited_once()
