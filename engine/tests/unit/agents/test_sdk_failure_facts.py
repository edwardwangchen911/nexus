import pytest
from nexus.agents import codex_sdk
from openai_codex.errors import TransportClosedError
from unit.agents.test_codex_sdk import _sdk


@pytest.mark.asyncio
async def test_close_failure_does_not_hide_turn_failure(monkeypatch):
    client, thread, _, _ = _sdk(monkeypatch)
    thread.run.side_effect = TransportClosedError("private-first")
    client.close.side_effect = RuntimeError("private-close")
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({})
    error = caught.value
    assert error.phase == "turn_run"
    assert error.failure_facts.exception_kind == "transport_closed"
    assert error.failure_facts.cleanup_kind == "runtime_error"
    assert "private" not in error.failure_facts.model_dump_json()
    thread.run.assert_awaited_once()
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_failure_preserves_reported_usage(monkeypatch):
    client, _, result, _ = _sdk(monkeypatch)
    client.close.side_effect = RuntimeError("private-close")
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({})
    assert caught.value.usage is not None
    assert caught.value.usage.input_tokens == result.usage.total.input_tokens


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_stage", ["research", "critique", "synthesis"])
@pytest.mark.parametrize("close_fails", [False, True])
async def test_failed_terminal_survives_real_collector_and_all_stages(
    monkeypatch, failed_stage, close_fails
):
    import json
    from unittest.mock import AsyncMock

    from nexus.agents import codex_staged
    from nexus.agents.committee import ROLE_ORDER, StageFailureDiagnostic
    from openai_codex._run import _collect_async_turn_result
    from openai_codex.generated.v2_all import TurnCompletedNotification
    from openai_codex.models import Notification
    from unit.agents.test_codex_sdk import _authority, _staged_batch, _tools

    client, thread, result, _ = _sdk(monkeypatch)
    event = Notification(
        "turn/completed",
        TurnCompletedNotification.model_validate(
            {
                "thread_id": "thread",
                "turn": {
                    "id": "turn",
                    "items": [],
                    "status": "failed",
                    "error": {
                        "message": "private-provider-canary",
                        "codex_error_info": "usageLimitExceeded",
                    },
                },
            }
        ),
    )
    client._client.next_turn_notification = AsyncMock(return_value=event)

    async def run(wire, **kwargs):
        payload = json.loads(wire)
        if payload["stage"] != failed_stage:
            result.final_response = json.dumps(
                _staged_batch(payload["stage"], payload["requested_roles"])
            )
            return result
        if close_fails:
            client.close.side_effect = RuntimeError("private-cleanup-canary")

        async def events():
            yield await client._client.next_turn_notification("turn")

        return await _collect_async_turn_result(events(), turn_id="turn")

    thread.run.side_effect = run
    request, evidence, prompts = _authority()
    model = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    with pytest.raises(codex_sdk.CodexInvocationError):
        for role in ROLE_ORDER:
            await model.analyze(role, request, evidence, prompts[role])
    diagnostic = model.failure_diagnostic
    assert diagnostic.stage == failed_stage
    assert diagnostic.invocation_phase == "turn_run"
    assert diagnostic.failure_facts.terminal_status == "failed"
    assert diagnostic.failure_facts.provider_reason == "usageLimitExceeded"
    assert diagnostic.failure_facts.cleanup_kind == ("runtime_error" if close_fails else None)
    encoded = diagnostic.model_dump_json()
    assert "private" not in encoded
    assert StageFailureDiagnostic.model_validate_json(encoded) == diagnostic
    assert model.failure_reason == "model_usage_uncertain"
    assert thread.run.await_count == ["research", "critique", "synthesis"].index(failed_stage) + 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason", ["unauthorized", {"responseStreamDisconnected": {"httpStatusCode": 503}}]
)
async def test_terminal_usage_and_reason_are_safe_and_turn_bound(monkeypatch, reason):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from nexus.agents.sdk_stream_progress import SdkStreamObserver, observe_sdk_notifications
    from openai_codex.generated.v2_all import (
        ThreadTokenUsageUpdatedNotification,
        TurnCompletedNotification,
    )
    from openai_codex.models import Notification

    usage = dict(
        input_tokens=11,
        output_tokens=3,
        total_tokens=14,
        cached_input_tokens=0,
        reasoning_output_tokens=0,
    )
    event = Notification(
        "thread/tokenUsage/updated",
        ThreadTokenUsageUpdatedNotification.model_validate(
            {"thread_id": "x", "turn_id": "t", "token_usage": {"last": usage, "total": usage}}
        ),
    )
    terminal = Notification(
        "turn/completed",
        TurnCompletedNotification.model_validate(
            {
                "thread_id": "x",
                "turn": {
                    "id": "t",
                    "items": [],
                    "status": "failed",
                    "error": {
                        "message": "private-secret",
                        "additional_details": "private-detail",
                        "codex_error_info": reason,
                    },
                },
            }
        ),
    )
    reader = AsyncMock(side_effect=[event, terminal])
    client = SimpleNamespace(_client=SimpleNamespace(next_turn_notification=reader))
    observer = SdkStreamObserver()
    with observe_sdk_notifications(client, observer):
        await client._client.next_turn_notification("t")
        assert observer.measured_usage is None
        await client._client.next_turn_notification("t")
    assert observer.measured_usage.input_tokens == 11
    assert observer.failure_facts.provider_reason == (
        "unauthorized" if isinstance(reason, str) else "responseStreamDisconnected"
    )
    assert "private" not in observer.failure_facts.model_dump_json()
    assert client._client.next_turn_notification is reader


@pytest.mark.asyncio
async def test_rpc_category_does_not_expose_message_or_data(monkeypatch):
    from openai_codex.errors import JsonRpcError

    client, thread, _, _ = _sdk(monkeypatch)
    thread.run.side_effect = JsonRpcError(-32000, "private-message", {"secret": "private-body"})
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({})
    assert caught.value.failure_facts.exception_kind == "rpc_error"
    assert "private" not in caught.value.failure_facts.model_dump_json()
    assert caught.value.usage is None


@pytest.mark.asyncio
async def test_interrupted_terminal_survives_response_check(monkeypatch):
    from nexus.agents.sdk_stream_progress import SdkStreamObserver
    from openai_codex.generated.v2_all import TurnCompletedNotification, TurnStatus
    from openai_codex.models import Notification

    _, _, result, _ = _sdk(monkeypatch)
    result.status = TurnStatus.interrupted
    observer = SdkStreamObserver()
    observer._active = True
    observer.observe(
        Notification(
            "turn/completed",
            TurnCompletedNotification.model_validate(
                {"thread_id": "x", "turn": {"id": "t", "items": [], "status": "interrupted"}}
            ),
        ),
        "t",
    )
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({}, stream_observer=observer)
    assert caught.value.kind == "response_incomplete"
    assert caught.value.failure_facts.terminal_status == "interrupted"


@pytest.mark.asyncio
async def test_typed_cleanup_timeout_keeps_exact_category(monkeypatch):
    client, thread, _, _ = _sdk(monkeypatch)
    thread.run.side_effect = TransportClosedError("private")
    client.close.side_effect = codex_sdk._CodexInvocationTimeout(
        "CODEX_TIMEOUT", phase="client_close"
    )
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({})
    assert caught.value.phase == "turn_run"
    assert caught.value.failure_facts.cleanup_kind == "timeout"


@pytest.mark.asyncio
@pytest.mark.parametrize("portfolio", [False, True])
async def test_legacy_timeout_does_not_settle_usage_returned_during_close(monkeypatch, portfolio):
    import asyncio

    client, thread, result, _ = _sdk(monkeypatch)
    closed = asyncio.Event()
    client.close.side_effect = closed.set
    timers = []
    original_timeout = asyncio.timeout

    def capture(delay):
        timer = original_timeout(delay)
        timers.append(timer)
        return timer

    monkeypatch.setattr(asyncio, "timeout", capture)

    async def late_result(*args, **kwargs):
        timers[0].reschedule(asyncio.get_running_loop().time())
        await closed.wait()
        return result

    thread.run.side_effect = late_result
    invoke = codex_sdk.invoke_codex_portfolio if portfolio else codex_sdk.invoke_codex
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await invoke({})
    assert caught.value.kind == "timeout"
    assert caught.value.usage is None
    assert caught.value.provider_call_started is True
    thread.run.assert_awaited_once()
    client.close.assert_awaited_once()
