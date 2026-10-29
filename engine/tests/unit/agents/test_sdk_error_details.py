"""Keep safe provider/RPC diagnostics without retaining private error bodies."""

import json
from unittest.mock import AsyncMock

import pytest
from nexus.agents import codex_sdk, codex_staged
from nexus.agents.committee import ROLE_ORDER, SdkFailureFacts, StageFailureDiagnostic
from nexus.agents.sdk_stream_progress import SdkStreamObserver
from openai_codex._run import _collect_async_turn_result
from openai_codex.errors import JsonRpcError
from openai_codex.generated.v2_all import TurnCompletedNotification
from openai_codex.models import Notification
from unit.agents.test_codex_sdk import _authority, _sdk, _staged_batch, _tools


def terminal(message, info="other", *, turn_id="turn"):
    return Notification(
        "turn/completed",
        TurnCompletedNotification.model_validate(
            {
                "thread_id": "thread",
                "turn": {
                    "id": turn_id,
                    "items": [],
                    "status": "failed",
                    "error": {
                        "message": message,
                        "additional_details": "PRIVATE_DETAIL",
                        "codex_error_info": info,
                    },
                },
            }
        ),
    )


@pytest.mark.parametrize(
    "message,expected",
    [
        (
            '{"error":{"code":"rate_limit_exceeded","message":"PRIVATE_BODY"}}',
            "rate_limit_exceeded",
        ),
        ('{"code":"server_error","message":"PRIVATE_BODY"}', "server_error"),
        ('{"error":{"code":"invalid_prompt","param":"PRIVATE_PARAM"}}', "invalid_prompt"),
        ('{"error":{"code":"PRIVATE_UNKNOWN_CODE"}}', None),
        ('{"message":"rate_limit_exceeded PRIVATE_BODY"}', None),
        ('PRIVATE_TEXT {"error":{"code":"server_error"}}', None),
        ('{"error":{"code":"server_error"},"error":{"code":"invalid_prompt"}}', None),
        ('{"error":{"code":"server_error","code":"invalid_prompt"}}', None),
        ('{"error":{"code":"server_error"},"padding":"' + "x" * 8192 + '"}', None),
        ("[" * 1000 + "0" + "]" * 1000, None),
        ("\ud800", None),
    ],
    ids=[
        "envelope",
        "direct",
        "prompt",
        "unknown",
        "quoted",
        "prose",
        "duplicate-envelope",
        "duplicate-code",
        "oversize",
        "deep",
        "invalid-unicode",
    ],
)
def test_terminal_error_retains_only_bounded_structured_allowlisted_code(message, expected):
    observer = SdkStreamObserver()
    observer._active = True
    observer.observe(terminal(message), "turn")
    facts = observer.failure_facts
    assert facts.provider_error_code == expected
    assert facts.provider_reason == "other"
    assert "PRIVATE" not in facts.model_dump_json()
    assert observer.measured_usage is None


@pytest.mark.parametrize("status,expected", [(429, 429), (503, 503), (0, None), (999, None)])
def test_typed_http_status_is_retained_without_parsing_error_prose(status, expected):
    observer = SdkStreamObserver()
    observer._active = True
    observer.observe(
        terminal("PRIVATE_BODY", {"responseStreamDisconnected": {"httpStatusCode": status}}),
        "turn",
    )
    assert observer.failure_facts.http_status_code == expected
    assert observer.failure_facts.provider_reason == "responseStreamDisconnected"
    assert "PRIVATE" not in observer.failure_facts.model_dump_json()


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_invalid_json_numeric_constant_does_not_become_a_provider_code(constant):
    observer = SdkStreamObserver()
    observer._active = True
    observer.observe(
        terminal('{"error":{"code":"server_error","message":' + constant + "}}"), "turn"
    )
    assert observer.failure_facts.provider_error_code is None


def test_wrong_turn_cannot_supply_provider_details():
    observer = SdkStreamObserver()
    observer._active = True
    observer.observe(terminal('{"error":{"code":"server_error"}}', turn_id="other"), "turn")
    assert observer.failure_facts is None


@pytest.mark.parametrize("code", [True, "-32602", 10000000, -99999])
@pytest.mark.asyncio
async def test_rpc_unrecognized_numeric_field_stays_unknown(monkeypatch, code):
    _, thread, _, _ = _sdk(monkeypatch)
    thread.run.side_effect = JsonRpcError(code, "PRIVATE_MESSAGE")
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({})
    assert caught.value.failure_facts.rpc_error_code is None
    assert caught.value.failure_facts.exception_kind == "rpc_error"


@pytest.mark.parametrize(
    "field", ["http_status_code", "rpc_error_code", "cleanup_rpc_error_code", "provider_error_code"]
)
def test_constructed_private_detail_cannot_bypass_persistence_validation(field):
    from nexus.agents.portfolio_synthesis_diagnostics import freeze_failure_observation

    invalid = SdkFailureFacts.model_construct(**{field: "PRIVATE_VALUE"})
    observed = freeze_failure_observation({"failure_facts": invalid})
    assert observed.local == "diagnostic_unavailable"
    assert observed.failure_facts is None
    assert "PRIVATE" not in observed.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase", ["client_init", "account", "config_read", "thread_start", "turn_run", "client_close"]
)
async def test_rpc_code_survives_every_invocation_phase_without_message_or_data(monkeypatch, phase):
    client, thread, _, _ = _sdk(monkeypatch)
    original = JsonRpcError(-32602, "PRIVATE_MESSAGE", {"account": "PRIVATE_DATA"})
    if phase == "client_init":

        def fail(config):
            raise original

        monkeypatch.setattr(codex_sdk, "AsyncCodex", fail)
    else:
        target = {
            "account": client.account,
            "config_read": client._client.request,
            "thread_start": client.thread_start,
            "turn_run": thread.run,
            "client_close": client.close,
        }[phase]
        target.side_effect = original
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({})
    assert caught.value.phase == phase
    assert caught.value.failure_facts.rpc_error_code == -32602
    assert caught.value.failure_facts.exception_kind == "rpc_error"
    assert "PRIVATE" not in caught.value.failure_facts.model_dump_json()


@pytest.mark.asyncio
async def test_cleanup_rpc_code_does_not_replace_original_rpc_code(monkeypatch):
    client, thread, _, _ = _sdk(monkeypatch)
    thread.run.side_effect = JsonRpcError(-32602, "PRIVATE_FIRST")
    client.close.side_effect = JsonRpcError(-32603, "PRIVATE_CLEANUP")
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({})
    facts = caught.value.failure_facts
    assert caught.value.phase == "turn_run"
    assert facts.rpc_error_code == -32602
    assert facts.cleanup_rpc_error_code == -32603
    assert facts.cleanup_kind == facts.exception_kind == "rpc_error"
    assert caught.value.usage is None
    assert "PRIVATE" not in facts.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_stage", ["research", "critique", "synthesis"])
async def test_detail_survives_real_sdk_collector_stage_diagnostic_and_cleanup(
    monkeypatch, failed_stage
):
    client, thread, result, _ = _sdk(monkeypatch)
    client._client.next_turn_notification = AsyncMock(
        return_value=terminal(
            '{"error":{"code":"rate_limit_exceeded","message":"PRIVATE_PROVIDER"}}',
            {"httpConnectionFailed": {"httpStatusCode": 429}},
        )
    )
    calls = []

    async def run(wire, **kwargs):
        payload = json.loads(wire)
        calls.append(payload["stage"])
        if payload["stage"] != failed_stage:
            result.final_response = json.dumps(
                _staged_batch(payload["stage"], payload["requested_roles"])
            )
            return result
        client.close.side_effect = JsonRpcError(-32603, "PRIVATE_CLEANUP")

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
    assert diagnostic.failure_facts.provider_error_code == "rate_limit_exceeded"
    assert diagnostic.failure_facts.http_status_code == 429
    assert diagnostic.failure_facts.cleanup_rpc_error_code == -32603
    assert diagnostic.failure_facts.rpc_error_code is None
    assert model.failure_reason == "model_usage_uncertain"
    assert (
        calls
        == ["research", "critique", "synthesis"][
            : ["research", "critique", "synthesis"].index(failed_stage) + 1
        ]
    )
    encoded = diagnostic.model_dump_json()
    assert "PRIVATE" not in encoded
    assert StageFailureDiagnostic.model_validate_json(encoded) == diagnostic


def test_historical_diagnostic_does_not_invent_new_details():
    facts = SdkFailureFacts.model_validate(
        {"exception_kind": "runtime_error", "provider_reason": "other"}
    )
    assert facts.http_status_code is facts.rpc_error_code is facts.provider_error_code is None
    assert facts.cleanup_rpc_error_code is None


def test_historical_failure_hash_input_is_byte_for_byte_compatible():
    from nexus.agents.portfolio_synthesis_diagnostics import SynthesisFailureDiagnostic

    legacy = {
        "schema_version": "portfolio_synthesis_failure_v1",
        "run_id": "00000000-0000-0000-0000-000000000001",
        "job_id": "00000000-0000-0000-0000-000000000002",
        "input_hash": "a" * 64,
        "output_id": "00000000-0000-0000-0000-000000000003",
        "binding_hash": "b" * 64,
        "attempt_id": None,
        "attempt_no": None,
        "fence_token": None,
        "observation": {
            "invocation_phase": "turn_run",
            "invocation_kind": "unexpected_exception",
            "timeout_source": None,
            "local": None,
            "failure_facts": {
                "exception_kind": "runtime_error",
                "cleanup_kind": None,
                "terminal_status": "failed",
                "provider_reason": "other",
            },
        },
    }
    actual = SynthesisFailureDiagnostic.model_validate(legacy).model_dump(mode="json")
    assert json.dumps(actual, sort_keys=True) == json.dumps(legacy, sort_keys=True)
