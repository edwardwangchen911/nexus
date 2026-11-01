"""Safe HTTP facts survive the native failure boundary without implying usage."""

import asyncio
import json

import httpx
import pytest
from nexus.agents.committee import StageFailureDiagnostic
from nexus.agents.research_stages.http_lifecycle import HttpStageFailure
from nexus.agents.research_stages.protocol import ProtocolRejected
from unit.agents import test_platform_http_stage_lifecycle as seam
from unit.agents.test_http_stage_lifecycle import Body, Protocol


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,code",
    [
        (400, "invalid_json_schema"),
        (401, "invalid_api_key"),
        (429, "insufficient_quota"),
        (502, "server_error"),
    ],
)
async def test_http_rejection_keeps_allowlisted_cause_without_body_or_credentials(status, code):
    requests = []
    body = Body(
        json.dumps(
            {"error": {"code": code, "message": "PRIVATE_SENTINEL", "param": "PRIVATE_SENTINEL"}}
        ).encode()
    )

    def handle(request):
        requests.append(request)
        return httpx.Response(status, headers={"content-type": "application/json"}, stream=body)

    credential = seam.credential.__wrapped__()

    async def qualify():
        return credential

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    protocol = Protocol()
    with pytest.raises(HttpStageFailure) as caught:
        await seam.call(protocol, client, seam.frozen.__wrapped__(), credential, qualify)
    facts = caught.value.http_failure_facts
    assert facts is not None
    assert facts.phase == "headers" and facts.error_kind == "http_status"
    assert facts.http_status == status and facts.provider_error_code == code
    assert facts.protocol_code is None
    assert facts.send_started and facts.headers_received and facts.cleanup_confirmed
    assert caught.value.usage is None and caught.value.failure_facts is None
    diagnostic = StageFailureDiagnostic(
        stage="research",
        boundary="invocation",
        response_hash=None,
        invocation_kind=caught.value.kind,
        invocation_phase=caught.value.phase,
        http_failure_facts=facts,
    )
    serialized = diagnostic.model_dump_json()
    assert "PRIVATE_SENTINEL" not in serialized and seam.KEY not in serialized
    assert len(requests) == 1 and client.is_closed and body.closed == 1
    assert not any(name == "observe" for name, _ in protocol.events)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case,phase,kind",
    [
        ("pre_send", "pre_send", "local_guard"),
        ("connect", "send", "connection"),
        ("truncated", "stream", "stream_protocol"),
    ],
)
async def test_http_failure_phase_is_observed_not_guessed_from_generic_sdk_code(case, phase, kind):
    sent = []
    credential = seam.credential.__wrapped__()

    async def qualify():
        if case == "pre_send":
            raise ProtocolRejected("PLATFORM_HTTP_STOPPED_OR_CREDENTIAL_CHANGED")
        return credential

    def handle(request):
        sent.append(True)
        if case == "connect":
            raise httpx.ConnectError("PRIVATE_SENTINEL", request=request)
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Body(b'data: {"type":"response.created","response":{"id":"resp_fixture"}}\n\n'),
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    with pytest.raises(HttpStageFailure) as caught:
        await seam.call(Protocol(), client, seam.frozen.__wrapped__(), credential, qualify)
    facts = caught.value.http_failure_facts
    assert facts is not None and (facts.phase, facts.error_kind) == (phase, kind)
    assert facts.send_started is (case != "pre_send")
    assert facts.headers_received is (case == "truncated")
    assert len(sent) == (0 if case == "pre_send" else 1)
    assert "PRIVATE_SENTINEL" not in facts.model_dump_json()
    assert caught.value.usage is None and facts.cleanup_confirmed


def test_old_diagnostic_payload_keeps_its_exact_shape():
    original = {
        "stage": "research",
        "boundary": "invocation",
        "role": None,
        "response_hash": None,
        "invocation_phase": "response_check",
        "invocation_kind": "response_incomplete",
        "validation_kind": None,
    }
    assert StageFailureDiagnostic.model_validate(original).model_dump(mode="json") == original


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,state",
    [
        ("private_code", "parsed"),
        ("malformed", "malformed"),
        ("oversized", "too_large"),
        ("read_error", "unavailable"),
        ("slow", "unavailable"),
    ],
)
async def test_error_body_is_bounded_optional_and_never_overwrites_http_rejection(mode, state):
    body = Body(
        {
            "private_code": b'{"error":{"code":"PRIVATE_SENTINEL","message":"PRIVATE_SENTINEL"}}',
            "malformed": b"PRIVATE_SENTINEL",
            "oversized": b"X" * 8193,
        }.get(mode, b""),
        after=httpx.ReadError("PRIVATE_SENTINEL") if mode == "read_error" else None,
    )
    if mode == "slow":

        class Slow(Body):
            async def __aiter__(self):
                await asyncio.Event().wait()
                yield b"unreachable"

        body = Slow(b"")
    credential = seam.credential.__wrapped__()

    async def qualify():
        return credential

    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(429, headers={"content-type": "application/json"}, stream=body)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    with pytest.raises(HttpStageFailure) as caught:
        await seam.call(Protocol(), client, seam.frozen.__wrapped__(), credential, qualify)
    facts = caught.value.http_failure_facts
    assert facts.error_kind == "http_status" and facts.http_status == 429
    assert facts.error_body_state == state and facts.provider_error_code is None
    assert "PRIVATE_SENTINEL" not in facts.model_dump_json()
    assert facts.cleanup_confirmed and caught.value.usage is None
    assert len(requests) == 1 and body.closed == 1 and client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["bad_stream", "after_terminal", "bad_output", "publication"])
async def test_stream_validation_publication_and_known_terminal_are_distinct(mode):
    from unit.agents.test_http_stage_lifecycle import terminal

    value = terminal()
    value["response"]["usage"]["input_tokens_details"]["cache_write_tokens"] = 3
    if mode == "bad_output":
        value["response"]["output"][0]["content"][0]["text"] = "PRIVATE_SENTINEL"
    body = Body(
        b"data: INVALID\n\n"
        if mode == "bad_stream"
        else ("data: " + json.dumps(value) + "\n\n").encode(),
        after=httpx.RemoteProtocolError("PRIVATE_SENTINEL") if mode == "after_terminal" else None,
    )
    protocol = Protocol()
    if mode == "publication":

        async def fail(*a, **k):
            raise RuntimeError("PRIVATE_SENTINEL")

        protocol.observe = fail
    credential = seam.credential.__wrapped__()

    async def qualify():
        return credential

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"content-type": "text/event-stream"}, stream=body
            )
        )
    )
    with pytest.raises(HttpStageFailure) as caught:
        await seam.call(protocol, client, seam.frozen.__wrapped__(), credential, qualify)
    facts = caught.value.http_failure_facts
    assert (
        facts.phase
        == {
            "bad_stream": "stream",
            "after_terminal": "stream",
            "bad_output": "response_validation",
            "publication": "publication",
        }[mode]
    )
    assert facts.terminal_seen is (mode != "bad_stream")
    assert (caught.value.usage is not None) is (mode in ("after_terminal", "bad_output"))
    assert facts.proof_error_phase == ("publication" if mode == "publication" else None)
    assert "PRIVATE_SENTINEL" not in facts.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_stage", ["research", "critique", "synthesis"])
@pytest.mark.parametrize("failure_mode", ["http", "cancel", "deadline"])
async def test_committee_preserves_http_facts_in_every_stage_and_timeout_cause(
    failed_stage, failure_mode
):
    from nexus.agents.codex_staged import StagedCodexCommittee
    from nexus.agents.committee import ROLE_ORDER
    from nexus.agents.responses_budget import UsageSnapshot
    from unit.agents.test_codex_sdk import _authority, _staged_batch, _tools

    seen = []
    credential = seam.credential.__wrapped__()

    async def qualify():
        if failure_mode == "cancel":
            raise asyncio.CancelledError("PRIVATE_SENTINEL")
        if failure_mode == "deadline":
            await asyncio.Event().wait()
        return credential

    async def invoke(payload, **kwargs):
        seen.append(payload["stage"])
        if payload["stage"] != failed_stage:
            return json.dumps(
                _staged_batch(payload["stage"], payload["requested_roles"])
            ), UsageSnapshot(input_tokens=20, output_tokens=9, provider_calls=1)
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    429,
                    headers={"content-type": "application/json"},
                    stream=Body(b'{"error":{"code":"insufficient_quota"}}'),
                )
            )
        )
        async with asyncio.timeout(None) as timer:
            if failure_mode == "deadline":
                # Trigger the actual timeout wrapper by scheduling, not a fragile
                # elapsed-time assertion. The HTTP owner must retain cancellation.
                loop = asyncio.get_running_loop()
                loop.call_soon(timer.reschedule, loop.time() - 1)
            return await seam.call(
                Protocol(), client, seam.frozen.__wrapped__(), credential, qualify
            )

    request, evidence, prompts = _authority()
    model = StagedCodexCommittee(prompts=prompts, tools=_tools())
    model.stage_invoker = invoke
    with pytest.raises(
        {"http": HttpStageFailure, "cancel": asyncio.CancelledError, "deadline": TimeoutError}[
            failure_mode
        ]
    ):
        for role in ROLE_ORDER:
            await model.analyze(role, request, evidence, prompts[role])
    diagnostic = model.failure_diagnostic
    assert diagnostic.stage == failed_stage and diagnostic.failure_facts is None
    assert diagnostic.http_failure_facts.error_kind == (
        "http_status" if failure_mode == "http" else "cancelled"
    )
    assert diagnostic.http_failure_facts.cleanup_confirmed
    assert (
        seen
        == ["research", "critique", "synthesis"][
            : ["research", "critique", "synthesis"].index(failed_stage) + 1
        ]
    )
    assert model.failure_reason == "model_usage_uncertain"
    assert model.usage.provider_calls == len(seen)
    assert "PRIVATE_SENTINEL" not in diagnostic.model_dump_json()


@pytest.mark.asyncio
async def test_native_cleanup_deadline_keeps_actual_phase_and_known_usage(monkeypatch):
    from types import SimpleNamespace

    from nexus.agents.research_stages import http_lifecycle
    from unit.agents.test_http_stage_lifecycle import terminal

    loop = asyncio.get_running_loop()
    clock = [loop.time()]
    entered = asyncio.Event()
    protocol = Protocol()

    async def pending_cleanup(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    protocol.cleanup = pending_cleanup

    async def deadline_wait(tasks, *, timeout):
        if any(task.get_coro().__qualname__.endswith(".<locals>.finish") for task in tasks):
            await entered.wait()
            clock[0] += 60
            return set(), tasks
        return await asyncio.wait(tasks, timeout=timeout)

    monkeypatch.setattr(
        http_lifecycle,
        "asyncio",
        SimpleNamespace(
            get_running_loop=lambda: SimpleNamespace(
                time=lambda: clock[0], call_soon=loop.call_soon
            ),
            create_task=asyncio.create_task,
            CancelledError=asyncio.CancelledError,
            shield=asyncio.shield,
            wait=deadline_wait,
        ),
    )
    credential = seam.credential.__wrapped__()

    async def qualify():
        return credential

    value = terminal()
    value["response"]["usage"]["input_tokens_details"]["cache_write_tokens"] = 3
    body = Body(("data: " + json.dumps(value) + "\n\n").encode())
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"content-type": "text/event-stream"}, stream=body
            )
        )
    )
    with pytest.raises(HttpStageFailure) as caught:
        await seam.call(protocol, client, seam.frozen.__wrapped__(), credential, qualify)
    await asyncio.gather(*caught.value.owned, return_exceptions=True)
    facts = caught.value.http_failure_facts
    assert facts.phase == "cleanup" and facts.proof_error_phase == "cleanup"
    assert facts.error_kind == "cleanup" and facts.cleanup_confirmed
    assert caught.value.usage.output_tokens == 9 and facts.terminal_seen
    assert client.is_closed and body.closed == 1


@pytest.mark.asyncio
async def test_error_body_reaching_original_work_deadline_keeps_http_rejection(monkeypatch):
    from types import SimpleNamespace

    from nexus.agents import responses_stage_stream as stream

    entered = asyncio.Event()

    class PendingBody(Body):
        async def __aiter__(self):
            entered.set()
            await asyncio.Event().wait()
            yield b"unreachable"

    async def work_deadline(tasks, *, timeout):
        await entered.wait()
        return set(), tasks

    monkeypatch.setattr(
        stream,
        "asyncio",
        SimpleNamespace(
            get_running_loop=asyncio.get_running_loop,
            create_task=asyncio.create_task,
            timeout_at=asyncio.timeout_at,
            wait=work_deadline,
        ),
    )
    credential = seam.credential.__wrapped__()

    async def qualify():
        return credential

    body = PendingBody(b"")
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                429, headers={"content-type": "application/json"}, stream=body
            )
        )
    )
    with pytest.raises(HttpStageFailure) as caught:
        await seam.call(Protocol(), client, seam.frozen.__wrapped__(), credential, qualify)
    facts = caught.value.http_failure_facts
    assert facts.phase == "headers" and facts.error_kind == "http_status"
    assert facts.http_status == 429 and facts.error_body_state == "unavailable"
    assert caught.value.usage is None and facts.cleanup_confirmed
    assert client.is_closed and body.closed == 1
