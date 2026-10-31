"""An opaque project key uses the existing owned HTTP lifecycle, never a Grant."""

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from nexus.agents.chatgpt_grant import Grant
from nexus.agents.platform_wire import PlatformProfile, PlatformStageWire, _hash, _wire
from nexus.agents.research_stages.http_lifecycle import HttpStageFailure, invoke_http_stage
from nexus.ops.platform_credentials import PlatformCredential, credential_fingerprint
from pydantic import SecretStr
from unit.agents.test_http_stage_lifecycle import Answer, Body, Protocol, terminal

KEY = "synthetic-platform-http-key"


@pytest.mark.asyncio
@pytest.mark.parametrize("header", ["OpenAI-Project", "OpenAI-Organization"])
async def test_key_scope_rejects_default_client_scope_on_actual_request(frozen, credential, header):
    credential = replace(credential, organization_id=None, project_id=None, key_scope_only=True)
    requests = []

    async def check():
        return credential

    def handle(request):
        requests.append(request)
        value = terminal()
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Body(("data: " + json.dumps(value) + "\n\n").encode()),
        )

    client = httpx.AsyncClient(
        headers={header: "unbound-scope"}, transport=httpx.MockTransport(handle)
    )
    with pytest.raises(HttpStageFailure):
        await call(Protocol(), client, frozen, credential, check)
    assert requests == []
    assert client.is_closed


@pytest.fixture
def credential():
    return PlatformCredential(SecretStr(KEY), credential_fingerprint(KEY), "org-test", "proj-test")


@pytest.fixture
def frozen():
    wire = _wire(
        {"stage": "research", "evidence": ['完整 UTF8 \\" 中文𠀀']},
        Answer,
        output_remaining=8192,
        profile=PlatformProfile(),
        forbidden=(),
    )
    body_hash = hashlib.sha256(wire).hexdigest()
    allocation = "a" * 64
    return PlatformStageWire(
        wire,
        body_hash,
        _hash(dict(allocation_hash=allocation, stage="research", body_sha256=body_hash)),
        allocation,
        "research",
        len(wire) + 8192,
        len(wire) + 8192,
        "https://api.openai.com/v1/responses",
        "platform_project_key",
    )


async def call(protocol, client, frozen, credential, pre_send, **overrides):
    anchor, mono = datetime.now(UTC), asyncio.get_running_loop().time()
    arguments = dict(
        protocol=protocol,
        scope=uuid4(),
        ticket=SimpleNamespace(stage=1),
        wire=frozen.wire,
        output_model=Answer,
        platform_credential=credential,
        platform_wire=frozen,
        platform_pre_send=pre_send,
        clock=lambda: anchor + timedelta(seconds=asyncio.get_running_loop().time() - mono),
        wall_anchor=anchor,
        monotonic_anchor=mono,
        work_deadline=mono + 30,
        cleanup_deadline=mono + 37,
        client=client,
    )
    arguments.update(overrides)
    return await invoke_http_stage(**arguments)


@pytest.mark.asyncio
async def test_platform_sends_one_frozen_body_and_preserves_all_native_usage_extra(
    frozen, credential
):
    order, requests = [], []
    value = terminal()
    value["response"]["service_tier"] = "default"
    value["response"]["usage"]["input_tokens_details"].update(
        cache_write_tokens=3, cache_write_tokens_details={"ttl_30m_tokens": 3, "ttl_12h_tokens": 0}
    )
    value["response"]["usage"]["output_tokens_details"]["reasoning_tokens"] = 7
    body = Body(("data: " + json.dumps(value) + "\n\n").encode())

    async def check():
        order.append("check")
        await asyncio.sleep(0)
        return replace(credential)

    def guard():
        order.append("guard")

    def handle(request):
        order.append("send")
        requests.append(request)
        assert request.content == frozen.wire
        assert str(request.url) == frozen.endpoint
        for name, content in credential.request_headers().items():
            assert request.headers[name] == content
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=body)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    protocol = Protocol()
    raw, usage = await call(protocol, client, frozen, credential, check, dispatch_guard=guard)
    assert order == ["check", "guard", "send"] and len(requests) == 1
    assert json.loads(raw) == {"answer": "own result"} and usage.output_tokens == 9
    assert json.loads(protocol.events[0][1]["payload"]) == value["response"]
    assert protocol.events[1][1]["wire_hash"] == frozen.body_sha256
    assert body.closed == 1 and client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "missing_callback",
        "missing_credential",
        "missing_wire",
        "both_auth",
        "none_auth",
        "wire_changed",
        "endpoint",
        "kind",
        "stage",
        "binding_hash",
        "unknown_stage",
        "boolean_stage",
        "capacity",
    ],
)
async def test_platform_invalid_binding_never_checks_or_sends(frozen, credential, fault):
    calls = []

    async def check():
        calls.append("precheck")
        return credential

    def handle(request):
        pytest.fail("invalid binding must not send")

    args = {}
    if fault == "missing_callback":
        args["platform_pre_send"] = None
    elif fault == "missing_credential":
        args["platform_credential"] = None
    elif fault == "missing_wire":
        args["platform_wire"] = None
    elif fault == "both_auth":
        args["grant"] = Grant({"access_token": "old"}, 9999999999, 0)
    elif fault == "none_auth":
        args.update(platform_pre_send=None, platform_credential=None, platform_wire=None)
    elif fault == "wire_changed":
        args["wire"] = frozen.wire + b" "
    elif fault == "endpoint":
        args["platform_wire"] = replace(frozen, endpoint="https://wrong.invalid/v1/responses")
    elif fault == "kind":
        args["platform_wire"] = replace(frozen, credential_kind="grant")
    elif fault == "stage":
        args["ticket"] = SimpleNamespace(stage=2)
    elif fault == "binding_hash":
        args["platform_wire"] = replace(frozen, binding_hash="b" * 64)
    elif fault == "unknown_stage":
        args["platform_wire"] = replace(
            frozen,
            stage="unregistered",
            binding_hash=_hash(
                dict(
                    allocation_hash=frozen.allocation_hash,
                    stage="unregistered",
                    body_sha256=frozen.body_sha256,
                )
            ),
        )
        args["ticket"] = SimpleNamespace(stage=None)
    elif fault == "boolean_stage":
        args["ticket"] = SimpleNamespace(stage=True)
    elif fault == "capacity":
        args["platform_wire"] = replace(frozen, input_upper_bound=272001)
    protocol = Protocol()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    with pytest.raises(HttpStageFailure) as error:
        await call(protocol, client, frozen, credential, check, **args)
    assert calls == [] and not error.value.provider_call_started
    assert error.value.usage is None and client.is_closed
    assert not any(name == "observe" for name, _ in protocol.events)
    assert KEY not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["key", "org", "project", "endpoint", "none", "stopped", "error"]
)
async def test_rechecked_credential_and_guard_still_match_original_binding(
    frozen, credential, change
):
    protocol, calls = Protocol(), []
    original = ValueError("private qualification: " + KEY)

    async def check():
        calls.append("check")
        if change == "error":
            raise original
        if change == "none":
            return None
        if change == "stopped":
            protocol.local_stopped = True
        if change == "key":
            key = "other-synthetic-key"
            return replace(
                credential, api_key=SecretStr(key), fingerprint=credential_fingerprint(key)
            )
        if change == "org":
            return replace(credential, organization_id="org-other")
        if change == "project":
            return replace(credential, project_id="proj-other")
        if change == "endpoint":
            return replace(credential, endpoint="https://wrong.invalid")
        return credential

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("must not send"))
    )
    with pytest.raises(HttpStageFailure) as error:
        await call(protocol, client, frozen, credential, check)
    assert calls == ["check"] and not error.value.provider_call_started and client.is_closed
    assert KEY not in str(error.value)
    if change == "error":
        assert error.value.primary is original


@pytest.mark.asyncio
async def test_sync_guard_after_async_check_can_stop_dispatch(frozen, credential):
    original = RuntimeError("guard private " + KEY)

    async def check():
        return credential

    def guard():
        raise original

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("must not send"))
    )
    with pytest.raises(HttpStageFailure) as error:
        await call(Protocol(), client, frozen, credential, check, dispatch_guard=guard)
    assert error.value.primary is original and not error.value.provider_call_started
    assert client.is_closed and KEY not in str(error.value)


@pytest.mark.asyncio
async def test_qualification_cannot_extend_the_original_work_deadline(
    frozen, credential, monkeypatch
):
    from nexus.agents import responses_request

    real = asyncio.get_running_loop().time()
    observed = real

    async def check():
        nonlocal observed
        observed = real + 60
        return credential

    def guard():
        pytest.fail("expired precheck must not reach the final dispatch guard")

    monkeypatch.setattr(
        responses_request,
        "asyncio",
        SimpleNamespace(
            get_running_loop=lambda: SimpleNamespace(time=lambda: observed),
            create_task=asyncio.create_task,
            wait=asyncio.wait,
        ),
    )
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("must not send"))
    )
    with pytest.raises(HttpStageFailure) as error:
        await call(Protocol(), client, frozen, credential, check, dispatch_guard=guard)
    assert isinstance(error.value.primary, TimeoutError)
    assert not error.value.provider_call_started and client.is_closed
    assert error.value.usage is None and error.value.observation.cleanup.confirmed


@pytest.mark.asyncio
async def test_platform_qualification_cancellation_keeps_original_and_no_send(frozen, credential):
    entered = asyncio.Event()

    async def check():
        entered.set()
        await asyncio.Event().wait()
        return credential  # pragma: no cover

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("must not send"))
    )
    task = asyncio.create_task(call(Protocol(), client, frozen, credential, check))
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel("original platform cancellation")
    with pytest.raises(asyncio.CancelledError) as error:
        await asyncio.wait_for(task, 5)
    failure = error.value.http_stage_failure
    assert error.value.args == ("original platform cancellation",)
    assert failure.primary is error.value and not failure.provider_call_started
    assert failure.usage is None and all(task.done() for task in failure.owned)
    assert failure.observation.cleanup.confirmed and client.is_closed


@pytest.mark.asyncio
async def test_platform_precheck_and_cleanup_errors_both_keep_identity(frozen, credential):
    primary, secondary = ValueError("private " + KEY), RuntimeError("cleanup " + KEY)

    async def check():
        raise primary

    class Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, req):
            pytest.fail("must not send")

        async def aclose(self):
            raise secondary

    client = httpx.AsyncClient(transport=Transport())
    with pytest.raises(HttpStageFailure) as error:
        await call(Protocol(), client, frozen, credential, check)
    assert error.value.primary is primary and error.value.observation.cleanup.error is secondary
    assert not error.value.provider_call_started and error.value.usage is None
    assert KEY not in str(error.value) and client.is_closed


@pytest.mark.asyncio
async def test_platform_native_terminal_survives_failed_stream_close(frozen, credential):
    original, requests = RuntimeError("private close " + KEY), []

    class FailedClose(Body):
        async def aclose(self):
            await super().aclose()
            raise original

    value = terminal()
    value["response"]["usage"]["input_tokens_details"]["cache_write_tokens"] = 3
    body = FailedClose(("data: " + json.dumps(value) + "\n\n").encode())

    async def check():
        return credential

    def handle(request):
        requests.append(request)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=body)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    protocol = Protocol()
    with pytest.raises(HttpStageFailure) as error:
        await call(protocol, client, frozen, credential, check)
    assert error.value.primary is original and error.value.usage.output_tokens == 9
    assert json.loads(protocol.events[0][1]["payload"]) == value["response"]
    assert len(requests) == 1 and requests[0].content == frozen.wire and body.closed == 1
    assert KEY not in str(error.value) and client.is_closed


@pytest.mark.asyncio
@pytest.mark.parametrize("where", ["precheck", "guard"])
async def test_platform_accounting_busy_retains_original_control_exception(
    frozen, credential, where
):
    from nexus.agents.budget import ResearchAccountingBusy

    original = ResearchAccountingBusy()

    async def check():
        if where == "precheck":
            raise original
        return credential

    def guard():
        if where == "guard":
            raise original

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: pytest.fail("must not send"))
    )
    with pytest.raises(ResearchAccountingBusy) as error:
        await call(Protocol(), client, frozen, credential, check, dispatch_guard=guard)
    assert error.value is original
    failure = error.value.http_stage_failure
    assert failure.primary is original and not failure.provider_call_started
    assert failure.usage is None and client.is_closed
