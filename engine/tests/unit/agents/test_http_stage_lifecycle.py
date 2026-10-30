import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from nexus.agents.chatgpt_grant import Grant
from nexus.agents.codex_sdk import (
    BASE_INSTRUCTIONS,
    DEVELOPER_INSTRUCTIONS,
    codex_output_schema,
    codex_request_wire,
)
from pydantic import BaseModel


class Answer(BaseModel):
    answer: str


class Body(httpx.AsyncByteStream):
    def __init__(self, data, after=None):
        self.data, self.after, self.closed = data, after, 0

    async def __aiter__(self):
        yield self.data
        if self.after:
            raise self.after

    async def aclose(self):
        self.closed += 1


def terminal(status="completed", usage=True):
    result = {
        "id": "resp_fixture",
        "status": status,
        "model": "gpt-5.6-terra",
        "created_at": 1793350800,
        "object": "response",
        "parallel_tool_calls": False,
        "tool_choice": "none",
        "tools": [],
        "output": [
            {
                "id": "msg_fixture",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {"type": "output_text", "annotations": [], "text": '{"answer":"own result"}'}
                ],
            }
        ],
    }
    if usage:
        result["usage"] = {
            "input_tokens": 20,
            "output_tokens": 9,
            "total_tokens": 29,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens_details": {"reasoning_tokens": 0},
        }
    return {"type": "response." + status, "response": result}


def test_http_envelope_preserves_the_sdk_business_input_and_instructions():
    from nexus.agents.research_stages.http_lifecycle import http_stage_wire

    payload = {"stage": "research", "evidence": ["冻结原件"]}
    wire = http_stage_wire(payload, output_model=Answer, forbidden=frozenset())
    body = json.loads(wire)
    assert body["instructions"] == BASE_INSTRUCTIONS
    assert body["input"] == [
        {"role": "developer", "content": DEVELOPER_INSTRUCTIONS},
        {"role": "user", "content": codex_request_wire(payload)},
    ]
    assert body["text"]["format"]["schema"] == codex_output_schema(Answer)
    assert body["model"] == "gpt-5.6-terra" and body["reasoning"] == {"effort": "low"}
    assert body["stream"] is True and body["store"] is False and body["tools"] == []
    assert "max_output_tokens" not in body
    with pytest.raises(ValueError):
        http_stage_wire(payload, output_model=Answer, forbidden=frozenset({"冻结原件"}))


class Protocol:
    """Boundary substitute; native transactions are covered separately in PG tests."""

    def __init__(self):
        self.events = []
        self.local_stopped = False

    async def observe(self, ticket, **kwargs):
        self.events.append(("observe", kwargs))

    async def verify(self, ticket, **kwargs):
        self.events.append(("verify", kwargs))
        return True

    async def cleanup(self, ticket, **kwargs):
        self.events.append(("cleanup", kwargs))

    async def stop(self, scope):
        self.local_stopped = True
        self.events.append(("stop", {}))


async def invoke(
    protocol,
    status="completed",
    usage=True,
    after=None,
    *,
    body_factory=Body,
    expired=False,
    transport_error=None,
):
    from nexus.agents.research_stages.http_lifecycle import http_stage_wire, invoke_http_stage

    wire = http_stage_wire({"stage": "research"}, output_model=Answer, forbidden=frozenset())
    data = ("data: " + json.dumps(terminal(status, usage)) + "\n\n").encode()
    body = body_factory(data, after)
    requests = []

    def handle(req):
        requests.append(req)
        assert req.content == wire and req.url == httpx.URL("https://api.openai.com/v1/responses")
        assert req.headers["authorization"] == "Bearer fixture-token"
        if transport_error is not None:
            raise transport_error
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=body)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    anchor = datetime.now(UTC)
    mono = asyncio.get_running_loop().time()
    grant = Grant(
        {"access_token": "fixture-token"}, anchor.timestamp() + (-1 if expired else 3600), 0
    )
    try:
        result = await invoke_http_stage(
            protocol=protocol,
            scope=uuid4(),
            ticket=object(),
            wire=wire,
            output_model=Answer,
            grant=grant,
            clock=lambda: anchor + timedelta(seconds=asyncio.get_running_loop().time() - mono),
            wall_anchor=anchor,
            monotonic_anchor=mono,
            work_deadline=mono + 30,
            cleanup_deadline=mono + 37,
            client=client,
        )
        return result, requests, body
    finally:
        assert client.is_closed
        assert body.closed == (0 if expired or transport_error is not None else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "failed", "incomplete"])
async def test_usage_is_published_before_terminal_error_and_cleanup(status):
    p = Protocol()
    if status == "completed":
        (raw, usage), requests, _ = await invoke(p, status)
        assert json.loads(raw) == {"answer": "own result"} and usage.input_tokens == 20
        assert len(requests) == 1
    else:
        with pytest.raises(ValueError) as error:
            await invoke(p, status)
        assert error.value.usage.input_tokens == 20
    assert [name for name, _ in p.events][:2] == ["observe", "verify"]
    observed = p.events[0][1]
    assert observed["tokens"] == (20, 9) and observed["valid"] == (status == "completed")
    assert next(value for name, value in p.events if name == "cleanup")["confirmed"]


@pytest.mark.asyncio
async def test_known_terminal_then_disconnect_keeps_usage_and_original_error():
    p = Protocol()
    original = httpx.ReadError("private transport detail")
    with pytest.raises(ValueError) as error:
        await invoke(p, after=original)
    assert error.value.usage.output_tokens == 9
    assert error.value.observation.error is original
    assert "private transport detail" not in str(error.value)
    assert p.local_stopped


@pytest.mark.asyncio
async def test_missing_usage_stays_unknown_after_confirmed_close():
    p = Protocol()
    with pytest.raises(ValueError) as error:
        await invoke(p, usage=False)
    assert error.value.usage is None
    assert not any(name == "verify" for name, _ in p.events)
    assert next(v for name, v in p.events if name == "cleanup")["confirmed"]


@pytest.mark.asyncio
async def test_native_usage_publication_does_not_wait_for_stream_close():
    started, release, verified = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class ClosingBody(Body):
        async def aclose(self):
            started.set()
            await release.wait()
            await super().aclose()

    class NotifyingProtocol(Protocol):
        async def verify(self, ticket, **kwargs):
            result = await super().verify(ticket, **kwargs)
            verified.set()
            return result

    task = asyncio.create_task(invoke(NotifyingProtocol(), body_factory=ClosingBody))
    try:
        await asyncio.wait_for(started.wait(), 5)
        await asyncio.wait_for(verified.wait(), 5)
        assert not task.done()
    finally:
        release.set()
        await task


@pytest.mark.asyncio
@pytest.mark.parametrize("expired,pre_header", [(True, False), (False, True)])
async def test_expired_grant_and_pre_header_failure_never_fabricate_usage(expired, pre_header):
    p = Protocol()
    with pytest.raises(ValueError) as error:
        await invoke(
            p,
            expired=expired,
            transport_error=httpx.ConnectError("fixture") if pre_header else None,
        )
    assert error.value.usage is None
    assert error.value.provider_call_started is (not expired)
    assert not any(name == "observe" for name, _ in p.events)
    assert next(v for name, v in p.events if name == "cleanup")["confirmed"]


@pytest.mark.asyncio
async def test_repeated_cancellation_retains_original_cancel_and_verified_usage():
    received, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class WaitingBody(Body):
        async def __aiter__(self):
            yield self.data
            await asyncio.Event().wait()

        async def aclose(self):
            closing.set()
            await release.wait()
            await super().aclose()

    class NotifyingProtocol(Protocol):
        async def verify(self, ticket, **kwargs):
            result = await super().verify(ticket, **kwargs)
            received.set()
            return result

    p = NotifyingProtocol()
    task = asyncio.create_task(invoke(p, body_factory=WaitingBody))
    await asyncio.wait_for(received.wait(), 5)
    task.cancel("first cancellation")
    await asyncio.wait_for(closing.wait(), 5)
    task.cancel("second cancellation")
    release.set()
    with pytest.raises(asyncio.CancelledError) as error:
        await task
    assert str(error.value) == "first cancellation"
    failure = error.value.http_stage_failure
    assert failure.primary is error.value and failure.usage.input_tokens == 20
    assert all(t.done() for t in failure.owned)
    assert next(v for name, v in p.events if name == "cleanup")["confirmed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("where", ["stream_close", "verification"])
async def test_native_failure_keeps_independent_cleanup_and_original_cause(where):
    original = RuntimeError("fixture original cause")

    class FailingClose(Body):
        async def aclose(self):
            await super().aclose()
            raise original

    class FailingVerification(Protocol):
        async def verify(self, ticket, **kwargs):
            await super().verify(ticket, **kwargs)
            raise original

    p = FailingVerification() if where == "verification" else Protocol()
    with pytest.raises(ValueError) as error:
        await invoke(p, body_factory=FailingClose if where == "stream_close" else Body)
    assert error.value.primary is original
    assert (error.value.usage is not None) == (where == "stream_close")
    assert next(v for name, v in p.events if name == "cleanup")["confirmed"] == (
        where != "stream_close"
    )
    assert p.local_stopped
    assert all(t.done() for t in error.value.owned)
