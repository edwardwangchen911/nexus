import asyncio
import json

import httpx
import pytest
from nexus.agents.responses_stage_stream import collect_response_stage
from unit.agents.test_responses_stream import event


class Body(httpx.AsyncByteStream):
    def __init__(self, payload, error=None):
        self.payload, self.error, self.closes = payload, error, 0

    async def __aiter__(self):
        for offset in range(0, len(self.payload), 7):
            yield self.payload[offset : offset + 7]
        if self.error:
            raise self.error

    async def aclose(self):
        self.closes += 1


def response(body):
    return httpx.Response(
        200,
        stream=body,
        headers={"content-type": "text/event-stream"},
        request=httpx.Request("POST", "https://example.invalid/responses"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "failed", "incomplete"])
async def test_actual_http_stream_retains_terminal_usage_and_closes_once(status):
    body = Body(("data: " + json.dumps(event("response." + status)) + "\n\n").encode())
    now = asyncio.get_running_loop().time()
    result = await collect_response_stage(
        response(body), work_deadline=now + 5, cleanup_deadline=now + 10
    )
    assert result.facts.usage == (7, 3)
    assert result.facts.terminal_status == status
    assert result.cleanup.confirmed and body.closes == 1
    assert result.error is None
    assert result.terminal["status"] == status


@pytest.mark.asyncio
async def test_disconnect_retains_original_error_without_inventing_terminal_or_usage():
    original = RuntimeError("private diagnostic")
    body = Body(b": keepalive\n\n", original)
    now = asyncio.get_running_loop().time()
    result = await collect_response_stage(
        response(body), work_deadline=now + 5, cleanup_deadline=now + 10
    )
    assert result.error is original
    assert result.facts.usage is None and result.terminal is None
    assert result.cleanup.confirmed and body.closes == 1


@pytest.mark.asyncio
async def test_post_terminal_disconnect_preserves_usage_but_is_not_success():
    original = RuntimeError("lost stream completion")
    body = Body(("data: " + json.dumps(event()) + "\n\n").encode(), original)
    now = asyncio.get_running_loop().time()
    result = await collect_response_stage(
        response(body), work_deadline=now + 5, cleanup_deadline=now + 10
    )
    assert result.error is original
    assert result.facts.usage == (7, 3)
    assert result.cleanup.confirmed


@pytest.mark.asyncio
async def test_expired_work_deadline_does_not_start_stream_or_extend_it():
    class NeverRead(Body):
        async def __aiter__(self):
            pytest.fail("expired work cannot read provider data")
            yield b""  # pragma: no cover

    body = NeverRead(b"")
    now = asyncio.get_running_loop().time()
    result = await collect_response_stage(
        response(body), work_deadline=now - 1, cleanup_deadline=now + 5
    )
    assert isinstance(result.error, TimeoutError)
    assert result.facts.usage is None and result.cleanup.confirmed and body.closes == 1


@pytest.mark.asyncio
async def test_eof_close_failure_cannot_be_hidden_by_second_httpx_close():
    close_error = RuntimeError("private close diagnostic")

    class FailingClose(Body):
        async def aclose(self):
            self.closes += 1
            raise close_error

    body = FailingClose(("data: " + json.dumps(event()) + "\n\n").encode())
    stream = response(body)
    now = asyncio.get_running_loop().time()
    result = await collect_response_stage(stream, work_deadline=now + 5, cleanup_deadline=now + 10)
    assert result.facts.usage == (7, 3)
    assert result.error is None
    assert stream.is_closed and not result.cleanup.confirmed
    assert result.cleanup.error is close_error and body.closes == 1
