import asyncio
import json

import httpx
import pytest
from nexus.agents.responses_request import send_response_stage
from unit.agents.test_responses_stage_stream import Body
from unit.agents.test_responses_stream import event


def request():
    return httpx.Request(
        "POST",
        "https://example.invalid/v1/responses",
        json={"stream": True},
        headers={"authorization": "Bearer offline-test-only"},
    )


def headers(body, status=200):
    return httpx.Response(status, stream=body, headers={"content-type": "text/event-stream"})


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "failed", "incomplete"])
async def test_owned_client_sends_once_and_keeps_stage_usage(status):
    body = Body(("data: " + json.dumps(event("response." + status)) + "\n\n").encode())
    calls = []

    async def handle(req):
        calls.append(req)
        return headers(body)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    now = asyncio.get_running_loop().time()
    result = await send_response_stage(
        client, request(), work_deadline=now + 10, cleanup_deadline=now + 20
    )
    assert len(calls) == 1 and result.owner.send_started
    assert result.error is None and result.stage.error is None
    assert result.stage.facts.usage == (7, 3)
    assert result.stage.facts.terminal_status == status
    assert result.cleanup.confirmed and client.is_closed and body.closes == 1


@pytest.mark.asyncio
async def test_preheader_error_and_client_close_failure_are_both_preserved():
    original, secondary = RuntimeError("private send"), RuntimeError("private close")

    class Transport(httpx.AsyncBaseTransport):
        calls = 0

        async def handle_async_request(self, req):
            self.calls += 1
            raise original

        async def aclose(self):
            raise secondary

    transport = Transport()
    client = httpx.AsyncClient(transport=transport)
    now = asyncio.get_running_loop().time()
    result = await send_response_stage(
        client, request(), work_deadline=now + 10, cleanup_deadline=now + 20
    )
    assert transport.calls == 1 and result.stage is None
    assert result.error is original and result.cleanup.error is secondary
    assert client.is_closed and not result.cleanup.confirmed


@pytest.mark.asyncio
async def test_expired_work_does_not_send_or_invent_remote_receipt():
    def handle(req):
        pytest.fail("expired work must not send")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    now = asyncio.get_running_loop().time()
    result = await send_response_stage(
        client, request(), work_deadline=now - 1, cleanup_deadline=now + 10
    )
    assert isinstance(result.error, TimeoutError)
    assert not result.owner.send_started and result.stage is None
    assert result.cleanup.confirmed and client.is_closed


@pytest.mark.asyncio
async def test_redirect_is_not_followed_even_if_client_default_allows_it():
    calls = []

    class UnexpectedDefaultAuth(httpx.Auth):
        def auth_flow(self, req):
            pytest.fail("the already-authorized request must not enter a client auth retry flow")
            yield req  # pragma: no cover

    def handle(req):
        calls.append(req)
        assert req.headers["authorization"] == "Bearer offline-test-only"
        return httpx.Response(307, headers={"location": "https://example.invalid/other"})

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handle), follow_redirects=True, auth=UnexpectedDefaultAuth()
    )
    now = asyncio.get_running_loop().time()
    result = await send_response_stage(
        client, request(), work_deadline=now + 10, cleanup_deadline=now + 20
    )
    assert len(calls) == 1
    assert str(result.stage.error) == "RESPONSES_STREAM_UNAVAILABLE"
    assert result.stage.facts.usage is None and result.cleanup.confirmed


@pytest.mark.asyncio
@pytest.mark.parametrize("late_close_fails", [False, True])
@pytest.mark.parametrize("client_close_fails", [False, True])
async def test_cancellation_retains_pending_owner_and_closes_late_headers(
    late_close_fails, client_close_fails
):
    entered, released, client_closed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    late_error = RuntimeError("private late close")
    client_error = RuntimeError("private client close")

    class LateBody(Body):
        async def aclose(self):
            self.closes += 1
            if late_close_fails:
                raise late_error

    body = LateBody(b"")

    class Transport(httpx.AsyncBaseTransport):
        calls = 0

        async def handle_async_request(self, req):
            self.calls += 1
            entered.set()
            while not released.is_set():
                try:
                    await released.wait()
                except asyncio.CancelledError:
                    pass
            return headers(body)

        async def aclose(self):
            client_closed.set()
            if client_close_fails:
                raise client_error

    transport = Transport()
    client = httpx.AsyncClient(transport=transport)
    now = asyncio.get_running_loop().time()
    task = asyncio.create_task(
        send_response_stage(client, request(), work_deadline=now + 30, cleanup_deadline=now + 60)
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel("primary cancellation")
        await asyncio.wait_for(client_closed.wait(), 5)
        task.cancel("cleanup cancellation")
        result = await asyncio.wait_for(task, 5)
        assert isinstance(result.error, asyncio.CancelledError)
        assert result.error.args == ("primary cancellation",)
        assert not result.cleanup.confirmed and result.cleanup.pending
        assert isinstance(result.cleanup.interrupted, asyncio.CancelledError)
        if client_close_fails:
            assert result.cleanup.error is client_error
        else:
            assert result.cleanup.error is result.cleanup.interrupted
        assert result.stage is None and transport.calls == 1
        released.set()
        await asyncio.gather(*result.cleanup.pending, return_exceptions=True)
        assert body.closes == 1 and result.owner.response.is_closed
        assert result.owner.response_close_error is (late_error if late_close_fails else None)
        # Late local cleanup never upgrades the original returned unknown proof.
        assert not result.cleanup.confirmed and result.stage is None
    finally:
        released.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancellation_after_headers_closes_body_and_client_without_usage_fiction():
    reading = asyncio.Event()

    class WaitingBody(Body):
        async def __aiter__(self):
            reading.set()
            await asyncio.Event().wait()
            yield b""  # pragma: no cover

    body = WaitingBody(b"")
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda req: headers(body)))
    now = asyncio.get_running_loop().time()
    task = asyncio.create_task(
        send_response_stage(client, request(), work_deadline=now + 30, cleanup_deadline=now + 60)
    )
    await asyncio.wait_for(reading.wait(), 5)
    task.cancel("reading cancellation")
    result = await asyncio.wait_for(task, 5)
    assert isinstance(result.error, asyncio.CancelledError)
    assert result.error is result.stage.error
    assert result.error.args == ("reading cancellation",)
    assert result.stage.facts.usage is None and result.stage.terminal is None
    assert result.cleanup.confirmed and client.is_closed and body.closes == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_cleanup", [False, True])
async def test_nonfinite_deadline_never_sends_and_keeps_unclosed_owner(bad_cleanup):
    def handle(req):
        pytest.fail("invalid deadlines must not send")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    now = asyncio.get_running_loop().time()
    result = await send_response_stage(
        client,
        request(),
        work_deadline=now + 10 if bad_cleanup else float("inf"),
        cleanup_deadline=float("nan") if bad_cleanup else now + 20,
    )
    assert str(result.error) == "RESPONSES_DEADLINE_INVALID"
    assert not result.owner.send_started and result.stage is None
    assert result.owner.client is client
    assert result.cleanup.confirmed is (not bad_cleanup)
    await client.aclose()


@pytest.mark.asyncio
async def test_submission_guard_cannot_extend_original_send_window(monkeypatch):
    from types import SimpleNamespace

    from nexus.agents import responses_request

    real = asyncio.get_running_loop().time()
    observed = real

    def guard():
        nonlocal observed
        observed = real + 31

    def unexpected(req):
        pytest.fail("expired final guard must not send")

    monkeypatch.setattr(
        responses_request,
        "asyncio",
        SimpleNamespace(
            get_running_loop=lambda: SimpleNamespace(time=lambda: observed),
            create_task=asyncio.create_task,
            wait=asyncio.wait,
        ),
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(unexpected))
    result = await send_response_stage(
        client, request(), work_deadline=real + 30, cleanup_deadline=real + 37, before_send=guard
    )
    assert not result.owner.send_started
    assert isinstance(result.error, TimeoutError)
    assert result.cleanup.confirmed and client.is_closed
