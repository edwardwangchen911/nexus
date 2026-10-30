"""No network: terminal status and local stream cleanup are independent facts."""

import pytest
from nexus.agents.responses_stream import ResponsesStreamFacts, StreamProtocolError


def event(kind="response.completed", *, usage=True, response_id="resp_one"):
    response = {"id": response_id, "status": kind.removeprefix("response."), "output": []}
    if usage:
        response["usage"] = {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10}
    return {"type": kind, "response": response}


def test_completed_requires_usage_and_matching_identity():
    facts = ResponsesStreamFacts()
    facts.observe({"type": "response.created", "response": {"id": "resp_one"}})
    facts.observe(event())
    assert facts.terminal_status == "completed"
    assert facts.usage == (7, 3)
    assert facts.cleanup_confirmed is False
    with pytest.raises(StreamProtocolError, match="RESPONSES_DUPLICATE_TERMINAL"):
        facts.observe(event())
    assert facts.usage == (7, 3)


@pytest.mark.parametrize("kind", ["response.failed", "response.incomplete"])
def test_failed_terminal_keeps_known_usage_without_success(kind):
    facts = ResponsesStreamFacts()
    facts.observe(event(kind))
    assert facts.usage == (7, 3)
    assert facts.terminal_status != "completed"
    assert not facts.cleanup_confirmed


def test_local_close_never_proves_remote_completion_or_usage():
    facts = ResponsesStreamFacts()
    facts.confirm_local_cleanup(stream_closed=True, request_task_done=True)
    assert facts.cleanup_confirmed
    assert facts.terminal_status is None and facts.usage is None


@pytest.mark.parametrize("stream_closed,task_done", [(False, True), (True, False), (False, False)])
def test_partial_local_cleanup_does_not_confirm(stream_closed, task_done):
    facts = ResponsesStreamFacts()
    with pytest.raises(StreamProtocolError, match="RESPONSES_CLEANUP_UNCONFIRMED"):
        facts.confirm_local_cleanup(stream_closed=stream_closed, request_task_done=task_done)
    assert not facts.cleanup_confirmed


def test_bad_usage_and_identity_fail_without_zero_imputation():
    facts = ResponsesStreamFacts()
    with pytest.raises(StreamProtocolError, match="RESPONSES_USAGE_UNKNOWN"):
        facts.observe(event(usage=False))
    assert facts.usage is None
    with pytest.raises(StreamProtocolError, match="RESPONSES_DUPLICATE_TERMINAL"):
        facts.observe(event())
    assert facts.usage is None
    other = ResponsesStreamFacts()
    other.observe({"type": "response.created", "response": {"id": "resp_one"}})
    with pytest.raises(StreamProtocolError, match="RESPONSES_IDENTITY_CHANGED"):
        other.observe(event(response_id="resp_other"))
    assert other.usage is None and other.terminal_status is None


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True])
async def test_owned_http_close_is_once_and_retains_close_error(fails):
    import asyncio

    from nexus.agents.responses_stream import close_owned_stream

    error = RuntimeError("private provider diagnostic")

    class Stream:
        is_closed = False
        calls = 0

        async def aclose(self):
            self.calls += 1
            if fails:
                raise error
            self.is_closed = True

    async def finished():
        return None

    task = asyncio.create_task(finished())
    await task
    stream = Stream()
    result = await close_owned_stream(stream, task, deadline=asyncio.get_running_loop().time() + 5)
    assert stream.calls == 1
    assert result.confirmed is (not fails)
    assert result.error is (error if fails else None)
    assert not result.pending


@pytest.mark.asyncio
async def test_expired_cleanup_never_claims_closed_or_extends_deadline():
    import asyncio

    from nexus.agents.responses_stream import close_owned_stream

    class Stream:
        is_closed = False

        async def aclose(self):
            pytest.fail("no cleanup operation may start after original deadline")

    async def finished():
        return None

    task = asyncio.create_task(finished())
    await task
    result = await close_owned_stream(Stream(), task, deadline=asyncio.get_running_loop().time())
    assert not result.confirmed
