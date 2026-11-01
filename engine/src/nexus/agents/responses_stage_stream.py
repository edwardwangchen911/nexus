"""Post-header HTTP observation and owned cleanup; no request/authorization is created.

The native lifecycle must persist these facts before propagating the original
error. A local close never supplies missing provider completion or usage.
Pre-header transport ownership and native experiment-stage routing remain separate.
"""

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, cast, get_args

import httpx

from nexus.agents.codex_sdk import _MAX_OUTPUT_BYTES
from nexus.agents.http_failure import HttpResponseRejected, ProviderErrorCode
from nexus.agents.responses_stream import (
    HttpCleanupResult,
    ResponsesStreamFacts,
    StreamProtocolError,
    close_owned_stream,
)


@dataclass(frozen=True)
class HttpStageObservation:
    facts: ResponsesStreamFacts
    terminal: dict[str, Any] | None
    error: BaseException | None
    cleanup: HttpCleanupResult


async def _read_rejection_code(
    response: httpx.Response, rejection: HttpResponseRejected, *, work_deadline: float
) -> None:
    """At most 8 KiB/one second of this existing response, inside the original deadline.

    This diagnostic is not a usage receipt. Never read a second response, decode
    arbitrary compression, retain message text, or let an error-body failure
    overwrite the already observed HTTP rejection.
    """
    if (
        response.headers.get("content-type", "").split(";")[0].strip() != "application/json"
        or response.headers.get("content-encoding", "identity") != "identity"
        or response.is_stream_consumed
        or response.is_closed
        or not isinstance(response.stream, httpx.AsyncByteStream)
    ):
        return
    try:
        async with asyncio.timeout_at(min(work_deadline, asyncio.get_running_loop().time() + 1)):
            response.is_stream_consumed = True
            body = bytearray()
            async for chunk in response.stream:
                if len(body) + len(chunk) > 8192:
                    rejection.body_state = "too_large"
                    return
                body.extend(chunk)
            value = json.loads(body)
            error = value.get("error") if isinstance(value, dict) else None
            if not isinstance(error, dict):
                rejection.body_state = "malformed"
                return
            code = error.get("code")
            if isinstance(code, str) and code in get_args(ProviderErrorCode.__value__):
                rejection.provider_error_code = cast(ProviderErrorCode, code)
            rejection.body_state = "parsed"
    except (ValueError, UnicodeError):
        rejection.body_state = "malformed"
    except Exception:
        rejection.body_state = "unavailable"


async def collect_response_stage(
    response: httpx.Response,
    *,
    work_deadline: float,
    cleanup_deadline: float,
    on_terminal: Callable[[dict[str, Any], ResponsesStreamFacts], None] | None = None,
) -> HttpStageObservation:
    """Read this exact response once, then close it within the original cleanup bound.

    This returns observations, not a successful product result. In particular,
    failed/incomplete can have known usage, and a completed event can be followed
    by a stream error. Both the primary error and cleanup failure are retained.
    """
    loop = asyncio.get_running_loop()
    facts, terminal = ResponsesStreamFacts(), None
    rejection: HttpResponseRejected | None = None

    async def receive() -> object:
        nonlocal terminal, rejection
        if loop.time() >= work_deadline:
            raise TimeoutError("RESPONSES_WORK_DEADLINE")
        if work_deadline >= cleanup_deadline:
            raise StreamProtocolError("RESPONSES_DEADLINE_INVALID")
        if response.status_code != 200:
            rejection = HttpResponseRejected()
            await _read_rejection_code(response, rejection, work_deadline=work_deadline)
            raise rejection
        if response.headers.get("content-type", "").split(";")[0].strip() != "text/event-stream":
            raise StreamProtocolError("RESPONSES_STREAM_UNAVAILABLE")
        if (
            response.is_stream_consumed
            or response.is_closed
            or not isinstance(response.stream, httpx.AsyncByteStream)
            or response.headers.get("content-encoding", "identity") != "identity"
        ):
            raise StreamProtocolError("RESPONSES_STREAM_UNAVAILABLE")
        response.is_stream_consumed = True

        owned_stream = response.stream

        async def chunks() -> AsyncIterator[bytes]:
            # Read available bytes immediately: httpx chunk_size buffering can
            # lose an already-received terminal when the next read disconnects.
            # Own the final close ourselves; implicit EOF close must not hide a
            # close failure behind Response.is_closed on a later second close.
            async for raw_chunk in owned_stream:
                for offset in range(0, len(raw_chunk), 4096):
                    yield raw_chunk[offset : offset + 4096]

        buffer = b""
        data: list[str] = []
        event_name: str | None = None
        ended = False
        async for chunk in chunks():
            buffer += chunk
            while b"\n" in buffer:
                raw, buffer = buffer.split(b"\n", 1)
                line = raw.removesuffix(b"\r").decode("utf-8")
                if line == "":
                    if not data:
                        event_name = None
                        continue
                    value = "\n".join(data)
                    data = []
                    if value == "[DONE]":
                        if ended or terminal is None:
                            raise StreamProtocolError("RESPONSES_TERMINAL_MISSING")
                        ended = True
                        event_name = None
                        continue
                    if ended:
                        raise StreamProtocolError("RESPONSES_EVENT_AFTER_TERMINAL")
                    event = json.loads(value)
                    if not isinstance(event, dict) or (
                        event_name is not None and event.get("type") != event_name
                    ):
                        raise StreamProtocolError("RESPONSES_EVENT_INVALID")
                    if terminal is not None:
                        raise StreamProtocolError("RESPONSES_EVENT_AFTER_TERMINAL")
                    facts.observe(event)
                    if facts.terminal_status is not None:
                        terminal = event["response"]
                        if on_terminal is not None:
                            on_terminal(terminal, facts)
                    event_name = None
                elif line.startswith(":"):
                    continue
                elif line.startswith("data:"):
                    data.append(line[5:].removeprefix(" "))
                elif line.startswith("event:"):
                    if event_name is not None:
                        raise StreamProtocolError("RESPONSES_EVENT_INVALID")
                    event_name = line[6:].removeprefix(" ")
                if sum(len(item.encode()) for item in data) > _MAX_OUTPUT_BYTES:
                    raise StreamProtocolError("RESPONSES_EVENT_TOO_LARGE")
            if len(buffer) > _MAX_OUTPUT_BYTES:
                raise StreamProtocolError("RESPONSES_EVENT_TOO_LARGE")
        if buffer.strip() or data:
            raise StreamProtocolError("RESPONSES_STREAM_TRUNCATED")
        if terminal is None:
            raise StreamProtocolError("RESPONSES_TERMINAL_MISSING")
        return None

    task = asyncio.create_task(receive())
    primary: BaseException | None = None
    try:
        done, _ = await asyncio.wait({task}, timeout=max(0, work_deadline - loop.time()))
        if not done:
            raise TimeoutError("RESPONSES_WORK_DEADLINE")
        await task
    except BaseException as error:
        # A diagnostic body read must not replace an already observed rejection
        # when the original work deadline wins the race. Cancellation remains
        # cancellation, and neither branch supplies a remote usage receipt.
        if isinstance(error, TimeoutError) and rejection is not None:
            if rejection.body_state == "not_read":
                rejection.body_state = "unavailable"
            primary = rejection
        else:
            primary = error
    cleanup = await close_owned_stream(response, task, deadline=cleanup_deadline)
    if cleanup.confirmed:
        facts.confirm_local_cleanup(stream_closed=response.is_closed, request_task_done=task.done())
    return HttpStageObservation(facts, terminal, primary, cleanup)
