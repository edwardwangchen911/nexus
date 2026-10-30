"""Post-header HTTP observation and owned cleanup; no request/authorization is created.

The native lifecycle must persist these facts before propagating the original
error. A local close never supplies missing provider completion or usage.
Pre-header transport ownership and native experiment-stage routing remain separate.
"""

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import httpx

from nexus.agents.codex_sdk import _MAX_OUTPUT_BYTES
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


async def collect_response_stage(
    response: httpx.Response,
    *,
    work_deadline: float,
    cleanup_deadline: float,
) -> HttpStageObservation:
    """Read this exact response once, then close it within the original cleanup bound.

    This returns observations, not a successful product result. In particular,
    failed/incomplete can have known usage, and a completed event can be followed
    by a stream error. Both the primary error and cleanup failure are retained.
    """
    loop = asyncio.get_running_loop()
    facts, terminal = ResponsesStreamFacts(), None

    async def receive() -> object:
        nonlocal terminal
        if loop.time() >= work_deadline:
            raise TimeoutError("RESPONSES_WORK_DEADLINE")
        if work_deadline >= cleanup_deadline:
            raise StreamProtocolError("RESPONSES_DEADLINE_INVALID")
        if (
            response.status_code != 200
            or response.headers.get("content-type", "").split(";")[0].strip() != "text/event-stream"
        ):
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
    primary = None
    try:
        done, _ = await asyncio.wait({task}, timeout=max(0, work_deadline - loop.time()))
        if not done:
            raise TimeoutError("RESPONSES_WORK_DEADLINE")
        await task
    except BaseException as error:
        primary = error
    cleanup = await close_owned_stream(response, task, deadline=cleanup_deadline)
    if cleanup.confirmed:
        facts.confirm_local_cleanup(stream_closed=response.is_closed, request_task_done=task.done())
    return HttpStageObservation(facts, terminal, primary, cleanup)
