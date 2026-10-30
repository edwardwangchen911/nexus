"""Responses transport facts; local cleanup never supplies a remote usage receipt.

This parser is deliberately not an execution or budget authority. An experiment
must acquire its native stage authorization before a future HTTP adapter uses it.
No business route currently selects this transport.
"""

import asyncio
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol


class StreamProtocolError(ValueError):
    """Fixed safe code only; never include provider error text or response content."""


class ResponsesStreamFacts:
    def __init__(self) -> None:
        self.response_id: str | None = None
        self.terminal_status: str | None = None
        self.usage: tuple[int, int] | None = None
        self.cleanup_confirmed = False
        self._terminal_seen = False

    def observe(self, event: Mapping[str, object]) -> None:
        kind = event.get("type")
        response = event.get("response")
        if not isinstance(kind, str):
            raise StreamProtocolError("RESPONSES_EVENT_INVALID")
        if kind not in {
            "response.created",
            "response.in_progress",
            "response.completed",
            "response.failed",
            "response.incomplete",
        }:
            # Delta content cannot establish completion or trustworthy total usage.
            return
        if not isinstance(response, Mapping):
            raise StreamProtocolError("RESPONSES_EVENT_INVALID")
        identity = response.get("id")
        if (
            not isinstance(identity, str)
            or re.fullmatch(r"resp_[A-Za-z0-9_-]{1,240}", identity) is None
        ):
            raise StreamProtocolError("RESPONSES_IDENTITY_INVALID")
        if self.response_id is not None and self.response_id != identity:
            raise StreamProtocolError("RESPONSES_IDENTITY_CHANGED")
        self.response_id = identity
        if kind in {"response.created", "response.in_progress"}:
            if self._terminal_seen:
                raise StreamProtocolError("RESPONSES_EVENT_AFTER_TERMINAL")
            return
        if self._terminal_seen:
            raise StreamProtocolError("RESPONSES_DUPLICATE_TERMINAL")
        # A malformed first terminal cannot be replaced by a later, better one.
        self._terminal_seen = True
        status = kind.removeprefix("response.")
        if response.get("status") != status:
            raise StreamProtocolError("RESPONSES_STATUS_MISMATCH")
        self.terminal_status = status
        usage = response.get("usage")
        if not isinstance(usage, Mapping):
            raise StreamProtocolError("RESPONSES_USAGE_UNKNOWN")
        counts = tuple(usage.get(key) for key in ("input_tokens", "output_tokens", "total_tokens"))
        if any(type(value) is not int or value < 0 for value in counts):
            raise StreamProtocolError("RESPONSES_USAGE_UNKNOWN")
        input_tokens, output_tokens, total_tokens = counts
        assert isinstance(input_tokens, int) and isinstance(output_tokens, int)
        if input_tokens + output_tokens != total_tokens:
            raise StreamProtocolError("RESPONSES_USAGE_UNKNOWN")
        self.usage = (input_tokens, output_tokens)

    def confirm_local_cleanup(self, *, stream_closed: bool, request_task_done: bool) -> None:
        if stream_closed is not True or request_task_done is not True:
            raise StreamProtocolError("RESPONSES_CLEANUP_UNCONFIRMED")
        self.cleanup_confirmed = True


@dataclass(frozen=True)
class HttpCleanupResult:
    confirmed: bool
    error: BaseException | None = None
    pending: tuple[asyncio.Task[object], ...] = ()
    interrupted: BaseException | None = None


class OwnedHttpStream(Protocol):
    @property
    def is_closed(self) -> bool: ...

    async def aclose(self) -> None: ...


def _consume(task: asyncio.Task[object]) -> None:
    try:
        task.result()
    except BaseException:
        pass


async def close_owned_stream(
    stream: OwnedHttpStream,
    request_task: asyncio.Task[object],
    *,
    deadline: float,
) -> HttpCleanupResult:
    """Close this exact stream/task, without process fiction or deadline renewal.

    The caller retains the primary request exception. A failed cleanup returns
    its original secondary exception separately and cannot convert unknown usage.
    Pending tasks remain owned by the returned result; cancellation is not exit.
    """
    loop = asyncio.get_running_loop()
    if not request_task.done():
        request_task.cancel()
    request_task.add_done_callback(_consume)
    if loop.time() >= deadline:
        return HttpCleanupResult(False, pending=(() if request_task.done() else (request_task,)))
    close_task: asyncio.Task[object] = asyncio.create_task(stream.aclose())
    close_task.add_done_callback(_consume)
    owned = {request_task, close_task}
    try:
        _, pending = await asyncio.wait(owned, timeout=max(0, deadline - loop.time()))
    except BaseException as interrupted:
        for task in owned:
            if not task.done():
                task.cancel()
        error = close_task.exception() if close_task.done() and not close_task.cancelled() else None
        return HttpCleanupResult(
            False,
            error or interrupted,
            tuple(task for task in owned if not task.done()),
            interrupted,
        )
    for task in pending:
        task.cancel()
    error = None
    if close_task.done() and not close_task.cancelled():
        error = close_task.exception()
    confirmed = (
        not pending
        and not close_task.cancelled()
        and error is None
        and stream.is_closed is True
        and loop.time() < deadline
    )
    return HttpCleanupResult(confirmed, error, tuple(pending))
