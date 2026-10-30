"""One owned HTTP attempt; caller must obtain native authorization before entry.

No producer selects this adapter yet. It grants neither execution nor budget
permission and never retries or follows redirects. Resource handles remain owned
after unconfirmed cleanup, including a response arriving after cancellation.
"""

import asyncio
import math
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx

from nexus.agents.responses_stage_stream import HttpStageObservation, collect_response_stage
from nexus.agents.responses_stream import (
    HttpCleanupResult,
    ResponsesStreamFacts,
    StreamProtocolError,
    close_owned_stream,
)


@dataclass
class HttpRequestOwner:
    client: httpx.AsyncClient = field(repr=False)
    response: httpx.Response | None = field(default=None, repr=False)
    send_started: bool = False
    abandoned: bool = False
    response_close_started: bool = False
    response_close_confirmed: bool = False
    response_close_error: BaseException | None = field(default=None, repr=False)
    stage: HttpStageObservation | None = None
    request_task: asyncio.Task[object] | None = field(default=None, repr=False)

    async def close_response(self) -> None:
        if self.response is None or self.response_close_started:
            return
        self.response_close_started = True
        try:
            await self.response.aclose()
            self.response_close_confirmed = self.response.is_closed
        except BaseException as error:
            self.response_close_error = error
            raise

    async def aclose(self) -> None:
        try:
            # If headers arrive later, acquire() owns that eventual response.
            if self.request_task is not None and self.request_task.done() and self.stage is None:
                await self.close_response()
        finally:
            await self.client.aclose()

    @property
    def is_closed(self) -> bool:
        return self.client.is_closed and (
            self.response is None
            or (self.stage.cleanup.confirmed if self.stage else self.response_close_confirmed)
        )


@dataclass(frozen=True)
class HttpRequestObservation:
    owner: HttpRequestOwner = field(repr=False)
    stage: HttpStageObservation | None
    error: BaseException | None
    cleanup: HttpCleanupResult


async def send_response_stage(
    client: httpx.AsyncClient,
    request: httpx.Request,
    *,
    work_deadline: float,
    cleanup_deadline: float,
    before_send: Callable[[], None] | None = None,
    on_terminal: Callable[[dict[str, Any], ResponsesStreamFacts], None] | None = None,
) -> HttpRequestObservation:
    """Consume an exclusively owned client and one already-authorized request.

    Deadlines are inherited absolute loop times, never renewed. A send attempt
    without headers has no provider receipt; even confirmed local closure does
    not establish remote cancellation or zero usage. The caller persists these
    facts and retains any pending tasks before propagating the original error.
    """
    owner = HttpRequestOwner(client)
    loop = asyncio.get_running_loop()

    async def acquire() -> object:
        if (
            not math.isfinite(work_deadline)
            or not math.isfinite(cleanup_deadline)
            or work_deadline >= cleanup_deadline
        ):
            raise StreamProtocolError("RESPONSES_DEADLINE_INVALID")
        if loop.time() >= work_deadline:
            raise TimeoutError("RESPONSES_WORK_DEADLINE")
        if before_send is not None:
            before_send()
        if loop.time() >= work_deadline:
            raise TimeoutError("RESPONSES_WORK_DEADLINE")
        owner.send_started = True
        owner.response = await client.send(request, stream=True, follow_redirects=False, auth=None)
        if owner.abandoned:
            await owner.close_response()
        return None

    task = owner.request_task = asyncio.create_task(acquire())
    error = None
    try:
        timeout = max(0, work_deadline - loop.time()) if math.isfinite(work_deadline) else 0
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if not done:
            raise TimeoutError("RESPONSES_WORK_DEADLINE")
        await task
        assert owner.response is not None
        owner.stage = await collect_response_stage(
            owner.response,
            work_deadline=work_deadline,
            cleanup_deadline=cleanup_deadline,
            on_terminal=on_terminal,
        )
        error = owner.stage.error or owner.stage.cleanup.interrupted
    except BaseException as caught:
        error = caught
    owner.abandoned = True
    cleanup = await close_owned_stream(
        owner,
        task,
        deadline=cleanup_deadline if math.isfinite(cleanup_deadline) else loop.time(),
    )
    # Keep child handles too: cancellation or a deadline is not task exit.
    if owner.stage is not None and owner.stage.cleanup.pending:
        cleanup = HttpCleanupResult(
            False,
            cleanup.error,
            tuple(dict.fromkeys((*cleanup.pending, *owner.stage.cleanup.pending))),
            cleanup.interrupted,
        )
    return HttpRequestObservation(owner, owner.stage, error, cleanup)
