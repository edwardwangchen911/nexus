"""Bounded stop and owned-task result collection for research execution."""

from __future__ import annotations

import asyncio
from typing import Any


async def stop_preserving_error(
    protocol: Any, scope: Any, error: Any, *, deadline: Any = None, task: Any = None
) -> Any:
    protocol.local_stopped = True
    try:
        if deadline is None:
            await protocol.stop(scope)
        else:
            if task is None:
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError
                task = asyncio.create_task(protocol.stop(scope))
                task.add_done_callback(consume_owned_task)
            if task.done():
                task.result()
                return
            done, _ = await asyncio.wait(
                (task,), timeout=max(0, deadline - asyncio.get_running_loop().time())
            )
            if task not in done:
                task.cancel()
                raise TimeoutError
            task.result()
    except BaseException:
        if task is not None and not task.done():
            task.cancel()
        if deadline is not None:
            from nexus.paper.manual_host_exit import mark_host_drain_unconfirmed

            mark_host_drain_unconfirmed()
        error.add_note("SCOPE_STOP_UNCONFIRMED")


def consume_owned_task(task: Any) -> Any:
    try:
        task.result()
    except BaseException:
        pass
