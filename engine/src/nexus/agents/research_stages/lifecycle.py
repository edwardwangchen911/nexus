"""One authorized SDK invocation and its original bounded cleanup lifecycle."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import timedelta
from typing import Any

from nexus.agents import codex_sdk
from nexus.agents.research_stages.protocol import ProtocolRejected
from nexus.agents.research_stages.support import consume_owned_task, stop_preserving_error


async def invoke_stage(
    protocol: Any,
    scope: Any,
    ticket: Any,
    wire: Any,
    clock: Any,
    wall_anchor: Any,
    monotonic_anchor: Any,
    original_deadline: Any,
    original_expiry: Any,
    payload: Any,
    options: Any,
) -> Any:
    observed = None
    cleanup_seen = False
    cleanup_deadline = None
    scope_stop_task = None

    def configure(state: Any) -> Any:
        state.cleanup_deadline = min(
            state.stage_deadline + 7,
            original_deadline,
            monotonic_anchor + (original_expiry - wall_anchor).total_seconds(),
        )

    def checked(raw: Any, usage: Any) -> Any:
        nonlocal observed

        async def write_observation() -> Any:
            await protocol.observe(
                ticket,
                now=clock(),
                payload=raw.encode(),
                tokens=(usage.input_tokens, usage.output_tokens),
            )
            if cleanup_deadline is None or asyncio.get_running_loop().time() >= cleanup_deadline:
                raise ProtocolRejected("VERIFICATION_WINDOW_CLOSED")
            await protocol.verify(
                ticket,
                now=clock(),
                wire_hash=hashlib.sha256(wire).hexdigest(),
                payload=raw.encode(),
                tokens=(usage.input_tokens, usage.output_tokens),
                not_after=wall_anchor + timedelta(seconds=cleanup_deadline - monotonic_anchor),
            )

        observed = asyncio.create_task(write_observation())

    async def close(client: Any, run_task: Any, *, deadline: Any, primary: Any) -> Any:
        nonlocal cleanup_seen, cleanup_deadline, scope_stop_task
        from nexus.agents.research_dispatch import stop_current_dispatch
        from nexus.paper.manual_host_exit import mark_host_drain_unconfirmed

        loop = asyncio.get_running_loop()
        cleanup_deadline = min(deadline, loop.time() + 7)
        primary_from_work = primary is not None
        owned = []
        process: Any = getattr(
            getattr(getattr(client, "_client", None), "_sync", None), "_proc", None
        )
        original_close = client.close
        sdk_close_tasks: list[asyncio.Task[Any]] = []

        async def tracked_close() -> Any:
            task = asyncio.current_task()
            assert task is not None
            sdk_close_tasks.append(task)
            task.add_done_callback(consume_owned_task)
            try:
                return await original_close()
            except BaseException as error:
                # Native close may catch this and keep waiting for
                # child exit. Fence siblings before that wait.
                stop_dispatch(error)
                raise

        client.close = tracked_close

        def own(task: Any, *, stops_on_failure: Any = False) -> Any:
            def collected(completed: Any) -> Any:
                try:
                    completed.result()
                except BaseException as error:
                    if stops_on_failure:
                        # Do not wait for the other cleanup child
                        # before fencing sibling stages/replacements.
                        stop_dispatch(error)

            owned.append(task)
            task.add_done_callback(collected)
            return task

        def stop_dispatch(error: Any) -> Any:
            nonlocal scope_stop_task
            # Synchronous shared stop precedes every await. Persist
            # concurrently so child termination cannot consume its budget.
            protocol.local_stopped = True
            stop_current_dispatch(error)
            if scope_stop_task is None and loop.time() < cleanup_deadline:
                scope_stop_task = own(asyncio.create_task(protocol.stop(scope)))

        if primary is not None:
            stop_dispatch(primary)

        async def finish_cleanup() -> Any:
            nonlocal cleanup_seen
            first = None
            native = own(
                asyncio.create_task(
                    codex_sdk._close_portfolio_client(client, run_task, deadline=cleanup_deadline)
                ),
                stops_on_failure=True,
            )
            waiting = {native}
            if observed is not None:
                waiting.add(own(observed, stops_on_failure=True))
            done, pending = await asyncio.wait(
                waiting, timeout=max(0, cleanup_deadline - loop.time())
            )
            for task in (observed, native):
                if task is not None and task in done:
                    try:
                        task.result()
                    except BaseException as error:
                        if first is None:
                            first = error
            if pending:
                for task in pending:
                    task.cancel()
                first = first or ProtocolRejected("SDK_CLEANUP_UNCONFIRMED")
                first.add_note("CLEANUP_NATIVE_OR_OBSERVATION_WAIT_EXPIRED")
            # Exceptions in poll/router may bypass native helper's
            # tail. Keep only the original child/task identities.
            if first is not None:
                mark_host_drain_unconfirmed()
                try:
                    if process.poll() is None:
                        process.kill()
                except BaseException:
                    try:
                        process.kill()
                    except BaseException:
                        pass
            tracked = set(sdk_close_tasks)
            if run_task is not None:
                tracked.add(run_task)
            for task in tracked:
                if task not in owned:
                    own(task)
            if tracked:
                _, unfinished = await asyncio.wait(
                    tracked, timeout=max(0, cleanup_deadline - loop.time())
                )
                if unfinished:
                    for task in unfinished:
                        task.cancel()
                    first = first or ProtocolRejected("SDK_CLEANUP_UNCONFIRMED")
                    mark_host_drain_unconfirmed()
            try:
                exited = process.poll() is not None
            except BaseException as error:
                exited = False
                first = first or error
            if not exited:
                first = first or ProtocolRejected("SDK_EXIT_UNCONFIRMED")
            native_ok = native in done and not native.cancelled() and native.exception() is None
            if native_ok and exited and loop.time() < cleanup_deadline:

                async def publish_cleanup() -> Any:
                    if loop.time() >= cleanup_deadline:
                        raise ProtocolRejected("CLEANUP_WINDOW_CLOSED")
                    await protocol.cleanup(
                        ticket,
                        now=clock(),
                        confirmed=True,
                        not_after=wall_anchor
                        + timedelta(seconds=cleanup_deadline - monotonic_anchor),
                    )

                receipt = own(asyncio.create_task(publish_cleanup()), stops_on_failure=True)
                done_receipt, _ = await asyncio.wait(
                    (receipt,), timeout=max(0, cleanup_deadline - loop.time())
                )
                if receipt in done_receipt:
                    try:
                        receipt.result()
                        cleanup_seen = True
                    except BaseException as error:
                        first = first or error
                else:
                    receipt.cancel()
                    first = first or ProtocolRejected("SDK_CLEANUP_UNCONFIRMED")
                    first.add_note("CLEANUP_RECEIPT_WAIT_EXPIRED")
            if not cleanup_seen:
                first = first or ProtocolRejected("SDK_CLEANUP_UNCONFIRMED")
            if first is not None:
                mark_host_drain_unconfirmed()
            return first

        cleanup_task = own(asyncio.create_task(finish_cleanup()))
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as error:
                stop_dispatch(error)
                if primary is None:
                    primary = error
            except BaseException:
                break
        try:
            failure = cleanup_task.result()
        except BaseException as error:
            mark_host_drain_unconfirmed()
            failure = error
        if primary is not None:
            if failure is not None:
                primary.add_note("SDK_CLEANUP_UNCONFIRMED")
            if not primary_from_work:
                raise primary
            # The SDK is already unwinding its own work exception.
            # Returning lets that original error propagate without
            # presenting it to SDK finally as a cleanup failure.
            return
        if failure is not None:
            raise failure

    hooks = codex_sdk.ResearchLifecycle(configure=configure, checked=checked, close=close)
    try:
        result = await codex_sdk.invoke_codex(payload, **options, lifecycle=hooks)
        if not cleanup_seen:
            raise ProtocolRejected("SDK_CLEANUP_UNCONFIRMED")
        return result
    except BaseException as error:
        await stop_preserving_error(
            protocol, scope, error, deadline=cleanup_deadline, task=scope_stop_task
        )
        raise
