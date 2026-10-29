"""Formal SDK hook ownership, without network, process launch or database writes."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from nexus.agents.codex_sdk import CodexInvocationError
from nexus.agents.research_stages.lifecycle import invoke_stage
from nexus.paper.manual_host_exit import host_drain_scope
from unit.agents.test_codex_sdk import _sdk


class ProofRecorder:
    local_stopped = False

    def __init__(self):
        self.facts = []

    async def observe(self, *args, **kwargs):
        self.facts.append("observation")

    async def verify(self, *args, **kwargs):
        self.facts.append("verification")

    async def cleanup(self, *args, **kwargs):
        self.facts.append("cleanup")

    async def stop(self, *args, **kwargs):
        self.local_stopped = True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    ["none", "ambient", "work", "cleanup", "cancel", "ambient_cleanup", "ambient_cancel_cleanup"],
)
async def test_original_work_cleanup_and_ambient_exception_ownership(monkeypatch, failure):
    client, thread, _, _ = _sdk(monkeypatch)
    exited = [False]
    client._client._sync = SimpleNamespace(
        _proc=SimpleNamespace(
            poll=lambda: 0 if exited[0] else None,
            kill=lambda: exited.__setitem__(0, True),
        )
    )
    entered, release = asyncio.Event(), asyncio.Event()
    close = client.close
    original = CodexInvocationError("CODEX_RESPONSE_INCOMPLETE", provider_call_started=True)
    cleanup_fails = failure in {"cleanup", "ambient_cleanup", "ambient_cancel_cleanup"}

    async def finish():
        entered.set()
        if failure == "cancel":
            await release.wait()
        if cleanup_fails:
            raise RuntimeError("controlled close failure")
        exited[0] = True

    close.side_effect = finish
    if failure == "work":
        thread.run.side_effect = original
    protocol = ProofRecorder()
    wall, anchor = datetime.now(UTC), asyncio.get_running_loop().time()

    async def invoke():
        return await invoke_stage(
            protocol,
            "scope",
            "ticket",
            b"wire",
            lambda: wall + timedelta(seconds=asyncio.get_running_loop().time() - anchor),
            wall,
            anchor,
            anchor + 10,
            wall + timedelta(seconds=10),
            {},
            {"timeout_seconds": 3, "deadline_at": anchor + 3},
        )

    with host_drain_scope() as drain:
        if failure == "ambient":
            try:
                raise original
            except CodexInvocationError:
                await invoke()
        elif failure in {"ambient_cleanup", "ambient_cancel_cleanup"}:
            ambient = (
                asyncio.CancelledError("prior cancel")
                if failure == "ambient_cancel_cleanup"
                else original
            )
            before = original.failure_facts
            try:
                raise ambient
            except BaseException:
                with pytest.raises(CodexInvocationError) as caught:
                    await invoke()
            assert caught.value is not ambient and caught.value.phase == "client_close"
            assert original.failure_facts == before
        elif failure == "work":
            with pytest.raises(CodexInvocationError) as caught:
                await invoke()
            assert caught.value is original
            assert original.failure_facts is None or original.failure_facts.cleanup_kind is None
        elif failure == "cleanup":
            with pytest.raises(CodexInvocationError) as caught:
                await invoke()
            assert caught.value.phase == "client_close"
        elif failure == "cancel":
            task = asyncio.create_task(invoke())
            await entered.wait()
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await invoke()
        assert drain.confirmed == (not cleanup_fails)
    thread.run.assert_awaited_once()
    close.assert_awaited_once()
    assert exited[0]
    assert protocol.local_stopped == (failure in {"work", "cancel"} or cleanup_fails)
    assert ("cleanup" in protocol.facts) == (not cleanup_fails)
    assert ("verification" in protocol.facts) == (failure != "work")
