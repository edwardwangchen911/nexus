"""The native controller must own its propagating error, not its caller's."""

import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.ops import agent_paper_once, manual_paper_host


@pytest.mark.asyncio
@pytest.mark.parametrize("outer_error", [False, True])
@pytest.mark.parametrize("outcome", ["success", "failure", "cancel"])
async def test_controller_disposal_preserves_only_its_own_exception(
    monkeypatch, outer_error, outcome
):
    disposal = RuntimeError("controlled disposal failure")
    caller = ValueError("unrelated caller diagnostic")
    primary = (
        asyncio.CancelledError("controlled cancellation")
        if outcome == "cancel"
        else ValueError("controlled worker failure")
    )
    disposals = []

    async def dispose():
        disposals.append(True)
        raise disposal

    def sessions():
        raise AssertionError("no database access in resource-exit test")

    sessions.kw = {"bind": SimpleNamespace(dispose=dispose)}
    monkeypatch.setattr(manual_paper_host, "session_factory", lambda settings: sessions)

    async def worker(*args, **kwargs):
        raise primary

    monkeypatch.setattr(agent_paper_once, "run_cycle_job_once", worker)
    members = [] if outcome == "success" else [SimpleNamespace(run_id=uuid4(), job_id=uuid4())]
    view = SimpleNamespace(request_id=uuid4(), research=SimpleNamespace(members=members))
    expected = disposal if outcome == "success" else primary

    async def invoke():
        with pytest.raises(type(expected)) as raised:
            await manual_paper_host._dispatch_two_flight_members(object(), view=view)
        assert raised.value is expected

    if outer_error:
        try:
            raise caller
        except ValueError:
            # Deliberately direct await: create_task would clear this caller's
            # active exception context and miss the regression.
            await invoke()
    else:
        await invoke()
    assert disposals == [True]
    assert not getattr(caller, "__notes__", [])
    if outcome != "success":
        assert "RESEARCH_DISPATCH_CLEANUP_UNCONFIRMED" in primary.__notes__
