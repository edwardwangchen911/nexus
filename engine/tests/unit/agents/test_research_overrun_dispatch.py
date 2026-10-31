"""A verified overrun stops already-authorized sibling guards before SQL I/O."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus.agents.codex_sdk import CodexInvocationError
from nexus.agents.research_dispatch import ResearchDispatch, dispatch_member, stop_current_dispatch
from nexus.agents.research_stages import protocol as native
from nexus.persistence.models.agents import AgentModelConfigRecord


@pytest.mark.asyncio
async def test_overrun_signals_shared_dispatch_before_stop_write(monkeypatch):
    first, peer, third = uuid4(), uuid4(), uuid4()
    scope = uuid4()
    dispatch = ResearchDispatch(
        request_id=scope,
        manifest=SimpleNamespace(members=[SimpleNamespace(run_id=r) for r in (first, peer, third)]),
    )
    dispatch.start(first)
    dispatch.start(peer)
    protocol = native.StagedProtocol(None, on_failure=stop_current_dispatch)
    row = {"verified": True, "eligible": True, "input_tokens": 200, "output_tokens": 0}
    monkeypatch.setattr(native, "stage_evidence", AsyncMock(return_value=[row]))
    model = SimpleNamespace(
        version="synthetic-legacy-overrun",
        max_input_tokens=100,
        max_output_tokens=10,
        input_usd_per_million=Decimal(1),
        output_usd_per_million=Decimal(1),
    )

    async def get(kind, key):
        return model if kind is AgentModelConfigRecord else SimpleNamespace(model_config_id=uuid4())

    async def write(*args, **kwargs):
        # This is the first awaited write after detecting the overrun. The
        # independent peer's final transport guard must already see the stop.
        assert dispatch.stopped and protocol.local_stopped
        with dispatch_member(dispatch, peer):
            with pytest.raises(CodexInvocationError, match="CODEX_RESEARCH_STOPPED"):
                dispatch.guard()
        with pytest.raises(ValueError, match="RESEARCH_DISPATCH_INVALID"):
            dispatch.start(third)

    session = SimpleNamespace(
        get=get,
        scalar=AsyncMock(return_value=SimpleNamespace(reserved_usd=Decimal(1))),
        execute=AsyncMock(side_effect=write),
    )
    with dispatch_member(dispatch, first):
        await protocol._fence_exceeded_usage(session, member={"scope_id": scope}, run_id=first)
    session.execute.assert_awaited_once()
