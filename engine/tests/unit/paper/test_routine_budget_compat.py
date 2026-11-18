"""Unavailable new-schema authority never means unprotected admission."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from nexus.agents.budget import BudgetExhausted
from nexus.paper import routine_budget


@pytest.mark.asyncio
async def test_missing_schema_without_policy_keeps_legacy_unprotected_behavior(monkeypatch):
    session = AsyncMock()
    session.scalar.return_value = None
    monkeypatch.setattr(routine_budget, "_policies", AsyncMock(return_value=[]))
    assert (
        await routine_budget.ensure_scope(
            session, now=datetime(2026, 11, 18, tzinfo=UTC), daily_limit_usd=Decimal(3)
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("read_only", [False, True])
async def test_active_policy_with_missing_schema_refuses_before_allocation(monkeypatch, read_only):
    session = AsyncMock()
    session.scalar.return_value = None
    monkeypatch.setattr(routine_budget, "_policies", AsyncMock(return_value=[object()]))
    with pytest.raises(BudgetExhausted, match="ROUTINE_BUDGET_AUTHORITY_UNAVAILABLE"):
        await routine_budget.ensure_scope(
            session,
            now=datetime(2026, 11, 18, tzinfo=UTC),
            daily_limit_usd=Decimal(13),
            read_only=read_only,
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_checkpoint_preserves_original_error_when_rollback_fails(monkeypatch):
    from nexus.agents.budget import ResearchAccountingBusy

    session = AsyncMock()
    session.scalar.return_value = False
    checkpoint = session.begin_nested.return_value
    checkpoint.rollback.side_effect = OSError("synthetic rollback failure")
    original = ResearchAccountingBusy()
    monkeypatch.setattr(routine_budget, "require_new_send", AsyncMock(side_effect=original))
    with pytest.raises(ResearchAccountingBusy) as caught:
        await routine_budget.require_legacy_new_send(
            session, business_date=datetime(2026, 11, 18, tzinfo=UTC).date()
        )
    assert caught.value is original
    assert original.__notes__ == ["BUDGET_CHECKPOINT_ROLLBACK_UNCONFIRMED"]


@pytest.mark.asyncio
async def test_legacy_checkpoint_does_not_authorize_after_failed_rollback(monkeypatch):
    session = AsyncMock()
    session.scalar.return_value = False
    checkpoint = session.begin_nested.return_value
    checkpoint.rollback.side_effect = OSError("synthetic rollback failure")
    monkeypatch.setattr(routine_budget, "require_new_send", AsyncMock())
    with pytest.raises(OSError, match="synthetic rollback failure"):
        await routine_budget.require_legacy_new_send(
            session, business_date=datetime(2026, 11, 18, tzinfo=UTC).date()
        )
