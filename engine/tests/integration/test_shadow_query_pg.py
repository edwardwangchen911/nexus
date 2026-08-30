"""Verified read projection for Proposal Shadow facts."""

import pytest
from helpers.portfolio_api import settings
from helpers.shadow import (
    seed_complete_shadow_window,
    seed_shadow_eligible_group,
    seed_started_shadow,
)
from nexus.persistence.session import session_factory
from nexus.shadow.query import load_shadow_view
from nexus.shadow.service import advance_shadow_session
from sqlalchemy import text


@pytest.fixture(autouse=True)
def _legacy_shadow_authority_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "nexus.shadow.service.current_proposal_shadow_price_authority_available",
        lambda _source_policy_id: True,
    )


@pytest.mark.asyncio
async def test_not_started_is_distinct_from_partial_data(
    migrated_database_url: str,
) -> None:
    eligible = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        with pytest.raises(ValueError, match="SHADOW_NOT_STARTED"):
            await load_shadow_view(session, group_id=eligible.group_id)


@pytest.mark.asyncio
async def test_view_reconstructs_fixed_nav_sources_failure_and_checkpoint(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    observed = await seed_complete_shadow_window(
        migrated_database_url, started=started, through_ordinal=5
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        for index in range(5):
            await advance_shadow_session(
                session,
                group_id=started.eligible.group_id,
                session_date=started.eligible.calendar_sessions[index],
                observed_at=observed[index],
            )
    async with factory() as session, session.begin():
        await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.eligible.calendar_sessions[6],
            observed_at=observed[-1],
        )
    async with factory() as session:
        view = await load_shadow_view(session, group_id=started.eligible.group_id)

    assert view.coverage.completed == 5
    assert view.coverage.due == 0
    assert view.coverage.required == 63
    assert view.coverage.missing_sessions == ()
    assert view.coverage.next_expected_session == started.eligible.calendar_sessions[5]
    assert tuple(item.variant for item in view.portfolios) == (
        "hold_current",
        "minimum_change",
        "strategy_target",
    )
    assert all(len(item.nav) == 5 for item in view.portfolios)
    assert (view.freshness.status, view.freshness.reason) == (
        "unknown",
        "NO_SESSION_DUE",
    )
    assert {(item.source, item.delivery_mode) for item in view.sources} == {
        ("alpaca", "delayed"),
        ("fixture", "unknown"),
    }
    assert view.last_failed_attempt is not None
    assert view.last_failed_attempt.reason == "SESSION_GAP"
    assert len(view.checkpoints) == 1
    assert view.checkpoints[0].ordinal == 5
    assert all(
        item.metrics.net_cumulative_return.status == "known" for item in view.checkpoints[0].metrics
    )
    assert view.creates_orders is view.trading_approval is False


@pytest.mark.asyncio
async def test_state_hash_corruption_fails_closed(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    observed = await seed_complete_shadow_window(
        migrated_database_url, started=started, through_ordinal=1
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=observed[0],
        )
    async with factory() as session, session.begin():
        await session.execute(text("ALTER TABLE shadow_portfolio_states DISABLE TRIGGER USER"))
        await session.execute(
            text("UPDATE shadow_portfolio_states SET state_hash = :value"),
            {"value": "f" * 64},
        )
        await session.execute(text("ALTER TABLE shadow_portfolio_states ENABLE TRIGGER USER"))
    async with factory() as session:
        with pytest.raises(ValueError, match="DATA_PARTIAL"):
            await load_shadow_view(session, group_id=started.eligible.group_id)
