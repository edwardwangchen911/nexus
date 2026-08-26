"""PostgreSQL point-in-time authority contracts for Proposal Shadow sessions."""

from datetime import UTC, datetime, timedelta

import pytest
from helpers.portfolio_api import settings
from helpers.shadow import (
    seed_shadow_event,
    seed_shadow_price_authority,
    seed_shadow_query_proof,
    seed_started_shadow,
)
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.shadow import ShadowGroupRecord
from nexus.persistence.session import session_factory
from nexus.shadow.authority import LockedShadowRoot, load_shadow_session_input
from sqlalchemy import select


async def _root_and_decision(
    database_url: str,
) -> tuple[LockedShadowRoot, datetime, tuple[object, ...]]:
    started = await seed_started_shadow(database_url)
    factory = session_factory(settings(database_url))
    async with factory() as session:
        record = await session.get(ShadowGroupRecord, started.start.shadow_group_id)
        assert record is not None
        decision = await session.scalar(
            select(ExchangeCalendarSessionRecord.decision_at).where(
                ExchangeCalendarSessionRecord.calendar_authority_id
                == record.calendar_authority_id,
                ExchangeCalendarSessionRecord.session == record.start_session,
            )
        )
    assert isinstance(decision, datetime)
    return (
        LockedShadowRoot.from_record(record),
        decision.astimezone(UTC),
        started.eligible.instrument_ids,
    )


@pytest.mark.asyncio
async def test_selects_latest_visible_raw_row_without_field_stitching(
    migrated_database_url: str,
) -> None:
    root, decision, raw_instruments = await _root_and_decision(migrated_database_url)
    instrument = raw_instruments[0]
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        old = await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[0],
            available_at=decision - timedelta(minutes=20),
            revision="1",
            open_price="99",
            close_price="100",
        )
        selected = await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[0],
            available_at=decision - timedelta(minutes=10),
            revision="2",
            open_price="101",
            close_price="103",
        )
        await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[0],
            available_at=decision + timedelta(minutes=1),
            revision="3",
            open_price="900",
            close_price="901",
        )
        adjusted = await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[0],
            available_at=decision - timedelta(minutes=5),
            adjustment="all",
            revision="4",
            open_price="500",
            close_price="501",
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=(instrument,),
            complete=True,
            member_ids=(old.bar_id, selected.bar_id, adjusted.bar_id),
            available_at=decision - timedelta(seconds=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=(instrument,),
            complete=True,
            available_at=decision - timedelta(seconds=1),
        )

    async with factory() as session:
        result = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision,
            required_instruments=(instrument,),
            initial_execution_instruments=(instrument,),
        )
        replay = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision,
            required_instruments=(instrument,),
            initial_execution_instruments=(instrument,),
        )

    assert result.status == "complete"
    assert result.reason is None
    assert result.authority_hash is not None and len(result.authority_hash) == 64
    assert len(result.bars) == 1
    assert result.bars[0].price_bar_record_id == selected.bar_id
    assert (str(result.bars[0].open), str(result.bars[0].close)) == (
        "101.000000000000",
        "103.000000000000",
    )
    assert result.bars[0].usage == "execution_and_mark"
    assert result.delivery_mode_summary == ("delayed",)
    assert result.maximum_available_at is not None
    assert result.maximum_available_at <= result.observed_at
    assert result.late_backfill is False
    assert replay.authority_hash == result.authority_hash
    assert replay.source_summary == result.source_summary
    assert replay.query_hash == result.query_hash
    assert tuple((item.source, item.kind) for item in result.source_summary) == (
        ("alpaca", "price_bars"),
        ("fixture", "corporate_actions"),
    )


@pytest.mark.asyncio
async def test_incomplete_query_returns_partial_without_authority(
    migrated_database_url: str,
) -> None:
    root, decision, raw_instruments = await _root_and_decision(migrated_database_url)
    instrument = raw_instruments[0]
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="price_bars",
            source="fosun",
            delivery_mode="real_time",
            instrument_ids=(instrument,),
            complete=False,
            available_at=decision - timedelta(seconds=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=(instrument,),
            complete=True,
            available_at=decision - timedelta(seconds=1),
        )

    async with factory() as session:
        result = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision,
            required_instruments=(instrument,),
            initial_execution_instruments=(instrument,),
        )
    assert (result.status, result.reason, result.authority_hash) == (
        "data_partial",
        "SOURCE_QUERY_INCOMPLETE",
        None,
    )
    assert result.bars == result.missing_execution_inputs == ()


@pytest.mark.asyncio
async def test_query_proof_rejects_member_without_visible_database_row(
    migrated_database_url: str,
) -> None:
    root, decision, raw_instruments = await _root_and_decision(migrated_database_url)
    instrument = raw_instruments[0]
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        bar = await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[0],
            available_at=decision - timedelta(minutes=2),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=(instrument,),
            complete=True,
            member_ids=(bar.bar_id,),
            available_at=decision - timedelta(seconds=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="price_bars",
            source="fosun",
            delivery_mode="real_time",
            instrument_ids=(instrument,),
            complete=True,
            member_ids=(new_uuid7(),),
            available_at=decision - timedelta(seconds=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=(instrument,),
            complete=True,
            available_at=decision - timedelta(seconds=1),
        )

    async with factory() as session:
        result = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision,
            required_instruments=(instrument,),
            initial_execution_instruments=(instrument,),
        )

    assert (result.status, result.reason, result.authority_hash) == (
        "data_partial",
        "SOURCE_QUERY_SET_MISMATCH",
        None,
    )


@pytest.mark.asyncio
async def test_query_proof_cannot_claim_same_instrument_present_and_missing(
    migrated_database_url: str,
) -> None:
    root, decision, raw_instruments = await _root_and_decision(migrated_database_url)
    instrument = raw_instruments[0]
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        bar = await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[0],
            available_at=decision - timedelta(minutes=2),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=(instrument,),
            complete=True,
            member_ids=(bar.bar_id,),
            negative_instrument_ids=(instrument,),
            available_at=decision - timedelta(seconds=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=(instrument,),
            complete=True,
            available_at=decision - timedelta(seconds=1),
        )

    async with factory() as session:
        result = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision,
            required_instruments=(instrument,),
            initial_execution_instruments=(instrument,),
        )

    assert (result.status, result.reason, result.authority_hash) == (
        "data_partial",
        "SOURCE_QUERY_SET_MISMATCH",
        None,
    )


@pytest.mark.asyncio
async def test_session_cannot_advance_before_frozen_calendar_decision_time(
    migrated_database_url: str,
) -> None:
    root, decision, raw_instruments = await _root_and_decision(migrated_database_url)
    instrument = raw_instruments[0]
    observed_at = decision - timedelta(minutes=1)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        bar = await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[0],
            available_at=observed_at - timedelta(minutes=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=(instrument,),
            complete=True,
            member_ids=(bar.bar_id,),
            available_at=observed_at - timedelta(seconds=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=(instrument,),
            complete=True,
            available_at=observed_at - timedelta(seconds=1),
        )

    async with factory() as session:
        result = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=observed_at,
            required_instruments=(instrument,),
            initial_execution_instruments=(instrument,),
        )

    assert (result.status, result.reason, result.authority_hash) == (
        "data_stale",
        "SESSION_NOT_DUE",
        None,
    )


@pytest.mark.asyncio
async def test_complete_negative_freezes_missing_bar_against_late_revision(
    migrated_database_url: str,
) -> None:
    root, decision, raw_instruments = await _root_and_decision(migrated_database_url)
    instrument = raw_instruments[0]
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=(instrument,),
            complete=True,
            negative_instrument_ids=(instrument,),
            available_at=decision - timedelta(seconds=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=(instrument,),
            complete=True,
            available_at=decision - timedelta(seconds=1),
        )
    async with factory() as session:
        first = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision,
            required_instruments=(instrument,),
            initial_execution_instruments=(instrument,),
        )
    assert first.status == "complete"
    assert len(first.missing_execution_inputs) == 1

    async with factory() as session, session.begin():
        await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[0],
            available_at=decision + timedelta(minutes=5),
            revision="late",
        )
    async with factory() as session:
        replay_input = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision,
            required_instruments=(instrument,),
            initial_execution_instruments=(instrument,),
        )
    assert replay_input.authority_hash == first.authority_hash
    assert replay_input.missing_execution_inputs == first.missing_execution_inputs
    assert replay_input.bars == ()


@pytest.mark.asyncio
async def test_missing_later_close_cannot_use_a_future_session_bar(
    migrated_database_url: str,
) -> None:
    root, first_decision, raw_instruments = await _root_and_decision(
        migrated_database_url
    )
    instrument = raw_instruments[0]
    session_date = root.evaluation_sessions[1]
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        decision = await session.scalar(
            select(ExchangeCalendarSessionRecord.decision_at).where(
                ExchangeCalendarSessionRecord.calendar_authority_id
                == root.calendar.authority_id,
                ExchangeCalendarSessionRecord.session == session_date,
            )
        )
    assert isinstance(decision, datetime) and decision > first_decision
    async with factory() as session, session.begin():
        await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[2],
            available_at=decision - timedelta(minutes=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=session_date,
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=(instrument,),
            complete=True,
            negative_instrument_ids=(instrument,),
            available_at=decision - timedelta(seconds=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=session_date,
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=(instrument,),
            complete=True,
            available_at=decision - timedelta(seconds=1),
        )
    async with factory() as session:
        result = await load_shadow_session_input(
            session,
            root=root,
            session_date=session_date,
            ordinal=2,
            observed_at=decision,
            required_instruments=(instrument,),
            initial_execution_instruments=(),
        )
    assert (result.status, result.reason, result.bars) == (
        "data_partial",
        "MARK_BAR_MISSING",
        (),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action_type", "rate", "expected_status", "expected_reason"),
    (
        ("merger_cash", "75", "complete", None),
        ("bankruptcy", "0", "complete", None),
        ("delisting", None, "data_partial", "UNRESOLVED_DELISTING"),
    ),
)
async def test_delisting_resolution_controls_close_requirement(
    migrated_database_url: str,
    action_type: str,
    rate: str | None,
    expected_status: str,
    expected_reason: str | None,
) -> None:
    root, decision, raw_instruments = await _root_and_decision(migrated_database_url)
    instrument = raw_instruments[0]
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        event = await seed_shadow_event(
            session,
            instrument_id=instrument,
            source="fixture",
            provider_event_id=f"delist-{action_type}",
            action_type=action_type,
            process_date=root.evaluation_sessions[0],
            available_at=decision - timedelta(days=1),
            rate=rate,
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=(instrument,),
            complete=True,
            negative_instrument_ids=(instrument,),
            available_at=decision - timedelta(seconds=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=(instrument,),
            complete=True,
            member_ids=(event.event_id,),
            available_at=decision - timedelta(seconds=1),
        )
    async with factory() as session:
        result = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision,
            required_instruments=(instrument,),
            initial_execution_instruments=(),
        )
    assert (result.status, result.reason) == (expected_status, expected_reason)
    if expected_status == "complete":
        assert result.bars == ()
        assert [item.event_kind for item in result.events] == ["delisting"]
        assert result.events[0].delisting is not None


@pytest.mark.asyncio
async def test_conflicting_raw_sources_fail_closed(
    migrated_database_url: str,
) -> None:
    root, decision, raw_instruments = await _root_and_decision(migrated_database_url)
    instrument = raw_instruments[0]
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        alpaca = await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[0],
            available_at=decision - timedelta(minutes=2),
            open_price="100",
            close_price="101",
        )
        fixture = await seed_shadow_price_authority(
            session,
            instrument_id=instrument,
            session_date=root.evaluation_sessions[0],
            available_at=decision - timedelta(minutes=1),
            source="fixture",
            delivery_mode="fixture",
            open_price="150",
            close_price="151",
        )
        for source, delivery_mode, bar_id in (
            ("alpaca", "delayed", alpaca.bar_id),
            ("fixture", "fixture", fixture.bar_id),
        ):
            await seed_shadow_query_proof(
                session,
                shadow_group_id=root.shadow_group_id,
                session_date=root.evaluation_sessions[0],
                kind="price_bars",
                source=source,
                delivery_mode=delivery_mode,
                instrument_ids=(instrument,),
                complete=True,
                member_ids=(bar_id,),
                available_at=decision - timedelta(seconds=1),
            )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=(instrument,),
            complete=True,
            available_at=decision - timedelta(seconds=1),
        )
    async with factory() as session:
        result = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision,
            required_instruments=(instrument,),
            initial_execution_instruments=(instrument,),
        )
    assert (result.status, result.reason, result.authority_hash) == (
        "source_conflict",
        "SOURCE_CONFLICT",
        None,
    )


@pytest.mark.asyncio
async def test_events_are_bound_in_kernel_order(
    migrated_database_url: str,
) -> None:
    root, decision, raw_instruments = await _root_and_decision(migrated_database_url)
    instruments = raw_instruments
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        bars = [
            await seed_shadow_price_authority(
                session,
                instrument_id=instrument,
                session_date=root.evaluation_sessions[0],
                available_at=decision - timedelta(minutes=5),
            )
            for instrument in instruments
        ]
        split = await seed_shadow_event(
            session,
            instrument_id=instruments[0],
            source="fixture",
            provider_event_id="split-1",
            action_type="stock_split",
            process_date=root.evaluation_sessions[0],
            ex_date=root.evaluation_sessions[0],
            available_at=decision - timedelta(days=2),
            rate="2",
        )
        dividend = await seed_shadow_event(
            session,
            instrument_id=instruments[1],
            source="fixture",
            provider_event_id="dividend-1",
            action_type="cash_dividend",
            process_date=root.evaluation_sessions[0],
            ex_date=root.evaluation_sessions[0],
            payable_date=root.evaluation_sessions[2],
            available_at=decision - timedelta(days=1),
            rate="1.25",
        )
        delisting = await seed_shadow_event(
            session,
            instrument_id=instruments[1],
            source="fixture",
            provider_event_id="delisting-1",
            action_type="merger_cash",
            process_date=root.evaluation_sessions[0],
            available_at=decision - timedelta(hours=1),
            rate="80",
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=instruments,
            complete=True,
            member_ids=tuple(item.bar_id for item in bars),
            available_at=decision - timedelta(seconds=1),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=instruments,
            complete=True,
            member_ids=(split.event_id, dividend.event_id, delisting.event_id),
            available_at=decision - timedelta(seconds=1),
        )
    async with factory() as session:
        result = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision,
            required_instruments=instruments,
            initial_execution_instruments=instruments,
        )
    assert result.status == "complete"
    assert [item.event_kind for item in result.events] == [
        "split",
        "dividend",
        "delisting",
    ]


@pytest.mark.asyncio
async def test_event_announced_after_effective_boundary_is_not_backfilled(
    migrated_database_url: str,
) -> None:
    root, decision, instruments = await _root_and_decision(migrated_database_url)
    instrument = instruments[0]
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        late = await seed_shadow_event(
            session,
            instrument_id=instrument,
            source="fixture",
            provider_event_id="late-split",
            action_type="stock_split",
            process_date=root.evaluation_sessions[0],
            ex_date=root.evaluation_sessions[0],
            available_at=decision + timedelta(minutes=1),
            rate="2",
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=root.shadow_group_id,
            session_date=root.evaluation_sessions[0],
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=(instrument,),
            complete=True,
            member_ids=(late.event_id,),
            available_at=decision + timedelta(minutes=1),
        )
    async with factory() as session:
        result = await load_shadow_session_input(
            session,
            root=root,
            session_date=root.evaluation_sessions[0],
            ordinal=1,
            observed_at=decision + timedelta(days=2),
            required_instruments=(instrument,),
            initial_execution_instruments=(),
        )
    assert (result.status, result.reason) == (
        "data_partial",
        "EVENT_QUERY_INCOMPLETE",
    )
    assert result.late_backfill is True
