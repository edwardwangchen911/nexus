"""Atomic PostgreSQL advancement for static Proposal Shadows."""

import asyncio
from datetime import UTC, timedelta

import pytest
from helpers.portfolio_api import settings
from helpers.shadow import (
    seed_complete_shadow_session_inputs,
    seed_complete_shadow_window,
    seed_shadow_event,
    seed_shadow_query_proof,
    seed_started_shadow,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.shadow import (
    ShadowAdvanceAttemptRecord,
    ShadowCheckpointMetricRecord,
    ShadowCheckpointRecord,
    ShadowPortfolioRecord,
    ShadowPortfolioStateRecord,
    ShadowSealRecord,
    ShadowSessionAuthorityRecord,
    ShadowSimulatedFillRecord,
    ShadowSimulatedOutcomeRecord,
)
from nexus.persistence.session import session_factory
from nexus.shadow.query import load_shadow_view
from nexus.shadow.service import advance_shadow_session
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_first_session_persists_one_authority_three_states_and_fills(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    observed_at = await seed_complete_shadow_session_inputs(
        migrated_database_url, started=started, ordinal=1
    )
    factory = session_factory(settings(migrated_database_url))

    async with factory() as session, session.begin():
        result = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=observed_at,
        )

    assert (result.result, result.ordinal, result.proposal_status) == (
        "applied",
        1,
        "shadowing",
    )
    async with factory() as session:
        counts = tuple(
            [
                await session.scalar(
                    select(func.count())
                    .select_from(model)
                    .where(model.shadow_group_id == started.start.shadow_group_id)
                )
                for model in (
                    ShadowSessionAuthorityRecord,
                    ShadowPortfolioStateRecord,
                    ShadowSimulatedOutcomeRecord,
                    ShadowSimulatedFillRecord,
                    ShadowAdvanceAttemptRecord,
                )
            ]
        )
    assert counts == (1, 3, 6, 6, 1)


@pytest.mark.asyncio
async def test_successful_session_replay_is_write_free(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    observed_at = await seed_complete_shadow_session_inputs(
        migrated_database_url, started=started, ordinal=1
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        first = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=observed_at,
        )
    async with factory() as session, session.begin():
        replay = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=observed_at,
        )
    assert (first.result, replay.result) == ("applied", "replayed")
    async with factory() as session:
        attempt_count = await session.scalar(
            select(func.count()).select_from(ShadowAdvanceAttemptRecord)
        )
    assert attempt_count == 1


@pytest.mark.asyncio
async def test_gap_and_incomplete_authority_write_only_sanitized_attempts(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        observed_at = await session.scalar(
            select(ExchangeCalendarSessionRecord.decision_at).where(
                ExchangeCalendarSessionRecord.calendar_authority_id
                == started.eligible.calendar_id,
                ExchangeCalendarSessionRecord.session == started.start.start_session,
            )
        )
    assert observed_at is not None
    observed_at = observed_at.astimezone(UTC)
    second_session = started.eligible.calendar_sessions[1]

    async with factory() as session, session.begin():
        gap = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=second_session,
            observed_at=observed_at,
        )
    assert (gap.result, gap.reason) == ("session_gap", "SESSION_GAP")

    async with factory() as session, session.begin():
        await seed_shadow_query_proof(
            session,
            shadow_group_id=started.start.shadow_group_id,
            session_date=started.start.start_session,
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=started.eligible.instrument_ids,
            complete=False,
            available_at=observed_at,
        )
    async with factory() as session, session.begin():
        failed = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=observed_at,
        )
    assert failed.result == "data_partial"
    async with factory() as session:
        attempts = tuple(
            await session.scalars(
                select(ShadowAdvanceAttemptRecord).order_by(ShadowAdvanceAttemptRecord.created_at)
            )
        )
        authority_count = await session.scalar(
            select(func.count()).select_from(ShadowSessionAuthorityRecord)
        )
    assert [(item.result, item.reason_code) for item in attempts] == [
        ("session_gap", "SESSION_GAP"),
        ("data_partial", "SOURCE_QUERY_INCOMPLETE"),
    ]
    assert authority_count == 0


@pytest.mark.asyncio
async def test_all_cash_variant_creates_no_initial_outcome(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url, all_cash_variant="all")
    observed_at = await seed_complete_shadow_session_inputs(
        migrated_database_url, started=started, ordinal=1, instrument_ids=()
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        result = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=observed_at,
        )
    async with factory() as session:
        outcomes_by_variant = dict(
            [
                (
                    row.variant,
                    await session.scalar(
                        select(func.count())
                        .select_from(ShadowSimulatedOutcomeRecord)
                        .where(ShadowSimulatedOutcomeRecord.shadow_portfolio_id == row.id)
                    ),
                )
                for row in await session.scalars(select(ShadowPortfolioRecord))
            ]
        )
    assert outcomes_by_variant == {
        "hold_current": 0,
        "minimum_change": 0,
        "strategy_target": 0,
    }
    assert result.result == "applied"


@pytest.mark.asyncio
async def test_sixty_three_sessions_create_checkpoints_and_auto_validate(
    migrated_database_url: str,
) -> None:
    from nexus.persistence.models.backtest import (
        BacktestFillRecord,
        BacktestOrderOutcomeRecord,
    )
    from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
    from nexus.persistence.models.portfolio import BrokerOrderSnapshot
    from nexus.persistence.models.runtime import AuditEvent, OutboxEvent

    started = await seed_started_shadow(migrated_database_url)
    observed = await seed_complete_shadow_window(
        migrated_database_url, started=started, through_ordinal=63
    )
    factory = session_factory(settings(migrated_database_url))
    protected_models = (
        BrokerOrderSnapshot,
        BacktestFillRecord,
        BacktestOrderOutcomeRecord,
    )
    async with factory() as session:
        before = tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in protected_models
            ]
        )
    async with factory() as session, session.begin():
        results = tuple(
            [
                await advance_shadow_session(
                    session,
                    group_id=started.eligible.group_id,
                    session_date=started.eligible.calendar_sessions[index],
                    observed_at=observed[index],
                )
                for index in range(63)
            ]
        )
    assert results[-1].proposal_status == "validated"
    assert results[-1].checkpoint_ordinal == 63
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, started.eligible.group_id)
        seal = await session.scalar(select(ShadowSealRecord))
        checkpoints = tuple(
            await session.scalars(
                select(ShadowCheckpointRecord).order_by(ShadowCheckpointRecord.ordinal)
            )
        )
        metric_count = await session.scalar(
            select(func.count()).select_from(ShadowCheckpointMetricRecord)
        )
        state_count = await session.scalar(
            select(func.count()).select_from(ShadowPortfolioStateRecord)
        )
        after = tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in protected_models
            ]
        )
        view = await load_shadow_view(session, group_id=started.eligible.group_id)
        audit_actions = tuple(
            await session.scalars(
                select(AuditEvent.action)
                .where(AuditEvent.action.like("shadow.%"))
                .order_by(AuditEvent.created_at, AuditEvent.id)
            )
        )
        outbox_types = tuple(
            await session.scalars(
                select(OutboxEvent.event_type)
                .where(OutboxEvent.event_type.like("shadow.%"))
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
            )
        )
    assert group is not None and group.status == "validated"
    assert seal is not None and (seal.reason, seal.final_coverage) == ("validated", 63)
    assert [(item.ordinal, item.status) for item in checkpoints] == [
        (5, "complete"),
        (21, "complete"),
        (63, "complete"),
    ]
    assert (metric_count, state_count) == (9, 189)
    assert after == before
    assert (view.sealed, view.seal_reason, view.coverage.completed) == (
        True,
        "validated",
        63,
    )
    assert audit_actions.count("shadow.started") == 1
    assert audit_actions.count("shadow.session_applied") == 63
    assert audit_actions.count("shadow.checkpoint_created") == 3
    assert audit_actions.count("shadow.validated") == 1
    assert outbox_types == audit_actions
    async with factory() as session, session.begin():
        sealed = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.eligible.calendar_sessions[63],
            observed_at=observed[-1] + timedelta(days=1),
        )
    assert sealed.result == "sealed"


@pytest.mark.asyncio
async def test_initial_missing_execution_expires_after_complete_window(
    migrated_database_url: str,
) -> None:
    from nexus.persistence.models.optimization import OptimizationProposalGroupRecord

    started = await seed_started_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        first_decision = await session.scalar(
            select(ExchangeCalendarSessionRecord.decision_at).where(
                ExchangeCalendarSessionRecord.calendar_authority_id
                == started.eligible.calendar_id,
                ExchangeCalendarSessionRecord.session == started.start.start_session,
            )
        )
    assert first_decision is not None
    first_decision = first_decision.astimezone(UTC)
    async with factory() as session, session.begin():
        await seed_shadow_query_proof(
            session,
            shadow_group_id=started.start.shadow_group_id,
            session_date=started.start.start_session,
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=started.eligible.instrument_ids,
            complete=True,
            negative_instrument_ids=started.eligible.instrument_ids,
            available_at=first_decision - timedelta(seconds=2),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=started.start.shadow_group_id,
            session_date=started.start.start_session,
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=started.eligible.instrument_ids,
            complete=True,
            available_at=first_decision - timedelta(seconds=2),
        )
    observed = await seed_complete_shadow_window(
        migrated_database_url,
        started=started,
        start_ordinal=2,
        through_ordinal=63,
    )
    async with factory() as session, session.begin():
        first = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=first_decision,
        )
        later = tuple(
            [
                await advance_shadow_session(
                    session,
                    group_id=started.eligible.group_id,
                    session_date=started.eligible.calendar_sessions[index],
                    observed_at=observed[index - 1],
                )
                for index in range(1, 63)
            ]
        )
    assert first.result == "applied"
    assert later[-1].proposal_status == "expired"
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, started.eligible.group_id)
        seal = await session.scalar(select(ShadowSealRecord))
        fill_count = await session.scalar(
            select(func.count()).select_from(ShadowSimulatedFillRecord)
        )
        view = await load_shadow_view(session, group_id=started.eligible.group_id)
    assert group is not None and group.status == "expired"
    assert seal is not None and seal.reason_code == "SHADOW_INITIAL_EXECUTION_INCOMPLETE"
    assert fill_count == 0
    assert (view.sealed, view.seal_reason, view.coverage.completed) == (
        True,
        "expired",
        63,
    )


@pytest.mark.asyncio
async def test_initial_untradable_execution_expires_after_complete_window(
    migrated_database_url: str,
) -> None:
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from nexus.persistence.models.optimization import OptimizationProposalGroupRecord

    started = await seed_started_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        first_decision = await session.scalar(
            select(ExchangeCalendarSessionRecord.decision_at).where(
                ExchangeCalendarSessionRecord.calendar_authority_id == started.eligible.calendar_id,
                ExchangeCalendarSessionRecord.session == started.start.start_session,
            )
        )
        assert first_decision is not None
        first_decision = first_decision.astimezone(UTC)
        events = tuple(
            [
                await seed_shadow_event(
                    session,
                    instrument_id=instrument_id,
                    source="fixture",
                    provider_event_id=f"delisted-{instrument_id}",
                    action_type="bankruptcy",
                    process_date=started.start.start_session,
                    available_at=first_decision - timedelta(days=1),
                    rate="0",
                )
                for instrument_id in started.eligible.instrument_ids
            ]
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=started.start.shadow_group_id,
            session_date=started.start.start_session,
            kind="price_bars",
            source="alpaca",
            delivery_mode="delayed",
            instrument_ids=started.eligible.instrument_ids,
            complete=True,
            negative_instrument_ids=started.eligible.instrument_ids,
            available_at=first_decision - timedelta(seconds=2),
        )
        await seed_shadow_query_proof(
            session,
            shadow_group_id=started.start.shadow_group_id,
            session_date=started.start.start_session,
            kind="corporate_actions",
            source="fixture",
            delivery_mode="unknown",
            instrument_ids=started.eligible.instrument_ids,
            complete=True,
            member_ids=tuple(item.event_id for item in events),
            available_at=first_decision - timedelta(seconds=2),
        )
    observed = await seed_complete_shadow_window(
        migrated_database_url,
        started=started,
        start_ordinal=2,
        through_ordinal=63,
        instrument_ids=(),
    )
    async with factory() as session, session.begin():
        first = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=first_decision,
        )
        later = tuple(
            [
                await advance_shadow_session(
                    session,
                    group_id=started.eligible.group_id,
                    session_date=started.eligible.calendar_sessions[index],
                    observed_at=observed[index - 1],
                )
                for index in range(1, 63)
            ]
        )
    assert first.result == "applied"
    assert later[-1].proposal_status == "expired"
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, started.eligible.group_id)
        outcomes = tuple(await session.scalars(select(ShadowSimulatedOutcomeRecord)))
    assert group is not None and group.status == "expired"
    assert len(outcomes) == 6
    assert {item.status for item in outcomes} == {"untradable"}


@pytest.mark.asyncio
async def test_grace_exhaustion_seals_incomplete_checkpoints(
    migrated_database_url: str,
) -> None:
    from nexus.persistence.models.optimization import OptimizationProposalGroupRecord

    started = await seed_started_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    observed_at = started.eligible.database_time.astimezone(UTC) + timedelta(days=365)
    async with factory() as session, session.begin():
        result = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.grace_end_session,
            observed_at=observed_at,
        )
    assert (result.result, result.proposal_status) == ("session_gap", "expired")
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, started.eligible.group_id)
        checkpoints = tuple(
            await session.scalars(
                select(ShadowCheckpointRecord).order_by(ShadowCheckpointRecord.ordinal)
            )
        )
        view = await load_shadow_view(session, group_id=started.eligible.group_id)
    assert group is not None and group.status == "expired"
    assert [(item.ordinal, item.status, item.coverage_count) for item in checkpoints] == [
        (5, "incomplete", 0),
        (21, "incomplete", 0),
        (63, "incomplete", 0),
    ]
    assert view.seal_reason == "expired"
    assert all(
        metric.metrics.net_cumulative_return.status == "unknown"
        for checkpoint in view.checkpoints
        for metric in checkpoint.metrics
    )


@pytest.mark.asyncio
async def test_grace_end_request_cannot_expire_shadow_before_deadline(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        result = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.grace_end_session,
            observed_at=started.eligible.database_time.astimezone(UTC),
        )

    assert (result.result, result.proposal_status) == ("session_gap", "shadowing")
    async with factory() as session:
        seal = await session.scalar(select(ShadowSealRecord))
    assert seal is None


@pytest.mark.asyncio
async def test_late_backfill_applies_only_the_original_missing_session(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    observed_first = await seed_complete_shadow_session_inputs(
        migrated_database_url, started=started, ordinal=1
    )
    second_session = started.eligible.calendar_sessions[1]
    async with factory() as session, session.begin():
        gap = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=second_session,
            observed_at=observed_first + timedelta(days=1),
        )
    async with factory() as session, session.begin():
        applied = await advance_shadow_session(
            session,
            group_id=started.eligible.group_id,
            session_date=started.start.start_session,
            observed_at=observed_first + timedelta(days=1),
        )
    assert (gap.result, applied.result) == ("session_gap", "applied")
    async with factory() as session:
        authority = await session.scalar(select(ShadowSessionAuthorityRecord))
    assert authority is not None
    assert authority.session == started.start.start_session
    assert authority.late_backfill is True


@pytest.mark.asyncio
async def test_two_workers_racing_one_session_converge_to_apply_and_replay(
    migrated_database_url: str,
) -> None:
    started = await seed_started_shadow(migrated_database_url)
    observed_at = await seed_complete_shadow_session_inputs(
        migrated_database_url, started=started, ordinal=1
    )
    factory = session_factory(settings(migrated_database_url))

    async def worker() -> str:
        async with factory() as session, session.begin():
            result = await advance_shadow_session(
                session,
                group_id=started.eligible.group_id,
                session_date=started.start.start_session,
                observed_at=observed_at,
            )
            return result.result

    results = await asyncio.gather(worker(), worker())
    assert sorted(results) == ["applied", "replayed"]
    async with factory() as session:
        counts = (
            await session.scalar(select(func.count()).select_from(ShadowSessionAuthorityRecord)),
            await session.scalar(select(func.count()).select_from(ShadowPortfolioStateRecord)),
            await session.scalar(select(func.count()).select_from(ShadowAdvanceAttemptRecord)),
        )
    assert counts == (1, 3, 1)


@pytest.mark.asyncio
async def test_one_variant_failure_rolls_back_the_whole_session(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nexus.shadow import service

    started = await seed_started_shadow(migrated_database_url)
    observed_at = await seed_complete_shadow_session_inputs(
        migrated_database_url, started=started, ordinal=1
    )
    factory = session_factory(settings(migrated_database_url))
    real_simulate = service.simulate_shadow_portfolio_session
    calls = 0

    def fail_second_variant(*args: object, **kwargs: object):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected variant failure")
        return real_simulate(*args, **kwargs)

    monkeypatch.setattr(service, "simulate_shadow_portfolio_session", fail_second_variant)
    with pytest.raises(RuntimeError, match="injected variant failure"):
        async with factory() as session, session.begin():
            await advance_shadow_session(
                session,
                group_id=started.eligible.group_id,
                session_date=started.start.start_session,
                observed_at=observed_at,
            )
    async with factory() as session:
        counts = (
            await session.scalar(select(func.count()).select_from(ShadowSessionAuthorityRecord)),
            await session.scalar(select(func.count()).select_from(ShadowPortfolioStateRecord)),
            await session.scalar(select(func.count()).select_from(ShadowAdvanceAttemptRecord)),
        )
    assert counts == (0, 0, 0)


@pytest.mark.asyncio
async def test_advance_and_reject_race_ends_with_one_rejected_seal(
    migrated_database_url: str,
) -> None:
    from nexus.optimizer.lifecycle import reject_group_idempotently
    from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
    from nexus.persistence.models.runtime import AuditEvent, OutboxEvent

    started = await seed_started_shadow(migrated_database_url)
    observed_at = await seed_complete_shadow_session_inputs(
        migrated_database_url, started=started, ordinal=1
    )
    factory = session_factory(settings(migrated_database_url))

    async def advance() -> str:
        async with factory() as session, session.begin():
            result = await advance_shadow_session(
                session,
                group_id=started.eligible.group_id,
                session_date=started.start.start_session,
                observed_at=observed_at,
            )
            return result.result

    async def reject() -> str:
        async with factory() as session, session.begin():
            result = await reject_group_idempotently(
                session,
                actor_id="shadow-race-user",
                idempotency_key="reject-race",
                request_hash="a" * 64,
                group_id=started.eligible.group_id,
                expected_version=2,
            )
            return result.status

    results = await asyncio.gather(advance(), reject())
    assert "rejected" in results
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, started.eligible.group_id)
        seal = await session.scalar(select(ShadowSealRecord))
        terminal_audits = await session.scalar(
            select(func.count())
            .select_from(AuditEvent)
            .where(AuditEvent.action == "shadow.rejected")
        )
        terminal_outbox = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(OutboxEvent.event_type == "shadow.rejected")
        )
        authority_count = await session.scalar(
            select(func.count()).select_from(ShadowSessionAuthorityRecord)
        )
    assert group is not None and group.status == "rejected"
    assert seal is not None and seal.reason == "rejected"
    assert (terminal_audits, terminal_outbox) == (1, 1)
    assert authority_count in (0, 1)


@pytest.mark.asyncio
async def test_finalize_and_reject_race_has_one_matching_terminal_fact(
    migrated_database_url: str,
) -> None:
    from nexus.optimizer.lifecycle import reject_group_idempotently
    from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
    from nexus.persistence.models.runtime import AuditEvent, OutboxEvent

    started = await seed_started_shadow(migrated_database_url)
    observed = await seed_complete_shadow_window(
        migrated_database_url, started=started, through_ordinal=63
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        for index in range(62):
            await advance_shadow_session(
                session,
                group_id=started.eligible.group_id,
                session_date=started.eligible.calendar_sessions[index],
                observed_at=observed[index],
            )

    async def finalize() -> str:
        async with factory() as session, session.begin():
            result = await advance_shadow_session(
                session,
                group_id=started.eligible.group_id,
                session_date=started.eligible.calendar_sessions[62],
                observed_at=observed[62],
            )
            return result.proposal_status

    async def reject() -> str:
        try:
            async with factory() as session, session.begin():
                result = await reject_group_idempotently(
                    session,
                    actor_id="shadow-final-race-user",
                    idempotency_key="reject-final-race",
                    request_hash="b" * 64,
                    group_id=started.eligible.group_id,
                    expected_version=2,
                )
                return result.status
        except ValueError as error:
            return str(error)

    await asyncio.gather(finalize(), reject())
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, started.eligible.group_id)
        seal = await session.scalar(select(ShadowSealRecord))
        terminal_audits = await session.scalar(
            select(func.count())
            .select_from(AuditEvent)
            .where(AuditEvent.action.in_(("shadow.validated", "shadow.rejected")))
        )
        terminal_outbox = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(OutboxEvent.event_type.in_(("shadow.validated", "shadow.rejected")))
        )
        coverage = await session.scalar(
            select(func.count()).select_from(ShadowSessionAuthorityRecord)
        )
    assert group is not None and group.status in {"validated", "rejected"}
    assert seal is not None and seal.reason == group.status
    assert (terminal_audits, terminal_outbox) == (1, 1)
    assert coverage == (63 if group.status == "validated" else 62)
