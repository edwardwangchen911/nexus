"""PostgreSQL contract tests for atomic Proposal Shadow startup."""

import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

import exchange_calendars
import nexus.shadow.start as shadow_start_module
import pytest
from helpers.portfolio_api import settings
from helpers.shadow import seed_shadow_eligible_group
from nexus.optimizer.lifecycle import reject_group_idempotently
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, OutboxEvent
from nexus.persistence.models.shadow import (
    ShadowGroupRecord,
    ShadowPortfolioRecord,
    ShadowSealRecord,
)
from nexus.persistence.session import session_factory
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.shadow.canonical import (
    proposal_bundle_content_hash,
    target_weights_content_hash,
)
from nexus.shadow.models import ProposalVariantHashInput
from nexus.shadow.start import start_proposal_shadow
from nexus.simulation.models import TargetWeight
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

_REQUEST_HASH = "1" * 64
_NEW_YORK = ZoneInfo("America/New_York")


@pytest.mark.asyncio
async def test_start_creates_exact_three_normalized_portfolios_and_audit(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        result = await start_proposal_shadow(
            session,
            actor_id="shadow-user",
            idempotency_key="start-primary",
            request_hash=_REQUEST_HASH,
            group_id=seed.group_id,
            expected_version=1,
        )

    assert result.status == "shadowing"
    assert result.version == 2
    assert result.start_session == seed.calendar_sessions[0]
    assert result.evaluation_end_session == seed.calendar_sessions[62]
    assert result.grace_end_session == seed.calendar_sessions[67]

    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, seed.group_id)
        shadow = await session.get(ShadowGroupRecord, result.shadow_group_id)
        portfolios = tuple(
            await session.scalars(
                select(ShadowPortfolioRecord).where(
                    ShadowPortfolioRecord.shadow_group_id == result.shadow_group_id
                )
            )
        )
        audit_count = await session.scalar(
            select(func.count()).select_from(AuditEvent).where(
                AuditEvent.action == "shadow.started"
            )
        )
        outbox_count = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(
                OutboxEvent.event_type == "shadow.started"
            )
        )
    assert group is not None and group.status == "shadowing" and group.version == 2
    assert shadow is not None
    assert shadow.initial_nav == Decimal("100.000000000000")
    assert shadow.calendar_authority_id == seed.calendar_id
    assert {portfolio.variant for portfolio in portfolios} == {
        "hold_current",
        "minimum_change",
        "strategy_target",
    }
    assert all(
        portfolio.initial_nav == Decimal("100.000000000000")
        and portfolio.initial_cash == Decimal("100.000000000000")
        for portfolio in portfolios
    )
    expected_weights = {
        "hold_current": (Decimal("0.20"), Decimal("0.10")),
        "minimum_change": (Decimal("0.15"), Decimal("0.15")),
        "strategy_target": (Decimal("0.125"), Decimal("0.125")),
    }
    proposal_hash_inputs: list[ProposalVariantHashInput] = []
    for portfolio in portfolios:
        weights = expected_weights[portfolio.variant]
        targets = tuple(
            TargetWeight(instrument_id=instrument_id, weight=weight)
            for instrument_id, weight in zip(
                seed.instrument_ids, weights, strict=True
            )
        )
        cash_weight = Decimal(1) - sum(weights, start=Decimal(0))
        assert portfolio.target_weights_hash == target_weights_content_hash(
            targets, cash_weight
        )
        proposal_hash_inputs.append(
            ProposalVariantHashInput(
                proposal_id=portfolio.proposal_id,
                variant=portfolio.variant,  # type: ignore[arg-type]
                targets=targets,
                cash_weight=cash_weight,
            )
        )
    assert shadow.proposal_bundle_hash == proposal_bundle_content_hash(
        tuple(proposal_hash_inputs)  # type: ignore[arg-type]
    )
    assert audit_count == 1
    assert outbox_count == 1


@pytest.mark.asyncio
async def test_start_uses_database_market_date_and_frozen_xnys_holidays(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        result = await start_proposal_shadow(
            session,
            actor_id="shadow-calendar",
            idempotency_key="official-calendar",
            request_hash=_REQUEST_HASH,
            group_id=seed.group_id,
            expected_version=1,
        )

    market_date = seed.database_time.astimezone(_NEW_YORK).date()
    official = exchange_calendars.get_calendar("XNYS")
    expected = tuple(
        value.date()
        for value in official.sessions_in_range(
            market_date + timedelta(days=1),
            market_date + timedelta(days=180),
        )[:68]
    )
    assert seed.calendar_sessions == expected
    assert result.start_session == expected[0] > market_date
    assert all(session.weekday() < 5 for session in expected)
    assert any((later - earlier).days > 1 for earlier, later in zip(expected, expected[1:]))


@pytest.mark.asyncio
async def test_start_rejects_calendar_whose_member_hash_does_not_match(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(
        migrated_database_url,
        calendar_content_hash_override="c" * 64,
    )
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(ValueError, match="SHADOW_CALENDAR_INCOMPLETE"):
        async with factory() as session, session.begin():
            await start_proposal_shadow(
                session,
                actor_id="shadow-calendar",
                idempotency_key="tampered-calendar",
                request_hash=_REQUEST_HASH,
                group_id=seed.group_id,
                expected_version=1,
            )
    await _assert_start_rolled_back(migrated_database_url, seed.group_id)


@pytest.mark.asyncio
async def test_start_replays_same_key_and_rejects_body_mismatch(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))

    async with factory() as session, session.begin():
        first = await start_proposal_shadow(
            session,
            actor_id="shadow-user",
            idempotency_key="replay-key",
            request_hash=_REQUEST_HASH,
            group_id=seed.group_id,
            expected_version=1,
        )
    async with factory() as session, session.begin():
        replay = await start_proposal_shadow(
            session,
            actor_id="shadow-user",
            idempotency_key="replay-key",
            request_hash=_REQUEST_HASH,
            group_id=seed.group_id,
            expected_version=1,
        )
    assert replay == first

    with pytest.raises(IdempotencyConflict):
        async with factory() as session, session.begin():
            await start_proposal_shadow(
                session,
                actor_id="shadow-user",
                idempotency_key="replay-key",
                request_hash="2" * 64,
                group_id=seed.group_id,
                expected_version=1,
            )

    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count()).select_from(ShadowGroupRecord)
            )
            == 1
        )
        assert (
            await session.scalar(
                select(func.count()).select_from(IdempotencyRecord)
            )
            == 1
        )


@pytest.mark.asyncio
async def test_same_textual_key_is_scoped_by_actor_not_globally(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await start_proposal_shadow(
            session,
            actor_id="actor-a",
            idempotency_key="shared-text",
            request_hash=_REQUEST_HASH,
            group_id=seed.group_id,
            expected_version=1,
        )
    with pytest.raises(ValueError, match="VERSION_CONFLICT"):
        async with factory() as session, session.begin():
            await start_proposal_shadow(
                session,
                actor_id="actor-b",
                idempotency_key="shared-text",
                request_hash=_REQUEST_HASH,
                group_id=seed.group_id,
                expected_version=1,
            )
    async with factory() as session:
        claims = tuple(await session.scalars(select(IdempotencyRecord)))
    assert [(claim.actor_id, claim.idempotency_key) for claim in claims] == [
        ("actor-a", "shared-text")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "eligibility_override",
    (
        {"authority_stale": True},
        {"partial": True},
        {"source_conflict": True},
        {"cost_complete": False},
        {"requires_fx": True},
        {"strategy_evidence_complete": False},
        {"comparison_non_executable": True},
    ),
)
async def test_ineligible_authority_rolls_back_claim_and_all_shadow_facts(
    migrated_database_url: str,
    eligibility_override: dict[str, bool],
) -> None:
    seed = await seed_shadow_eligible_group(
        migrated_database_url, **eligibility_override  # type: ignore[arg-type]
    )
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(ValueError, match="PROPOSAL_NOT_EXECUTABLE"):
        async with factory() as session, session.begin():
            await start_proposal_shadow(
                session,
                actor_id="shadow-ineligible",
                idempotency_key="ineligible",
                request_hash=_REQUEST_HASH,
                group_id=seed.group_id,
                expected_version=1,
            )
    await _assert_start_rolled_back(migrated_database_url, seed.group_id)


@pytest.mark.asyncio
async def test_start_rolls_back_claim_and_facts_when_calendar_is_incomplete(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(
        migrated_database_url, calendar_session_count=67
    )
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(ValueError, match="SHADOW_CALENDAR_INCOMPLETE"):
        async with factory() as session, session.begin():
            await start_proposal_shadow(
                session,
                actor_id="shadow-user",
                idempotency_key="incomplete-calendar",
                request_hash=_REQUEST_HASH,
                group_id=seed.group_id,
                expected_version=1,
            )

    await _assert_start_rolled_back(migrated_database_url, seed.group_id)


@pytest.mark.asyncio
async def test_start_rolls_back_after_partial_shadow_graph_flush_failure(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    shadow_id = UUID("019d0000-0000-7000-8000-000000000001")
    duplicate_portfolio_id = UUID("019d0000-0000-7000-8000-000000000002")
    generated_ids = iter(
        (shadow_id, duplicate_portfolio_id, duplicate_portfolio_id, duplicate_portfolio_id)
    )
    monkeypatch.setattr(shadow_start_module, "new_uuid7", lambda: next(generated_ids))

    with pytest.raises(IntegrityError):
        async with factory() as session, session.begin():
            await start_proposal_shadow(
                session,
                actor_id="shadow-rollback",
                idempotency_key="duplicate-child",
                request_hash=_REQUEST_HASH,
                group_id=seed.group_id,
                expected_version=1,
            )
    await _assert_start_rolled_back(migrated_database_url, seed.group_id)


@pytest.mark.asyncio
async def test_start_rolls_back_after_lifecycle_cas_when_outbox_flush_fails(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    outbox_type = shadow_start_module.OutboxEvent

    def invalid_outbox(**values: object) -> OutboxEvent:
        values["event_type"] = "x" * 256
        return outbox_type(**values)  # type: ignore[arg-type]

    monkeypatch.setattr(shadow_start_module, "OutboxEvent", invalid_outbox)
    with pytest.raises(DBAPIError):
        async with factory() as session, session.begin():
            await start_proposal_shadow(
                session,
                actor_id="shadow-rollback",
                idempotency_key="bad-outbox",
                request_hash=_REQUEST_HASH,
                group_id=seed.group_id,
                expected_version=1,
            )
    await _assert_start_rolled_back(migrated_database_url, seed.group_id)


@pytest.mark.asyncio
async def test_reject_seals_existing_shadow_and_replays_without_duplicate_events(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        started = await start_proposal_shadow(
            session,
            actor_id="shadow-user",
            idempotency_key="start-before-reject",
            request_hash=_REQUEST_HASH,
            group_id=seed.group_id,
            expected_version=1,
        )
    async with factory() as session, session.begin():
        rejected = await reject_group_idempotently(
            session,
            actor_id="shadow-user",
            idempotency_key="reject-shadow",
            request_hash="3" * 64,
            group_id=seed.group_id,
            expected_version=2,
        )
    async with factory() as session, session.begin():
        replay = await reject_group_idempotently(
            session,
            actor_id="shadow-user",
            idempotency_key="reject-shadow",
            request_hash="3" * 64,
            group_id=seed.group_id,
            expected_version=2,
        )

    assert rejected.status == "rejected" and rejected.version == 3
    assert replay.group_id == rejected.group_id
    assert replay.status == rejected.status
    assert replay.version == rejected.version
    assert replay.replayed is True
    async with factory() as session:
        seal = await session.scalar(
            select(ShadowSealRecord).where(
                ShadowSealRecord.shadow_group_id == started.shadow_group_id
            )
        )
        rejected_audits = await session.scalar(
            select(func.count()).select_from(AuditEvent).where(
                AuditEvent.action == "shadow.rejected"
            )
        )
        rejected_outbox = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(
                OutboxEvent.event_type == "shadow.rejected"
            )
        )
    assert seal is not None and seal.reason == "rejected"
    assert seal.final_coverage == 0
    assert rejected_audits == 1
    assert rejected_outbox == 1

    async with factory() as session, session.begin():
        start_replay = await start_proposal_shadow(
            session,
            actor_id="shadow-user",
            idempotency_key="start-before-reject",
            request_hash=_REQUEST_HASH,
            group_id=seed.group_id,
            expected_version=1,
        )
    assert start_replay == started


@pytest.mark.asyncio
async def test_reject_compared_group_does_not_create_shadow_or_seal(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        result = await reject_group_idempotently(
            session,
            actor_id="shadow-user",
            idempotency_key="reject-compared",
            request_hash="4" * 64,
            group_id=seed.group_id,
            expected_version=1,
        )
    assert result.status == "rejected" and result.version == 2
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(ShadowGroupRecord))
            == 0
        )
        assert (
            await session.scalar(select(func.count()).select_from(ShadowSealRecord))
            == 0
        )


@pytest.mark.asyncio
async def test_reject_draft_group_does_not_create_shadow_or_seal(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url, status="draft")
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        result = await reject_group_idempotently(
            session,
            actor_id="shadow-user",
            idempotency_key="reject-draft",
            request_hash="7" * 64,
            group_id=seed.group_id,
            expected_version=1,
        )
    assert result.status == "rejected" and result.version == 2
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(ShadowGroupRecord))
            == 0
        )
        assert (
            await session.scalar(select(func.count()).select_from(ShadowSealRecord))
            == 0
        )


@pytest.mark.asyncio
async def test_direct_shadow_status_write_cannot_bypass_complete_graph(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(IntegrityError, match="PROPOSAL_SHADOW_INCOMPLETE"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE optimization_proposal_groups "
                    "SET status='shadowing', version=2 WHERE id=:id"
                ),
                {"id": seed.group_id},
            )
    await _assert_start_rolled_back(migrated_database_url, seed.group_id)


@pytest.mark.asyncio
async def test_two_start_keys_converge_to_one_complete_shadow(
    migrated_database_url: str,
) -> None:
    seed = await seed_shadow_eligible_group(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    barrier = asyncio.Barrier(2)

    async def start(key: str, digest: str) -> object:
        async with factory() as session, session.begin():
            await barrier.wait()
            return await start_proposal_shadow(
                session,
                actor_id="shadow-race",
                idempotency_key=key,
                request_hash=digest * 64,
                group_id=seed.group_id,
                expected_version=1,
            )

    results = await asyncio.wait_for(
        asyncio.gather(
            start("start-a", "5"),
            start("start-b", "6"),
            return_exceptions=True,
        ),
        timeout=5,
    )
    assert sum(not isinstance(result, Exception) for result in results) == 1
    assert sum(
        isinstance(result, ValueError) and str(result) == "VERSION_CONFLICT"
        for result in results
    ) == 1
    async with factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(ShadowGroupRecord))
            == 1
        )
        assert (
            await session.scalar(
                select(func.count()).select_from(ShadowPortfolioRecord)
            )
            == 3
        )
        assert (
            await session.scalar(
                select(func.count()).select_from(IdempotencyRecord)
            )
            == 1
        )


async def _assert_start_rolled_back(
    database_url: str,
    group_id: UUID,
) -> None:
    factory = session_factory(settings(database_url))
    async with factory() as session:
        group = await session.get(OptimizationProposalGroupRecord, group_id)
        shadow_count = await session.scalar(
            select(func.count()).select_from(ShadowGroupRecord)
        )
        portfolio_count = await session.scalar(
            select(func.count()).select_from(ShadowPortfolioRecord)
        )
        claim_count = await session.scalar(
            select(func.count()).select_from(IdempotencyRecord)
        )
    assert group is not None
    assert group.status in {"draft", "compared"} and group.version == 1
    assert shadow_count == 0
    assert portfolio_count == 0
    assert claim_count == 0
