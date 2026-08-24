"""Real-PostgreSQL admission, budget, and evidence-authority tests for Agents."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.budget import BudgetExhausted
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import (
    admit_research_run,
    release_daily_budget,
    reserve_daily_budget,
    settle_daily_budget,
)
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.persistence.session import session_factory
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

_NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000001501")
_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000001502")
_PROMPT_ID = UUID("0198d500-1000-7000-8000-000000001503")
_MODEL_ID = UUID("0198d500-1000-7000-8000-000000001504")
_RUN_IDS = tuple(UUID(f"0198d500-1000-7000-8000-0000000015{index:02d}") for index in range(10, 20))


@pytest.mark.asyncio
async def test_concurrent_budget_counts_settled_usage_and_only_release_restores_capacity(
    migrated_database_url: str,
) -> None:
    """Two sessions cannot oversubscribe; settled actual cost remains in the daily total."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _seed_runs(factory, _RUN_IDS[:4])
    barrier = asyncio.Barrier(2)

    async def reserve(run_id: UUID, amount: str):
        async with factory() as session, session.begin():
            await barrier.wait()
            return await reserve_daily_budget(
                session,
                run_id=run_id,
                now=_NOW,
                daily_limit_usd=Decimal("3.00"),
                worst_case_usd=Decimal(amount),
                pricing_version="fixture-pricing-v1",
                pricing_fresh=True,
            )

    results = await asyncio.wait_for(
        asyncio.gather(
            reserve(_RUN_IDS[0], "2.00"),
            reserve(_RUN_IDS[1], "2.00"),
            return_exceptions=True,
        ),
        timeout=5,
    )
    winner = next(result for result in results if not isinstance(result, BaseException))
    assert sum(isinstance(result, BudgetExhausted) for result in results) == 1
    async with factory() as session, session.begin():
        await settle_daily_budget(
            session,
            run_id=winner.run_id,
            actual_usd=Decimal("1.50"),
            input_tokens=120,
            output_tokens=80,
        )
    async with factory() as session, session.begin():
        with pytest.raises(BudgetExhausted):
            await reserve_daily_budget(
                session,
                run_id=_RUN_IDS[2],
                now=_NOW,
                daily_limit_usd=Decimal("3.00"),
                worst_case_usd=Decimal("1.51"),
                pricing_version="fixture-pricing-v1",
                pricing_fresh=True,
            )
    async with factory() as session, session.begin():
        reservation = await reserve_daily_budget(
            session,
            run_id=_RUN_IDS[2],
            now=_NOW,
            daily_limit_usd=Decimal("3.00"),
            worst_case_usd=Decimal("1.50"),
            pricing_version="fixture-pricing-v1",
            pricing_fresh=True,
        )
        await release_daily_budget(session, run_id=reservation.run_id)
    async with factory() as session:
        rows = list(
            await session.scalars(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_((_RUN_IDS[0], _RUN_IDS[1], _RUN_IDS[2]))
                )
            )
        )
        settled = next(row for row in rows if row.id == winner.run_id)
        released = await session.scalar(
            select(func.count())
            .select_from(text("agent_daily_budget_reservations"))
            .where(text("state = 'released'"))
        )
    assert (settled.settled_cost_usd, settled.input_tokens, settled.output_tokens) == (
        Decimal("1.500000000000"),
        120,
        80,
    )
    assert released == 1


@pytest.mark.asyncio
async def test_atomic_admission_replays_same_body_and_conflicts_changed_body(
    migrated_database_url: str,
) -> None:
    """The idempotency claim, prepared run, and reservation are one transaction."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    request = ResearchRequest(
        instrument_id=_INSTRUMENT_ID,
        data_snapshot_id=_SNAPSHOT_ID,
        decision_time=_NOW,
    )
    barrier = asyncio.Barrier(2)

    async def admit(request_hash: str):
        async with factory() as session, session.begin():
            return await admit_research_run(
                session,
                actor_id="fixture-actor",
                route="/internal/v1/research-runs",
                idempotency_key="same-key",
                request_hash=request_hash,
                request=request,
                prompt_version_id=_PROMPT_ID,
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal("3.00"),
                now=_NOW,
                after_authority=barrier.wait,
            )

    same = await asyncio.wait_for(asyncio.gather(admit("a" * 64), admit("a" * 64)), timeout=5)
    assert same[0].run_id == same[1].run_id
    assert {same[0].replayed, same[1].replayed} == {False, True}
    with pytest.raises(IdempotencyConflict):
        async with factory() as session, session.begin():
            await admit_research_run(
                session,
                actor_id="fixture-actor",
                route="/internal/v1/research-runs",
                idempotency_key="same-key",
                request_hash="b" * 64,
                request=request,
                prompt_version_id=_PROMPT_ID,
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal("3.00"),
                now=_NOW,
            )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 1
        reservations = await session.scalar(
            select(func.count()).select_from(text("agent_daily_budget_reservations"))
        )
    assert reservations == 1


@pytest.mark.asyncio
async def test_admission_rejects_future_snapshot_and_rolls_back_claim_and_budget(
    migrated_database_url: str,
) -> None:
    """Point-in-time authority failure leaves no idempotency, run, or reservation artifact."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory, snapshot_available_at=_NOW + timedelta(seconds=1))
    with pytest.raises(ValueError, match="point-in-time data authority"):
        async with factory() as session, session.begin():
            await admit_research_run(
                session,
                actor_id="fixture-actor",
                route="/internal/v1/research-runs",
                idempotency_key="future-data",
                request_hash="c" * 64,
                request=ResearchRequest(
                    instrument_id=_INSTRUMENT_ID,
                    data_snapshot_id=_SNAPSHOT_ID,
                    decision_time=_NOW,
                ),
                prompt_version_id=_PROMPT_ID,
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal("3.00"),
                now=_NOW,
            )
    async with factory() as session:
        counts = (
            await session.scalar(select(func.count()).select_from(IdempotencyRecord)),
            await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)),
            await session.scalar(
                select(func.count()).select_from(text("agent_daily_budget_reservations"))
            ),
        )
    assert counts == (0, 0, 0)


@pytest.mark.asyncio
async def test_output_evidence_requires_exact_task14_owner_and_point_in_time(
    migrated_database_url: str,
) -> None:
    """Raw SQL cannot attach future or hash/location-mismatched evidence to Agent output."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _seed_runs(factory, (_RUN_IDS[0],))
    output_id = UUID("0198d500-1000-7000-8000-000000001550")
    evidence_id = UUID("0198d500-1000-7000-8000-000000001551")
    async with factory() as session, session.begin():
        session.add_all(
            (
                AgentResearchOutputRecord(
                    id=output_id,
                    run_id=_RUN_IDS[0],
                    sequence=1,
                    status="validated",
                    content_hash="d" * 64,
                    intent=None,
                ),
                AgentResearchOutputRecord(
                    id=UUID("0198d500-1000-7000-8000-000000001552"),
                    run_id=_RUN_IDS[0],
                    sequence=2,
                    status="audited",
                    content_hash="c" * 64,
                    intent=None,
                ),
                ResearchEvidenceRef(
                    id=evidence_id,
                    source="fixture",
                    provider_object_id="evidence-1",
                    revision="v1",
                    available_at=_NOW - timedelta(minutes=1),
                    location=f"memory://sha256/{'e' * 64}",
                    content_hash="e" * 64,
                    byte_length=42,
                ),
            )
        )
    async with factory() as session, session.begin():
        session.add(
            AgentResearchOutputEvidenceRecord(
                output_id=output_id,
                evidence_ref_id=evidence_id,
                source="fixture",
                provider_object_id="evidence-1",
                revision="v1",
                available_at=_NOW - timedelta(minutes=1),
                location=f"memory://sha256/{'e' * 64}",
                content_hash="e" * 64,
            )
        )
    with pytest.raises(IntegrityError):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO agent_research_output_evidence "
                    "(output_id,evidence_ref_id,source,provider_object_id,revision,"
                    "available_at,location,content_hash) VALUES "
                    "(:output,:evidence,'fixture','evidence-1','v1',"
                    ":available,:location,:hash)"
                ),
                {
                    "output": UUID("0198d500-1000-7000-8000-000000001552"),
                    "evidence": evidence_id,
                    "available": _NOW - timedelta(minutes=1),
                    "location": f"memory://sha256/{'f' * 64}",
                    "hash": "f" * 64,
                },
            )
    future_id = UUID("0198d500-1000-7000-8000-000000001553")
    async with factory() as session, session.begin():
        session.add(
            ResearchEvidenceRef(
                id=future_id,
                source="fixture",
                provider_object_id="future-evidence",
                revision="v1",
                available_at=_NOW + timedelta(seconds=1),
                location=f"memory://sha256/{'f' * 64}",
                content_hash="f" * 64,
                byte_length=42,
            )
        )
    with pytest.raises(IntegrityError, match="future evidence"):
        async with factory() as session, session.begin():
            session.add(
                AgentResearchOutputEvidenceRecord(
                    output_id=output_id,
                    evidence_ref_id=future_id,
                    source="fixture",
                    provider_object_id="future-evidence",
                    revision="v1",
                    available_at=_NOW + timedelta(seconds=1),
                    location=f"memory://sha256/{'f' * 64}",
                    content_hash="f" * 64,
                )
            )


async def _seed_authority(factory, *, snapshot_available_at: datetime = _NOW) -> None:
    async with factory() as session, session.begin():
        session.add_all(
            (
                Instrument(
                    id=_INSTRUMENT_ID,
                    asset_type="equity",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="equity",
                ),
                DataSnapshot(
                    id=_SNAPSHOT_ID,
                    source="fixture",
                    provider_object_id="agent-snapshot",
                    revision="v1",
                    observed_at=_NOW,
                    available_at=snapshot_available_at,
                    received_at=snapshot_available_at,
                    schema_version="1",
                    payload={},
                ),
                AgentPromptVersionRecord(
                    id=_PROMPT_ID,
                    role="orchestrator",
                    version="fixture-v1",
                    content_hash="1" * 64,
                    content="fixture prompt",
                ),
                AgentModelConfigRecord(
                    id=_MODEL_ID,
                    model_id="fixture-model",
                    version="fixture-pricing-v1",
                    config_hash="2" * 64,
                    max_input_tokens=1_000_000,
                    max_tool_calls=8,
                    max_output_tokens=10_000,
                    input_usd_per_million=Decimal("1.00"),
                    output_usd_per_million=Decimal("2.00"),
                    pricing_available_at=_NOW - timedelta(days=1),
                    pricing_expires_at=_NOW + timedelta(days=1),
                ),
            )
        )


async def _seed_runs(factory, run_ids: tuple[UUID, ...]) -> None:
    async with factory() as session, session.begin():
        session.add_all(
            AgentResearchRunRecord(
                id=run_id,
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                prompt_version_id=_PROMPT_ID,
                model_config_id=_MODEL_ID,
                decision_time=_NOW,
                actor_id="fixture-actor",
                idempotency_key=f"seed-{run_id}",
                status="prepared",
                retry_count=0,
                schema_hash=hashlib.sha256(b"schema").hexdigest(),
                model_hash="2" * 64,
                prompt_hash="1" * 64,
                tool_hash=hashlib.sha256(b"tools").hexdigest(),
                input_hash=hashlib.sha256(str(run_id).encode()).hexdigest(),
                reserved_cost_usd=Decimal("2.00"),
                settled_cost_usd=None,
                pricing_version="fixture-pricing-v1",
                input_tokens=None,
                output_tokens=None,
            )
            for run_id in run_ids
        )
