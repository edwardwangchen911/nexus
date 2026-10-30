"""Synthetic native records only; no business database, provider or Worker process."""

import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from nexus.agents.comparison_manifest import ACTOR, ComparisonManifest
from nexus.agents.comparison_registration import register_comparison, reserve_comparison_chain
from nexus.agents.persistence import BudgetExhausted, reserve_daily_budget
from nexus.agents.research_only import require_formal_research
from nexus.jobs.models import job_input_hash
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from test_agent_persistence_pg import _RUN_IDS, _seed_authority, _seed_runs


@pytest.mark.asyncio
@pytest.mark.parametrize("competition", [False, True])
async def test_comparison_registry_shares_native_budget_and_preserves_unknown(
    migrated_database_url,
    competition,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _seed_runs(factory, _RUN_IDS[:3])
    experiment_id, run_id, job_id = uuid4(), uuid4(), uuid4()
    async with factory() as session, session.begin():
        now = await session.scalar(text("SELECT clock_timestamp()"))
        original = await session.get(AgentResearchRunRecord, _RUN_IDS[0])
        model = AgentModelConfigRecord(
            id=uuid4(),
            model_id="gpt-5.6-terra",
            version="fixture-comparison-api",
            config_hash="a" * 64,
            max_input_tokens=96000,
            max_output_tokens=8192,
            max_tool_calls=0,
            input_usd_per_million=Decimal("12.5"),
            output_usd_per_million=Decimal(0),
            pricing_available_at=now - timedelta(days=1),
            pricing_expires_at=now + timedelta(days=1),
        )
        session.add(model)
        values = {
            column.key: getattr(original, column.key)
            for column in AgentResearchRunRecord.__table__.columns
        }
        values.update(
            id=run_id,
            actor_id=ACTOR,
            idempotency_key=f"comparison:{experiment_id}:2",
            model_config_id=model.id,
            model_hash=model.config_hash,
            pricing_version=model.version,
            reserved_cost_usd=Decimal("1.2"),
            created_at=now,
        )
        session.add(AgentResearchRunRecord(**values))
        payload = {"run_id": str(run_id)}
        session.add(
            Job(
                id=job_id,
                job_key=f"comparison:{experiment_id}:2",
                input_hash=job_input_hash(payload),
                status="queued",
                schema_version="comparison-v1",
                payload=payload,
                job_type="agent.research.comparison",
                queue="strategy_rnd",
                scope_kind="instrument",
                scope_id=str(original.instrument_id),
                effective_at=original.decision_time,
                priority=0,
                retry_policy="never",
                max_attempts=1,
                attempt_count=0,
                manual_retry_count=0,
                available_at=now,
            )
        )
        await session.flush()
        manifest = ComparisonManifest(
            experiment_id=experiment_id,
            sdk_config_hash="cdf5e7d3b0cfd2a3e6fda267f74bc9b3d135f28e33f965431f17175b9f2a8720",
            api_config_hash=model.config_hash,
            implementation_sha="b" * 40,
            source_manifest_hash="c" * 64,
            identity_equivalence_hash="d" * 64,
            outbound_safety_hash="e" * 64,
            scoring_hash="f" * 64,
            blinding_hash="1" * 64,
            input_hashes=("2" * 64, "3" * 64, "4" * 64),
            instrument_ids=(original.instrument_id, uuid4(), uuid4()),
        )
        await register_comparison(session, manifest)
        old = await reserve_daily_budget(
            session,
            run_id=_RUN_IDS[0],
            now=now,
            daily_limit_usd=Decimal(4),
            worst_case_usd=Decimal(2),
            pricing_version="fixture-pricing-v1",
            pricing_fresh=True,
        )
        old_id = old.id
    # A new process can read the same manifest; changed registration never resets it.
    async with factory() as session, session.begin():
        await register_comparison(session, manifest)
    with pytest.raises(ValueError, match="COMPARISON_REGISTRATION_CONFLICT"):
        async with factory() as session, session.begin():
            await register_comparison(
                session, manifest.model_copy(update={"scoring_hash": "0" * 64})
            )
    with pytest.raises(ValueError, match="COMPARISON_BUSINESS_CONSUMPTION_FORBIDDEN"):
        async with factory() as session:
            await require_formal_research(
                session, await session.get(AgentResearchRunRecord, run_id)
            )
    with pytest.raises(BudgetExhausted):
        async with factory() as session, session.begin():
            await reserve_comparison_chain(
                session,
                experiment_id=experiment_id,
                ordinal=2,
                run_id=run_id,
                job_id=job_id,
                daily_limit_usd=Decimal(3),
            )
    barrier = asyncio.Barrier(2) if competition else None

    async def experiment():
        async with factory() as session, session.begin():
            if barrier:
                await barrier.wait()
            return await reserve_comparison_chain(
                session,
                experiment_id=experiment_id,
                ordinal=2,
                run_id=run_id,
                job_id=job_id,
                daily_limit_usd=Decimal(4),
            )

    async def business():
        async with factory() as session, session.begin():
            await barrier.wait()
            current = await session.scalar(text("SELECT clock_timestamp()"))
            return await reserve_daily_budget(
                session,
                run_id=_RUN_IDS[1],
                now=current,
                daily_limit_usd=Decimal(4),
                worst_case_usd=Decimal("1.2"),
                pricing_version="fixture-pricing-v1",
                pricing_fresh=True,
            )

    if competition:
        results = await asyncio.gather(experiment(), business(), return_exceptions=True)
        assert sum(isinstance(item, BudgetExhausted) for item in results) == 1
        assert sum(not isinstance(item, BaseException) for item in results) == 1
    else:
        await experiment()
        with pytest.raises(ValueError, match="COMPARISON_POSITION_CONSUMED"):
            await experiment()
    async with factory() as session:
        budgets = list(await session.scalars(select(AgentDailyBudgetReservationRecord)))
        assert sum(item.reserved_usd for item in budgets) == Decimal("3.2")
        old = await session.get(AgentDailyBudgetReservationRecord, old_id)
        assert (old.state, old.reserved_usd, old.settled_usd) == ("reserved", Decimal(2), None)
        assert all(item.business_date == now.date() for item in budgets)
        assert original.decision_time.date() != now.date()
    # The future stage extension must supply a precise claim fence. Until then,
    # even a direct claim cannot turn this offline preparation into execution.
    with pytest.raises(DBAPIError, match="COMPARISON_DISPATCH_NOT_ENABLED"):
        async with factory() as session, session.begin():
            await session.execute(
                text("""UPDATE jobs SET status='running',
              started_at=clock_timestamp(),attempt_count=1 WHERE id=:id"""),
                {"id": job_id},
            )
