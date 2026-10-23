"""Workbench reuses saved frozen authority without hydrating research inputs."""

from datetime import UTC, datetime, timedelta

import pytest
from nexus.agents.observation_readback import ObservationReadback
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.workbench.research_summary import _project, _validated_intent
from sqlalchemy import select, text

from engine.tests.integration.test_observation_readback_pg import V11, digest, seed_observation


@pytest.mark.parametrize(
    "defect",
    [
        None,
        "hash",
        "binding",
        "nested",
        "frozen_hash",
        "model_version",
        "prompt_version",
        "future_output",
        "identity",
        "audit",
    ],
)
@pytest.mark.asyncio
async def test_saved_summary_requires_frozen_output_authority(
    migrated_database_url, defect, monkeypatch
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, _ = await seed_observation(factory, V11, defect)
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        reader = ObservationReadback(session, read_at=datetime.now(UTC))
        await reader.prime([run])
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run_id)
        )
        intent = await _validated_intent(session, run, (output,), reader, read_at=datetime.now(UTC))
        assert (intent is not None) is (defect is None)


@pytest.mark.asyncio
async def test_partial_completed_preserves_runtime_and_expired_readback(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, _ = await seed_observation(factory, V11)
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        job = Job(
            payload={"run_id": str(run_id)},
            scope_id=str(run.instrument_id),
            job_type="agent.paper.research",
            effective_at=run.decision_time,
            created_at=run.created_at,
            started_at=run.created_at,
            finished_at=run.created_at,
            status="succeeded",
        )
        at = datetime.now(UTC) + timedelta(days=5)
        reader = ObservationReadback(session, read_at=at)
        await reader.prime([run])
        result = await _project(
            session,
            instrument_id=run.instrument_id,
            symbol="AAPL",
            run=run,
            jobs=(job,),
            reader=reader,
            read_at=at,
        )
        assert result.run_status == "completed"
        assert result.quality == "partial"
        assert result.read_status == "verified"
        assert result.href == f"/research/{run.instrument_id}?run_id={run_id}"
        assert result.limitations
        assert all(item.source == "model" for item in result.limitations)
        duplicate_job = job
        damaged = await _project(
            session,
            instrument_id=run.instrument_id,
            symbol="AAPL",
            run=run,
            jobs=(job, duplicate_job),
            reader=reader,
            read_at=at,
        )
        assert damaged.run_status == "completed"
        assert damaged.quality is None
        assert damaged.limitations == ()
        assert damaged.reason_code == "RESEARCH_JOB_BINDING_INVALID"


@pytest.mark.asyncio
async def test_valid_saved_review_excerpt_never_becomes_verified_system_gap(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, _ = await seed_observation(factory, V11)
    async with factory() as session, session.begin():
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run_id)
        )
        doc = dict(output.intent)
        review = doc["synthesis_assessment"]["reviews"][0]
        review.update(
            status="unresolved",
            resolved=False,
            missing_evidence=["synthetic missing EPS scope"],
            prevented_conclusion="synthetic denominator limited",
        )
        doc["synthesis_assessment"]["review_conclusion"] = "limited"
        content = {
            "status": output.status,
            "role": output.role,
            "payload": output.payload,
            "intent": doc,
        }
        # Corrupt/repair only isolated synthetic immutable data, never live facts.
        await session.execute(text("ALTER TABLE agent_research_outputs DISABLE TRIGGER USER"))
        import json

        await session.execute(
            text(
                "UPDATE agent_research_outputs SET intent=CAST(:doc AS jsonb), "
                "content_hash=:hash WHERE id=:id"
            ),
            {"doc": json.dumps(doc), "hash": digest(content), "id": output.id},
        )
        await session.execute(text("ALTER TABLE agent_research_outputs ENABLE TRIGGER USER"))
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run_id)
        )
        reader = ObservationReadback(session, read_at=datetime.now(UTC))
        intent = await _validated_intent(session, run, (output,), reader, read_at=datetime.now(UTC))
        assert intent is not None
        from nexus.workbench.research_summary import _limitations

        items, _ = _limitations(intent)
        assert items[0].source == "model"
        assert items[0].text == "synthetic missing EPS scope"
        assert items[0].impact == "synthetic denominator limited"


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", [None, "event_hash", "run_snapshot"])
async def test_cycle_freezes_order_and_partial_candidate_failure(migrated_database_url, defect):
    from decimal import Decimal

    from helpers.paper_batch import DECISION_TIME, INSTRUMENTS, seed_paper_batch
    from nexus.paper import agent_cycle
    from nexus.paper.agent_cycle_models import floor_decision_slot
    from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.strategy.forward import load_forward_strategy_decision
    from nexus.workbench.research_summary import cycle_candidates

    from engine.tests.integration.test_agent_paper_cycle_pg import _seed_two_candidate_authority

    seed = await seed_paper_batch(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        registered = await load_forward_strategy_decision(
            session,
            account_id=seed.account_id,
            strategy_version="baseline_v1",
            decision_session=DECISION_TIME.date(),
        )
        authority = await _seed_two_candidate_authority(
            session, registered, candidate_instruments=INSTRUMENTS
        )
        authority = authority.model_copy(update={"decision_time": seed.request_time})
        batch = await create_agent_paper_candidate_batch(
            session,
            account_id=seed.account_id,
            universe_snapshot_id=authority.universe_snapshot_id,
            instrument_ids=tuple(item[0] for item in INSTRUMENTS),
            idempotency_key="workbench-frozen-candidates",
            created_at=seed.request_time,
        )
        started = await agent_cycle._start_cycle(
            session,
            authority=authority,
            decision_slot_at=floor_decision_slot(DECISION_TIME),
            daily_limit_usd=Decimal("3"),
            candidate_batch_id=batch.batch_id,
            requested_instrument_ids=batch.instrument_ids,
        )
        assert started.status == "researching"
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord)
                .where(AgentResearchRunRecord.id.in_(started.research_run_ids))
                .order_by(AgentResearchRunRecord.instrument_id)
            )
        )
        jobs = tuple(
            await session.scalars(
                select(Job).where(
                    Job.payload["run_id"].as_string().in_([str(run.id) for run in runs])
                )
            )
        )
        for run in runs[:2]:
            job = next(job for job in jobs if job.payload["run_id"] == str(run.id))
            job.status = "running"
            job.started_at = seed.request_time
            job.attempt_count = 1
            job.version += 1
        await session.flush()
        damaged_id = runs[0].id
        if defect == "event_hash":
            await session.execute(text("ALTER TABLE agent_paper_cycle_events DISABLE TRIGGER USER"))
            await session.execute(
                text("UPDATE agent_paper_cycle_events SET event_hash=:hash WHERE cycle_id=:id"),
                {"hash": "0" * 64, "id": started.cycle_id},
            )
            await session.execute(text("ALTER TABLE agent_paper_cycle_events ENABLE TRIGGER USER"))
        if defect == "run_snapshot":
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            await session.execute(
                text("UPDATE agent_research_runs SET data_snapshot_id=:snapshot WHERE id=:id"),
                {"snapshot": runs[1].data_snapshot_id, "id": damaged_id},
            )
            await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
    async with factory() as session:
        cycle = await session.get(AgentPaperCycleRecord, started.cycle_id)
        if defect == "event_hash":
            with pytest.raises(ValueError, match="WORKBENCH_CANDIDATE_BINDING_INVALID"):
                await cycle_candidates(session, cycle, read_at=datetime.now(UTC))
            return
        result = await cycle_candidates(session, cycle, read_at=datetime.now(UTC))
        assert [str(row.instrument_id) for row in result] == [
            row["instrument_id"] for row in cycle.candidate_manifest
        ]
        assert {row.job_status for row in result if row.read_status == "verified"} == {
            "running",
            "queued",
        }
        if defect == "run_snapshot":
            assert result[0].read_status == "unavailable"
            assert result[0].limitations == ()
            assert all(row.read_status == "verified" for row in result[1:])
        else:
            assert all(row.read_status == "verified" for row in result)
        assert all(row.quality is None for row in result)


@pytest.mark.asyncio
async def test_failed_run_with_unknown_result_keeps_unknown_usage_reason(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    run_id, _ = await seed_observation(factory, V11, status="failed")
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        job = Job(
            payload={"run_id": str(run_id)},
            scope_id=str(run.instrument_id),
            job_type="agent.paper.research",
            effective_at=run.decision_time,
            created_at=run.created_at,
            started_at=run.created_at,
            finished_at=run.created_at,
            status="failed",
        )
        at = datetime.now(UTC)
        result = await _project(
            session,
            instrument_id=run.instrument_id,
            symbol="AAPL",
            run=run,
            jobs=(job,),
            reader=ObservationReadback(session, read_at=at),
            read_at=at,
        )
        assert result.run_status == "failed"
        assert result.reason_code == "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN"
        assert result.limitations[0].source == "system"
        assert "未知" in result.limitations[0].text
        assert run.settled_cost_usd is None
