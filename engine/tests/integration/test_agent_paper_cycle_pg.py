from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import DECISION_TIME, INSTRUMENTS, seed_paper_batch
from nexus.agents.actionable_research import ActionableResearchDecision
from nexus.agents.committee import load_prompt_assets
from nexus.agents.execution_authority import ROLE_ORDER
from nexus.paper.agent_cycle import advance_agent_paper_cycle
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from test_agent_paper_decision_pg import _seed_adjusted_authority


async def _seed_cycle_research_authority(
    session: AsyncSession, *, registered_decision: object
) -> None:
    await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
    try:
        await session.execute(
            text(
                "UPDATE universe_snapshots SET key='us_liquid',version='us_liquid_v1',"
                "source='nexus_internal_us_liquid_v1'"
            )
        )
    finally:
        await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
    prompts = load_prompt_assets()
    session.add_all(
        AgentPromptVersionRecord(
            id=UUID(int=0x63000000000000000000000000000100 + index),
            role=role,
            version=f"agent-paper-{role}-v1",
            content_hash=prompts[role].content_hash,
            content=prompts[role].content,
        )
        for index, role in enumerate(ROLE_ORDER)
    )
    session.add(
        AgentModelConfigRecord(
            id=UUID("63000000-0000-0000-0000-000000000001"),
            model_id="fixture-agent-paper",
            version="codex-sdk-chatgpt-v4-actionable",
            config_hash="6" * 64,
            max_input_tokens=100,
            max_tool_calls=1,
            max_output_tokens=100,
            input_usd_per_million=Decimal("1"),
            output_usd_per_million=Decimal("1"),
            pricing_available_at=DECISION_TIME - timedelta(days=1),
            pricing_expires_at=DECISION_TIME + timedelta(days=1),
        )
    )
    for index, (instrument_id, _, _, _) in enumerate(INSTRUMENTS, start=1):
        evidence = ResearchEvidenceRef(
            id=UUID(int=0x63000000000000000000000000000200 + index),
            source="sec",
            provider_object_id=f"agent-paper:{instrument_id}",
            revision="v1",
            available_at=DECISION_TIME - timedelta(hours=2),
            location=f"memory://sha256/{f'{index + 6}' * 64}",
            content_hash=f"{index + 6}" * 64,
            byte_length=10,
        )
        snapshot = DataSnapshot(
            id=UUID(int=0x63000000000000000000000000000300 + index),
            source="nexus_research",
            provider_object_id=str(instrument_id),
            revision="agent-paper-v1",
            observed_at=DECISION_TIME - timedelta(hours=2),
            available_at=DECISION_TIME - timedelta(hours=2),
            received_at=DECISION_TIME - timedelta(hours=2),
            schema_version="research_filings_v1",
            payload={"scope": "persisted_research_observations"},
        )
        session.add_all(
            (
                InstrumentAlias(
                    id=UUID(int=0x63000000000000000000000000000400 + index),
                    instrument_id=instrument_id,
                    symbol=f"T{index}",
                    alias_type="ticker",
                    valid_from=date(2026, 1, 1),
                    valid_to=None,
                ),
                evidence,
                snapshot,
            )
        )
        await session.flush()
        session.add(
            Filing(
                instrument_id=instrument_id,
                evidence_ref_id=evidence.id,
                source="sec",
                provider_object_id=evidence.provider_object_id,
                revision=evidence.revision,
                accession_number=f"000000000{index}-26-000001",
                form="10-Q",
                filed_at=DECISION_TIME - timedelta(days=1),
                accepted_at=DECISION_TIME - timedelta(hours=2),
                created_at=DECISION_TIME - timedelta(hours=2),
            )
        )
        await session.flush()
        session.add(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=snapshot.id,
                instrument_id=instrument_id,
                evidence_ref_id=evidence.id,
                authority_kind="research",
                selected_at=None,
                source=evidence.source,
                provider_object_id=evidence.provider_object_id,
                revision=evidence.revision,
                available_at=evidence.available_at,
                location=evidence.location,
                content_hash=evidence.content_hash,
            )
        )
    await session.flush()
    await _seed_adjusted_authority(session, registered_decision)


@pytest.mark.asyncio
async def test_first_call_enqueues_existing_research_jobs_and_replay_does_not_duplicate(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(session, registered_decision=registered.decision)

        async with sessions.begin() as session:
            first = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
        assert first.status == "researching", first.result_code
        assert first.replayed is False
        assert len(first.research_run_ids) == 3

        async with sessions.begin() as session:
            second = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
            assert second == first.model_copy(update={"replayed": True})
            assert await session.scalar(select(func.count()).select_from(Job)) == 3
            cycle_count = await session.scalar(
                select(func.count()).select_from(AgentPaperCycleRecord)
            )
            assert cycle_count == 1
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleEventRecord))
                == 2
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_IDEMPOTENCY_CONFLICT"):
                await advance_agent_paper_cycle(
                    session,
                    account_id=seed.account_id,
                    decision_time=DECISION_TIME + timedelta(seconds=1),
                    idempotency_key="agent-paper-cycle-1",
                )

        admitted: dict[UUID, ActionableResearchDecision] = {}
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE strategy_decision_targets DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE strategy_decisions DISABLE TRIGGER USER"))
            try:
                await session.execute(text("DELETE FROM strategy_decision_targets"))
                await session.execute(text("DELETE FROM strategy_decisions"))
            finally:
                await session.execute(text("ALTER TABLE strategy_decisions ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE strategy_decision_targets ENABLE TRIGGER USER")
                )
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE agent_research_runs SET status='completed' WHERE id = ANY(:run_ids)"
                    ),
                    {"run_ids": list(first.research_run_ids)},
                )
            finally:
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
            await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE jobs SET status='succeeded',started_at=:at,finished_at=:at,"
                        "attempt_count=1,result_code='AGENT_RESEARCH_COMPLETED',version=version+1"
                    ),
                    {"at": DECISION_TIME},
                )
            finally:
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
            for index, (run_id, instrument) in enumerate(
                zip(first.research_run_ids, INSTRUMENTS, strict=True), start=1
            ):
                output = AgentResearchOutputRecord(
                    id=UUID(int=0x63000000000000000000000000000500 + index),
                    run_id=run_id,
                    sequence=1,
                    status="completed",
                    role=None,
                    prompt_hash=None,
                    content_hash=f"{index}" * 64,
                    payload={},
                    intent={},
                )
                session.add(output)
                admitted[run_id] = ActionableResearchDecision(
                    instrument_id=instrument[0],
                    output_id=output.id,
                    intent_hash=f"{index + 3}" * 64,
                    stance="watch",
                    sizing_action="none",
                    evidence_ref_ids=(UUID(int=0x63000000000000000000000000000200 + index),),
                )

        async def fake_load_actionable(
            _session: AsyncSession, *, run_id: UUID, decision_time: datetime
        ) -> ActionableResearchDecision:
            assert decision_time == DECISION_TIME
            return admitted[run_id]

        monkeypatch.setattr(
            "nexus.paper.agent_cycle.load_actionable_research_output", fake_load_actionable
        )
        monkeypatch.setattr(
            "nexus.paper.agent_decision_store.load_actionable_research_output",
            fake_load_actionable,
        )
        async with sessions.begin() as session:
            completed = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
        assert completed.status == "completed"
        assert completed.strategy_decision_id is not None
        assert completed.batch_id is not None
        assert completed.order_ids == ()
        assert completed.replayed is False

        async with sessions.begin() as session:
            replayed = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
            assert replayed == completed.model_copy(update={"replayed": True})
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mixed_terminal_research_jobs_stop_without_replacement_runs(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(session, registered_decision=registered.decision)
        async with sessions.begin() as session:
            first = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-terminal-failure",
            )
        assert first.status == "researching"

        async with sessions.begin() as session:
            jobs = tuple(await session.scalars(select(Job).order_by(Job.id)))
            await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
            try:
                for index, job in enumerate(jobs):
                    status = "succeeded" if index == 0 else "dead_letter"
                    await session.execute(
                        text(
                            "UPDATE jobs SET status=:status,started_at=:at,finished_at=:at,"
                            "attempt_count=1,version=version+1 WHERE id=:job_id"
                        ),
                        {"status": status, "at": DECISION_TIME, "job_id": job.id},
                    )
            finally:
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
        async with sessions.begin() as session:
            stopped = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-terminal-failure",
            )
            run_count = await session.scalar(
                select(func.count()).select_from(AgentResearchRunRecord)
            )
            job_count = await session.scalar(select(func.count()).select_from(Job))
        assert stopped.status == "unavailable"
        assert stopped.result_code == "RESEARCH_OUTPUT_NOT_ACTIONABLE"
        assert stopped.research_run_ids == first.research_run_ids
        assert run_count == 3
        assert job_count == 3
    finally:
        await engine.dispose()
