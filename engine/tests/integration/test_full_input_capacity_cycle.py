"""Complete frozen candidate cycles use v9 without rewriting any earlier profile."""

import json
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from integration.test_candidate_multistock_path import AT, FIVE, _set_test_insert_clock
from integration.test_candidate_research_authority import OPTIONS
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.agents.codex_capacity import capacity_config, capacity_config_hash
from nexus.agents.responses_budget import UsageSnapshot
from nexus.ops.openai_local import prepare_database
from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.projection import load_paper_projection
from nexus.paper.target_service import approve_initial_target_projection
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job, JobAttempt
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select
from unit.agents.test_codex_sdk import _assessment_payload, _staged_batch


@pytest.mark.asyncio
@pytest.mark.parametrize("formal", [False, True], ids=["isolated", "formal"])
@pytest.mark.parametrize(
    "count,oversized_tail",
    [(3, False), (5, False), (3, True), (3, "invalid_context"), (3, "wrong_mode")],
)
async def test_full_cycle_worker_rederives_whole_pool_before_claim_or_sdk(
    migrated_database_url, monkeypatch, count, oversized_tail, formal
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings, **OPTIONS)
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=FIVE[:count], decision_time=AT
    )
    calls = []

    async def invoke(payload, **options):
        calls.append(payload["stage"])
        assert options["max_wire_bytes"] == 100479
        body = (
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        return json.dumps(body), UsageSnapshot(input_tokens=100, output_tokens=80)

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    try:
        await _set_test_insert_clock(sessions, AT)
        async with sessions.begin() as session:
            config = capacity_config()
            model = AgentModelConfigRecord(
                model_id=config["model_id"],
                version=config["version"],
                config_hash=capacity_config_hash(),
                max_input_tokens=96000,
                max_output_tokens=8192,
                max_tool_calls=0,
                input_usd_per_million=Decimal(0),
                output_usd_per_million=Decimal(0),
                pricing_available_at=datetime.fromisoformat(config["pricing_available_at"]),
                pricing_expires_at=datetime.fromisoformat(config["pricing_expires_at"]),
                created_at=AT - timedelta(seconds=1),
            )
            session.add(model)
            await session.flush()
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(row.instrument_id for row in manifest),
                idempotency_key="v9-whole-batch",
                created_at=AT,
            )
            projection = await load_paper_projection(
                session, account_id=authority.account_id, as_of=AT
            )
            await approve_initial_target_projection(
                session,
                account_id=authority.account_id,
                projection_as_of=AT,
                expected_projection_hash=projection.projection_hash,
                approved_by="isolated-fixture",
                approved_at=AT,
                idempotency_key="v9-initial-baseline",
            )
            result = await start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                idempotency_key="v9-fresh-cycle",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                **(
                    {"formal_stage_capacity": True, "formal_model_config_id": model.id}
                    if formal
                    else {"candidate_stage_capacity": True, "candidate_model_config_id": model.id}
                ),
            )
            assert result.result_code == "RESEARCH_JOBS_PREPARED"
            assert len(result.research_run_ids) == count
            jobs = tuple(await session.scalars(select(Job).order_by(Job.created_at, Job.id)))
        if oversized_tail == "wrong_mode":
            with pytest.raises(ValueError, match="CODEX_CAPACITY_POOL_BINDING_INVALID"):
                await run_agent_worker_once(
                    settings,
                    job_ids=(jobs[0].id,),
                    **(
                        {"full_input_capacity": True} if formal else {"formal_stage_capacity": True}
                    ),
                )
            async with sessions() as session:
                assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 0
                assert all(job.status == "queued" for job in await session.scalars(select(Job)))
            assert calls == []
            return
        if oversized_tail:
            from nexus.agents.codex_staged import StructuredStagedCodexCommittee

            original = StructuredStagedCodexCommittee._inputs

            async def inputs(self, request):
                data = await original(self, request)
                if request.instrument_id == manifest[-1].instrument_id:
                    data["filing_disclosures"] = {"items": [], "text": "x" * 110000}
                return data

            if oversized_tail == "invalid_context":
                from nexus.agents import paper_context

                original_validator = paper_context.revalidate_frozen_agent_paper_context

                async def validator(session, *, run, evidence_records):
                    if run.instrument_id == manifest[-1].instrument_id:
                        return paper_context.FrozenAgentPaperContextStatus.INVALID
                    return await original_validator(
                        session, run=run, evidence_records=evidence_records
                    )

                monkeypatch.setattr(
                    paper_context, "revalidate_frozen_agent_paper_context", validator
                )
                error = "CODEX_CAPACITY_POOL_BINDING_INVALID"
            else:
                monkeypatch.setattr(StructuredStagedCodexCommittee, "_inputs", inputs)
                error = "CODEX_CAPACITY_POOL_WIRE_EXCEEDED"
            with pytest.raises(ValueError, match=error):
                await run_agent_worker_once(
                    settings,
                    job_ids=(jobs[0].id,),
                    **(
                        {"formal_stage_capacity": True} if formal else {"full_input_capacity": True}
                    ),
                )
            async with sessions() as session:
                assert await session.scalar(select(func.count()).select_from(JobAttempt)) == 0
                assert all(job.status == "queued" for job in await session.scalars(select(Job)))
            assert calls == []
            return
        for job in jobs:
            assert await run_agent_worker_once(
                settings,
                job_ids=(job.id,),
                **({"formal_stage_capacity": True} if formal else {"full_input_capacity": True}),
            )
            assert not await run_agent_worker_once(
                settings,
                job_ids=(job.id,),
                **({"formal_stage_capacity": True} if formal else {"full_input_capacity": True}),
            )
        async with sessions() as session:
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            assert len(runs) == count and all(
                r.status == "completed" and r.retry_count == 0 for r in runs
            )
            assert all(
                r.pricing_version == config["version"] and r.model_hash == capacity_config_hash()
                for r in runs
            )
            assert all(
                j.status == "succeeded" and j.attempt_count == 1
                for j in await session.scalars(select(Job))
            )
        assert calls == ["research", "critique", "synthesis"] * count
    finally:
        await engine.dispose()
