"""Pure-research purpose is frozen independently of legacy prompt authority."""

from hashlib import sha256

import pytest
from nexus.agents.persistence import load_complete_prompt_authority
from nexus.agents.research_only import RESEARCH_ONLY_INSTRUCTIONS, ensure_research_only_prompts
from nexus.ops.openai_local import prepare_database
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


@pytest.mark.asyncio
async def test_pure_prompt_registration_is_immutable_idempotent_and_not_active(
    migrated_database_url,
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings)
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            old = await load_complete_prompt_authority(session)
            old_hashes = {role: row.content_hash for role, row in old.items()}
            new = await ensure_research_only_prompts(session)
            repeated = await ensure_research_only_prompts(session)
            assert {role: row.id for role, row in new.items()} == {
                role: row.id for role, row in repeated.items()
            }
            assert all(row.release_channel == "challenger" for row in new.values())
            assert all(row.content.endswith(RESEARCH_ONLY_INSTRUCTIONS) for row in new.values())
            assert all(
                sha256(row.content.encode()).hexdigest() == row.content_hash for row in new.values()
            )
            assert all(new[role].content_hash != old_hashes[role] for role in new)
            active = await load_complete_prompt_authority(session)
            assert {role: row.content_hash for role, row in active.items()} == old_hashes
            explicit = await load_complete_prompt_authority(
                session, orchestrator_id=new["orchestrator"].id
            )
            assert {role: row.id for role, row in explicit.items()} == {
                role: row.id for role, row in new.items()
            }
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_pure_snapshot_run_and_frozen_tools_without_paper(
    migrated_database_url, tmp_path
):
    import importlib
    from decimal import Decimal

    from helpers.current_identity import archives
    from helpers.research_snapshots import seed_filing
    from integration.test_research_identity_context import seed, source_archive
    from nexus.agents.codex_formal_stage import formal_stage_job_qualified
    from nexus.agents.database_tools import database_read_only_tools
    from nexus.agents.execution_authority import load_frozen_evidence
    from nexus.agents.models import ResearchRequest
    from nexus.agents.persistence import admit_research_run, select_authoritative_snapshot
    from nexus.agents.snapshots import prepare_research_snapshot
    from nexus.jobs.models import JobType
    from nexus.market.object_store import S3ProviderEvidenceStore
    from nexus.persistence.models.agents import AgentModelConfigRecord
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.persistence.models.runtime import Job
    from nexus.research.current_identity_storage import import_identity_binding
    from nexus.research.observation_identity_storage import register_observation_identity_binding
    from sqlalchemy import func, select
    from unit.market.test_object_store import FakeS3Client
    from unit.ops.test_formal_stage_registration import OPTIONS

    settings = EngineSettings(
        database_url=migrated_database_url, agent_runtime_mode="codex_sdk"
    ).model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
        }
    )
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    try:
        instrument = await seed(factory)
        fm, sc = archives(tmp_path / "base")
        base = await importlib.import_module("nexus.ops.instrument_identity_local").revise_listing(
            settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
        )
        source_archive(tmp_path / "source")
        await prepare_database(settings, **OPTIONS)
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            await seed_filing(session, instrument_id=instrument, seed_instrument=False)
            await import_identity_binding(
                session,
                store,
                identity_snapshot_id=base.snapshot_id,
                collection_dir=tmp_path / "source",
            )
            ref = await register_observation_identity_binding(
                session, instrument_id=instrument, store=store
            )
            prompts = await ensure_research_only_prompts(session)
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            cutoff = await session.scalar(select(func.clock_timestamp()))
            snapshot = await prepare_research_snapshot(
                session, instrument, as_of=cutoff, research_only=True
            )
            assert snapshot.source == "nexus_research_only"
            assert snapshot.payload["purpose"] == "research_only_v2"
            assert (
                "portfolio_context" not in snapshot.payload
                and "agent_paper_context" not in snapshot.payload
            )
            assert (
                await select_authoritative_snapshot(session, instrument_id=instrument, as_of=cutoff)
                is None
            )
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v8-stage-capacity"
                )
            )
            result = await admit_research_run(
                session,
                actor_id="pure-test",
                route="/test",
                idempotency_key="pure",
                request_hash="a" * 64,
                request=ResearchRequest(
                    instrument_id=instrument, data_snapshot_id=snapshot.id, decision_time=cutoff
                ),
                prompt_version_id=prompts["orchestrator"].id,
                model_config_id=model.id,
                daily_limit_usd=Decimal(3),
                now=cutoff,
                formal_stage_capacity=True,
                research_only=True,
                job_type=JobType.AGENT_PAPER_RESEARCH,
            )
            frozen = await load_frozen_evidence(session, result.run, allow_fixture_evidence=False)
            assert ref.id in {row.id for row in frozen}
            assert "research_current_identity" not in {row.source for row in frozen}
            job = await session.scalar(select(Job))
            assert await formal_stage_job_qualified(session, job)
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 0
            )
            tools = await database_read_only_tools(
                session, result.run, allow_fixture_evidence=False
            )
            security = await tools.execute(
                "get_security_snapshot",
                {
                    "instrument_id": str(instrument),
                    "data_snapshot_id": str(snapshot.id),
                    "as_of": cutoff.isoformat(),
                },
            )
            assert security["purpose"] == "research_only_v2"
            assert security["portfolio_context_status"] == "not_applicable"
    finally:
        await factory.kw["bind"].dispose()
