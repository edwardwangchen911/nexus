"""Real persisted request receipts, never model or provider calls."""

from datetime import timedelta
from uuid import uuid4

import pytest
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.ops.openai_local import prepare_database
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.manual_request import manual_request_hash
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.settings import EngineSettings
from sqlalchemy import select
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.asyncio
async def test_activity_preparing_without_cycle_and_cross_day_unknown(
    migrated_database_url, monkeypatch
):
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **OPTIONS,
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=AT
    )
    try:
        async with sessions.begin() as session:
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v8-stage-capacity"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(m.instrument_id for m in manifest),
                idempotency_key="workbench-scope",
                created_at=AT,
            )
            row = ManualPaperRequest(
                id=uuid4(),
                account_id=authority.account_id,
                candidate_batch_id=batch.batch_id,
                model_config_id=model.id,
                mode="research_and_paper",
                contract_version=1,
                research_profile=None,
                key_hash="c" * 64,
                request_hash=manual_request_hash(
                    account_id=authority.account_id,
                    batch_id=batch.batch_id,
                    mode="research_and_paper",
                ),
                batch_hash=batch.batch_hash,
                model_hash=model.config_hash,
                requested_at=AT,
                created_at=AT,
            )
            session.add(row)
            await session.flush()
            # Persist through the existing event writer and its hash contract.
            from nexus.paper.manual_request import _append

            await _append(
                session, row, status="queued", code="MANUAL_REQUEST_QUEUED", cycle_id=None, at=AT
            )
            await _append(
                session,
                row,
                status="preparing",
                code="MANUAL_PREPARATION_STARTED",
                cycle_id=None,
                at=AT + timedelta(seconds=1),
            )
            request_id = row.id
        from nexus.workbench.activity_read import read_activity

        async with sessions() as session:
            result = await read_activity(
                session, read_at=AT + timedelta(seconds=2), decision_session=AT.date()
            )
            formal, pure, independent = result.scopes
            assert independent.mode == "research_and_monitor"
            assert independent.today_registered == 0 and independent.selection == "none"
            assert independent.portfolio_status == "not_applicable"
            assert result.read_status == "complete"
            assert formal.request_id == request_id and formal.cycle_id is None
            assert formal.status == "preparing" and formal.selection == "active"
            assert formal.today_registered == 1 and formal.today_started == 0
            assert formal.portfolio_status == "awaiting_cycle"
            assert pure.today_registered == 0 and pure.selection == "none"
        async with sessions.begin() as session:
            row = await session.get(ManualPaperRequest, request_id)
            await _append(
                session,
                row,
                status="unknown",
                code="MANUAL_RESULT_UNKNOWN",
                cycle_id=None,
                at=AT + timedelta(seconds=3),
            )
        async with sessions() as session:
            result = await read_activity(
                session,
                read_at=AT + timedelta(days=1),
                decision_session=(AT + timedelta(days=1)).date(),
            )
            formal = result.scopes[0]
            assert formal.today_registered == 0 and formal.today_started == 0
            assert formal.selection == "historical_unresolved" and formal.historical
            assert formal.status == "unknown" and formal.occupancy == "unresolved"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_workbench_business_snapshot_is_read_only_repeatable_read(migrated_database_url):
    from nexus.api.routes.workbench import workbench_read_session
    from nexus.persistence.session import session_factory
    from sqlalchemy import text

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async for session in workbench_read_session(factory):
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
    finally:
        await factory.kw["bind"].dispose()
