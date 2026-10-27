"""New explicit request lineage without a PaperCycle or a model/provider call."""

from uuid import uuid4

import pytest
from integration.test_research_request_pg import prepare_scope
from nexus.monitoring.independent_storage import read_independent_context
from nexus.ops.openai_local import prepare_database
from nexus.paper.manual_request import (
    admit_manual_request,
    claim_manual_request,
    read_manual_request,
)
from nexus.paper.research_request import finish_research_request, prepare_research_request
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperOrderRecord
from sqlalchemy import func, select
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.mark.asyncio
async def test_independent_three_stock_preparation_freezes_identity_scope_and_budget_without_cycle(
    migrated_database_url, tmp_path, monkeypatch
):
    factory, settings, store, ids = await prepare_scope(
        migrated_database_url, tmp_path, bounded=True
    )
    await prepare_database(
        settings,
        **(
            OPTIONS
            | dict(
                max_wire_bytes=100479,
                full_input_capacity=True,
                formal_bounded_stage_budget=True,
                price_monitoring=True,
                disclosure_monitoring=True,
            )
        ),
    )
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        receipt = await admit_manual_request(
            session,
            account_id=settings.agent_paper_account_id,
            batch_id=settings.agent_paper_candidate_batch_id,
            model_config_id=settings.agent_paper_formal_model_config_id,
            mode="research_and_monitor",
            contract_version=3,
            idempotency_key="independent-new-one",
            two_flight=True,
        )
        assert receipt.research_contract == "independent_monitor_research_v1"
        await claim_manual_request(session, request_id=receipt.request_id)
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        prepared = await prepare_research_request(session, settings, request_id=receipt.request_id)
        assert prepared.status == "active" and prepared.cycle_id is None
        assert {m.instrument_id for m in prepared.research.members} == set(ids)
        assert len(prepared.research.members) == 3
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        row = await session.get(ManualPaperRequest, receipt.request_id)
        read = await read_manual_request(session, row)
        assert read == prepared
        for member in read.research.members:
            run = await session.get(AgentResearchRunRecord, member.run_id)
            context = await read_independent_context(
                session,
                snapshot_id=run.data_snapshot_id,
                instrument_id=run.instrument_id,
                as_of=run.decision_time,
            )
            assert context is not None
            assert context[0].request_id == row.id
            assert set(context[0].candidate_ids) == set(ids)
            assert context[0].predecessor("price").plan_id is None
            assert context[0].predecessor("disclosure").plan_id is None
            assert context[0].disclosure_monitoring.gap_reason == "SEC_BASELINE_MISSING"
        assert await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
        assert (
            await session.scalar(
                select(func.count()).select_from(AgentDailyBudgetReservationRecord)
            )
            == 3
        )
    async with factory.begin() as session:
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            await admit_manual_request(
                session,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                model_config_id=settings.agent_paper_formal_model_config_id,
                mode="research_and_monitor",
                contract_version=3,
                idempotency_key=str(uuid4()),
                two_flight=True,
            )
    async with factory.begin() as session:
        row = await session.get(ManualPaperRequest, receipt.request_id)
        closed = await finish_research_request(session, row=row, cancel_unused=True)
        assert closed.status == "unavailable" and closed.cycle_id is None
    async with factory.begin() as session:
        next_request = await admit_manual_request(
            session,
            account_id=settings.agent_paper_account_id,
            batch_id=settings.agent_paper_candidate_batch_id,
            model_config_id=settings.agent_paper_formal_model_config_id,
            mode="research_and_monitor",
            contract_version=3,
            idempotency_key="next-after-safe-close",
            two_flight=True,
        )
        assert next_request.status == "queued" and next_request.request_id != receipt.request_id
    await factory.kw["bind"].dispose()
    # The new identity may never be erased by rolling back its reader contract.
    import asyncio
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from sqlalchemy import text

    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    with pytest.raises(Exception, match="INDEPENDENT_MONITOR_DOWNGRADE_FORBIDDEN"):
        await asyncio.to_thread(command.downgrade, config, "0097_monitor_daily_events")
    async with factory() as session:
        assert (
            await session.scalar(text("SELECT version_num FROM alembic_version"))
            == "0098_independent_monitor"
        )
        assert (await session.get(ManualPaperRequest, receipt.request_id)).contract_version == 3
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [False, True, "veto", "rollback", "sec_only"])
async def test_native_independent_price_sec_scan_to_single_stock_successor(
    migrated_database_url, monkeypatch, tmp_path, outcome
):
    from integration.test_monitor_mixed_increment_pg import (
        test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls as full_chain,
    )

    await full_chain(migrated_database_url, monkeypatch, tmp_path, outcome, independent=True)
