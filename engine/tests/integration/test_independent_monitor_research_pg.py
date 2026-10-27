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
    calls = []

    async def unexpected_sdk_call(*args, **kwargs):
        calls.append(True)
        raise AssertionError("admission and identity readback must not call the model")

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", unexpected_sdk_call)
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
            from nexus.agents.codex_staged import StructuredStagedCodexCommittee
            from nexus.ops.research_workload_inspection import _inspection_committee
            from nexus.persistence.models.agents import AgentModelConfigRecord

            profile = await session.get(AgentModelConfigRecord, run.model_config_id)
            inspection = await _inspection_committee(session, settings, run, model=profile)
            assert isinstance(inspection, StructuredStagedCodexCommittee)
            assert inspection.usage.provider_calls == 0
        assert await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
        assert (
            await session.scalar(
                select(func.count()).select_from(AgentDailyBudgetReservationRecord)
            )
            == 3
        )
    # Same native admission identity is purpose-bound even on its early replay path.
    from nexus.agents.models import ResearchRequest
    from nexus.agents.persistence import admit_research_run
    from nexus.jobs.models import JobType, job_input_hash
    from nexus.persistence.models.runtime import Job

    async with factory.begin() as session:
        run = await session.get(AgentResearchRunRecord, prepared.research.members[0].run_id)
        request = ResearchRequest(
            instrument_id=run.instrument_id,
            data_snapshot_id=run.data_snapshot_id,
            decision_time=run.decision_time,
        )
        kwargs = dict(
            actor_id="local-independent-monitor-research-v1",
            route="independent-monitor-research-v1",
            idempotency_key=run.idempotency_key,
            request_hash=job_input_hash(request.model_dump(mode="json")),
            request=request,
            prompt_version_id=run.prompt_version_id,
            model_config_id=run.model_config_id,
            daily_limit_usd=settings.agent_daily_budget_usd,
            now=run.decision_time,
            job_type=JobType.AGENT_PAPER_RESEARCH,
            formal_stage_capacity=True,
        )
        before = tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
            ]
        )
        holds_before = tuple(
            (row.id, row.state, row.reserved_usd, row.settled_usd)
            for row in await session.scalars(
                select(AgentDailyBudgetReservationRecord).order_by(
                    AgentDailyBudgetReservationRecord.id
                )
            )
        )
        for flags, reason in (
            ({}, "RESEARCH_ONLY_ADMISSION_REQUIRED"),
            ({"research_only": True}, "RESEARCH_ONLY_SNAPSHOT_INVALID"),
        ):
            with pytest.raises(ValueError, match=f"^{reason}$"):
                await admit_research_run(session, **kwargs, **flags)
        replay = await admit_research_run(session, **kwargs, independent_monitor=True)
        assert replay.replayed and replay.run_id == run.id
        assert calls == []
        assert (
            tuple(
                [
                    await session.scalar(select(func.count()).select_from(model))
                    for model in (AgentResearchRunRecord, Job, AgentDailyBudgetReservationRecord)
                ]
            )
            == before
        )
        assert (
            tuple(
                (row.id, row.state, row.reserved_usd, row.settled_usd)
                for row in await session.scalars(
                    select(AgentDailyBudgetReservationRecord).order_by(
                        AgentDailyBudgetReservationRecord.id
                    )
                )
            )
            == holds_before
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
@pytest.mark.parametrize(
    "outcome", [False, True, "veto", "rollback", "sec_only", "tokenizer_missing"]
)
async def test_native_independent_price_sec_scan_to_single_stock_successor(
    migrated_database_url, monkeypatch, tmp_path, outcome
):
    from integration.test_monitor_mixed_increment_pg import (
        test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls as full_chain,
    )

    await full_chain(migrated_database_url, monkeypatch, tmp_path, outcome, independent=True)


@pytest.mark.asyncio
async def test_sec_only_new_root_cannot_replace_after_other_kind_is_installed_late(
    migrated_database_url, monkeypatch, tmp_path
):
    import json
    from datetime import timedelta

    from helpers.disclosure_observations import reader_source
    from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
    from nexus.agents import codex_staged
    from nexus.monitoring.installation import install_plan_from_output
    from nexus.monitoring.reconciliation import reconcile_account_results
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from nexus.persistence.models.monitoring import MonitorCoverageReceipt, MonitorPlan
    from nexus.persistence.models.runtime import Job
    from nexus.settings import AgentRuntimeMode
    from nexus.worker import run_agent_worker_once
    from unit.agents.test_codex_sdk import _restore_view

    factory, settings, _, captured, original = await reader_source(
        migrated_database_url, monkeypatch, tmp_path, independent=True
    )
    async with factory() as session:
        first = await session.get(AgentResearchRunRecord, original.source_run_id)
        prior = await session.scalar(select(ManualPaperRequest))
    settings = settings.model_copy(
        update=dict(
            agent_paper_account_id=prior.account_id,
            agent_paper_candidate_batch_id=prior.candidate_batch_id,
            agent_paper_formal_model_config_id=first.model_config_id,
            agent_runtime_mode=AgentRuntimeMode.CODEX_SDK,
        )
    )
    captured["clock"][0] = AT + timedelta(seconds=2)
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory.begin() as session:
        receipt = await admit_manual_request(
            session,
            account_id=prior.account_id,
            batch_id=prior.candidate_batch_id,
            model_config_id=first.model_config_id,
            mode="research_and_monitor",
            contract_version=3,
            idempotency_key="new-sec-only-before-late-price",
        )
        await claim_manual_request(session, request_id=receipt.request_id)
    async with factory.begin() as session:
        prepared = await prepare_research_request(session, settings, request_id=receipt.request_id)
    native = codex_staged.invoke_codex

    async def sec_only(payload, **kwargs):
        raw, usage = await native(payload, **kwargs)
        wire = _restore_view(payload)
        body = json.loads(raw)
        if wire["stage"] == "research":
            for role in body["roles"]:
                role["monitoring_proposal"] = None
        elif wire["stage"] == "critique":
            for role in body["roles"]:
                role["monitoring_review"] = None
        else:
            body["monitoring_adoption_hash"] = None
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", sec_only)
    for member in prepared.research.members:
        assert await run_agent_worker_once(
            settings, job_ids=(member.job_id,), formal_stage_capacity=True
        )
    async with factory.begin() as session:
        request = await session.get(ManualPaperRequest, receipt.request_id)
        assert (await finish_research_request(session, row=request)).status == "completed"
        member = next(
            m for m in prepared.research.members if m.instrument_id == original.instrument_id
        )
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == member.run_id,
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status == "completed",
            )
        )
        assert output.intent.get("monitoring_proposal") is None
        assert output.intent["disclosure_proposal"] is not None
        frozen_hash = output.content_hash
    captured["clock"][0] = AT + timedelta(seconds=3)
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory.begin() as session:
        installed = await install_plan_from_output(
            session,
            account_id=prior.account_id,
            output_id=original.source_output_id,
            expected_predecessor_id=None,
            now=captured["clock"][0],
        )
        assert installed.source_run_id == original.source_run_id
    async with factory.begin() as session:
        result = await reconcile_account_results(
            session,
            account_id=prior.account_id,
            instrument_ids=(original.instrument_id,),
            now=captured["clock"][0],
        )
        assert result["superseded"] == 2, result
    async with factory() as session:
        assert not await session.scalar(
            select(MonitorPlan.id).where(MonitorPlan.source_run_id == member.run_id)
        )
        coverage = tuple(
            await session.scalars(
                select(MonitorCoverageReceipt).where(MonitorCoverageReceipt.run_id == member.run_id)
            )
        )
        assert len(coverage) == 2 and all(r.status == "superseded" for r in coverage)
        assert (await session.get(AgentResearchOutputRecord, output.id)).content_hash == frozen_hash
        assert await session.scalar(select(func.count()).select_from(Job)) == 6
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_independent_source_rechecks_current_pricing_for_historical_reads(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers.disclosure_observations import reader_source
    from nexus.monitoring.source_qualification import load_qualified_source
    from nexus.persistence.models.agents import AgentModelConfigRecord

    factory, _, _, captured, pointer = await reader_source(
        migrated_database_url, monkeypatch, tmp_path, independent=True
    )
    original_time = captured["clock"][0]
    async with factory() as session:
        source = await load_qualified_source(
            session,
            account_id=pointer.account_id,
            output_id=pointer.source_output_id,
            now=original_time,
        )
        model = await session.get(AgentModelConfigRecord, source.run.model_config_id)
        captured["clock"][0] = model.pricing_expires_at
        with pytest.raises(ValueError, match="MONITOR_SOURCE_INVALID"):
            await load_qualified_source(
                session,
                account_id=pointer.account_id,
                output_id=pointer.source_output_id,
                now=original_time,
            )
    await factory.kw["bind"].dispose()


async def _new_independent_result(factory, settings, captured, pointer, *, second):
    from datetime import timedelta

    from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from nexus.settings import AgentRuntimeMode
    from nexus.worker import run_agent_worker_once

    async with factory() as session:
        original = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        prior = await session.scalar(
            select(ManualPaperRequest).order_by(ManualPaperRequest.requested_at).limit(1)
        )
    settings = settings.model_copy(
        update=dict(
            agent_paper_account_id=prior.account_id,
            agent_paper_candidate_batch_id=prior.candidate_batch_id,
            agent_paper_formal_model_config_id=original.model_config_id,
            agent_runtime_mode=AgentRuntimeMode.CODEX_SDK,
        )
    )
    captured["clock"][0] = AT + timedelta(seconds=second)
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory.begin() as session:
        receipt = await admit_manual_request(
            session,
            account_id=prior.account_id,
            batch_id=prior.candidate_batch_id,
            model_config_id=original.model_config_id,
            mode="research_and_monitor",
            contract_version=3,
            idempotency_key=f"independent-pair-{second}",
        )
        await claim_manual_request(session, request_id=receipt.request_id)
    async with factory.begin() as session:
        prepared = await prepare_research_request(session, settings, request_id=receipt.request_id)
    for member in prepared.research.members:
        assert await run_agent_worker_once(
            settings, job_ids=(member.job_id,), formal_stage_capacity=True
        )
    async with factory.begin() as session:
        row = await session.get(ManualPaperRequest, receipt.request_id)
        assert (await finish_research_request(session, row=row)).status == "completed"
        member = next(
            m for m in prepared.research.members if m.instrument_id == pointer.instrument_id
        )
        run = await session.get(AgentResearchRunRecord, member.run_id)
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == run.id,
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status == "completed",
            )
        )
    return run, output


@pytest.mark.asyncio
@pytest.mark.parametrize("with_receipt", [False, True])
async def test_independent_reconcile_completes_existing_same_run_kind_once(
    migrated_database_url, monkeypatch, tmp_path, with_receipt
):
    from helpers.disclosure_observations import reader_source
    from nexus.monitoring.installation import install_plan_from_output
    from nexus.monitoring.reconciliation import _reconcile_run, reconcile_account_results
    from nexus.persistence.models.monitoring import MonitorCoverageReceipt, MonitorPlan

    factory, _, _, captured, pointer = await reader_source(
        migrated_database_url, monkeypatch, tmp_path, independent=True
    )
    from datetime import timedelta

    from integration.test_candidate_multistock_path import AT, _set_test_insert_clock

    captured["clock"][0] = AT + timedelta(seconds=2)
    await _set_test_insert_clock(factory, captured["clock"][0])
    now = captured["clock"][0]
    async with factory.begin() as session:
        plan = await install_plan_from_output(
            session,
            account_id=pointer.account_id,
            output_id=pointer.source_output_id,
            expected_predecessor_id=None,
            now=now,
        )
        run = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        if with_receipt:
            receipt = await _reconcile_run(
                session, account_id=pointer.account_id, run=run, now=now, kind="price"
            )
            assert receipt.status == "installed" and receipt.active_plan_id == plan.id
            receipt_id = receipt.id
    async with factory.begin() as session:
        result = await reconcile_account_results(
            session, account_id=pointer.account_id, instrument_ids=(pointer.instrument_id,), now=now
        )
        assert result["installed"] == (1 if with_receipt else 2), result
        plans = tuple(
            await session.scalars(
                select(MonitorPlan).where(MonitorPlan.instrument_id == pointer.instrument_id)
            )
        )
        receipts = tuple(
            await session.scalars(
                select(MonitorCoverageReceipt).where(
                    MonitorCoverageReceipt.run_id == pointer.source_run_id
                )
            )
        )
        assert len(plans) == len(receipts) == 2
        assert {p.kind for p in plans} == {"price", "disclosure"}
        assert all(p.generation == 1 and p.predecessor_id is None for p in plans)
        assert next(p for p in plans if p.kind == "price").id == plan.id
        if with_receipt:
            assert next(r for r in receipts if r.kind == "price").id == receipt_id
        frozen = ({p.id for p in plans}, {r.id for r in receipts})
    async with factory.begin() as session:
        assert (
            await reconcile_account_results(
                session,
                account_id=pointer.account_id,
                instrument_ids=(pointer.instrument_id,),
                now=now,
            )
        )["recorded"] == 0
        assert (
            {
                p.id
                for p in await session.scalars(
                    select(MonitorPlan).where(MonitorPlan.instrument_id == pointer.instrument_id)
                )
            },
            {
                r.id
                for r in await session.scalars(
                    select(MonitorCoverageReceipt).where(
                        MonitorCoverageReceipt.run_id == pointer.source_run_id
                    )
                )
            },
        ) == frozen
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("own_price_receipt", [False, True])
async def test_independent_existing_predecessor_change_fences_whole_pair(
    migrated_database_url, monkeypatch, tmp_path, own_price_receipt
):
    from helpers.disclosure_observations import reader_source
    from nexus.monitoring.disclosure_installation import install_disclosure_plan
    from nexus.monitoring.reconciliation import _reconcile_run, reconcile_account_results
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from nexus.persistence.models.monitoring import MonitorCoverageReceipt, MonitorPlan

    factory, settings, _, captured, pointer = await reader_source(
        migrated_database_url, monkeypatch, tmp_path, independent=True
    )
    from datetime import timedelta

    from integration.test_candidate_multistock_path import AT, _set_test_insert_clock

    captured["clock"][0] = AT + timedelta(seconds=2)
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory.begin() as session:
        assert (
            await reconcile_account_results(
                session,
                account_id=pointer.account_id,
                instrument_ids=(pointer.instrument_id,),
                now=captured["clock"][0],
            )
        )["installed"] == 2
        prior = {
            p.kind: p.id
            for p in await session.scalars(
                select(MonitorPlan).where(MonitorPlan.instrument_id == pointer.instrument_id)
            )
        }
    foreign, foreign_output = await _new_independent_result(
        factory, settings, captured, pointer, second=3
    )
    target, output = await _new_independent_result(factory, settings, captured, pointer, second=4)
    captured["clock"][0] = AT + timedelta(seconds=5)
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory.begin() as session:
        bound = await read_independent_context(
            session,
            snapshot_id=target.data_snapshot_id,
            instrument_id=target.instrument_id,
            as_of=target.decision_time,
        )
        assert {p.kind: p.plan_id for p in bound[0].predecessors} == prior
        if own_price_receipt:
            receipt = await _reconcile_run(
                session,
                account_id=pointer.account_id,
                run=target,
                now=captured["clock"][0],
                kind="price",
            )
            assert receipt.status == "installed"
            own_receipt = (receipt.id, receipt.active_plan_id, receipt.status)
        replacement = await install_disclosure_plan(
            session,
            account_id=pointer.account_id,
            output_id=foreign_output.id,
            expected_predecessor_id=prior["disclosure"],
            now=captured["clock"][0],
        )
        assert replacement.generation == 2 and replacement.source_run_id == foreign.id
        plan_facts = {
            (p.id, p.source_hash, p.generation, p.predecessor_id)
            for p in await session.scalars(
                select(MonitorPlan).where(MonitorPlan.instrument_id == pointer.instrument_id)
            )
        }
    async with factory.begin() as session:
        result = await reconcile_account_results(
            session,
            account_id=pointer.account_id,
            instrument_ids=(pointer.instrument_id,),
            now=captured["clock"][0],
        )
        assert result["superseded"] == (1 if own_price_receipt else 2), result
        assert {
            (p.id, p.source_hash, p.generation, p.predecessor_id)
            for p in await session.scalars(
                select(MonitorPlan).where(MonitorPlan.instrument_id == pointer.instrument_id)
            )
        } == plan_facts
        receipts = tuple(
            await session.scalars(
                select(MonitorCoverageReceipt).where(MonitorCoverageReceipt.run_id == target.id)
            )
        )
        assert len(receipts) == 2
        if own_price_receipt:
            saved = next(r for r in receipts if r.kind == "price")
            assert (saved.id, saved.active_plan_id, saved.status) == own_receipt
        assert next(r for r in receipts if r.kind == "disclosure").status == "superseded"
        assert (
            await session.get(AgentResearchOutputRecord, output.id)
        ).content_hash == output.content_hash
    await factory.kw["bind"].dispose()
