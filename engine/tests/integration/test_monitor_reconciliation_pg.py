"""Normal completion to automatic coverage, using original frozen Worker outputs."""

import asyncio
from datetime import timedelta

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import AT
from integration.test_candidate_multistock_path import (
    test_fresh_multistock_candidate_cycle_freezes_native_jobs as normal_cycle,
)
from nexus.ops.research_monitor import parser, run
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorCoverageReceipt, MonitorPlan
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["approved", "audit-veto", "invalid-one", "current-scope"])
async def test_normal_scan_reconciles_daily_outputs_once_without_model_calls(
    migrated_database_url, monkeypatch, case
):
    rejected = case == "audit-veto"
    expected = 2 if case in {"invalid-one", "current-scope"} else 3
    await normal_cycle(
        migrated_database_url,
        monkeypatch,
        3,
        False,
        "formal_v11",
        monitoring=True,
        monitoring_rejected=rejected,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        cycle = await session.scalar(select(AgentPaperCycleRecord))
        original_runs = tuple(await session.scalars(select(AgentResearchRunRecord.id)))
        scan_at = max(
            AT + timedelta(seconds=10), await session.scalar(select(func.clock_timestamp()))
        )
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord

    async with factory() as session, session.begin():
        monday = AT + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=monday.date(),
                decision_at=monday.replace(hour=21),
                market_open_at=monday.replace(hour=13, minute=30),
                market_close_at=monday.replace(hour=20),
                created_at=AT - timedelta(days=1),
            )
        )
    settings = EngineSettings(
        database_url=migrated_database_url,
        agent_paper_account_id=cycle.account_id,
        agent_paper_candidate_batch_id=cycle.candidate_batch_id,
    )
    from nexus.monitoring import reconciliation
    from nexus.paper.candidate_batch import create_agent_paper_candidate_batch

    async with factory() as session:
        securities = tuple(
            await session.scalars(
                select(AgentResearchRunRecord.instrument_id).order_by(
                    AgentResearchRunRecord.instrument_id
                )
            )
        )
    if case == "invalid-one":
        native_scope = reconciliation._scope_evidence

        async def one_invalid(session, *, run, account_id):
            if run.instrument_id == securities[0]:
                raise ValueError("MONITOR_SOURCE_INVALID")
            return await native_scope(session, run=run, account_id=account_id)

        monkeypatch.setattr(reconciliation, "_scope_evidence", one_invalid)
    if case == "current-scope":
        async with factory() as session, session.begin():
            current = await create_agent_paper_candidate_batch(
                session,
                account_id=cycle.account_id,
                universe_snapshot_id=cycle.universe_snapshot_id,
                instrument_ids=securities[:2],
                idempotency_key="new-current-monitor-scope",
                created_at=scan_at,
            )
        settings = settings.model_copy(update={"agent_paper_candidate_batch_id": current.batch_id})
    old_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return scan_at
        return await old_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    args = parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)])
    first = await run(settings, args)
    assert first["reconciled_results"] == (3 if case == "invalid-one" else expected)
    assert first["model_requests"] == first["provider_requests"] == 0
    assert first["checks"] == (0 if rejected else expected)
    assert first["coverage_gap"] == (3 if rejected else 1 if case == "invalid-one" else 0)
    async with factory() as session:
        plans = tuple(await session.scalars(select(MonitorPlan)))
        receipts = tuple(await session.scalars(select(MonitorCoverageReceipt)))
        assert len(plans) == (0 if rejected else expected)
        if case == "invalid-one":
            unavailable = [r for r in receipts if r.status == "unverified"]
            assert len(unavailable) == 1 and unavailable[0].active_plan_id is None
            assert unavailable[0].quality_status is None and unavailable[0].output_id is None
        assert len(receipts) == (3 if case == "invalid-one" else expected)
        verified = [r for r in receipts if r.status != "unverified"]
        assert {r.status for r in verified} == ({"gap"} if rejected else {"installed"})
        assert {r.reason for r in verified} == (
            {"MONITOR_AUDIT_REJECTED"} if rejected else {"MONITOR_INSTALLED"}
        )
        assert {r.quality_status for r in verified} == {"complete"}
        ids = {r.id for r in receipts}
        assert set(await session.scalars(select(AgentResearchRunRecord.id))) == set(original_runs)
    repeated = await asyncio.gather(
        *(run(settings, parser().parse_args(["reconcile"])) for _ in range(2))
    )
    assert all(r["recorded"] == 0 for r in repeated)
    async with factory() as session:
        assert set(await session.scalars(select(MonitorCoverageReceipt.id))) == ids
        assert await session.scalar(
            select(func.count())
            .select_from(AuditEvent)
            .where(AuditEvent.action == "research.monitor.coverage_recorded")
        ) == (3 if case == "invalid-one" else expected)

    if case == "approved":
        from pathlib import Path

        from alembic import command
        from alembic.config import Config
        from helpers.migration_guard import historical_downgrade_guard
        from sqlalchemy import update
        from sqlalchemy.exc import DBAPIError

        async with factory() as session, session.begin():
            with pytest.raises(DBAPIError, match="MONITOR_FACT_IMMUTABLE"):
                async with session.begin_nested():
                    await session.execute(update(MonitorCoverageReceipt).values(reason="rewritten"))
        monkeypatch.setenv("DATABASE_URL", migrated_database_url)
        config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
        with pytest.raises(Exception, match="RESEARCH_STAGE_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(command.downgrade, config, "0089_monitor_evidence")
        with pytest.raises(Exception, match="MONITOR_COVERAGE_HISTORY_DOWNGRADE_FORBIDDEN"):
            await historical_downgrade_guard(migrated_database_url, "0090_monitor_coverage")
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["complete", "partial", "veto", "no-adoption", "unknown", "withdrawn-unknown"]
)
async def test_increment_completion_replaces_or_preserves_explicit_coverage(
    migrated_database_url, monkeypatch, mode
):
    import json
    from uuid import UUID, uuid4

    from integration.test_monitor_increment_pg import ready_trigger
    from nexus.agents import codex_staged
    from nexus.monitoring.reconciliation import plan_covered
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from nexus.persistence.models.monitoring import MonitorTrigger
    from nexus.worker import run_agent_worker_once

    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    first = await run(settings, parser().parse_args(["reconcile"]))
    assert first["installed"] == 3
    async with factory() as session:
        old = await session.scalar(
            select(MonitorPlan).where(MonitorPlan.source_run_id == parent.id)
        )
    admitted = await run(
        settings,
        parser().parse_args(
            [
                "admit",
                "--trigger-id",
                str(trigger.id),
                "--calendar-authority-id",
                str(CALENDAR_ID),
            ]
        ),
    )
    assert admitted["status"] == "admitted"
    unknown = mode in {"unknown", "withdrawn-unknown"}
    if mode == "withdrawn-unknown":
        # Controlled earlier withdrawal boundary: native current work is already
        # in flight when the prior refusal receipt becomes visible. Its source
        # validation is covered by the normal veto case below, not this seed.
        async with factory() as session, session.begin():
            values = {
                c.name: getattr(parent, c.name)
                for c in parent.__table__.columns
                if c.name not in {"id", "created_at", "decision_time", "idempotency_key"}
            }
            earlier = AgentResearchRunRecord(
                **values,
                id=uuid4(),
                decision_time=parent.decision_time + timedelta(minutes=1),
                idempotency_key="controlled-prior-withdrawal",
            )
            session.add(earlier)
            await session.flush()
            session.add(
                MonitorCoverageReceipt(
                    account_id=settings.agent_paper_account_id,
                    instrument_id=parent.instrument_id,
                    run_id=earlier.id,
                    output_id=None,
                    prior_plan_id=old.id,
                    active_plan_id=None,
                    research_as_of=earlier.decision_time,
                    observed_at=now,
                    status="gap",
                    reason="MONITOR_AUDIT_REJECTED",
                    quality_status="partial",
                )
            )
    native = codex_staged.invoke_codex
    calls = []

    async def model(payload, **kwargs):
        calls.append(payload["stage"])
        if unknown and payload["stage"] == "critique":
            raise TimeoutError("controlled unknown stage")
        raw, usage = await native(payload, **kwargs)
        value = json.loads(raw)
        if mode == "partial" and payload["stage"] == "synthesis":
            value["synthesis_assessment"]["review_conclusion"] = "limited"
            value["synthesis_assessment"]["uncertainties"] = [
                "Independent evidence remains incomplete."
            ]
        if mode == "veto" and payload["stage"] == "critique":
            for role in value["roles"]:
                if role["role"] == "bear":
                    role["monitoring_review"]["decision"] = "rejected"
        if mode == "no-adoption" and payload["stage"] == "synthesis":
            value["monitoring_adoption_hash"] = None
        return json.dumps(value), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", model)
    assert await run_agent_worker_once(
        settings, job_ids=(UUID(admitted["job_id"]),), formal_stage_capacity=True
    )
    async with factory() as session:
        now = await session.scalar(select(func.clock_timestamp()))
    if mode == "veto":
        async with factory() as session:
            # A newly finished rejection must block old-parent admission even
            # before the next automatic reconciliation has written its receipt.
            assert not await plan_covered(session, plan=old, now=now)
    final = await run(settings, parser().parse_args(["reconcile"]))
    assert final["recorded"] == 1
    expected = (
        "unknown"
        if unknown
        else "gap"
        if mode == "veto"
        else "retained"
        if mode == "no-adoption"
        else "installed"
    )
    async with factory() as session:
        receipt = await session.scalar(
            select(MonitorCoverageReceipt).where(
                MonitorCoverageReceipt.run_id == UUID(admitted["run_id"])
            )
        )
        assert receipt.status == expected, (receipt.status, receipt.reason)
        assert receipt.quality_status == (
            None if unknown else "partial" if mode == "partial" else "complete"
        )
        current = await session.scalar(
            select(MonitorPlan)
            .where(MonitorPlan.instrument_id == parent.instrument_id)
            .order_by(MonitorPlan.generation.desc())
            .limit(1)
        )
        assert current.generation == (2 if mode in {"complete", "partial"} else 1)
        if mode in {"veto", "withdrawn-unknown"}:
            assert receipt.active_plan_id is None
            assert not await plan_covered(session, plan=old, now=now)
        else:
            assert receipt.active_plan_id == current.id
            assert await plan_covered(session, plan=current, now=now)
        original_trigger = await session.get(MonitorTrigger, trigger.id)
        assert original_trigger.status == ("unknown" if unknown else "completed")
        budget = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == UUID(admitted["run_id"])
            )
        )
        assert budget.state == ("reserved" if unknown else "settled")
        assert (budget.settled_usd is None) is unknown
        trigger_count = await session.scalar(select(func.count()).select_from(MonitorTrigger))
    inspected = await run(settings, parser().parse_args(["inspect", "--plan-id", str(current.id)]))
    assert inspected["coverage"] == (
        "unavailable" if mode in {"veto", "withdrawn-unknown"} else "active"
    )
    assert inspected["research_quality"] == receipt.quality_status
    assert inspected["latest_result_state"] == receipt.status
    scanned = await run(
        settings, parser().parse_args(["scan", "--calendar-authority-id", str(CALENDAR_ID)])
    )
    assert scanned["reconciled_results"] == 0
    assert scanned["coverage_gap"] == (1 if mode in {"veto", "withdrawn-unknown"} else 0)
    async with factory() as session:
        # Same matched condition is not a new episode merely because a new result installed.
        assert (
            await session.scalar(select(func.count()).select_from(MonitorTrigger)) == trigger_count
        )
    assert calls == (["research", "critique"] if unknown else ["research", "critique", "synthesis"])


@pytest.mark.asyncio
@pytest.mark.parametrize("price_already_installed", [False, True, "receipt"])
async def test_normal_daily_reconcile_adopts_original_sec_plan_independently(
    migrated_database_url, monkeypatch, tmp_path, price_already_installed
):
    from helpers.disclosure_observations import reader_source
    from integration.test_candidate_multistock_path import _set_test_insert_clock
    from nexus.api.research_disclosure_monitoring import read_disclosure_monitoring_view
    from nexus.monitoring.batches import disclosure_source_current
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchOutputRecord,
    )
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.monitoring import (
        MonitorDailyEventReceipt,
        MonitorMixedEventReceipt,
    )
    from nexus.persistence.models.runtime import Job, OutboxEvent

    factory, settings, _, captured, pointer = await reader_source(
        migrated_database_url, monkeypatch, tmp_path
    )
    captured["clock"][0] = max(captured["clock"][0], AT + timedelta(seconds=6))
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory() as session:
        source = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        snapshot = await session.get(DataSnapshot, source.data_snapshot_id)
        paper = snapshot.payload["agent_paper_context"]
        assert paper.get("pending_monitoring_v2") is None
        assert paper["disclosure_monitoring"] is not None
        cycle = await session.get(AgentPaperCycleRecord, paper["cycle_id"])
        # Original native output and all cost/accounting facts stay frozen.
        frozen_query = select(AgentResearchOutputRecord.id, AgentResearchOutputRecord.content_hash)
        budget_query = select(
            AgentDailyBudgetReservationRecord.id,
            AgentDailyBudgetReservationRecord.state,
            AgentDailyBudgetReservationRecord.reserved_usd,
            AgentDailyBudgetReservationRecord.settled_usd,
        )
        before_outputs = set((await session.execute(frozen_query)).all())
        before_budget = set((await session.execute(budget_query)).all())
        before_counts = [
            await session.scalar(select(func.count()).select_from(model))
            for model in (Job, OutboxEvent, MonitorDailyEventReceipt, MonitorMixedEventReceipt)
        ]
    settings = settings.model_copy(
        update={
            "agent_paper_account_id": pointer.account_id,
            "agent_paper_candidate_batch_id": cycle.candidate_batch_id,
        }
    )
    if price_already_installed:
        installed = await run(
            settings,
            parser().parse_args(["install", "--output-id", str(pointer.source_output_id)]),
        )
        assert installed["plan_id"]
    saved_price_receipt = None
    if price_already_installed == "receipt":
        from nexus.monitoring.reconciliation import _reconcile_run

        async with factory() as session, session.begin():
            source = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            price_receipt = await _reconcile_run(
                session, account_id=pointer.account_id, run=source, now=captured["clock"][0]
            )
            saved_price_receipt = price_receipt.id
    try:
        result = await run(settings, parser().parse_args(["reconcile"]))
        assert result["model_requests"] == result["provider_requests"] == 0
        async with factory() as session:
            plans = tuple(
                await session.scalars(
                    select(MonitorPlan).where(MonitorPlan.source_run_id == pointer.source_run_id)
                )
            )
            assert {row.kind for row in plans} == {"price", "disclosure"}
            receipts = tuple(
                await session.scalars(
                    select(MonitorCoverageReceipt).where(
                        MonitorCoverageReceipt.run_id == pointer.source_run_id
                    )
                )
            )
            assert len(receipts) == 2
            assert {row.kind for row in receipts} == {"price", "disclosure"}
            assert all(row.status == "installed" for row in receipts)
            saved = {row.id for row in receipts}
            if saved_price_receipt is not None:
                assert saved_price_receipt in saved
            source = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            view = await read_disclosure_monitoring_view(session, source, now=captured["clock"][0])
            disclosure = next(row for row in plans if row.kind == "disclosure")
            assert view.state == "valid" and view.plan_id == disclosure.id
            assert await disclosure_source_current(session, disclosure, captured["clock"][0])
        import os
        from pathlib import Path

        browser_root = os.environ.get("NEXUS_TEST_MONITOR_BROWSER_DIR")
        if browser_root and price_already_installed == "receipt":
            import json

            import httpx
            from helpers.monitoring_readview_browser import verify_monitoring_browser
            from helpers.paper_api import app, headers

            monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: captured["clock"][0])
            application = app(migrated_database_url, now=captured["clock"][0])
            path = f"/internal/v1/research-runs/{pointer.source_run_id}"
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application), base_url="http://testserver"
            ) as client:
                response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200
            for field in ("monitoring", "disclosure_monitoring"):
                assert response.json()["data"][field]["state"] == "valid"
                assert response.json()["data"][field]["trigger_count"] == 0
            destination = Path(browser_root).resolve() / "ordinary-daily"
            destination.mkdir(parents=True, exist_ok=False)
            (destination / "ready.json").write_text(
                json.dumps(
                    {
                        "run_id": str(pointer.source_run_id),
                        "instrument_id": str(pointer.instrument_id),
                        "response": response.json(),
                    }
                )
            )
            await verify_monitoring_browser(
                application,
                directory=destination,
                spec_path="tests/e2e/monitoring-ordinary-daily-live.spec.ts",
            )
        repeated = await run(settings, parser().parse_args(["reconcile"]))
        assert repeated["recorded"] == 0
        async with factory() as session:
            assert saved == set(
                await session.scalars(
                    select(MonitorCoverageReceipt.id).where(
                        MonitorCoverageReceipt.run_id == pointer.source_run_id
                    )
                )
            )
            assert set((await session.execute(frozen_query)).all()) == before_outputs
            assert set((await session.execute(budget_query)).all()) == before_budget
            assert before_counts == [
                await session.scalar(select(func.count()).select_from(model))
                for model in (Job, OutboxEvent, MonitorDailyEventReceipt, MonitorMixedEventReceipt)
            ]
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["veto", "no-adoption", "unknown", "replacement-fail", "replacement-fail-legacy"]
)
async def test_new_ordinary_daily_sec_refusal_preserves_native_facts(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    import json

    from helpers.disclosure_observations import reader_source
    from integration.test_candidate_multistock_path import _set_test_insert_clock
    from integration.test_daily_v2_frozen_context_pg import new_daily
    from nexus.agents import codex_staged
    from nexus.monitoring.batches import disclosure_source_current
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchOutputRecord,
    )
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.monitoring import (
        MonitorDailyEventReceipt,
        MonitorDailyTriggerReceipt,
        MonitorMixedEventReceipt,
    )
    from nexus.persistence.models.runtime import AuditEvent, Job, OutboxEvent
    from nexus.settings import AgentRuntimeMode
    from nexus.worker import run_agent_worker_once
    from unit.agents.test_codex_sdk import _restore_view

    factory, settings, _, captured, pointer = await reader_source(
        migrated_database_url, monkeypatch, tmp_path
    )
    captured["clock"][0] = max(captured["clock"][0], AT + timedelta(seconds=6))
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory() as session:
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        snapshot = await session.get(DataSnapshot, parent.data_snapshot_id)
        cycle = await session.get(
            AgentPaperCycleRecord, snapshot.payload["agent_paper_context"]["cycle_id"]
        )
    settings = settings.model_copy(
        update={
            "agent_runtime_mode": AgentRuntimeMode.CODEX_SDK,
            "agent_paper_account_id": pointer.account_id,
            "agent_paper_candidate_batch_id": cycle.candidate_batch_id,
        }
    )
    await run(settings, parser().parse_args(["reconcile"]))
    async with factory() as session:
        prior = await session.scalar(
            select(MonitorPlan).where(
                MonitorPlan.source_run_id == pointer.source_run_id, MonitorPlan.kind == "disclosure"
            )
        )
        assert prior is not None
        old_source_hash = prior.source_hash
    if mode == "replacement-fail-legacy":
        from nexus.monitoring.observations import read_latest_price
        from nexus.monitoring.storage import record_check

        async with factory() as session, session.begin():
            price = await session.scalar(
                select(MonitorPlan).where(
                    MonitorPlan.source_run_id == pointer.source_run_id, MonitorPlan.kind == "price"
                )
            )
            check = await record_check(
                session,
                plan_id=price.id,
                scheduled_at=AT,
                checked_at=captured["clock"][0],
                observation=await read_latest_price(
                    session, instrument_id=pointer.instrument_id, now=captured["clock"][0]
                ),
            )
            assert check.result == "true"
    now, result = await new_daily(factory, settings, captured, pointer)
    async with factory() as session:
        target = await session.scalar(
            select(AgentResearchRunRecord).where(
                AgentResearchRunRecord.id.in_(result.research_run_ids),
                AgentResearchRunRecord.instrument_id == pointer.instrument_id,
            )
        )
        snapshot = await session.get(DataSnapshot, target.data_snapshot_id)
        assert snapshot.payload["agent_paper_context"].get("pending_monitoring_v2") is None
        job = await session.scalar(select(Job).where(Job.payload == {"run_id": str(target.id)}))
    native = codex_staged.invoke_codex
    seen = []
    consumed = []

    async def invoke(payload, **kwargs):
        wire = _restore_view(payload)
        stage = wire["stage"]
        seen.append(stage)
        if mode == "unknown" and stage == "critique":
            raise TimeoutError("controlled ordinary daily unknown")
        raw, usage = await native(payload, **kwargs)
        body = json.loads(raw)
        if mode == "veto" and stage == "critique":
            for role in body["roles"]:
                if role["role"] == "evidence_audit":
                    assert role["disclosure_review"] is not None
                    role["disclosure_review"]["decision"] = "rejected"
        if mode == "no-adoption" and stage == "synthesis":
            body["disclosure_adoption_hash"] = None
        if mode == "replacement-fail-legacy":
            frozen = snapshot.payload["agent_paper_context"]["pending_monitoring"]
            assert wire["daily_pending_context_hash"] is not None
            citation = next(
                row["index"]
                for row in wire["evidence"]
                if row["source"] == "nexus_agent_paper_context"
            )
            if stage == "research":
                for role in body["roles"]:
                    if role["role"] == "orchestrator":
                        role["daily_pending_proposal"] = {
                            "context_hash": wire["daily_pending_context_hash"],
                            "items": [
                                {
                                    "trigger_id": row["trigger_id"],
                                    "status": "addressed",
                                    "rationale": "逐项核对冻结触发与原始证据。",
                                    "citations": [citation],
                                }
                                for row in frozen["trigger_checks"]
                            ],
                        }
            elif stage == "critique":
                for role in body["roles"]:
                    role["daily_pending_review"] = {
                        "proposal_hash": wire["daily_pending_proposal"]["content_hash"],
                        "decision": "accepted",
                        "rationale": "精确核对每项冻结证据。",
                        "citations": [citation],
                    }
            else:
                body["daily_pending_adoption_hash"] = wire["daily_pending_proposal"]["content_hash"]
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    if mode.startswith("replacement-fail"):
        from nexus.monitoring import disclosure_installation

        install = disclosure_installation.install_disclosure_plan

        async def failed_install(*args, **kwargs):
            await install(*args, **kwargs)
            raise ValueError("MONITOR_DISCLOSURE_SOURCE_INVALID")

        monkeypatch.setattr(disclosure_installation, "install_disclosure_plan", failed_install)
        if mode == "replacement-fail-legacy":
            from nexus.monitoring import daily_consumption

            consume = daily_consumption.record_daily_consumption

            async def consumed_once(*args, **kwargs):
                count = await consume(*args, **kwargs)
                if kwargs["qualified"].run.id == target.id:
                    consumed.append(count)
                return count

            monkeypatch.setattr(daily_consumption, "record_daily_consumption", consumed_once)
    try:
        assert await run_agent_worker_once(settings, job_ids=(job.id,), formal_stage_capacity=True)
        assert seen == (
            ["research", "critique"] if mode == "unknown" else ["research", "critique", "synthesis"]
        )
        async with factory() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            budget_query = select(
                AgentDailyBudgetReservationRecord.id,
                AgentDailyBudgetReservationRecord.state,
                AgentDailyBudgetReservationRecord.reserved_usd,
                AgentDailyBudgetReservationRecord.settled_usd,
            )
            output_query = select(
                AgentResearchOutputRecord.id, AgentResearchOutputRecord.content_hash
            )
            audit_query = select(AuditEvent.id).where(
                AuditEvent.action.in_(
                    (
                        "research.monitor.plan_installed",
                        "research.monitor.disclosure_plan_installed",
                        "research.monitor.daily_trigger_covered",
                    )
                )
            )
            installation_audits = set(await session.scalars(audit_query))
            budgets = set((await session.execute(budget_query)).all())
            outputs = set((await session.execute(output_query)).all())
            unchanged_models = (
                Job,
                OutboxEvent,
                MonitorDailyEventReceipt,
                MonitorDailyTriggerReceipt,
                MonitorMixedEventReceipt,
            )
            counts = [
                await session.scalar(select(func.count()).select_from(model))
                for model in unchanged_models
            ]
        observed = await run(settings, parser().parse_args(["reconcile"]))
        assert observed["model_requests"] == observed["provider_requests"] == 0
        async with factory() as session:
            receipt = await session.scalar(
                select(MonitorCoverageReceipt).where(
                    MonitorCoverageReceipt.run_id == target.id,
                    MonitorCoverageReceipt.kind == "disclosure",
                )
            )
            assert receipt.status == (
                "unknown" if mode == "unknown" else "retained" if mode == "no-adoption" else "gap"
            )
            assert receipt.reason == (
                "MONITOR_CONDITION_REPLACEMENT_REJECTED"
                if mode.startswith("replacement-fail")
                else "MONITOR_UNKNOWN_USAGE"
                if mode == "unknown"
                else "MONITOR_AUDIT_REJECTED"
                if mode == "veto"
                else "MONITOR_PRIOR_EVIDENCE_UNCHANGED"
            )
            assert receipt.active_plan_id == (
                prior.id if mode in {"no-adoption", "unknown"} else None
            )
            if mode.startswith("replacement-fail"):
                assert not await session.scalar(
                    select(MonitorPlan.id).where(MonitorPlan.source_run_id == target.id)
                )
                paired = tuple(
                    await session.scalars(
                        select(MonitorCoverageReceipt).where(
                            MonitorCoverageReceipt.run_id == target.id
                        )
                    )
                )
                assert len(paired) == 2
                assert all(row.status == "gap" and row.active_plan_id is None for row in paired)
            assert await disclosure_source_current(session, prior, now) is (mode == "no-adoption")
            assert (await session.get(MonitorPlan, prior.id)).source_hash == old_source_hash
            assert not await session.scalar(
                select(MonitorPlan.id).where(
                    MonitorPlan.source_run_id == target.id, MonitorPlan.kind == "disclosure"
                )
            )
            if mode == "unknown":
                target = await session.get(AgentResearchRunRecord, target.id)
                reservation = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == target.id
                    )
                )
                # Unknown usage keeps the existing reservation; the accounting
                # state machine has no synthetic "unknown" reservation state.
                assert target.settled_cost_usd is None and reservation.state == "reserved"
                # This subscription profile prices USD at zero; preserve the
                # exact original reserve rather than inventing a positive fee.
                original_reserve = next(row for row in budgets if row[0] == reservation.id)
                assert (
                    reservation.id,
                    reservation.state,
                    reservation.reserved_usd,
                    reservation.settled_usd,
                ) == original_reserve
                assert reservation.pricing_version == target.pricing_version
                assert target.retry_count == 0
                assert reservation.settled_usd is None
            if mode.startswith("replacement-fail"):
                assert set(await session.scalars(audit_query)) == installation_audits
            if mode == "replacement-fail-legacy":
                assert consumed == [1]
            assert set((await session.execute(budget_query)).all()) == budgets
            assert set((await session.execute(output_query)).all()) == outputs
            assert counts == [
                await session.scalar(select(func.count()).select_from(model))
                for model in unchanged_models
            ]
        assert (await run(settings, parser().parse_args(["reconcile"])))["recorded"] == 0
    finally:
        await factory.kw["bind"].dispose()
