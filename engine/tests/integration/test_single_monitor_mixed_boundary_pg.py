"""A genuine mixed reader must carry the once root from its secondary parent."""

import importlib
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import pytest
from helpers.single_monitor_disclosure import (
    append_recent_capture,
    baseline_for_existing_identity,
    cached_stage_usage,
    disclosure_response,
    import_content_and_prepare,
)
from integration import test_single_sol_request_pg as legacy
from integration.test_single_monitor_increment_once_pg import (
    _approved_monitor_response,
    _canonical_market_scope,
    _canonical_market_tick,
    _set_clock,
)
from nexus.agents.platform_native import credential_for_preparation
from nexus.monitoring.batches import scan_stock_batch
from nexus.monitoring.mixed_admission import admit_batch
from nexus.monitoring.mixed_context import read_mixed_context
from nexus.ops import manual_paper, research_monitor
from nexus.ops.manual_paper_host import dispatch_pure_research_request
from nexus.paper.manual_request import read_manual_request
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.monitoring import MonitorPlan
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
async def test_secondary_only_v6_root_rejects_another_increment(
    migrated_database_url, tmp_path, monkeypatch
):
    factory, settings, store, ids, _, _ = await legacy.single_scope(
        migrated_database_url, tmp_path, monkeypatch
    )
    assert settings.agent_daily_budget_usd == Decimal("13")
    calendar = await _canonical_market_scope(factory, settings, store, ids[0])
    async with factory() as db:
        clock = [await db.scalar(text("SELECT clock_timestamp()"))]

    class FixtureTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0].astimezone(tz)

    for module in (
        "nexus.worker",
        "nexus.agents.snapshots",
        "nexus.research.current_identity_archive",
        "integration.test_research_identity_context",
        "nexus.ops.instrument_identity_local",
        "helpers.current_identity",
    ):
        monkeypatch.setattr(importlib.import_module(module), "datetime", FixtureTime)
    await _set_clock(factory, clock[0])
    factory.kw["info"] = {"sec_exhibit_store": store}
    for module in (
        "nexus.monitoring.disclosure_context",
        "nexus.monitoring.disclosure_observations",
        "nexus.monitoring.filing_archive",
        "nexus.monitoring.disclosure_forward",
    ):
        monkeypatch.setattr(module + ".configured_exhibit_store", lambda _: store)

    async def call(*args):
        return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

    async def output_for(request_id):
        async with factory() as db:
            view = await read_manual_request(db, await db.get(ManualPaperRequest, request_id))
            assert view.status == "completed", view
            member = next(m for m in view.research.members if m.instrument_id == ids[0])
            output = await db.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == member.run_id,
                    AgentResearchOutputRecord.role.is_(None),
                    AgentResearchOutputRecord.status == "completed",
                )
            )
            assert output is not None
            return output

    async def install(kind, output_id):
        result = await research_monitor.run(
            settings,
            research_monitor.parser().parse_args(
                ["install", "--kind", kind, "--output-id", str(output_id)]
            ),
        )
        assert result.get("plan_id"), result
        return UUID(result["plan_id"])

    # Keep the executor's anchored monotonic clock on the same controlled
    # logical instant as this disposable database and all archive producers.
    # Actual asyncio scheduling and timeout contexts remain the real ones.
    import asyncio

    from nexus.agents.research_stages import executor

    real_loop = asyncio.get_running_loop()
    fixed_monotonic = real_loop.time()

    class LogicalLoop:
        def time(self):
            return fixed_monotonic

        def __getattr__(self, name):
            return getattr(real_loop, name)

    class LogicalAsyncio:
        def get_running_loop(self):
            return LogicalLoop()

        def __getattr__(self, name):
            return getattr(asyncio, name)

    monkeypatch.setattr(executor, "asyncio", LogicalAsyncio())

    try:
        # Same legitimate decision cutoff: the independent newer disclosure root
        # does not supersede the original price plan (strictly later is required).
        scope = await baseline_for_existing_identity(
            factory,
            settings,
            store,
            ids[0],
            tmp_path / "sec-baseline",
            received_at=clock[0] - timedelta(seconds=1),
        )
        with monkeypatch.context() as cached:
            cached.setattr("integration.test_sol_native_stage_pg.usage", cached_stage_usage)
            old = await call(
                "start",
                "--mode",
                "research_and_monitor",
                "--idempotency-key",
                "mixed-baseline-three",
            )
            sent, _ = legacy.install_sol_http(
                cached, store, model_response=_approved_monitor_response
            )
            assert await dispatch_pure_research_request(
                settings, request_id=old.request_id, calendar_authority_id=calendar
            )
            assert len(sent) == 9
        # Preserve only the normal isolated original store after restoring the
        # baseline's synthetic usage composition; no further model send here.
        legacy.install_sol_http(monkeypatch, store, model_response=_approved_monitor_response)
        old_output = await output_for(old.request_id)
        offer = await call("inspect-single-monitor-sol-intent", "--instrument-id", str(ids[0]))
        root = await call(
            "start-single-monitor-sol",
            "--instrument-id",
            str(ids[0]),
            "--idempotency-key",
            "mixed-secondary-root",
            "--expected-intent-hash",
            offer.intent_hash,
        )

        def response(body, payload):
            if payload["stage"] == "research":
                _approved_monitor_response(body, payload)
            elif payload["stage"] == "critique":
                for role in body["roles"]:
                    role["vote"] = "support"
            else:
                assessment = body["synthesis_assessment"]
                for review in assessment["reviews"]:
                    review.update(
                        status="accepted",
                        resolved=True,
                        missing_evidence=[],
                        prevented_conclusion=None,
                        rationale="已核对冻结证据。",
                    )
                assessment.update(
                    review_conclusion="sufficient",
                    uncertainties=[],
                    claims=[{"text": "证据支持继续观察并复核披露。", "citations": [0]}],
                )
            disclosure_response(body, payload)

        sent, _ = legacy.install_sol_http(monkeypatch, store, model_response=response)
        assert await dispatch_pure_research_request(
            settings, request_id=root.request_id, calendar_authority_id=calendar
        )
        assert sent == ["research", "critique", "synthesis"]
        root_output = await output_for(root.request_id)
        # Install after the common research cutoff, so neither frozen request
        # gains a plan that did not exist when its predecessors were frozen.
        clock[0] += timedelta(seconds=1)
        await _set_clock(factory, clock[0])
        primary_id = await install("price", old_output.id)
        secondary_id = await install("disclosure", root_output.id)
        opening = datetime.combine(clock[0].date(), time(10), ZoneInfo("America/New_York"))
        clock[0] = max(opening.astimezone(UTC), clock[0] + timedelta(seconds=1))
        await _set_clock(factory, clock[0])
        await _canonical_market_tick(factory, settings, ids[0], clock[0], midpoint=Decimal(100))
        index_id = await append_recent_capture(
            factory, settings, store, scope, tmp_path / "sec-added", received_at=clock[0]
        )
        async with factory.begin() as db:
            scan = await scan_stock_batch(
                db, account_id=settings.agent_paper_account_id, instrument_id=ids[0], now=clock[0]
            )
            assert scan["new_triggers"] == 2, scan
        batch_id = UUID(scan["batch_id"])
        material_id = await import_content_and_prepare(
            factory,
            settings,
            store,
            scope,
            index_id,
            batch_id,
            tmp_path / "content",
            received_at=clock[0],
        )
        async with factory.begin() as db:
            first = await admit_batch(
                db,
                account_id=settings.agent_paper_account_id,
                batch_id=batch_id,
                materials_snapshot_id=material_id,
                candidate_batch_id=settings.agent_paper_candidate_batch_id,
                calendar_authority_id=calendar,
                model_config_id=settings.agent_paper_formal_model_config_id,
                daily_limit_usd=settings.agent_daily_budget_usd,
                daily_sdk_call_limit=settings.agent_monitor_daily_sdk_call_limit,
                platform_credential=credential_for_preparation(settings),
                now=clock[0],
            )
            assert first.plan_id == primary_id
            verified = await read_mixed_context(
                db, snapshot_id=first.snapshot_id, instrument_id=ids[0], as_of=clock[0]
            )
            assert verified is not None
            context, _ = verified
            parents = {p["kind"]: p for p in context.materials.parents}
            assert parents["price"]["plan_id"] == str(primary_id)
            assert parents["disclosure"]["plan_id"] == str(secondary_id)
            primary = await db.get(MonitorPlan, primary_id)
            assert (
                await db.scalar(
                    text("SELECT single_monitor_root_request(:r)"), {"r": primary.source_run_id}
                )
                is None
            )
            assert await db.scalar(
                text("SELECT single_monitor_increment_roots(:p,:s)"),
                {"p": primary_id, "s": first.snapshot_id},
            ) == [root.request_id]
            first_id, first_run = first.id, first.run_id

        async def custody():
            async with factory() as db:
                claim = (
                    await db.execute(
                        text("SELECT root_request_id,run_id FROM single_monitor_increment_claims")
                    )
                ).one()
                counts = tuple(
                    [
                        await db.scalar(text(f"SELECT count(*) FROM {t}"))
                        for t in (
                            "agent_research_runs",
                            "jobs",
                            "agent_daily_budget_reservations",
                            "research_monitor_increments",
                            "single_monitor_increment_claims",
                        )
                    ]
                )
                return claim, counts

        before = await custody()
        assert before[0] == (root.request_id, first_run)
        distinct_run = uuid4()
        async with factory() as db:
            assert not await db.scalar(
                text("SELECT EXISTS(SELECT FROM single_monitor_increment_claims WHERE run_id=:r)"),
                {"r": distinct_run},
            )
        # Component INSERT boundary: reuse the normally verified frozen material,
        # with a fresh distinct Run identity. The original BEFORE fence must fire
        # before any duplicate member/FK constraint, with its exact SQLSTATE.
        with pytest.raises(DBAPIError, match="SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED") as error:
            async with factory.begin() as db:
                await db.execute(
                    text("""
                    INSERT INTO research_monitor_increments
                    (id,batch_id,account_id,instrument_id,trigger_id,plan_id,run_id,job_id,
                     snapshot_id,research_as_of,session_date,reserved_sdk_calls,
                     daily_sdk_call_limit,context_hash,manifest_hash)
                    SELECT :new_id,batch_id,account_id,instrument_id,trigger_id,plan_id,
                           :new_run,job_id,snapshot_id,research_as_of,session_date,
                           reserved_sdk_calls,daily_sdk_call_limit,context_hash,manifest_hash
                    FROM research_monitor_increments WHERE id=:first
                """),
                    {"new_id": uuid4(), "new_run": distinct_run, "first": first_id},
                )
        assert error.value.orig.sqlstate == "23514"
        assert await custody() == before
    finally:
        await factory.kw["bind"].dispose()
