"""Controlled clock and source archive; real DB/producer/admission, no model calls."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from helpers.paper_batch import CALENDAR_ID, INSTRUMENTS
from nexus.paper.economic_pair import register_economic_identity
from nexus.paper.economic_version_binding import bind_versions
from nexus.paper.portfolio_economic_preflight import EconomicPlanRequest
from nexus.paper.portfolio_economic_protocol import register_protocol
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.economic import EconomicLaneCycleRecord, EconomicPairSlotRecord
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.runtime import Job
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from engine.tests.integration.test_economic_authorities_pg import seed_pair
from engine.tests.integration.test_portfolio_same_day_research_pg import seed_normal_path

SLOT = datetime(2026, 10, 9, 14, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "complete_synthesis,normal_resume",
    [(False, False), (True, False), ("unknown", False), (True, True), ("unknown", True)],
)
async def test_normal_pair_producer_binds_four_research_jobs_and_exact_budget(
    migrated_database_url,
    monkeypatch,
    tmp_path,
    complete_synthesis,
    normal_resume,
    runtime_hold=False,
    multi_day=False,
    window_test=False,
    collection_test=False,
    execution_status_test=False,
    api_read_test=False,
):
    from nexus.paper.economic_cycle import start_economic_pair

    # Only the clock transport is controlled. No trigger disabling or business
    # history edits: source fixtures are constructed once at their own cutoff.
    clock = [SLOT - timedelta(minutes=10)]
    original = AsyncSession.scalar

    async def scalar(self, statement, *args, **kwargs):
        if str(statement).strip() == "SELECT clock_timestamp() AS clock_timestamp_1":
            return clock[0]
        return await original(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", scalar)
    engine, sessions, _, _ = await seed_normal_path(
        migrated_database_url, monkeypatch, decision_time=SLOT
    )
    # API/browser fixtures need actual creation chronology. The shared PG fixture
    # defaults created_at to January for legacy fixed-clock tests; stamp new ORM
    # facts before INSERT, never edit an immutable saved fact after creation.
    from sqlalchemy import event
    from sqlalchemy.orm import Session

    def stamp_api_fact_created_at(session, flush_context, instances):
        for record in session.new:
            if hasattr(record, "created_at") and record.created_at is None:
                record.created_at = clock[0]

    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.url import make_url

    expected_api_database = make_url(migrated_database_url).database

    def set_api_sql_clock(connection, cursor, statement, parameters, context, executemany):
        if (
            connection.engine.url.database == expected_api_database
            and statement.lstrip().upper().startswith("INSERT ")
        ):
            cursor.execute(
                "SELECT set_config('nexus.economic_test_clock', $1, false)", (clock[0].isoformat(),)
            )

    if api_read_test:
        event.listen(Session, "before_flush", stamp_api_fact_created_at)
        event.listen(Engine, "before_cursor_execute", set_api_sql_clock)
    try:
        async with sessions.begin() as session:
            actual = await session.scalar(text("SELECT current_database()"))
            if api_read_test:
                # Control defaults for new Core/ORM INSERTs as well as the SDK clock.
                # No previously stored creation times are edited.
                await session.execute(
                    text(
                        "DO $$ DECLARE r record; BEGIN FOR r IN SELECT table_name "
                        "FROM information_schema.columns "
                        "WHERE table_schema='public' AND column_name='created_at' "
                        "LOOP EXECUTE format("
                        "'ALTER TABLE %I ALTER COLUMN created_at SET DEFAULT "
                        "coalesce(nullif("
                        "current_setting(''nexus.economic_test_clock'', true), ''''), "
                        "''2026-01-01T00:00:00+00:00'')::timestamptz', "
                        "r.table_name); END LOOP; END $$"
                    )
                )
            for offset in (3, 4):
                next_day = SLOT + timedelta(days=offset)
                session.add(
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=CALENDAR_ID,
                        session=next_day.date(),
                        decision_at=next_day.replace(hour=21),
                        market_open_at=next_day.replace(hour=13, minute=30),
                        market_close_at=next_day.replace(hour=20),
                    )
                )
            await session.flush()
            await seed_pair(
                session,
                False,
                pricing_days=25 / 1440
                if collection_test == "pair_followup_expired"
                else 7
                if multi_day == "valid"
                or collection_test
                in {
                    "pair_plan_sale",
                    "pair_stage_one_not_required",
                    "pair_stage_both_not_required",
                    "pair_stage_one_rejected",
                    "pair_stage_both_rejected",
                    "pair_plan_rejected_buy",
                    "pair_plan_execute",
                    "pair_plan_partial",
                    "pair_plan_missed",
                }
                else 1,
            )
            protocol = await register_protocol(
                session,
                EconomicPlanRequest(
                    evaluation_key="normal-economic-pair",
                    instruments=tuple(
                        [
                            {
                                "instrument_id": item[0],
                                "symbol": await session.scalar(
                                    select(InstrumentAlias.symbol).where(
                                        InstrumentAlias.instrument_id == item[0],
                                        InstrumentAlias.valid_to.is_(None),
                                    )
                                ),
                            }
                            for item in INSTRUMENTS[:2]
                        ]
                    ),
                    start_session=SLOT.date(),
                    research_cost_usd="0",
                    synthesis_cost_usd="0",
                    global_daily_limit_usd="20",
                ),
                expected_database=actual,
            )
            binding = await bind_versions(
                session,
                protocol_hash=protocol.payload["content_hash"],
                proposal_hash="b" * 64,
                expected_database=actual,
            )
            identity = await register_economic_identity(
                session,
                protocol_hash=protocol.payload["content_hash"],
                version_binding_hash=binding.payload["content_hash"],
                expected_database=actual,
            )
            pair = await session.scalar(
                select(EconomicPairSlotRecord).where(
                    EconomicPairSlotRecord.evaluation_id == identity.id,
                    EconomicPairSlotRecord.ordinal == 1,
                )
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="ECONOMIC_PAIR_SLOT_NOT_DUE"):
                await start_economic_pair(session, pair_slot_id=pair.id, expected_database=actual)
            assert await session.scalar(select(func.count()).select_from(Job)) == 0
        clock[0] = SLOT + timedelta(minutes=5)
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="ECONOMIC_PAIR_SLOT_MISSED"):
                await start_economic_pair(session, pair_slot_id=pair.id, expected_database=actual)
            assert await session.scalar(select(func.count()).select_from(Job)) == 0
        clock[0] = SLOT + timedelta(seconds=1)
        if runtime_hold:
            await verify_preheld_start_isolation(sessions, pair, identity, actual, clock[0])
        # Catching the second lane failure may not commit the first lane or its hold.
        import nexus.paper.agent_cycle as native

        original_start = native._start_cycle
        calls = [0]

        async def fail_second(*args, **kwargs):
            calls[0] += 1
            if calls[0] == 2:
                raise ValueError("TEST_SECOND_LANE_FAILED")
            return await original_start(*args, **kwargs)

        with monkeypatch.context() as scoped:
            scoped.setattr(native, "_start_cycle", fail_second)
            async with sessions.begin() as session:
                with pytest.raises(ValueError, match="TEST_SECOND_LANE_FAILED"):
                    await start_economic_pair(
                        session, pair_slot_id=pair.id, expected_database=actual
                    )
                for table in (
                    Job,
                    AgentResearchRunRecord,
                    EconomicLaneCycleRecord,
                    AgentDailyBudgetReservationRecord,
                ):
                    assert await session.scalar(select(func.count()).select_from(table)) == 0
                assert (
                    await session.scalar(
                        text(
                            "SELECT count(*) FROM audit_events "
                            "WHERE action='portfolio.economic.budget_held'"
                        )
                    )
                    == 0
                )
        async with sessions.begin() as session:
            result = await start_economic_pair(
                session, pair_slot_id=pair.id, expected_database=actual
            )
            assert result.status == "researching" and len(result.cycle_ids) == 2
            if window_test:
                from nexus.paper.economic_window import freeze_common_window

                with pytest.raises(ValueError, match="ECONOMIC_WINDOW_NOT_READY"):
                    await freeze_common_window(session, pair_slot_id=pair.id, window_seq=1)
            links = tuple(
                await session.scalars(
                    select(EconomicLaneCycleRecord).where(
                        EconomicLaneCycleRecord.pair_slot_id == pair.id
                    )
                )
            )
            assert len(links) == 2 and {link.lane for link in links} == {"incumbent", "challenger"}
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            jobs = tuple(await session.scalars(select(Job)))
            reservations = tuple(await session.scalars(select(AgentDailyBudgetReservationRecord)))
            assert len(runs) == len(jobs) == len(reservations) == 4
            assert all(
                run.model_config_id == binding.payload["shared_model_id"]
                or str(run.model_config_id) == binding.payload["shared_model_id"]
                for run in runs
            )
            from nexus.agents.paper_context import read_agent_paper_research_context
            from nexus.jobs.daily_research import admit_agent_paper_research

            first = runs[0]
            context = await read_agent_paper_research_context(
                session,
                snapshot_id=first.data_snapshot_id,
                instrument_id=first.instrument_id,
                as_of=SLOT,
            )
            assert context is not None
            other = next(link.cycle_id for link in links if link.cycle_id != context[0].cycle_id)
            with pytest.raises(ValueError, match="ECONOMIC_PAIR_RESEARCH_SNAPSHOT_INVALID"):
                await admit_agent_paper_research(
                    session,
                    cycle_id=other,
                    instrument_id=first.instrument_id,
                    research_snapshot_id=first.data_snapshot_id,
                    research_as_of=SLOT,
                    daily_limit_usd=Decimal(20),
                    now=clock[0],
                )
            from nexus.paper.economic_budget_hold import (
                _require_new_transfer_authority,
                decode_hold,
            )
            from nexus.paper.economic_cycle import read_economic_cycle, research_allocation
            from nexus.persistence.models.runtime import AuditEvent

            owner = await read_economic_cycle(session, cycle_id=context[0].cycle_id)
            assert owner is not None
            allocation = await research_allocation(
                session,
                ref=owner[0],
                authority=owner[1],
                instrument_id=first.instrument_id,
            )
            held_event = await session.get(AuditEvent, allocation.hold_event_id)
            wrong_lane = "challenger" if owner[0].lane == "incumbent" else "incumbent"
            with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_LANE_AUTHORITY_INVALID"):
                await _require_new_transfer_authority(
                    session,
                    hold=decode_hold(held_event),
                    allocation=allocation.model_copy(update={"lane": wrong_lane}),
                    run=first,
                    now=clock[0],
                )
            assert all(job.max_attempts == 1 for job in jobs)
            assert all(row.reserved_usd == Decimal(0) for row in reservations)
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='portfolio.economic.budget_transferred'"
                    )
                )
                == 4
            )
        async with sessions.begin() as session:
            repeated = await start_economic_pair(
                session, pair_slot_id=pair.id, expected_database=actual
            )
            assert repeated.cycle_ids == result.cycle_ids and repeated.replayed
            assert await session.scalar(select(func.count()).select_from(Job)) == 4
        if normal_resume:
            from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
            from sqlalchemy import event

            # Single-lane native callers must use the same account-first lock order
            # as the pair coordinator, including before claiming a resume key.
            def observe_claim(conn, cursor, statement, parameters, context, executemany):
                if "from idempotency_records" in statement.lower():
                    held = conn.exec_driver_sql(
                        "SELECT count(*) FROM pg_locks "
                        "WHERE locktype='advisory' AND pid=pg_backend_pid()"
                    ).scalar_one()
                    assert held >= 2, "economic native resume claimed before account locks"

            event.listen(engine.sync_engine, "before_cursor_execute", observe_claim)
            try:
                async with sessions.begin() as session:
                    await resume_schwab_agent_paper_cycle(
                        session,
                        cycle_id=result.cycle_ids[0],
                        idempotency_key="economic-single-lane-pending",
                        now=clock[0],
                        daily_limit_usd=Decimal(20),
                    )
            finally:
                event.remove(engine.sync_engine, "before_cursor_execute", observe_claim)
            import nexus.paper.economic_cycle as economic

            assert callable(getattr(economic, "resume_economic_pair", None)), (
                "normal pair resume missing"
            )
            async with sessions.begin() as session:
                waiting = await economic.resume_economic_pair(
                    session, pair_slot_id=pair.id, expected_database=actual
                )
                assert waiting.status == "researching" and waiting.replayed
                assert await session.scalar(select(func.count()).select_from(Job)) == 4
        if complete_synthesis:
            await complete_controlled_pair(
                sessions,
                migrated_database_url,
                monkeypatch,
                clock,
                result.cycle_ids,
                binding,
                complete_synthesis,
                pair.id if normal_resume else None,
                actual,
                staggered=bool(window_test),
            )
        # The real console reads the already persisted pair after the historical slot;
        # no clock override enters the product CLI and no provider/model is called.
        import asyncio
        import json
        import subprocess
        import sys
        from pathlib import Path

        config = tmp_path / "database-url"
        config.write_text(migrated_database_url)
        config.chmod(0o600)
        completed = await asyncio.to_thread(
            subprocess.run,
            [
                str(Path(sys.executable).with_name("nexus-engine-economic-cycle")),
                "resume" if normal_resume else "start",
                "--database-url-file",
                str(config),
                "--expected-database",
                actual,
                "--pair-slot-id",
                str(pair.id),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert completed.returncode == 0 and not completed.stderr
        receipt = json.loads(completed.stdout)
        assert receipt["replayed"] and receipt["execution_locked"]
        assert set(receipt["cycle_ids"]) == {str(item) for item in result.cycle_ids}
        async with sessions.begin() as session:
            assert await session.scalar(select(func.count()).select_from(Job)) == (
                6 if complete_synthesis else 4
            )
        if execution_status_test:
            from engine.tests.integration.test_economic_no_trade_pg import verify_no_trade_status

            await verify_no_trade_status(
                sessions, clock, pair.id, execution_status_test, config, actual
            )
        if window_test:
            await verify_common_window(sessions, clock, pair.id, window_test, monkeypatch)
            if collection_test:
                from engine.tests.integration.test_economic_collection_pg import (
                    verify_shared_collection,
                )

                await verify_shared_collection(
                    sessions, clock, pair.id, collection_test, monkeypatch, tmp_path
                )
                if collection_test in {
                    "pair_plan_sale",
                    "pair_stage_one_not_required",
                    "pair_stage_both_not_required",
                    "pair_stage_one_rejected",
                    "pair_stage_both_rejected",
                    "pair_plan_rejected_buy",
                    "pair_plan_execute",
                    "pair_plan_partial",
                    "pair_plan_missed",
                }:
                    from engine.tests.integration.test_portfolio_plan_continuation_pg import (
                        verify_sale_continuation,
                    )

                    await verify_sale_continuation(
                        sessions,
                        migrated_database_url,
                        monkeypatch,
                        clock,
                        identity,
                        binding,
                        actual,
                        tmp_path,
                        rejected_buy=collection_test == "pair_plan_rejected_buy",
                        execute_plan=collection_test
                        in {"pair_plan_execute", "pair_plan_partial", "pair_plan_missed"},
                        partial_plan=collection_test == "pair_plan_partial",
                        late_plan=collection_test == "pair_plan_missed",
                        source_before_slot=bool(api_read_test),
                        boundary_case=collection_test.removeprefix("pair_stage_")
                        if collection_test.startswith("pair_stage_")
                        else "",
                    )
            await verify_freeze_cli(config, actual, pair.id)
            if window_test == "frozen":
                await verify_window_downgrade(
                    config,
                    monkeypatch,
                    expected_error="PORTFOLIO_PLAN_DOWNGRADE_FORBIDDEN"
                    if collection_test == "pair_stage_both_rejected"
                    else "ECONOMIC_WINDOW_DOWNGRADE_FORBIDDEN",
                )
        if api_read_test:
            from engine.tests.integration.test_economic_pair_api_pg import verify_pair_api

            await verify_pair_api(sessions, clock, pair.id, migrated_database_url, api_read_test)
        if multi_day:
            await verify_second_day_pair(
                sessions,
                migrated_database_url,
                monkeypatch,
                clock,
                identity,
                binding,
                actual,
                multi_day,
            )
        if runtime_hold:
            await verify_registered_multi_day_hold(
                sessions, monkeypatch, clock, identity, actual, complete_synthesis, runtime_hold
            )
    finally:
        if api_read_test:
            event.remove(Session, "before_flush", stamp_api_fact_created_at)
            event.remove(Engine, "before_cursor_execute", set_api_sql_clock)
        await engine.dispose()


async def complete_controlled_pair(
    sessions,
    url,
    monkeypatch,
    clock,
    cycle_ids,
    binding,
    outcome,
    pair_slot_id=None,
    expected_database=None,
    decision_time=SLOT,
    staggered=False,
    synthesis_variant="",
):
    import json

    from nexus import worker
    from nexus.agents import codex_staged
    from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.agents import (
        AgentPortfolioSynthesisRunRecord,
        AgentResearchOutputRecord,
    )
    from nexus.persistence.models.paper import (
        AgentPaperCycleRecord,
        PaperOrderRecord,
        PaperTargetVersionRecord,
    )
    from nexus.settings import EngineSettings

    async with sessions() as session:
        base_jobs = await session.scalar(select(func.count()).select_from(Job)) - 4
        base_synthesis = await session.scalar(
            select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
        )
        base_transfer = (
            await session.scalar(
                text(
                    "SELECT count(*) FROM audit_events "
                    "WHERE action='portfolio.economic.budget_transferred'"
                )
            )
            - 4
        )

    async with sessions() as session:
        base_orders = await session.scalar(select(func.count()).select_from(PaperOrderRecord))

    class ControlledDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0] if tz else clock[0].replace(tzinfo=None)

    monkeypatch.setattr(worker, "datetime", ControlledDateTime)
    monkeypatch.setattr(worker, "_ONCE_STARTED_AT", SLOT)
    calls = []
    current_instrument = None
    current_lane = None

    async def invoke(payload, **options):
        calls.append(payload["stage"])
        if payload["stage"] == "synthesis":
            response = {
                "synthesis_assessment": {
                    "stance": "reduce_candidate"
                    if outcome == "rotate"
                    and (
                        current_instrument == INSTRUMENTS[0][0]
                        or synthesis_variant == "both_not_required"
                        or (
                            synthesis_variant == "one_not_required" and current_lane == "challenger"
                        )
                    )
                    else "buy_candidate"
                    if outcome in {"trade", "mixed", "rotate"}
                    else "watch",
                    "claims": [{"text": "受控证据只支持继续观察。", "citations": [0]}],
                    "reviews": [
                        {
                            "claim_id": item,
                            "status": "accepted",
                            "rationale": "仅用于受控研究链验证。",
                            "citations": [0],
                            "resolved": True,
                            "missing_evidence": [],
                            "prevented_conclusion": None,
                        }
                        for item in ["bear:0", "evidence_audit:0"]
                    ],
                    "review_conclusion": "sufficient"
                    if outcome in {"trade", "mixed", "rotate"}
                    else "limited",
                    "uncertainties": ["不据此证明策略有效。"],
                    "conflicting_facts": False,
                }
            }
        else:
            response = {
                "roles": [
                    {
                        "role": prompt["role"],
                        "vote": "support" if outcome in {"trade", "mixed", "rotate"} else "abstain",
                        "claims": [{"text": "现有冻结证据支持观察。", "citations": [0]}],
                        "conflicting_facts": False,
                        "reviewed_claim_ids": [] if payload["stage"] == "research" else ["bull:0"],
                    }
                    for prompt in payload["prompts"]
                ]
            }
        return json.dumps(response), UsageSnapshot(input_tokens=10, output_tokens=20)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    settings = EngineSettings(database_url=url, agent_runtime_mode="codex_sdk")
    async with sessions() as session:
        jobs = tuple(await session.scalars(select(Job.id).where(Job.effective_at == decision_time)))
    clock[0] = decision_time + timedelta(minutes=1)
    for job_id in jobs:
        if outcome == "rotate":
            from uuid import UUID

            async with sessions() as session:
                job = await session.get(Job, job_id)
                research_run = await session.get(
                    AgentResearchRunRecord, UUID(job.payload["run_id"])
                )
                current_instrument = research_run.instrument_id
                current_lane = await session.scalar(
                    select(EconomicLaneCycleRecord.lane).where(
                        EconomicLaneCycleRecord.cycle_id
                        == next(
                            cycle_id
                            for cycle_id in cycle_ids
                            if research_run.idempotency_key.startswith(f"agent-paper:{cycle_id}:")
                        )
                    )
                )
        assert await worker.run_agent_worker_once(settings, job_ids=(job_id,))
        assert not await worker.run_agent_worker_once(settings, job_ids=(job_id,))
    if pair_slot_id is not None:
        from nexus.ops.agent_paper_once import AgentPaperResumeRequest, resume_once

        legacy = await resume_once(
            settings.model_copy(update={"agent_daily_budget_usd": Decimal(1)}),
            AgentPaperResumeRequest(
                action="resume",
                cycle_id=cycle_ids[0],
                idempotency_key=f"legacy-economic-resume:{cycle_ids[0]}",
            ),
        )
        assert (
            legacy.status == "researching" and legacy.result_code == "PORTFOLIO_SYNTHESIS_PENDING"
        )
    async with sessions.begin() as session:
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.decision_time == decision_time
                )
            )
        )
        assert len(runs) == 4 and all(run.status == "completed" for run in runs)
        assert len(calls) == 12
        if pair_slot_id is not None:
            import nexus.paper.agent_cycle as native
            from nexus.paper.economic_cycle import resume_economic_pair

            original_resume = native.resume_schwab_agent_paper_cycle
            attempts = [0]

            async def reject_second(*args, **kwargs):
                attempts[0] += 1
                if attempts[0] == 2:
                    raise ValueError("TEST_SECOND_RESUME_FAILED")
                return await original_resume(*args, **kwargs)

            with monkeypatch.context() as scoped:
                scoped.setattr(native, "resume_schwab_agent_paper_cycle", reject_second)
                with pytest.raises(ValueError, match="TEST_SECOND_RESUME_FAILED"):
                    await resume_economic_pair(
                        session, pair_slot_id=pair_slot_id, expected_database=expected_database
                    )
                assert await session.scalar(select(func.count()).select_from(Job)) == base_jobs + 5
                assert (
                    await session.scalar(
                        select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
                    )
                    == base_synthesis + 1
                )
            admitted_pair = await resume_economic_pair(
                session, pair_slot_id=pair_slot_id, expected_database=expected_database
            )
            assert admitted_pair.status == "researching" and not admitted_pair.replayed
            assert await session.scalar(select(func.count()).select_from(Job)) == base_jobs + 6
            repeated_pair = await resume_economic_pair(
                session, pair_slot_id=pair_slot_id, expected_database=expected_database
            )
            assert repeated_pair.replayed
            assert await session.scalar(select(func.count()).select_from(Job)) == base_jobs + 6
        for cycle_id in cycle_ids:
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
            outputs = tuple(
                await session.scalars(
                    select(AgentResearchOutputRecord.id)
                    .join(
                        AgentResearchRunRecord,
                        AgentResearchOutputRecord.run_id == AgentResearchRunRecord.id,
                    )
                    .where(
                        AgentResearchOutputRecord.role.is_(None),
                        AgentResearchOutputRecord.status == "completed",
                        AgentResearchRunRecord.idempotency_key.like(f"agent-paper:{cycle_id}:%"),
                    )
                )
            )
            projection = await load_paper_projection(
                session, account_id=cycle.account_id, as_of=decision_time
            )
            target = await session.scalar(
                select(PaperTargetVersionRecord.id).where(
                    PaperTargetVersionRecord.account_id == cycle.account_id
                )
            )
            before_jobs = await session.scalar(select(func.count()).select_from(Job))
            with pytest.raises(ValueError, match="ECONOMIC_PAIR_SYNTHESIS_ARGUMENT_INVALID"):
                await admit_portfolio_synthesis(
                    session,
                    cycle_id=cycle_id,
                    research_output_ids=outputs,
                    active_target_version_id=target,
                    projection_hash=projection.projection_hash,
                    daily_limit_usd=Decimal(19),
                    now=clock[0],
                )
            assert await session.scalar(select(func.count()).select_from(Job)) == before_jobs
            admitted = await admit_portfolio_synthesis(
                session,
                cycle_id=cycle_id,
                research_output_ids=outputs,
                active_target_version_id=target,
                projection_hash=projection.projection_hash,
                daily_limit_usd=Decimal(20),
                now=clock[0],
            )
            lane = await session.scalar(
                select(EconomicLaneCycleRecord.lane).where(
                    EconomicLaneCycleRecord.cycle_id == cycle_id
                )
            )
            expected = binding.payload[f"{lane}_prompt_id"]
            assert str(admitted.run.prompt_version_id) == expected
            repeated = await admit_portfolio_synthesis(
                session,
                cycle_id=cycle_id,
                research_output_ids=outputs,
                active_target_version_id=target,
                projection_hash=projection.projection_hash,
                daily_limit_usd=Decimal(20),
                now=clock[0],
            )
            assert repeated.replayed and repeated.run.id == admitted.run.id
            assert await session.scalar(select(func.count()).select_from(Job)) == before_jobs + (
                0 if pair_slot_id is not None else 1
            )
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM audit_events "
                    "WHERE action='portfolio.economic.budget_transferred'"
                )
            )
            == base_transfer + 6
        )
        assert (
            await session.scalar(select(func.count()).select_from(AgentPortfolioSynthesisRunRecord))
            == base_synthesis + 2
        )

    from nexus.agents.codex_sdk import CodexInvocationError
    from nexus.agents.portfolio_synthesis_worker import (
        CodexSynthesisTransport,
        run_synthesis_job_once,
    )
    from nexus.persistence.models.agents import (
        AgentModelConfigRecord,
        AgentPortfolioSynthesisOutputRecord,
    )

    from engine.tests.integration.test_portfolio_synthesis_persistence_pg import synthesis_intent

    responses = {}
    instructions = {}
    async with sessions() as session:
        synthesis_runs = tuple(
            await session.scalars(
                select(AgentPortfolioSynthesisRunRecord).where(
                    AgentPortfolioSynthesisRunRecord.cycle_id.in_(cycle_ids)
                )
            )
        )
        for run in synthesis_runs:
            lane = await session.scalar(
                select(EconomicLaneCycleRecord.lane).where(
                    EconomicLaneCycleRecord.cycle_id == run.cycle_id
                )
            )
            if outcome == "rotate":
                from nexus.agents.portfolio_synthesis_contracts import PortfolioSynthesisIntentV1

                from engine.tests.integration.test_portfolio_management_e2e import _include_two

                document = _include_two(run).model_dump(mode="json")
                document["choices"][0].update(action="exit", role="none", rank=None)
                document["choices"][1].update(action="increase", role="tactical", rank=1)
                if synthesis_variant == "both_not_required" or (
                    synthesis_variant == "one_not_required" and lane == "challenger"
                ):
                    document["choices"][1].update(action="exit", role="none", rank=None)

                responses[run.input_hash] = PortfolioSynthesisIntentV1.model_validate(
                    document
                ).model_dump_json()
            elif outcome in {"trade", "rotate"} or (
                outcome == "mixed" and run.cycle_id == cycle_ids[0]
            ):
                from engine.tests.integration.test_portfolio_management_e2e import _include_two

                responses[run.input_hash] = _include_two(run).model_dump_json()
            else:
                responses[run.input_hash] = synthesis_intent(run).model_dump_json()
            lane = await session.scalar(
                select(EconomicLaneCycleRecord.lane).where(
                    EconomicLaneCycleRecord.cycle_id == run.cycle_id
                )
            )
            instructions[run.input_hash] = lane
            assert CodexSynthesisTransport().supports(
                await session.get(AgentModelConfigRecord, run.model_config_id)
            )
        synthesis_jobs = tuple(
            await session.scalars(
                select(Job.id).where(
                    Job.job_type == "agent.portfolio_synthesis", Job.effective_at == decision_time
                )
            )
        )
    sent = []

    class Transport:
        def supports(self, model):
            return CodexSynthesisTransport().supports(model)

        async def invoke(self, payload):
            sent.append(payload["input_hash"])
            assert payload["instructions"] == instructions[payload["input_hash"]]
            if outcome == "unknown":
                raise CodexInvocationError("CODEX_TIMEOUT", phase="turn_run")
            return responses[payload["input_hash"]], UsageSnapshot(
                input_tokens=10, output_tokens=20
            )

    clock[0] = decision_time + timedelta(minutes=2)
    for index, job_id in enumerate(synthesis_jobs):
        if staggered:
            clock[0] = decision_time + timedelta(minutes=2 + index * 2)
        options = dict(
            transport=Transport(),
            worker_id="economic-synthesis-test",
            clock=lambda: clock[0],
            job_ids=(job_id,),
        )
        assert await run_synthesis_job_once(sessions, **options)
        assert not await run_synthesis_job_once(sessions, **options)
        if staggered:
            from nexus.ops.agent_paper_once import AgentPaperResumeRequest, resume_once

            async with sessions() as session:
                job = await session.get(Job, job_id)
                from uuid import UUID

                run = await session.get(
                    AgentPortfolioSynthesisRunRecord, UUID(job.payload["run_id"])
                )
                cycle_id = run.cycle_id
            result = await resume_once(
                settings,
                AgentPaperResumeRequest(
                    action="resume",
                    cycle_id=cycle_id,
                    idempotency_key=f"staggered-economic-resume:{cycle_id}",
                ),
            )
            assert result.status in {"awaiting_market", "completed"}
    async with sessions() as session:
        terminal = tuple(
            await session.scalars(
                select(AgentPortfolioSynthesisRunRecord).where(
                    AgentPortfolioSynthesisRunRecord.cycle_id.in_(cycle_ids)
                )
            )
        )
        assert all(
            run.status == ("failed" if outcome == "unknown" else "completed") for run in terminal
        )
        if outcome == "unknown":
            from nexus.persistence.models.agents import (
                AgentPortfolioSynthesisBudgetReservationRecord,
            )

            held = tuple(
                await session.scalars(select(AgentPortfolioSynthesisBudgetReservationRecord))
            )
            assert len(held) == 2 and all(row.state == "reserved" for row in held)
            assert all(run.settled_cost_usd is None for run in terminal)
        assert len(sent) == 2
        assert (
            await session.scalar(
                select(func.count()).select_from(AgentPortfolioSynthesisOutputRecord)
            )
            == base_synthesis + 2
        )

    if pair_slot_id is not None:
        from nexus.paper.economic_cycle import resume_economic_pair
        from nexus.persistence.models.paper import StrategyDecisionRecord

        async with sessions.begin() as session:
            resumed = await resume_economic_pair(
                session, pair_slot_id=pair_slot_id, expected_database=expected_database
            )
            assert resumed.status == (
                "unavailable"
                if outcome == "unknown"
                else "partial"
                if outcome == "mixed"
                else "awaiting_market"
                if outcome in {"trade", "rotate"}
                else "completed"
            )
            assert {code for _, _, code in resumed.lane_states} == (
                {"PAPER_ORDERS_AWAITING_MARKET", "NO_ORDER_REQUIRED"}
                if outcome == "mixed"
                else {
                    "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
                    if outcome == "unknown"
                    else "PAPER_ORDERS_AWAITING_MARKET"
                    if outcome in {"trade", "rotate"}
                    else "NO_ORDER_REQUIRED"
                }
            )
            from nexus.persistence.models.paper import PaperOrderRecord

            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == (
                base_orders
                + (4 if outcome in {"trade", "rotate"} else 2 if outcome == "mixed" else 0)
            )
            decisions = tuple(
                await session.scalars(
                    select(StrategyDecisionRecord).where(
                        StrategyDecisionRecord.agent_paper_cycle_id.in_(cycle_ids)
                    )
                )
            )
            assert len(decisions) == (0 if outcome == "unknown" else 2)
            repeated = await resume_economic_pair(
                session, pair_slot_id=pair_slot_id, expected_database=expected_database
            )
            assert repeated.replayed and repeated.status == resumed.status
            assert await session.scalar(select(func.count()).select_from(Job)) == base_jobs + 6
            assert len(sent) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [False, True, "unknown"])
@pytest.mark.parametrize("contamination", ["foreign", "borrowed"])
async def test_registered_second_day_budget_reuses_owned_native_facts(
    migrated_database_url, monkeypatch, tmp_path, outcome, contamination
):
    await test_normal_pair_producer_binds_four_research_jobs_and_exact_budget(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        outcome,
        bool(outcome),
        runtime_hold=contamination,
    )


async def verify_registered_multi_day_hold(
    sessions, monkeypatch, clock, identity, actual, outcome, contamination
):
    from nexus.agents.persistence import _shared_daily_budget_used
    from nexus.paper.economic_budget_hold import HoldRequest, hold_pair_budget
    from nexus.paper.economic_cycle import resume_economic_pair
    from nexus.persistence.models.paper import (
        AgentPaperCycleEventRecord,
        PaperLedgerEntryRecord,
        PaperLedgerTransactionRecord,
    )

    async with sessions() as session:
        pair = await session.scalar(
            select(EconomicPairSlotRecord).where(
                EconomicPairSlotRecord.evaluation_id == identity.id,
                EconomicPairSlotRecord.ordinal == 2,
            )
        )
        before_ledger = tuple(
            await session.scalars(
                select(PaperLedgerTransactionRecord.id).order_by(PaperLedgerTransactionRecord.id)
            )
        )
        before_entries = await session.scalar(
            select(func.count()).select_from(PaperLedgerEntryRecord)
        )
        before_jobs = await session.scalar(select(func.count()).select_from(Job))
        first_pair_id = await session.scalar(
            select(EconomicPairSlotRecord.id).where(
                EconomicPairSlotRecord.evaluation_id == identity.id,
                EconomicPairSlotRecord.ordinal == 1,
            )
        )
    clock[0] = pair.decision_at
    request = HoldRequest(
        evaluation_key="normal-economic-pair",
        protocol_hash=identity.protocol_hash,
        slot_ordinal=2,
        research_cost_usd=Decimal(0),
        synthesis_cost_usd=Decimal(0),
        global_limit_usd=Decimal(20),
        uncovered_formal_usd=Decimal(0),
    )
    async with sessions.begin() as session:
        # Existing native Jobs cannot force the registered next budget day
        # back into the empty-DB gate. The supported ChatGPT profile is $0.
        event = await hold_pair_budget(session, request, expected_database=actual)
        repeated = await hold_pair_budget(session, request, expected_database=actual)
        assert event.id == repeated.id
        assert event.occurred_at.date() == pair.decision_at.date()
        assert await _shared_daily_budget_used(
            session, business_date=pair.decision_at.date()
        ) == Decimal(0)
        assert await session.scalar(select(func.count()).select_from(Job)) == before_jobs
        assert (
            tuple(
                await session.scalars(
                    select(PaperLedgerTransactionRecord.id).order_by(
                        PaperLedgerTransactionRecord.id
                    )
                )
            )
            == before_ledger
        )
        assert (
            await session.scalar(select(func.count()).select_from(PaperLedgerEntryRecord))
            == before_entries
        )
    # Future day, wrong frozen costs and an unrelated Job are distinct rejects.
    request3 = request.model_copy(update={"slot_ordinal": 3})
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_SLOT_DAY_MISMATCH"):
            await hold_pair_budget(session, request3, expected_database=actual)
        pair3 = await session.scalar(
            select(EconomicPairSlotRecord).where(
                EconomicPairSlotRecord.evaluation_id == identity.id,
                EconomicPairSlotRecord.ordinal == 3,
            )
        )
    clock[0] = pair3.decision_at
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match="ECONOMIC_HOLD_AUTHORITY_MISMATCH"):
            await hold_pair_budget(
                session,
                request3.model_copy(update={"research_cost_usd": Decimal(2)}),
                expected_database=actual,
            )
        original_job = await session.scalar(
            select(Job).where(Job.job_type == "agent.paper.research").limit(1)
        )
        session.add(
            Job(
                job_key="foreign-runtime-job",
                input_hash="a" * 64,
                status="queued",
                schema_version=original_job.schema_version
                if contamination == "borrowed"
                else "test_v1",
                payload=original_job.payload if contamination == "borrowed" else {},
                job_type="agent.paper.research"
                if contamination == "borrowed"
                else "test.unrelated",
                queue="general",
                scope_kind="global",
                scope_id="global",
                effective_at=clock[0],
                priority=1,
                retry_policy="never",
                max_attempts=1,
                available_at=clock[0],
                started_at=None,
                finished_at=None,
            )
        )
    async with sessions.begin() as session:
        before_events = await session.scalar(
            select(func.count()).select_from(AgentPaperCycleEventRecord)
        )
        with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN"):
            await hold_pair_budget(session, request3, expected_database=actual)
        if outcome:
            historical = await resume_economic_pair(
                session, pair_slot_id=first_pair_id, expected_database=actual
            )
            assert historical.replayed and historical.status == (
                "unavailable" if outcome == "unknown" else "completed"
            )
        else:
            with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN"):
                await resume_economic_pair(
                    session, pair_slot_id=first_pair_id, expected_database=actual
                )
        # A preexisting exact hold is still historical readback, not a new permit.
        assert (await hold_pair_budget(session, request, expected_database=actual)).id == event.id
        assert (
            await session.scalar(select(func.count()).select_from(AgentPaperCycleEventRecord))
            == before_events
        )
        assert await _shared_daily_budget_used(
            session, business_date=pair3.decision_at.date()
        ) == Decimal(0)


async def verify_preheld_start_isolation(sessions, pair, identity, actual, now):
    from nexus.paper.economic_budget_hold import HoldRequest, hold_pair_budget
    from nexus.paper.economic_cycle import start_economic_pair

    request = HoldRequest(
        evaluation_key="normal-economic-pair",
        protocol_hash=identity.protocol_hash,
        slot_ordinal=1,
        research_cost_usd=Decimal(0),
        synthesis_cost_usd=Decimal(0),
        global_limit_usd=Decimal(20),
        uncovered_formal_usd=Decimal(0),
    )
    async with sessions.begin() as session:
        isolated = await session.begin_nested()
        try:
            held = await hold_pair_budget(session, request, expected_database=actual)
            session.add(
                Job(
                    job_key="preheld-foreign-job",
                    input_hash="a" * 64,
                    status="queued",
                    schema_version="test_v1",
                    payload={},
                    job_type="test.unrelated",
                    queue="general",
                    scope_kind="global",
                    scope_id="global",
                    effective_at=now,
                    priority=1,
                    retry_policy="never",
                    max_attempts=1,
                    available_at=now,
                    started_at=None,
                    finished_at=None,
                )
            )
            await session.flush()
            assert (
                await hold_pair_budget(session, request, expected_database=actual)
            ).id == held.id
            with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN"):
                await start_economic_pair(session, pair_slot_id=pair.id, expected_database=actual)
            for table in (AgentResearchRunRecord, EconomicLaneCycleRecord):
                assert await session.scalar(select(func.count()).select_from(table)) == 0
            assert await session.scalar(select(func.count()).select_from(Job)) == 1
        finally:
            await isolated.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize("pricing", ["valid", "expired"])
async def test_second_frozen_day_requires_new_sources_and_valid_frozen_pricing(
    migrated_database_url, monkeypatch, tmp_path, pricing
):
    await test_normal_pair_producer_binds_four_research_jobs_and_exact_budget(
        migrated_database_url, monkeypatch, tmp_path, True, True, multi_day=pricing
    )


async def verify_second_day_pair(
    sessions, url, monkeypatch, clock, identity, binding, actual, pricing
):
    from nexus.paper.economic_cycle import resume_economic_pair, start_economic_pair
    from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord
    from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperLedgerTransactionRecord

    async with sessions() as session:
        pair = await session.scalar(
            select(EconomicPairSlotRecord).where(
                EconomicPairSlotRecord.evaluation_id == identity.id,
                EconomicPairSlotRecord.ordinal == 2,
            )
        )
        old_cycles = tuple(await session.scalars(select(AgentPaperCycleRecord)))
        before = {row.id: (row.candidate_hash, row.candidate_manifest) for row in old_cycles}
        ledger = tuple(await session.scalars(select(PaperLedgerTransactionRecord.id)))
    clock[0] = pair.decision_at + timedelta(seconds=1)
    # Friday quotes/risk cannot grant Monday admission. The rejected whole pair
    # must leave all committed first-day facts intact.
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match="ECONOMIC_PAIR_RESEARCH_UNAVAILABLE"):
            await start_economic_pair(session, pair_slot_id=pair.id, expected_database=actual)
        assert await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 2
        assert await session.scalar(select(func.count()).select_from(Job)) == 6
    await append_second_day_sources(sessions, pair.decision_at)
    async with sessions.begin() as session:
        if pricing == "expired":
            from nexus.paper.agent_cycle_authority import (
                load_quasi_realtime_cycle_authority,
                prepare_candidates,
            )
            from nexus.persistence.models.agents import AgentModelConfigRecord

            model = await session.get(AgentModelConfigRecord, binding.payload["shared_model_id"])
            assert model.pricing_expires_at <= pair.decision_at
            authority = await load_quasi_realtime_cycle_authority(
                session,
                account_id=identity.incumbent_account_id,
                now=pair.decision_at,
                expected_calendar_authority_id=CALENDAR_ID,
            )
            assert (
                len(
                    await prepare_candidates(
                        session,
                        authority=authority,
                        requested_instrument_ids=tuple(item[0] for item in INSTRUMENTS[:2]),
                    )
                )
                == 2
            )
            with pytest.raises(ValueError, match="ECONOMIC_PAIR_RESEARCH_UNAVAILABLE"):
                await start_economic_pair(session, pair_slot_id=pair.id, expected_database=actual)
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 2
            )
            assert await session.scalar(select(func.count()).select_from(Job)) == 6
            return
        started = await start_economic_pair(session, pair_slot_id=pair.id, expected_database=actual)
        assert started.status == "researching" and not started.replayed
        assert set(started.cycle_ids).isdisjoint(before)
        assert await session.scalar(select(func.count()).select_from(Job)) == 10
    await complete_controlled_pair(
        sessions,
        url,
        monkeypatch,
        clock,
        started.cycle_ids,
        binding,
        True,
        pair.id,
        actual,
        decision_time=pair.decision_at,
    )
    async with sessions.begin() as session:
        replay = await resume_economic_pair(session, pair_slot_id=pair.id, expected_database=actual)
        assert replay.replayed and replay.status == "completed"
        assert await session.scalar(select(func.count()).select_from(Job)) == 12
        assert (
            await session.scalar(select(func.count()).select_from(AgentPortfolioSynthesisRunRecord))
            == 4
        )
        assert tuple(await session.scalars(select(PaperLedgerTransactionRecord.id))) == ledger
        for cycle_id, facts in before.items():
            old = await session.get(AgentPaperCycleRecord, cycle_id)
            assert (old.candidate_hash, old.candidate_manifest) == facts
        for cycle_id in started.cycle_ids:
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
            assert cycle.decision_time == pair.decision_at and cycle.execution_locked
            assert {item["decision_quote_id"] for item in cycle.candidate_manifest}.isdisjoint(
                {item["decision_quote_id"] for _, items in before.values() for item in items}
            )


async def append_second_day_sources(sessions, at, *, prices=None):
    """Append new synthetic market facts through normal ingestion, never retime old rows."""
    from hashlib import sha256
    from uuid import uuid4

    from integration.test_agent_paper_cycle_pg import _ObjectStore
    from nexus.market.ingestion import ingest_price_page
    from nexus.market.intraday_ingestion import canonical_quote_observation
    from nexus.persistence.models.data import DataArtifact
    from nexus.persistence.models.market import LatestQuote, UniverseSnapshot
    from nexus.persistence.models.paper import (
        PAPER_DECISION_SOURCE_POLICY_ID,
        PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    )
    from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
    from nexus.risk.query_authority_producer import RiskSourceProof, persist_query_authorities

    async with sessions.begin() as session:
        universe_id = await session.scalar(
            select(UniverseSnapshot.id).where(UniverseSnapshot.key == "us_liquid")
        )
        for instrument_id, close, _, _ in INSTRUMENTS[:2]:
            close = close if prices is None else prices.get(instrument_id, close)
            symbol = await session.scalar(
                select(InstrumentAlias.symbol).where(
                    InstrumentAlias.instrument_id == instrument_id,
                    InstrumentAlias.valid_to.is_(None),
                )
            )
            raw = f"synthetic-quote:{symbol}:{at.isoformat()}".encode()
            revision = sha256(raw).hexdigest()
            artifact = DataArtifact(
                id=uuid4(),
                uri=f"memory://raw/market/schwab/stream/{revision}.json",
                sha256=revision,
                byte_size=len(raw),
                mime_type="application/json",
                schema_version="schwab_level_one_stream_v1",
                source="schwab",
                observed_at=at - timedelta(seconds=10),
                available_at=at,
                received_at=at,
                created_at=at,
            )
            canonical = canonical_quote_observation(
                instrument_id=instrument_id,
                source="schwab",
                feed="l1",
                delivery_mode="real_time",
                event_kind="quote",
                bid=close - Decimal("0.1"),
                ask=close + Decimal("0.1"),
                last=None,
                observed_at=at - timedelta(seconds=10),
                available_at=at,
                received_at=at,
                revision=revision,
            )
            session.add_all(
                (
                    artifact,
                    LatestQuote(
                        instrument_id=instrument_id,
                        source="schwab",
                        feed="l1",
                        delivery_mode="real_time",
                        bid=close - Decimal("0.1"),
                        ask=close + Decimal("0.1"),
                        last=None,
                        observed_at=artifact.observed_at,
                        available_at=at,
                        received_at=at,
                        revision=revision,
                        quality_flags=[],
                        event_kind="quote",
                        content_hash=sha256(canonical).hexdigest(),
                        evidence_status="verified",
                        data_artifact_id=artifact.id,
                        created_at=at,
                    ),
                )
            )
            # Previous complete XNYS session is Friday; never fabricate a Sunday bar.
            prior = at - timedelta(days=3)
            page = ProviderPage(
                items=(
                    PriceBar(
                        instrument_id=instrument_id,
                        session=prior.date(),
                        open=close,
                        high=close,
                        low=close,
                        close=close,
                        volume=Decimal(1000000),
                        observed_at=prior.replace(hour=20),
                        available_at=at - timedelta(hours=1),
                        received_at=at - timedelta(hours=1),
                        revision=f"second-day-prior-{symbol}",
                        quality_flags=(),
                    ),
                ),
                next_cursor=None,
                raw_payload=f"synthetic-prior:{symbol}:{prior.date()}".encode(),
                source_request_id=None,
                as_of=at - timedelta(hours=1),
                provenance=MarketDataProvenance(
                    source="alpaca",
                    feed="sip",
                    adjustment="all",
                    interval="1Day",
                    mode="delayed",
                    delivery_mode="delayed",
                ),
            )
            await ingest_price_page(
                session,
                _ObjectStore(),
                page,
                commit=False,
                source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
            )
            await persist_query_authorities(
                session,
                universe_snapshot_id=universe_id,
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                decision_session=at.date(),
                symbol=symbol,
                proofs=tuple(
                    RiskSourceProof(
                        kind, False, sha256(f"{kind}:{symbol}:{at.date()}".encode()).hexdigest(), at
                    )
                    for kind in ("corporate_action", "suspension", "delisting")
                ),
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome,case",
    [
        ("trade", "frozen"),
        ("trade", "missed"),
        (True, "not_required"),
        ("mixed", "mixed"),
        ("trade", "freeze_late"),
    ],
)
async def test_first_common_window_uses_actual_dual_trade_qualification(
    migrated_database_url, monkeypatch, tmp_path, outcome, case
):
    await test_normal_pair_producer_binds_four_research_jobs_and_exact_budget(
        migrated_database_url, monkeypatch, tmp_path, outcome, True, window_test=case
    )


async def verify_common_window(sessions, clock, pair_id, case, monkeypatch):
    import nexus.paper.economic_window as windows
    from nexus.paper.agent_market import resolve_agent_paper_market_scope
    from nexus.paper.execution_service import advance_internal_paper_order
    from nexus.persistence.models.paper import PaperOrderRecord, StrategyDecisionRecord

    async with sessions.begin() as session:
        orders = tuple(await session.scalars(select(PaperOrderRecord)))
        if case == "not_required":
            assert not orders
            result = await windows.freeze_common_window(session, pair_slot_id=pair_id, window_seq=1)
            assert result.status == "not_required" and result.window_id is None
            from nexus.persistence.models.economic import EconomicCommonWindowRecord

            assert (
                await session.scalar(select(func.count()).select_from(EconomicCommonWindowRecord))
                == 0
            )
            return
        assert len(orders) == (2 if case == "mixed" else 4)
        order = orders[0]
        decision = await session.get(StrategyDecisionRecord, order.strategy_decision_id)
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_REQUIRED"):
            await resolve_agent_paper_market_scope(
                session, cycle_id=decision.agent_paper_cycle_id, instrument_id=order.instrument_id
            )
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_REQUIRED"):
            await advance_internal_paper_order(
                session,
                order_id=order.id,
                idempotency_key="economic-before-window",
                evaluated_at=clock[0],
            )
        if case == "frozen":
            await verify_missing_window_links(session, pair_id)
        if case == "missed":
            clock[0] += timedelta(minutes=10)
        if case == "freeze_late":
            clock[0] = clock[0].replace(second=59)
            parent_scalar = AsyncSession.scalar
            clock_reads = [0]

            async def crossing_clock(self, statement, *args, **kwargs):
                if str(statement).strip() == "SELECT clock_timestamp() AS clock_timestamp_1":
                    clock_reads[0] += 1
                    if clock_reads[0] == 3:
                        clock[0] += timedelta(seconds=1)
                return await parent_scalar(self, statement, *args, **kwargs)

            with monkeypatch.context() as scoped:
                scoped.setattr(AsyncSession, "scalar", crossing_clock)
                frozen = await windows.freeze_common_window(
                    session, pair_slot_id=pair_id, window_seq=1
                )
        else:
            frozen = await windows.freeze_common_window(session, pair_slot_id=pair_id, window_seq=1)
        if case in {"missed", "freeze_late"}:
            assert frozen.status == "missed" and frozen.window_start <= clock[0]
            again = await windows.freeze_common_window(session, pair_slot_id=pair_id, window_seq=1)
            assert again == windows.EconomicWindowResult(
                frozen.status, frozen.window_id, frozen.window_start, frozen.window_end, True
            )
            with pytest.raises(ValueError, match="ECONOMIC_WINDOW_MISSED"):
                await resolve_agent_paper_market_scope(
                    session,
                    cycle_id=decision.agent_paper_cycle_id,
                    instrument_id=order.instrument_id,
                )
            return
        assert frozen.status == "frozen" and frozen.window_start > clock[0]
        from nexus.persistence.models.economic import EconomicCommonWindowRecord

        row = await session.get(EconomicCommonWindowRecord, frozen.window_id)
        actual_eligible = [
            datetime.fromisoformat(lane["eligible_at"]) for lane in row.qualification["lanes"]
        ]
        assert len(set(actual_eligible)) == 2
        assert row.eligible_at == max(actual_eligible)
        assert frozen.window_start == max(actual_eligible).replace(
            second=0, microsecond=0
        ) + timedelta(minutes=1)
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_ARCHIVES_REQUIRED"):
            await windows.freeze_common_window(session, pair_slot_id=pair_id, window_seq=2)
        again = await windows.freeze_common_window(session, pair_slot_id=pair_id, window_seq=1)
        assert again.replayed and again.window_id == frozen.window_id
        starts = set()
        for order in orders:
            decision = await session.get(StrategyDecisionRecord, order.strategy_decision_id)
            scope = await resolve_agent_paper_market_scope(
                session, cycle_id=decision.agent_paper_cycle_id, instrument_id=order.instrument_id
            )
            starts.add(scope.window_start)
        assert starts == {frozen.window_start}
        clock[0] = frozen.window_end + timedelta(seconds=1)
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_ARCHIVES_REQUIRED"):
            await advance_internal_paper_order(
                session,
                order_id=order.id,
                idempotency_key="economic-before-archives",
                evaluated_at=clock[0],
            )


async def verify_freeze_cli(config, actual, pair_id):
    import asyncio
    import json
    import subprocess
    import sys
    from pathlib import Path

    completed = await asyncio.to_thread(
        subprocess.run,
        [
            str(Path(sys.executable).with_name("nexus-engine-economic-cycle")),
            "freeze-window",
            "--database-url-file",
            str(config),
            "--expected-database",
            actual,
            "--pair-slot-id",
            str(pair_id),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0 and not completed.stderr
    receipt = json.loads(completed.stdout)
    assert receipt["status"] in {"frozen", "missed", "not_required"}
    assert receipt["execution_locked"] and receipt["consumption_status"] == "unavailable"


async def verify_missing_window_links(session, pair_id):
    from nexus.paper.economic_window import _read_window, freeze_common_window
    from nexus.persistence.models.economic import (
        EconomicCommonWindowRecord,
        EconomicWindowOrderRecord,
    )
    from sqlalchemy.exc import DBAPIError

    savepoint = await session.begin_nested()
    frozen = await freeze_common_window(session, pair_slot_id=pair_id, window_seq=1)
    row = await session.get(EconomicCommonWindowRecord, frozen.window_id)
    links = tuple(
        await session.scalars(
            select(EconomicWindowOrderRecord).where(EconomicWindowOrderRecord.window_id == row.id)
        )
    )
    values = {column.name: getattr(row, column.name) for column in row.__table__.columns}
    link_values = [
        {column.name: getattr(link, column.name) for column in link.__table__.columns}
        for link in links
    ]
    await savepoint.rollback()
    # A real but unqualified prepared event cannot become window authority.
    from copy import deepcopy
    from uuid import UUID, uuid4

    from nexus.paper.economic_window import _hash
    from nexus.persistence.models.paper import AgentPaperCycleEventRecord

    altered = {**values, "id": uuid4(), "qualification": deepcopy(values["qualification"])}
    lane = altered["qualification"]["lanes"][0]
    prepared = await session.scalar(
        select(AgentPaperCycleEventRecord).where(
            AgentPaperCycleEventRecord.cycle_id == UUID(lane["cycle_id"]),
            AgentPaperCycleEventRecord.to_status == "prepared",
        )
    )
    lane["event_sequence"], lane["event_hash"] = prepared.sequence, prepared.event_hash
    altered["content_hash"] = _hash(altered["qualification"])
    with pytest.raises(DBAPIError, match="ECONOMIC_WINDOW_INVALID"):
        async with session.begin_nested():
            session.add(EconomicCommonWindowRecord(**altered))
            await session.flush()
    for incomplete in ([], link_values[:-1]):
        from uuid import uuid4

        values["id"] = uuid4()
        with pytest.raises(DBAPIError, match="ECONOMIC_WINDOW_ORDER_SET_INVALID"):
            async with session.begin_nested():
                invalid = EconomicCommonWindowRecord(**values)
                session.add(invalid)
                await session.flush()
                session.add_all(
                    EconomicWindowOrderRecord(**{**value, "id": uuid4(), "window_id": values["id"]})
                    for value in incomplete
                )
                await session.flush()
                with pytest.raises(ValueError, match="ECONOMIC_WINDOW_INVALID"):
                    await _read_window(session, invalid)
                await session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))


async def verify_window_downgrade(
    config_path, monkeypatch, *, expected_error="ECONOMIC_WINDOW_DOWNGRADE_FORBIDDEN"
):
    import asyncio
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from sqlalchemy.exc import DBAPIError

    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    with monkeypatch.context() as scoped:
        scoped.setenv("DATABASE_URL", config_path.read_text())
        with pytest.raises(DBAPIError, match=expected_error):
            await asyncio.to_thread(command.downgrade, config, "0070_economic_pair_identity")
