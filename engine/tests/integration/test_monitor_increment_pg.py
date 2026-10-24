"""Normal formal three-stock parent to exactly one incremental native Job."""

from datetime import datetime, timedelta
from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_candidate_multistock_path import (
    test_fresh_multistock_candidate_cycle_freezes_native_jobs as normal_cycle,
)
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.monitoring.installation import install_plan_from_output
from nexus.monitoring.observations import read_latest_price
from nexus.monitoring.storage import record_check
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


async def ready_trigger(database_url, monkeypatch):
    await normal_cycle(
        database_url,
        monkeypatch,
        3,
        False,
        "formal_v11",
        monitoring=True,
        monitoring_threshold="100000",
    )
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session, session.begin():
        output = await session.scalar(
            select(AgentResearchOutputRecord)
            .where(AgentResearchOutputRecord.status == "completed")
            .order_by(AgentResearchOutputRecord.id)
            .limit(1)
        )
        run = await session.get(AgentResearchRunRecord, output.run_id)
        context = await read_agent_paper_research_context(
            session,
            snapshot_id=run.data_snapshot_id,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
        )
        cycle = await session.get(AgentPaperCycleRecord, context[0].cycle_id)
        plan = await install_plan_from_output(
            session,
            account_id=cycle.account_id,
            output_id=output.id,
            expected_predecessor_id=None,
            now=AT + timedelta(seconds=3),
        )
        at = AT + timedelta(seconds=4)
        await record_check(
            session,
            plan_id=plan.id,
            scheduled_at=AT,
            checked_at=at,
            observation=await read_latest_price(session, instrument_id=run.instrument_id, now=at),
        )
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
        database_url=database_url,
        agent_paper_account_id=cycle.account_id,
        agent_paper_candidate_batch_id=cycle.candidate_batch_id,
        agent_runtime_mode="codex_sdk",
        agent_paper_formal_model_config_id=run.model_config_id,
    )
    async with factory() as session:
        trigger = await session.scalar(
            select(MonitorTrigger).where(MonitorTrigger.account_id == cycle.account_id)
        )
        assert trigger is not None
    # The ordinary prior-cycle guard keeps its unexpired leases. Advance beyond
    # them and append genuinely new synthetic quote/risk facts; never rewrite old rows.
    async with factory() as session:
        later = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
    await _set_test_insert_clock(factory, later)
    await append_fresh_inputs(factory, run.instrument_id, later)
    from sqlalchemy.ext.asyncio import AsyncSession

    old_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return later
        return await old_scalar(self, statement, *args, **kwargs)

    class WorkerClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return later if tz is None else later.astimezone(tz)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    monkeypatch.setattr("nexus.worker.datetime", WorkerClock)
    return factory, settings, trigger, run, later


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown", [False, True], ids=["settled", "unknown"])
async def test_normal_trigger_admission_is_single_stock_idempotent_and_blocks_competing_entry(
    migrated_database_url, monkeypatch, unknown
):
    from nexus.monitoring.admission import require_increment_binding
    from nexus.monitoring.context import read_monitor_context
    from nexus.ops.research_monitor import parser
    from nexus.ops.research_monitor import run as monitor_entry
    from nexus.paper.manual_request import (
        require_manual_account_available,
        require_no_running_manual_request,
    )

    factory, settings, trigger, parent, now = await ready_trigger(
        migrated_database_url, monkeypatch
    )
    async with factory() as session, session.begin():
        await require_manual_account_available(
            session, account_id=settings.agent_paper_account_id, now=now
        )
    args = parser().parse_args(
        ["admit", "--trigger-id", str(trigger.id), "--calendar-authority-id", str(CALENDAR_ID)]
    )
    from nexus.monitoring import admission

    errors = []
    native_admit = admission.admit_trigger

    async def capture(*args, **kwargs):
        try:
            return await native_admit(*args, **kwargs)
        except ValueError as error:
            errors.append(str(error))
            raise

    monkeypatch.setattr(admission, "admit_trigger", capture)
    async with factory() as stale, stale.begin():
        # Two requests can both load pending before either acquires the account lock.
        pending = await stale.get(MonitorTrigger, trigger.id)
        assert pending.status == "pending"
        first = await monitor_entry(settings, args)
        assert first["status"] == "admitted", (first, errors)
        original = await native_admit(
            stale,
            account_id=settings.agent_paper_account_id,
            trigger_id=trigger.id,
            candidate_batch_id=settings.agent_paper_candidate_batch_id,
            calendar_authority_id=CALENDAR_ID,
            model_config_id=settings.agent_paper_formal_model_config_id,
            daily_limit_usd=settings.agent_daily_budget_usd,
            daily_sdk_call_limit=settings.agent_monitor_daily_sdk_call_limit,
            now=now,
        )
        assert str(original.run_id) == first["run_id"]
        assert str(original.job_id) == first["job_id"]
    again = await monitor_entry(settings, args)
    assert first["run_id"] == again["run_id"] and first["execution_locked"] is True
    async with factory() as session:
        runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
        assert len(runs) == 4
        child = await session.get(AgentResearchRunRecord, UUID(first["run_id"]))
        assert child.instrument_id == parent.instrument_id and child.status == "prepared"
        assert child.data_snapshot_id != parent.data_snapshot_id
        member = await require_increment_binding(session, child)
        assert member.reserved_sdk_calls == 3 and member.daily_sdk_call_limit == 9
        bound = await read_monitor_context(
            session,
            snapshot_id=child.data_snapshot_id,
            instrument_id=child.instrument_id,
            as_of=child.decision_time,
        )
        assert bound[0].parent_run_id == parent.id
        assert bound[0].trigger_checks[0].trigger_id == trigger.id
        assert await session.scalar(select(func.count()).select_from(MonitorIncrement)) == 1
        job = await session.get(Job, member.job_id)
        assert job.max_attempts == 1 and job.attempt_count == 0
    for guard in (require_manual_account_available, require_no_running_manual_request):
        async with factory() as session, session.begin():
            with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                await guard(
                    session,
                    account_id=settings.agent_paper_account_id,
                    **({"now": now} if guard is require_manual_account_available else {}),
                )
    from nexus.agents.research_only import require_formal_research

    async with factory() as session:
        with pytest.raises(ValueError, match="MONITOR_FORMAL_CONSUMPTION_FORBIDDEN"):
            await require_formal_research(session, child)

    from nexus.agents import codex_staged
    from nexus.worker import run_agent_worker_once

    original_invoke = codex_staged.invoke_codex
    calls = []

    async def tracked(payload, **kwargs):
        calls.append(payload["stage"])
        if unknown and payload["stage"] == "critique":
            raise TimeoutError("controlled incremental usage unknown")
        return await original_invoke(payload, **kwargs)

    monkeypatch.setattr(codex_staged, "invoke_codex", tracked)
    assert await run_agent_worker_once(
        settings, job_ids=(member.job_id,), formal_stage_capacity=True
    )
    async with factory() as session:
        child = await session.get(AgentResearchRunRecord, member.run_id)
        job = await session.get(Job, member.job_id)
        assert child.status == ("failed" if unknown else "completed"), (
            child.status,
            job.last_error_code,
        )
        assert (child.settled_cost_usd is None) is unknown
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 4
        if not unknown:
            assert (child.input_tokens, child.output_tokens) == (300, 300)
            terminal = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == child.id,
                    AgentResearchOutputRecord.status == "completed",
                )
            )
            assert terminal.intent["quality_status"] == "complete"
            assert (
                terminal.intent["stance"] == "watch"
                and terminal.intent["exposure_action"] == "none"
            )
    assert calls == (["research", "critique"] if unknown else ["research", "critique", "synthesis"])
    assert not await run_agent_worker_once(
        settings, job_ids=(member.job_id,), formal_stage_capacity=True
    )
    assert len(calls) == (2 if unknown else 3)

    if unknown:
        # Expiry does not erase the native exit proof or the unknown budget hold.
        from sqlalchemy.ext.asyncio import AsyncSession

        now += timedelta(minutes=2)
        previous_scalar = AsyncSession.scalar

        async def after_expiry(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return now
            return await previous_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", after_expiry)
    finish_args = parser().parse_args(["finish", "--trigger-id", str(trigger.id)])
    finished = await monitor_entry(settings, finish_args)
    assert finished["status"] == ("unknown" if unknown else "completed")
    assert await monitor_entry(settings, finish_args) == finished
    original_receipt = await monitor_entry(settings, args)
    assert original_receipt["status"] == finished["status"]
    assert original_receipt["run_id"] == first["run_id"]
    assert len(calls) == (2 if unknown else 3)
    async with factory() as session, session.begin():
        from nexus.monitoring.admission import require_monitor_account_available
        from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord

        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == member.run_id
            )
        )
        from nexus.persistence.models.runtime import AuditEvent

        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(
                    AuditEvent.action == "research.monitor.increment_finished",
                    AuditEvent.payload["trigger_id"].astext == str(trigger.id),
                )
            )
            == 1
        )
        assert reservation.reserved_usd == 0
        assert reservation.state == ("reserved" if unknown else "settled")
        assert (reservation.settled_usd is None) is unknown
        assert (await session.get(MonitorIncrement, member.id)).reserved_sdk_calls == 3
        if unknown:
            with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_monitor_account_available(
                    session, account_id=settings.agent_paper_account_id, now=now
                )
        else:
            await require_monitor_account_available(
                session, account_id=settings.agent_paper_account_id, now=now
            )

    for guard in (require_manual_account_available, require_no_running_manual_request):
        async with factory() as session, session.begin():
            params = {"now": now} if guard is require_manual_account_available else {}
            if unknown:
                with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await guard(session, account_id=settings.agent_paper_account_id, **params)
            else:
                await guard(session, account_id=settings.agent_paper_account_id, **params)

    if unknown:
        from sqlalchemy import update
        from sqlalchemy.exc import DBAPIError

        async with factory() as session, session.begin():
            with pytest.raises(DBAPIError, match="MONITOR_TRIGGER_TRANSITION_INVALID"):
                async with session.begin_nested():
                    await session.execute(
                        update(MonitorTrigger)
                        .where(MonitorTrigger.id == trigger.id)
                        .values(status="completed", reason=None)
                    )
    if not unknown:
        # A second security has valid current inputs, but a finite call cap still blocks
        # its new job even when the subscription's dollar estimate is zero.
        async with factory() as session:
            other_output = await session.scalar(
                select(AgentResearchOutputRecord)
                .join(
                    AgentResearchRunRecord,
                    AgentResearchRunRecord.id == AgentResearchOutputRecord.run_id,
                )
                .where(
                    AgentResearchRunRecord.instrument_id != parent.instrument_id,
                    AgentResearchOutputRecord.status == "completed",
                )
                .limit(1)
            )
            other_run = await session.get(AgentResearchRunRecord, other_output.run_id)
        await append_fresh_inputs(factory, other_run.instrument_id, now)
        async with factory() as session, session.begin():
            other_plan = await install_plan_from_output(
                session,
                account_id=settings.agent_paper_account_id,
                output_id=other_output.id,
                expected_predecessor_id=None,
                now=now,
            )
            checked = await record_check(
                session,
                plan_id=other_plan.id,
                scheduled_at=now.replace(minute=(now.minute // 5) * 5, second=0, microsecond=0),
                checked_at=now,
                observation=await read_latest_price(
                    session, instrument_id=other_run.instrument_id, now=now
                ),
            )
            other_trigger = await session.scalar(
                select(MonitorTrigger).where(MonitorTrigger.check_id == checked.id)
            )
            outbox_id = other_trigger.outbox_id
        limited = settings.model_copy(update={"agent_monitor_daily_sdk_call_limit": 3})
        blocked = await monitor_entry(
            limited,
            parser().parse_args(
                [
                    "admit",
                    "--trigger-id",
                    str(other_trigger.id),
                    "--calendar-authority-id",
                    str(CALENDAR_ID),
                ]
            ),
        )
        assert blocked["error_code"] == "MONITOR_DAILY_CALL_LIMIT", blocked
        async with factory() as session, session.begin():
            from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
            from sqlalchemy.exc import DBAPIError

            retained = await session.get(MonitorTrigger, other_trigger.id)
            assert retained.status == "blocked" and retained.outbox_id == outbox_id
            assert retained.research_run_id is None
            assert await session.scalar(select(func.count()).select_from(MonitorIncrement)) == 1
            assert (
                await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 4
            )
            source = await session.scalar(
                select(AgentDataSnapshotEvidenceRecord).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == member.snapshot_id,
                    AgentDataSnapshotEvidenceRecord.source == "nexus_price_monitor_increment",
                )
            )
            fields = {c.name: getattr(source, c.name) for c in source.__table__.columns}
            fields["instrument_id"] = other_run.instrument_id
            with pytest.raises(DBAPIError, match="not owned by the instrument"):
                async with session.begin_nested():
                    session.add(AgentDataSnapshotEvidenceRecord(**fields))
                    await session.flush()
            import json
            from copy import deepcopy

            from nexus.persistence.models.data import DataSnapshot

            snapshot = await session.get(DataSnapshot, member.snapshot_id)
            for wrong_field in ("account_id", "trigger_id"):
                forged = {c.name: getattr(snapshot, c.name) for c in snapshot.__table__.columns}
                forged["id"] = uuid4()
                forged["payload"] = deepcopy(snapshot.payload)
                context = forged["payload"]["monitor_context"]
                if wrong_field == "account_id":
                    context["account_id"] = str(uuid4())
                else:
                    context["trigger_checks"][0]["trigger_id"] = str(uuid4())
                forged["revision"] = sha256(
                    json.dumps(forged["payload"], sort_keys=True).encode()
                ).hexdigest()
                evidence_fields = {
                    c.name: getattr(source, c.name) for c in source.__table__.columns
                }
                evidence_fields["data_snapshot_id"] = forged["id"]
                with pytest.raises(DBAPIError, match="not owned by the instrument"):
                    async with session.begin_nested():
                        session.add(DataSnapshot(**forged))
                        await session.flush()
                        session.add(AgentDataSnapshotEvidenceRecord(**evidence_fields))
                        await session.flush()
        import asyncio
        from pathlib import Path

        from alembic import command
        from alembic.config import Config

        monkeypatch.setenv("DATABASE_URL", migrated_database_url)
        config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
        with pytest.raises(Exception, match="MONITOR_EVIDENCE_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(command.downgrade, config, "0088_monitor_increments")


async def append_fresh_inputs(factory, instrument_id, at, *, midpoint=None):
    from nexus.market.intraday_ingestion import canonical_quote_observation
    from nexus.persistence.models.data import DataArtifact
    from nexus.persistence.models.market import LatestQuote
    from nexus.persistence.models.paper import RiskQueryAuthorityRecord
    from nexus.risk.authority import canonical_query_authority_hash

    async with factory() as session, session.begin():
        old = await session.scalar(
            select(LatestQuote)
            .where(LatestQuote.instrument_id == instrument_id)
            .order_by(LatestQuote.received_at.desc())
            .limit(1)
        )
        artifact = DataArtifact(
            id=uuid4(),
            uri="memory://synthetic/monitor-fresh",
            sha256=sha256(f"synthetic:{instrument_id}:{at.isoformat()}".encode()).hexdigest(),
            byte_size=100,
            mime_type="application/json",
            schema_version="schwab_level_one_stream_v1",
            source="schwab",
            observed_at=at,
            available_at=at,
            received_at=at,
            created_at=at,
        )
        session.add(artifact)
        await session.flush()
        values = {
            c.name: getattr(old, c.name)
            for c in old.__table__.columns
            if c.name not in {"id", "created_at", "content_hash"}
        }
        values.update(
            observed_at=at,
            available_at=at,
            received_at=at,
            data_artifact_id=artifact.id,
            revision=artifact.sha256,
        )
        if midpoint is not None:
            values.update(bid=midpoint, ask=midpoint)
        raw = canonical_quote_observation(
            instrument_id=instrument_id,
            source="schwab",
            feed="l1",
            delivery_mode="real_time",
            event_kind="quote",
            bid=values["bid"],
            ask=values["ask"],
            last=None,
            observed_at=at,
            available_at=at,
            received_at=at,
            revision=artifact.sha256,
        )
        session.add(LatestQuote(**values, content_hash=sha256(raw).hexdigest(), created_at=at))
        risks = tuple(
            await session.scalars(
                select(RiskQueryAuthorityRecord).where(
                    RiskQueryAuthorityRecord.required_instrument_ids == [instrument_id]
                )
            )
        )
        # Repeated preparation appends one new fact per authority scope; prior
        # snapshots stay immutable and are not duplicated at the new timestamp.
        latest = {}
        for risk in risks:
            key = (
                risk.source_policy_id,
                risk.universe_snapshot_id,
                risk.decision_session,
                risk.kind,
                tuple(risk.required_instrument_ids),
            )
            if key not in latest or risk.available_at > latest[key].available_at:
                latest[key] = risk
        for risk in latest.values():
            values = {
                c.name: getattr(risk, c.name)
                for c in risk.__table__.columns
                if c.name not in {"id", "created_at", "content_hash"}
            }
            values["available_at"] = at
            digest = canonical_query_authority_hash(
                **{
                    k: values[k]
                    for k in (
                        "source_policy_id",
                        "universe_snapshot_id",
                        "decision_session",
                        "kind",
                        "required_instrument_ids",
                        "blocked_instrument_ids",
                        "complete",
                        "source_watermark_hash",
                        "available_at",
                    )
                }
            )
            session.add(RiskQueryAuthorityRecord(**values, content_hash=digest, created_at=at))
