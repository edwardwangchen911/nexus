"""Real PG CAS/episode transitions after a controlled approved-source boundary.

The separate normal three-stock test proves source qualification. These tests
replace only that reader to construct otherwise hard-to-time replacement races.
"""

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from integration.test_agent_persistence_pg import _NOW
from integration.test_research_monitoring_pg import quote, seed
from nexus.domain.evidence import EvidenceRef
from nexus.monitoring import installation
from nexus.monitoring.contracts import PriceMonitoringProposal, PriceReviewCondition
from nexus.monitoring.installation import ApprovedSource, install_plan_from_output
from nexus.monitoring.storage import record_check
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorEpisode, MonitorPlan, MonitorTrigger
from sqlalchemy import func, select


async def synthetic_source(factory, plan, *, minute, threshold):
    at = _NOW + timedelta(minutes=minute)
    async with factory() as session, session.begin():
        original = await session.get(AgentResearchRunRecord, plan.source_run_id)
        fields = {
            column.name: getattr(original, column.name)
            for column in original.__table__.columns
            if column.name not in {"id", "created_at", "decision_time", "idempotency_key"}
        }
        run = AgentResearchRunRecord(
            **fields, id=uuid4(), decision_time=at, idempotency_key=str(uuid4())
        )
        session.add(run)
        await session.flush()
        output = AgentResearchOutputRecord(
            run_id=run.id,
            sequence=1,
            status="completed",
            payload={},
            intent={},
            content_hash="d" * 64,
        )
        session.add(output)
        await session.flush()
    proposal = PriceMonitoringProposal(
        condition=PriceReviewCondition.model_validate({**plan.condition, "threshold": threshold}),
        rationale="受控版本接替测试。",
        evidence_refs=(
            EvidenceRef(
                source="fixture",
                object_id="replacement",
                available_at=at,
                location="memory://replacement",
                content_hash="e" * 64,
            ),
        ),
        review_due_at=at + timedelta(hours=1),
        valid_until=at + timedelta(hours=2),
    )
    context = SimpleNamespace(
        decision_time=at,
        decision_quote=SimpleNamespace(
            bid="100",
            ask="100",
            observed_at=at,
            available_at=at,
            received_at=at,
        ),
    )
    return ApprovedSource(run, output, proposal, context)


@pytest.mark.asyncio
@pytest.mark.parametrize("later_check", [False, True])
async def test_returning_condition_consumes_frozen_quote_but_not_postfreeze_checks(
    migrated_database_url, monkeypatch, later_check
):
    factory, plan_a = await seed(migrated_database_url)
    first_check = _NOW + timedelta(minutes=5 if later_check else 0)
    async with factory() as session, session.begin():
        await record_check(
            session,
            plan_id=plan_a.id,
            scheduled_at=first_check,
            checked_at=first_check,
            observation=quote(plan_a, first_check, False),
        )
    source_b = await synthetic_source(factory, plan_a, minute=1, threshold="200")
    source_a = await synthetic_source(factory, plan_a, minute=2, threshold="100")
    source_late = await synthetic_source(factory, plan_a, minute=1.5, threshold="150")
    sources = {source.output.id: source for source in (source_b, source_a, source_late)}

    async def approved(_session, *, account_id, output_id, now):
        assert account_id == plan_a.account_id
        return sources[output_id]

    monkeypatch.setattr(installation, "load_approved_source", approved)

    async def install(source, predecessor, minute):
        async with factory() as session, session.begin():
            return await install_plan_from_output(
                session,
                account_id=plan_a.account_id,
                output_id=source.output.id,
                expected_predecessor_id=predecessor,
                now=_NOW + timedelta(minutes=minute),
            )

    twins = await asyncio.gather(*(install(source_b, plan_a.id, 6) for _ in range(2)))
    assert twins[0].id == twins[1].id and twins[0].generation == 2
    plan_b = twins[0]
    replacement = await install(source_a, plan_b.id, 7)
    assert replacement.generation == 3
    async with factory() as session:
        state = await session.get(
            MonitorEpisode, (plan_a.account_id, plan_a.instrument_id, plan_a.condition_hash)
        )
        assert state.latched is (not later_check)
    with pytest.raises(ValueError, match="MONITOR_PLAN_CONFLICT"):
        await install(source_late, plan_a.id, 8)
    with pytest.raises(ValueError, match="MONITOR_SOURCE_NOT_NEWER"):
        await install(source_late, replacement.id, 8)
    tick = _NOW + timedelta(minutes=10)
    async with factory() as session, session.begin():
        await record_check(
            session,
            plan_id=replacement.id,
            scheduled_at=tick,
            checked_at=tick,
            observation=quote(replacement, tick),
        )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(MonitorPlan)) == 3
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == int(
            later_check
        )
    original = await install(source_b, plan_a.id, 11)
    assert original.id == plan_b.id  # Idempotent old receipt never reactivates the old plan.
    async with factory() as session:
        current = await session.scalar(
            select(MonitorPlan).order_by(MonitorPlan.generation.desc()).limit(1)
        )
        assert current.id == replacement.id


@pytest.mark.asyncio
@pytest.mark.parametrize("installation_minute", [4, 6])
async def test_scanner_reloads_generation_after_concurrent_installer(
    migrated_database_url, monkeypatch, installation_minute
):
    from nexus.monitoring import scanning
    from nexus.monitoring.storage import lock_security
    from nexus.persistence.models.monitoring import MonitorCheck

    factory, original = await seed(migrated_database_url)
    source = await synthetic_source(factory, original, minute=1, threshold="200")
    selected, resume = asyncio.Event(), asyncio.Event()
    at = _NOW + timedelta(minutes=5)

    async def approved(*_args, **_kwargs):
        return source

    async def calendar(*_args, **_kwargs):
        return SimpleNamespace(market_open_at=_NOW, market_close_at=_NOW + timedelta(hours=3))

    async def delayed_lock(session, **kwargs):
        selected.set()
        await resume.wait()
        await lock_security(session, **kwargs)

    async def observation(*_args, **_kwargs):
        return quote(original, at)

    monkeypatch.setattr(installation, "load_approved_source", approved)
    monkeypatch.setattr(scanning, "load_agent_paper_service_calendar_authority", calendar)
    monkeypatch.setattr(scanning, "lock_security", delayed_lock)
    monkeypatch.setattr(scanning, "read_latest_price", observation)

    async def scan():
        async with factory() as session, session.begin():
            return await scanning.scan_account(
                session, account_id=original.account_id, calendar_authority_id=uuid4(), now=at
            )

    task = asyncio.create_task(scan())
    try:
        await selected.wait()
        async with factory() as session, session.begin():
            replacement = await install_plan_from_output(
                session,
                account_id=original.account_id,
                output_id=source.output.id,
                expected_predecessor_id=original.id,
                now=_NOW + timedelta(minutes=installation_minute),
            )
        resume.set()
        result = await task
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    future = installation_minute > 5
    assert result["not_effective"] == int(future)
    assert result["checks"] == int(not future)
    async with factory() as session:
        checks = tuple(await session.scalars(select(MonitorCheck)))
        assert [check.plan_id for check in checks] == ([] if future else [replacement.id])
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 0
