"""Persisted as-of and account boundaries for bounded Workbench selection.

All records belong to isolated synthetic databases. No workers or sources run.
"""

from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import uuid4

import pytest
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.ops.openai_local import prepare_database
from nexus.paper.agent_cycle import _append_event
from nexus.paper.agent_cycle_models import LegacyCandidateManifestEntry, candidate_manifest_hash
from nexus.paper.agent_decision import AGENT_PAPER_SIZING_POLICY_HASH
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.manual_request import _append, _event_hash, manual_request_hash
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_POLICY_HASH
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCandidateBatchRecord,
    AgentPaperCycleRecord,
    PaperAccountRecord,
)
from nexus.persistence.models.portfolio import Account, Portfolio
from nexus.persistence.models.runtime import Job, JobLease
from nexus.settings import EngineSettings
from nexus.workbench.activity_read import _unsettled_cycles, read_activity
from sqlalchemy import select
from unit.ops.test_formal_stage_registration import OPTIONS


async def scope_fixture(url, monkeypatch):
    await prepare_database(
        EngineSettings(database_url=url, agent_runtime_mode="codex_sdk"), **OPTIONS
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        url, monkeypatch, decision_time=AT
    )
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
            idempotency_key="workbench-boundary-scope",
            created_at=AT,
        )
    return engine, sessions, authority, manifest, model, batch


async def request_row(session, authority, model, batch, at):
    row = ManualPaperRequest(
        id=uuid4(),
        account_id=authority.account_id,
        candidate_batch_id=batch.batch_id,
        model_config_id=model.id,
        mode="research_and_paper",
        contract_version=1,
        research_profile=None,
        key_hash=sha256(str(at).encode()).hexdigest(),
        request_hash=manual_request_hash(
            account_id=authority.account_id, batch_id=batch.batch_id, mode="research_and_paper"
        ),
        batch_hash=batch.batch_hash,
        model_hash=model.config_hash,
        requested_at=at,
        created_at=at,
    )
    session.add(row)
    await session.flush()
    await _append(session, row, status="queued", code="MANUAL_REQUEST_QUEUED", cycle_id=None, at=at)
    return row


@pytest.mark.asyncio
@pytest.mark.parametrize("future_field", ["occurred_at", "created_at"])
async def test_future_terminal_cannot_hide_older_preparing_request(
    migrated_database_url, monkeypatch, future_field
):
    engine, sessions, authority, _, model, batch = await scope_fixture(
        migrated_database_url, monkeypatch
    )
    try:
        async with sessions.begin() as session:
            older = await request_row(session, authority, model, batch, AT)
            await _append(
                session,
                older,
                status="preparing",
                code="MANUAL_PREPARATION_STARTED",
                cycle_id=None,
                at=AT + timedelta(seconds=1),
            )
            # Persisted later terminal must not remove this request from the as-of pending query.
            prior = await session.scalar(
                select(ManualPaperRequestEvent)
                .where(ManualPaperRequestEvent.request_id == older.id)
                .order_by(ManualPaperRequestEvent.sequence.desc())
                .limit(1)
            )
            terminal = ManualPaperRequestEvent(
                request_id=older.id,
                sequence=3,
                status="unavailable",
                code="MANUAL_INPUTS_UNAVAILABLE",
                cycle_id=None,
                occurred_at=AT
                + (timedelta(hours=1) if future_field == "occurred_at" else timedelta(seconds=3)),
                created_at=AT
                + (timedelta(hours=1) if future_field == "created_at" else timedelta(seconds=3)),
            )
            terminal.event_hash = _event_hash(older, terminal, prior.event_hash)
            session.add(terminal)
            await session.flush()
            newer = await request_row(session, authority, model, batch, AT + timedelta(seconds=2))
            await _append(
                session,
                newer,
                status="unavailable",
                code="MANUAL_INPUTS_UNAVAILABLE",
                cycle_id=None,
                at=AT + timedelta(seconds=3),
            )
        async with sessions() as session:
            result = await read_activity(
                session, read_at=AT + timedelta(seconds=4), decision_session=AT.date()
            )
            formal = result.scopes[0]
            # Existing receipt validation is all-events. It must fail closed rather than
            # silently selecting the newer terminal and claiming no outstanding work.
            assert result.read_status == "partial"
            assert formal.selection == "unavailable"
            assert formal.reason_code == "WORKBENCH_ACTIVITY_UNVERIFIED"
            assert formal.occupancy == "unverified"
            assert formal.request_id != newer.id
    finally:
        await engine.dispose()


async def cycle_row(session, authority, at, *, batch_id=None, manifest=(), account=None):
    cycle = AgentPaperCycleRecord(
        id=uuid4(),
        account_id=authority.account_id if account is None else account.id,
        portfolio_id=authority.portfolio_id if account is None else account.portfolio_id,
        decision_session=at.date(),
        decision_time=at,
        decision_slot_at=at,
        execution_session=at.date() + timedelta(days=1),
        universe_snapshot_id=authority.universe_snapshot_id,
        quality_report_id=None,
        candidate_batch_id=batch_id,
        decision_price_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
        execution_price_policy_id=PAPER_EXECUTION_SOURCE_POLICY_ID,
        candidate_manifest=[item.model_dump(mode="json") for item in manifest],
        candidate_hash=candidate_manifest_hash(manifest),
        sizing_policy_version="agent_paper_sizing_v1"
        if batch_id is None
        else "portfolio_allocation_v1",
        sizing_policy_hash=AGENT_PAPER_SIZING_POLICY_HASH
        if batch_id is None
        else PORTFOLIO_ALLOCATION_POLICY_HASH,
        execution_locked=True,
        created_at=at,
    )
    session.add(cycle)
    await session.flush()
    await _append_event(session, cycle, None, "prepared", "CYCLE_PREPARED", {})
    return cycle


@pytest.mark.asyncio
async def test_selected_historical_cycle_does_not_replace_today_count_session(
    migrated_database_url, monkeypatch
):
    engine, sessions, authority, _, _, _ = await scope_fixture(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as session:
            historical = await cycle_row(session, authority, AT)
            for minute in (1, 2):
                current = await cycle_row(
                    session, authority, AT + timedelta(days=1, minutes=minute)
                )
                await _append_event(
                    session,
                    current,
                    "prepared",
                    "unavailable",
                    "RESEARCH_ADMISSION_UNAVAILABLE",
                    {},
                )
        today = (AT + timedelta(days=1)).date()
        async with sessions() as session:
            result = await read_activity(
                session, read_at=AT + timedelta(days=1, hours=1), decision_session=today
            )
            formal = result.scopes[0]
            assert formal.selection == "historical_unresolved"
            assert formal.cycle_id == historical.id
            assert formal.decision_session == today
            assert formal.selected_decision_session == AT.date()
            assert formal.today_started == 2
            assert formal.historical and formal.occupancy == "unresolved"
    finally:
        await engine.dispose()


async def foreign_account(session):
    # The existing read contract excludes the separate evaluation account from the
    # main account selector, while its Jobs/leases still occupy the shared tables.
    account = Account(
        id=uuid4(), provider="internal_paper_evaluation", status="ACTIVE", created_at=AT
    )
    portfolio = Portfolio(
        id=uuid4(), account_id=account.id, name="Synthetic independent lane", created_at=AT
    )
    session.add(account)
    await session.flush()
    session.add(portfolio)
    await session.flush()
    paper = PaperAccountRecord(
        id=uuid4(),
        account_id=account.id,
        portfolio_id=portfolio.id,
        code="synthetic-other",
        currency="USD",
        account_model="cash",
        position_model="long_only",
        initial_capital=Decimal("100"),
        capability="simulation_write_internal",
        created_at=AT,
    )
    session.add(paper)
    await session.flush()
    return paper


async def run_and_job(session, *, kind, cycle, model, prompt, candidate, batch_id, live=False):
    common = dict(
        id=uuid4(),
        model_config_id=model.id,
        prompt_version_id=prompt.id,
        decision_time=cycle.decision_time,
        status="failed",
        settled_cost_usd=Decimal("0"),
        model_hash=model.config_hash,
        prompt_hash="a" * 64,
        input_hash="b" * 64,
        pricing_version=model.version,
        idempotency_key=f"agent-paper:{cycle.id}:{candidate.instrument_id}",
        created_at=cycle.decision_time,
    )
    if kind == "research":
        run = AgentResearchRunRecord(
            **common,
            instrument_id=candidate.instrument_id,
            data_snapshot_id=candidate.research_snapshot_id,
            actor_id="local-agent-paper-cycle",
            retry_count=0,
            schema_hash="a" * 64,
            tool_hash="a" * 64,
            prompt_manifest={},
            evidence_manifest=[{"synthetic": True}],
            evidence_hash="a" * 64,
            reserved_cost_usd=Decimal("0"),
            input_tokens=0,
            output_tokens=0,
        )
    else:
        run = AgentPortfolioSynthesisRunRecord(
            **common,
            cycle_id=cycle.id,
            candidate_batch_id=batch_id,
            research_output_ids=[uuid4()],
            active_target_version_id=None,
            projection_hash="a" * 64,
            frozen_input={},
            input_tokens=0,
            output_tokens=0,
            completed_at=cycle.decision_time,
        )
    session.add(run)
    await session.flush()
    job = Job(
        id=uuid4(),
        job_key=f"boundary:{run.id}",
        input_hash="a" * 64,
        status="running" if live else "failed",
        schema_version="research_v1",
        payload={"run_id": str(run.id)},
        job_type="agent.paper.research" if kind == "research" else "agent.portfolio_synthesis",
        queue="general",
        scope_kind="instrument" if kind == "research" else "portfolio",
        scope_id=str(candidate.instrument_id if kind == "research" else cycle.portfolio_id),
        effective_at=cycle.decision_time,
        priority=0,
        retry_policy="never",
        max_attempts=1,
        attempt_count=1,
        manual_retry_count=0,
        available_at=cycle.decision_time,
        started_at=cycle.decision_time,
        finished_at=None if live else cycle.decision_time,
        version=1,
        created_at=cycle.decision_time,
    )
    session.add(job)
    await session.flush()
    if live:
        session.add(
            JobLease(
                job_id=job.id,
                worker_id="synthetic-foreign-worker",
                lease_token=uuid4(),
                fence_token=1,
                acquired_at=cycle.decision_time,
                heartbeat_at=cycle.decision_time,
                expires_at=AT + timedelta(days=3),
                created_at=cycle.decision_time,
            )
        )
    if kind == "synthesis":
        # Keep a valid preceding research binding so the selected cycle can be
        # read through the normal detail service, independently of synthesis SQL.
        await run_and_job(
            session,
            kind="research",
            cycle=cycle,
            model=model,
            prompt=prompt,
            candidate=candidate,
            batch_id=batch_id,
        )
        return run
    await _append_event(
        session,
        cycle,
        "prepared",
        "researching",
        "RESEARCH_JOBS_PREPARED",
        {"research_run_ids": [str(run.id)]},
    )
    await _append_event(
        session, cycle, "researching", "unavailable", "RESEARCH_OUTPUT_NOT_ACTIONABLE", {}
    )
    return run


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["research", "synthesis"])
async def test_foreign_live_lease_cannot_push_21_settled_cycles_into_pending_limit(
    migrated_database_url, monkeypatch, kind
):
    engine, sessions, authority, manifest, model, batch = await scope_fixture(
        migrated_database_url, monkeypatch
    )
    try:
        async with sessions.begin() as session:
            prompt = await session.scalar(
                select(AgentPromptVersionRecord)
                .where(AgentPromptVersionRecord.role == "orchestrator")
                .limit(1)
            )
            candidate = LegacyCandidateManifestEntry(
                instrument_id=manifest[0].instrument_id,
                symbol=manifest[0].symbol,
                inclusion_reason="synthetic boundary fixture",
                research_snapshot_id=manifest[0].research_snapshot_id,
            )
            ours = []
            for index in range(21):
                cycle = await cycle_row(
                    session,
                    authority,
                    AT + timedelta(minutes=index),
                    batch_id=batch.batch_id,
                    manifest=(candidate,),
                )
                await run_and_job(
                    session,
                    kind=kind,
                    cycle=cycle,
                    model=model,
                    prompt=prompt,
                    candidate=candidate,
                    batch_id=batch.batch_id,
                )
                ours.append(cycle.id)
            other = await foreign_account(session)
            other_batch = AgentPaperCandidateBatchRecord(
                id=uuid4(),
                account_id=other.id,
                universe_snapshot_id=authority.universe_snapshot_id,
                candidate_instrument_ids=[candidate.instrument_id],
                source="explicit_research_scope",
                batch_hash="f" * 64,
                created_at=AT,
            )
            session.add(other_batch)
            await session.flush()
            outsider = await cycle_row(
                session,
                authority,
                AT + timedelta(minutes=30),
                batch_id=other_batch.id,
                account=other,
                manifest=(candidate,),
            )
            await run_and_job(
                session,
                kind=kind,
                cycle=outsider,
                model=model,
                prompt=prompt,
                candidate=candidate,
                batch_id=other_batch.id,
                live=True,
            )
        now = AT + timedelta(days=2)
        async with sessions() as session:
            unsettled = set(await session.scalars(_unsettled_cycles(now)))
            assert unsettled == {outsider.id}
            assert not unsettled.intersection(ours)
            result = await read_activity(session, read_at=now, decision_session=now.date())
            formal = result.scopes[0]
            assert formal.reason_code != "WORKBENCH_ACTIVITY_LIMIT"
            assert formal.selection == "historical"
            assert formal.cycle_id == ours[-1]
            assert formal.occupancy == "not_indicated"
            assert formal.today_started == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("cycle_count,selection", [(2, "ambiguous"), (21, "unavailable")])
async def test_unresolved_cycle_selection_never_claims_waiting_for_cycle(
    migrated_database_url, monkeypatch, cycle_count, selection
):
    engine, sessions, authority, _, _, _ = await scope_fixture(migrated_database_url, monkeypatch)
    try:
        async with sessions.begin() as session:
            for index in range(cycle_count):
                await cycle_row(session, authority, AT + timedelta(minutes=index))
        async with sessions() as session:
            result = await read_activity(
                session, read_at=AT + timedelta(hours=1), decision_session=AT.date()
            )
            formal = result.scopes[0]
            assert formal.selection == selection
            assert formal.portfolio_status == "unavailable"
            assert formal.cycle_id is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect", ["event_occurred_future", "event_created_future", "request_future", "foreign_account"]
)
async def test_unavailable_manual_link_cannot_hide_valid_scheduled_cycle_or_count(
    migrated_database_url, monkeypatch, defect
):
    engine, sessions, authority, _, model, batch = await scope_fixture(
        migrated_database_url, monkeypatch
    )
    try:
        async with sessions.begin() as session:
            scheduled = await cycle_row(session, authority, AT)
            owner = await foreign_account(session) if defect == "foreign_account" else None
            at = AT + timedelta(hours=2) if defect == "request_future" else AT
            row = ManualPaperRequest(
                id=uuid4(),
                account_id=authority.account_id if owner is None else owner.id,
                candidate_batch_id=batch.batch_id,
                model_config_id=model.id,
                mode="research_only",
                contract_version=1,
                research_profile=None,
                key_hash="d" * 64,
                request_hash=manual_request_hash(
                    account_id=authority.account_id if owner is None else owner.id,
                    batch_id=batch.batch_id,
                    mode="research_only",
                ),
                batch_hash=batch.batch_hash,
                model_hash=model.config_hash,
                requested_at=at,
                created_at=at,
            )
            session.add(row)
            await session.flush()
            # Deliberately unverified linkage: reading it must never reclassify
            # the valid current-account cycle before time/account validation.
            event = ManualPaperRequestEvent(
                request_id=row.id,
                sequence=1,
                status="active",
                code="MANUAL_CYCLE_CREATED",
                cycle_id=scheduled.id,
                occurred_at=AT + timedelta(hours=2) if defect == "event_occurred_future" else AT,
                created_at=AT + timedelta(hours=2) if defect == "event_created_future" else AT,
            )
            event.event_hash = _event_hash(row, event, None)
            session.add(event)
        async with sessions() as session:
            result = await read_activity(
                session, read_at=AT + timedelta(hours=1), decision_session=AT.date()
            )
            formal = result.scopes[0]
            assert formal.today_started == 1
            assert formal.selection == "active"
            assert formal.cycle_id == scheduled.id
            assert formal.trigger == "scheduled"
            assert formal.portfolio_status != "awaiting_cycle"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_pending_request_limit_does_not_claim_a_specific_cycle_is_absent(
    migrated_database_url, monkeypatch
):
    engine, sessions, authority, _, model, batch = await scope_fixture(
        migrated_database_url, monkeypatch
    )
    try:
        async with sessions.begin() as session:
            for index in range(21):
                await request_row(session, authority, model, batch, AT + timedelta(minutes=index))
        async with sessions() as session:
            formal = (
                await read_activity(
                    session, read_at=AT + timedelta(hours=1), decision_session=AT.date()
                )
            ).scopes[0]
            assert formal.selection == "unavailable"
            assert formal.reason_code == "WORKBENCH_ACTIVITY_LIMIT"
            assert formal.portfolio_status == "unavailable"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("future_field", ["occurred_at", "created_at"])
async def test_future_cycle_links_do_not_pull_21_closed_requests_into_pending(
    migrated_database_url, monkeypatch, future_field
):
    engine, sessions, authority, manifest, model, batch = await scope_fixture(
        migrated_database_url, monkeypatch
    )
    try:
        async with sessions.begin() as session:
            candidate = LegacyCandidateManifestEntry(
                instrument_id=manifest[0].instrument_id,
                symbol=manifest[0].symbol,
                inclusion_reason="synthetic future association",
                research_snapshot_id=manifest[0].research_snapshot_id,
            )
            cycle = await cycle_row(
                session, authority, AT, batch_id=batch.batch_id, manifest=(candidate,)
            )
            prompt = await session.scalar(
                select(AgentPromptVersionRecord)
                .where(AgentPromptVersionRecord.role == "orchestrator")
                .limit(1)
            )
            await run_and_job(
                session,
                kind="research",
                cycle=cycle,
                model=model,
                prompt=prompt,
                candidate=candidate,
                batch_id=batch.batch_id,
                live=True,
            )
            for index in range(21):
                at = AT + timedelta(minutes=index)
                row = await request_row(session, authority, model, batch, at)
                await _append(
                    session,
                    row,
                    status="unavailable",
                    code="MANUAL_INPUTS_UNAVAILABLE",
                    cycle_id=None,
                    at=at + timedelta(seconds=1),
                )
                prior = await session.scalar(
                    select(ManualPaperRequestEvent)
                    .where(ManualPaperRequestEvent.request_id == row.id)
                    .order_by(ManualPaperRequestEvent.sequence.desc())
                    .limit(1)
                )
                event = ManualPaperRequestEvent(
                    request_id=row.id,
                    sequence=3,
                    status="unavailable",
                    code="MANUAL_INPUTS_UNAVAILABLE",
                    cycle_id=cycle.id,
                    occurred_at=AT + timedelta(hours=2)
                    if future_field == "occurred_at"
                    else at + timedelta(seconds=2),
                    created_at=AT + timedelta(hours=2)
                    if future_field == "created_at"
                    else at + timedelta(seconds=2),
                )
                event.event_hash = _event_hash(row, event, prior.event_hash)
                session.add(event)
                await session.flush()
            # Keep the latest request independently valid; old future associations
            # must be excluded before the bounded pending scan is materialized.
            latest = await request_row(session, authority, model, batch, AT + timedelta(minutes=25))
            await _append(
                session,
                latest,
                status="unavailable",
                code="MANUAL_INPUTS_UNAVAILABLE",
                cycle_id=None,
                at=AT + timedelta(minutes=25, seconds=1),
            )
        async with sessions() as session:
            formal = (
                await read_activity(
                    session, read_at=AT + timedelta(hours=1), decision_session=AT.date()
                )
            ).scopes[0]
            assert formal.reason_code != "WORKBENCH_ACTIVITY_LIMIT"
            assert formal.selection == "active"
            assert formal.trigger == "scheduled"
            assert formal.cycle_id == cycle.id
            assert formal.today_started == 1
    finally:
        await engine.dispose()
