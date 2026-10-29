"""Formal scheduled and manual entry share historical research safety."""

import json
from datetime import timedelta
from decimal import Decimal
from unittest.mock import Mock
from uuid import UUID, uuid4

import httpx
import pytest
from helpers.paper_api import headers
from helpers.paper_api import settings as api_settings
from helpers.paper_batch import CALENDAR_ID, INSTRUMENTS
from integration.test_manual_paper_request_pg import AT
from integration.test_portfolio_same_day_research_pg import seed_normal_path
from nexus.main import create_engine_app
from nexus.ops import agent_paper_once
from nexus.ops.agent_paper_loop import heartbeat_agent_paper_loop
from nexus.ops.openai_local import prepare_database
from nexus.ops.research_admit_local import admit_local_research
from nexus.paper import agent_cycle
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.manual_request import admit_manual_request
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import IdempotencyRecord, Job, JobLease
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from unit.ops.test_full_input_capacity_registration import V9_OPTIONS


@pytest.mark.asyncio
async def test_completed_scheduled_v10_history_admission(migrated_database_url, monkeypatch):
    # This file is in the default merge gate. Reuse the existing controlled
    # three-stock Worker -> orders -> fills -> ledger path, including pending
    # orders and post-completion lease checks, without broadening that CI job.
    from integration.test_candidate_multistock_path import (
        test_fresh_multistock_candidate_cycle_freezes_native_jobs,
    )

    await test_fresh_multistock_candidate_cycle_freezes_native_jobs(
        migrated_database_url, monkeypatch, count=3, trade=True, profile="formal_v10"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shape",
    [
        "valid",
        "duplicate_missing",
        "wrong_family",
        "extra_payload",
        "queued",
        "running",
        "live_lease",
        "expired_lease",
        "unknown_usage",
        "active_cycle",
        "late_lease",
    ],
)
async def test_formal_entries_require_settled_matching_historical_jobs(
    migrated_database_url, monkeypatch, tmp_path, shape
):
    # This matrix constructs archived failed/unknown history, without native stage facts.
    from helpers.native_execution import use_historical_research_preparation

    use_historical_research_preparation(monkeypatch)
    await prepare_database(
        EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk"),
        **{**V9_OPTIONS, "formal_flexible_stage_budget": True},
    )
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, candidate_instruments=INSTRUMENTS, decision_time=AT
    )
    original_scalar = AsyncSession.scalar
    observed_at = AT + timedelta(seconds=2) if shape == "expired_lease" else AT

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return observed_at
        return await original_scalar(self, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)
    app = None
    try:
        async with sessions.begin() as session:
            next_at = AT + timedelta(days=3)
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=next_at.date(),
                    decision_at=next_at.replace(hour=21),
                    market_open_at=next_at.replace(hour=13, minute=30),
                    market_close_at=next_at.replace(hour=20),
                    created_at=AT - timedelta(hours=1),
                )
            )
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == "codex-sdk-chatgpt-v10-flexible-stage-budget"
                )
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(item.instrument_id for item in manifest),
                idempotency_key="historical-binding-batch",
                created_at=AT,
            )
            result = await agent_cycle._start_cycle(
                session,
                authority=authority,
                decision_slot_at=AT,
                daily_limit_usd=Decimal("3"),
                candidate_batch_id=batch.batch_id,
                requested_instrument_ids=batch.instrument_ids,
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            assert len(result.research_run_ids) == 3
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            if shape != "active_cycle":
                await agent_cycle._append_event(
                    session,
                    cycle,
                    "researching",
                    "unavailable",
                    "RESEARCH_OUTPUT_NOT_ACTIONABLE",
                    {},
                )
            runs = list(await session.scalars(select(AgentResearchRunRecord)))
            jobs = list(await session.scalars(select(Job).order_by(Job.id)))
            assert len(jobs) == len(runs) == 3
            assert all(
                job.retry_policy == "automatic_safe" and job.max_attempts == 1 for job in jobs
            )
            for run in runs:
                run.status = "failed"
                run.settled_cost_usd = Decimal("0")
            if shape == "unknown_usage":
                runs[0].settled_cost_usd = None
            for job in jobs:
                if shape == "queued" and job == jobs[0]:
                    continue
                job.status = "running"
                job.started_at = AT
                job.attempt_count = 1
                job.version += 1
            await session.flush()
            for job in jobs:
                if shape in {"queued", "running"} and job == jobs[0]:
                    continue
                job.status = "dead_letter"
                job.finished_at = AT
                job.version += 1
            await session.flush()
            if shape in {"live_lease", "expired_lease"}:
                session.add(
                    JobLease(
                        job_id=jobs[0].id,
                        worker_id="historical-binding-test",
                        expires_at=(
                            AT + timedelta(minutes=1)
                            if shape == "live_lease"
                            else AT + timedelta(seconds=1)
                        ),
                        lease_token=uuid4(),
                        fence_token=1,
                        acquired_at=AT - timedelta(minutes=1),
                        heartbeat_at=AT - timedelta(minutes=1),
                    )
                )
                await session.flush()
            if shape in {"duplicate_missing", "wrong_family", "extra_payload"}:
                # Corruption fixture only, in a freshly provisioned test database.
                # Normal producers cannot mutate immutable Job authority this way.
                await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
                if shape == "duplicate_missing":
                    jobs[1].payload = dict(jobs[0].payload)
                elif shape == "wrong_family":
                    jobs[1].job_type = "agent.portfolio_synthesis"
                else:
                    jobs[1].payload = {**jobs[1].payload, "unexpected": True}
                await session.flush()
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
            before = [
                (run.id, run.status, run.reserved_cost_usd, run.settled_cost_usd) for run in runs
            ]
            budget_query = (
                select(
                    AgentDailyBudgetReservationRecord.run_id,
                    AgentDailyBudgetReservationRecord.state,
                    AgentDailyBudgetReservationRecord.reserved_usd,
                    AgentDailyBudgetReservationRecord.settled_usd,
                )
                .where(AgentDailyBudgetReservationRecord.run_id.in_(result.research_run_ids))
                .order_by(AgentDailyBudgetReservationRecord.run_id)
            )
            before_budgets = (await session.execute(budget_query)).all()
            assert len(before_budgets) == 3
        settings = EngineSettings(
            **{
                **api_settings(migrated_database_url).model_dump(),
                "agent_runtime_mode": "codex_sdk",
                "agent_paper_account_id": authority.account_id,
                "agent_paper_candidate_batch_id": batch.batch_id,
                "agent_paper_formal_model_config_id": model.id,
                "agent_paper_cycle_enabled": True,
                "agent_paper_manual_enabled": True,
                "release_revision": "a" * 40,
            }
        )
        # The independent research CLI is not a second formal v10 entry.
        # Check the same history without invoking a model or acquiring a new Run.
        independent = tmp_path / "independent.json"
        independent.write_text(
            json.dumps(
                {
                    "request": {
                        "instrument_id": str(manifest[0].instrument_id),
                        "data_snapshot_id": str(manifest[0].research_snapshot_id),
                        "decision_time": AT.isoformat(),
                    },
                    "model_config_id": str(model.id),
                    "idempotency_key": "cross-entry-independent-v10",
                }
            )
        )
        with pytest.raises(ValueError, match="^DAILY_ONCE_RESEARCH_MODEL_UNAVAILABLE$"):
            await admit_local_research(settings, independent)

        admitted = shape in {"valid", "expired_lease", "late_lease"}
        # Scheduled reservation is the gate before any provider collection.
        # Roll the probe back so the subsequent HTTP request sees identical history.
        async with sessions() as session:
            transaction = await session.begin()
            try:
                if admitted:
                    queued = await admit_manual_request(
                        session,
                        account_id=authority.account_id,
                        batch_id=batch.batch_id,
                        model_config_id=model.id,
                        mode="research_and_paper",
                        idempotency_key="queued-yields-to-scheduled",
                    )
                    assert queued.status == "queued"

                async def reserve():
                    return await agent_cycle.reserve_schwab_agent_paper_trigger(
                        session,
                        account_id=authority.account_id,
                        candidate_batch_id=batch.batch_id,
                        client_idempotency_key="cross-entry-scheduled",
                        now=observed_at,
                        formal_stage_capacity=True,
                        formal_model_config_id=model.id,
                    )

                if admitted:
                    assert (await reserve()).replay is None
                else:
                    with pytest.raises(ValueError, match="^MANUAL_ACCOUNT_BUSY_OR_UNKNOWN$"):
                        await reserve()
            finally:
                await transaction.rollback()
        if shape == "late_lease":
            # A producer can commit after reservation has released its lock.
            # The next boundary must observe that newly persisted occupancy.
            async with sessions.begin() as session:
                session.add(
                    JobLease(
                        job_id=jobs[0].id,
                        worker_id="late-historical-lease",
                        expires_at=AT + timedelta(minutes=1),
                        lease_token=uuid4(),
                        fence_token=1,
                        acquired_at=AT,
                        heartbeat_at=AT,
                    )
                )
            admitted = False
        # Monitor admission may qualify a completed scheduled execution, but
        # that option cannot waive exact Job identity, known usage or leases.
        from nexus.paper.manual_request import require_manual_account_available

        async with sessions.begin() as session:
            if admitted:
                await require_manual_account_available(
                    session,
                    account_id=authority.account_id,
                    now=observed_at,
                    allow_scheduled_completion=True,
                )
            else:
                with pytest.raises(ValueError, match="^MANUAL_ACCOUNT_BUSY_OR_UNKNOWN$"):
                    await require_manual_account_available(
                        session,
                        account_id=authority.account_id,
                        now=observed_at,
                        allow_scheduled_completion=True,
                    )
        # Provider collection releases the reservation transaction. Recheck
        # history at cycle creation before admitting any additional research.
        async with sessions() as session:
            transaction = await session.begin()
            try:

                async def start():
                    return await agent_cycle._start_cycle(
                        session,
                        authority=authority,
                        decision_slot_at=AT + timedelta(seconds=1),
                        daily_limit_usd=Decimal("3"),
                        candidate_batch_id=batch.batch_id,
                        requested_instrument_ids=batch.instrument_ids,
                        formal_stage_capacity=True,
                        formal_model_config_id=model.id,
                    )

                if admitted:
                    assert len((await start()).research_run_ids) == 3
                else:
                    with pytest.raises(ValueError, match="^MANUAL_ACCOUNT_BUSY_OR_UNKNOWN$"):
                        await start()
            finally:
                await transaction.rollback()
        if not admitted:
            # Exercise the normal producer's own settings/session/transaction,
            # not just the reservation helper. Source collection must not begin.
            store = Mock(side_effect=AssertionError("provider boundary reached"))
            monkeypatch.setattr(agent_paper_once, "S3ProviderEvidenceStore", store)
            trigger_settings = agent_paper_once.AgentPaperTriggerSettings.model_validate(
                settings.model_dump()
                | {
                    "object_store_endpoint": "http://localhost:9000",
                    "alpaca_key": "fixture",
                    "alpaca_secret": "fixture",
                }
            )
            trigger_request = agent_paper_once.AgentPaperTriggerRequest(
                action="trigger",
                account_id=authority.account_id,
                candidate_batch_id=batch.batch_id,
                idempotency_key="cross-entry-normal-trigger",
                formal_stage_capacity=True,
                formal_model_config_id=model.id,
            )
            with pytest.raises(ValueError, match="^MANUAL_ACCOUNT_BUSY_OR_UNKNOWN$") as refused:
                await agent_paper_once.trigger_once(
                    trigger_settings, trigger_request, repo_root=tmp_path
                )
            assert refused.value._nexus_agent_paper_failure_phase == "trigger_reservation"
            store.assert_not_called()
            async with sessions() as session:
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(IdempotencyRecord)
                        .where(IdempotencyRecord.route == "agent-paper-schwab-trigger-v2")
                    )
                    == 0
                )
        await heartbeat_agent_paper_loop(
            settings,
            account_id=authority.account_id,
            worker_id=f"agent-paper-loop-{UUID(int=1)}",
            started_at=AT - timedelta(minutes=1),
            seen_at=AT,
        )
        app = create_engine_app(settings)
        path = "/internal/v1/paper/manual-requests"
        body = json.dumps({"mode": "research_and_paper"}).encode()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                path,
                content=body,
                headers={**headers("POST", path, body), "Idempotency-Key": "independent-new"},
            )
        if admitted:
            assert response.status_code == 200, response.text
            assert response.json()["data"]["status"] == "queued"
            # Missing legacy Attempt history is not permission to replay a v10
            # Job. Exercise the normal retry API with its exact current version.
            retry_path = f"/internal/v1/jobs/{jobs[0].id}/retry"
            retry_body = b"{}"
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                retry = await client.post(
                    retry_path,
                    content=retry_body,
                    headers={
                        **headers("POST", retry_path, retry_body),
                        "Idempotency-Key": "old-research-must-not-replay",
                        "If-Match": f'"{jobs[0].version}"',
                    },
                )
            assert retry.status_code == 403, retry.text
            assert retry.json()["error"]["code"] == "FORBIDDEN"
        else:
            assert response.status_code == 409, response.text
            assert (
                response.json()["error"]["details"]["reason_code"]
                == "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
            )
        async with sessions.begin() as session:
            assert await session.scalar(select(func.count()).select_from(ManualPaperRequest)) == (
                1 if admitted else 0
            )
            old_job = await session.get(Job, jobs[0].id)
            assert (old_job.status, old_job.version, old_job.manual_retry_count) == (
                jobs[0].status,
                jobs[0].version,
                0,
            )
            after = list(await session.scalars(select(AgentResearchRunRecord)))
            assert sorted(before) == sorted(
                (run.id, run.status, run.reserved_cost_usd, run.settled_cost_usd) for run in after
            )
            assert (await session.execute(budget_query)).all() == before_budgets
    finally:
        if app is not None:
            await app.state.session_factory.kw["bind"].dispose()
        await engine.dispose()
