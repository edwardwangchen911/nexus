"""A manual root's snapshot must not drift to the later portfolio snapshot."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.agents.committee import FixtureCommittee
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.agents.tools import ToolInputError
from nexus.jobs.daily_manual import (
    ManualDailyRequest,
    build_manual_daily_registry,
    enqueue_manual_daily_root,
    manual_daily_job_ids,
)
from nexus.jobs.daily_research import admit_daily_research, resolve_daily_research_job
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import claim_next_job, enqueue_job, fail_job
from nexus.jobs.worker import run_job_worker_once
from nexus.ops.daily_once import (
    DailyOnceError,
    DailyOnceSettings,
    DailyResearchAdmitRequest,
    DailyResearchRunRequest,
    run_once,
)
from nexus.ops.openai_local import MODEL_ID, prepare_database
from nexus.optimizer.research_context import freeze_daily_research_context
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.models.runtime import Job, JobDependency
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import func, select


async def _snapshot(session, portfolio, *, offset=0, held=True, status="READY"):
    at = NOW + timedelta(seconds=offset)
    sync = BrokerSyncRun(
        account_id=portfolio.account_id,
        portfolio_id=portfolio.id,
        trigger_kind="MANUAL",
        actor_id="synthetic",
        route="explicit-context",
        idempotency_key=str(offset),
        request_hash="c" * 64,
        status=status,
        accepted_at=at,
        completed_at=at,
        attempt_count=1,
        cash_flow_quality="PASS" if status == "READY" else "NOT_RUN",
        created_at=at,
    )
    session.add(sync)
    await session.flush()
    snapshot = PortfolioSnapshotRecord(
        portfolio_id=portfolio.id,
        sync_run_id=sync.id,
        status=status,
        as_of=at,
        position_hash_version="broker_position_hash_v1",
        position_hash="d" * 64,
        order_hash_version="broker_order_hash_v1",
        order_hash="e" * 64,
        source_request_ids=[],
        source_observations=[],
        created_at=at,
    )
    session.add(snapshot)
    await session.flush()
    session.add(
        CashSnapshot(
            portfolio_snapshot_id=snapshot.id,
            currency="USD",
            cash_balance=100,
            available_cash=100,
            provider_as_of=at,
            created_at=at,
        )
    )
    if held:
        session.add(
            PositionSnapshot(
                portfolio_snapshot_id=snapshot.id,
                instrument_id=INSTRUMENT,
                currency="USD",
                quantity=10,
                available_quantity=10,
                frozen_quantity=0,
                avg_cost=10,
                diluted_cost=10,
                current_price=10,
                provider_as_of=at,
                created_at=at,
            )
        )
    await session.flush()
    return snapshot


async def _seed(session):
    await seed_filing(session)
    account = Account(provider="fosun", status="ACTIVE", created_at=NOW)
    session.add(account)
    await session.flush()
    portfolio = Portfolio(account_id=account.id, name="synthetic", created_at=NOW)
    session.add(portfolio)
    await session.flush()
    return portfolio, await _snapshot(session, portfolio)


@pytest.mark.asyncio
async def test_explicit_binding_preserves_old_hash_and_does_not_choose_later_snapshot(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        portfolio, old = await _seed(session)
        original = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        explicit = await prepare_research_snapshot(
            session,
            INSTRUMENT,
            as_of=NOW,
            portfolio_snapshot_id=old.id,
        )
        assert original is not None and explicit is not None
        assert original.id == explicit.id and original.revision == explicit.revision
        original_payload = dict(original.payload)
        latest = await _snapshot(session, portfolio, offset=1)
        later = NOW + timedelta(seconds=2)
        selected = await prepare_research_snapshot(
            session,
            INSTRUMENT,
            as_of=later,
            portfolio_snapshot_id=old.id,
        )
        default = await prepare_research_snapshot(session, INSTRUMENT, as_of=later)
        assert selected is not None and default is not None
        assert selected.payload["portfolio_context"]["portfolio_snapshot_id"] == str(old.id)
        assert default.payload["portfolio_context"]["portfolio_snapshot_id"] == str(latest.id)
        assert selected.revision != default.revision
        await session.refresh(original)
        assert original.payload == original_payload


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["missing", "future", "not_held"])
async def test_explicit_missing_context_does_not_fall_back_to_generic_research(
    migrated_database_url,
    case,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        portfolio, _ = await _seed(session)
        target_id = UUID(int=999)
        if case != "missing":
            target = await _snapshot(session, portfolio, offset=1, held=case != "not_held")
            target_id = target.id
        at = NOW if case == "future" else NOW + timedelta(seconds=2)
        with pytest.raises(ValueError, match="OPENAI_PORTFOLIO_CONTEXT_UNAVAILABLE"):
            await prepare_research_snapshot(
                session,
                INSTRUMENT,
                as_of=at,
                portfolio_snapshot_id=target_id,
            )
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0


@pytest.mark.asyncio
async def test_explicit_partial_snapshot_does_not_fall_back_to_ready(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        portfolio, _ = await _seed(session)
        partial = await _snapshot(session, portfolio, offset=1, status="PARTIAL")
        result = await prepare_research_snapshot(
            session,
            INSTRUMENT,
            as_of=NOW + timedelta(seconds=2),
            portfolio_snapshot_id=partial.id,
        )
        assert result is not None
        context = result.payload["portfolio_context"]
        assert context["portfolio_snapshot_id"] == str(partial.id)
        assert context["snapshot_decision_ready"] is False
        assert context["execution_locked"] is True


async def _daily_case(url, *, ready=True, advance=True, staged=False):
    settings = EngineSettings(
        database_url=url,
        agent_runtime_mode="codex_sdk" if staged else "fixture",
    )
    await prepare_database(settings, staged=staged)
    factory = session_factory(settings)
    close = NOW - timedelta(hours=8)
    async with factory() as session, session.begin():
        portfolio, snapshot = await _seed(session)
        if not ready:
            snapshot = await _snapshot(session, portfolio, offset=1, status="PARTIAL")
        calendar = ExchangeCalendarAuthorityRecord(
            exchange="XNYS",
            version="daily-research-test",
            source="fixture",
            range_start=close.date(),
            range_end=close.date(),
            query_parameters={},
            completeness_status="complete",
            content_hash="f" * 64,
            published_at=close,
        )
        session.add(calendar)
        await session.flush()
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=calendar.id,
                session=close.date(),
                market_open_at=close - timedelta(hours=6, minutes=30),
                market_close_at=close,
                decision_at=close + timedelta(hours=1),
            )
        )
        await session.flush()
    async with factory() as session, session.begin():
        root = await enqueue_manual_daily_root(
            session,
            request=ManualDailyRequest(
                snapshot_id=snapshot.id,
                calendar_authority_id=calendar.id,
                session=close.date(),
                cutoff_at=datetime.now(UTC),
            ),
            now=datetime.now(UTC),
        )
        model_id = await session.scalar(select(AgentModelConfigRecord.id))
    if advance:
        for _ in range(3):
            async with factory() as session:
                ids = await manual_daily_job_ids(session, root_id=root.id)
            assert await run_job_worker_once(
                factory,
                registry=build_manual_daily_registry(artifact_reader=FixtureArtifactReader({})),
                queue=QueueName.GENERAL,
                worker_id="daily-research-test",
                worker_started_at=NOW,
                process_version="test",
                migration_head="0036",
                clock=lambda: datetime.now(UTC),
                job_ids=ids,
                claim_registered_only=True,
            )
    return factory, root, snapshot, model_id


def _admission(root, model_id, *, at=None):
    return dict(
        root_id=root.id,
        instrument_id=INSTRUMENT,
        research_as_of=at or datetime.now(UTC),
        model_config_id=model_id,
        daily_limit_usd=Decimal("3"),
        now=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_daily_research_admits_one_native_job_with_exact_dependency_and_frozen_context(
    migrated_database_url,
):
    factory, root, snapshot, model_id = await _daily_case(migrated_database_url)
    request = _admission(root, model_id)
    async with factory() as session, session.begin():
        portfolio = await session.get(Portfolio, snapshot.portfolio_id)
        await _snapshot(session, portfolio, offset=2)
        first = await admit_daily_research(session, **request)
        replay = await admit_daily_research(session, **request)
        assert not first.replayed and replay.replayed and replay.run_id == first.run_id
        job = await resolve_daily_research_job(session, root_id=root.id, run_id=first.run_id)
        assert job.payload == {"run_id": str(first.run_id)}
        assert job.schema_version == "agent_research_job_v1"
        assert job.job_type == "agent.research"
        ids = await manual_daily_job_ids(session, root_id=root.id)
        assert len(ids) == 3 and job.id not in ids
        assert tuple(
            (
                await session.execute(
                    select(
                        JobDependency.depends_on_job_id,
                        JobDependency.required_outcome,
                    ).where(JobDependency.job_id == job.id)
                )
            ).all()
        ) == ((ids[-1], "succeeded"),)
        data = await session.get(DataSnapshot, first.run.data_snapshot_id)
        assert data.payload["portfolio_context"]["portfolio_snapshot_id"] == str(snapshot.id)
        assert first.run.decision_time == request["research_as_of"]
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 1
        assert (
            await session.scalar(
                select(func.count()).select_from(AgentDailyBudgetReservationRecord)
            )
            == 1
        )
    async with factory() as session:
        assert (
            await resolve_daily_research_job(session, root_id=root.id, run_id=first.run_id)
        ).id == job.id


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["pending", "blocked", "before_diagnostic", "future"])
async def test_daily_research_unready_or_wrong_cutoff_reserves_nothing(migrated_database_url, case):
    factory, root, _, model_id = await _daily_case(
        migrated_database_url,
        ready=case != "blocked",
        advance=case != "pending",
    )
    at = root.effective_at if case == "before_diagnostic" else datetime.now(UTC)
    if case == "future":
        at += timedelta(hours=1)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="MANUAL_DAILY_RESEARCH_"):
            await admit_daily_research(session, **_admission(root, model_id, at=at))
        for table in (AgentResearchRunRecord, AgentDailyBudgetReservationRecord, DataSnapshot):
            assert await session.scalar(select(func.count()).select_from(table)) == 0


@pytest.mark.asyncio
async def test_daily_research_bad_model_rolls_back_all_new_authority_and_budget(
    migrated_database_url,
):
    factory, root, _, _ = await _daily_case(migrated_database_url)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError):
            await admit_daily_research(session, **_admission(root, UUID(int=999)))
        for table in (AgentResearchRunRecord, AgentDailyBudgetReservationRecord, DataSnapshot):
            assert await session.scalar(select(func.count()).select_from(table)) == 0
        assert await session.scalar(select(func.count()).select_from(Job)) == 3


@pytest.mark.asyncio
async def test_daily_research_resolver_rejects_unrelated_run(migrated_database_url):
    factory, root, _, model_id = await _daily_case(migrated_database_url)
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="MANUAL_DAILY_RESEARCH_BINDING_INVALID"):
            await resolve_daily_research_job(session, root_id=root.id, run_id=UUID(int=999))


@pytest.mark.asyncio
async def test_daily_research_never_backfills_dependency_on_existing_native_run(
    migrated_database_url,
):
    factory, root, snapshot, model_id = await _daily_case(migrated_database_url)
    request = _admission(root, model_id)
    async with factory() as session, session.begin():
        ids = await manual_daily_job_ids(session, root_id=root.id)
        data = await prepare_research_snapshot(
            session,
            INSTRUMENT,
            as_of=request["research_as_of"],
            portfolio_snapshot_id=snapshot.id,
        )
        digest = job_input_hash(
            {
                "schema": "manual-daily-research-v1",
                "root_id": str(root.id),
                "diagnostic_job_id": str(ids[-1]),
                "instrument_id": str(INSTRUMENT),
                "research_as_of": request["research_as_of"].isoformat(),
                "model_config_id": str(model_id),
            }
        )
        native = await admit_research_run(
            session,
            actor_id="local-daily-research",
            route="manual-daily-research-v1",
            idempotency_key=digest,
            request_hash=digest,
            request=ResearchRequest(
                instrument_id=INSTRUMENT,
                data_snapshot_id=data.id,
                decision_time=request["research_as_of"],
            ),
            prompt_version_id=None,
            model_config_id=model_id,
            daily_limit_usd=request["daily_limit_usd"],
            now=request["now"],
        )
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="MANUAL_DAILY_RESEARCH_BINDING_INVALID"):
            await admit_daily_research(session, **request)
        job = await session.scalar(
            select(Job).where(
                Job.payload["run_id"].as_string() == str(native.run_id),
            )
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(JobDependency)
                .where(
                    JobDependency.job_id == job.id,
                )
            )
            == 0
        )
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 1
        assert (
            await session.scalar(
                select(func.count()).select_from(
                    AgentDailyBudgetReservationRecord,
                )
            )
            == 1
        )


def _research_settings(url):
    return DailyOnceSettings(
        database_url=url,
        agent_runtime_mode="responses",
        openai_api_key="synthetic-test-key",
        openai_reasoning_model=MODEL_ID,
        openai_extraction_model=MODEL_ID,
        object_store_endpoint="http://127.0.0.1:19000",
        object_store_region="test-region",
        object_store_bucket="synthetic-test",
        object_store_access_key_id="synthetic-key",
        object_store_secret_access_key="synthetic-secret",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("future_availability", [False, True])
async def test_daily_research_cli_calls_native_worker_only_for_bound_job(
    migrated_database_url,
    monkeypatch,
    future_availability,
):
    factory, root, _, model_id = await _daily_case(migrated_database_url)
    settings = _research_settings(migrated_database_url)

    async def unavailable(*args, **kwargs):
        raise ToolInputError("synthetic unavailable evidence; no external call")

    monkeypatch.setattr("nexus.worker._committee", unavailable)
    admission = await run_once(
        settings,
        DailyResearchAdmitRequest(
            action="research-admit",
            root_id=root.id,
            instrument_id=INSTRUMENT,
            research_as_of=datetime.now(UTC),
            model_config_id=model_id,
        ),
    )
    assert admission["status"] == "prepared"
    run_id = UUID(admission["run_id"])
    async with factory() as session, session.begin():
        others = []
        for label in ("expired", "queued"):
            payload = {"run_id": str(UUID(int=500 if label == "expired" else 501))}
            other = await enqueue_job(
                session,
                key=JobKeyV1(
                    job_type=JobType.AGENT_RESEARCH,
                    scope_kind="instrument",
                    scope_id=label,
                    effective_at=NOW,
                    input_hash=job_input_hash(payload),
                ),
                schema_version="agent_research_job_v1",
                payload=payload,
                queue=QueueName.GENERAL,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                available_at=NOW,
            )
            others.append(other.id)
        assert await claim_next_job(
            session, queue=QueueName.GENERAL, worker_id="other-owner", now=NOW, job_ids=(others[0],)
        )
        if future_availability:
            target = await resolve_daily_research_job(session, root_id=root.id, run_id=run_id)
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="previous-target-owner",
                now=datetime.now(UTC),
                job_ids=(target.id,),
            )
            assert claim is not None
            await fail_job(
                session,
                claim=claim,
                now=datetime.now(UTC),
                error_code="SYNTHETIC_WAIT",
                retry_delay_seconds=60,
            )
    request = DailyResearchRunRequest(action="research-run", root_id=root.id, run_id=run_id)
    result = await run_once(settings, request)
    assert result["processed"] == (0 if future_availability else 1)
    assert result["status"] == ("pending" if future_availability else "blocked")
    assert result["run_status"] == ("prepared" if future_availability else "failed")
    assert (await run_once(settings, request))["processed"] == 0
    async with factory() as session:
        expired = await session.get(Job, others[0])
        queued = await session.get(Job, others[1])
        assert (expired.status, expired.attempt_count) == ("running", 1)
        assert (queued.status, queued.attempt_count) == ("queued", 0)
        assert (await session.get(AgentResearchRunRecord, run_id)).status == (
            "prepared" if future_availability else "failed"
        )


@pytest.mark.asyncio
async def test_daily_research_cli_wrong_runtime_model_never_reserves(migrated_database_url):
    factory, root, _, model_id = await _daily_case(migrated_database_url)
    settings = _research_settings(migrated_database_url).model_copy(
        update={"agent_runtime_mode": AgentRuntimeMode.CODEX_SDK},
    )
    with pytest.raises(DailyOnceError, match="DAILY_ONCE_RESEARCH_MODEL_UNAVAILABLE"):
        await run_once(
            settings,
            DailyResearchAdmitRequest(
                action="research-admit",
                root_id=root.id,
                instrument_id=INSTRUMENT,
                research_as_of=datetime.now(UTC),
                model_config_id=model_id,
            ),
        )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        assert (
            await session.scalar(
                select(func.count()).select_from(
                    AgentDailyBudgetReservationRecord,
                )
            )
            == 0
        )


@pytest.mark.asyncio
async def test_daily_research_claim_guard_rechecks_before_heartbeat_or_claim(
    migrated_database_url,
    monkeypatch,
):
    from nexus.jobs.daily_pipeline import assert_daily_pipeline_schema_ready
    from nexus.persistence.models.runtime import WorkerHeartbeat

    factory, root, _, model_id = await _daily_case(migrated_database_url)
    settings = _research_settings(migrated_database_url)
    admission = await run_once(
        settings,
        DailyResearchAdmitRequest(
            action="research-admit",
            root_id=root.id,
            instrument_id=INSTRUMENT,
            research_as_of=datetime.now(UTC),
            model_config_id=model_id,
        ),
    )
    run_id = UUID(admission["run_id"])
    calls = 0

    async def guard(session):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ValueError("DAILY_PIPELINE_DATABASE_HEAD_MISMATCH")
        await assert_daily_pipeline_schema_ready(session)

    monkeypatch.setattr("nexus.ops.daily_once.assert_daily_pipeline_schema_ready", guard)
    async with factory() as session:
        heartbeats = await session.scalar(select(func.count()).select_from(WorkerHeartbeat))
    with pytest.raises(DailyOnceError, match="DAILY_PIPELINE_DATABASE_HEAD_MISMATCH"):
        await run_once(
            settings,
            DailyResearchRunRequest(
                action="research-run",
                root_id=root.id,
                run_id=run_id,
            ),
        )
    assert calls == 2
    async with factory() as session:
        job = await resolve_daily_research_job(session, root_id=root.id, run_id=run_id)
        assert (job.status, job.attempt_count) == ("queued", 0)
        assert (await session.get(AgentResearchRunRecord, run_id)).status == "prepared"
        assert await session.scalar(select(func.count()).select_from(WorkerHeartbeat)) == heartbeats


async def _finished_daily_research(url, monkeypatch, *, failed=False, staged=False):
    factory, root, snapshot, model_id = await _daily_case(url, staged=staged)
    async with factory() as session, session.begin():
        admission = await admit_daily_research(session, **_admission(root, model_id))
        job = await resolve_daily_research_job(session, root_id=root.id, run_id=admission.run_id)
    if failed:

        async def unavailable(*args, **kwargs):
            raise ToolInputError("synthetic unavailable evidence")

        monkeypatch.setattr("nexus.worker._committee", unavailable)
    elif staged:

        async def fixture(*args, **kwargs):
            return FixtureCommittee()

        monkeypatch.setattr("nexus.worker._committee", fixture)
    assert await run_agent_worker_once(
        EngineSettings(
            database_url=url,
            agent_runtime_mode="codex_sdk" if staged else "fixture",
        ),
        job_ids=(job.id,),
    )
    return factory, root, snapshot, admission.run_id


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("staged", [False, True])
async def test_daily_research_context_preserves_actual_output_time_and_unknowns(
    migrated_database_url,
    monkeypatch,
    failed,
    staged,
):
    factory, root, snapshot, run_id = await _finished_daily_research(
        migrated_database_url,
        monkeypatch,
        failed=failed,
        staged=staged,
    )
    at = datetime.now(UTC)
    async with factory() as session:
        context = await freeze_daily_research_context(
            session,
            root_id=root.id,
            run_id=run_id,
            context_as_of=at,
        )
        assert context.mode == "context_only" and context.execution_locked
        assert context.portfolio_snapshot_id == snapshot.id
        assert context.valuation_time == snapshot.as_of
        assert context.output_available_at <= at and context.output_available_at > root.effective_at
        assert context.research_as_of > context.valuation_time
        assert context.run_status == ("failed" if failed else "completed")
        assert context.quality_status == "partial"
        assert (context.intent is None) is failed
        assert context.unavailable_reason == ("RESEARCH_FAILED" if failed else None)
        again = await freeze_daily_research_context(
            session,
            root_id=root.id,
            run_id=run_id,
            context_as_of=at,
        )
        assert again == context
        with pytest.raises(ValueError, match="DRAFT_RESEARCH_OUTPUT_UNAVAILABLE"):
            await freeze_daily_research_context(
                session,
                root_id=root.id,
                run_id=run_id,
                context_as_of=root.effective_at,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["hash", "evidence"])
async def test_daily_research_context_rejects_new_invalid_output_without_changing_old_context(
    migrated_database_url,
    monkeypatch,
    invalid,
):
    factory, root, _, run_id = await _finished_daily_research(migrated_database_url, monkeypatch)
    cutoff = datetime.now(UTC)
    async with factory() as session:
        frozen = await freeze_daily_research_context(
            session,
            root_id=root.id,
            run_id=run_id,
            context_as_of=cutoff,
        )
    async with factory() as session, session.begin():
        old = await session.get(AgentResearchOutputRecord, frozen.output_id)
        session.add(
            AgentResearchOutputRecord(
                run_id=run_id,
                sequence=old.sequence + 1,
                status=old.status,
                role=None,
                prompt_hash=None,
                content_hash="0" * 64 if invalid == "hash" else old.content_hash,
                payload=old.payload,
                intent=old.intent,
                # The shared PG fixture freezes default created_at to an older
                # clock; this test specifically needs a genuinely later row.
                created_at=datetime.now(UTC),
            )
        )
    async with factory() as session:
        assert (
            await freeze_daily_research_context(
                session,
                root_id=root.id,
                run_id=run_id,
                context_as_of=cutoff,
            )
            == frozen
        )
        with pytest.raises(ValueError, match="DRAFT_RESEARCH_OUTPUT_INVALID"):
            await freeze_daily_research_context(
                session,
                root_id=root.id,
                run_id=run_id,
                context_as_of=datetime.now(UTC),
            )
