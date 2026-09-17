"""Persisted read-model coverage for the internal-paper workbench."""

from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import pytest
from helpers.paper_api import settings
from helpers.paper_batch import (
    EXECUTION_SESSION,
    UNIVERSE_ID,
    seed_paper_batch,
)
from helpers.paper_execution import seed_paper_execution
from nexus.ops.agent_paper_loop import heartbeat_agent_paper_loop
from nexus.paper.agent_cycle_models import candidate_manifest_hash, canonical_cycle_event_hash
from nexus.paper.read_service import PaperReadService
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
)
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.workbench.read_service import WorkbenchReadService
from sqlalchemy import select

ACTIVE_DECISION_TIME = datetime(2026, 8, 27, 14, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_workbench_reads_online_loop_active_order_and_empty_account(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_execution(migrated_database_url)
    request_time = datetime(2026, 8, 27, 13, 45, tzinfo=UTC)
    heartbeat_time = datetime.now(UTC)
    await heartbeat_agent_paper_loop(
        settings(migrated_database_url),
        worker_id="agent-paper-loop-72000000-0000-0000-0000-000000000001",
        started_at=heartbeat_time - timedelta(minutes=1),
        seen_at=heartbeat_time,
    )

    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        result = await WorkbenchReadService(session, request_time).read()

    assert result.mode == "internal_paper"
    assert result.execution_locked is True
    assert result.loop.status == "online"
    assert result.loop.reason_code is None
    assert result.loop.last_seen_at == heartbeat_time
    assert result.compatibility is not None
    assert result.compatibility.status == "compatible"
    assert result.compatibility.database_migration_heads == ("0064_prompt_release_channel",)
    assert result.readiness is not None
    assert result.readiness.previous_cycle_id is None
    assert result.latest_cycle is None
    assert len(result.active_orders) == 2
    assert seed.order_id in {order.order_id for order in result.active_orders}
    assert result.account is not None
    assert result.account.data_state == "EMPTY"
    assert result.account.nav is None
    assert result.attention == ()


@pytest.mark.asyncio
async def test_workbench_keeps_no_order_cycle_empty_and_reports_only_stale_loop(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    cycle_id = UUID("72000000-0000-0000-0000-000000000301")
    request_time = ACTIVE_DECISION_TIME + timedelta(minutes=1)
    await _seed_no_order_cycle(
        migrated_database_url,
        account_id=seed.account_id,
        portfolio_id=seed.portfolio_id,
        cycle_id=cycle_id,
    )
    await heartbeat_agent_paper_loop(
        settings(migrated_database_url),
        worker_id="agent-paper-loop-72000000-0000-0000-0000-000000000002",
        started_at=request_time - timedelta(minutes=10),
        seen_at=request_time - timedelta(seconds=31),
    )

    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        result = await WorkbenchReadService(session, request_time).read()

    assert result.loop.status == "offline"
    assert result.loop.reason_code == "HEARTBEAT_STALE"
    assert result.latest_cycle is not None
    assert result.latest_cycle.cycle_id == cycle_id
    assert result.latest_cycle.result_code == "NO_ORDER_REQUIRED"
    assert result.active_orders == ()
    assert [(item.kind, item.reason_code) for item in result.attention] == [
        ("service", "HEARTBEAT_STALE")
    ]


@pytest.mark.asyncio
async def test_workbench_reports_incompatible_agent_paper_heartbeat(
    migrated_database_url: str,
) -> None:
    request_time = datetime(2026, 8, 27, 13, 45, tzinfo=UTC)
    await heartbeat_agent_paper_loop(
        settings(migrated_database_url),
        worker_id="agent-paper-loop-72000000-0000-0000-0000-000000000003",
        started_at=request_time - timedelta(minutes=1),
        seen_at=request_time - timedelta(seconds=5),
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        heartbeat = await session.scalar(
            select(WorkerHeartbeat).where(
                WorkerHeartbeat.worker_id == "agent-paper-loop-72000000-0000-0000-0000-000000000003"
            )
        )
        assert heartbeat is not None
        heartbeat.process_version = "agent-paper-loop-obsolete"

    async with factory() as session:
        result = await WorkbenchReadService(session, request_time).read()

    assert result.loop.status == "offline"
    assert result.loop.reason_code == "HEARTBEAT_INCOMPATIBLE"


@pytest.mark.asyncio
async def test_cycle_detail_is_owned_time_bounded_and_has_linked_orders_only(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    cycle_id = UUID("72000000-0000-0000-0000-000000000302")
    await _seed_no_order_cycle(
        migrated_database_url,
        account_id=seed.account_id,
        portfolio_id=seed.portfolio_id,
        cycle_id=cycle_id,
        future_completion_at=ACTIVE_DECISION_TIME + timedelta(minutes=2),
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        request_time = ACTIVE_DECISION_TIME + timedelta(minutes=1)
        detail = await PaperReadService(session, request_time).agent_cycle_detail(cycle_id)
        workbench = await WorkbenchReadService(session, request_time).read()

    assert detail is not None
    assert detail.cycle.status == "risk_evaluated"
    assert detail.cycle.result_code == "PAPER_BATCH_EVALUATED"
    assert detail.orders == ()
    assert workbench.latest_cycle is not None
    assert workbench.latest_cycle.status == "risk_evaluated"
    assert workbench.session.completed_count == 0
    assert await _read_missing_cycle(migrated_database_url, UUID(int=999)) is None


async def _seed_no_order_cycle(
    database_url: str,
    *,
    account_id: UUID,
    portfolio_id: UUID,
    cycle_id: UUID,
    future_completion_at: datetime | None = None,
    decision_time: datetime = ACTIVE_DECISION_TIME,
) -> None:
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        session.add(
            AgentPaperCycleRecord(
                id=cycle_id,
                account_id=account_id,
                portfolio_id=portfolio_id,
                decision_session=EXECUTION_SESSION,
                decision_time=decision_time,
                decision_slot_at=decision_time,
                execution_session=date(2026, 8, 28),
                universe_snapshot_id=UNIVERSE_ID,
                quality_report_id=None,
                decision_price_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
                execution_price_policy_id=PAPER_EXECUTION_SOURCE_POLICY_ID,
                candidate_manifest=[],
                candidate_hash=candidate_manifest_hash(()),
                sizing_policy_version="agent_paper_sizing_v1",
                sizing_policy_hash="9" * 64,
                execution_locked=True,
            )
        )
        await session.flush()
        events = [
            _cycle_event(cycle_id, 1, None, "prepared", "CYCLE_PREPARED"),
            _cycle_event(cycle_id, 2, "prepared", "researching", "RESEARCH_ADMITTED"),
            _cycle_event(
                cycle_id,
                3,
                "researching",
                "decision_recorded",
                "AGENT_DECISION_RECORDED",
            ),
            _cycle_event(
                cycle_id,
                4,
                "decision_recorded",
                "risk_evaluated",
                "PAPER_BATCH_EVALUATED",
            ),
            _cycle_event(
                cycle_id,
                5,
                "risk_evaluated",
                "completed",
                "NO_ORDER_REQUIRED",
                occurred_at=future_completion_at or ACTIVE_DECISION_TIME,
            ),
        ]
        session.add_all(events)


def _cycle_event(
    cycle_id: UUID,
    sequence: int,
    from_status: str | None,
    to_status: str,
    result_code: str,
    occurred_at: datetime = ACTIVE_DECISION_TIME,
) -> AgentPaperCycleEventRecord:
    fact_ids: dict[str, object] = {}
    return AgentPaperCycleEventRecord(
        cycle_id=cycle_id,
        sequence=sequence,
        from_status=from_status,
        to_status=to_status,
        result_code=result_code,
        fact_ids=fact_ids,
        occurred_at=occurred_at,
        event_hash=canonical_cycle_event_hash(
            cycle_id=cycle_id,
            sequence=sequence,
            from_status=from_status,
            to_status=to_status,
            result_code=result_code,
            fact_ids=fact_ids,
            occurred_at=occurred_at,
        ),
    )


async def _read_missing_cycle(database_url: str, cycle_id: UUID):
    factory = session_factory(settings(database_url))
    async with factory() as session:
        return await PaperReadService(
            session, ACTIVE_DECISION_TIME + timedelta(minutes=1)
        ).agent_cycle_detail(cycle_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["identity", "future"])
async def test_workbench_never_calls_invalid_heartbeat_online(
    migrated_database_url: str,
    invalid: str,
) -> None:
    request_time = datetime.now(UTC)
    worker_id = "agent-paper-loop-72000000-0000-0000-0000-000000000399"
    if invalid == "identity":
        worker_id = "agent-paper-loop-not-a-uuid"
    await heartbeat_agent_paper_loop(
        settings(migrated_database_url),
        worker_id=worker_id,
        started_at=request_time - timedelta(minutes=1),
        seen_at=request_time + timedelta(hours=1) if invalid == "future" else request_time,
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        result = await WorkbenchReadService(session, request_time).read()
    assert result.loop.status == "unknown"
    assert result.loop.reason_code == "HEARTBEAT_INVALID"
    assert result.compatibility is not None
    assert result.compatibility.status == "unknown"


@pytest.mark.asyncio
async def test_readiness_round_trips_persisted_adjacent_cycles_through_read_only_api(
    migrated_database_url: str,
) -> None:
    import httpx
    from helpers.paper_api import app, headers
    from sqlalchemy import text

    seed = await seed_paper_batch(migrated_database_url)
    previous_id, latest_id = UUID(int=401), UUID(int=402)
    await _seed_no_order_cycle(
        migrated_database_url,
        account_id=seed.account_id,
        portfolio_id=seed.portfolio_id,
        cycle_id=previous_id,
        decision_time=ACTIVE_DECISION_TIME - timedelta(minutes=5),
    )
    await _seed_no_order_cycle(
        migrated_database_url,
        account_id=seed.account_id,
        portfolio_id=seed.portfolio_id,
        cycle_id=latest_id,
        future_completion_at=ACTIVE_DECISION_TIME + timedelta(hours=1),
    )
    request_time = ACTIVE_DECISION_TIME + timedelta(minutes=1)
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        result = await WorkbenchReadService(session, request_time).read()
    assert result.readiness is not None
    assert result.readiness.previous_cycle_id == previous_id
    assert result.readiness.previous_result_code == "NO_ORDER_REQUIRED"
    assert result.readiness.stage == "execution"
    assert result.readiness.changes == ("stage", "result")
    path = "/internal/v1/workbench"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=request_time)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    assert response.json()["data"] == result.model_dump(mode="json")
