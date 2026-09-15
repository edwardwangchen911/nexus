"""Fail-closed foreground scheduler for Agent internal-paper cycles."""

import argparse
import asyncio
import json
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from pathlib import Path
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.ops.agent_paper_once import (
    AgentPaperCollectWindowRequest,
    AgentPaperMarketSettings,
    AgentPaperResumeRequest,
    AgentPaperTriggerRequest,
    AgentPaperTriggerSettings,
    collect_window_once,
    resume_once,
    trigger_once,
)
from nexus.ops.alpaca_adjusted_authority_import import import_adjusted_authority
from nexus.ops.alpaca_prices_local import collect_prices
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.openai_local import local_settings
from nexus.ops.paper_once import PaperAdvanceRequest
from nexus.ops.paper_once import run_once as run_paper_once
from nexus.paper.agent_cycle_authority import load_quasi_realtime_cycle_authority
from nexus.paper.agent_cycle_models import floor_decision_slot
from nexus.paper.agent_market import resolve_agent_paper_market_scope
from nexus.paper.errors import PaperPerformanceError
from nexus.paper.performance import publish_paper_performance_snapshot
from nexus.paper.read_service import PaperReadService
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    PaperPerformanceSnapshotRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.research.alpaca_prices_archive import collection_window
from nexus.risk.query_authority_producer import inspect_query_authority_scope
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once


class AgentPaperLoopAction(StrEnum):
    TRIGGER = "trigger"
    RUN_RESEARCH_JOB = "run_research_job"
    RESUME = "resume"
    COLLECT_WINDOW = "collect_window"
    ADVANCE_ORDER = "advance_order"
    PUBLISH_PERFORMANCE = "publish_performance"
    WAIT_FOR_SLOT = "wait_for_slot"
    WAIT_FOR_MARKET = "wait_for_market"
    STOP_RESULT_UNKNOWN = "stop_result_unknown"
    STOP_CYCLE_UNAVAILABLE = "stop_cycle_unavailable"
    STOP_ORDER_UNAVAILABLE = "stop_order_unavailable"
    STOP_PERFORMANCE_UNAVAILABLE = "stop_performance_unavailable"
    STOP_PREREQUISITE_UNAVAILABLE = "stop_prerequisite_unavailable"
    STOP_DISABLED = "stop_disabled"
    STOP_SESSION_CLOSED = "stop_session_closed"


@dataclass(frozen=True)
class AgentPaperLoopSnapshot:
    now: datetime
    enabled: bool = True
    cycle_id: UUID | None = None
    cycle_status: str | None = None
    cycle_slot_at: datetime | None = None
    research_job_status: str | None = None
    research_job_id: UUID | str | None = None
    market_window_ready: bool = False
    market_window_end_at: datetime | None = None
    has_open_order: bool = False
    order_id: UUID | None = None
    performance_due_at: datetime | None = None


@dataclass(frozen=True)
class PriorDailyScope:
    instrument_id: UUID
    prior_session: date
    authority_ready: bool


_TERMINAL_JOB_STATUSES = frozenset({"succeeded", "failed", "dead_letter", "skipped"})
_MARKET_PUBLICATION_LAG = timedelta(seconds=60)


async def _inspect_prior_daily_scope(
    settings: EngineSettings, *, account_id: UUID, now: datetime
) -> PriorDailyScope:
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    database = make_url(settings.database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            authority = await load_quasi_realtime_cycle_authority(
                session, account_id=account_id, now=now
            )
            scope = await inspect_query_authority_scope(
                session,
                decision_session=authority.decision_session,
                symbol="AAPL",
                as_of=now,
                universe_snapshot_id=authority.universe_snapshot_id,
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
            )
            prior_session = await session.scalar(
                select(ExchangeCalendarSessionRecord.session)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == authority.calendar_authority_id,
                    ExchangeCalendarSessionRecord.session < authority.decision_session,
                )
                .order_by(ExchangeCalendarSessionRecord.session.desc())
                .limit(1)
            )
            if not isinstance(prior_session, date):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            existing = await session.scalar(
                select(PriceBarSetAuthority.id)
                .join(
                    PriceSeriesManifest,
                    PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
                )
                .join(
                    PriceBarRecord,
                    PriceBarRecord.price_series_manifest_id == PriceSeriesManifest.id,
                )
                .where(
                    PriceBarSetAuthority.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
                    PriceBarSetAuthority.available_at <= now,
                    PriceBarSetAuthority.created_at <= now,
                    PriceBarRecord.instrument_id == scope.instrument_id,
                    PriceBarRecord.session == prior_session,
                    PriceBarRecord.available_at <= now,
                    PriceBarRecord.created_at <= now,
                    PriceSeriesManifest.created_at <= now,
                    PriceSeriesManifest.source == "alpaca",
                    PriceSeriesManifest.feed == "sip",
                    PriceSeriesManifest.delivery_mode == "delayed",
                    PriceSeriesManifest.adjustment == "all",
                    PriceSeriesManifest.interval == "1Day",
                )
                .order_by(PriceBarSetAuthority.available_at.desc(), PriceBarSetAuthority.id)
                .limit(1)
            )
            return PriorDailyScope(
                instrument_id=scope.instrument_id,
                prior_session=prior_session,
                authority_ready=existing is not None,
            )
    finally:
        await engine.dispose()


async def prepare_prior_daily_once(
    settings: AgentPaperTriggerSettings,
    *,
    account_id: UUID,
    repo_root: Path,
    now: datetime,
) -> str:
    """Create at most one exact prior-session daily authority before a cycle."""
    scope = await _inspect_prior_daily_scope(settings, account_id=account_id, now=now)
    if scope.authority_ready:
        return "ready"
    parent = repo_root / ".secrets" / "nexus-agent-paper" / "daily"
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    output_dir = parent / scope.prior_session.isoformat()
    try:
        output_dir.mkdir(mode=0o700)
    except FileExistsError:
        return "unavailable"
    try:
        request, calendar = collection_window(
            scope.instrument_id,
            scope.prior_session,
            scope.prior_session,
            "all",
            now=now,
        )
        collected = await collect_prices(
            request,
            symbol="AAPL",
            calendar=calendar,
            resolved_at=now,
            key_id=settings.alpaca_key,
            secret=settings.alpaca_secret,
            output_dir=output_dir,
            max_pages=10,
            identity_asof=scope.prior_session,
        )
        if collected.get("status") != "collected" or collected.get("readback_verified") is not True:
            return "unavailable"
        imported = await import_adjusted_authority(
            settings,
            instrument_id=scope.instrument_id,
            collection_dir=output_dir,
            purpose="agent-paper-decision",
        )
        if imported.get("status") != "stored_authority":
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    except ValueError:
        return "unavailable"
    verified = await _inspect_prior_daily_scope(
        settings, account_id=account_id, now=datetime.now(UTC)
    )
    if (
        not verified.authority_ready
        or verified.instrument_id != scope.instrument_id
        or verified.prior_session != scope.prior_session
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    return "prepared"


def resolve_cycle_research_job(
    *, events: Sequence[object], jobs: Sequence[object]
) -> tuple[UUID, str] | None:
    """Resolve exactly one Job whose persisted run ID belongs to the cycle."""
    run_ids: list[str] = []
    for event in events:
        fact_ids = getattr(event, "fact_ids", None)
        raw = fact_ids.get("research_run_ids") if isinstance(fact_ids, dict) else None
        if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
            run_ids.extend(raw)
    if len(run_ids) != 1 or len(jobs) != 1:
        return None
    job = jobs[0]
    payload = getattr(job, "payload", None)
    job_id = getattr(job, "id", None)
    status = getattr(job, "status", None)
    if (
        not isinstance(payload, dict)
        or payload.get("run_id") != run_ids[0]
        or not isinstance(job_id, UUID)
        or not isinstance(status, str)
    ):
        return None
    return job_id, status


def snapshot_from_cycle_view(
    *,
    now: datetime,
    view: object,
    research_job: tuple[UUID, str] | None,
    market_scope: object | None,
    performance_due_at: datetime | None = None,
) -> AgentPaperLoopSnapshot:
    """Map the bounded read projection into the scheduler's fail-closed contract."""
    cycle_id = getattr(view, "cycle_id", None)
    cycle_status = getattr(view, "status", None)
    cycle_slot_at = getattr(view, "decision_slot_at", None)
    order_ids = getattr(view, "order_ids", ())
    execution_market = getattr(view, "execution_market", None)
    market_status = getattr(execution_market, "status", None)
    if (
        not isinstance(cycle_id, UUID)
        or not isinstance(cycle_status, str)
        or not isinstance(cycle_slot_at, datetime)
        or not isinstance(order_ids, tuple)
        or any(not isinstance(value, UUID) for value in order_ids)
        or len(order_ids) > 1
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    order_id = order_ids[0] if order_ids else None
    has_open_order = cycle_status == "awaiting_market" and order_id is not None
    market_window_ready = has_open_order and market_status == "qualified"
    market_window_end_at = None
    if has_open_order and not market_window_ready:
        scope_cycle_id = getattr(market_scope, "cycle_id", None)
        market_window_end_at = getattr(market_scope, "window_end", None)
        if scope_cycle_id != cycle_id or not isinstance(market_window_end_at, datetime):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        market_window_end_at += _MARKET_PUBLICATION_LAG
    return AgentPaperLoopSnapshot(
        now=now,
        cycle_id=cycle_id,
        cycle_status=cycle_status,
        cycle_slot_at=cycle_slot_at,
        research_job_status=None if research_job is None else research_job[1],
        research_job_id=None if research_job is None else research_job[0],
        market_window_ready=market_window_ready,
        market_window_end_at=market_window_end_at,
        has_open_order=has_open_order,
        order_id=order_id,
        performance_due_at=performance_due_at,
    )


def choose_loop_action(snapshot: AgentPaperLoopSnapshot) -> AgentPaperLoopAction:
    """Choose one safe next action without performing I/O or retrying unknown work."""
    if not snapshot.enabled:
        return AgentPaperLoopAction.STOP_DISABLED

    if snapshot.cycle_status == "researching":
        if snapshot.research_job_status == "queued":
            return AgentPaperLoopAction.RUN_RESEARCH_JOB
        if snapshot.research_job_status in _TERMINAL_JOB_STATUSES:
            return AgentPaperLoopAction.RESUME
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN

    if (
        snapshot.cycle_status == "awaiting_market"
        and snapshot.has_open_order
        and snapshot.market_window_ready
    ):
        return AgentPaperLoopAction.ADVANCE_ORDER

    if snapshot.cycle_status == "awaiting_market" and snapshot.has_open_order:
        if snapshot.market_window_end_at is None:
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        if snapshot.now < snapshot.market_window_end_at:
            return AgentPaperLoopAction.WAIT_FOR_MARKET
        return AgentPaperLoopAction.COLLECT_WINDOW

    if snapshot.cycle_status in {"decision_recorded", "risk_evaluated"}:
        return AgentPaperLoopAction.RESUME

    if snapshot.performance_due_at is not None:
        return AgentPaperLoopAction.PUBLISH_PERFORMANCE

    local_time = snapshot.now.astimezone(ZoneInfo("America/New_York"))
    cycle_local_date = (
        None
        if snapshot.cycle_slot_at is None
        else snapshot.cycle_slot_at.astimezone(ZoneInfo("America/New_York")).date()
    )
    if snapshot.cycle_status in {"unavailable", "failed"} and (
        cycle_local_date is None or cycle_local_date == local_time.date()
    ):
        return AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE

    if snapshot.cycle_status not in {None, "completed", "unavailable", "failed"}:
        return AgentPaperLoopAction.STOP_RESULT_UNKNOWN

    if not time(7) <= local_time.time().replace(tzinfo=None) < time(16):
        return AgentPaperLoopAction.STOP_SESSION_CLOSED
    if snapshot.cycle_slot_at == floor_decision_slot(snapshot.now):
        return AgentPaperLoopAction.WAIT_FOR_SLOT
    return AgentPaperLoopAction.TRIGGER


async def run_foreground_loop(
    *,
    inspect: Callable[[], Awaitable[AgentPaperLoopSnapshot]],
    execute: Callable[
        [AgentPaperLoopAction, AgentPaperLoopSnapshot],
        Awaitable[AgentPaperLoopAction | None],
    ],
    sleep: Callable[[float], Awaitable[None]],
) -> AgentPaperLoopAction:
    """Run persisted one-step actions until a fail-closed stop condition is reached."""
    while True:
        snapshot = await inspect()
        action = choose_loop_action(snapshot)
        if action in {
            AgentPaperLoopAction.STOP_RESULT_UNKNOWN,
            AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE,
            AgentPaperLoopAction.STOP_DISABLED,
            AgentPaperLoopAction.STOP_SESSION_CLOSED,
        }:
            return action
        if action == AgentPaperLoopAction.WAIT_FOR_SLOT:
            next_slot = floor_decision_slot(snapshot.now) + timedelta(minutes=5)
            await sleep((next_slot - snapshot.now).total_seconds())
            continue
        if action == AgentPaperLoopAction.WAIT_FOR_MARKET:
            assert snapshot.market_window_end_at is not None
            await sleep((snapshot.market_window_end_at - snapshot.now).total_seconds())
            continue
        stop = await execute(action, snapshot)
        if stop is not None:
            return stop


async def run_local_agent_paper_loop(
    settings: EngineSettings,
    *,
    account_id: UUID,
    repo_root: Path,
) -> AgentPaperLoopAction:
    """Run the concrete local loop through persisted inspection and scoped actions."""

    async def inspect() -> AgentPaperLoopSnapshot:
        current = EngineSettings.model_validate(load_local_environment(repo_root, {}))
        if not current.agent_paper_cycle_enabled:
            return AgentPaperLoopSnapshot(now=datetime.now(UTC), enabled=False)
        return await inspect_agent_paper_loop(current, account_id=account_id)

    async def execute(
        action: AgentPaperLoopAction, snapshot: AgentPaperLoopSnapshot
    ) -> AgentPaperLoopAction | None:
        return await execute_agent_paper_loop_action(
            settings,
            account_id=account_id,
            repo_root=repo_root,
            action=action,
            snapshot=snapshot,
        )

    return await run_foreground_loop(inspect=inspect, execute=execute, sleep=asyncio.sleep)


async def execute_agent_paper_loop_action(
    settings: EngineSettings,
    *,
    account_id: UUID,
    repo_root: Path,
    action: AgentPaperLoopAction,
    snapshot: AgentPaperLoopSnapshot,
) -> AgentPaperLoopAction | None:
    """Execute exactly one planner-approved action through existing product services."""
    if action == AgentPaperLoopAction.TRIGGER:
        environment = load_local_environment(repo_root, {})
        trigger_settings = AgentPaperTriggerSettings.model_validate(environment)
        prior_daily = await prepare_prior_daily_once(
            trigger_settings,
            account_id=account_id,
            repo_root=repo_root,
            now=snapshot.now,
        )
        if prior_daily == "prepared":
            return None
        if prior_daily == "unavailable":
            return AgentPaperLoopAction.STOP_PREREQUISITE_UNAVAILABLE
        if prior_daily != "ready":
            return AgentPaperLoopAction.STOP_RESULT_UNKNOWN
        await trigger_once(
            trigger_settings,
            AgentPaperTriggerRequest(
                action="trigger",
                account_id=account_id,
                idempotency_key=(
                    f"agent-paper-loop:{floor_decision_slot(snapshot.now).isoformat()}"
                ),
            ),
            repo_root=repo_root,
        )
        return None
    if action == AgentPaperLoopAction.RUN_RESEARCH_JOB:
        if not isinstance(snapshot.research_job_id, UUID):
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        worked = await run_agent_worker_once(
            local_settings(repo_root, backend="codex_sdk"),
            job_ids=(snapshot.research_job_id,),
        )
        if not worked:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        return None
    if action == AgentPaperLoopAction.PUBLISH_PERFORMANCE:
        if snapshot.performance_due_at is None:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        published = await publish_due_performance_once(
            settings,
            account_id=account_id,
            as_of=snapshot.performance_due_at,
        )
        if not published:
            return AgentPaperLoopAction.STOP_PERFORMANCE_UNAVAILABLE
        return None
    if snapshot.cycle_id is None:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    if action == AgentPaperLoopAction.RESUME:
        await resume_once(
            settings,
            AgentPaperResumeRequest(
                action="resume",
                cycle_id=snapshot.cycle_id,
                idempotency_key=f"agent-paper-loop-resume:{snapshot.cycle_id}",
            ),
        )
        return None
    if action == AgentPaperLoopAction.COLLECT_WINDOW:
        environment = load_local_environment(repo_root, {})
        await collect_window_once(
            AgentPaperMarketSettings.model_validate(environment),
            AgentPaperCollectWindowRequest(
                action="collect-window", cycle_id=snapshot.cycle_id
            ),
            repo_root=repo_root,
        )
        return None
    if action == AgentPaperLoopAction.ADVANCE_ORDER and snapshot.order_id is not None:
        result = await run_paper_once(
            settings,
            PaperAdvanceRequest(
                action="advance",
                account_id=account_id,
                order_id=snapshot.order_id,
                evaluated_at=snapshot.now,
                idempotency_key=f"agent-paper-loop-advance:{snapshot.order_id}",
            ),
        )
        if result.get("action") in {"no_data", "no_capacity"}:
            return AgentPaperLoopAction.STOP_ORDER_UNAVAILABLE
        return None
    raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")


async def publish_due_performance_once(
    settings: EngineSettings,
    *,
    account_id: UUID,
    as_of: datetime,
) -> bool:
    """Publish one exact close, stopping cleanly when its price facts are incomplete."""
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    database = make_url(settings.database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory.begin() as session:
                await publish_paper_performance_snapshot(
                    session,
                    account_id=account_id,
                    as_of=as_of,
                )
        except PaperPerformanceError as error:
            if error.code == "PAPER_PERFORMANCE_PRICE_INCOMPLETE":
                return False
            raise
        return True
    finally:
        await engine.dispose()


async def inspect_agent_paper_loop(
    settings: EngineSettings, *, account_id: UUID
) -> AgentPaperLoopSnapshot:
    """Read one bounded scheduler snapshot without starting or replaying work."""
    if not settings.database_url:
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    database = make_url(settings.database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            service = PaperReadService(session, now)
            account, _ = await service.account()
            if account.id != account_id:
                raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
            latest_completed_close = await session.scalar(
                select(func.max(ExchangeCalendarSessionRecord.market_close_at))
                .join(
                    ExchangeCalendarAuthorityRecord,
                    ExchangeCalendarAuthorityRecord.id
                    == ExchangeCalendarSessionRecord.calendar_authority_id,
                )
                .where(
                    ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                    ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                    ExchangeCalendarAuthorityRecord.published_at
                    <= ExchangeCalendarSessionRecord.market_close_at,
                    ExchangeCalendarSessionRecord.market_close_at <= now,
                )
            )
            performance_due_at = None
            if isinstance(latest_completed_close, datetime):
                existing_performance = await session.scalar(
                    select(PaperPerformanceSnapshotRecord.id).where(
                        PaperPerformanceSnapshotRecord.account_id == account_id,
                        PaperPerformanceSnapshotRecord.as_of == latest_completed_close,
                    )
                )
                if existing_performance is None:
                    performance_due_at = latest_completed_close
            view = await service.latest_agent_cycle()
            if view is None:
                return AgentPaperLoopSnapshot(
                    now=now,
                    performance_due_at=performance_due_at,
                )

            events: tuple[object, ...] = ()
            jobs: tuple[object, ...] = ()
            if view.status == "researching":
                events = tuple(
                    await session.scalars(
                        select(AgentPaperCycleEventRecord)
                        .where(AgentPaperCycleEventRecord.cycle_id == view.cycle_id)
                        .order_by(AgentPaperCycleEventRecord.sequence)
                    )
                )
                run_ids: list[str] = []
                for event in events:
                    fact_ids = getattr(event, "fact_ids", None)
                    raw = fact_ids.get("research_run_ids") if isinstance(fact_ids, dict) else None
                    if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
                        run_ids.extend(raw)
                if run_ids:
                    jobs = tuple(
                        await session.scalars(
                            select(Job).where(
                                Job.job_type == "agent.research",
                                Job.payload["run_id"].as_string().in_(tuple(run_ids)),
                            )
                        )
                    )
            research_job = resolve_cycle_research_job(events=events, jobs=jobs)
            market_scope = None
            if (
                view.status == "awaiting_market"
                and len(view.order_ids) == 1
                and view.execution_market.status != "qualified"
            ):
                market_scope = await resolve_agent_paper_market_scope(
                    session, cycle_id=view.cycle_id
                )
            return snapshot_from_cycle_view(
                now=now,
                view=view,
                research_job=research_job,
                market_scope=market_scope,
                performance_due_at=performance_due_at,
            )
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nexus-engine-agent-paper-loop")
    parser.add_argument("--account-id", required=True)
    args = parser.parse_args(argv)
    root = Path.cwd()
    try:
        try:
            account_id = UUID(args.account_id)
        except (AttributeError, TypeError, ValueError):
            print(json.dumps({"error_code": "AGENT_PAPER_ACCOUNT_INVALID"}))
            return 1
        settings = EngineSettings.model_validate(load_local_environment(root, {}))
        if not settings.agent_paper_cycle_enabled:
            print(json.dumps({"error_code": "AGENT_PAPER_CYCLE_DISABLED"}))
            return 1
        reason = asyncio.run(
            run_local_agent_paper_loop(settings, account_id=account_id, repo_root=root)
        )
        if reason == AgentPaperLoopAction.STOP_RESULT_UNKNOWN:
            raise RuntimeError("AGENT_PAPER_LOOP_RESULT_UNKNOWN")
        if reason == AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE:
            print(json.dumps({"error_code": "AGENT_PAPER_LOOP_CYCLE_UNAVAILABLE"}))
            return 1
        if reason == AgentPaperLoopAction.STOP_PERFORMANCE_UNAVAILABLE:
            print(json.dumps({"error_code": "AGENT_PAPER_LOOP_PERFORMANCE_UNAVAILABLE"}))
            return 1
        if reason == AgentPaperLoopAction.STOP_PREREQUISITE_UNAVAILABLE:
            print(json.dumps({"error_code": "AGENT_PAPER_LOOP_PREREQUISITE_UNAVAILABLE"}))
            return 1
    except KeyboardInterrupt:
        print(json.dumps({"error_code": "AGENT_PAPER_LOOP_INTERRUPTED"}))
        return 130
    except Exception:
        print(json.dumps({"error_code": "AGENT_PAPER_LOOP_RESULT_UNKNOWN"}))
        return 1
    print(
        json.dumps(
            {"execution_locked": True, "status": "stopped", "reason": reason.value}
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
