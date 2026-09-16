"""Redacted PostgreSQL read models for the private internal-paper console."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import and_, desc, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.paper import (
    AgentPaperAlpacaExecutionMarketResponse,
    AgentPaperCandidateResponse,
    AgentPaperCycleDetailResponse,
    AgentPaperCyclePageResponse,
    AgentPaperCycleResponse,
    AgentPaperDecisionMarketResponse,
    AgentPaperDecisionResponse,
    AgentPaperSchwabExecutionMarketResponse,
    AgentPaperSessionSummaryResponse,
    AgentPaperVersionObservationResponse,
    PaperAttributionResponse,
    PaperComparisonResponse,
    PaperComparisonRowResponse,
    PaperFillResponse,
    PaperOrderEventResponse,
    PaperOrderPageResponse,
    PaperOrderResponse,
    PaperPerformanceResponse,
    PaperPortfolioResponse,
    PaperPositionResponse,
)
from nexus.domain.decimal import decimal_to_string
from nexus.domain.time import require_utc
from nexus.market.universe import stable_instrument_id
from nexus.paper.agent_cycle_models import (
    CandidateManifestEntry,
    floor_decision_slot,
    parse_candidate_manifest,
)
from nexus.paper.cursor import PaperOrderCursorCodec, PaperOrderCursorState
from nexus.paper.models import InternalOrderStatus
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
    StrategyDefinitionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.evolution import StrategyRegistryEventRecord
from nexus.persistence.models.market import InstrumentAlias, LatestQuote
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    AgentPaperMarketWindowRecord,
    PaperAccountRecord,
    PaperExecutionWindowAuthorityRecord,
    PaperFillRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceAttributionRecord,
    PaperPerformanceSnapshotRecord,
    PaperPositionSnapshotRecord,
    PaperReservationEventRecord,
    PaperReservationRecord,
    RiskEvaluationRecord,
    RiskNoChangeEvaluationRecord,
    RiskPolicyRecord,
    StrategyDecisionAgentOutputRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.portfolio import Portfolio

_MAX_POSITIONS = 500
_MAX_ATTRIBUTIONS = 500
_MAX_EVENTS = 128
_MAX_FILLS = 128
_MAX_AGENT_CYCLES_PER_SESSION = 108
_MAX_AGENT_ORDERS_PER_CYCLE = 50
_STALE_AFTER = timedelta(hours=36)


def market_benchmark_row_from_closes(
    *, initial_capital: Decimal, closes: Sequence[Decimal]
) -> PaperComparisonRowResponse:
    """Build the passive SPY row from a complete persisted close path."""
    if len(closes) < 2 or initial_capital <= 0 or any(close <= 0 for close in closes):
        return PaperComparisonRowResponse(
            key="market_benchmark",
            data_state="UNAVAILABLE",
            nav=None,
            total_return_ratio=None,
            max_drawdown_ratio=None,
            turnover_ratio=None,
            total_cost=None,
            reason_code="SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE",
        )
    peak = closes[0]
    max_drawdown = Decimal(0)
    for close in closes:
        peak = max(peak, close)
        max_drawdown = max(max_drawdown, Decimal(1) - close / peak)
    total_return = closes[-1] / closes[0] - Decimal(1)
    return PaperComparisonRowResponse(
        key="market_benchmark",
        data_state="READY",
        nav=decimal_to_string(initial_capital * (Decimal(1) + total_return)),
        total_return_ratio=decimal_to_string(total_return),
        max_drawdown_ratio=decimal_to_string(max_drawdown),
        turnover_ratio=decimal_to_string(Decimal(0)),
        total_cost=decimal_to_string(Decimal(0)),
        reason_code="PERSISTED_MARKET_BENCHMARK",
    )


class PaperReadService:
    def __init__(
        self,
        session: AsyncSession,
        request_time: datetime,
        *,
        cursor_signing_secrets: Sequence[str] = (),
    ) -> None:
        self._session = session
        self._request_time = require_utc(request_time)
        self._cursor_secrets = tuple(cursor_signing_secrets)

    async def account(self) -> tuple[PaperAccountRecord, Portfolio]:
        rows = (
            await self._session.execute(
                select(PaperAccountRecord, Portfolio)
                .join(Portfolio, Portfolio.id == PaperAccountRecord.portfolio_id)
                .where(PaperAccountRecord.capability == "simulation_write_internal")
                .order_by(PaperAccountRecord.id)
                .limit(2)
            )
        ).all()
        if len(rows) != 1:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The internal paper account is unavailable.",
                status_code=409,
            )
        return rows[0][0], rows[0][1]

    async def latest_agent_cycle(self) -> AgentPaperCycleResponse | None:
        account, _ = await self.account()
        cycle = await self._session.scalar(
            select(AgentPaperCycleRecord)
            .where(
                AgentPaperCycleRecord.account_id == account.id,
                AgentPaperCycleRecord.decision_time <= self._request_time,
                AgentPaperCycleRecord.created_at <= self._request_time,
            )
            .order_by(
                AgentPaperCycleRecord.decision_slot_at.desc(),
                AgentPaperCycleRecord.id.desc(),
            )
            .limit(1)
        )
        if cycle is None:
            return None
        return await self._agent_cycle(cycle)

    async def agent_cycle_detail(
        self, cycle_id: UUID
    ) -> AgentPaperCycleDetailResponse | None:
        account, _ = await self.account()
        cycle = await self._session.scalar(
            select(AgentPaperCycleRecord).where(
                AgentPaperCycleRecord.id == cycle_id,
                AgentPaperCycleRecord.account_id == account.id,
                AgentPaperCycleRecord.decision_time <= self._request_time,
                AgentPaperCycleRecord.created_at <= self._request_time,
            )
        )
        if cycle is None:
            return None
        return AgentPaperCycleDetailResponse(
            cycle=await self._agent_cycle(cycle),
            orders=await self._orders_for_cycle(cycle_id),
        )

    async def agent_cycles(
        self,
        *,
        decision_session: date | None,
        limit: int,
    ) -> AgentPaperCyclePageResponse:
        if not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        account, _ = await self.account()
        resolved_session = decision_session
        if resolved_session is None:
            resolved_session = await self._current_xnys_session()
        if resolved_session is None:
            return AgentPaperCyclePageResponse(
                items=(),
                summary=await self._agent_session_summary(
                    account_id=account.id,
                    decision_session=None,
                    cycles=(),
                    historical=False,
                ),
                version_observation=await self._agent_version_observation(
                    account_id=account.id
                ),
            )
        cycles = tuple(
            await self._session.scalars(
                select(AgentPaperCycleRecord)
                .where(
                    AgentPaperCycleRecord.account_id == account.id,
                    AgentPaperCycleRecord.decision_session == resolved_session,
                    AgentPaperCycleRecord.decision_time <= self._request_time,
                    AgentPaperCycleRecord.created_at <= self._request_time,
                )
                .order_by(
                    AgentPaperCycleRecord.decision_slot_at.desc(),
                    AgentPaperCycleRecord.id.desc(),
                )
                .limit(limit)
            )
        )
        items = tuple([await self._agent_cycle(cycle) for cycle in cycles])
        cycle_count, completed_count = await self._agent_session_counts(
            account_id=account.id,
            decision_session=resolved_session,
        )
        return AgentPaperCyclePageResponse(
            items=items,
            summary=await self._agent_session_summary(
                account_id=account.id,
                decision_session=resolved_session,
                cycles=items,
                cycle_count=cycle_count,
                completed_count=completed_count,
                historical=decision_session is not None,
            ),
            version_observation=await self._agent_version_observation(
                account_id=account.id
            ),
        )

    async def _agent_version_observation(
        self, *, account_id: UUID
    ) -> AgentPaperVersionObservationResponse | None:
        row = (
            await self._session.execute(
                text(
                    """
                    WITH scoped_cycles AS (
                        SELECT id, decision_session, decision_time
                        FROM agent_paper_cycles
                        WHERE account_id = :account_id
                          AND decision_time <= :cutoff
                          AND created_at <= :cutoff
                    ),
                    research_runs AS (
                        SELECT DISTINCT
                            cycle.id AS cycle_id,
                            cycle.decision_session,
                            cycle.decision_time,
                            run_id.value::uuid AS run_id
                        FROM scoped_cycles AS cycle
                        JOIN agent_paper_cycle_events AS event
                          ON event.cycle_id = cycle.id
                        CROSS JOIN LATERAL jsonb_array_elements_text(
                            event.fact_ids -> 'research_run_ids'
                        ) AS run_id(value)
                        WHERE event.result_code = 'RESEARCH_JOBS_PREPARED'
                          AND event.occurred_at <= :cutoff
                          AND event.created_at <= :cutoff
                    ),
                    cycle_version_counts AS (
                        SELECT
                            research.cycle_id,
                            count(DISTINCT (run.prompt_hash, run.model_hash)) AS version_count
                        FROM research_runs AS research
                        JOIN agent_research_runs AS run ON run.id = research.run_id
                        WHERE run.decision_time <= :cutoff
                          AND run.created_at <= :cutoff
                        GROUP BY research.cycle_id
                    ),
                    latest_events AS (
                        SELECT DISTINCT ON (event.cycle_id)
                            event.cycle_id,
                            event.to_status
                        FROM agent_paper_cycle_events AS event
                        JOIN scoped_cycles AS cycle ON cycle.id = event.cycle_id
                        WHERE event.occurred_at <= :cutoff
                          AND event.created_at <= :cutoff
                        ORDER BY event.cycle_id, event.sequence DESC
                    ),
                    cycle_orders AS (
                        SELECT
                            decision.agent_paper_cycle_id AS cycle_id,
                            count(paper_order.id) AS order_count
                        FROM strategy_decisions AS decision
                        JOIN scoped_cycles AS cycle
                          ON cycle.id = decision.agent_paper_cycle_id
                        JOIN paper_orders AS paper_order
                          ON paper_order.strategy_decision_id = decision.id
                         AND paper_order.created_at <= :cutoff
                        WHERE decision.decision_origin = 'agent_research'
                          AND decision.created_at <= :cutoff
                        GROUP BY decision.agent_paper_cycle_id
                    ),
                    latest_order_events AS (
                        SELECT DISTINCT ON (order_event.order_id)
                            order_event.order_id,
                            order_event.to_status
                        FROM paper_order_events AS order_event
                        JOIN paper_orders AS paper_order ON paper_order.id = order_event.order_id
                        JOIN strategy_decisions AS decision
                          ON decision.id = paper_order.strategy_decision_id
                        JOIN scoped_cycles AS cycle
                          ON cycle.id = decision.agent_paper_cycle_id
                        WHERE order_event.occurred_at <= :cutoff
                          AND order_event.created_at <= :cutoff
                          AND paper_order.created_at <= :cutoff
                          AND decision.created_at <= :cutoff
                        ORDER BY order_event.order_id, order_event.event_seq DESC
                    ),
                    cycle_terminal_orders AS (
                        SELECT
                            decision.agent_paper_cycle_id AS cycle_id,
                            count(latest_order.order_id) FILTER (
                                WHERE latest_order.to_status IN (
                                    'filled', 'cancelled', 'rejected', 'risk_rejected'
                                )
                            ) AS terminal_order_count
                        FROM strategy_decisions AS decision
                        JOIN scoped_cycles AS cycle
                          ON cycle.id = decision.agent_paper_cycle_id
                        JOIN paper_orders AS paper_order
                          ON paper_order.strategy_decision_id = decision.id
                         AND paper_order.created_at <= :cutoff
                        LEFT JOIN latest_order_events AS latest_order
                          ON latest_order.order_id = paper_order.id
                        WHERE decision.decision_origin = 'agent_research'
                          AND decision.created_at <= :cutoff
                        GROUP BY decision.agent_paper_cycle_id
                    ),
                    version_rows AS (
                        SELECT
                            research.cycle_id,
                            research.decision_session,
                            research.decision_time,
                            prompt.version AS prompt_version,
                            model.model_id,
                            model.version AS model_version,
                            run.prompt_hash,
                            run.model_hash,
                            latest.to_status,
                            coalesce(cycle_orders.order_count, 0) AS cycle_order_count,
                            coalesce(cycle_terminal_orders.terminal_order_count, 0)
                                AS terminal_order_count,
                            decision.id AS decision_id,
                            paper_order.id AS order_id,
                            fill.id AS fill_id
                        FROM research_runs AS research
                        JOIN agent_research_runs AS run ON run.id = research.run_id
                        JOIN cycle_version_counts AS cycle_version
                          ON cycle_version.cycle_id = research.cycle_id
                         AND cycle_version.version_count = 1
                        JOIN agent_prompt_versions AS prompt ON prompt.id = run.prompt_version_id
                        JOIN agent_model_configs AS model ON model.id = run.model_config_id
                        LEFT JOIN latest_events AS latest ON latest.cycle_id = research.cycle_id
                        LEFT JOIN cycle_orders ON cycle_orders.cycle_id = research.cycle_id
                        LEFT JOIN cycle_terminal_orders
                          ON cycle_terminal_orders.cycle_id = research.cycle_id
                        LEFT JOIN strategy_decisions AS decision
                          ON decision.agent_paper_cycle_id = research.cycle_id
                         AND decision.created_at <= :cutoff
                        LEFT JOIN paper_orders AS paper_order
                          ON paper_order.strategy_decision_id = decision.id
                         AND paper_order.created_at <= :cutoff
                        LEFT JOIN paper_fills AS fill
                          ON fill.order_id = paper_order.id
                         AND fill.created_at <= :cutoff
                         AND fill.occurred_at <= :cutoff
                         AND fill.evaluated_at <= :cutoff
                        WHERE run.decision_time <= :cutoff
                          AND run.created_at <= :cutoff
                          AND prompt.created_at <= :cutoff
                          AND model.created_at <= :cutoff
                    ),
                    version_stats AS (
                        SELECT
                            prompt_version,
                            model_id,
                            model_version,
                            prompt_hash,
                            model_hash,
                            min(decision_time) AS first_decision_at,
                            max(decision_time) AS last_decision_at,
                            count(DISTINCT decision_session) AS observed_sessions,
                            count(DISTINCT cycle_id) AS cycle_count,
                            count(DISTINCT cycle_id) FILTER (
                                WHERE to_status = 'completed'
                                   OR (
                                       to_status = 'awaiting_market'
                                       AND cycle_order_count > 0
                                       AND cycle_order_count = terminal_order_count
                                   )
                            )
                                AS completed_count,
                            count(DISTINCT cycle_id) FILTER (WHERE to_status = 'unavailable')
                                AS unavailable_count,
                            count(DISTINCT order_id) AS order_count,
                            count(DISTINCT fill_id) AS fill_count
                        FROM version_rows
                        GROUP BY
                            prompt_version,
                            model_id,
                            model_version,
                            prompt_hash,
                            model_hash
                    )
                    SELECT
                        version_stats.*,
                        (
                            SELECT count(*)
                            FROM cycle_version_counts
                            WHERE version_count > 1
                        ) AS mixed_cycle_count
                    FROM version_stats
                    ORDER BY last_decision_at DESC, prompt_version, model_id, model_version
                    LIMIT 1
                    """
                ),
                {"account_id": account_id, "cutoff": self._request_time},
            )
        ).mappings().one_or_none()
        if row is None:
            return None
        if int(row["mixed_cycle_count"]) > 0:
            return None
        reason_codes = ["VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE"]
        if int(row["order_count"]) == 0 or int(row["fill_count"]) == 0:
            reason_codes.append("VERSION_ORDER_PATH_UNOBSERVED")
        reason_codes.append("VERSION_CHALLENGER_UNAVAILABLE")
        return AgentPaperVersionObservationResponse(
            prompt_version=cast(str, row["prompt_version"]),
            prompt_hash=cast(str, row["prompt_hash"]),
            model_id=cast(str, row["model_id"]),
            model_version=cast(str, row["model_version"]),
            model_hash=cast(str, row["model_hash"]),
            first_decision_at=cast(datetime, row["first_decision_at"]),
            last_decision_at=cast(datetime, row["last_decision_at"]),
            observed_sessions=int(row["observed_sessions"]),
            cycle_count=int(row["cycle_count"]),
            completed_count=int(row["completed_count"]),
            unavailable_count=int(row["unavailable_count"]),
            order_count=int(row["order_count"]),
            fill_count=int(row["fill_count"]),
            challenger_available=False,
            verdict="continue_observing",
            reason_codes=cast(Any, tuple(reason_codes)),
        )

    async def _agent_session_counts(
        self,
        *,
        account_id: UUID,
        decision_session: date,
    ) -> tuple[int, int]:
        session_cycle_ids = select(AgentPaperCycleRecord.id).where(
            AgentPaperCycleRecord.account_id == account_id,
            AgentPaperCycleRecord.decision_session == decision_session,
            AgentPaperCycleRecord.decision_time <= self._request_time,
            AgentPaperCycleRecord.created_at <= self._request_time,
        )
        latest_cycle_events = (
            select(
                AgentPaperCycleEventRecord.cycle_id,
                func.max(AgentPaperCycleEventRecord.sequence).label("sequence"),
            )
            .where(
                AgentPaperCycleEventRecord.cycle_id.in_(session_cycle_ids),
                AgentPaperCycleEventRecord.occurred_at <= self._request_time,
                AgentPaperCycleEventRecord.created_at <= self._request_time,
            )
            .group_by(AgentPaperCycleEventRecord.cycle_id)
            .subquery()
        )
        session_order_ids = (
            select(PaperOrderRecord.id)
            .join(
                StrategyDecisionRecord,
                StrategyDecisionRecord.id == PaperOrderRecord.strategy_decision_id,
            )
            .where(
                StrategyDecisionRecord.agent_paper_cycle_id.in_(session_cycle_ids),
                StrategyDecisionRecord.decision_origin == "agent_research",
                StrategyDecisionRecord.created_at <= self._request_time,
                PaperOrderRecord.created_at <= self._request_time,
            )
        )
        latest_order_events = (
            select(
                PaperOrderEventRecord.order_id,
                func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
            )
            .where(
                PaperOrderEventRecord.order_id.in_(session_order_ids),
                PaperOrderEventRecord.occurred_at <= self._request_time,
                PaperOrderEventRecord.created_at <= self._request_time,
            )
            .group_by(PaperOrderEventRecord.order_id)
            .subquery()
        )
        terminal_order_statuses = {"filled", "cancelled", "rejected", "risk_rejected"}
        rows = (
            await self._session.execute(
                select(
                    AgentPaperCycleEventRecord.to_status,
                    func.count(PaperOrderRecord.id).label("order_count"),
                    func.count(PaperOrderEventRecord.order_id).label("terminal_order_count"),
                )
                .select_from(AgentPaperCycleRecord)
                .outerjoin(
                    latest_cycle_events,
                    latest_cycle_events.c.cycle_id == AgentPaperCycleRecord.id,
                )
                .outerjoin(
                    AgentPaperCycleEventRecord,
                    and_(
                        AgentPaperCycleEventRecord.cycle_id == AgentPaperCycleRecord.id,
                        AgentPaperCycleEventRecord.sequence
                        == latest_cycle_events.c.sequence,
                    ),
                )
                .outerjoin(
                    StrategyDecisionRecord,
                    and_(
                        StrategyDecisionRecord.agent_paper_cycle_id
                        == AgentPaperCycleRecord.id,
                        StrategyDecisionRecord.decision_origin == "agent_research",
                        StrategyDecisionRecord.created_at <= self._request_time,
                    ),
                )
                .outerjoin(
                    PaperOrderRecord,
                    and_(
                        PaperOrderRecord.strategy_decision_id == StrategyDecisionRecord.id,
                        PaperOrderRecord.created_at <= self._request_time,
                    ),
                )
                .outerjoin(
                    latest_order_events,
                    latest_order_events.c.order_id == PaperOrderRecord.id,
                )
                .outerjoin(
                    PaperOrderEventRecord,
                    and_(
                        PaperOrderEventRecord.order_id == PaperOrderRecord.id,
                        PaperOrderEventRecord.event_seq == latest_order_events.c.event_seq,
                        PaperOrderEventRecord.to_status.in_(terminal_order_statuses),
                        PaperOrderEventRecord.created_at <= self._request_time,
                    ),
                )
                .where(
                    AgentPaperCycleRecord.account_id == account_id,
                    AgentPaperCycleRecord.decision_session == decision_session,
                    AgentPaperCycleRecord.decision_time <= self._request_time,
                    AgentPaperCycleRecord.created_at <= self._request_time,
                )
                .group_by(
                    AgentPaperCycleRecord.id,
                    AgentPaperCycleEventRecord.to_status,
                )
                .order_by(AgentPaperCycleRecord.id)
                .limit(_MAX_AGENT_CYCLES_PER_SESSION + 1)
            )
        ).all()
        if len(rows) > _MAX_AGENT_CYCLES_PER_SESSION or any(
            order_count > _MAX_AGENT_ORDERS_PER_CYCLE
            for _, order_count, _ in rows
        ):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper session is unbounded.",
                status_code=409,
            )
        if any(status is None for status, _, _ in rows):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper session projection is incomplete.",
                status_code=409,
            )
        completed_count = sum(
            status == "completed"
            or (status == "awaiting_market" and order_count > 0 and order_count == terminal_count)
            for status, order_count, terminal_count in rows
        )
        return len(rows), completed_count

    async def _agent_session_summary(
        self,
        *,
        account_id: UUID,
        decision_session: date | None,
        cycles: tuple[AgentPaperCycleResponse, ...],
        cycle_count: int = 0,
        completed_count: int = 0,
        historical: bool,
    ) -> AgentPaperSessionSummaryResponse:
        last_completed = await self._session.scalar(
            select(AgentPaperCycleRecord.decision_time)
            .join(
                AgentPaperCycleEventRecord,
                AgentPaperCycleEventRecord.cycle_id == AgentPaperCycleRecord.id,
            )
            .where(
                AgentPaperCycleEventRecord.to_status == "completed",
                AgentPaperCycleEventRecord.occurred_at <= self._request_time,
                AgentPaperCycleEventRecord.created_at <= self._request_time,
                AgentPaperCycleRecord.account_id == account_id,
                AgentPaperCycleRecord.decision_time <= self._request_time,
                AgentPaperCycleRecord.created_at <= self._request_time,
            )
            .order_by(
                AgentPaperCycleEventRecord.occurred_at.desc(),
                AgentPaperCycleRecord.id.desc(),
            )
            .limit(1)
        )
        latest = cycles[0] if cycles else None
        if historical:
            return AgentPaperSessionSummaryResponse(
                decision_session=decision_session,
                status="historical",
                cycle_count=cycle_count,
                completed_count=completed_count,
                latest_result_code=None if latest is None else latest.result_code,
                last_completed_decision_at=last_completed,
                next_eligible_at=None,
                next_eligible_reason=None,
                execution_locked=True,
            )

        local_now = self._request_time.astimezone(ZoneInfo("America/New_York"))
        next_at: datetime | None = None
        next_reason: Literal[
            "CURRENT_SLOT",
            "NEXT_SLOT",
            "SESSION_OPEN",
            "NEXT_SESSION",
            "CURRENT_CYCLE_IN_PROGRESS",
            "CALENDAR_UNAVAILABLE",
        ] | None = None
        status: Literal[
            "calendar_unavailable",
            "not_started",
            "ready",
            "in_progress",
            "completed",
            "stopped",
            "closed",
            "historical",
        ] = "calendar_unavailable"
        if decision_session is None:
            next_session = await self._next_xnys_session(local_now.date())
            if next_session is not None:
                status = "closed"
                next_at = self._paper_window_open(next_session)
                next_reason = "NEXT_SESSION"
        elif latest is not None and latest.status not in {
            "completed",
            "unavailable",
            "failed",
        }:
            status = "in_progress"
            next_reason = "CURRENT_CYCLE_IN_PROGRESS"
        elif local_now.time().replace(tzinfo=None) < time(7):
            status = "not_started" if latest is None else "completed"
            next_at = self._paper_window_open(decision_session)
            next_reason = "SESSION_OPEN"
        elif local_now.time().replace(tzinfo=None) >= time(16):
            status = "closed"
            next_session = await self._next_xnys_session(decision_session)
            if next_session is None:
                next_reason = "CALENDAR_UNAVAILABLE"
            else:
                next_at = self._paper_window_open(next_session)
                next_reason = "NEXT_SESSION"
        else:
            current_slot = floor_decision_slot(self._request_time)
            failed_current_slot = (
                latest is not None
                and latest.status in {"unavailable", "failed"}
                and latest.decision_slot_at == current_slot
            )
            if failed_current_slot:
                status = "stopped"
            elif latest is None or latest.status in {"unavailable", "failed"}:
                status = "ready"
            else:
                status = "completed"
            if latest is not None and latest.decision_slot_at == current_slot:
                next_at = current_slot + timedelta(minutes=5)
                next_reason = "NEXT_SLOT"
            else:
                next_at = current_slot
                next_reason = "CURRENT_SLOT"

        if next_at is None and next_reason is None:
            next_reason = "CALENDAR_UNAVAILABLE"
        return AgentPaperSessionSummaryResponse(
            decision_session=decision_session,
            status=status,
            cycle_count=cycle_count,
            completed_count=completed_count,
            latest_result_code=None if latest is None else latest.result_code,
            last_completed_decision_at=last_completed,
            next_eligible_at=next_at,
            next_eligible_reason=next_reason,
            execution_locked=True,
        )

    async def _next_xnys_session(self, after: date) -> date | None:
        value = await self._session.scalar(
            select(ExchangeCalendarSessionRecord.session)
            .join(
                ExchangeCalendarAuthorityRecord,
                ExchangeCalendarAuthorityRecord.id
                == ExchangeCalendarSessionRecord.calendar_authority_id,
            )
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.published_at <= self._request_time,
                ExchangeCalendarAuthorityRecord.created_at <= self._request_time,
                ExchangeCalendarSessionRecord.created_at <= self._request_time,
                ExchangeCalendarSessionRecord.session > after,
            )
            .order_by(
                ExchangeCalendarSessionRecord.session.asc(),
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
            .limit(1)
        )
        return cast(date | None, value)

    @staticmethod
    def _paper_window_open(session_date: date) -> datetime:
        return datetime.combine(
            session_date,
            time(7),
            tzinfo=ZoneInfo("America/New_York"),
        ).astimezone(UTC)

    async def _current_xnys_session(self) -> date | None:
        local_date = self._request_time.astimezone(ZoneInfo("America/New_York")).date()
        session_date = await self._session.scalar(
            select(ExchangeCalendarSessionRecord.session)
            .join(
                ExchangeCalendarAuthorityRecord,
                ExchangeCalendarAuthorityRecord.id
                == ExchangeCalendarSessionRecord.calendar_authority_id,
            )
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.published_at <= self._request_time,
                ExchangeCalendarAuthorityRecord.created_at <= self._request_time,
                ExchangeCalendarSessionRecord.created_at <= self._request_time,
                ExchangeCalendarSessionRecord.session == local_date,
            )
            .order_by(
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.version.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
            .limit(1)
        )
        return cast(date | None, session_date)

    async def _agent_cycle(
        self,
        cycle: AgentPaperCycleRecord,
    ) -> AgentPaperCycleResponse:
        event = await self._session.scalar(
            select(AgentPaperCycleEventRecord)
            .where(
                AgentPaperCycleEventRecord.cycle_id == cycle.id,
                AgentPaperCycleEventRecord.occurred_at <= self._request_time,
                AgentPaperCycleEventRecord.created_at <= self._request_time,
            )
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
            .limit(1)
        )
        if event is None:
            raise NexusError(
                ErrorCode.DATA_PARTIAL, "The Agent paper cycle is incomplete.", status_code=409
            )
        try:
            manifest = parse_candidate_manifest(
                cycle.candidate_manifest,
                allow_legacy=cycle.decision_price_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
            )
        except (TypeError, ValueError):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper cycle manifest is incomplete.",
                status_code=409,
            ) from None
        decision = await self._session.scalar(
            select(StrategyDecisionRecord).where(
                StrategyDecisionRecord.agent_paper_cycle_id == cycle.id,
                StrategyDecisionRecord.decision_origin == "agent_research",
                StrategyDecisionRecord.created_at <= self._request_time,
            )
        )
        outputs: dict[
            UUID, tuple[AgentResearchOutputRecord, StrategyDecisionAgentOutputRecord]
        ] = {}
        if decision is not None:
            rows = (
                await self._session.execute(
                    select(
                        AgentResearchRunRecord.instrument_id,
                        AgentResearchOutputRecord,
                        StrategyDecisionAgentOutputRecord,
                    )
                    .join(
                        AgentResearchOutputRecord,
                        AgentResearchOutputRecord.run_id == AgentResearchRunRecord.id,
                    )
                    .join(
                        StrategyDecisionAgentOutputRecord,
                        StrategyDecisionAgentOutputRecord.agent_research_output_id
                        == AgentResearchOutputRecord.id,
                    )
                    .where(
                        StrategyDecisionAgentOutputRecord.strategy_decision_id == decision.id,
                        StrategyDecisionAgentOutputRecord.created_at <= self._request_time,
                        AgentResearchOutputRecord.created_at <= self._request_time,
                        AgentResearchRunRecord.created_at <= self._request_time,
                    )
                    .limit(6)
                )
            ).all()
            if len(rows) > 5:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL, "The Agent paper cycle is unbounded.", status_code=409
                )
            outputs = {instrument_id: (output, binding) for instrument_id, output, binding in rows}
        candidates = []
        for candidate in manifest:
            instrument_id = candidate.instrument_id
            bound = outputs.get(instrument_id)
            output, binding = bound if bound is not None else (None, None)
            intent = output.intent if output is not None and isinstance(output.intent, dict) else {}
            stance = intent.get("stance")
            candidates.append(
                AgentPaperCandidateResponse(
                    instrument_id=instrument_id,
                    symbol=candidate.symbol,
                    selection_reasons=(candidate.inclusion_reason,),
                    research_status="completed" if bound is not None else "pending",
                    stance=cast(
                        Literal["buy_candidate", "reduce_candidate", "hold", "watch"] | None,
                        stance
                        if stance in {"buy_candidate", "reduce_candidate", "hold", "watch"}
                        else None,
                    ),
                    sizing_action=cast(
                        Literal["increase", "reduce", "maintain", "none"] | None,
                        None if binding is None else binding.sizing_action,
                    ),
                    intent_hash=None if binding is None else binding.intent_hash,
                    output_id=None if output is None else output.id,
                    rejection_code=None,
                )
            )
        order_ids: tuple[UUID, ...] = ()
        risk_outcomes: tuple[str, ...] = ()
        decision_response = None
        if decision is not None:
            order_ids = tuple(
                await self._session.scalars(
                    select(PaperOrderRecord.id)
                    .where(
                        PaperOrderRecord.strategy_decision_id == decision.id,
                        PaperOrderRecord.created_at <= self._request_time,
                    )
                    .order_by(PaperOrderRecord.id)
                    .limit(51)
                )
            )
            if len(order_ids) > 50:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL, "The Agent paper cycle is unbounded.", status_code=409
                )
            risk_outcomes = tuple(
                await self._session.scalars(
                    select(RiskEvaluationRecord.outcome)
                    .join(PaperOrderRecord, PaperOrderRecord.id == RiskEvaluationRecord.order_id)
                    .where(PaperOrderRecord.strategy_decision_id == decision.id)
                    .where(
                        RiskEvaluationRecord.created_at <= self._request_time,
                        PaperOrderRecord.created_at <= self._request_time,
                    )
                    .order_by(PaperOrderRecord.id)
                    .limit(51)
                )
            )
            remaining_risk_outcomes = 50 - len(risk_outcomes)
            no_change_ids = tuple(
                await self._session.scalars(
                    select(RiskNoChangeEvaluationRecord.id)
                    .join(
                        PaperOrderBatchRecord,
                        PaperOrderBatchRecord.id == RiskNoChangeEvaluationRecord.batch_id,
                    )
                    .where(
                        PaperOrderBatchRecord.account_id == decision.account_id,
                        PaperOrderBatchRecord.strategy_decision_id == decision.id,
                        RiskNoChangeEvaluationRecord.created_at <= self._request_time,
                    )
                    .order_by(RiskNoChangeEvaluationRecord.instrument_id)
                    .limit(remaining_risk_outcomes + 1)
                )
            )
            if len(no_change_ids) > remaining_risk_outcomes:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The Agent paper risk projection is unbounded.",
                    status_code=409,
                )
            risk_outcomes += ("no_change",) * len(no_change_ids)
            decision_response = AgentPaperDecisionResponse(
                decision_id=decision.id,
                sizing_policy_version="agent_paper_sizing_v1",
                target_cash_weight=decimal_to_string(decision.cash_weight),
                risk_outcomes=cast(
                    tuple[Literal["approved", "reduced", "rejected", "no_change"], ...],
                    risk_outcomes,
                ),
            )
        projected_status = event.to_status
        projected_result_code = event.result_code
        if event.to_status == "awaiting_market" and order_ids:
            latest_order_events = (
                select(
                    PaperOrderEventRecord.order_id,
                    func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
                )
                .where(
                    PaperOrderEventRecord.order_id.in_(order_ids),
                    PaperOrderEventRecord.occurred_at <= self._request_time,
                    PaperOrderEventRecord.created_at <= self._request_time,
                )
                .group_by(PaperOrderEventRecord.order_id)
                .subquery()
            )
            order_statuses = tuple(
                await self._session.scalars(
                    select(PaperOrderEventRecord.to_status)
                    .join(
                        latest_order_events,
                        and_(
                            latest_order_events.c.order_id == PaperOrderEventRecord.order_id,
                            latest_order_events.c.event_seq == PaperOrderEventRecord.event_seq,
                        ),
                    )
                    .order_by(PaperOrderEventRecord.order_id)
                )
            )
            if len(order_statuses) != len(order_ids):
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The Agent paper order projection is incomplete.",
                    status_code=409,
                )
            if "partially_filled" in order_statuses:
                projected_status = "partially_filled"
                projected_result_code = "PAPER_ORDERS_PARTIALLY_FILLED"
            elif all(
                status in {"filled", "cancelled", "rejected", "risk_rejected"}
                for status in order_statuses
            ):
                projected_status = "completed"
                projected_result_code = "PAPER_CYCLE_COMPLETED"
        windows = tuple(
            await self._session.scalars(
                select(AgentPaperMarketWindowRecord)
                .where(
                    AgentPaperMarketWindowRecord.cycle_id == cycle.id,
                    AgentPaperMarketWindowRecord.evaluated_at <= self._request_time,
                    AgentPaperMarketWindowRecord.created_at <= self._request_time,
                )
                .order_by(AgentPaperMarketWindowRecord.created_at.desc())
                .limit(51)
            )
        )
        if len(windows) > 50:
            raise NexusError(
                ErrorCode.DATA_PARTIAL, "The Agent paper cycle is unbounded.", status_code=409
            )
        market_status: Literal["pending", "qualified", "probe", "unavailable"] = "pending"
        if any(row.purpose == "paper_fill" for row in windows):
            market_status = "qualified"
        elif windows:
            market_status = "probe"
        elif projected_result_code == "PAPER_MARKET_WINDOW_UNAVAILABLE":
            market_status = "unavailable"
        schwab_execution = (
            cycle.execution_price_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
        )
        expected_execution = (
            ("schwab", "price_history", "unknown")
            if schwab_execution
            else ("alpaca", "iex", "real_time")
        )
        if any((row.source, row.feed, row.delivery_mode) != expected_execution for row in windows):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper market binding is incomplete.",
                status_code=409,
            )
        decision_market = None
        if cycle.decision_price_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID:
            terminal_without_candidate = (
                not manifest
                and projected_status == "unavailable"
                and projected_result_code in {"RISK_INPUT_REQUIRED", "OPEN_ORDER_EXISTS"}
            )
            if len(manifest) != 1 and not terminal_without_candidate:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The Agent paper decision market is incomplete.",
                    status_code=409,
                )
            if manifest:
                candidate = cast(CandidateManifestEntry, manifest[0])
                quote = await self._session.get(LatestQuote, candidate.decision_quote_id)
                artifact = await self._session.get(DataArtifact, candidate.decision_artifact_id)
                if (
                    quote is None
                    or artifact is None
                    or quote.instrument_id != candidate.instrument_id
                    or quote.data_artifact_id != artifact.id
                    or quote.source != "schwab"
                    or quote.feed != "l1"
                    or quote.delivery_mode != "real_time"
                    or quote.event_kind != "quote"
                    or quote.evidence_status != "verified"
                    or quote.bid is None
                    or quote.ask is None
                    or quote.bid <= 0
                    or quote.ask <= 0
                    or quote.bid > quote.ask
                    or quote.available_at > cycle.decision_time
                    or quote.received_at > cycle.decision_time
                    or artifact.source != "schwab"
                    or artifact.schema_version != "schwab_level_one_stream_v1"
                    or artifact.sha256 != candidate.decision_evidence_hash
                ):
                    raise NexusError(
                        ErrorCode.DATA_PARTIAL,
                        "The Agent paper decision market is incomplete.",
                        status_code=409,
                    )
                decision_market = AgentPaperDecisionMarketResponse(
                    source="schwab",
                    feed="l1",
                    delivery_mode="real_time",
                    delayed=False,
                    quote_at=quote.observed_at,
                )
        return AgentPaperCycleResponse(
            cycle_id=cycle.id,
            status=cast(Any, projected_status),
            result_code=projected_result_code,
            decision_session=cycle.decision_session,
            decision_time=cycle.decision_time,
            decision_slot_at=cycle.decision_slot_at,
            execution_session=cycle.execution_session,
            execution_locked=True,
            candidate_count=len(candidates),
            candidates=tuple(candidates),
            decision=decision_response,
            decision_market=decision_market,
            execution_market=(
                AgentPaperSchwabExecutionMarketResponse(
                    source="schwab",
                    feed="price_history",
                    delivery_mode="unknown",
                    window_minutes=5,
                    status=market_status,
                )
                if schwab_execution
                else AgentPaperAlpacaExecutionMarketResponse(
                    source="alpaca",
                    feed="iex",
                    delivery_mode="real_time",
                    window_minutes=5,
                    status=market_status,
                )
            ),
            order_ids=order_ids,
        )

    async def portfolio(self) -> PaperPortfolioResponse:
        account, portfolio = await self.account()
        snapshot = await self._session.scalar(
            select(PaperPerformanceSnapshotRecord)
            .where(
                PaperPerformanceSnapshotRecord.account_id == account.id,
                PaperPerformanceSnapshotRecord.as_of <= self._request_time,
            )
            .order_by(PaperPerformanceSnapshotRecord.as_of.desc())
            .limit(1)
        )
        if snapshot is None:
            return PaperPortfolioResponse(
                portfolio_id=portfolio.id,
                name=portfolio.name,
                mode="internal_paper",
                currency="USD",
                initial_capital=decimal_to_string(account.initial_capital),
                data_state="EMPTY",
                performance=None,
                comparison=None,
                positions=(),
                attributions=(),
            )

        projection = await load_paper_projection(
            self._session,
            account_id=account.id,
            as_of=snapshot.as_of,
        )
        if projection.portfolio_id != portfolio.id:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The internal paper projection is unavailable.",
                status_code=409,
            )

        position_rows = (
            await self._session.execute(
                select(
                    PaperPositionSnapshotRecord,
                    PriceBarSetAuthority,
                    PriceSeriesManifest,
                )
                .join(
                    PriceBarSetAuthority,
                    PriceBarSetAuthority.id == PaperPositionSnapshotRecord.price_authority_id,
                )
                .join(
                    PriceSeriesManifest,
                    PriceSeriesManifest.id
                    == PriceBarSetAuthority.price_series_manifest_id,
                )
                .where(
                    PaperPositionSnapshotRecord.account_id == account.id,
                    PaperPositionSnapshotRecord.as_of == snapshot.as_of,
                )
                .order_by(PaperPositionSnapshotRecord.instrument_id)
                .limit(_MAX_POSITIONS + 1)
            )
        ).all()
        _require_bounded("positions", position_rows, _MAX_POSITIONS)
        attribution_rows = tuple(
            (
                await self._session.scalars(
                    select(PaperPerformanceAttributionRecord)
                    .where(PaperPerformanceAttributionRecord.performance_snapshot_id == snapshot.id)
                    .order_by(
                        PaperPerformanceAttributionRecord.dimension,
                        PaperPerformanceAttributionRecord.dimension_key,
                    )
                    .limit(_MAX_ATTRIBUTIONS + 1)
                )
            ).all()
        )
        _require_bounded("attributions", attribution_rows, _MAX_ATTRIBUTIONS)
        symbols = await self._symbols(
            tuple(row.instrument_id for row, _, _ in position_rows),
            snapshot.as_of,
        )
        positions = tuple(
            PaperPositionResponse(
                instrument_id=row.instrument_id,
                symbol=symbols.get(row.instrument_id),
                quantity=decimal_to_string(row.quantity),
                reserved_quantity=decimal_to_string(row.reserved_quantity),
                available_quantity=decimal_to_string(row.quantity - row.reserved_quantity),
                cost_basis=decimal_to_string(row.cost_basis),
                average_cost=decimal_to_string(
                    Decimal(0) if row.quantity == 0 else row.cost_basis / row.quantity
                ),
                reference_close=decimal_to_string(row.reference_close),
                market_value=decimal_to_string(row.quantity * row.reference_close),
                unrealized_pnl=decimal_to_string(
                    row.quantity * row.reference_close - row.cost_basis
                ),
                price_source=manifest.source,
                price_feed=manifest.feed,
                delivery_mode=cast(
                    Literal["real_time", "delayed", "fixture", "unknown"],
                    manifest.delivery_mode,
                ),
                price_as_of=snapshot.as_of,
            )
            for row, _, manifest in position_rows
        )
        attributions = tuple(
            PaperAttributionResponse(
                dimension=cast(Literal["cash", "instrument", "sector"], row.dimension),
                dimension_key=row.dimension_key,
                market_value=decimal_to_string(row.market_value),
                realized_pnl=decimal_to_string(row.realized_pnl),
                unrealized_pnl=decimal_to_string(row.unrealized_pnl),
                fees=decimal_to_string(row.fees),
                slippage=decimal_to_string(row.slippage),
                completeness=cast(Literal["FULL", "PARTIAL"], row.completeness),
            )
            for row in attribution_rows
        )
        market_value = sum(
            (row.quantity * row.reference_close for row, _, _ in position_rows),
            start=Decimal(0),
        )
        state: Literal["READY", "PARTIAL", "STALE"]
        if self._request_time - snapshot.as_of > _STALE_AFTER:
            state = "STALE"
        elif any(row.completeness == "PARTIAL" for row in attribution_rows):
            state = "PARTIAL"
        else:
            state = "READY"
        return PaperPortfolioResponse(
            portfolio_id=portfolio.id,
            name=portfolio.name,
            mode="internal_paper",
            currency="USD",
            initial_capital=decimal_to_string(account.initial_capital),
            data_state=state,
            performance=PaperPerformanceResponse(
                as_of=snapshot.as_of,
                cash=decimal_to_string(snapshot.cash),
                available_cash=decimal_to_string(projection.available_cash),
                reserved_cash=decimal_to_string(projection.reserved_cash),
                market_value=decimal_to_string(market_value),
                nav=decimal_to_string(snapshot.nav),
                peak_nav=decimal_to_string(snapshot.peak_nav),
                realized_pnl=decimal_to_string(snapshot.realized_pnl),
                unrealized_pnl=decimal_to_string(snapshot.unrealized_pnl),
                fees=decimal_to_string(snapshot.fees),
                drawdown_ratio=decimal_to_string(snapshot.drawdown_ratio),
            ),
            comparison=await self._paper_comparison(
                account=account,
                snapshot=snapshot,
                data_state=state,
            ),
            positions=positions,
            attributions=attributions,
        )

    async def _paper_comparison(
        self,
        *,
        account: PaperAccountRecord,
        snapshot: PaperPerformanceSnapshotRecord,
        data_state: Literal["READY", "PARTIAL", "STALE"],
    ) -> PaperComparisonResponse:
        max_drawdown = await self._session.scalar(
            select(func.max(PaperPerformanceSnapshotRecord.drawdown_ratio)).where(
                PaperPerformanceSnapshotRecord.account_id == account.id,
                PaperPerformanceSnapshotRecord.as_of <= snapshot.as_of,
            )
        )
        fill_rows = (
            await self._session.execute(
                select(
                    StrategyDecisionRecord.decision_origin,
                    func.sum(PaperFillRecord.notional),
                    func.sum(PaperFillRecord.slippage_amount),
                )
                .select_from(PaperFillRecord)
                .join(PaperOrderRecord, PaperOrderRecord.id == PaperFillRecord.order_id)
                .join(
                    StrategyDecisionRecord,
                    StrategyDecisionRecord.id == PaperOrderRecord.strategy_decision_id,
                )
                .where(
                    PaperFillRecord.account_id == account.id,
                    PaperFillRecord.occurred_at <= snapshot.as_of,
                )
                .group_by(StrategyDecisionRecord.decision_origin)
            )
        ).all()
        mixed_strategy_facts = any(origin != "agent_research" for origin, _, _ in fill_rows)
        agent_turnover = sum(
            (notional for origin, notional, _ in fill_rows if origin == "agent_research"),
            start=Decimal(0),
        )
        agent_slippage = sum(
            (slippage for origin, _, slippage in fill_rows if origin == "agent_research"),
            start=Decimal(0),
        )
        agent_row = (
            PaperComparisonRowResponse(
                key="agent",
                data_state="UNAVAILABLE",
                nav=None,
                total_return_ratio=None,
                max_drawdown_ratio=None,
                turnover_ratio=None,
                total_cost=None,
                reason_code="MIXED_STRATEGY_FACTS_UNAVAILABLE",
            )
            if mixed_strategy_facts
            else PaperComparisonRowResponse(
                key="agent",
                data_state=data_state,
                nav=decimal_to_string(snapshot.nav),
                total_return_ratio=decimal_to_string(
                    snapshot.nav / account.initial_capital - Decimal(1)
                ),
                max_drawdown_ratio=decimal_to_string(
                    snapshot.drawdown_ratio if max_drawdown is None else max_drawdown
                ),
                turnover_ratio=decimal_to_string(agent_turnover / account.initial_capital),
                total_cost=decimal_to_string(snapshot.fees + agent_slippage),
                reason_code="PERSISTED_AGENT_PERFORMANCE",
            )
        )
        benchmark_row = await self._market_benchmark_row(
            account=account,
            snapshot=snapshot,
        )
        return PaperComparisonResponse(
            window_start=account.created_at,
            as_of=snapshot.as_of,
            rows=(
                agent_row,
                PaperComparisonRowResponse(
                    key="hold_current",
                    data_state="READY",
                    nav=decimal_to_string(account.initial_capital),
                    total_return_ratio=decimal_to_string(Decimal(0)),
                    max_drawdown_ratio=decimal_to_string(Decimal(0)),
                    turnover_ratio=decimal_to_string(Decimal(0)),
                    total_cost=decimal_to_string(Decimal(0)),
                    reason_code="CASH_HOLD_CURRENT",
                ),
                PaperComparisonRowResponse(
                    key="baseline_v1",
                    data_state="UNAVAILABLE",
                    nav=None,
                    total_return_ratio=None,
                    max_drawdown_ratio=None,
                    turnover_ratio=None,
                    total_cost=None,
                    reason_code="SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE",
                ),
                benchmark_row,
            ),
        )

    async def _market_benchmark_row(
        self,
        *,
        account: PaperAccountRecord,
        snapshot: PaperPerformanceSnapshotRecord,
    ) -> PaperComparisonRowResponse:
        unavailable = market_benchmark_row_from_closes(
            initial_capital=account.initial_capital,
            closes=(),
        )
        start_session = account.created_at.astimezone(ZoneInfo("America/New_York")).date()
        end_session = snapshot.as_of.astimezone(ZoneInfo("America/New_York")).date()
        expected_sessions = tuple(
            await self._session.scalars(
                select(ExchangeCalendarSessionRecord.session)
                .join(
                    ExchangeCalendarAuthorityRecord,
                    ExchangeCalendarAuthorityRecord.id
                    == ExchangeCalendarSessionRecord.calendar_authority_id,
                )
                .distinct()
                .where(
                    ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                    ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                    ExchangeCalendarAuthorityRecord.published_at <= self._request_time,
                    ExchangeCalendarAuthorityRecord.created_at <= self._request_time,
                    ExchangeCalendarSessionRecord.session >= start_session,
                    ExchangeCalendarSessionRecord.session <= end_session,
                )
                .order_by(ExchangeCalendarSessionRecord.session)
            )
        )
        if len(expected_sessions) < 2:
            return unavailable
        candidate = (
            await self._session.execute(
                select(PriceBarSetAuthority, PriceSeriesManifest)
                .join(
                    PriceSeriesManifest,
                    PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
                )
                .where(
                    PriceBarSetAuthority.source_policy_id
                    == PAPER_DECISION_SOURCE_POLICY_ID,
                    PriceBarSetAuthority.first_session <= expected_sessions[0],
                    PriceBarSetAuthority.last_session >= expected_sessions[-1],
                    PriceBarSetAuthority.available_at <= self._request_time,
                    PriceBarSetAuthority.created_at <= self._request_time,
                    PriceSeriesManifest.instrument == str(stable_instrument_id("SPY")),
                    PriceSeriesManifest.source == "alpaca",
                    PriceSeriesManifest.feed == "sip",
                    PriceSeriesManifest.delivery_mode == "delayed",
                    PriceSeriesManifest.adjustment == "all",
                    PriceSeriesManifest.interval == "1Day",
                    PriceSeriesManifest.created_at <= self._request_time,
                )
                .order_by(
                    PriceBarSetAuthority.available_at.desc(),
                    PriceBarSetAuthority.id,
                )
                .limit(1)
            )
        ).one_or_none()
        if candidate is None:
            return unavailable
        authority, manifest = candidate
        bars = tuple(
            await self._session.scalars(
                select(PriceBarRecord)
                .where(
                    PriceBarRecord.price_series_manifest_id == manifest.id,
                    PriceBarRecord.instrument_id == stable_instrument_id("SPY"),
                    PriceBarRecord.session >= expected_sessions[0],
                    PriceBarRecord.session <= expected_sessions[-1],
                    PriceBarRecord.available_at <= self._request_time,
                    PriceBarRecord.received_at <= self._request_time,
                    PriceBarRecord.created_at <= self._request_time,
                )
                .order_by(PriceBarRecord.session, PriceBarRecord.observed_at)
            )
        )
        if (
            authority.price_series_manifest_id != manifest.id
            or tuple(bar.session for bar in bars) != expected_sessions
        ):
            return unavailable
        return market_benchmark_row_from_closes(
            initial_capital=account.initial_capital,
            closes=tuple(bar.close for bar in bars),
        )

    async def orders(
        self,
        *,
        status: InternalOrderStatus | None,
        cursor: str | None,
        limit: int,
    ) -> tuple[PaperOrderPageResponse, datetime]:
        account, _ = await self.account()
        codec = PaperOrderCursorCodec(self._cursor_secrets, self._request_time)
        state = codec.decode(cursor, requested_status=status) if cursor else None
        decision_time = state.decision_time if state is not None else self._request_time
        latest_events = (
            select(
                PaperOrderEventRecord.order_id,
                func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
            )
            .where(
                PaperOrderEventRecord.account_id == account.id,
                PaperOrderEventRecord.occurred_at <= decision_time,
            )
            .group_by(PaperOrderEventRecord.order_id)
            .subquery()
        )
        statement = (
            select(PaperOrderRecord, PaperOrderEventRecord)
            .join(latest_events, latest_events.c.order_id == PaperOrderRecord.id)
            .join(
                PaperOrderEventRecord,
                and_(
                    PaperOrderEventRecord.order_id == latest_events.c.order_id,
                    PaperOrderEventRecord.event_seq == latest_events.c.event_seq,
                ),
            )
            .where(
                PaperOrderRecord.account_id == account.id,
                PaperOrderRecord.created_at <= decision_time,
            )
            .order_by(desc(PaperOrderRecord.created_at), desc(PaperOrderRecord.id))
            .limit(limit + 1)
        )
        if status is not None:
            statement = statement.where(PaperOrderEventRecord.to_status == status)
        if state is not None:
            statement = statement.where(
                or_(
                    PaperOrderRecord.created_at < state.after_created_at,
                    and_(
                        PaperOrderRecord.created_at == state.after_created_at,
                        PaperOrderRecord.id < state.after_order_id,
                    ),
                )
            )
        rows = (await self._session.execute(statement)).all()
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        items = tuple(
            [
                await self._order(
                    order,
                    latest_event=latest_event,
                    decision_time=decision_time,
                )
                for order, latest_event in page_rows
            ]
        )
        next_cursor = None
        if has_more and page_rows:
            last_order = page_rows[-1][0]
            next_cursor = codec.encode(
                PaperOrderCursorState(
                    decision_time=decision_time,
                    after_created_at=last_order.created_at,
                    after_order_id=last_order.id,
                    status=status,
                )
            )
        return PaperOrderPageResponse(items=items, next_cursor=next_cursor), decision_time

    async def active_orders(self, *, limit: int) -> tuple[PaperOrderResponse, ...]:
        if not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        account, _ = await self.account()
        latest_events = (
            select(
                PaperOrderEventRecord.order_id,
                func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
            )
            .where(
                PaperOrderEventRecord.account_id == account.id,
                PaperOrderEventRecord.occurred_at <= self._request_time,
                PaperOrderEventRecord.created_at <= self._request_time,
            )
            .group_by(PaperOrderEventRecord.order_id)
            .subquery()
        )
        terminal = ("risk_rejected", "filled", "cancelled", "rejected")
        rows = (
            await self._session.execute(
                select(PaperOrderRecord, PaperOrderEventRecord)
                .join(latest_events, latest_events.c.order_id == PaperOrderRecord.id)
                .join(
                    PaperOrderEventRecord,
                    and_(
                        PaperOrderEventRecord.order_id == latest_events.c.order_id,
                        PaperOrderEventRecord.event_seq == latest_events.c.event_seq,
                    ),
                )
                .where(
                    PaperOrderRecord.account_id == account.id,
                    PaperOrderRecord.created_at <= self._request_time,
                    PaperOrderEventRecord.to_status.not_in(terminal),
                )
                .order_by(desc(PaperOrderRecord.created_at), desc(PaperOrderRecord.id))
                .limit(limit)
            )
        ).all()
        return tuple(
            [
                await self._order(
                    order,
                    latest_event=latest_event,
                    decision_time=self._request_time,
                )
                for order, latest_event in rows
            ]
        )

    async def _orders_for_cycle(self, cycle_id: UUID) -> tuple[PaperOrderResponse, ...]:
        account, _ = await self.account()
        latest_events = (
            select(
                PaperOrderEventRecord.order_id,
                func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
            )
            .where(
                PaperOrderEventRecord.account_id == account.id,
                PaperOrderEventRecord.occurred_at <= self._request_time,
                PaperOrderEventRecord.created_at <= self._request_time,
            )
            .group_by(PaperOrderEventRecord.order_id)
            .subquery()
        )
        rows = (
            await self._session.execute(
                select(PaperOrderRecord, PaperOrderEventRecord)
                .join(
                    StrategyDecisionRecord,
                    StrategyDecisionRecord.id == PaperOrderRecord.strategy_decision_id,
                )
                .outerjoin(latest_events, latest_events.c.order_id == PaperOrderRecord.id)
                .outerjoin(
                    PaperOrderEventRecord,
                    and_(
                        PaperOrderEventRecord.order_id == latest_events.c.order_id,
                        PaperOrderEventRecord.event_seq == latest_events.c.event_seq,
                    ),
                )
                .where(
                    StrategyDecisionRecord.agent_paper_cycle_id == cycle_id,
                    StrategyDecisionRecord.decision_origin == "agent_research",
                    StrategyDecisionRecord.created_at <= self._request_time,
                    PaperOrderRecord.account_id == account.id,
                    PaperOrderRecord.created_at <= self._request_time,
                )
                .order_by(desc(PaperOrderRecord.created_at), desc(PaperOrderRecord.id))
                .limit(_MAX_AGENT_ORDERS_PER_CYCLE + 1)
            )
        ).all()
        if len(rows) > _MAX_AGENT_ORDERS_PER_CYCLE or any(event is None for _, event in rows):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper order projection is incomplete.",
                status_code=409,
            )
        return tuple(
            [
                await self._order(
                    order,
                    latest_event=event,
                    decision_time=self._request_time,
                )
                for order, event in rows
                if event is not None
            ]
        )

    async def owns_order(self, order_id: UUID) -> bool:
        account, _ = await self.account()
        return (
            await self._session.scalar(
                select(PaperOrderRecord.id).where(
                    PaperOrderRecord.id == order_id,
                    PaperOrderRecord.account_id == account.id,
                )
            )
            is not None
        )

    async def _order(
        self,
        order: PaperOrderRecord,
        *,
        latest_event: PaperOrderEventRecord,
        decision_time: datetime,
    ) -> PaperOrderResponse:
        events = tuple(
            (
                await self._session.scalars(
                    select(PaperOrderEventRecord)
                    .where(
                        PaperOrderEventRecord.order_id == order.id,
                        PaperOrderEventRecord.occurred_at <= decision_time,
                        PaperOrderEventRecord.created_at <= decision_time,
                    )
                    .order_by(PaperOrderEventRecord.event_seq)
                    .limit(_MAX_EVENTS + 1)
                )
            ).all()
        )
        _require_bounded("order_events", events, _MAX_EVENTS)
        fill_rows = (
            await self._session.execute(
                select(PaperFillRecord, PaperExecutionWindowAuthorityRecord)
                .join(
                    PaperExecutionWindowAuthorityRecord,
                    PaperExecutionWindowAuthorityRecord.id
                    == PaperFillRecord.execution_window_authority_id,
                )
                .where(
                    PaperFillRecord.order_id == order.id,
                    PaperFillRecord.occurred_at <= decision_time,
                )
                .order_by(PaperFillRecord.fill_seq)
                .limit(_MAX_FILLS + 1)
            )
        ).all()
        _require_bounded("fills", fill_rows, _MAX_FILLS)
        authority = (
            await self._session.execute(
                select(
                    StrategyDecisionRecord.strategy_version,
                    RiskPolicyRecord.version,
                    StrategyDecisionRecord.strategy_definition_id,
                    StrategyDecisionRecord.registry_event_id,
                    StrategyDecisionRecord.champion_generation,
                    StrategyDefinitionRecord.strategy_version,
                    StrategyRegistryEventRecord.to_definition_id,
                    StrategyRegistryEventRecord.champion_generation,
                    StrategyRegistryEventRecord.event_type,
                )
                .join(
                    PaperOrderBatchRecord,
                    PaperOrderBatchRecord.strategy_decision_id == StrategyDecisionRecord.id,
                )
                .join(
                    RiskPolicyRecord,
                    RiskPolicyRecord.id == PaperOrderBatchRecord.risk_policy_id,
                )
                .join(
                    StrategyDefinitionRecord,
                    StrategyDefinitionRecord.id == StrategyDecisionRecord.strategy_definition_id,
                )
                .join(
                    StrategyRegistryEventRecord,
                    StrategyRegistryEventRecord.id == StrategyDecisionRecord.registry_event_id,
                )
                .where(PaperOrderBatchRecord.id == order.batch_id)
            )
        ).one_or_none()
        if (
            authority is None
            or not events
            or authority[0] != authority[5]
            or authority[2] != authority[6]
            or authority[4] != authority[7]
            or authority[8] not in {"GENESIS", "PROMOTE", "ROLLBACK"}
        ):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The internal paper order projection is incomplete.",
                status_code=409,
            )
        reservation = await self._session.scalar(
            select(PaperReservationRecord).where(PaperReservationRecord.order_id == order.id)
        )
        outstanding = Decimal(0)
        if reservation is not None:
            reservation_events = tuple(
                (
                    await self._session.scalars(
                        select(PaperReservationEventRecord)
                        .where(
                            PaperReservationEventRecord.reservation_id == reservation.id,
                            PaperReservationEventRecord.occurred_at <= decision_time,
                        )
                        .order_by(PaperReservationEventRecord.event_seq)
                    )
                ).all()
            )
            for event in reservation_events:
                outstanding += event.amount if event.event_type == "reserved" else -event.amount
            if outstanding < 0:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The internal paper reservation projection is invalid.",
                    status_code=409,
                )
        filled = sum((fill.quantity for fill, _ in fill_rows), start=Decimal(0))
        symbol = await self._symbol(order.instrument_id, latest_event.occurred_at)
        submitted_at = next(
            (event.occurred_at for event in events if event.to_status == "submitted"),
            events[0].occurred_at,
        )
        status = cast(InternalOrderStatus, latest_event.to_status)
        return PaperOrderResponse(
            order_id=order.id,
            instrument_id=order.instrument_id,
            symbol=symbol,
            side=cast(Literal["buy", "sell"], order.side),
            quantity=decimal_to_string(order.quantity),
            filled_quantity=decimal_to_string(filled),
            remaining_quantity=decimal_to_string(order.quantity - filled),
            worst_price=decimal_to_string(order.worst_price),
            status=status,
            cancellable=(status in {"submitted", "partially_filled"} and outstanding > 0),
            current_event_seq=latest_event.event_seq,
            submitted_at=submitted_at,
            strategy_version=authority[0],
            risk_policy_version=authority[1],
            events=tuple(
                PaperOrderEventResponse(
                    event_seq=event.event_seq,
                    from_status=cast(InternalOrderStatus | None, event.from_status),
                    to_status=cast(InternalOrderStatus, event.to_status),
                    actor_kind=cast(
                        Literal["task18", "task19", "user", "reconciliation"],
                        event.actor_kind,
                    ),
                    reason_code=event.reason_code,
                    occurred_at=event.occurred_at,
                )
                for event in events
            ),
            fills=tuple(
                PaperFillResponse(
                    fill_id=fill.id,
                    fill_seq=fill.fill_seq,
                    quantity=decimal_to_string(fill.quantity),
                    price=decimal_to_string(fill.price),
                    notional=decimal_to_string(fill.notional),
                    fee=decimal_to_string(fill.fee),
                    occurred_at=fill.occurred_at,
                    source=window.source,
                    feed=window.feed,
                    delivery_mode=cast(
                        Literal["real_time", "delayed", "fixture", "unknown"],
                        window.delivery_mode,
                    ),
                    source_as_of=window.data_available_at,
                    execution_model_hash=fill.execution_model_hash,
                    slippage_bps=cast(Literal[10, 25], fill.slippage_bps),
                    slippage=decimal_to_string(fill.slippage_amount),
                )
                for fill, window in fill_rows
            ),
        )

    async def _symbols(
        self,
        instrument_ids: tuple[UUID, ...],
        as_of: datetime,
    ) -> dict[UUID, str]:
        if not instrument_ids:
            return {}
        rows = await self._session.execute(
            select(InstrumentAlias.instrument_id, InstrumentAlias.symbol)
            .where(
                InstrumentAlias.instrument_id.in_(instrument_ids),
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= as_of.date(),
                or_(
                    InstrumentAlias.valid_to.is_(None),
                    InstrumentAlias.valid_to >= as_of.date(),
                ),
            )
            .order_by(
                InstrumentAlias.instrument_id,
                desc(InstrumentAlias.valid_from),
                desc(InstrumentAlias.id),
            )
        )
        result: dict[UUID, str] = {}
        for instrument_id, symbol in rows:
            result.setdefault(instrument_id, symbol)
        return result

    async def _symbol(self, instrument_id: UUID, as_of: datetime) -> str | None:
        return (await self._symbols((instrument_id,), as_of)).get(instrument_id)


def _require_bounded(name: str, values: Sequence[object], maximum: int) -> None:
    if len(values) <= maximum:
        return
    raise NexusError(
        ErrorCode.DATA_PARTIAL,
        "The internal paper response exceeds a safe bound.",
        status_code=409,
        details={"collection": name, "maximum": maximum},
    )
