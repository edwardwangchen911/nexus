"""Redacted PostgreSQL read models for the private internal-paper console."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, cast
from uuid import UUID

from sqlalchemy import and_, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.paper import (
    AgentPaperCandidateResponse,
    AgentPaperCycleResponse,
    AgentPaperDecisionResponse,
    AgentPaperMarketResponse,
    PaperAttributionResponse,
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
from nexus.paper.cursor import PaperOrderCursorCodec, PaperOrderCursorState
from nexus.paper.models import InternalOrderStatus
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.data import PriceBarSetAuthority, PriceSeriesManifest
from nexus.persistence.models.evolution import StrategyRegistryEventRecord
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.paper import (
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
    RiskPolicyRecord,
    StrategyDecisionAgentOutputRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.portfolio import Portfolio

_MAX_POSITIONS = 500
_MAX_ATTRIBUTIONS = 500
_MAX_EVENTS = 128
_MAX_FILLS = 128
_STALE_AFTER = timedelta(hours=36)


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
            )
            .order_by(AgentPaperCycleRecord.decision_time.desc(), AgentPaperCycleRecord.id.desc())
            .limit(1)
        )
        if cycle is None:
            return None
        event = await self._session.scalar(
            select(AgentPaperCycleEventRecord)
            .where(AgentPaperCycleEventRecord.cycle_id == cycle.id)
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
            .limit(1)
        )
        if event is None:
            raise NexusError(
                ErrorCode.DATA_PARTIAL, "The Agent paper cycle is incomplete.", status_code=409
            )
        decision = await self._session.scalar(
            select(StrategyDecisionRecord).where(
                StrategyDecisionRecord.account_id == cycle.account_id,
                StrategyDecisionRecord.decision_session == cycle.decision_session,
                StrategyDecisionRecord.decision_origin == "agent_research",
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
                    .where(StrategyDecisionAgentOutputRecord.strategy_decision_id == decision.id)
                    .limit(6)
                )
            ).all()
            if len(rows) > 5:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL, "The Agent paper cycle is unbounded.", status_code=409
                )
            outputs = {instrument_id: (output, binding) for instrument_id, output, binding in rows}
        candidates = []
        for raw in cycle.candidate_manifest:
            instrument_id = UUID(str(raw["instrument_id"]))
            bound = outputs.get(instrument_id)
            output, binding = bound if bound is not None else (None, None)
            intent = output.intent if output is not None and isinstance(output.intent, dict) else {}
            stance = intent.get("stance")
            candidates.append(
                AgentPaperCandidateResponse(
                    instrument_id=instrument_id,
                    symbol=str(raw["symbol"]),
                    selection_reasons=(str(raw["inclusion_reason"]),),
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
                    .where(PaperOrderRecord.strategy_decision_id == decision.id)
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
                    .order_by(PaperOrderRecord.id)
                    .limit(50)
                )
            )
            decision_response = AgentPaperDecisionResponse(
                decision_id=decision.id,
                sizing_policy_version="agent_paper_sizing_v1",
                target_cash_weight=decimal_to_string(decision.cash_weight),
                risk_outcomes=cast(
                    tuple[Literal["approved", "reduced", "rejected"], ...], risk_outcomes
                ),
            )
        windows = tuple(
            await self._session.scalars(
                select(AgentPaperMarketWindowRecord)
                .where(AgentPaperMarketWindowRecord.cycle_id == cycle.id)
                .order_by(AgentPaperMarketWindowRecord.created_at.desc())
                .limit(51)
            )
        )
        if len(windows) > 50:
            raise NexusError(
                ErrorCode.DATA_PARTIAL, "The Agent paper cycle is unbounded.", status_code=409
            )
        market_status: Literal["pending", "qualified", "probe"] = "pending"
        if any(row.purpose == "paper_fill" for row in windows):
            market_status = "qualified"
        elif windows:
            market_status = "probe"
        return AgentPaperCycleResponse(
            cycle_id=cycle.id,
            status=cast(Any, event.to_status),
            result_code=event.result_code,
            decision_session=cycle.decision_session,
            decision_time=cycle.decision_time,
            execution_session=cycle.execution_session,
            execution_locked=True,
            candidate_count=len(candidates),
            candidates=tuple(candidates),
            decision=decision_response,
            market=AgentPaperMarketResponse(
                source="alpaca",
                feed="iex",
                delivery_mode="real_time",
                coverage="IEX_EXCHANGE_SUBSET",
                window_minutes=5,
                status=market_status,
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
                    PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
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
            positions=positions,
            attributions=attributions,
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
