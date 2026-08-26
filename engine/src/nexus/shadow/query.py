"""Fail-closed reconstruction of the public Proposal Shadow read model."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.optimizer.models import ProposalGroupStatus
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.research import CorporateActionRecord
from nexus.persistence.models.shadow import (
    ShadowAdvanceAttemptRecord,
    ShadowCheckpointMetricRecord,
    ShadowCheckpointRecord,
    ShadowGroupRecord,
    ShadowInstrumentStateRecord,
    ShadowPortfolioRecord,
    ShadowPortfolioStateRecord,
    ShadowReceivableStateRecord,
    ShadowSealRecord,
    ShadowSessionAuthorityRecord,
    ShadowSessionEventInputRecord,
    ShadowSessionPriceInputRecord,
    ShadowSimulatedFillRecord,
    ShadowSimulatedOutcomeRecord,
)
from nexus.shadow.authority import ShadowSourceSummary
from nexus.shadow.canonical import (
    checkpoint_metrics_content_hash,
    final_authority_content_hash,
    proposal_bundle_content_hash,
    shadow_query_bundle_content_hash,
    shadow_session_content_hash,
    simulation_state_content_hash,
    target_weights_content_hash,
)
from nexus.shadow.models import (
    SHADOW_VARIANTS,
    CalendarHashBinding,
    EventInputHash,
    MissingExecutionInputHash,
    PriceInputHash,
    ProposalVariantHashInput,
    ShadowMetrics,
    ShadowMetricValue,
    ShadowNavPoint,
    ShadowSessionHashInput,
    ShadowVariant,
    VariantMetrics,
    WorstSession,
)
from nexus.shadow.repository import load_proposals_and_positions, target_weight_for
from nexus.simulation.canonical import canonical_decimal
from nexus.simulation.models import (
    DividendReceivable,
    SimulationInstrumentState,
    SimulationState,
    TargetWeight,
)


@dataclass(frozen=True)
class ShadowCoverageView:
    completed: int
    due: int
    required: Literal[63]
    next_expected_session: date | None
    missing_sessions: tuple[date, ...]


@dataclass(frozen=True)
class ShadowFreshnessView:
    status: Literal["current", "stale", "unknown"]
    latest_session: date | None
    available_at: datetime | None
    last_attempt_at: datetime | None
    database_time: datetime
    reason: str | None


@dataclass(frozen=True)
class ShadowSourceView:
    source: str
    kind: str
    delivery_mode: str
    latest_session: date
    available_at: datetime


@dataclass(frozen=True)
class ShadowFillView:
    side: str
    quantity: Decimal
    price: Decimal
    notional: Decimal
    cost: Decimal


@dataclass(frozen=True)
class ShadowOutcomeView:
    instrument_id: UUID
    side: str
    status: str
    reason: str
    fill: ShadowFillView | None


@dataclass(frozen=True)
class ShadowPortfolioView:
    shadow_portfolio_id: UUID
    variant: ShadowVariant
    nav: tuple[ShadowNavPoint, ...]
    initial_outcomes: tuple[ShadowOutcomeView, ...]


@dataclass(frozen=True)
class ShadowCheckpointView:
    ordinal: int
    session: date
    status: str
    coverage_count: int
    reason: str | None
    metrics: tuple[VariantMetrics, VariantMetrics, VariantMetrics]


@dataclass(frozen=True)
class ShadowAttemptView:
    requested_session: date
    expected_session: date | None
    result: str
    reason: str
    observed_at: datetime
    created_at: datetime


@dataclass(frozen=True)
class ShadowGroupView:
    proposal_group_id: UUID
    shadow_group_id: UUID
    proposal_status: ProposalGroupStatus
    version: int
    start_session: date
    evaluation_end_session: date
    grace_end_session: date
    coverage: ShadowCoverageView
    freshness: ShadowFreshnessView
    sources: tuple[ShadowSourceView, ...]
    portfolios: tuple[ShadowPortfolioView, ShadowPortfolioView, ShadowPortfolioView]
    checkpoints: tuple[ShadowCheckpointView, ...]
    last_failed_attempt: ShadowAttemptView | None
    sealed: bool
    seal_reason: Literal["validated", "rejected", "expired"] | None
    creates_orders: Literal[False] = False
    trading_approval: Literal[False] = False


async def load_shadow_view(
    session: AsyncSession,
    *,
    group_id: UUID,
) -> ShadowGroupView:
    """Return a fully verified projection or one stable fail-closed reason."""
    group = await session.get(OptimizationProposalGroupRecord, group_id)
    if group is None:
        raise ValueError("PROPOSAL_GROUP_NOT_FOUND")
    shadow = cast(
        ShadowGroupRecord | None,
        await session.scalar(
            select(ShadowGroupRecord).where(ShadowGroupRecord.proposal_group_id == group_id)
        ),
    )
    if shadow is None:
        raise ValueError("SHADOW_NOT_STARTED")
    try:
        return await _load_verified_view(session, group=group, shadow=shadow)
    except (KeyError, TypeError, ValueError, IndexError):
        raise ValueError("DATA_PARTIAL") from None


async def _load_verified_view(
    session: AsyncSession,
    *,
    group: OptimizationProposalGroupRecord,
    shadow: ShadowGroupRecord,
) -> ShadowGroupView:
    portfolios = await _verify_proposal_root(session, group=group, shadow=shadow)
    authorities = tuple(
        await session.scalars(
            select(ShadowSessionAuthorityRecord)
            .where(ShadowSessionAuthorityRecord.shadow_group_id == shadow.id)
            .order_by(ShadowSessionAuthorityRecord.ordinal)
        )
    )
    if len(authorities) > 63 or any(
        authority.ordinal != index or authority.session != shadow.evaluation_sessions[index - 1]
        for index, authority in enumerate(authorities, start=1)
    ):
        raise ValueError("authority sequence is invalid")
    source_rows: list[tuple[date, ShadowSourceSummary]] = []
    for authority in authorities:
        source_rows.extend(
            (authority.session, item)
            for item in await _verify_session_authority(session, shadow=shadow, authority=authority)
        )

    states = await _verify_states(
        session,
        shadow=shadow,
        portfolios=portfolios,
        authorities=authorities,
    )
    outcomes = await _load_verified_outcomes(
        session, shadow=shadow, portfolios=portfolios, states=states
    )
    portfolio_views = cast(
        tuple[ShadowPortfolioView, ShadowPortfolioView, ShadowPortfolioView],
        tuple(
            ShadowPortfolioView(
                shadow_portfolio_id=portfolio.id,
                variant=cast(ShadowVariant, portfolio.variant),
                nav=tuple(
                    ShadowNavPoint(session=state.session, nav=state.nav)
                    for state in states[portfolio.id]
                ),
                initial_outcomes=outcomes[portfolio.id],
            )
            for portfolio in portfolios
        ),
    )
    checkpoints = await _load_verified_checkpoints(session, shadow=shadow, portfolios=portfolios)
    seal = cast(
        ShadowSealRecord | None,
        await session.scalar(
            select(ShadowSealRecord).where(ShadowSealRecord.shadow_group_id == shadow.id)
        ),
    )
    _verify_seal_status(
        group=group,
        authorities=authorities,
        checkpoints=checkpoints,
        seal=seal,
    )
    attempts = tuple(
        await session.scalars(
            select(ShadowAdvanceAttemptRecord)
            .where(ShadowAdvanceAttemptRecord.shadow_group_id == shadow.id)
            .order_by(
                ShadowAdvanceAttemptRecord.created_at,
                ShadowAdvanceAttemptRecord.id,
            )
        )
    )
    database_time = await session.scalar(select(func.transaction_timestamp()))
    if not isinstance(database_time, datetime):
        raise ValueError("database time is unavailable")
    database_time = database_time.astimezone(UTC)
    calendar_rows = tuple(
        await session.scalars(
            select(ExchangeCalendarSessionRecord)
            .where(
                ExchangeCalendarSessionRecord.calendar_authority_id == shadow.calendar_authority_id,
                ExchangeCalendarSessionRecord.session.in_(shadow.evaluation_sessions),
            )
            .order_by(ExchangeCalendarSessionRecord.session)
        )
    )
    if len(calendar_rows) != 63:
        raise ValueError("calendar members are incomplete")
    due_sessions = tuple(row.session for row in calendar_rows if row.decision_at <= database_time)
    successful_sessions = {authority.session for authority in authorities}
    missing_sessions = tuple(
        session_date for session_date in due_sessions if session_date not in successful_sessions
    )
    completed = len(authorities)
    due = len(due_sessions)
    freshness_status: Literal["current", "stale", "unknown"] = (
        "unknown" if due == 0 else "current" if completed == due else "stale"
    )
    last_failed = next(
        (attempt for attempt in reversed(attempts) if attempt.result != "applied"),
        None,
    )
    latest_available = max((authority.available_at for authority in authorities), default=None)
    return ShadowGroupView(
        proposal_group_id=group.id,
        shadow_group_id=shadow.id,
        proposal_status=cast(ProposalGroupStatus, group.status),
        version=group.version,
        start_session=shadow.start_session,
        evaluation_end_session=shadow.evaluation_end_session,
        grace_end_session=shadow.grace_end_session,
        coverage=ShadowCoverageView(
            completed=completed,
            due=due,
            required=63,
            next_expected_session=(
                None if completed == 63 else shadow.evaluation_sessions[completed]
            ),
            missing_sessions=missing_sessions,
        ),
        freshness=ShadowFreshnessView(
            status=freshness_status,
            latest_session=None if not authorities else authorities[-1].session,
            available_at=latest_available,
            last_attempt_at=None if not attempts else attempts[-1].created_at,
            database_time=database_time,
            reason=(
                None
                if freshness_status == "current"
                else "NO_SESSION_DUE"
                if freshness_status == "unknown"
                else "SHADOW_SESSION_DUE"
            ),
        ),
        sources=_source_views(source_rows),
        portfolios=portfolio_views,
        checkpoints=checkpoints,
        last_failed_attempt=(None if last_failed is None else _attempt_view(last_failed)),
        sealed=seal is not None,
        seal_reason=(
            None if seal is None else cast(Literal["validated", "rejected", "expired"], seal.reason)
        ),
    )


async def _verify_proposal_root(
    session: AsyncSession,
    *,
    group: OptimizationProposalGroupRecord,
    shadow: ShadowGroupRecord,
) -> tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord]:
    proposal_rows, position_rows = await load_proposals_and_positions(session, group.id)
    portfolio_rows = tuple(
        await session.scalars(
            select(ShadowPortfolioRecord).where(ShadowPortfolioRecord.shadow_group_id == shadow.id)
        )
    )
    by_variant = {row.variant: row for row in portfolio_rows}
    proposal_by_variant = {row.variant: row for row in proposal_rows}
    if (
        len(portfolio_rows) != 3
        or set(by_variant) != set(SHADOW_VARIANTS)
        or shadow.proposal_group_id != group.id
        or shadow.source_policy_id != group.source_policy_id
        or shadow.cost_policy_id != group.cost_policy_id
        or shadow.baseline_portfolio_snapshot_id != group.baseline_portfolio_snapshot_id
    ):
        raise ValueError("proposal root cardinality is invalid")
    hash_inputs: list[ProposalVariantHashInput] = []
    for variant in SHADOW_VARIANTS:
        portfolio = by_variant[variant]
        proposal = proposal_by_variant[variant]
        targets = tuple(
            TargetWeight(
                instrument_id=position.instrument_id,
                weight=target_weight_for(variant, position),
            )
            for position in position_rows[proposal.id]
        )
        cash_weight = Decimal(1) - sum((item.weight for item in targets), start=Decimal(0))
        if (
            portfolio.proposal_id != proposal.id
            or portfolio.target_weights_hash != target_weights_content_hash(targets, cash_weight)
        ):
            raise ValueError("proposal target binding is invalid")
        hash_inputs.append(
            ProposalVariantHashInput(
                proposal_id=proposal.id,
                variant=variant,
                targets=targets,
                cash_weight=cash_weight,
            )
        )
    if proposal_bundle_content_hash(tuple(hash_inputs)) != shadow.proposal_bundle_hash:  # type: ignore[arg-type]
        raise ValueError("proposal bundle hash mismatch")
    return cast(
        tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord],
        tuple(by_variant[variant] for variant in SHADOW_VARIANTS),
    )


async def _verify_session_authority(
    session: AsyncSession,
    *,
    shadow: ShadowGroupRecord,
    authority: ShadowSessionAuthorityRecord,
) -> tuple[ShadowSourceSummary, ...]:
    summary_document = authority.source_summary
    raw_queries = summary_document.get("queries")
    raw_missing = summary_document.get("missing_execution_inputs")
    raw_modes = authority.delivery_mode_summary.get("modes")
    if (
        set(summary_document) != {"queries", "missing_execution_inputs"}
        or set(authority.delivery_mode_summary) != {"modes"}
        or not isinstance(raw_queries, list)
        or not isinstance(raw_missing, list)
        or not isinstance(raw_modes, list)
        or any(not isinstance(mode, str) for mode in raw_modes)
    ):
        raise ValueError("authority summaries are invalid")
    summaries = tuple(_source_summary(item) for item in raw_queries)
    if (
        shadow_query_bundle_content_hash(cast(tuple[object, ...], summaries))
        != authority.query_hash
    ):
        raise ValueError("authority query hash mismatch")
    price_rows = tuple(
        await session.scalars(
            select(ShadowSessionPriceInputRecord).where(
                ShadowSessionPriceInputRecord.session_authority_id == authority.id
            )
        )
    )
    event_rows = tuple(
        await session.scalars(
            select(ShadowSessionEventInputRecord).where(
                ShadowSessionEventInputRecord.session_authority_id == authority.id
            )
        )
    )
    events: list[EventInputHash] = []
    for row in event_rows:
        action = await session.get(CorporateActionRecord, row.corporate_action_id)
        if action is None or action.instrument_id is None:
            raise ValueError("event authority member is missing")
        events.append(
            EventInputHash(
                instrument_id=action.instrument_id,
                authority_id=row.corporate_action_id,
                event_kind=cast(Literal["split", "dividend", "delisting"], row.event_kind),
                source=action.source,
                provider_event_id=row.provider_event_id,
                revision=row.revision,
                available_at=row.available_at,
                evidence_hash=row.evidence_hash,
            )
        )
    missing = tuple(_missing_input(item) for item in raw_missing)
    hash_input = ShadowSessionHashInput(
        shadow_group_id=shadow.id,
        session=authority.session,
        ordinal=authority.ordinal,
        calendar=CalendarHashBinding(
            authority_id=shadow.calendar_authority_id,
            version=shadow.calendar_version,
            content_hash=shadow.calendar_hash,
        ),
        source_policy_id=shadow.source_policy_id,
        cost_policy_id=shadow.cost_policy_id,
        bars=tuple(
            PriceInputHash(
                instrument_id=row.instrument_id,
                authority_id=row.price_bar_set_authority_id,
                source=row.source,
                delivery_mode=row.delivery_mode,
                revision=row.revision,
                available_at=row.available_at,
                content_hash=row.content_hash,
                purpose=cast(Literal["execution_and_mark", "mark"], row.usage),
            )
            for row in price_rows
        ),
        missing_execution_inputs=missing,
        events=tuple(events),
    )
    if (
        authority.calendar_authority_id != shadow.calendar_authority_id
        or authority.source_policy_id != shadow.source_policy_id
        or authority.cost_policy_id != shadow.cost_policy_id
        or shadow_session_content_hash(hash_input) != authority.content_hash
        or sorted(set(raw_modes)) != sorted({row.delivery_mode for row in price_rows})
    ):
        raise ValueError("session authority hash mismatch")
    maximum_available = max(
        (
            *(row.available_at for row in price_rows),
            *(row.available_at for row in event_rows),
            *(item.available_at for item in summaries),
        ),
        default=authority.observed_at,
    )
    if (
        authority.available_at != maximum_available
        or authority.available_at > authority.observed_at
    ):
        raise ValueError("session authority availability mismatch")
    return summaries


def _source_summary(value: object) -> ShadowSourceSummary:
    if not isinstance(value, dict):
        raise TypeError("source summary must be an object")
    if set(value) != {
        "source",
        "kind",
        "delivery_mode",
        "complete",
        "query_hash",
        "member_count",
        "negative_count",
        "available_at",
    }:
        raise ValueError("source summary fields are invalid")
    return ShadowSourceSummary(
        source=cast(str, value["source"]),
        kind=cast(Literal["price_bars", "corporate_actions"], value["kind"]),
        delivery_mode=cast(str, value["delivery_mode"]),
        complete=cast(bool, value["complete"]),
        query_hash=cast(str, value["query_hash"]),
        member_count=cast(int, value["member_count"]),
        negative_count=cast(int, value["negative_count"]),
        available_at=datetime.fromisoformat(cast(str, value["available_at"])),
    )


def _missing_input(value: object) -> MissingExecutionInputHash:
    if not isinstance(value, dict):
        raise TypeError("missing input must be an object")
    if set(value) != {"instrument_id", "source", "query_hash", "reason"}:
        raise ValueError("missing input fields are invalid")
    return MissingExecutionInputHash(
        instrument_id=UUID(cast(str, value["instrument_id"])),
        source=cast(str, value["source"]),
        query_hash=cast(str, value["query_hash"]),
        reason=cast(str, value["reason"]),
    )


async def _verify_states(
    session: AsyncSession,
    *,
    shadow: ShadowGroupRecord,
    portfolios: tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord],
    authorities: tuple[ShadowSessionAuthorityRecord, ...],
) -> dict[UUID, tuple[ShadowPortfolioStateRecord, ...]]:
    state_rows = tuple(
        await session.scalars(
            select(ShadowPortfolioStateRecord)
            .where(ShadowPortfolioStateRecord.shadow_group_id == shadow.id)
            .order_by(
                ShadowPortfolioStateRecord.shadow_portfolio_id,
                ShadowPortfolioStateRecord.ordinal,
            )
        )
    )
    instrument_rows = tuple(
        await session.scalars(
            select(ShadowInstrumentStateRecord).where(
                ShadowInstrumentStateRecord.shadow_group_id == shadow.id
            )
        )
    )
    receivable_rows = tuple(
        await session.scalars(
            select(ShadowReceivableStateRecord).where(
                ShadowReceivableStateRecord.shadow_group_id == shadow.id
            )
        )
    )
    instruments_by_state: dict[UUID, list[ShadowInstrumentStateRecord]] = defaultdict(list)
    receivables_by_state: dict[UUID, list[ShadowReceivableStateRecord]] = defaultdict(list)
    for instrument_row in instrument_rows:
        instruments_by_state[instrument_row.portfolio_state_id].append(instrument_row)
    for receivable_row in receivable_rows:
        receivables_by_state[receivable_row.portfolio_state_id].append(receivable_row)
    portfolio_ids = {portfolio.id for portfolio in portfolios}
    authority_by_ordinal = {authority.ordinal: authority for authority in authorities}
    by_portfolio: dict[UUID, list[ShadowPortfolioStateRecord]] = {
        portfolio.id: [] for portfolio in portfolios
    }
    if len(state_rows) != len(authorities) * 3:
        raise ValueError("state cardinality mismatch")
    for state_row in state_rows:
        if state_row.shadow_portfolio_id not in portfolio_ids:
            raise ValueError("state portfolio mismatch")
        authority = authority_by_ordinal.get(state_row.ordinal)
        if (
            authority is None
            or state_row.session_authority_id != authority.id
            or state_row.session != authority.session
        ):
            raise ValueError("state authority binding mismatch")
        instrument_states = tuple(
            SimulationInstrumentState(
                instrument_id=item.instrument_id,
                quantity=item.quantity,
                cost_basis=item.cost_basis,
                realized_pnl=item.realized_pnl,
                cumulative_cost=item.cumulative_cost,
                last_close=item.last_close,
                untradable=item.untradable,
            )
            for item in instruments_by_state[state_row.id]
        )
        receivables = tuple(
            DividendReceivable(
                authority_id=item.corporate_action_id,
                instrument_id=item.instrument_id,
                amount=item.amount,
                payable_session=item.payable_session,
            )
            for item in receivables_by_state[state_row.id]
        )
        state = SimulationState(
            cash=state_row.cash,
            instruments=instrument_states,
            receivables=receivables,
        )
        reconstructed_nav = state_row.cash + sum(
            (item.amount for item in receivables), start=Decimal(0)
        )
        for item in instrument_states:
            if item.quantity > 0 and not item.untradable:
                if item.last_close is None:
                    raise ValueError("held state has no close")
                reconstructed_nav += item.quantity * item.last_close
        if simulation_state_content_hash(state) != state_row.state_hash:
            raise ValueError("state hash mismatch")
        if canonical_decimal(reconstructed_nav) != state_row.nav:
            raise ValueError("state nav mismatch")
        if (
            sum(
                (item.realized_pnl for item in instrument_states),
                start=Decimal(0),
            )
            != state_row.cumulative_realized_pnl
        ):
            raise ValueError("state realized pnl mismatch")
        if (
            sum(
                (item.cumulative_cost for item in instrument_states),
                start=Decimal(0),
            )
            != state_row.cumulative_fees
        ):
            raise ValueError("state fees mismatch")
        by_portfolio[state_row.shadow_portfolio_id].append(state_row)
    for portfolio in portfolios:
        rows = by_portfolio[portfolio.id]
        if len(rows) != len(authorities) or any(
            state_row.ordinal != index for index, state_row in enumerate(rows, start=1)
        ):
            raise ValueError("portfolio state sequence mismatch")
    return {key: tuple(value) for key, value in by_portfolio.items()}


async def _load_verified_outcomes(
    session: AsyncSession,
    *,
    shadow: ShadowGroupRecord,
    portfolios: tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord],
    states: dict[UUID, tuple[ShadowPortfolioStateRecord, ...]],
) -> dict[UUID, tuple[ShadowOutcomeView, ...]]:
    outcome_rows = tuple(
        await session.scalars(
            select(ShadowSimulatedOutcomeRecord).where(
                ShadowSimulatedOutcomeRecord.shadow_group_id == shadow.id
            )
        )
    )
    fill_rows = tuple(
        await session.scalars(
            select(ShadowSimulatedFillRecord).where(
                ShadowSimulatedFillRecord.shadow_group_id == shadow.id
            )
        )
    )
    fills_by_outcome = {row.outcome_id: row for row in fill_rows}
    if len(fills_by_outcome) != len(fill_rows):
        raise ValueError("fill cardinality mismatch")
    result: dict[UUID, list[ShadowOutcomeView]] = {portfolio.id: [] for portfolio in portfolios}
    turnover: dict[UUID, Decimal] = defaultdict(Decimal)
    fees: dict[UUID, Decimal] = defaultdict(Decimal)
    for outcome in outcome_rows:
        fill = fills_by_outcome.get(outcome.id)
        if (outcome.status == "filled") != (fill is not None):
            raise ValueError("outcome fill binding mismatch")
        if fill is not None:
            turnover[outcome.shadow_portfolio_id] += fill.notional
            fees[outcome.shadow_portfolio_id] += fill.cost
        result[outcome.shadow_portfolio_id].append(
            ShadowOutcomeView(
                instrument_id=outcome.instrument_id,
                side=outcome.side,
                status=outcome.status,
                reason=outcome.reason,
                fill=(
                    None
                    if fill is None
                    else ShadowFillView(
                        side=fill.side,
                        quantity=fill.quantity,
                        price=fill.price,
                        notional=fill.notional,
                        cost=fill.cost,
                    )
                ),
            )
        )
    for portfolio in portfolios:
        for state in states[portfolio.id]:
            if (
                state.cumulative_traded_notional != turnover[portfolio.id]
                or state.cumulative_fees != fees[portfolio.id]
            ):
                raise ValueError("cumulative execution totals mismatch")
    return {
        key: tuple(sorted(value, key=lambda item: item.instrument_id.int))
        for key, value in result.items()
    }


async def _load_verified_checkpoints(
    session: AsyncSession,
    *,
    shadow: ShadowGroupRecord,
    portfolios: tuple[ShadowPortfolioRecord, ShadowPortfolioRecord, ShadowPortfolioRecord],
) -> tuple[ShadowCheckpointView, ...]:
    checkpoint_rows = tuple(
        await session.scalars(
            select(ShadowCheckpointRecord)
            .where(ShadowCheckpointRecord.shadow_group_id == shadow.id)
            .order_by(ShadowCheckpointRecord.ordinal)
        )
    )
    portfolio_by_variant = {portfolio.variant: portfolio for portfolio in portfolios}
    result: list[ShadowCheckpointView] = []
    for checkpoint in checkpoint_rows:
        metric_rows = tuple(
            await session.scalars(
                select(ShadowCheckpointMetricRecord).where(
                    ShadowCheckpointMetricRecord.checkpoint_id == checkpoint.id
                )
            )
        )
        by_variant = {row.variant: row for row in metric_rows}
        if len(metric_rows) != 3 or set(by_variant) != set(SHADOW_VARIANTS):
            raise ValueError("checkpoint metric cardinality mismatch")
        metrics = cast(
            tuple[VariantMetrics, VariantMetrics, VariantMetrics],
            tuple(
                _variant_metrics(
                    by_variant[variant],
                    expected_portfolio_id=portfolio_by_variant[variant].id,
                    expected_known=checkpoint.status == "complete",
                )
                for variant in SHADOW_VARIANTS
            ),
        )
        if checkpoint_metrics_content_hash(metrics) != checkpoint.metrics_hash:
            raise ValueError("checkpoint metric hash mismatch")
        result.append(
            ShadowCheckpointView(
                ordinal=checkpoint.ordinal,
                session=checkpoint.session,
                status=checkpoint.status,
                coverage_count=checkpoint.coverage_count,
                reason=checkpoint.reason,
                metrics=metrics,
            )
        )
    return tuple(result)


def _variant_metrics(
    row: ShadowCheckpointMetricRecord,
    *,
    expected_portfolio_id: UUID,
    expected_known: bool,
) -> VariantMetrics:
    known = row.metric_status == "known"
    if row.shadow_portfolio_id != expected_portfolio_id or known != expected_known:
        raise ValueError("checkpoint metric portfolio or status mismatch")
    if known:
        if row.worst_sessions is None:
            raise ValueError("known checkpoint metric lacks worst sessions")
        worst = tuple(
            WorstSession(
                session=date.fromisoformat(cast(str, item["session"])),
                return_value=Decimal(cast(str, item["return"])),
            )
            for item in row.worst_sessions
        )
        return VariantMetrics(
            variant=cast(ShadowVariant, row.variant),
            metrics=ShadowMetrics(
                net_cumulative_return=ShadowMetricValue.known(_known(row.net_cumulative_return)),
                return_delta_vs_hold=ShadowMetricValue.known(_known(row.return_delta_vs_hold)),
                max_drawdown=ShadowMetricValue.known(_known(row.max_drawdown)),
                drawdown_improvement=ShadowMetricValue.known(_known(row.drawdown_improvement)),
                annualized_volatility=ShadowMetricValue.known(_known(row.annualized_volatility)),
                volatility_improvement=ShadowMetricValue.known(_known(row.volatility_improvement)),
                worst_sessions=worst,
                cumulative_turnover=ShadowMetricValue.known(_known(row.cumulative_turnover)),
                fee_drag_index_points=ShadowMetricValue.known(_known(row.fee_drag_index_points)),
            ),
        )
    if not row.unknown_reason:
        raise ValueError("unknown checkpoint metric lacks reason")
    unknown = ShadowMetricValue.unknown
    return VariantMetrics(
        variant=cast(ShadowVariant, row.variant),
        metrics=ShadowMetrics(
            net_cumulative_return=unknown(row.unknown_reason),
            return_delta_vs_hold=unknown(row.unknown_reason),
            max_drawdown=unknown(row.unknown_reason),
            drawdown_improvement=unknown(row.unknown_reason),
            annualized_volatility=unknown(row.unknown_reason),
            volatility_improvement=unknown(row.unknown_reason),
            worst_sessions=(),
            cumulative_turnover=unknown(row.unknown_reason),
            fee_drag_index_points=unknown(row.unknown_reason),
        ),
    )


def _known(value: Decimal | None) -> Decimal:
    if value is None:
        raise ValueError("known metric value is missing")
    return value


def _verify_seal_status(
    *,
    group: OptimizationProposalGroupRecord,
    authorities: tuple[ShadowSessionAuthorityRecord, ...],
    checkpoints: tuple[ShadowCheckpointView, ...],
    seal: ShadowSealRecord | None,
) -> None:
    if seal is None:
        if group.status != "shadowing" or len(authorities) == 63:
            raise ValueError("unsealed lifecycle mismatch")
        return
    if group.status != seal.reason or seal.final_coverage != len(authorities):
        raise ValueError("seal lifecycle mismatch")
    expected_hash = final_authority_content_hash(
        tuple((item.session, item.content_hash) for item in authorities),
        tuple(
            (item.ordinal, checkpoint_metrics_content_hash(item.metrics)) for item in checkpoints
        ),
    )
    if seal.final_authority_hash != expected_hash:
        raise ValueError("seal authority hash mismatch")


def _source_views(
    values: list[tuple[date, ShadowSourceSummary]],
) -> tuple[ShadowSourceView, ...]:
    latest: dict[tuple[str, str, str], tuple[date, datetime]] = {}
    for session_date, summary in values:
        key = (summary.source, summary.kind, summary.delivery_mode)
        candidate = (session_date, summary.available_at)
        if key not in latest or candidate > latest[key]:
            latest[key] = candidate
    return tuple(
        ShadowSourceView(
            source=key[0],
            kind=key[1],
            delivery_mode=key[2],
            latest_session=value[0],
            available_at=value[1],
        )
        for key, value in sorted(latest.items())
    )


def _attempt_view(value: ShadowAdvanceAttemptRecord) -> ShadowAttemptView:
    return ShadowAttemptView(
        requested_session=value.requested_session,
        expected_session=value.expected_session,
        result=value.result,
        reason=value.reason_code,
        observed_at=value.observed_at,
        created_at=value.created_at,
    )
