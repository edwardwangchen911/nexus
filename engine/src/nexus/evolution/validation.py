"""Deterministic validation-only evaluation for admitted strategy candidates."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from nexus.backtest.canonical import canonical_decimal
from nexus.backtest.engine import (
    AuthorityCollection,
    BacktestBar,
    BacktestDataAuthority,
    BacktestProtocol,
    ExchangeCalendarAuthority,
    PointInTimeDataset,
    PriceSeriesAuthority,
    _add_months,
    _window_metrics,
    authority_collection_content_hash,
    exchange_calendar_content_hash,
    run_challenger_backtest_v1,
)
from nexus.strategy.dsl import CompiledStrategy


class CandidateValidationReportV1(BaseModel):
    """Frozen aggregate over validation windows only; OOS is never evaluated here."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: str = "candidate_validation_report_v1"
    validation_sharpe: Decimal
    validation_max_drawdown: Decimal
    validation_cost: Decimal = Field(ge=0)
    valid_window_count: int = Field(ge=0)
    authority_complete: bool
    validation_cutoff: date | None
    incomplete_reasons: tuple[str, ...] = ()

    @field_validator(
        "validation_sharpe", "validation_max_drawdown", "validation_cost", mode="after"
    )
    @classmethod
    def _canonical_metrics(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


def run_candidate_validation_v1(
    strategy: CompiledStrategy,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
) -> CandidateValidationReportV1:
    """Evaluate only frozen validation intervals with the deterministic interpreter."""
    boundaries = _validation_boundaries(protocol)
    if not boundaries:
        return CandidateValidationReportV1(
            validation_sharpe=Decimal(0),
            validation_max_drawdown=Decimal(0),
            validation_cost=Decimal(0),
            valid_window_count=0,
            authority_complete=False,
            validation_cutoff=None,
            incomplete_reasons=("VALIDATION_WINDOWS_MISSING",),
        )
    validation_cutoff = max(end for _, end in boundaries)
    eligible_sessions = tuple(
        session
        for session in data.authority.exchange_calendar.sessions
        if session < validation_cutoff
    )
    if not eligible_sessions:
        return CandidateValidationReportV1(
            validation_sharpe=Decimal(0),
            validation_max_drawdown=Decimal(0),
            validation_cost=Decimal(0),
            valid_window_count=0,
            authority_complete=False,
            validation_cutoff=validation_cutoff,
            incomplete_reasons=("VALIDATION_CALENDAR_MISSING",),
        )
    evaluation_end = eligible_sessions[-1]
    evaluation_data = _validation_slice(data, evaluation_end)
    evaluation_protocol = protocol.model_copy(update={"end": evaluation_end})
    result = run_challenger_backtest_v1(
        strategy,
        evaluation_data,
        evaluation_protocol,
        experiment_attempt_count=protocol.experiment_attempt_count,
    )
    bars_by_session = {bar.session for bar in evaluation_data.bars if bar.session <= evaluation_end}
    incomplete: list[str] = []
    metrics = []
    for start, end in boundaries:
        expected = tuple(session for session in eligible_sessions if start <= session < end)
        coverage = (
            Decimal(sum(session in bars_by_session for session in expected))
            / Decimal(len(expected))
            if expected
            else Decimal(0)
        )
        if len(expected) < 200 or coverage < Decimal("0.8"):
            incomplete.append(f"VALIDATION_WINDOW_{start.isoformat()}_INCOMPLETE")
            continue
        if sum(start <= point.session < end for point in result.nav) < 2:
            incomplete.append(f"VALIDATION_WINDOW_{start.isoformat()}_METRICS_MISSING")
            continue
        metrics.append(
            _window_metrics(
                start,
                end,
                nav_points=result.nav,
                fills=result.fills,
                initial_nav=protocol.initial_nav,
                experiment_attempt_count=protocol.experiment_attempt_count,
            )
        )
    valid_metrics = tuple(metrics)
    authority_complete = bool(valid_metrics) and not incomplete
    return CandidateValidationReportV1(
        validation_sharpe=(
            sum((item.sharpe for item in valid_metrics), start=Decimal(0))
            / Decimal(len(valid_metrics))
            if valid_metrics
            else Decimal(0)
        ),
        validation_max_drawdown=(
            min(item.max_drawdown for item in valid_metrics) if valid_metrics else Decimal(0)
        ),
        validation_cost=sum((item.total_cost for item in valid_metrics), start=Decimal(0)),
        valid_window_count=len(valid_metrics),
        authority_complete=authority_complete,
        validation_cutoff=validation_cutoff,
        incomplete_reasons=tuple(sorted(set(incomplete))),
    )


def _validation_boundaries(protocol: BacktestProtocol) -> tuple[tuple[date, date], ...]:
    oos_start = _add_months(protocol.start, (protocol.train_years + protocol.validation_years) * 12)
    result: list[tuple[date, date]] = []
    while _add_months(oos_start, protocol.out_of_sample_months) <= protocol.end:
        result.append((_add_months(oos_start, -protocol.validation_years * 12), oos_start))
        oos_start = _add_months(oos_start, protocol.step_months)
    return tuple(result)


def _validation_slice(data: PointInTimeDataset, end: date) -> PointInTimeDataset:
    """Build an ephemeral exact subset that physically excludes later sessions."""
    sessions = tuple(value for value in data.authority.exchange_calendar.sessions if value <= end)
    calendar = data.authority.exchange_calendar.model_copy(
        update={
            "range_end": sessions[-1],
            "sessions": sessions,
            "decision_times": {
                value: data.authority.exchange_calendar.decision_times[value] for value in sessions
            },
            "content_hash": exchange_calendar_content_hash(
                sessions,
                {
                    value: data.authority.exchange_calendar.decision_times[value]
                    for value in sessions
                },
            ),
        }
    )
    bars = tuple(item for item in data.bars if item.session <= end)
    adjusted_bars = tuple(item for item in data.adjusted_bars if item.session <= end)
    universes = tuple(
        item
        for item in data.universe_snapshots
        if item.effective_session <= end and item.published_at <= calendar.decision_times[end]
    )
    sectors = tuple(
        item
        for item in data.sector_classifications
        if item.effective_session <= end and item.published_at <= calendar.decision_times[end]
    )
    corporate_actions = tuple(item for item in data.corporate_actions if item.session <= end)
    delistings = tuple(item for item in data.delistings if item.session <= end)
    unresolved = tuple(item for item in data.unresolved_delistings if item.session <= end)
    price_series = tuple(
        _trim_price_authority(item, bars if item.adjustment == "raw" else adjusted_bars)
        for item in data.authority.price_series
    )
    selections = tuple(
        item.model_copy(update={"request_end_session": end})
        for item in data.authority.source_selections
    )
    universe_collection = _trim_collection(
        data.authority.universe_collection,
        end=end,
        member_ids=tuple(item.authority_id for item in universes),
    )
    sector_collection = _trim_collection(
        data.authority.sector_collection,
        end=end,
        member_ids=tuple(item.authority_id for item in sectors),
    )
    action_member_ids = (
        tuple(item.authority_id for item in corporate_actions)
        + tuple(item.authority_id for item in delistings)
        + tuple(item.authority_id for item in unresolved)
    )
    action_collection = _trim_collection(
        data.authority.corporate_action_collection,
        end=end,
        member_ids=action_member_ids,
    )
    authority = data.authority.model_copy(
        update={
            "exchange_calendar": ExchangeCalendarAuthority.model_validate(calendar),
            "universe_collection": universe_collection,
            "sector_collection": sector_collection,
            "corporate_action_collection": action_collection,
            "price_series": price_series,
            "source_selections": selections,
        }
    )
    return PointInTimeDataset(
        bars=bars,
        adjusted_bars=adjusted_bars,
        universe_snapshots=universes,
        asset_types=data.asset_types,
        risk_buckets=data.risk_buckets,
        sector_classifications=sectors,
        authority=BacktestDataAuthority.model_validate(authority),
        spy_instrument_id=data.spy_instrument_id,
        corporate_actions=corporate_actions,
        delistings=delistings,
        unresolved_delistings=unresolved,
    )


def _trim_collection(
    collection: AuthorityCollection,
    *,
    end: date,
    member_ids: tuple[UUID, ...],
) -> AuthorityCollection:
    return collection.model_copy(
        update={
            "range_end": end,
            "member_ids": member_ids,
            "content_hash": authority_collection_content_hash(
                kind=collection.kind,
                source=collection.source,
                range_start=collection.range_start,
                range_end=end,
                query_parameters=collection.query_parameters,
                member_ids=member_ids,
            ),
        }
    )


def _trim_price_authority(
    authority: PriceSeriesAuthority,
    bars: tuple[BacktestBar, ...],
) -> PriceSeriesAuthority:
    eligible = tuple(
        item for item in bars if getattr(item, "instrument_id", None) == authority.instrument_id
    )
    return authority.model_copy(
        update={
            "end_at": eligible[-1].available_at,
            "row_count": len(eligible),
        }
    )
