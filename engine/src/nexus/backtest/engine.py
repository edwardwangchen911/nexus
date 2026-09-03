"""Point-in-time, event-driven baseline backtesting with no model calls."""

from __future__ import annotations

import calendar
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.backtest.canonical import RESULT_QUANTUM, canonical_decimal
from nexus.backtest.costs import CostModel
from nexus.backtest.metrics import BacktestMetrics, calculate_metrics
from nexus.domain.time import require_utc
from nexus.simulation.engine import (
    apply_session_events as apply_simulation_events,
)
from nexus.simulation.engine import (
    execute_target_weights,
)
from nexus.simulation.engine import (
    mark_to_market as mark_simulation_to_market,
)
from nexus.simulation.models import (
    DelistingEvent as SimulationDelistingEvent,
)
from nexus.simulation.models import (
    DividendEvent as SimulationDividendEvent,
)
from nexus.simulation.models import (
    DividendReceivable,
    SimulationBar,
    SimulationInstrumentState,
    SimulationMark,
    SimulationState,
    TargetWeight,
    TargetWeightCommand,
)
from nexus.simulation.models import (
    SessionEvents as SimulationSessionEvents,
)
from nexus.simulation.models import (
    SplitEvent as SimulationSplitEvent,
)
from nexus.strategy.baseline import (
    BaselineInstrumentSnapshot,
    BaselineSectorExposure,
    PositionRankState,
    StrategyDataSnapshot,
)
from nexus.strategy.dsl import (
    CompiledStrategy,
    baseline_feature_registry,
    baseline_v1_document,
    compile_strategy,
    evolution_feature_registry_v1,
    verify_compiled_strategy,
)
from nexus.strategy.features import (
    BacktestBar as BacktestBar,
)
from nexus.strategy.features import (
    SectorClassification as SectorClassification,
)
from nexus.strategy.features import (
    align_feature_window,
    select_sector_exposures,
)
from nexus.strategy.features import (
    newer_bar as _newer_bar,
)
from nexus.strategy.features import (
    normalized_sector_weights as _normalized_sector_weights,
)
from nexus.strategy.features import (
    sector_exposures_complete as _sector_exposures_complete,
)
from nexus.strategy.interpreter import evaluate_compiled_strategy


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ExchangeCalendarAuthority(_FrozenModel):
    authority_id: UUID
    exchange: Literal["XNYS"] = "XNYS"
    version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
    source: str = Field(min_length=1, max_length=255)
    range_start: date
    range_end: date
    query_parameters: Mapping[str, object]
    completeness_status: Literal["complete"] = "complete"
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sessions: tuple[date, ...]
    decision_times: Mapping[date, datetime]

    @field_validator("decision_times")
    @classmethod
    def _canonical_decision_times(cls, value: Mapping[date, datetime]) -> dict[date, datetime]:
        return {session: require_utc(value[session]) for session in sorted(value)}

    @model_validator(mode="after")
    def _valid_sessions(self) -> ExchangeCalendarAuthority:
        if not self.sessions or tuple(sorted(self.sessions)) != self.sessions:
            raise ValueError("exchange sessions must be nonempty and ordered")
        if len(self.sessions) != len(set(self.sessions)):
            raise ValueError("exchange sessions must be unique")
        if any(session.weekday() >= 5 for session in self.sessions):
            raise ValueError("XNYS authority cannot contain weekend sessions")
        if set(self.decision_times) != set(self.sessions):
            raise ValueError("every exchange session requires one frozen decision time")
        for decision_at in self.decision_times.values():
            require_utc(decision_at)
        if (
            self.range_start > self.range_end
            or self.sessions[0] != self.range_start
            or self.sessions[-1] != self.range_end
        ):
            raise ValueError("exchange-calendar authority range must match its sessions")
        if self.content_hash != exchange_calendar_content_hash(self.sessions, self.decision_times):
            raise ValueError("exchange calendar content hash does not match its sessions")
        return self


def exchange_calendar_content_hash(
    sessions: tuple[date, ...], decision_times: Mapping[date, datetime]
) -> str:
    """Return the identity of sessions and their close-plus-30-minute cutoffs."""
    return hashlib.sha256(
        json.dumps(
            [
                {
                    "session": session.isoformat(),
                    "decision_at": require_utc(decision_times[session]).isoformat(),
                }
                for session in sessions
            ],
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


class PriceSeriesAuthority(_FrozenModel):
    price_bar_set_authority_id: UUID
    price_series_manifest_id: UUID
    instrument_id: UUID
    source: str = Field(min_length=1, max_length=100)
    feed: str = Field(min_length=1, max_length=100)
    delivery_mode: Literal["real_time", "delayed", "fixture"]
    adjustment: Literal["raw", "all"]
    interval: Literal["1Day"] = "1Day"
    manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    start_at: datetime
    end_at: datetime
    row_count: int = Field(ge=1, le=10_000)
    quality: Literal["PASS"] = "PASS"

    @field_validator("start_at", "end_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _ordered_range(self) -> PriceSeriesAuthority:
        if self.start_at > self.end_at:
            raise ValueError("price-series authority range must be ordered")
        return self


class PriceSourceSelectionAuthority(_FrozenModel):
    instrument_id: UUID
    adjustment: Literal["raw", "all"]
    request_start_session: date
    request_end_session: date
    decision_time: datetime
    tick_size: Decimal
    candidate_price_bar_set_authority_ids: tuple[UUID, ...]
    selected_price_bar_set_authority_id: UUID
    quality: Literal["PASS"] = "PASS"
    reasons: tuple[str, ...] = ()

    @field_validator("decision_time")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @field_validator("tick_size", mode="before")
    @classmethod
    def _exact_tick_size(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("source-selection tick size must be exact")
        return value

    @field_validator("tick_size", mode="after")
    @classmethod
    def _canonical_tick_size(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @model_validator(mode="after")
    def _complete_candidates(self) -> PriceSourceSelectionAuthority:
        if self.request_start_session > self.request_end_session:
            raise ValueError("source-selection request range must be ordered")
        if self.tick_size <= 0:
            raise ValueError("source-selection tick size must be positive")
        if (
            not self.candidate_price_bar_set_authority_ids
            or len(self.candidate_price_bar_set_authority_ids)
            != len(set(self.candidate_price_bar_set_authority_ids))
            or self.selected_price_bar_set_authority_id
            not in self.candidate_price_bar_set_authority_ids
        ):
            raise ValueError("source selection requires a unique complete candidate set")
        if self.reasons:
            raise ValueError("PASS source selection cannot carry quality reasons")
        return self


class UniverseSnapshot(_FrozenModel):
    authority_id: UUID
    key: Literal["us_liquid"] = "us_liquid"
    version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source: str = Field(min_length=1, max_length=255)
    effective_session: date
    published_at: datetime
    members: tuple[UUID, ...]
    eligibility_status: Literal["PASS"] = "PASS"

    @field_validator("published_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _unique_members(self) -> UniverseSnapshot:
        if len(self.members) != len(set(self.members)):
            raise ValueError("universe members must be unique")
        return self


class EqualWeightBenchmarkAuthority(_FrozenModel):
    universe_snapshot_id: UUID
    key: Literal["us_liquid"] = "us_liquid"
    universe_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source: str = Field(min_length=1, max_length=255)
    effective_session: date
    published_at: datetime
    instrument_ids: tuple[UUID, ...]

    @field_validator("published_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _members(self) -> EqualWeightBenchmarkAuthority:
        if not self.instrument_ids or len(self.instrument_ids) != len(set(self.instrument_ids)):
            raise ValueError("equal-weight benchmark members must be nonempty and unique")
        return self


class BenchmarkInstrumentAuthority(_FrozenModel):
    """Point-in-time ticker identity for the canonical SPY benchmark."""

    alias_id: UUID
    instrument_id: UUID
    symbol: Literal["SPY"] = "SPY"
    alias_type: Literal["ticker"] = "ticker"
    valid_from: date
    valid_to: date | None = None
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _valid_range(self) -> BenchmarkInstrumentAuthority:
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise ValueError("benchmark alias range must be ordered")
        return self


class AuthorityCollection(_FrozenModel):
    authority_id: UUID
    kind: Literal["universe", "sector", "corporate_action"]
    source: str = Field(min_length=1, max_length=255)
    range_start: date
    range_end: date
    query_parameters: Mapping[str, object]
    member_ids: tuple[UUID, ...]
    completeness_status: Literal["complete"] = "complete"
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _canonical_collection(self) -> AuthorityCollection:
        if self.range_start > self.range_end:
            raise ValueError("authority collection range must be ordered")
        if len(self.member_ids) != len(set(self.member_ids)):
            raise ValueError("authority collection members must be unique")
        expected = authority_collection_content_hash(
            kind=self.kind,
            source=self.source,
            range_start=self.range_start,
            range_end=self.range_end,
            query_parameters=self.query_parameters,
            member_ids=self.member_ids,
        )
        if self.content_hash != expected:
            raise ValueError("authority collection hash does not match its canonical members")
        return self


def authority_collection_content_hash(
    *,
    kind: str,
    source: str,
    range_start: date,
    range_end: date,
    query_parameters: Mapping[str, object],
    member_ids: tuple[UUID, ...],
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "kind": kind,
                "source": source,
                "range_start": range_start.isoformat(),
                "range_end": range_end.isoformat(),
                "query_parameters": query_parameters,
                "member_ids": sorted(str(value) for value in member_ids),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


class BacktestDataAuthority(_FrozenModel):
    authority_bundle_id: UUID
    data_snapshot_id: UUID
    source_policy_id: UUID
    cost_policy_id: UUID
    source_policy_version: Literal["source_policy_v1"] = "source_policy_v1"
    cost_policy_version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
    exchange_calendar: ExchangeCalendarAuthority
    universe_collection: AuthorityCollection
    sector_collection: AuthorityCollection
    corporate_action_collection: AuthorityCollection
    price_series: tuple[PriceSeriesAuthority, ...]
    source_selections: tuple[PriceSourceSelectionAuthority, ...]
    spy_benchmark: BenchmarkInstrumentAuthority
    equal_weight_benchmark: EqualWeightBenchmarkAuthority

    @model_validator(mode="after")
    def _complete_price_authorities(self) -> BacktestDataAuthority:
        identities = [
            (authority.instrument_id, authority.adjustment) for authority in self.price_series
        ]
        if not identities or len(identities) != len(set(identities)):
            raise ValueError("price authorities must be nonempty and unique by adjustment")
        expected = {
            (instrument_id, adjustment)
            for instrument_id, _ in identities
            for adjustment in ("raw", "all")
        }
        if set(identities) != expected:
            raise ValueError("each instrument requires raw execution and all-adjusted signal bars")
        bar_set_ids = {authority.price_bar_set_authority_id for authority in self.price_series}
        manifest_ids = {authority.price_series_manifest_id for authority in self.price_series}
        if len(bar_set_ids) != len(identities) or len(manifest_ids) != len(identities):
            raise ValueError("price authority and manifest IDs must be unique")
        selection_identities = [
            (item.instrument_id, item.adjustment) for item in self.source_selections
        ]
        if set(selection_identities) != set(identities) or len(selection_identities) != len(
            set(selection_identities)
        ):
            raise ValueError("source selections must exactly cover every price authority")
        selected_by_identity = {
            (item.instrument_id, item.adjustment): item.selected_price_bar_set_authority_id
            for item in self.source_selections
        }
        if any(
            selected_by_identity[(item.instrument_id, item.adjustment)]
            != item.price_bar_set_authority_id
            for item in self.price_series
        ):
            raise ValueError("source selection must bind each selected price authority")
        if (
            self.universe_collection.kind != "universe"
            or self.sector_collection.kind != "sector"
            or self.corporate_action_collection.kind != "corporate_action"
        ):
            raise ValueError("backtest collection authorities have invalid kinds")
        if any(
            item.request_start_session != self.exchange_calendar.range_start
            or item.request_end_session != self.exchange_calendar.range_end
            for item in self.source_selections
        ):
            raise ValueError("price request range must match the frozen exchange calendar")
        return self


class CorporateActionEvent(_FrozenModel):
    authority_id: UUID
    instrument_id: UUID
    source: str = Field(min_length=1, max_length=100)
    provider_event_id: str = Field(min_length=1, max_length=255)
    provider_action_type: Literal["cash_dividend", "stock_split", "reverse_split"]
    status: Literal["complete"]
    provider_ex_date: date
    provider_record_date: date | None = None
    provider_payable_date: date | None = None
    session: date
    payable_session: date | None = None
    action_type: Literal["split", "dividend"]
    value: Decimal
    available_at: datetime
    evidence_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    revision: str = "1"

    @field_validator("value", mode="before")
    @classmethod
    def _reject_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("corporate action values must not contain binary floats")
        return value

    @field_validator("value", mode="after")
    @classmethod
    def _canonical_value(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @field_validator("available_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _valid_action(self) -> CorporateActionEvent:
        if self.value <= 0:
            raise ValueError("corporate action value must be positive")
        if self.available_at.date() >= self.session:
            raise ValueError("corporate action must be available before its effective session")
        if self.action_type == "dividend" and self.provider_action_type != "cash_dividend":
            raise ValueError("dividend effects require cash-dividend provider evidence")
        if self.action_type == "split" and self.provider_action_type not in {
            "stock_split",
            "reverse_split",
        }:
            raise ValueError("split effects require split provider evidence")
        if self.action_type == "dividend":
            if self.provider_payable_date is None:
                raise ValueError("cash dividends require a provider payable date")
            if self.provider_payable_date < self.provider_ex_date:
                raise ValueError("cash-dividend provider dates must be ordered")
            if self.provider_record_date is not None and not (
                self.provider_ex_date <= self.provider_record_date <= self.provider_payable_date
            ):
                raise ValueError("cash-dividend provider dates must be ordered")
            if self.payable_session is not None and self.payable_session < self.session:
                raise ValueError("cash-dividend settlement session must be ordered")
        elif (
            self.provider_record_date is not None
            or self.provider_payable_date is not None
            or self.payable_session is not None
        ):
            raise ValueError("split events cannot carry dividend payment sessions")
        return self


class DelistingEvent(_FrozenModel):
    authority_id: UUID
    instrument_id: UUID
    source: str = Field(min_length=1, max_length=100)
    provider_event_id: str = Field(min_length=1, max_length=255)
    provider_action_type: Literal["bankruptcy", "merger_cash"]
    recovery_method: Literal["cash_recovery", "zero_recovery"]
    status: Literal["complete"]
    provider_process_date: date
    session: date
    recovery_price: Decimal
    available_at: datetime
    evidence_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    revision: str = "1"

    @field_validator("recovery_price", mode="before")
    @classmethod
    def _reject_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("delisting recovery must not contain binary floats")
        return value

    @field_validator("recovery_price", mode="after")
    @classmethod
    def _canonical_recovery_price(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @field_validator("available_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _valid_delisting(self) -> DelistingEvent:
        if self.recovery_price < 0:
            raise ValueError("delisting recovery must be nonnegative")
        if self.provider_action_type == "merger_cash" and (
            self.recovery_method != "cash_recovery" or self.recovery_price <= 0
        ):
            raise ValueError("cash mergers require an explicit positive cash recovery")
        if self.provider_action_type == "bankruptcy" and (
            self.recovery_method != "zero_recovery" or self.recovery_price != 0
        ):
            raise ValueError("bankruptcy requires explicit zero-recovery evidence")
        if self.available_at.date() >= self.session:
            raise ValueError("delisting must be available before its effective session")
        return self


class UnresolvedDelistingEvent(_FrozenModel):
    authority_id: UUID
    instrument_id: UUID
    source: str = Field(min_length=1, max_length=100)
    provider_event_id: str = Field(min_length=1, max_length=255)
    provider_action_type: Literal["delisting", "bankruptcy"]
    recovery_method: Literal["unresolved"] = "unresolved"
    status: Literal["complete"]
    provider_process_date: date
    session: date
    available_at: datetime
    evidence_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    revision: str = "1"

    @field_validator("available_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _valid_unresolved(self) -> UnresolvedDelistingEvent:
        if self.available_at.date() >= self.session:
            raise ValueError("delisting must be available before its effective session")
        return self


class PointInTimeDataset(_FrozenModel):
    bars: tuple[BacktestBar, ...]
    adjusted_bars: tuple[BacktestBar, ...]
    universe_snapshots: tuple[UniverseSnapshot, ...]
    asset_types: Mapping[UUID, Literal["stock", "equity_etf", "non_equity_etf"]]
    risk_buckets: Mapping[UUID, str]
    sector_classifications: tuple[SectorClassification, ...]
    authority: BacktestDataAuthority
    spy_instrument_id: UUID
    corporate_actions: tuple[CorporateActionEvent, ...] = ()
    delistings: tuple[DelistingEvent, ...] = ()
    unresolved_delistings: tuple[UnresolvedDelistingEvent, ...] = ()

    @model_validator(mode="after")
    def _complete_authority(self) -> PointInTimeDataset:
        universe_clocks = [
            (item.effective_session, item.published_at) for item in self.universe_snapshots
        ]
        raw_clocks = [
            (
                item.instrument_id,
                item.session,
                item.available_at,
                item.received_at,
                item.revision,
            )
            for item in self.bars
        ]
        adjusted_clocks = [
            (
                item.instrument_id,
                item.session,
                item.available_at,
                item.received_at,
                item.revision,
            )
            for item in self.adjusted_bars
        ]
        if (
            len(universe_clocks) != len(set(universe_clocks))
            or len(raw_clocks) != len(set(raw_clocks))
            or len(adjusted_clocks) != len(set(adjusted_clocks))
        ):
            raise ValueError("point-in-time selection clocks must be unambiguous")
        universe_authority_ids = [item.authority_id for item in self.universe_snapshots]
        sector_authority_ids = [item.authority_id for item in self.sector_classifications]
        events: tuple[CorporateActionEvent | DelistingEvent | UnresolvedDelistingEvent, ...] = (
            *self.corporate_actions,
            *self.delistings,
            *self.unresolved_delistings,
        )
        all_delistings: tuple[DelistingEvent | UnresolvedDelistingEvent, ...] = (
            *self.delistings,
            *self.unresolved_delistings,
        )
        event_authority_ids = [item.authority_id for item in events]
        provider_event_ids = [
            (item.source, item.provider_event_id, item.revision) for item in events
        ]
        delisted_instruments = [item.instrument_id for item in all_delistings]
        if (
            len(universe_authority_ids) != len(set(universe_authority_ids))
            or len(sector_authority_ids) != len(set(sector_authority_ids))
            or len(event_authority_ids) != len(set(event_authority_ids))
            or len(provider_event_ids) != len(set(provider_event_ids))
            or len(delisted_instruments) != len(set(delisted_instruments))
        ):
            raise ValueError("point-in-time authorities and corporate events must be unique")
        delisting_session = {item.instrument_id: item.session for item in all_delistings}
        if any(
            item.instrument_id in delisting_session
            and item.session >= delisting_session[item.instrument_id]
            for item in self.corporate_actions
        ):
            raise ValueError("corporate actions cannot occur on or after a delisting")
        raw_bar_ids = {bar.instrument_id for bar in self.bars}
        adjusted_bar_ids = {bar.instrument_id for bar in self.adjusted_bars}
        universe_ids = {
            instrument_id
            for snapshot in self.universe_snapshots
            for instrument_id in snapshot.members
        }
        benchmark_ids = set(self.authority.equal_weight_benchmark.instrument_ids) | {
            self.spy_instrument_id
        }
        required_ids = raw_bar_ids | adjusted_bar_ids | universe_ids | benchmark_ids
        if required_ids != set(self.asset_types) or required_ids != set(self.risk_buckets):
            raise ValueError("asset metadata must be complete for every backtest instrument")
        price_ids = {authority.instrument_id for authority in self.authority.price_series}
        if required_ids != price_ids or raw_bar_ids != adjusted_bar_ids:
            raise ValueError(
                "raw and all-adjusted price authorities must exactly cover every instrument"
            )
        for instrument_id in required_ids:
            pair = tuple(
                item for item in self.authority.price_series if item.instrument_id == instrument_id
            )
            if {item.adjustment for item in pair} != {"raw", "all"} or len(pair) != 2:
                raise ValueError("each instrument requires exactly one raw/all price pair")
            raw = next(item for item in pair if item.adjustment == "raw")
            adjusted = next(item for item in pair if item.adjustment == "all")
            paired_fields = (
                "source",
                "feed",
                "delivery_mode",
                "interval",
                "start_at",
                "end_at",
                "row_count",
            )
            if any(getattr(raw, field) != getattr(adjusted, field) for field in paired_fields):
                raise ValueError("raw/all price authorities must be one coherent source pair")
        sector_ids = {
            classification.instrument_id for classification in self.sector_classifications
        }
        equity_sector_ids = {
            instrument_id
            for instrument_id in universe_ids
            if self.asset_types[instrument_id] != "non_equity_etf"
        }
        if not equity_sector_ids <= sector_ids:
            raise ValueError("point-in-time sector authority is required for universe members")
        non_equity_ids = {
            instrument_id
            for instrument_id in universe_ids
            if self.asset_types[instrument_id] == "non_equity_etf"
        }
        if non_equity_ids & sector_ids:
            raise ValueError("non-equity ETFs must not carry equity-sector authority")
        if any(self.risk_buckets[item] not in {"US_TREASURY", "GOLD"} for item in non_equity_ids):
            raise ValueError("non-equity ETF risk bucket is not supported by baseline_v1")
        for instrument_id in universe_ids:
            if self.asset_types[instrument_id] == "equity_etf" and not any(
                item.instrument_id == instrument_id
                and (
                    item.reviewed
                    or (self.authority.sector_collection.source == "fixture" and item.synthetic)
                )
                for item in self.sector_classifications
            ):
                raise ValueError("ETF sector exposure must be reviewed")
        if any(item.synthetic for item in self.sector_classifications) and (
            self.authority.sector_collection.source != "fixture"
        ):
            raise ValueError("synthetic sector exposure is allowed only in fixture authority")
        if self.authority.sector_collection.source == "fixture":
            if any(not item.synthetic or item.reviewed for item in self.sector_classifications):
                raise ValueError("fixture sector authority must be synthetic and unreviewed")
        else:
            if self.authority.sector_collection.source != "controlled_csv_v1":
                raise ValueError("sector authority source is not admitted")
            if any(item.synthetic or not item.reviewed for item in self.sector_classifications):
                raise ValueError("production sector authority must be reviewed and nonsynthetic")
        if self.authority.corporate_action_collection.source not in {"fixture", "alpaca"}:
            raise ValueError("corporate-action authority source is not admitted")
        if any(item.source != self.authority.corporate_action_collection.source for item in events):
            raise ValueError("corporate-action collection members must share its source")
        benchmark = self.authority.equal_weight_benchmark
        spy_benchmark = self.authority.spy_benchmark
        if (
            spy_benchmark.instrument_id != self.spy_instrument_id
            or spy_benchmark.valid_from > self.authority.exchange_calendar.range_start
            or (
                spy_benchmark.valid_to is not None
                and spy_benchmark.valid_to < self.authority.exchange_calendar.range_end
            )
        ):
            raise ValueError("SPY benchmark alias must cover the frozen calendar range")
        bound_snapshot = next(
            (
                snapshot
                for snapshot in self.universe_snapshots
                if snapshot.authority_id == benchmark.universe_snapshot_id
            ),
            None,
        )
        if bound_snapshot is not None and (
            bound_snapshot.key != benchmark.key
            or bound_snapshot.version != benchmark.universe_version
            or bound_snapshot.content_hash != benchmark.content_hash
            or bound_snapshot.source != benchmark.source
            or bound_snapshot.effective_session != benchmark.effective_session
            or bound_snapshot.published_at != benchmark.published_at
            or set(bound_snapshot.members) != set(benchmark.instrument_ids)
        ):
            raise ValueError("equal-weight benchmark authority conflicts with its universe")
        expected_collection_members = {
            "universe": {item.authority_id for item in self.universe_snapshots}
            | {benchmark.universe_snapshot_id},
            "sector": {item.authority_id for item in self.sector_classifications},
            "corporate_action": {item.authority_id for item in events},
        }
        actual_collections = {
            "universe": set(self.authority.universe_collection.member_ids),
            "sector": set(self.authority.sector_collection.member_ids),
            "corporate_action": set(self.authority.corporate_action_collection.member_ids),
        }
        if actual_collections != expected_collection_members:
            raise ValueError("authority collections must exactly enumerate the dataset")
        return self


class BacktestProtocol(_FrozenModel):
    start: date
    end: date
    initial_nav: Decimal
    cost_model: CostModel
    train_years: Literal[3] = 3
    validation_years: Literal[1] = 1
    out_of_sample_months: Literal[6] = 6
    step_months: Literal[3] = 3
    walk_forward: bool = True
    experiment_attempt_count: int = Field(default=1, ge=1, le=1_000_000)
    source_policy_version: Literal["source_policy_v1"] = "source_policy_v1"
    code_version: str = Field(
        default="nexus_backtest_engine_v1", pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$"
    )
    random_seed: Literal[0] = 0

    @field_validator("initial_nav", mode="before")
    @classmethod
    def _reject_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("NAV must not contain binary floats")
        return value

    @field_validator("initial_nav", mode="after")
    @classmethod
    def _canonical_initial_nav(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)

    @model_validator(mode="after")
    def _ordered(self) -> BacktestProtocol:
        if self.start > self.end or self.initial_nav <= 0:
            raise ValueError("backtest protocol range and NAV must be valid")
        return self


class BacktestFill(_FrozenModel):
    instrument_id: UUID
    signal_session: date
    fill_session: date
    side: Literal["buy", "sell"]
    quantity: Decimal
    price: Decimal
    notional: Decimal
    cost: Decimal

    @field_validator("quantity", "price", "notional", "cost", mode="after")
    @classmethod
    def _canonical(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class BacktestOrderOutcome(_FrozenModel):
    instrument_id: UUID
    signal_session: date
    target_session: date
    side: Literal["buy", "sell", "rebalance"]
    status: Literal["filled", "missing_bar", "untradable"]
    reason: str


class NavPoint(_FrozenModel):
    session: date
    nav: Decimal
    cash: Decimal

    @field_validator("nav", "cash", mode="after")
    @classmethod
    def _canonical(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class WalkForwardWindow(_FrozenModel):
    ordinal: int
    train_start: date
    train_end: date
    validation_start: date
    validation_end: date
    out_of_sample_start: date
    out_of_sample_end: date
    session_coverage: Decimal
    valid: bool
    invalid_reasons: tuple[str, ...] = ()
    metrics: BacktestMetrics | None = None

    @field_validator("session_coverage", mode="after")
    @classmethod
    def _canonical(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class CostSensitivityResult(_FrozenModel):
    half_cost_terminal_nav: Decimal
    base_terminal_nav: Decimal
    double_cost_terminal_nav: Decimal

    @field_validator("*", mode="after")
    @classmethod
    def _canonical(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class ParameterSensitivityResult(_FrozenModel):
    adv20_40m_terminal_nav: Decimal
    base_terminal_nav: Decimal
    adv20_60m_terminal_nav: Decimal

    @field_validator("*", mode="after")
    @classmethod
    def _canonical(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class ExecutionSensitivityResult(_FrozenModel):
    next_open_terminal_nav: Decimal
    next_close_terminal_nav: Decimal

    @field_validator("*", mode="after")
    @classmethod
    def _canonical(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class AttributionContribution(_FrozenModel):
    key: str
    value: Decimal

    @field_validator("value", mode="after")
    @classmethod
    def _canonical(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class PerformanceAttribution(_FrozenModel):
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    costs: Decimal
    cash_contribution: Decimal
    reconciliation_difference: Decimal
    by_instrument: tuple[AttributionContribution, ...]
    by_sector: tuple[AttributionContribution, ...]
    reconciled: bool

    @field_validator(
        "realized_pnl",
        "unrealized_pnl",
        "costs",
        "cash_contribution",
        "reconciliation_difference",
        mode="after",
    )
    @classmethod
    def _canonical(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class BenchmarkComparison(_FrozenModel):
    comparison_key: Literal["spy", "equal_weight", "champion"]
    terminal_nav: Decimal
    metrics: BacktestMetrics
    excess_total_return: Decimal
    complete: bool
    incomplete_reasons: tuple[str, ...] = ()

    @field_validator("terminal_nav", "excess_total_return", mode="after")
    @classmethod
    def _canonical(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class BacktestResult(_FrozenModel):
    strategy_version: str
    expression_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    authority_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    authority_bundle_id: UUID
    data_snapshot_id: UUID
    source_policy_id: UUID
    cost_policy_id: UUID
    cost_policy_version: str
    data_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    code_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    protocol_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    nav: tuple[NavPoint, ...]
    fills: tuple[BacktestFill, ...]
    order_outcomes: tuple[BacktestOrderOutcome, ...]
    windows: tuple[WalkForwardWindow, ...]
    metrics: BacktestMetrics
    attribution: PerformanceAttribution
    comparisons: tuple[BenchmarkComparison, ...]
    strategy_data_complete: bool
    strategy_incomplete_reasons: tuple[str, ...]
    benchmark_comparison_complete: bool
    cost_sensitivity: CostSensitivityResult
    parameter_sensitivity: ParameterSensitivityResult
    execution_sensitivity: ExecutionSensitivityResult
    valid_oos_window_count: int
    history_requirement_met: bool
    oos_window_count_sufficient: bool
    promotable: Literal[False] = False
    historical_agent_overlay: Literal[False] = False
    source_policy_version: Literal["source_policy_v1"]
    random_seed: Literal[0] = 0
    delisting_count: int


@dataclass
class _Ledger:
    cash: Decimal
    holdings: dict[UUID, Decimal] = field(default_factory=dict)
    cost_basis: dict[UUID, Decimal] = field(default_factory=dict)
    realized: dict[UUID, Decimal] = field(default_factory=dict)
    costs: dict[UUID, Decimal] = field(default_factory=dict)
    dividend_receivables: dict[UUID, tuple[UUID, Decimal, date]] = field(default_factory=dict)
    untradable: set[UUID] = field(default_factory=set)


@dataclass(frozen=True)
class _SimulationOutcome:
    nav: tuple[NavPoint, ...]
    fills: tuple[BacktestFill, ...]
    order_outcomes: tuple[BacktestOrderOutcome, ...]
    metrics: BacktestMetrics
    attribution: PerformanceAttribution
    data_complete: bool
    incomplete_reasons: tuple[str, ...]
    delisting_count: int


@dataclass(frozen=True)
class _PassiveBenchmarkOutcome:
    terminal_nav: Decimal
    metrics: BacktestMetrics
    complete: bool
    incomplete_reasons: tuple[str, ...]


@dataclass
class _PreparedDataset:
    raw_by_session: dict[date, dict[UUID, BacktestBar]]
    adjusted_by_session: dict[date, dict[UUID, BacktestBar]]
    raw_by_instrument: dict[UUID, tuple[BacktestBar, ...]]
    adjusted_by_instrument: dict[UUID, tuple[BacktestBar, ...]]
    feature_windows: dict[
        tuple[date, UUID], tuple[tuple[BacktestBar, ...], tuple[BacktestBar, ...]]
    ] = field(default_factory=dict)
    strategy_instruments: dict[date, tuple[BaselineInstrumentSnapshot, ...]] = field(
        default_factory=dict
    )


def run_backtest(
    strategy: CompiledStrategy,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
) -> BacktestResult:
    """Run the verified historical baseline over frozen, point-in-time authorities."""
    _verify_baseline_strategy(strategy)
    return simulation_core_v1(strategy, data, protocol)


def run_challenger_backtest_v1(
    strategy: CompiledStrategy,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
    *,
    experiment_attempt_count: int,
) -> BacktestResult:
    """Run one verified Phase 2 challenger without changing historical promotion flags."""
    if not strategy.strategy_version.startswith("challenger_"):
        raise ValueError("challenger strategy version is required")
    verify_compiled_strategy(strategy, evolution_feature_registry_v1())
    if experiment_attempt_count < 1:
        raise ValueError("experiment attempt count must be positive")
    return simulation_core_v1(
        strategy,
        data,
        protocol.model_copy(update={"experiment_attempt_count": experiment_attempt_count}),
    )


def simulation_core_v1(
    strategy: CompiledStrategy,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
) -> BacktestResult:
    """Execute a verified restricted strategy over one frozen point-in-time dataset."""
    if strategy.strategy_version == "baseline_v1":
        _verify_baseline_strategy(strategy)
    else:
        if not strategy.strategy_version.startswith("challenger_"):
            raise ValueError("challenger strategy version is required")
        verify_compiled_strategy(strategy, evolution_feature_registry_v1())
    _validate_authority_binding(data, protocol)
    prepared = _prepare_dataset(data)
    base = _run_simulation(strategy, data, protocol, prepared=prepared)
    half = _run_simulation(
        strategy,
        data,
        protocol.model_copy(update={"cost_model": protocol.cost_model.scaled(Decimal("0.5"))}),
        prepared=prepared,
    )
    double = _run_simulation(
        strategy,
        data,
        protocol.model_copy(update={"cost_model": protocol.cost_model.scaled(Decimal("2"))}),
        prepared=prepared,
    )
    adv_low = _run_simulation(
        strategy, data, protocol, prepared=prepared, min_adv20_usd=Decimal("40000000")
    )
    adv_high = _run_simulation(
        strategy, data, protocol, prepared=prepared, min_adv20_usd=Decimal("60000000")
    )
    close_fill = _run_simulation(
        strategy, data, protocol, prepared=prepared, execution_price="close"
    )
    base_terminal_nav = _terminal_nav(base, protocol.initial_nav)
    comparisons = _benchmark_comparisons(
        data=data,
        prepared=prepared,
        protocol=protocol,
        strategy_metrics=base.metrics,
        strategy_terminal_nav=base_terminal_nav,
    )
    comparison_keys = {comparison.comparison_key for comparison in comparisons}
    benchmark_complete = comparison_keys == {"spy", "equal_weight", "champion"} and all(
        comparison.complete for comparison in comparisons
    )
    windows = _walk_forward_windows(
        protocol,
        calendar_sessions=data.authority.exchange_calendar.sessions,
        bars_by_session=prepared.raw_by_session,
        nav_points=base.nav,
        fills=base.fills,
    )
    valid_oos_window_count = sum(window.valid for window in windows)
    history_requirement_met = _history_requirement_met(
        protocol, data.authority.exchange_calendar.sessions
    )
    authority_hash = _authority_bundle_hash(data)
    protocol_hash = _hash(protocol.model_dump(mode="json"))
    provisional = BacktestResult(
        strategy_version=strategy.strategy_version,
        expression_hash=strategy.expression_hash,
        authority_hash=authority_hash,
        authority_bundle_id=data.authority.authority_bundle_id,
        data_snapshot_id=data.authority.data_snapshot_id,
        source_policy_id=data.authority.source_policy_id,
        cost_policy_id=data.authority.cost_policy_id,
        cost_policy_version=data.authority.cost_policy_version,
        data_hash=_audited_data_hash(data, protocol),
        code_hash=_engine_code_hash(protocol.code_version),
        protocol_hash=protocol_hash,
        result_hash="0" * 64,
        nav=base.nav,
        fills=base.fills,
        order_outcomes=base.order_outcomes,
        windows=windows,
        metrics=base.metrics,
        attribution=base.attribution,
        comparisons=comparisons,
        strategy_data_complete=base.data_complete,
        strategy_incomplete_reasons=base.incomplete_reasons,
        benchmark_comparison_complete=benchmark_complete,
        cost_sensitivity=CostSensitivityResult(
            half_cost_terminal_nav=_terminal_nav(half, protocol.initial_nav),
            base_terminal_nav=base_terminal_nav,
            double_cost_terminal_nav=_terminal_nav(double, protocol.initial_nav),
        ),
        parameter_sensitivity=ParameterSensitivityResult(
            adv20_40m_terminal_nav=_terminal_nav(adv_low, protocol.initial_nav),
            base_terminal_nav=base_terminal_nav,
            adv20_60m_terminal_nav=_terminal_nav(adv_high, protocol.initial_nav),
        ),
        execution_sensitivity=ExecutionSensitivityResult(
            next_open_terminal_nav=base_terminal_nav,
            next_close_terminal_nav=_terminal_nav(close_fill, protocol.initial_nav),
        ),
        valid_oos_window_count=valid_oos_window_count,
        history_requirement_met=history_requirement_met,
        oos_window_count_sufficient=history_requirement_met and valid_oos_window_count >= 8,
        source_policy_version=protocol.source_policy_version,
        delisting_count=base.delisting_count,
    )
    return provisional.model_copy(
        update={"result_hash": _hash(provisional.model_dump(mode="json", exclude={"result_hash"}))}
    )


def _verify_baseline_strategy(strategy: CompiledStrategy) -> None:
    registry = baseline_feature_registry()
    verify_compiled_strategy(strategy, registry)
    expected = compile_strategy(baseline_v1_document(), registry)
    if strategy != expected:
        raise ValueError("baseline_v1 policy does not match the published declarative strategy")


def _validate_authority_binding(data: PointInTimeDataset, protocol: BacktestProtocol) -> None:
    if data.authority.source_policy_version != protocol.source_policy_version:
        raise ValueError("dataset source-policy authority does not match the protocol")
    calendar_sessions = set(data.authority.exchange_calendar.sessions)
    if any(snapshot.key != "us_liquid" for snapshot in data.universe_snapshots):
        raise ValueError("baseline_v1 requires Task 7 point-in-time us_liquid snapshots")
    selected = _selected_bars_by_session(data.bars, data.authority.exchange_calendar.decision_times)
    selected_adjusted = _selected_bars_by_session(
        data.adjusted_bars, data.authority.exchange_calendar.decision_times
    )
    if any(session not in calendar_sessions for session in (*selected, *selected_adjusted)):
        raise ValueError("an available price bar is outside the frozen exchange calendar")
    benchmark = data.authority.equal_weight_benchmark
    start_cutoff = _decision_time(data, protocol.start)
    spy_benchmark = data.authority.spy_benchmark
    if (
        spy_benchmark.instrument_id != data.spy_instrument_id
        or spy_benchmark.valid_from > protocol.start
        or (spy_benchmark.valid_to is not None and spy_benchmark.valid_to < protocol.end)
        or spy_benchmark.created_at > start_cutoff
    ):
        raise ValueError("SPY benchmark identity is unavailable for the protocol range")
    if benchmark.effective_session > protocol.start or benchmark.published_at > start_cutoff:
        raise ValueError("equal-weight benchmark universe must be available at protocol start")
    start_universe = max(
        (
            snapshot
            for snapshot in data.universe_snapshots
            if snapshot.effective_session <= protocol.start
            and snapshot.published_at <= start_cutoff
        ),
        key=lambda item: (item.effective_session, item.published_at, item.version),
        default=None,
    )
    if start_universe is None or (
        start_universe.authority_id != benchmark.universe_snapshot_id
        or start_universe.version != benchmark.universe_version
        or start_universe.content_hash != benchmark.content_hash
        or start_universe.source != benchmark.source
        or set(start_universe.members) != set(benchmark.instrument_ids)
    ):
        raise ValueError("equal-weight benchmark must match the strategy start universe")
    all_delistings: tuple[DelistingEvent | UnresolvedDelistingEvent, ...] = (
        *data.delistings,
        *data.unresolved_delistings,
    )
    event_sessions = (
        {event.session for event in all_delistings}
        | {action.session for action in data.corporate_actions}
        | {
            action.payable_session
            for action in data.corporate_actions
            if action.payable_session is not None
        }
    )
    if not event_sessions <= calendar_sessions:
        raise ValueError("corporate-action sessions must belong to the frozen calendar")
    for action in data.corporate_actions:
        if action.session != _mapped_session_on_or_after(
            data.authority.exchange_calendar.sessions, action.provider_ex_date
        ):
            raise ValueError("corporate-action ex date is not mapped by the frozen calendar")
        expected_payable_session = (
            None
            if action.provider_payable_date is None
            else _mapped_session_on_or_after(
                tuple(
                    session
                    for session in data.authority.exchange_calendar.sessions
                    if session <= protocol.end
                ),
                action.provider_payable_date,
            )
        )
        if action.payable_session != expected_payable_session:
            raise ValueError("cash-dividend payable date is not mapped by the frozen calendar")
    for event in all_delistings:
        if event.session != _mapped_session_on_or_after(
            data.authority.exchange_calendar.sessions,
            event.provider_process_date,
        ):
            raise ValueError("delisting process date is not mapped by the frozen calendar")
    cutoff = _decision_time(data, protocol.end)
    unavailable_action = any(
        action.session <= protocol.end and action.available_at > cutoff
        for action in data.corporate_actions
    ) or any(
        event.session <= protocol.end and event.available_at > cutoff for event in all_delistings
    )
    if unavailable_action:
        raise ValueError("corporate-action authority is unavailable at the protocol cutoff")


def _run_simulation(
    strategy: CompiledStrategy,
    data: PointInTimeDataset,
    protocol: BacktestProtocol,
    *,
    prepared: _PreparedDataset,
    min_adv20_usd: Decimal = Decimal("50000000"),
    execution_price: Literal["open", "close"] = "open",
) -> _SimulationOutcome:
    sessions = [
        session
        for session in data.authority.exchange_calendar.sessions
        if protocol.start <= session <= protocol.end
    ]
    bars_by_session = prepared.raw_by_session
    adjusted_bars_by_session = prepared.adjusted_by_session
    action_by_session = _group_actions(data.corporate_actions)
    delisting_by_session = _group_delistings(data.delistings)
    unresolved_delisting_by_session = _group_unresolved_delistings(data.unresolved_delistings)
    ledger = _Ledger(cash=protocol.initial_nav)
    pending: tuple[date, Mapping[UUID, Decimal]] | None = None
    fills: list[BacktestFill] = []
    order_outcomes: list[BacktestOrderOutcome] = []
    incomplete_reasons: set[str] = set()
    nav_points: list[NavPoint] = []
    position_rank_streaks: dict[UUID, int] = {}
    last_closes: dict[UUID, Decimal] = {}
    turnover_notional = Decimal(0)
    delisting_count = 0
    prior_cumulative_pnl: dict[UUID, Decimal] = {}
    sector_contributions: dict[str, Decimal] = {}

    for session in sessions:
        session_outcomes: list[BacktestOrderOutcome] = []
        session_bars = bars_by_session.get(session, {})
        adjusted_session_bars = adjusted_bars_by_session.get(session, {})
        delisting_count += _apply_session_events_to_ledger(
            ledger,
            session=session,
            actions=action_by_session.get(session, ()),
            delistings=delisting_by_session.get(session, ()),
            last_closes=last_closes,
        )
        for event in unresolved_delisting_by_session.get(session, ()):
            ledger.untradable.add(event.instrument_id)
            last_closes.pop(event.instrument_id, None)
            incomplete_reasons.add(f"{session}:{event.instrument_id}:delisting_recovery_unresolved")
        if pending is not None:
            signal_session, target_weights = pending
            new_fills, outcomes, traded = _rebalance(
                session=session,
                signal_session=signal_session,
                target_weights=target_weights,
                bars=session_bars,
                ledger=ledger,
                cost_model=protocol.cost_model,
                fallback_prices=last_closes,
                execution_price=execution_price,
            )
            fills.extend(new_fills)
            order_outcomes.extend(outcomes)
            session_outcomes.extend(outcomes)
            turnover_notional += traded
            pending = None

        incomplete_reasons.update(
            _strategy_session_incomplete_reasons(
                data=data,
                prepared=prepared,
                session=session,
                raw_bars=session_bars,
                adjusted_bars=adjusted_session_bars,
                untradable=ledger.untradable,
                holdings=set(ledger.holdings),
                pending_instruments=set() if pending is None else set(pending[1]),
            )
        )
        for outcome in session_outcomes:
            if outcome.status == "missing_bar":
                incomplete_reasons.add(f"{session}:{outcome.instrument_id}:order_{outcome.status}")

        last_closes.update(
            {
                instrument_id: bar.close
                for instrument_id, bar in session_bars.items()
                if instrument_id not in ledger.untradable
            }
        )
        cumulative_pnl = _cumulative_pnl_by_instrument(
            ledger,
            {instrument_id: bar.close for instrument_id, bar in session_bars.items()},
            prior_cumulative_pnl,
        )
        for instrument_id, cumulative_value in cumulative_pnl.items():
            contribution = cumulative_value - prior_cumulative_pnl.get(instrument_id, Decimal(0))
            exposures = _sector_exposures_for(data, instrument_id, session)
            if not exposures:
                sector_contributions["unclassified"] = (
                    sector_contributions.get("unclassified", Decimal(0)) + contribution
                )
                continue
            for sector, weight in _normalized_sector_weights(exposures):
                sector_contributions[sector] = (
                    sector_contributions.get(sector, Decimal(0)) + contribution * weight
                )
        prior_cumulative_pnl = cumulative_pnl
        nav_points.append(
            NavPoint(
                session=session,
                nav=_mark_to_market(
                    ledger.cash,
                    ledger.holdings,
                    {instrument_id: bar.close for instrument_id, bar in session_bars.items()},
                    receivables=ledger.dividend_receivables,
                ),
                cash=ledger.cash,
            )
        )
        next_session = _next_session(data.authority.exchange_calendar.sessions, session)
        if not _is_strategy_decision_session(session, next_session):
            continue
        snapshot = _strategy_snapshot(
            data=data,
            prepared=prepared,
            decision_session=session,
            next_session=next_session,
            current_positions=tuple(ledger.holdings),
            position_rank_streaks=position_rank_streaks,
        )
        decision = evaluate_compiled_strategy(
            strategy,
            snapshot,
            session,
            _parameter_overrides={"min_adv20_usd": min_adv20_usd},
        )
        if decision.reason == "WEEKLY_REBALANCE" and next_session is not None:
            position_rank_streaks = {
                state.instrument_id: state.consecutive_outside_top20_weeks
                for state in decision.position_rank_states
            }
            pending = (
                session,
                {target.instrument_id: target.weight for target in decision.targets},
            )

    total_cost = sum((fill.cost for fill in fills), start=Decimal(0))
    metrics = calculate_metrics(
        tuple(point.nav for point in nav_points),
        turnover_notional=turnover_notional,
        initial_nav=protocol.initial_nav,
        total_cost=total_cost,
        experiment_attempt_count=protocol.experiment_attempt_count,
    )
    terminal_nav = nav_points[-1].nav if nav_points else canonical_decimal(protocol.initial_nav)
    attribution = _performance_attribution(
        ledger=ledger,
        terminal_nav=terminal_nav,
        initial_nav=protocol.initial_nav,
        last_closes=last_closes,
        data=data,
        end_session=protocol.end,
        sector_contributions=sector_contributions,
    )
    return _SimulationOutcome(
        nav=tuple(nav_points),
        fills=tuple(fills),
        order_outcomes=tuple(order_outcomes),
        metrics=metrics,
        attribution=attribution,
        data_complete=not incomplete_reasons,
        incomplete_reasons=tuple(sorted(incomplete_reasons)),
        delisting_count=delisting_count,
    )


def _selected_bars_by_session(
    bars: tuple[BacktestBar, ...],
    decision_times: Mapping[date, datetime],
) -> dict[date, dict[UUID, BacktestBar]]:
    selected: dict[date, dict[UUID, BacktestBar]] = {}
    for bar in sorted(
        bars,
        key=lambda item: (
            item.session,
            item.available_at,
            item.received_at,
            item.revision,
            _canonical_json(item.model_dump(mode="json")),
        ),
    ):
        cutoff = decision_times.get(bar.session)
        if cutoff is None:
            continue
        if bar.available_at > cutoff:
            continue
        selected.setdefault(bar.session, {})[bar.instrument_id] = bar
    return selected


def _decision_time(data: PointInTimeDataset, session: date) -> datetime:
    try:
        return data.authority.exchange_calendar.decision_times[session]
    except KeyError as error:
        raise ValueError("session has no frozen exchange decision time") from error


def _prepare_dataset(data: PointInTimeDataset) -> _PreparedDataset:
    def by_instrument(bars: tuple[BacktestBar, ...]) -> dict[UUID, tuple[BacktestBar, ...]]:
        grouped: dict[UUID, list[BacktestBar]] = {}
        for bar in bars:
            grouped.setdefault(bar.instrument_id, []).append(bar)
        return {
            instrument_id: tuple(
                sorted(
                    values,
                    key=lambda item: (
                        item.session,
                        item.available_at,
                        item.received_at,
                        item.revision,
                    ),
                )
            )
            for instrument_id, values in grouped.items()
        }

    prepared = _PreparedDataset(
        raw_by_session=_selected_bars_by_session(
            data.bars, data.authority.exchange_calendar.decision_times
        ),
        adjusted_by_session=_selected_bars_by_session(
            data.adjusted_bars, data.authority.exchange_calendar.decision_times
        ),
        raw_by_instrument=by_instrument(data.bars),
        adjusted_by_instrument=by_instrument(data.adjusted_bars),
    )
    _prepare_feature_windows(data, prepared)
    return prepared


def _prepare_feature_windows(data: PointInTimeDataset, prepared: _PreparedDataset) -> None:
    sessions = data.authority.exchange_calendar.sessions
    decisions = tuple(
        session
        for index, session in enumerate(sessions)
        if _is_strategy_decision_session(
            session, sessions[index + 1] if index + 1 < len(sessions) else None
        )
    )
    decision_times = data.authority.exchange_calendar.decision_times

    def activated(
        values: tuple[BacktestBar, ...],
    ) -> list[tuple[datetime, BacktestBar]]:
        return sorted(
            (
                (max(bar.available_at, decision_times[bar.session]), bar)
                for bar in values
                if bar.session in decision_times
            ),
            key=lambda item: (
                item[0],
                item[1].session,
                item[1].received_at,
                item[1].revision,
                _canonical_json(item[1].model_dump(mode="json")),
            ),
        )

    for instrument_id in sorted(data.asset_types, key=lambda value: value.int):
        raw_events = activated(prepared.raw_by_instrument.get(instrument_id, ()))
        adjusted_events = activated(prepared.adjusted_by_instrument.get(instrument_id, ()))
        raw_state: dict[date, BacktestBar] = {}
        adjusted_state: dict[date, BacktestBar] = {}
        raw_index = 0
        adjusted_index = 0
        for decision_session in decisions:
            cutoff = decision_times[decision_session]
            while raw_index < len(raw_events) and raw_events[raw_index][0] <= cutoff:
                _, bar = raw_events[raw_index]
                raw_state[bar.session] = _newer_bar(raw_state.get(bar.session), bar)
                raw_index += 1
            while (
                adjusted_index < len(adjusted_events)
                and adjusted_events[adjusted_index][0] <= cutoff
            ):
                _, bar = adjusted_events[adjusted_index]
                adjusted_state[bar.session] = _newer_bar(adjusted_state.get(bar.session), bar)
                adjusted_index += 1
            prepared.feature_windows[(decision_session, instrument_id)] = align_feature_window(
                sessions, decision_session, raw_state, adjusted_state
            )


def _is_strategy_decision_session(session: date, next_session: date | None) -> bool:
    return next_session is None or next_session.isocalendar()[:2] != session.isocalendar()[:2]


def _strategy_snapshot(
    *,
    data: PointInTimeDataset,
    prepared: _PreparedDataset,
    decision_session: date,
    next_session: date | None,
    current_positions: tuple[UUID, ...],
    position_rank_streaks: Mapping[UUID, int],
) -> StrategyDataSnapshot:
    cutoff = _decision_time(data, decision_session)
    universe = max(
        (
            snapshot
            for snapshot in data.universe_snapshots
            if snapshot.effective_session <= decision_session and snapshot.published_at <= cutoff
        ),
        key=lambda item: (item.effective_session, item.published_at, item.version),
        default=None,
    )
    members = () if universe is None else universe.members
    cached_instruments = prepared.strategy_instruments.get(decision_session)
    if cached_instruments is None:
        instruments: list[BaselineInstrumentSnapshot] = []
        for instrument_id in sorted(members, key=lambda item: item.int):
            instrument = _strategy_instrument_snapshot(
                data=data,
                prepared=prepared,
                instrument_id=instrument_id,
                decision_session=decision_session,
            )
            if instrument is not None:
                instruments.append(instrument)
        cached_instruments = tuple(instruments)
        prepared.strategy_instruments[decision_session] = cached_instruments
    return StrategyDataSnapshot(
        decision_session=decision_session,
        next_session=next_session,
        instruments=cached_instruments,
        current_positions=current_positions,
        position_rank_states=tuple(
            PositionRankState(
                instrument_id=instrument_id,
                consecutive_outside_top20_weeks=position_rank_streaks.get(instrument_id, 0),
            )
            for instrument_id in sorted(current_positions, key=lambda value: value.int)
        ),
    )


def _strategy_instrument_snapshot(
    *,
    data: PointInTimeDataset,
    prepared: _PreparedDataset,
    instrument_id: UUID,
    decision_session: date,
) -> BaselineInstrumentSnapshot | None:
    visible_raw, visible_adjusted = prepared.feature_windows.get(
        (decision_session, instrument_id), ((), ())
    )
    sector_exposures = _sector_exposures_for(data, instrument_id, decision_session)
    if (
        not visible_adjusted
        or visible_adjusted[-1].session != decision_session
        or not _sector_exposures_complete(data.asset_types[instrument_id], sector_exposures)
    ):
        return None
    return BaselineInstrumentSnapshot(
        instrument_id=instrument_id,
        symbol=str(instrument_id),
        asset_type=data.asset_types[instrument_id],
        sector_exposures=tuple(
            BaselineSectorExposure(sector=sector, weight=weight)
            for sector, weight in _normalized_sector_weights(sector_exposures)
        ),
        raw_close=visible_raw[-1].close,
        closes=tuple(bar.close for bar in visible_adjusted),
        dollar_volumes=tuple(bar.close * bar.volume for bar in visible_raw),
        eligible_for_new_position=not _sector_exposure_stale(
            data, instrument_id, decision_session, sector_exposures
        ),
    )


def _strategy_session_incomplete_reasons(
    *,
    data: PointInTimeDataset,
    prepared: _PreparedDataset,
    session: date,
    raw_bars: Mapping[UUID, BacktestBar],
    adjusted_bars: Mapping[UUID, BacktestBar],
    untradable: set[UUID],
    holdings: set[UUID],
    pending_instruments: set[UUID],
) -> tuple[str, ...]:
    cutoff = _decision_time(data, session)
    universe = max(
        (
            snapshot
            for snapshot in data.universe_snapshots
            if snapshot.effective_session <= session and snapshot.published_at <= cutoff
        ),
        key=lambda item: (item.effective_session, item.published_at, item.version),
        default=None,
    )
    if universe is None:
        return (f"{session}:strategy_universe_missing",)
    reasons: list[str] = []
    required_instruments = set(universe.members) | holdings | pending_instruments
    is_decision_session = _is_strategy_decision_session(
        session, _next_session(data.authority.exchange_calendar.sessions, session)
    )
    for instrument_id in sorted(required_instruments, key=lambda value: value.int):
        if instrument_id in untradable:
            continue
        if instrument_id not in raw_bars:
            reasons.append(f"{session}:{instrument_id}:raw_bar_missing")
        if instrument_id not in adjusted_bars:
            reasons.append(f"{session}:{instrument_id}:adjusted_bar_missing")
        if is_decision_session:
            raw_history, adjusted_history = prepared.feature_windows.get(
                (session, instrument_id), ((), ())
            )
            if (
                len(raw_history) < 200
                or len(adjusted_history) < 200
                or raw_history[-1].session != session
                or adjusted_history[-1].session != session
            ):
                reasons.append(f"{session}:{instrument_id}:strategy_warmup_incomplete")
        if not _sector_exposures_complete(
            data.asset_types[instrument_id],
            _sector_exposures_for(data, instrument_id, session),
        ):
            reasons.append(f"{session}:{instrument_id}:sector_exposure_incomplete")
    return tuple(reasons)


def _sector_exposures_for(
    data: PointInTimeDataset, instrument_id: UUID, session: date
) -> tuple[SectorClassification, ...]:
    if data.asset_types[instrument_id] == "non_equity_etf":
        return ()
    return select_sector_exposures(
        classifications=data.sector_classifications,
        instrument_id=instrument_id,
        asset_type=data.asset_types[instrument_id],
        sector_source=data.authority.sector_collection.source,
        session=session,
        cutoff=_decision_time(data, session),
    )


def _sector_exposure_stale(
    data: PointInTimeDataset,
    instrument_id: UUID,
    session: date,
    exposures: tuple[SectorClassification, ...],
) -> bool:
    return (
        data.asset_types[instrument_id] == "equity_etf"
        and data.authority.sector_collection.source != "fixture"
        and bool(exposures)
        and (session - exposures[0].effective_session).days > 30
    )


def _rebalance(
    *,
    session: date,
    signal_session: date,
    target_weights: Mapping[UUID, Decimal],
    bars: Mapping[UUID, BacktestBar],
    ledger: _Ledger,
    cost_model: CostModel,
    fallback_prices: Mapping[UUID, Decimal],
    execution_price: Literal["open", "close"],
) -> tuple[list[BacktestFill], list[BacktestOrderOutcome], Decimal]:
    result = execute_target_weights(
        _ledger_to_simulation_state(ledger, fallback_prices),
        TargetWeightCommand(
            signal_session=signal_session,
            target_session=session,
            targets=tuple(
                TargetWeight(instrument_id=instrument_id, weight=weight)
                for instrument_id, weight in target_weights.items()
            ),
            bars=tuple(
                SimulationBar(
                    instrument_id=instrument_id,
                    open=bar.open,
                    close=bar.close,
                )
                for instrument_id, bar in bars.items()
            ),
            fallback_marks=tuple(
                SimulationMark(instrument_id=instrument_id, price=price)
                for instrument_id, price in fallback_prices.items()
            ),
            cost_model=cost_model,
            execution_price=execution_price,
        ),
    )
    _replace_ledger_from_simulation_state(ledger, result.state)
    return (
        [
            BacktestFill(
                instrument_id=fill.instrument_id,
                signal_session=fill.signal_session,
                fill_session=fill.fill_session,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                notional=fill.notional,
                cost=fill.cost,
            )
            for fill in result.fills
        ],
        [
            BacktestOrderOutcome(
                instrument_id=outcome.instrument_id,
                signal_session=outcome.signal_session,
                target_session=outcome.target_session,
                side=outcome.side,
                status=outcome.status,
                reason=outcome.reason,
            )
            for outcome in result.outcomes
        ],
        result.turnover_notional,
    )


def _apply_session_events_to_ledger(
    ledger: _Ledger,
    *,
    session: date,
    actions: tuple[CorporateActionEvent, ...] | list[CorporateActionEvent],
    delistings: tuple[DelistingEvent, ...] | list[DelistingEvent],
    last_closes: dict[UUID, Decimal],
) -> int:
    result = apply_simulation_events(
        _ledger_to_simulation_state(ledger, last_closes),
        SimulationSessionEvents(
            session=session,
            splits=tuple(
                SimulationSplitEvent(
                    authority_id=action.authority_id,
                    instrument_id=action.instrument_id,
                    ratio=action.value,
                )
                for action in actions
                if action.action_type == "split"
            ),
            dividends=tuple(
                SimulationDividendEvent(
                    authority_id=action.authority_id,
                    instrument_id=action.instrument_id,
                    amount_per_share=action.value,
                    entitlement_session=action.session,
                    payable_session=action.payable_session or date.max,
                )
                for action in actions
                if action.action_type == "dividend"
            ),
            delistings=tuple(
                SimulationDelistingEvent(
                    authority_id=event.authority_id,
                    instrument_id=event.instrument_id,
                    recovery_price=event.recovery_price,
                )
                for event in delistings
            ),
        ),
    )
    _replace_ledger_from_simulation_state(ledger, result.state)
    for event in delistings:
        last_closes.pop(event.instrument_id, None)
    return result.delisting_count


def _ledger_to_simulation_state(
    ledger: _Ledger,
    last_closes: Mapping[UUID, Decimal],
) -> SimulationState:
    instrument_ids = (
        set(ledger.holdings)
        | set(ledger.cost_basis)
        | set(ledger.realized)
        | set(ledger.costs)
        | set(ledger.untradable)
        | {instrument_id for instrument_id, _, _ in ledger.dividend_receivables.values()}
    )
    return SimulationState(
        cash=ledger.cash,
        instruments=tuple(
            SimulationInstrumentState(
                instrument_id=instrument_id,
                quantity=ledger.holdings.get(instrument_id, Decimal(0)),
                cost_basis=ledger.cost_basis.get(instrument_id, Decimal(0)),
                realized_pnl=ledger.realized.get(instrument_id, Decimal(0)),
                cumulative_cost=ledger.costs.get(instrument_id, Decimal(0)),
                last_close=last_closes.get(instrument_id),
                untradable=instrument_id in ledger.untradable,
            )
            for instrument_id in instrument_ids
        ),
        receivables=tuple(
            DividendReceivable(
                authority_id=authority_id,
                instrument_id=instrument_id,
                amount=amount,
                payable_session=payable_session,
            )
            for authority_id, (
                instrument_id,
                amount,
                payable_session,
            ) in ledger.dividend_receivables.items()
        ),
    )


def _replace_ledger_from_simulation_state(
    ledger: _Ledger,
    state: SimulationState,
) -> None:
    ledger.cash = state.cash
    ledger.holdings = {
        item.instrument_id: item.quantity for item in state.instruments if item.quantity != 0
    }
    ledger.cost_basis = {
        item.instrument_id: item.cost_basis for item in state.instruments if item.cost_basis != 0
    }
    ledger.realized = {
        item.instrument_id: item.realized_pnl
        for item in state.instruments
        if item.realized_pnl != 0
    }
    ledger.costs = {
        item.instrument_id: item.cumulative_cost
        for item in state.instruments
        if item.cumulative_cost != 0
    }
    ledger.dividend_receivables = {
        item.authority_id: (item.instrument_id, item.amount, item.payable_session)
        for item in state.receivables
    }
    ledger.untradable = {item.instrument_id for item in state.instruments if item.untradable}


def _performance_attribution(
    *,
    ledger: _Ledger,
    terminal_nav: Decimal,
    initial_nav: Decimal,
    last_closes: Mapping[UUID, Decimal],
    data: PointInTimeDataset,
    end_session: date,
    sector_contributions: Mapping[str, Decimal],
) -> PerformanceAttribution:
    del data, end_session
    unrealized: dict[UUID, Decimal] = {}
    for instrument_id, quantity in ledger.holdings.items():
        if instrument_id in last_closes:
            unrealized[instrument_id] = quantity * last_closes[
                instrument_id
            ] - ledger.cost_basis.get(instrument_id, Decimal(0))
    for instrument_id, amount, _ in ledger.dividend_receivables.values():
        unrealized[instrument_id] = unrealized.get(instrument_id, Decimal(0)) + amount
    instruments = set(ledger.realized) | set(unrealized) | set(ledger.costs)
    net_by_instrument = {
        instrument_id: ledger.realized.get(instrument_id, Decimal(0))
        + unrealized.get(instrument_id, Decimal(0))
        - ledger.costs.get(instrument_id, Decimal(0))
        for instrument_id in instruments
    }
    realized_total = sum(ledger.realized.values(), start=Decimal(0))
    unrealized_total = sum(unrealized.values(), start=Decimal(0))
    cost_total = sum(ledger.costs.values(), start=Decimal(0))
    expected_change = realized_total + unrealized_total - cost_total
    actual_change = terminal_nav - initial_nav
    difference = canonical_decimal(actual_change - expected_change)
    return PerformanceAttribution(
        realized_pnl=realized_total,
        unrealized_pnl=unrealized_total,
        costs=cost_total,
        cash_contribution=Decimal(0),
        reconciliation_difference=difference,
        by_instrument=tuple(
            AttributionContribution(key=str(instrument_id), value=value)
            for instrument_id, value in sorted(
                net_by_instrument.items(), key=lambda item: item[0].int
            )
        ),
        by_sector=tuple(
            AttributionContribution(key=sector, value=value)
            for sector, value in sorted(sector_contributions.items())
        ),
        reconciled=abs(difference) <= RESULT_QUANTUM,
    )


def _cumulative_pnl_by_instrument(
    ledger: _Ledger,
    current_prices: Mapping[UUID, Decimal],
    prior: Mapping[UUID, Decimal],
) -> dict[UUID, Decimal]:
    receivables: dict[UUID, Decimal] = {}
    for instrument_id, amount, _ in ledger.dividend_receivables.values():
        receivables[instrument_id] = receivables.get(instrument_id, Decimal(0)) + amount
    instrument_ids = (
        set(prior)
        | set(ledger.realized)
        | set(ledger.holdings)
        | set(ledger.costs)
        | set(receivables)
    )
    cumulative: dict[UUID, Decimal] = {}
    for instrument_id in instrument_ids:
        if instrument_id in ledger.holdings and instrument_id not in current_prices:
            cumulative[instrument_id] = prior.get(instrument_id, Decimal(0))
            continue
        unrealized = ledger.holdings.get(instrument_id, Decimal(0)) * current_prices.get(
            instrument_id, Decimal(0)
        ) - ledger.cost_basis.get(instrument_id, Decimal(0))
        cumulative[instrument_id] = (
            ledger.realized.get(instrument_id, Decimal(0))
            + unrealized
            + receivables.get(instrument_id, Decimal(0))
            - ledger.costs.get(instrument_id, Decimal(0))
        )
    return cumulative


def _mark_to_market(
    cash: Decimal,
    holdings: Mapping[UUID, Decimal],
    prices: Mapping[UUID, Decimal],
    *,
    receivables: Mapping[UUID, tuple[UUID, Decimal, date]],
) -> Decimal:
    return mark_simulation_to_market(
        SimulationState(
            cash=cash,
            instruments=tuple(
                SimulationInstrumentState(
                    instrument_id=instrument_id,
                    quantity=quantity,
                    cost_basis=Decimal(0),
                    realized_pnl=Decimal(0),
                    cumulative_cost=Decimal(0),
                    last_close=None,
                    untradable=False,
                )
                for instrument_id, quantity in holdings.items()
            ),
            receivables=tuple(
                DividendReceivable(
                    authority_id=authority_id,
                    instrument_id=instrument_id,
                    amount=amount,
                    payable_session=payable_session,
                )
                for authority_id, (
                    instrument_id,
                    amount,
                    payable_session,
                ) in receivables.items()
            ),
        ),
        tuple(
            SimulationMark(instrument_id=instrument_id, price=price)
            for instrument_id, price in prices.items()
        ),
    ).nav


def _next_session(sessions: tuple[date, ...] | list[date], current: date) -> date | None:
    for session in sessions:
        if session > current:
            return session
    return None


def _mapped_session_on_or_after(
    sessions: tuple[date, ...] | list[date], provider_date: date
) -> date | None:
    return next((session for session in sessions if session >= provider_date), None)


def _walk_forward_windows(
    protocol: BacktestProtocol,
    *,
    calendar_sessions: tuple[date, ...],
    bars_by_session: Mapping[date, Mapping[UUID, BacktestBar]],
    nav_points: tuple[NavPoint, ...],
    fills: tuple[BacktestFill, ...],
) -> tuple[WalkForwardWindow, ...]:
    if not protocol.walk_forward:
        return ()
    oos_start = _add_months(protocol.start, (protocol.train_years + protocol.validation_years) * 12)
    windows: list[WalkForwardWindow] = []
    ordinal = 1
    while _add_months(oos_start, protocol.out_of_sample_months) <= protocol.end:
        oos_end = _add_months(oos_start, protocol.out_of_sample_months)
        boundaries = {
            "train": (
                _add_months(oos_start, -(protocol.train_years + protocol.validation_years) * 12),
                _add_months(oos_start, -protocol.validation_years * 12),
            ),
            "validation": (_add_months(oos_start, -protocol.validation_years * 12), oos_start),
            "oos": (oos_start, oos_end),
        }
        reasons: list[str] = []
        coverages: list[Decimal] = []
        minimums = {"train": 600, "validation": 200, "oos": 100}
        for name, (start, end) in boundaries.items():
            expected = [session for session in calendar_sessions if start <= session < end]
            observed = sum(bool(bars_by_session.get(session)) for session in expected)
            coverage = Decimal(observed) / Decimal(len(expected)) if expected else Decimal(0)
            coverages.append(coverage)
            if len(expected) < minimums[name]:
                reasons.append(f"{name}_calendar_sessions_insufficient")
            if coverage < Decimal("0.8"):
                reasons.append(f"{name}_bar_coverage_below_80pct")
        window = WalkForwardWindow(
            ordinal=ordinal,
            train_start=boundaries["train"][0],
            train_end=boundaries["train"][1],
            validation_start=boundaries["validation"][0],
            validation_end=boundaries["validation"][1],
            out_of_sample_start=oos_start,
            out_of_sample_end=oos_end,
            session_coverage=min(coverages, default=Decimal(0)),
            valid=not reasons,
            invalid_reasons=tuple(reasons),
            metrics=_window_metrics(
                oos_start,
                oos_end,
                nav_points=nav_points,
                fills=fills,
                initial_nav=protocol.initial_nav,
                experiment_attempt_count=protocol.experiment_attempt_count,
            ),
        )
        windows.append(window)
        ordinal += 1
        oos_start = _add_months(oos_start, protocol.step_months)
    return tuple(windows)


def _window_metrics(
    start: date,
    end: date,
    *,
    nav_points: tuple[NavPoint, ...],
    fills: tuple[BacktestFill, ...],
    initial_nav: Decimal,
    experiment_attempt_count: int,
) -> BacktestMetrics:
    window_nav = tuple(point.nav for point in nav_points if start <= point.session < end)
    preceding = tuple(point.nav for point in nav_points if point.session < start)
    window_initial_nav = preceding[-1] if preceding else initial_nav
    window_fills = tuple(fill for fill in fills if start <= fill.fill_session < end)
    return calculate_metrics(
        (window_initial_nav, *window_nav) if window_nav else (),
        turnover_notional=sum((fill.notional for fill in window_fills), start=Decimal(0)),
        initial_nav=window_initial_nav,
        total_cost=sum((fill.cost for fill in window_fills), start=Decimal(0)),
        experiment_attempt_count=experiment_attempt_count,
    )


def _history_requirement_met(
    protocol: BacktestProtocol, calendar_sessions: tuple[date, ...]
) -> bool:
    sessions = [
        session for session in calendar_sessions if protocol.start <= session <= protocol.end
    ]
    return protocol.end >= _add_months(protocol.start, 84) and len(sessions) >= 1400


def _benchmark_comparisons(
    *,
    data: PointInTimeDataset,
    prepared: _PreparedDataset,
    protocol: BacktestProtocol,
    strategy_metrics: BacktestMetrics,
    strategy_terminal_nav: Decimal,
) -> tuple[BenchmarkComparison, ...]:
    comparisons: list[BenchmarkComparison] = [
        BenchmarkComparison(
            comparison_key="champion",
            terminal_nav=strategy_terminal_nav,
            metrics=strategy_metrics,
            excess_total_return=Decimal(0),
            complete=True,
        )
    ]
    configured: tuple[tuple[Literal["spy", "equal_weight"], tuple[UUID, ...]], ...] = (
        ("spy", (data.spy_instrument_id,)),
        ("equal_weight", data.authority.equal_weight_benchmark.instrument_ids),
    )
    for comparison_key, instrument_ids in configured:
        outcome = _run_passive_benchmark(
            instrument_ids=instrument_ids,
            data=data,
            prepared=prepared,
            protocol=protocol,
        )
        comparisons.append(
            BenchmarkComparison(
                comparison_key=comparison_key,
                terminal_nav=outcome.terminal_nav,
                metrics=outcome.metrics,
                excess_total_return=strategy_metrics.total_return - outcome.metrics.total_return,
                complete=outcome.complete,
                incomplete_reasons=outcome.incomplete_reasons,
            )
        )
    return tuple(sorted(comparisons, key=lambda item: item.comparison_key))


def _run_passive_benchmark(
    *,
    instrument_ids: tuple[UUID, ...],
    data: PointInTimeDataset,
    prepared: _PreparedDataset,
    protocol: BacktestProtocol,
) -> _PassiveBenchmarkOutcome:
    sessions = [
        session
        for session in data.authority.exchange_calendar.sessions
        if protocol.start <= session <= protocol.end
    ]
    if len(sessions) < 2:
        metrics = calculate_metrics(
            (), turnover_notional=Decimal(0), initial_nav=protocol.initial_nav
        )
        return _PassiveBenchmarkOutcome(
            terminal_nav=canonical_decimal(protocol.initial_nav),
            metrics=metrics,
            complete=False,
            incomplete_reasons=("fewer_than_two_exchange_sessions",),
        )
    bars_by_session = prepared.raw_by_session
    actions = _group_actions(data.corporate_actions)
    delistings = _group_delistings(data.delistings)
    unresolved_delistings = _group_unresolved_delistings(data.unresolved_delistings)
    ledger = _Ledger(cash=protocol.initial_nav)
    fills: list[BacktestFill] = []
    outcomes: list[BacktestOrderOutcome] = []
    nav_values: list[Decimal] = []
    last_closes: dict[UUID, Decimal] = {}
    signal_session = sessions[0]
    fill_session = sessions[1]
    target_weight = Decimal(1) / Decimal(len(instrument_ids))
    for session in sessions:
        session_bars = bars_by_session.get(session, {})
        _apply_session_events_to_ledger(
            ledger,
            session=session,
            actions=actions.get(session, ()),
            delistings=delistings.get(session, ()),
            last_closes=last_closes,
        )
        for event in unresolved_delistings.get(session, ()):
            ledger.untradable.add(event.instrument_id)
            last_closes.pop(event.instrument_id, None)
        if session == fill_session:
            new_fills, new_outcomes, _ = _rebalance(
                session=session,
                signal_session=signal_session,
                target_weights={instrument_id: target_weight for instrument_id in instrument_ids},
                bars=session_bars,
                ledger=ledger,
                cost_model=protocol.cost_model,
                fallback_prices=last_closes,
                execution_price="open",
            )
            fills.extend(new_fills)
            outcomes.extend(new_outcomes)
        last_closes.update(
            {
                instrument_id: bar.close
                for instrument_id, bar in session_bars.items()
                if instrument_id not in ledger.untradable
            }
        )
        nav_values.append(
            _mark_to_market(
                ledger.cash,
                ledger.holdings,
                last_closes,
                receivables=ledger.dividend_receivables,
            )
        )
    incomplete = [
        f"{outcome.instrument_id}:{outcome.status}"
        for outcome in outcomes
        if outcome.status != "filled"
    ]
    incomplete.extend(
        f"{event.instrument_id}:delisting_recovery_unresolved"
        for event in data.unresolved_delistings
        if event.instrument_id in instrument_ids
    )
    for instrument_id in instrument_ids:
        terminal_sessions = (
            *(event.session for event in data.delistings if event.instrument_id == instrument_id),
            *(
                event.session
                for event in data.unresolved_delistings
                if event.instrument_id == instrument_id
            ),
        )
        terminal_session = min(terminal_sessions, default=None)
        expected_marks = [
            session
            for session in sessions[1:]
            if terminal_session is None or session < terminal_session
        ]
        observed = sum(
            instrument_id in bars_by_session.get(session, {}) for session in expected_marks
        )
        if observed != len(expected_marks):
            incomplete.append(f"{instrument_id}:mark_coverage_incomplete")
    turnover = sum((fill.notional for fill in fills), start=Decimal(0))
    total_cost = sum((fill.cost for fill in fills), start=Decimal(0))
    metrics = calculate_metrics(
        tuple(nav_values),
        turnover_notional=turnover,
        initial_nav=protocol.initial_nav,
        total_cost=total_cost,
        experiment_attempt_count=protocol.experiment_attempt_count,
    )
    return _PassiveBenchmarkOutcome(
        terminal_nav=canonical_decimal(nav_values[-1]),
        metrics=metrics,
        complete=not incomplete and len(fills) == len(instrument_ids),
        incomplete_reasons=tuple(sorted(incomplete)),
    )


def _group_actions(
    actions: tuple[CorporateActionEvent, ...],
) -> dict[date, list[CorporateActionEvent]]:
    grouped: dict[date, list[CorporateActionEvent]] = {}
    for action in actions:
        grouped.setdefault(action.session, []).append(action)
    for values in grouped.values():
        values.sort(key=lambda item: _canonical_json(item.model_dump(mode="json")))
    return grouped


def _group_delistings(
    events: tuple[DelistingEvent, ...],
) -> dict[date, list[DelistingEvent]]:
    grouped: dict[date, list[DelistingEvent]] = {}
    for event in events:
        grouped.setdefault(event.session, []).append(event)
    for values in grouped.values():
        values.sort(key=lambda item: _canonical_json(item.model_dump(mode="json")))
    return grouped


def _group_unresolved_delistings(
    events: tuple[UnresolvedDelistingEvent, ...],
) -> dict[date, list[UnresolvedDelistingEvent]]:
    grouped: dict[date, list[UnresolvedDelistingEvent]] = {}
    for event in events:
        grouped.setdefault(event.session, []).append(event)
    for values in grouped.values():
        values.sort(key=lambda item: _canonical_json(item.model_dump(mode="json")))
    return grouped


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _canonical_model_items(items: Sequence[BaseModel]) -> list[dict[str, object]]:
    payloads = [item.model_dump(mode="json") for item in items]
    return sorted(payloads, key=_canonical_json)


def _canonical_authority_payload(authority: BacktestDataAuthority) -> dict[str, object]:
    payload = authority.model_dump(mode="json")
    payload["price_series"] = _canonical_model_items(list(authority.price_series))
    payload["source_selections"] = _canonical_model_items(list(authority.source_selections))
    for report in payload["source_selections"]:
        report["candidate_price_bar_set_authority_ids"] = sorted(
            report["candidate_price_bar_set_authority_ids"]
        )
    for name in ("universe_collection", "sector_collection", "corporate_action_collection"):
        collection = dict(payload[name])
        collection["member_ids"] = sorted(collection["member_ids"])
        payload[name] = collection
    benchmark = dict(payload["equal_weight_benchmark"])
    benchmark["instrument_ids"] = sorted(benchmark["instrument_ids"])
    payload["equal_weight_benchmark"] = benchmark
    return payload


def _audited_data_hash(data: PointInTimeDataset, protocol: BacktestProtocol) -> str:
    cutoff = _decision_time(data, protocol.end)
    bars = sorted(
        (bar for bar in data.bars if bar.available_at <= cutoff and bar.session <= protocol.end),
        key=lambda bar: (bar.instrument_id.int, bar.session, bar.available_at, bar.revision),
    )
    adjusted_bars = sorted(
        (
            bar
            for bar in data.adjusted_bars
            if bar.available_at <= cutoff and bar.session <= protocol.end
        ),
        key=lambda bar: (bar.instrument_id.int, bar.session, bar.available_at, bar.revision),
    )
    universes = sorted(
        (
            snapshot
            for snapshot in data.universe_snapshots
            if snapshot.published_at <= cutoff and snapshot.effective_session <= protocol.end
        ),
        key=lambda snapshot: (snapshot.effective_session, snapshot.published_at, snapshot.version),
    )
    sectors = sorted(
        (
            item
            for item in data.sector_classifications
            if item.published_at <= cutoff and item.effective_session <= protocol.end
        ),
        key=lambda item: (item.instrument_id.int, item.effective_session, item.published_at),
    )
    actions = sorted(
        (
            action
            for action in data.corporate_actions
            if action.session <= protocol.end and action.available_at <= cutoff
        ),
        key=lambda action: (action.session, action.instrument_id.int, action.action_type),
    )
    delistings = sorted(
        (
            event
            for event in data.delistings
            if event.session <= protocol.end and event.available_at <= cutoff
        ),
        key=lambda event: (event.session, event.instrument_id.int),
    )
    unresolved_delistings = sorted(
        (
            event
            for event in data.unresolved_delistings
            if event.session <= protocol.end and event.available_at <= cutoff
        ),
        key=lambda event: (event.session, event.instrument_id.int),
    )
    return _hash(
        {
            "authority": _canonical_authority_payload(data.authority),
            "bars": _canonical_model_items(bars),
            "adjusted_bars": _canonical_model_items(adjusted_bars),
            "universe_snapshots": sorted(
                [
                    {
                        **snapshot.model_dump(mode="json"),
                        "members": sorted(str(value) for value in snapshot.members),
                    }
                    for snapshot in universes
                ],
                key=_canonical_json,
            ),
            "asset_types": sorted((str(key), value) for key, value in data.asset_types.items()),
            "risk_buckets": sorted((str(key), value) for key, value in data.risk_buckets.items()),
            "sector_classifications": _canonical_model_items(sectors),
            "corporate_actions": _canonical_model_items(actions),
            "delistings": _canonical_model_items(delistings),
            "unresolved_delistings": _canonical_model_items(unresolved_delistings),
            "spy_instrument_id": str(data.spy_instrument_id),
        }
    )


def _authority_bundle_hash(data: PointInTimeDataset) -> str:
    """Hash every frozen authority identity that may affect the historical run."""
    return _hash(
        {
            "data_authority": _canonical_authority_payload(data.authority),
            "universe_snapshots": sorted(
                [
                    {
                        **item.model_dump(mode="json"),
                        "members": sorted(str(value) for value in item.members),
                    }
                    for item in data.universe_snapshots
                ],
                key=_canonical_json,
            ),
            "sector_classifications": _canonical_model_items(data.sector_classifications),
            "corporate_actions": _canonical_model_items(data.corporate_actions),
            "delistings": _canonical_model_items(data.delistings),
            "unresolved_delistings": _canonical_model_items(data.unresolved_delistings),
            "asset_types": sorted((str(key), value) for key, value in data.asset_types.items()),
            "risk_buckets": sorted((str(key), value) for key, value in data.risk_buckets.items()),
        }
    )


def _engine_code_hash(code_version: str) -> str:
    root = Path(__file__).resolve().parents[1]
    files = (
        Path(__file__).resolve(),
        root / "backtest" / "cost_policy.py",
        root / "backtest" / "metrics.py",
        root / "simulation" / "canonical.py",
        root / "simulation" / "costs.py",
        root / "simulation" / "engine.py",
        root / "simulation" / "models.py",
        root / "strategy" / "baseline.py",
        root / "strategy" / "features.py",
        root / "strategy" / "dsl.py",
        root / "strategy" / "interpreter.py",
    )
    digest = hashlib.sha256()
    digest.update(code_version.encode())
    for path in sorted(files, key=lambda item: (item.parent.name, item.name)):
        digest.update(f"{path.parent.name}/{path.name}".encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _terminal_nav(outcome: _SimulationOutcome, fallback: Decimal) -> Decimal:
    return outcome.nav[-1].nav if outcome.nav else canonical_decimal(fallback)


def _add_months(value: date, months: int) -> date:
    total = value.year * 12 + value.month - 1 + months
    year, month_index = divmod(total, 12)
    month = month_index + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
