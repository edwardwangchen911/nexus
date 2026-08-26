"""Immutable value objects for deterministic portfolio simulation."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import UUID

from nexus.simulation.canonical import RESULT_QUANTUM, canonical_input, validated_decimal
from nexus.simulation.costs import CostModel


def _canonical(value: Decimal | int, field: str) -> Decimal:
    return canonical_input(value, field=field)


def _exact(value: Decimal | int, field: str) -> Decimal:
    return validated_decimal(value, field=field)


def _require_nonnegative(value: Decimal, field: str) -> None:
    if value < 0:
        raise ValueError(f"{field} must be nonnegative")


def _require_positive(value: Decimal, field: str) -> None:
    if value <= 0:
        raise ValueError(f"{field} must be positive")


@dataclass(frozen=True)
class SimulationInstrumentState:
    instrument_id: UUID
    quantity: Decimal
    cost_basis: Decimal
    realized_pnl: Decimal
    cumulative_cost: Decimal
    last_close: Decimal | None
    untradable: bool

    def __post_init__(self) -> None:
        for field in ("quantity", "cost_basis", "realized_pnl", "cumulative_cost"):
            object.__setattr__(self, field, _exact(getattr(self, field), field))
        _require_nonnegative(self.quantity, "quantity")
        _require_nonnegative(self.cost_basis, "cost_basis")
        _require_nonnegative(self.cumulative_cost, "cumulative_cost")
        if self.last_close is not None:
            object.__setattr__(self, "last_close", _canonical(self.last_close, "last_close"))
            _require_positive(self.last_close, "last_close")


@dataclass(frozen=True)
class DividendReceivable:
    authority_id: UUID
    instrument_id: UUID
    amount: Decimal
    payable_session: date

    def __post_init__(self) -> None:
        object.__setattr__(self, "amount", _exact(self.amount, "amount"))
        _require_nonnegative(self.amount, "amount")


@dataclass(frozen=True)
class SimulationState:
    cash: Decimal
    instruments: tuple[SimulationInstrumentState, ...]
    receivables: tuple[DividendReceivable, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "cash", _exact(self.cash, "cash"))
        instrument_ids = [item.instrument_id for item in self.instruments]
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("state instruments must be unique")
        authority_ids = [item.authority_id for item in self.receivables]
        if len(authority_ids) != len(set(authority_ids)):
            raise ValueError("receivable authorities must be unique")
        object.__setattr__(
            self,
            "instruments",
            tuple(sorted(self.instruments, key=lambda item: item.instrument_id.int)),
        )
        object.__setattr__(
            self,
            "receivables",
            tuple(sorted(self.receivables, key=lambda item: item.authority_id.int)),
        )


@dataclass(frozen=True)
class SimulationBar:
    instrument_id: UUID
    open: Decimal
    close: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "open", _canonical(self.open, "open"))
        object.__setattr__(self, "close", _canonical(self.close, "close"))
        _require_positive(self.open, "open")
        _require_positive(self.close, "close")


@dataclass(frozen=True)
class SimulationMark:
    instrument_id: UUID
    price: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "price", _canonical(self.price, "price"))
        _require_positive(self.price, "price")


@dataclass(frozen=True)
class TargetWeight:
    instrument_id: UUID
    weight: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "weight", _exact(self.weight, "weight"))
        if not Decimal(0) <= self.weight <= Decimal(1):
            raise ValueError("target weight must be between zero and one")


@dataclass(frozen=True)
class TargetWeightCommand:
    signal_session: date
    target_session: date
    targets: tuple[TargetWeight, ...]
    bars: tuple[SimulationBar, ...]
    fallback_marks: tuple[SimulationMark, ...]
    cost_model: CostModel
    execution_price: Literal["open", "close"]

    def __post_init__(self) -> None:
        if self.target_session <= self.signal_session:
            raise ValueError("target session must follow signal session")
        target_ids = [item.instrument_id for item in self.targets]
        if len(target_ids) != len(set(target_ids)):
            raise ValueError("target instruments must be unique")
        bar_ids = [item.instrument_id for item in self.bars]
        if len(bar_ids) != len(set(bar_ids)):
            raise ValueError("execution bars must be unique")
        fallback_ids = [item.instrument_id for item in self.fallback_marks]
        if len(fallback_ids) != len(set(fallback_ids)):
            raise ValueError("fallback marks must be unique")
        total_weight = sum((item.weight for item in self.targets), start=Decimal(0))
        if total_weight - Decimal(1) > RESULT_QUANTUM:
            raise ValueError("target weights must not exceed one")
        object.__setattr__(
            self,
            "targets",
            tuple(sorted(self.targets, key=lambda item: item.instrument_id.int)),
        )
        object.__setattr__(
            self,
            "bars",
            tuple(sorted(self.bars, key=lambda item: item.instrument_id.int)),
        )
        object.__setattr__(
            self,
            "fallback_marks",
            tuple(sorted(self.fallback_marks, key=lambda item: item.instrument_id.int)),
        )


@dataclass(frozen=True)
class SimulationFill:
    instrument_id: UUID
    signal_session: date
    fill_session: date
    side: Literal["buy", "sell"]
    quantity: Decimal
    price: Decimal
    notional: Decimal
    cost: Decimal

    def __post_init__(self) -> None:
        for field in ("quantity", "price", "notional", "cost"):
            object.__setattr__(self, field, _canonical(getattr(self, field), field))


@dataclass(frozen=True)
class SimulationOutcome:
    instrument_id: UUID
    signal_session: date
    target_session: date
    side: Literal["buy", "sell", "rebalance"]
    status: Literal["filled", "missing_bar", "untradable"]
    reason: str


@dataclass(frozen=True)
class ExecutionResult:
    state: SimulationState
    fills: tuple[SimulationFill, ...]
    outcomes: tuple[SimulationOutcome, ...]
    turnover_notional: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "turnover_notional",
            _exact(self.turnover_notional, "turnover_notional"),
        )


@dataclass(frozen=True)
class SplitEvent:
    authority_id: UUID
    instrument_id: UUID
    ratio: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "ratio", _canonical(self.ratio, "ratio"))
        _require_positive(self.ratio, "ratio")


@dataclass(frozen=True)
class DividendEvent:
    authority_id: UUID
    instrument_id: UUID
    amount_per_share: Decimal
    entitlement_session: date
    payable_session: date

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "amount_per_share",
            _canonical(self.amount_per_share, "amount_per_share"),
        )
        _require_nonnegative(self.amount_per_share, "amount_per_share")
        if self.payable_session < self.entitlement_session:
            raise ValueError("dividend payable session cannot precede entitlement")


@dataclass(frozen=True)
class DelistingEvent:
    authority_id: UUID
    instrument_id: UUID
    recovery_price: Decimal | None

    def __post_init__(self) -> None:
        if self.recovery_price is not None:
            object.__setattr__(
                self,
                "recovery_price",
                _canonical(self.recovery_price, "recovery_price"),
            )
            _require_nonnegative(self.recovery_price, "recovery_price")


@dataclass(frozen=True)
class SessionEvents:
    session: date
    splits: tuple[SplitEvent, ...] = ()
    dividends: tuple[DividendEvent, ...] = ()
    delistings: tuple[DelistingEvent, ...] = ()

    def __post_init__(self) -> None:
        event_ids = [
            *(item.authority_id for item in self.splits),
            *(item.authority_id for item in self.dividends),
            *(item.authority_id for item in self.delistings),
        ]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("session event authorities must be unique")
        object.__setattr__(
            self,
            "splits",
            tuple(
                sorted(
                    self.splits,
                    key=lambda item: (item.instrument_id.int, item.authority_id.int),
                )
            ),
        )
        object.__setattr__(
            self,
            "dividends",
            tuple(
                sorted(
                    self.dividends,
                    key=lambda item: (item.instrument_id.int, item.authority_id.int),
                )
            ),
        )
        object.__setattr__(
            self,
            "delistings",
            tuple(
                sorted(
                    self.delistings,
                    key=lambda item: (item.instrument_id.int, item.authority_id.int),
                )
            ),
        )


@dataclass(frozen=True)
class EventApplicationResult:
    state: SimulationState
    delisting_count: int


@dataclass(frozen=True)
class ValuationResult:
    state: SimulationState
    nav: Decimal
    missing_instruments: tuple[UUID, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "nav", _exact(self.nav, "nav"))
