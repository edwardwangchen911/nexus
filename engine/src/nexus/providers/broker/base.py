"""Provider-neutral, read-only broker facts used by reconciliation."""

from datetime import date, datetime
from decimal import Decimal
from typing import Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from nexus.domain.time import require_utc

SnapshotStatus = Literal["READY", "PARTIAL", "STALE", "RECONCILIATION_REQUIRED"]
BrokerPageQuality = Literal["PASS", "PARTIAL"]
SnapshotTimeBasis = Literal["provider_event", "gateway_observed"]
BrokerOrderStatus = Literal["pending", "submitted", "partially_filled"]


class BrokerFact(BaseModel):
    """Immutable fact base that never accepts binary floating-point ledger values."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    @field_validator("*", mode="before")
    @classmethod
    def _reject_binary_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise TypeError("broker financial values must not be binary floats")
        return value


class CashBalance(BrokerFact):
    currency: str
    cash_balance: Decimal
    available_cash: Decimal
    provider_as_of: datetime | None = None

    @field_validator("currency")
    @classmethod
    def _currency_is_canonical(cls, value: str) -> str:
        return _canonical_currency(value)

    @field_validator("provider_as_of")
    @classmethod
    def _provider_as_of_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)


class SourceObservation(BrokerFact):
    source_request_id: str
    response_as_of: datetime
    quality: BrokerPageQuality
    time_basis: SnapshotTimeBasis = "provider_event"

    @field_validator("response_as_of")
    @classmethod
    def _response_as_of_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class BrokerPosition(BrokerFact):
    instrument_id: UUID
    currency: str
    quantity: Decimal
    available_quantity: Decimal
    frozen_quantity: Decimal
    avg_cost: Decimal | None
    diluted_cost: Decimal | None
    current_price: Decimal | None = None
    provider_as_of: datetime | None = None

    @field_validator("currency")
    @classmethod
    def _currency_is_canonical(cls, value: str) -> str:
        return _canonical_currency(value)

    @field_validator("provider_as_of")
    @classmethod
    def _provider_as_of_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)


class BrokerOrderFact(BrokerFact):
    provider_order_id: str
    instrument_id: UUID
    side: Literal["buy", "sell"]
    quantity: Decimal
    filled_quantity: Decimal
    price: Decimal | None
    status: BrokerOrderStatus
    updated_at: datetime | None

    @field_validator("updated_at")
    @classmethod
    def _updated_at_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)


class BrokerCashFlow(BrokerFact):
    provider_cash_flow_id: str | None
    currency: str
    amount: Decimal
    occurred_at: datetime | None
    provider_business_date: date | None = None
    kind: str

    @field_validator("currency")
    @classmethod
    def _currency_is_canonical(cls, value: str) -> str:
        return _canonical_currency(value)

    @field_validator("occurred_at")
    @classmethod
    def _occurred_at_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)

    @model_validator(mode="after")
    def _cash_flow_date_required(self) -> "BrokerCashFlow":
        if self.occurred_at is None and self.provider_business_date is None:
            raise ValueError("cash flow requires a business date or an aware timestamp")
        return self


class DateRange(BrokerFact):
    start: date
    end: date

    @model_validator(mode="after")
    def _ordered(self) -> "DateRange":
        if self.end < self.start:
            raise ValueError("date range end must not precede start")
        return self


class BrokerPage[ItemT](BrokerFact):
    items: tuple[ItemT, ...]
    has_more: bool
    next_cursor: str | None
    source_request_id: str
    as_of: datetime
    quality: BrokerPageQuality = "PASS"
    time_basis: SnapshotTimeBasis = "provider_event"

    @field_validator("as_of")
    @classmethod
    def _as_of_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _cursor_matches_page_state(self) -> "BrokerPage[ItemT]":
        if self.has_more != (self.next_cursor is not None):
            raise ValueError("pagination state is inconsistent")
        if self.next_cursor == "":
            raise ValueError("next_cursor must not be empty")
        return self


class PortfolioSnapshot(BrokerFact):
    id: UUID
    portfolio_id: UUID
    status: SnapshotStatus
    cash: tuple[CashBalance, ...]
    positions: tuple[BrokerPosition, ...]
    open_orders: tuple[BrokerOrderFact, ...]
    as_of: datetime | None
    stale_from_snapshot_id: UUID | None = None
    source_request_ids: tuple[str, ...] = ()
    source_observations: tuple[SourceObservation, ...] = ()

    @field_validator("as_of")
    @classmethod
    def _snapshot_as_of_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else require_utc(value)


class BrokerSyncResult(BrokerFact):
    sync_run_id: UUID
    attempt_count: int
    status: SnapshotStatus
    portfolio_snapshot: PortfolioSnapshot
    reconciliation_reasons: tuple[str, ...] = ()
    attempt_traces: tuple[dict[str, object], ...] = ()


class BrokerProviderError(RuntimeError):
    """A sanitized read failure that cannot carry provider payloads or credentials."""


class BrokerProvider(Protocol):
    """Only the four broker read operations the Engine may invoke."""

    async def read_cash(
        self,
        account_id: UUID,
        *,
        sync_run_id: UUID,
    ) -> BrokerPage[CashBalance]: ...

    async def read_positions_page(
        self,
        account_id: UUID,
        *,
        sync_run_id: UUID,
        cursor: str | None,
        limit: int,
    ) -> BrokerPage[BrokerPosition]: ...

    async def read_open_orders_page(
        self,
        account_id: UUID,
        *,
        sync_run_id: UUID,
        cursor: str | None,
        limit: int,
    ) -> BrokerPage[BrokerOrderFact]: ...

    async def read_cash_flows(
        self,
        account_id: UUID,
        window: DateRange,
        cursor: str | None,
        *,
        sync_run_id: UUID,
        limit: int,
    ) -> BrokerPage[BrokerCashFlow]: ...


def _canonical_currency(value: str) -> str:
    if len(value) != 3 or not value.isascii() or not value.isalpha() or value != value.upper():
        raise ValueError("currency must be canonical three-letter uppercase ASCII")
    return value
