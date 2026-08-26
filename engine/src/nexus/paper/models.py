"""Immutable internal paper order, reservation, and ledger value objects."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation, localcontext
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_validator,
)

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
OrderSide = Literal["buy", "sell"]
InternalOrderStatus = Literal[
    "proposed",
    "risk_rejected",
    "risk_approved",
    "awaiting_approval",
    "submitted",
    "partially_filled",
    "filled",
    "cancelled",
    "rejected",
    "reconciling",
]
ReservationType = Literal["cash", "position"]
ReservationEventType = Literal["reserved", "consumed", "released"]
LedgerSide = Literal["debit", "credit"]
_QUANTUM = Decimal("0.000000000001")
_MAX_ABSOLUTE = Decimal("1e26")


def exact_decimal(value: object, *, field: str) -> Decimal:
    if isinstance(value, float):
        raise ValueError(f"{field} must not contain a binary float")
    if isinstance(value, bool) or not isinstance(value, Decimal | int):
        raise ValueError(f"{field} must be a Decimal")
    result = Decimal(value)
    if not result.is_finite() or abs(result) >= _MAX_ABSOLUTE:
        raise ValueError(f"{field} exceeds NUMERIC(38,12)")
    try:
        with localcontext() as context:
            context.prec = 50
            canonical = result.quantize(_QUANTUM)
    except InvalidOperation as error:
        raise ValueError(f"{field} exceeds NUMERIC(38,12)") from error
    if canonical != result:
        raise ValueError(f"{field} must have no more than 12 decimal places")
    return result


def _aware(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value


class FrozenPaperModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class OrderEventInput(FrozenPaperModel):
    event_id: UUID
    event_seq: int = Field(ge=1)
    occurred_at: datetime
    from_status: InternalOrderStatus | None
    to_status: InternalOrderStatus
    actor_kind: Literal["task18", "task19", "user", "reconciliation"]
    reason_code: str = Field(min_length=1, max_length=100)
    payload_hash: Hash64

    @field_validator("occurred_at")
    @classmethod
    def _occurred_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field="occurred_at")


class PaperOrder(FrozenPaperModel):
    order_id: UUID
    account_id: UUID
    strategy_decision_id: UUID
    client_order_id: str = Field(min_length=1, max_length=100)
    instrument_id: UUID
    side: OrderSide
    quantity: Decimal
    worst_price: Decimal | None = None
    events: tuple[OrderEventInput, ...]

    @field_validator("quantity", "worst_price", mode="before")
    @classmethod
    def _quantity_is_exact(cls, value: object, info: ValidationInfo) -> Decimal | None:
        if value is None:
            return None
        return exact_decimal(value, field=info.field_name or "value")

    @model_validator(mode="after")
    def _valid_order(self) -> PaperOrder:
        if self.quantity <= 0:
            raise ValueError("order quantity must be positive")
        if self.quantity != self.quantity.to_integral_value():
            raise ValueError("order quantity must be whole shares")
        if self.worst_price is not None and self.worst_price <= 0:
            raise ValueError("order worst price must be positive")
        event_ids = [event.event_id for event in self.events]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("order event ids must be unique")
        return self


class ReservationEvent(FrozenPaperModel):
    event_id: UUID
    event_seq: int = Field(ge=1)
    event_type: ReservationEventType
    amount: Decimal
    occurred_at: datetime
    payload_hash: Hash64

    @field_validator("amount", mode="before")
    @classmethod
    def _amount_is_exact(cls, value: object) -> Decimal:
        result = exact_decimal(value, field="reservation event amount")
        if result <= 0:
            raise ValueError("reservation event amount must be positive")
        return result

    @field_validator("occurred_at")
    @classmethod
    def _occurred_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field="occurred_at")


class Reservation(FrozenPaperModel):
    reservation_id: UUID
    order_id: UUID
    account_id: UUID
    reservation_type: ReservationType
    currency: Literal["USD"] | None
    instrument_id: UUID | None
    amount: Decimal
    events: tuple[ReservationEvent, ...]

    @field_validator("amount", mode="before")
    @classmethod
    def _amount_is_exact(cls, value: object) -> Decimal:
        result = exact_decimal(value, field="reservation amount")
        if result <= 0:
            raise ValueError("reservation amount must be positive")
        return result

    @model_validator(mode="after")
    def _valid_reservation(self) -> Reservation:
        if self.reservation_type == "cash" and (
            self.currency != "USD" or self.instrument_id is not None
        ):
            raise ValueError("cash reservation requires USD and no instrument")
        if self.reservation_type == "position" and (
            self.instrument_id is None or self.currency is not None
        ):
            raise ValueError("position reservation requires an instrument and no currency")
        event_ids = [event.event_id for event in self.events]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("reservation event ids must be unique")
        return self


class LedgerAccount(FrozenPaperModel):
    account_id: UUID
    paper_account_id: UUID
    code: str = Field(min_length=1, max_length=100)
    normal_side: LedgerSide
    currency: Literal["USD"]


class LedgerEntry(FrozenPaperModel):
    entry_id: UUID
    entry_seq: int = Field(ge=1)
    account_code: str = Field(min_length=1, max_length=100)
    currency: Literal["USD"]
    debit_amount: Decimal
    credit_amount: Decimal

    @field_validator("debit_amount", "credit_amount", mode="before")
    @classmethod
    def _amount_is_exact(cls, value: object, info: ValidationInfo) -> Decimal:
        return exact_decimal(value, field=info.field_name or "value")

    @model_validator(mode="after")
    def _one_side(self) -> LedgerEntry:
        debit_positive = self.debit_amount > 0 and self.credit_amount == 0
        credit_positive = self.credit_amount > 0 and self.debit_amount == 0
        if not (debit_positive ^ credit_positive):
            raise ValueError("ledger entry requires exactly one positive side")
        return self


class LedgerTransaction(FrozenPaperModel):
    transaction_id: UUID
    account_id: UUID
    transaction_type: Literal["funding", "cash_reservation", "fill", "release", "adjustment"]
    reference_type: Literal[
        "initial_funding",
        "cash_reservation",
        "fill",
        "release",
        "adjustment",
    ]
    reference_hash: Hash64
    occurred_at: datetime
    entries: tuple[LedgerEntry, ...]
    transaction_hash: Hash64

    @field_validator("occurred_at")
    @classmethod
    def _occurred_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field="occurred_at")

    @model_validator(mode="after")
    def _unique_entries(self) -> LedgerTransaction:
        entry_ids = [entry.entry_id for entry in self.entries]
        if len(entry_ids) != len(set(entry_ids)):
            raise ValueError("ledger entry ids must be unique")
        entry_sequences = [entry.entry_seq for entry in self.entries]
        if len(entry_sequences) != len(set(entry_sequences)):
            raise ValueError("ledger entry sequences must be unique")
        return self


class Ledger(FrozenPaperModel):
    paper_account_id: UUID
    accounts: tuple[LedgerAccount, ...]
    transactions: tuple[LedgerTransaction, ...]

    @model_validator(mode="after")
    def _unique_roots(self) -> Ledger:
        account_ids = [account.account_id for account in self.accounts]
        account_codes = [account.code for account in self.accounts]
        if len(account_ids) != len(set(account_ids)):
            raise ValueError("ledger account ids must be unique")
        if len(account_codes) != len(set(account_codes)):
            raise ValueError("ledger account codes must be unique")
        if any(account.paper_account_id != self.paper_account_id for account in self.accounts):
            raise ValueError("ledger accounts must belong to the ledger paper account")
        transaction_ids = [transaction.transaction_id for transaction in self.transactions]
        if len(transaction_ids) != len(set(transaction_ids)):
            raise ValueError("ledger transaction ids must be unique")
        return self
