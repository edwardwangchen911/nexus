"""Closed, immutable contracts for the broker-simulation authority."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Set
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from enum import StrEnum
from typing import Annotated, Any, Literal, TypeVar
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StringConstraints,
    field_validator,
    model_validator,
)

from nexus.audit.canonical import canonical_json
from nexus.domain.decimal import decimal_to_string
from nexus.domain.time import require_utc

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_MAX_ABSOLUTE = Decimal("1e26")
_QUANTUM = Decimal("0.000000000001")


class CapabilityStatus(StrEnum):
    OFFLINE_FIXTURE = "OFFLINE_FIXTURE"
    READ_ONLY_ADMITTED = "READ_ONLY_ADMITTED"
    READ_ONLY_ACTIVATED = "READ_ONLY_ACTIVATED"
    SIMULATION_CONTRACT_ADMITTED = "SIMULATION_CONTRACT_ADMITTED"
    SIMULATION_WRITE_ACTIVATED = "SIMULATION_WRITE_ACTIVATED"


class ActivationDisplay(StrEnum):
    OFFLINE_READY = "offline_ready"
    READ_ONLY_ACTIVATED = "read_only_activated"
    SIMULATION_WRITE_ACTIVATED = "simulation_write_activated"


class ExternalActivation(StrEnum):
    BLOCKED = "external_activation_blocked"
    ACTIVATED = "externally_activated"


class IntentStatus(StrEnum):
    DRAFT = "draft"
    PREPARED = "prepared"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    DISPATCHING = "dispatching"
    COMPLETED = "completed"
    PARTIALLY_COMPLETED = "partially_completed"
    FAILED = "failed"
    SUBMISSION_UNKNOWN = "submission_unknown"
    RECONCILING = "reconciling"
    RECONCILED = "reconciled"


class CommandKind(StrEnum):
    CREATE = "create"
    MODIFY = "modify"
    CANCEL = "cancel"


class CommandStatus(StrEnum):
    PREPARED = "prepared"
    SENDING = "sending"
    ACKNOWLEDGED = "acknowledged"
    REJECTED = "rejected"
    TRANSPORT_FAILED = "transport_failed"
    RESULT_UNKNOWN = "result_unknown"
    RECONCILED = "reconciled"
    RECONCILIATION_REQUIRED = "reconciliation_required"


class OrderStatus(StrEnum):
    NOT_SENT = "not_sent"
    ACKNOWLEDGED = "acknowledged"
    WORKING = "working"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    RECONCILING = "reconciling"


class ReconciliationStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    MATCHED = "matched"
    CONFLICT = "conflict"
    ACKNOWLEDGED = "acknowledged"


class BrokerSimulationTransitionError(ValueError):
    """A caller attempted a state edge outside the closed state machine."""


_State = TypeVar("_State", bound=StrEnum)


def _exact_decimal(value: object, *, field: str) -> Decimal:
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


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class BrokerOrderDocument(_FrozenModel):
    instrument_id: UUID
    side: Literal["buy", "sell"]
    quantity: StrictInt = Field(gt=0)
    limit_price: Decimal
    currency: Literal["USD"] = "USD"
    order_type: Literal["limit"] = "limit"
    time_in_force: Literal["DAY"] = "DAY"
    session: Literal["regular"] = "regular"
    sort_key: int = Field(ge=0)

    @field_validator("quantity", mode="before")
    @classmethod
    def _quantity_is_whole_shares(cls, value: object) -> object:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("quantity must be positive whole shares")
        return value

    @field_validator("limit_price", mode="before")
    @classmethod
    def _price_is_exact(cls, value: object) -> Decimal:
        return _exact_decimal(value, field="limit_price")

    @model_validator(mode="after")
    def _price_uses_us_tick(self) -> BrokerOrderDocument:
        if self.limit_price <= 0:
            raise ValueError("limit price must be positive")
        tick = Decimal("0.0001") if self.limit_price < 1 else Decimal("0.01")
        if self.limit_price % tick != 0:
            raise ValueError("limit price must align to the deterministic tick")
        return self


class BrokerIntentDocument(_FrozenModel):
    proposal_group_id: UUID
    strategy_decision_id: UUID
    portfolio_snapshot_id: UUID
    evidence_hashes: tuple[Hash64, ...]
    risk_policy_hash: Hash64
    execution_model_hash: Hash64
    account_binding_id: UUID
    orders: tuple[BrokerOrderDocument, ...]
    total_notional: Decimal

    @field_validator("total_notional", mode="before")
    @classmethod
    def _notional_is_exact(cls, value: object) -> Decimal:
        return _exact_decimal(value, field="total_notional")

    @model_validator(mode="after")
    def _orders_are_canonical(self) -> BrokerIntentDocument:
        if not self.orders:
            raise ValueError("intent requires at least one order")
        expected = tuple(
            sorted(self.orders, key=lambda item: (item.sort_key, item.instrument_id.int))
        )
        if self.orders != expected:
            raise ValueError("orders must use canonical sort order")
        instrument_ids = tuple(order.instrument_id for order in self.orders)
        if len(instrument_ids) != len(set(instrument_ids)):
            raise ValueError("orders require unique instruments")
        if self.evidence_hashes != tuple(sorted(set(self.evidence_hashes))):
            raise ValueError("evidence hashes must be sorted and unique")
        expected_notional = sum(
            (order.limit_price * order.quantity for order in self.orders), Decimal(0)
        )
        if self.total_notional != expected_notional:
            raise ValueError("total notional must equal the canonical order batch")
        return self


class ApprovalDocument(_FrozenModel):
    intent_id: UUID
    intent_version: int = Field(ge=1)
    intent_body_hash: Hash64
    request_body_hash: Hash64
    actor_id: str = Field(min_length=1, max_length=255)
    idempotency_key: str = Field(min_length=1, max_length=255)
    approved_at: datetime
    expires_at: datetime

    @field_validator("approved_at", "expires_at")
    @classmethod
    def _time_is_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _expiry_follows_approval(self) -> ApprovalDocument:
        if self.expires_at <= self.approved_at:
            raise ValueError("approval expiry must be after approval")
        return self


def _canonical_value(value: object) -> object:
    if isinstance(value, BaseModel):
        return _canonical_value(value.model_dump())
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        rendered = decimal_to_string(value)
        if "." in rendered:
            rendered = rendered.rstrip("0").rstrip(".")
        return "0" if rendered == "-0" else rendered
    if isinstance(value, datetime):
        return require_utc(value).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if isinstance(value, float):
        raise TypeError("canonical broker documents reject binary floats")
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical broker document keys must be strings")
        return {key: _canonical_value(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_canonical_value(item) for item in value]
    return value


def canonical_document_hash(document: BaseModel | Mapping[str, Any]) -> str:
    """Hash the strict canonical JSON representation of an authority document."""
    content = canonical_json(_canonical_value(document))
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


_INTENT_EDGES = {
    IntentStatus.DRAFT: {IntentStatus.PREPARED, IntentStatus.REJECTED, IntentStatus.EXPIRED},
    IntentStatus.PREPARED: {
        IntentStatus.APPROVED,
        IntentStatus.REJECTED,
        IntentStatus.EXPIRED,
    },
    IntentStatus.APPROVED: {IntentStatus.EXPIRED, IntentStatus.DISPATCHING},
    IntentStatus.DISPATCHING: {
        IntentStatus.COMPLETED,
        IntentStatus.PARTIALLY_COMPLETED,
        IntentStatus.FAILED,
        IntentStatus.SUBMISSION_UNKNOWN,
        IntentStatus.RECONCILING,
    },
    IntentStatus.SUBMISSION_UNKNOWN: {
        IntentStatus.RECONCILED,
        IntentStatus.RECONCILING,
    },
    IntentStatus.RECONCILING: {
        IntentStatus.COMPLETED,
        IntentStatus.PARTIALLY_COMPLETED,
        IntentStatus.FAILED,
    },
}

_COMMAND_EDGES = {
    CommandStatus.PREPARED: {CommandStatus.SENDING},
    CommandStatus.SENDING: {
        CommandStatus.ACKNOWLEDGED,
        CommandStatus.REJECTED,
        CommandStatus.TRANSPORT_FAILED,
        CommandStatus.RESULT_UNKNOWN,
    },
    CommandStatus.RESULT_UNKNOWN: {
        CommandStatus.RECONCILED,
        CommandStatus.RECONCILIATION_REQUIRED,
    },
}

_ORDER_EDGES = {
    OrderStatus.NOT_SENT: {
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.REJECTED,
        OrderStatus.RECONCILING,
    },
    OrderStatus.ACKNOWLEDGED: {
        OrderStatus.WORKING,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
        OrderStatus.RECONCILING,
    },
    OrderStatus.WORKING: {
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
        OrderStatus.RECONCILING,
    },
    OrderStatus.PARTIALLY_FILLED: {
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.EXPIRED,
        OrderStatus.RECONCILING,
    },
    OrderStatus.RECONCILING: {
        OrderStatus.ACKNOWLEDGED,
        OrderStatus.WORKING,
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    },
}


def _require_transition(
    source: _State,
    target: _State,
    *,
    edges: Mapping[_State, Set[_State]],
    code: str,
) -> None:
    if target not in edges.get(source, set()):
        raise BrokerSimulationTransitionError(f"{code}: {source.value}->{target.value}")


def require_intent_transition(source: IntentStatus, target: IntentStatus) -> None:
    _require_transition(source, target, edges=_INTENT_EDGES, code="INTENT_TRANSITION_INVALID")


def require_command_transition(source: CommandStatus, target: CommandStatus) -> None:
    _require_transition(source, target, edges=_COMMAND_EDGES, code="COMMAND_TRANSITION_INVALID")


def require_order_transition(source: OrderStatus, target: OrderStatus) -> None:
    _require_transition(source, target, edges=_ORDER_EDGES, code="ORDER_TRANSITION_INVALID")


def effective_approval_expiry(
    *,
    prepared_at: datetime,
    quote_expires_at: datetime,
    capability_expires_at: datetime,
    session_close_at: datetime,
) -> datetime:
    """Return the earliest independent authority, including the five-minute ceiling."""
    prepared = require_utc(prepared_at)
    candidates = (
        prepared + timedelta(minutes=5),
        require_utc(quote_expires_at),
        require_utc(capability_expires_at),
        require_utc(session_close_at),
    )
    return min(candidates)
