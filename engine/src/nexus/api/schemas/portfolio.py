"""Strict redacted contracts for broker-backed portfolio reads and sync admission."""

from collections.abc import Mapping, Sequence
from typing import Annotated, Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

type PortfolioKind = Literal["real_read_only", "internal_paper", "shadow"]
type SnapshotStatus = Literal["READY", "PARTIAL", "STALE", "RECONCILIATION_REQUIRED"]
type DisplayReadiness = Literal["DISPLAY_READY", "DISPLAY_UNAVAILABLE"]
type DecisionReadiness = Literal[
    "DECISION_READY", "RECONCILIATION_PENDING", "DATA_STALE", "RECONCILIATION_REQUIRED"
]
type SyncRunStatus = Literal[
    "ACTIVE", "READY", "PARTIAL", "STALE", "RECONCILIATION_REQUIRED", "FAILED"
]
type CurrencyCode = Annotated[str, StringConstraints(pattern=r"^[A-Z]{3}$")]
type CashFlowQuality = Literal["PASS", "PARTIAL", "NOT_RUN"]
type SnapshotTimeBasis = Literal["provider_event", "gateway_observed", "unknown"]


def snapshot_time_basis(observations: Sequence[Mapping[str, object]]) -> SnapshotTimeBasis:
    """Expose stored time provenance without reinterpreting legacy snapshots."""
    bases = {item.get("time_basis") for item in observations}
    if bases == {"gateway_observed"}:
        return "gateway_observed"
    if bases == {"provider_event"}:
        return "provider_event"
    return "unknown"


def derive_snapshot_readiness(
    status: SnapshotStatus | str,
    *,
    has_display_facts: bool,
    cash_flow_quality: CashFlowQuality | str,
) -> tuple[DisplayReadiness, DecisionReadiness]:
    """Separate factual display availability from decision-grade readiness."""
    display: DisplayReadiness = "DISPLAY_READY" if has_display_facts else "DISPLAY_UNAVAILABLE"
    decisions: dict[str, DecisionReadiness] = {
        "READY": ("DECISION_READY" if cash_flow_quality == "PASS" else "RECONCILIATION_PENDING"),
        "PARTIAL": "RECONCILIATION_PENDING",
        "STALE": "DATA_STALE",
        "RECONCILIATION_REQUIRED": "RECONCILIATION_REQUIRED",
    }
    try:
        return display, decisions[status]
    except KeyError as exc:
        raise ValueError(f"unsupported snapshot status: {status!r}") from exc


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BrokerSyncCreateRequest(_StrictSchema):
    portfolio_id: UUID


class PortfolioListItemResponse(_StrictSchema):
    portfolio_id: UUID
    name: str = Field(min_length=1, max_length=100)
    kind: Literal["real_read_only"]
    coverage_scope: Literal["broker_account"]
    provider: str = Field(min_length=1, max_length=50)
    latest_snapshot_status: SnapshotStatus | None
    latest_snapshot_as_of: AwareDatetime | None
    original_currencies: tuple[CurrencyCode, ...] = Field(max_length=32)
    last_sync_run_status: SyncRunStatus | None


class InternalPaperPortfolioListItemResponse(_StrictSchema):
    portfolio_id: UUID
    name: str = Field(min_length=1, max_length=100)
    kind: Literal["internal_paper"]
    coverage_scope: Literal["internal_paper_account"]
    source: Literal["internal_ledger"]
    currency: Literal["USD"]
    account_model: Literal["cash"]
    position_model: Literal["long_only"]
    data_state: Literal["READY", "ERROR"]
    projection_error_code: Literal["PAPER_PROJECTION_INVALID"] | None
    nav: DecimalString | None
    available_cash: DecimalString | None
    reserved_cash: DecimalString | None
    as_of: AwareDatetime | None


type PortfolioListItem = Annotated[
    PortfolioListItemResponse | InternalPaperPortfolioListItemResponse,
    Field(discriminator="kind"),
]


class PortfolioPageResponse(_StrictSchema):
    items: tuple[PortfolioListItem, ...] = Field(max_length=50)
    next_cursor: str | None


class PortfolioPageEnvelope(_StrictSchema):
    data: PortfolioPageResponse
    meta: SuccessMetaResponse


class CashBucketResponse(_StrictSchema):
    currency: CurrencyCode
    cash_balance: DecimalString
    available_cash: DecimalString
    provider_as_of: AwareDatetime | None


type PositionResearchStatus = Literal[
    "not_started",
    "prepared",
    "running",
    "validated",
    "audited",
    "synthesized",
    "completed",
    "failed",
    "abstained",
]


class PositionResearchResponse(_StrictSchema):
    """Current read-only research index, not a change to the broker snapshot."""

    read_at: AwareDatetime
    status: PositionResearchStatus
    run_id: UUID | None = None
    data_snapshot_id: UUID | None = None
    decision_time: AwareDatetime | None = None
    quality_status: Literal["complete", "partial", "failed"] | None = None
    summary: str | None = Field(default=None, max_length=400)
    uncertainties: tuple[Annotated[str, StringConstraints(max_length=400)], ...] = Field(
        default=(), max_length=3
    )
    newer_inputs_available: bool = False


class PositionResponse(_StrictSchema):
    instrument_id: UUID
    symbol: str | None = Field(min_length=1, max_length=32)
    exchange: str = Field(min_length=1, max_length=20)
    currency: CurrencyCode
    quantity: DecimalString
    available_quantity: DecimalString
    frozen_quantity: DecimalString
    avg_cost: DecimalString | None
    diluted_cost: DecimalString | None
    current_price: DecimalString | None
    provider_as_of: AwareDatetime | None
    read_only: Literal[True]
    research: PositionResearchResponse | None = None


class OpenOrderResponse(_StrictSchema):
    broker_order_snapshot_id: UUID
    instrument_id: UUID
    symbol: str | None = Field(min_length=1, max_length=32)
    currency: CurrencyCode
    side: str = Field(min_length=1, max_length=10)
    quantity: DecimalString
    filled_quantity: DecimalString
    price: DecimalString | None
    status: str = Field(min_length=1, max_length=30)
    updated_at: AwareDatetime | None
    read_only: Literal[True]


class PortfolioSnapshotResponse(_StrictSchema):
    portfolio_snapshot_id: UUID
    portfolio_id: UUID
    coverage_scope: Literal["broker_account"]
    status: SnapshotStatus
    display_readiness: DisplayReadiness
    decision_readiness: DecisionReadiness
    cash_flow_quality: CashFlowQuality
    cash_flow_count: int = Field(ge=0)
    execution_locked: Literal[True]
    cash: tuple[CashBucketResponse, ...] = Field(max_length=32)
    positions: tuple[PositionResponse, ...] = Field(max_length=2000)
    open_orders: tuple[OpenOrderResponse, ...] = Field(max_length=2000)
    converted_view: None
    as_of: AwareDatetime | None
    time_basis: SnapshotTimeBasis = "unknown"


class PortfolioSnapshotEnvelope(_StrictSchema):
    data: PortfolioSnapshotResponse
    meta: SuccessMetaResponse


class BrokerSyncRunResponse(_StrictSchema):
    broker_sync_run_id: UUID
    portfolio_id: UUID
    status: SyncRunStatus
    trigger_kind: Literal["MANUAL", "AUTOMATIC"]
    accepted_at: AwareDatetime
    completed_at: AwareDatetime | None
    attempt_count: int = Field(ge=0)
    cash_flow_quality: CashFlowQuality
    failure_category: Literal["SYNC_FAILED"] | None


class BrokerSyncRunEnvelope(_StrictSchema):
    data: BrokerSyncRunResponse
    meta: SuccessMetaResponse
