"""Closed and redacted contracts for the offline broker-simulation workbench."""

from datetime import UTC, datetime
from typing import Literal, Self
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _StrictBrokerSchema(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BrokerIntentPrepareRequest(_StrictBrokerSchema):
    proposal_group_id: UUID | None = None
    proposal_id: UUID | None = None
    agent_paper_cycle_id: UUID | None = None
    account_binding_id: UUID | None = None
    portfolio_snapshot_id: UUID | None = None

    @model_validator(mode="after")
    def _requires_exactly_one_source(self) -> Self:
        proposal = (self.proposal_group_id, self.proposal_id)
        agent_paper = (
            self.agent_paper_cycle_id,
            self.account_binding_id,
            self.portfolio_snapshot_id,
        )
        if (all(value is not None for value in proposal) and not any(agent_paper)) or (
            all(value is not None for value in agent_paper) and not any(proposal)
        ):
            return self
        raise ValueError("exactly one broker intent source is required")


class BrokerIntentApprovalRequest(_StrictBrokerSchema):
    version: int = Field(ge=1)


class BrokerIntentRejectRequest(_StrictBrokerSchema):
    version: int = Field(ge=1)
    reason: Literal["user_rejected", "authority_changed"]


class BrokerModifyRequest(_StrictBrokerSchema):
    expected_event_sequence: int = Field(ge=0)
    reason: Literal["replace_price", "replace_quantity"]


class BrokerCancelRequest(_StrictBrokerSchema):
    expected_event_sequence: int = Field(ge=0)
    reason: Literal["user_cancel", "risk_cancel"]


class BrokerReconciliationAcknowledgeRequest(_StrictBrokerSchema):
    pass


class BrokerCapabilityResponse(_StrictBrokerSchema):
    status: Literal["offline_ready", "read_only_activated", "simulation_write_activated"]
    external_activation: Literal["external_activation_blocked", "externally_activated"]
    live_trading: Literal[False] = False
    simulation_write_enabled: bool


class BrokerAccountResponse(_StrictBrokerSchema):
    binding_id: UUID
    redacted_label: str = Field(min_length=1, max_length=100)
    reconciliation_status: Literal["pending", "running", "matched", "conflict", "acknowledged"]
    locked: bool
    capability_expires_at: datetime

    @field_validator("capability_expires_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("capability expiry must be timezone-aware")
        return value.astimezone(UTC)


class BrokerOrderResponse(_StrictBrokerSchema):
    order_id: UUID
    instrument_id: UUID
    side: Literal["buy", "sell"]
    quantity: DecimalString
    limit_price: DecimalString
    currency: Literal["USD"]
    order_type: Literal["limit"]
    time_in_force: Literal["DAY"]
    session: Literal["regular"]
    status: Literal[
        "not_sent",
        "acknowledged",
        "working",
        "partially_filled",
        "filled",
        "cancelled",
        "rejected",
        "expired",
        "reconciling",
    ]


class BrokerIntentResponse(_StrictBrokerSchema):
    intent_id: UUID
    status: Literal[
        "draft",
        "prepared",
        "approved",
        "rejected",
        "expired",
        "dispatching",
        "completed",
        "partially_completed",
        "failed",
        "submission_unknown",
        "reconciling",
        "reconciled",
    ]
    version: int = Field(ge=1)
    body_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    total_notional: DecimalString
    prepared_at: datetime
    expires_at: datetime
    evidence_hashes: tuple[str, ...]
    orders: tuple[BrokerOrderResponse, ...]

    @field_validator("prepared_at", "expires_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("intent time must be timezone-aware")
        return value.astimezone(UTC)


class BrokerEventResponse(_StrictBrokerSchema):
    sequence: int = Field(ge=1)
    order_id: UUID
    source: Literal["gateway", "webhook", "poll", "recovery"]
    status: str = Field(min_length=1, max_length=32)
    occurred_at: datetime

    @field_validator("occurred_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("event time must be timezone-aware")
        return value.astimezone(UTC)


class BrokerReconciliationResponse(_StrictBrokerSchema):
    run_id: UUID
    status: Literal["pending", "running", "matched", "conflict", "acknowledged"]
    diff_count: int = Field(ge=0)
    started_at: datetime
    completed_at: datetime | None

    @field_validator("started_at", "completed_at")
    @classmethod
    def _utc(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("reconciliation time must be timezone-aware")
        return None if value is None else value.astimezone(UTC)


class BrokerAvailableActionsResponse(_StrictBrokerSchema):
    can_prepare: bool
    can_approve: bool
    can_reject: bool
    can_dispatch: Literal[False] = False
    can_modify: Literal[False] = False
    can_cancel: Literal[False] = False


class BrokerSimulationResponse(_StrictBrokerSchema):
    capability: BrokerCapabilityResponse
    account: BrokerAccountResponse | None
    intent: BrokerIntentResponse | None
    events: tuple[BrokerEventResponse, ...] = Field(max_length=200)
    reconciliation: BrokerReconciliationResponse | None
    actions: BrokerAvailableActionsResponse


class BrokerActionResponse(_StrictBrokerSchema):
    resource_id: UUID
    action: Literal["prepare", "approve", "reject", "acknowledge"]
    status: str = Field(min_length=1, max_length=32)
    version: int | None = Field(default=None, ge=1)
    replayed: bool
    dispatch_created: Literal[False] = False


class BrokerSimulationEnvelope(_StrictBrokerSchema):
    data: BrokerSimulationResponse
    meta: SuccessMetaResponse


class BrokerIntentEnvelope(_StrictBrokerSchema):
    data: BrokerIntentResponse
    meta: SuccessMetaResponse


class BrokerActionEnvelope(_StrictBrokerSchema):
    data: BrokerActionResponse
    meta: SuccessMetaResponse
