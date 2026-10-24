"""Closed, versioned price-review conditions and observations."""

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from nexus.domain.decimal import DecimalString
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc


class MonitoringModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class PriceReviewCondition(MonitoringModel):
    """A price review threshold; neither an order nor proof a thesis is false."""

    signal: Literal["schwab.l1.bid_ask_midpoint"]
    operator: Literal["gte", "lte"]
    threshold: DecimalString
    unit: Literal["currency"]
    currency: Literal["USD"]
    purpose: Literal["review"]
    source: Literal["schwab"]
    feed: Literal["l1"]
    price_basis: Literal["unadjusted_bid_ask_midpoint"]
    max_age_seconds: Literal[60]

    @field_validator("threshold")
    @classmethod
    def _positive_decimal(cls, value: str) -> str:
        parsed = Decimal(value)
        exponent = parsed.as_tuple().exponent
        if (parsed <= 0 or len(parsed.as_tuple().digits) > 38
                or not isinstance(exponent, int) or exponent < -12):
            raise ValueError("MONITOR_THRESHOLD_INVALID")
        return value

    @property
    def semantic_hash(self) -> str:
        # Deliberately excludes plan generation and model-provided labels.
        payload = self.model_dump(mode="json")
        text = format(Decimal(self.threshold), "f")
        payload["threshold"] = text.rstrip("0").rstrip(".") if "." in text else text
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


class PriceObservation(MonitoringModel):
    """An immutable source observation; authority is checked by the DB loader."""

    instrument_id: UUID
    quote_id: UUID
    artifact_id: UUID
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source: str
    feed: str
    currency: str
    price_basis: str
    delivery_mode: str
    evidence_status: str
    bid: Decimal | None
    ask: Decimal | None
    observed_at: datetime
    available_at: datetime
    received_at: datetime

    @field_validator("observed_at", "available_at", "received_at")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @field_validator("bid", "ask", mode="before")
    @classmethod
    def _no_float(cls, value: object) -> object:
        if isinstance(value, float | bool):
            raise ValueError("MONITOR_PRICE_INVALID")
        return value


class PriceMonitoringProposal(MonitoringModel):
    """One proposed review condition; it requires independent bear and audit approval."""

    schema_version: Literal["price_monitor_proposal_v1"] = "price_monitor_proposal_v1"
    condition: PriceReviewCondition
    rationale: str = Field(min_length=1, max_length=500)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=4)
    review_due_at: datetime
    valid_until: datetime

    @field_validator("review_due_at", "valid_until")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


class PriceMonitoringReview(MonitoringModel):
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision: Literal["accepted", "rejected"]
    rationale: str = Field(min_length=1, max_length=500)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=4)
