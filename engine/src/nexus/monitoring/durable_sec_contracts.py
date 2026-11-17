"""Pure, immutable contracts; discovery grants neither notification nor research authority."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from nexus.domain.time import require_utc
from nexus.research.sec_disclosure_index import SecIndexEntry

SecForm = Literal["8-K", "8-K/A"]
RuleState = Literal["active", "paused", "revoked"]
DiscoveryReason = Literal[
    "DURABLE_SEC_RULE_INACTIVE",
    "DURABLE_SEC_SOURCE_INVALID",
    "DURABLE_SEC_TIME_INVALID",
    "DURABLE_SEC_SOURCE_STALE",
    "DURABLE_SEC_HISTORY_INVALID",
    "DURABLE_SEC_EVENT_CONFLICT",
    "DURABLE_SEC_EVENT_TIME_INVALID",
    "DURABLE_SEC_COVERAGE_CONFLICT",
    "DURABLE_SEC_COVERAGE_INCOMPLETE",
]


class DurableSecPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    account_id: UUID
    instrument_id: UUID
    issuer_cik: str = Field(pattern=r"^[0-9]{10}$")
    forms: tuple[SecForm, ...] = ("8-K", "8-K/A")
    effective_at: AwareDatetime
    policy_version: Literal["durable_sec_v1"] = "durable_sec_v1"

    @field_validator("effective_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @field_validator("issuer_cik")
    @classmethod
    def nonzero_issuer(cls, value: str) -> str:
        if int(value) == 0:
            raise ValueError("DURABLE_SEC_POLICY_INVALID")
        return value

    @field_validator("forms")
    @classmethod
    def exact_forms(cls, value: tuple[SecForm, ...]) -> tuple[SecForm, ...]:
        if not value or len(set(value)) != len(value):
            raise ValueError("DURABLE_SEC_POLICY_INVALID")
        return tuple(sorted(value))


class DiscoveryEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    event_key: str
    entry: SecIndexEntry
    classification: Literal["new", "known", "historical", "unknown", "out_of_scope"]
    reason: DiscoveryReason | None = None


class DiscoveryResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    account_id: UUID
    instrument_id: UUID
    issuer_cik: str
    forms: tuple[SecForm, ...]
    policy_version: Literal["durable_sec_v1"] = "durable_sec_v1"
    result: Literal["true", "false", "unknown"]
    coverage: Literal["complete", "partial", "unknown"]
    # An interval is only reported for a valid, current shared source.
    interval_start: AwareDatetime | None
    interval_end: AwareDatetime | None
    events: tuple[DiscoveryEvent, ...] = ()
    reason: DiscoveryReason | None = None
