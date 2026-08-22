"""Public, redacted schemas for deterministic integration status."""

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

type ProviderName = Literal["fosun", "alpaca", "sec", "fred", "openai"]
type IntegrationMode = Literal["disabled", "fixture", "read_only"]


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IntegrationStatusResponse(_StrictSchema):
    """A provider's safe-to-display capability and fixture state."""

    provider: Literal["fosun", "alpaca", "sec", "fred", "openai"]
    mode: Literal["disabled", "fixture", "read_only"]
    quality: Literal["PASS", "PARTIAL", "FAIL"]
    as_of: datetime | None
    capability_expires_at: datetime | None
    contract_hash: str | None
    next_action: str | None

    @field_validator("as_of", "capability_expires_at")
    @classmethod
    def require_timezone_aware(cls, value: datetime | None) -> datetime | None:
        """Reject naïve external times before they enter the public contract."""
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(UTC) if value is not None else None


class IntegrationModeChangeRequest(_StrictSchema):
    """The only safe mode values accepted by the engine contract."""

    mode: Literal["disabled", "fixture", "read_only"]


class IntegrationConnectivityTestRequest(_StrictSchema):
    """A deliberately empty non-trading fixture connectivity request."""


class SuccessMetaResponse(_StrictSchema):
    """The stable public success metadata shape."""

    request_id: UUID
    schema_version: Literal["1"] = "1"
    as_of: datetime

    @field_validator("as_of")
    @classmethod
    def require_timezone_aware(cls, value: datetime) -> datetime:
        """Keep metadata dates compatible with the status timestamp contract."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(UTC)


class IntegrationStatusEnvelope(_StrictSchema):
    """A single integration status in the public success envelope."""

    data: IntegrationStatusResponse
    meta: SuccessMetaResponse


class IntegrationStatusListEnvelope(_StrictSchema):
    """A complete deterministic integration status list."""

    data: list[IntegrationStatusResponse]
    meta: SuccessMetaResponse


class IntegrationConnectivityTestResponse(IntegrationStatusEnvelope):
    """The response to a fixture-only connectivity refresh."""


class IntegrationModeChangeResponse(IntegrationStatusEnvelope):
    """The response to a safe persisted mode change."""
