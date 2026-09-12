"""Provider-scoped Fosun discovery evidence for unresolved US holdings."""

import json
from datetime import UTC, datetime
from typing import Annotated, Literal, Self

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StringConstraints,
    field_validator,
    model_validator,
)

from nexus.domain.time import require_utc
from nexus.providers.market.fosun_gateway import (
    FosunSecurityClassification,
    FosunSecurityFamily,
    _validated_code,
    classify_fosun_security_family,
)
from nexus.research.fosun_security_classification import (
    FosunClassificationLevels,
    FosunFundAssetType,
    FosunRawQuoteBinding,
)

SOURCE: Literal["fosun_instrument_discovery"] = "fosun_instrument_discovery"
SCHEMA: Literal["fosun_instrument_discovery_v1"] = "fosun_instrument_discovery_v1"
MAX_CANONICAL_BYTES = 16_384

FosunIdentityRoute = Literal[
    "current_held_common_stock",
    "current_held_fund_or_structured",
    "manual_review",
]


def recommended_identity_route(family: FosunSecurityFamily) -> FosunIdentityRoute:
    """Map provider classification to an identity workflow, never a canonical type."""
    if family == "company_equity_like":
        return "current_held_common_stock"
    if family == "fund_or_structured_like":
        return "current_held_fund_or_structured"
    return "manual_review"


class FosunInstrumentDiscoveryDocument(BaseModel):
    """Immutable raw-bound discovery for a holding without canonical identity."""

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    schema_version: Literal["fosun_instrument_discovery_v1"] = SCHEMA
    source: Literal["fosun_instrument_discovery"] = SOURCE
    provider_code: str
    provider_name: (
        Annotated[str, StringConstraints(strict=True, min_length=1, max_length=256)] | None
    ) = None
    market_scope: Literal["US"] = "US"
    currency: Literal["USD"] = "USD"
    provider_product_type: Annotated[int, Field(strict=True)] | None = None
    levels: FosunClassificationLevels
    equity_profile_fields_complete: StrictBool
    net_asset_profile_fields_complete: StrictBool
    fund_asset_type: FosunFundAssetType
    routing_family: FosunSecurityFamily
    recommended_identity_route: FosunIdentityRoute
    holding_observed_at: AwareDatetime
    received_at: AwareDatetime
    available_at: AwareDatetime
    raw_quote: FosunRawQuoteBinding
    canonical_instrument_id: None = None
    canonical_asset_type: None = None
    identity_authoritative: Literal[False] = False
    decision_eligible: Literal[False] = False
    execution_locked: Literal[True] = True

    @field_validator("provider_code")
    @classmethod
    def _provider_code(cls, value: str) -> str:
        try:
            return _validated_code(value)
        except (TypeError, ValueError, RuntimeError) as error:
            raise ValueError("FOSUN_DISCOVERY_PROVIDER_CODE_INVALID") from error

    @model_validator(mode="after")
    def _validate_evidence(self) -> Self:
        require_utc(self.holding_observed_at)
        require_utc(self.received_at)
        require_utc(self.available_at)
        if self.holding_observed_at > self.received_at:
            raise ValueError("FOSUN_DISCOVERY_HOLDING_CLOCK_INVALID")
        if self.received_at != self.available_at:
            raise ValueError("FOSUN_DISCOVERY_CLOCK_MISMATCH")
        if self.available_at > datetime.now(UTC):
            raise ValueError("FOSUN_DISCOVERY_TIME_IN_FUTURE")
        evidence = FosunSecurityClassification(
            level_1=self.levels.first,
            level_2=self.levels.second,
            level_3=self.levels.third,
            equity_profile_fields_complete=self.equity_profile_fields_complete,
            net_asset_profile_fields_complete=self.net_asset_profile_fields_complete,
            fund_asset_type_chs=self.fund_asset_type.chs,
            fund_asset_type_cht=self.fund_asset_type.cht,
            fund_asset_type_eng=self.fund_asset_type.eng,
        )
        family = classify_fosun_security_family(evidence)
        if family != self.routing_family:
            raise ValueError("FOSUN_DISCOVERY_ROUTING_FAMILY_MISMATCH")
        if recommended_identity_route(family) != self.recommended_identity_route:
            raise ValueError("FOSUN_DISCOVERY_IDENTITY_ROUTE_MISMATCH")
        if len(self.canonical_bytes()) > MAX_CANONICAL_BYTES:
            raise ValueError("FOSUN_DISCOVERY_DOCUMENT_TOO_LARGE")
        return self

    @property
    def provider_object_id(self) -> str:
        return f"discovery:{self.provider_code}"

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
