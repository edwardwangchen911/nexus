"""Receipt-time Fosun classification hints for research routing only."""

import json
from datetime import UTC, datetime
from typing import Annotated, Literal, Self
from uuid import UUID

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

SOURCE: Literal["fosun_security_classification"] = "fosun_security_classification"
SCHEMA: Literal["fosun_security_classification_v1"] = "fosun_security_classification_v1"
RAW_SCHEMA = "fosun_gateway_market_snapshot_v1"
MAX_CANONICAL_BYTES = 16_384
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_ProviderText = Annotated[str, StringConstraints(strict=True, max_length=128)]


class _StrictDocument(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class FosunClassificationLevels(_StrictDocument):
    first: int | None = Field(default=None, strict=True)
    second: int | None = Field(default=None, strict=True)
    third: int | None = Field(default=None, strict=True)


class FosunFundAssetType(_StrictDocument):
    chs: _ProviderText | None = None
    cht: _ProviderText | None = None
    eng: _ProviderText | None = None


class FosunRawQuoteBinding(_StrictDocument):
    artifact_sha256: str = Field(pattern=_HASH_PATTERN)
    artifact_byte_length: int = Field(strict=True, gt=0, le=1024 * 1024)
    artifact_uri: str = Field(min_length=1, max_length=4096)
    gateway_request_id: UUID
    request_body_sha256: str = Field(pattern=_HASH_PATTERN)


class FosunSecurityClassificationDocument(_StrictDocument):
    schema_version: Literal["fosun_security_classification_v1"] = SCHEMA
    source: Literal["fosun_security_classification"] = SOURCE
    instrument_id: UUID
    provider_code: str
    provider_name: (
        Annotated[str, StringConstraints(strict=True, min_length=1, max_length=256)] | None
    ) = None
    levels: FosunClassificationLevels
    equity_profile_fields_complete: StrictBool
    net_asset_profile_fields_complete: StrictBool
    fund_asset_type: FosunFundAssetType
    routing_family: FosunSecurityFamily
    received_at: AwareDatetime
    available_at: AwareDatetime
    raw_quote: FosunRawQuoteBinding
    revision_semantics: Literal["local_content_hash"] = "local_content_hash"
    canonical_asset_type: None = None
    identity_authoritative: Literal[False] = False
    price_authority_eligible: Literal[False] = False
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True

    @field_validator("provider_code")
    @classmethod
    def _provider_code(cls, value: str) -> str:
        try:
            return _validated_code(value)
        except (TypeError, ValueError, RuntimeError) as error:
            raise ValueError("FOSUN_CLASSIFICATION_PROVIDER_CODE_INVALID") from error

    @model_validator(mode="after")
    def _binding_and_family(self) -> Self:
        require_utc(self.received_at)
        require_utc(self.available_at)
        if self.received_at != self.available_at:
            raise ValueError("FOSUN_CLASSIFICATION_CLOCK_MISMATCH")
        if self.available_at > datetime.now(UTC):
            raise ValueError("FOSUN_CLASSIFICATION_TIME_IN_FUTURE")
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
        if classify_fosun_security_family(evidence) != self.routing_family:
            raise ValueError("FOSUN_CLASSIFICATION_ROUTING_FAMILY_MISMATCH")
        if len(self.canonical_bytes()) > MAX_CANONICAL_BYTES:
            raise ValueError("FOSUN_CLASSIFICATION_DOCUMENT_TOO_LARGE")
        return self

    @property
    def provider_object_id(self) -> str:
        return f"classification:{self.instrument_id}:{self.provider_code}"

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()

    def tool_payload(self) -> dict[str, object]:
        return {
            "provider": "fosun",
            "provider_name": self.provider_name,
            "provider_code": self.provider_code,
            "levels": self.levels.model_dump(mode="json"),
            "equity_profile_fields_complete": self.equity_profile_fields_complete,
            "net_asset_profile_fields_complete": self.net_asset_profile_fields_complete,
            "fund_asset_type": self.fund_asset_type.model_dump(mode="json"),
            "routing_family": self.routing_family,
            "canonical_asset_type": None,
            "identity_authoritative": False,
        }
