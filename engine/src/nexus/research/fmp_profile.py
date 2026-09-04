"""Current vendor profile background, not historical or deterministic financial authority."""

import json
from typing import Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.time import require_utc
from nexus.providers.fmp_profile import MAX_RAW_BYTES, ORIGIN, PATH, validate_symbol

SOURCE = "fmp_company_profile"
SCHEMA = "fmp_company_profile_v1"
RAW_SCHEMA = "fmp_profile_raw_v1"
RECEIPT_SCHEMA = "fmp_profile_receipt_v1"


class FmpCompanyProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    instrument_id: UUID
    symbol: str
    company_name: str | None = Field(default=None, max_length=1000)
    description: str | None = Field(default=None, max_length=16000)
    cik: str | None = Field(default=None, max_length=1000)
    isin: str | None = Field(default=None, max_length=1000)
    cusip: str | None = Field(default=None, max_length=1000)
    industry: str | None = Field(default=None, max_length=1000)
    sector: str | None = Field(default=None, max_length=1000)
    source_url: str
    received_at: AwareDatetime
    available_at: AwareDatetime
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_byte_length: int = Field(strict=True, gt=0, le=MAX_RAW_BYTES)
    receipt_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt_byte_length: int = Field(strict=True, gt=0, le=8192)
    source_published_at: None = None
    valid_until: None = None
    sector_taxonomy: None = None
    revision_semantics: Literal["local_content_hash"] = "local_content_hash"
    sector_risk_eligible: Literal[False] = False
    financial_fact_eligible: Literal[False] = False
    price_authority_eligible: Literal[False] = False
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True

    @field_validator("symbol")
    @classmethod
    def _symbol(cls, value: str) -> str:
        return validate_symbol(value)

    @model_validator(mode="after")
    def _identity_and_clocks(self) -> Self:
        require_utc(self.received_at)
        require_utc(self.available_at)
        if self.received_at != self.available_at or self.source_url != (
            f"{ORIGIN}{PATH}?symbol={self.symbol}"
        ):
            raise ValueError("FMP_PROFILE_BINDING_INVALID")
        return self

    @property
    def provider_object_id(self) -> str:
        return f"profile:{self.instrument_id}:{self.symbol}"

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
