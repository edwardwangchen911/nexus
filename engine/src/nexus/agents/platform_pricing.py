"""Immutable Sol price data, not a sender, model registry or funding authority.

Prices are supplied from a verified source record, rather than a daily code
whitelist. The seven-day window is a local freshness ceiling, not a provider
price guarantee. Historical accounting deliberately does not consult a clock.
The registration/dispatch boundary must verify source evidence and eligibility.
"""

import hashlib
import json
from datetime import datetime, timedelta
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.time import require_utc

Rate = Annotated[str, Field(pattern=r"^(0|[1-9][0-9]{0,11})(\.[0-9]{1,6})?$")]


class SolPriceRecord(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    schema_version: Literal["platform-sol-pricing-v1"]
    model_id: Literal["gpt-6.1-sol"]
    service_tier: Literal["default"]
    cache_mode: Literal["implicit"]
    cache_ttl: Literal["30m"]
    currency: Literal["USD"]
    source_url: Literal["https://developers.openai.com/api/docs/pricing?latest-pricing=standard"]
    source_evidence_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
    verified_at: datetime
    expires_at: datetime
    ordinary_input_usd_per_million: Rate
    cache_read_usd_per_million: Rate
    cache_write_30m_usd_per_million: Rate
    output_usd_per_million: Rate

    @field_validator("verified_at", "expires_at", mode="before")
    @classmethod
    def utc_instant(cls, value: object) -> datetime:
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if not isinstance(value, datetime):
            raise ValueError("PLATFORM_PRICE_RECORD_INVALID")
        return require_utc(value)

    @model_validator(mode="after")
    def bounded_window(self) -> "SolPriceRecord":
        if not timedelta(0) < self.expires_at - self.verified_at <= timedelta(days=7):
            raise ValueError("PLATFORM_PRICE_RECORD_INVALID")
        return self

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    @property
    def version(self) -> str:
        return "platform-sol-price-v1-" + self.content_hash

    @property
    def token_picodollars(self) -> tuple[int, int, int, int]:
        # Six decimal places per million map exactly onto the ledger's 12 USD
        # decimal places. Integer arithmetic is independent of Decimal context.
        def scaled(rate: str) -> int:
            whole, _, fraction = rate.partition(".")
            return int(whole) * 1_000_000 + int(fraction.ljust(6, "0"))

        return (
            scaled(self.ordinary_input_usd_per_million),
            scaled(self.cache_read_usd_per_million),
            scaled(self.cache_write_30m_usd_per_million),
            scaled(self.output_usd_per_million),
        )

    def require_current(self, *, now: datetime, deadline: datetime) -> None:
        """Send/preparation check only; never invoke while reading old usage."""
        now, deadline = require_utc(now), require_utc(deadline)
        if not self.verified_at <= now < deadline <= self.expires_at:
            raise ValueError("PLATFORM_PRICE_WINDOW_INVALID")


def sol_price_record(value: object) -> SolPriceRecord:
    """Revalidate even model instances; a copied/constructed model is not proof."""
    if isinstance(value, SolPriceRecord):
        value = value.model_dump(mode="json")
    try:
        return SolPriceRecord.model_validate(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError("PLATFORM_PRICE_RECORD_INVALID") from exc
