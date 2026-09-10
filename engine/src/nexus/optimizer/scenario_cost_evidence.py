"""Versioned public regulatory-fee summaries for deterministic scenario analysis."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Literal

from nexus.domain.time import require_utc


@dataclass(frozen=True)
class RegulatoryFeeSource:
    authority: Literal["SEC", "FINRA", "FOSUN"]
    document_id: str
    url: str
    fact: str


@dataclass(frozen=True)
class RegulatoryFeeReference:
    version: str
    evidence_hash: str
    available_at: datetime
    coverage_start: datetime
    verified_through: datetime
    sources: tuple[RegulatoryFeeSource, ...]
    source_material_status: Literal["normalized_summary_only"]
    customer_charge_status: Literal["broker_pass_through_unverified"]


_VERSION = "us_regulatory_fee_reference_2026_verified_20260911_v1"
_AVAILABLE_AT = datetime(2026, 2, 28, tzinfo=UTC)
_COVERAGE_START = datetime(2026, 4, 4, tzinfo=UTC)
_VERIFIED_THROUGH = datetime(2026, 9, 11, 23, 59, 59, tzinfo=UTC)
_SOURCES = (
    RegulatoryFeeSource(
        authority="SEC",
        document_id="FY2026 Section 31 Fee Rate Advisory; Release 34-104909",
        url="https://www.sec.gov/rules-regulations/fee-rate-advisories/2026-2",
        fact="covered_sales_rate_usd_per_million=20.60;effective=2026-04-04",
    ),
    RegulatoryFeeSource(
        authority="FINRA",
        document_id="SR-FINRA-2024-019 fee adjustment schedule",
        url=(
            "https://www.finra.org/rules-guidance/rule-filings/"
            "sr-finra-2024-019/fee-adjustment-schedule"
        ),
        fact="covered_equity_taf_usd_per_share=0.000195;trade_cap_usd=9.79;effective=2026-01-01",
    ),
    RegulatoryFeeSource(
        authority="FOSUN",
        document_id="Fosun International Securities client agreement v9",
        url=("https://fosunxcf.com/static/download/20240528/a0a86354f5938a75878c389b6bdaac6a.pdf"),
        fact="customer_commissions_and_charges_are_broker_notified",
    ),
)
_HASH_PAYLOAD = {
    "version": _VERSION,
    "available_at": _AVAILABLE_AT.isoformat(),
    "coverage_start": _COVERAGE_START.isoformat(),
    "verified_through": _VERIFIED_THROUGH.isoformat(),
    "sources": [asdict(source) for source in _SOURCES],
    "source_material_status": "normalized_summary_only",
    "customer_charge_status": "broker_pass_through_unverified",
}
_EVIDENCE_HASH = hashlib.sha256(
    json.dumps(_HASH_PAYLOAD, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
_REFERENCE = RegulatoryFeeReference(
    version=_VERSION,
    evidence_hash=_EVIDENCE_HASH,
    available_at=_AVAILABLE_AT,
    coverage_start=_COVERAGE_START,
    verified_through=_VERIFIED_THROUGH,
    sources=_SOURCES,
    source_material_status="normalized_summary_only",
    customer_charge_status="broker_pass_through_unverified",
)


def regulatory_fee_reference(as_of: datetime) -> RegulatoryFeeReference | None:
    """Return the normalized summary only inside its manually verified interval."""
    valuation_time = require_utc(as_of)
    if not _COVERAGE_START <= valuation_time <= _VERIFIED_THROUGH:
        return None
    return _REFERENCE
