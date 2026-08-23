"""Canonical, database-authoritative real-account advisory policy."""

import json
from datetime import UTC
from decimal import Decimal
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import ConfigDict, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.models import _DiagnosticModel
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.diagnostics import DiagnosticPolicy

REAL_ADVISORY_POLICY_ID = UUID("801f00b8-f963-5164-a6e7-34c4d9955d31")
REAL_ADVISORY_POLICY_KEY = "real_advisory"
REAL_ADVISORY_POLICY_VERSION = "real_advisory_policy_v1"
REAL_ADVISORY_POLICY_PAYLOAD: dict[str, str] = {
    "single_name_warning": "0.15",
    "single_name_critical": "0.30",
    "top3_warning": "0.60",
    "sector_warning": "0.30",
    "sector_critical": "0.50",
    "currency_warning": "0.80",
    "correlation_cluster_warning": "0.50",
    "cash_minimum": "0.10",
    "drawdown_warning": "0.08",
    "drawdown_critical": "0.15",
    "days_to_exit_warning": "2",
    "days_to_exit_critical": "5",
}


class RealAdvisoryPolicy(_DiagnosticModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    version: Literal["real_advisory_policy_v1"] = "real_advisory_policy_v1"
    single_name_warning: Decimal = Decimal("0.15")
    single_name_critical: Decimal = Decimal("0.30")
    top3_warning: Decimal = Decimal("0.60")
    sector_warning: Decimal = Decimal("0.30")
    sector_critical: Decimal = Decimal("0.50")
    currency_warning: Decimal = Decimal("0.80")
    correlation_cluster_warning: Decimal = Decimal("0.50")
    cash_minimum: Decimal = Decimal("0.10")
    drawdown_warning: Decimal = Decimal("0.08")
    drawdown_critical: Decimal = Decimal("0.15")
    days_to_exit_warning: Decimal = Decimal("2")
    days_to_exit_critical: Decimal = Decimal("5")
    authority_refs: tuple[EvidenceRef, ...] = ()

    @model_validator(mode="after")
    def _canonical_values(self) -> "RealAdvisoryPolicy":
        actual = {key: str(getattr(self, key)) for key in REAL_ADVISORY_POLICY_PAYLOAD}
        if actual != REAL_ADVISORY_POLICY_PAYLOAD:
            raise ValueError("real advisory policy must use canonical v1 thresholds")
        return self

    @classmethod
    def v1(
        cls, *, authority_refs: tuple[EvidenceRef, ...] = ()
    ) -> "RealAdvisoryPolicy":
        return cls(authority_refs=authority_refs)


async def load_real_advisory_policy_v1(session: AsyncSession) -> RealAdvisoryPolicy:
    record = await session.scalar(
        select(DiagnosticPolicy).where(
            DiagnosticPolicy.key == REAL_ADVISORY_POLICY_KEY,
            DiagnosticPolicy.version == REAL_ADVISORY_POLICY_VERSION,
        )
    )
    if (
        record is None
        or record.id != REAL_ADVISORY_POLICY_ID
        or record.schema_version != "1"
        or record.payload != REAL_ADVISORY_POLICY_PAYLOAD
    ):
        raise ValueError("published real_advisory_policy_v1 is missing or invalid")
    payload = {
        "id": str(record.id),
        "key": record.key,
        "version": record.version,
        "schema_version": record.schema_version,
        "payload": record.payload,
        "created_at": record.created_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    }
    content_hash = sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return RealAdvisoryPolicy.v1(
        authority_refs=(
            EvidenceRef(
                source="nexus_db",
                object_id=f"diagnostic-policy:{record.id}:{record.version}",
                available_at=record.created_at,
                location=f"db://diagnostic-policies/{record.id}",
                content_hash=content_hash,
            ),
        )
    )
