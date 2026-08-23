"""Canonical immutable cost authority for optimizer_v1 proposals."""

from datetime import UTC, datetime
from typing import Final
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.optimization import CostPolicyRecord

COST_POLICY_ID: Final = UUID("d9cdf971-59f6-5a52-9df4-4e1f861d8075")
COST_POLICY_KEY: Final = "cost_model"
COST_POLICY_VERSION: Final = "cost_policy_v1"
COST_POLICY_PARAMETERS: Final[dict[str, object]] = {
    "commission_bps": "0",
    "slippage_bps": "0",
    "tax_treatment": "unknown",
}
COST_POLICY_EVIDENCE_REFS: Final[tuple[str, ...]] = ("cost_policy_v1_methodology",)
COST_POLICY_AVAILABLE_AT: Final = datetime(2020, 1, 1, tzinfo=UTC)


async def load_cost_policy_v1(session: AsyncSession) -> CostPolicyRecord:
    """Load the one published, immutable cost-policy record or fail closed."""
    record = await session.get(CostPolicyRecord, COST_POLICY_ID)
    if (
        record is None
        or record.key != COST_POLICY_KEY
        or record.version != COST_POLICY_VERSION
        or record.schema_version != "1"
        or record.parameters != COST_POLICY_PARAMETERS
        or tuple(record.evidence_refs) != COST_POLICY_EVIDENCE_REFS
        or record.available_at != COST_POLICY_AVAILABLE_AT
    ):
        raise ValueError("published cost_policy_v1 is missing or invalid")
    return record
