"""Published non-zero cost authority for historical daily backtests."""

from datetime import UTC, datetime
from typing import Final
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.optimization import CostPolicyRecord

BACKTEST_COST_POLICY_ID: Final = UUID("86a6428c-1a47-5ef1-b35f-216713f2df6d")
BACKTEST_COST_POLICY_KEY: Final = "backtest_cost_model"
BACKTEST_COST_POLICY_VERSION: Final = "backtest_cost_policy_v1"
BACKTEST_COST_POLICY_PARAMETERS: Final[dict[str, object]] = {
    "commission_bps": "0",
    "spread_bps": "5",
    "slippage_bps": "10",
    "tax_treatment": "unknown",
}
BACKTEST_COST_POLICY_EVIDENCE_REFS: Final[tuple[str, ...]] = (
    "backtest_cost_policy_v1_conservative_daily_assumption",
)
BACKTEST_COST_POLICY_AVAILABLE_AT: Final = datetime(2020, 1, 1, tzinfo=UTC)


async def load_backtest_cost_policy_v1(session: AsyncSession) -> CostPolicyRecord:
    """Load the exact historical backtest cost policy or fail closed."""
    record = await session.get(CostPolicyRecord, BACKTEST_COST_POLICY_ID, populate_existing=True)
    if (
        record is None
        or record.key != BACKTEST_COST_POLICY_KEY
        or record.version != BACKTEST_COST_POLICY_VERSION
        or record.schema_version != "1"
        or record.parameters != BACKTEST_COST_POLICY_PARAMETERS
        or tuple(record.evidence_refs) != BACKTEST_COST_POLICY_EVIDENCE_REFS
        or record.available_at != BACKTEST_COST_POLICY_AVAILABLE_AT
    ):
        raise ValueError("published backtest_cost_policy_v1 is missing or invalid")
    return record
