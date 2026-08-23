"""Monotonic broker cash-flow watermark updates."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.persistence.models.portfolio import BrokerCashFlowWatermark
from nexus.reconciliation.cash_flows import CashFlowSyncResult


async def advance_cash_flow_watermark(
    session: AsyncSession,
    account_id: object,
    result: CashFlowSyncResult,
) -> None:
    """Advance only complete evidence; preserve any fallback downgrade."""
    if not result.pagination_complete or result.watermark_date is None:
        return
    watermark = await session.scalar(
        select(BrokerCashFlowWatermark)
        .where(BrokerCashFlowWatermark.account_id == account_id)
        .with_for_update()
    )
    identity_quality = (
        "FALLBACK_PARTIAL"
        if any(fact.identity_quality == "FALLBACK_PARTIAL" for fact in result.facts)
        else "STABLE"
    )
    if watermark is None:
        session.add(
            BrokerCashFlowWatermark(
                id=new_uuid7(),
                account_id=account_id,
                provider_business_date=result.watermark_date,
                identity_quality=identity_quality,
                version=1,
            )
        )
    elif result.watermark_date > watermark.provider_business_date:
        watermark.provider_business_date = result.watermark_date
        if watermark.identity_quality == "FALLBACK_PARTIAL":
            identity_quality = "FALLBACK_PARTIAL"
        watermark.identity_quality = identity_quality
        watermark.version += 1
    elif (
        result.watermark_date == watermark.provider_business_date
        and watermark.identity_quality == "STABLE"
        and identity_quality == "FALLBACK_PARTIAL"
    ):
        watermark.identity_quality = "FALLBACK_PARTIAL"
        watermark.version += 1
