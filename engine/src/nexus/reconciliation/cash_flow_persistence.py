"""Conflict-aware cash-flow identity persistence."""

from typing import Literal

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.persistence.models.portfolio import BrokerCashFlow as BrokerCashFlowRecord
from nexus.persistence.models.portfolio import BrokerSyncRun
from nexus.reconciliation.cash_flow_validation import validate_cash_flow_result
from nexus.reconciliation.cash_flow_watermarks import advance_cash_flow_watermark
from nexus.reconciliation.cash_flows import CashFlowSyncResult, NormalizedCashFlow

CashFlowQuality = Literal["PASS", "PARTIAL", "NOT_RUN"]


async def persist_cash_flows(
    session: AsyncSession,
    run: BrokerSyncRun,
    provider: str,
    result: CashFlowSyncResult,
) -> CashFlowQuality:
    """Claim provider identities transactionally before inserting facts."""
    validate_cash_flow_result(result)
    identities = tuple(fact.provider_cash_flow_id for fact in result.facts)
    for provider_cash_flow_id in sorted(identities):
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
            {"identity": f"broker-cash-flow:v1:{provider}:{provider_cash_flow_id}"},
        )
    existing = {
        row.provider_cash_flow_id: row
        for row in (
            await session.scalars(
                select(BrokerCashFlowRecord).where(
                    BrokerCashFlowRecord.provider == provider,
                    BrokerCashFlowRecord.provider_cash_flow_id.in_(identities),
                )
            )
            if identities
            else ()
        )
    }
    conflict = False
    for fact in result.facts:
        prior = existing.get(fact.provider_cash_flow_id)
        if prior is not None:
            conflict = conflict or not _same_body(prior, run.account_id, fact)
            continue
        session.add(
            BrokerCashFlowRecord(
                id=new_uuid7(),
                account_id=run.account_id,
                sync_run_id=run.id,
                provider=provider,
                provider_cash_flow_id=fact.provider_cash_flow_id,
                identity_version=fact.identity_version,
                identity_quality=fact.identity_quality,
                currency=fact.currency,
                amount=fact.amount,
                occurred_at=fact.occurred_at,
                provider_business_date=fact.provider_business_date,
                kind=fact.kind,
                source_request_id=fact.source_request_id,
                response_as_of=fact.response_as_of,
            )
        )
    if conflict:
        return "PARTIAL"
    await advance_cash_flow_watermark(session, run.account_id, result)
    return result.quality


def _same_body(prior: BrokerCashFlowRecord, account_id: object, fact: NormalizedCashFlow) -> bool:
    return (
        prior.account_id == account_id
        and prior.currency == fact.currency
        and prior.amount == fact.amount
        and prior.occurred_at == fact.occurred_at
        and prior.provider_business_date == fact.provider_business_date
        and prior.kind == fact.kind
        and prior.identity_version == fact.identity_version
        and prior.identity_quality == fact.identity_quality
    )
