from datetime import date
from decimal import Decimal

from nexus.providers.broker.base import SourceObservation
from nexus.reconciliation.cash_flows import CashFlowSyncResult, NormalizedCashFlow

from helpers.broker_seed import NOW


def _cash_flow_result(*, watermark: date) -> CashFlowSyncResult:
    return CashFlowSyncResult(
        facts=(
            NormalizedCashFlow(
                provider_cash_flow_id="synthetic-flow-stable",
                identity_version="provider_cash_flow_id_v1",
                identity_quality="STABLE",
                currency="USD",
                amount=Decimal("10"),
                occurred_at=NOW,
                provider_business_date=date(2026, 8, 23),
                kind="deposit",
                source_request_id="synthetic-cash-flow-page",
                response_as_of=NOW,
            ),
            NormalizedCashFlow(
                provider_cash_flow_id="fallback:v1:" + "a" * 64,
                identity_version="broker_cash_flow_fallback_v1",
                identity_quality="FALLBACK_PARTIAL",
                currency="USD",
                amount=Decimal("-5"),
                occurred_at=NOW,
                provider_business_date=date(2026, 8, 23),
                kind="fee",
                source_request_id="synthetic-cash-flow-page",
                response_as_of=NOW,
            ),
        ),
        quality="PARTIAL",
        pagination_complete=True,
        watermark_date=watermark,
        source_observations=(
            SourceObservation(
                source_request_id="synthetic-cash-flow-page",
                response_as_of=NOW,
                quality="PASS",
            ),
        ),
    )


def _stable_cash_flow_result(
    *, provider_id: str = "synthetic-flow-stable", amount: Decimal = Decimal("10")
) -> CashFlowSyncResult:
    fact = NormalizedCashFlow(
        provider_cash_flow_id=provider_id,
        identity_version="provider_cash_flow_id_v1",
        identity_quality="STABLE",
        currency="USD",
        amount=amount,
        occurred_at=NOW,
        provider_business_date=date(2026, 8, 23),
        kind="deposit",
        source_request_id="synthetic-cash-flow-page",
        response_as_of=NOW,
    )
    return CashFlowSyncResult(
        facts=(fact,),
        quality="PASS",
        pagination_complete=True,
        watermark_date=date(2026, 8, 23),
        source_observations=(
            SourceObservation(
                source_request_id=fact.source_request_id,
                response_as_of=NOW,
                quality="PASS",
            ),
        ),
    )
