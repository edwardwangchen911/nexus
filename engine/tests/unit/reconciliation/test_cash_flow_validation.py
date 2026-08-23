from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal

import pytest
from nexus.providers.broker.base import SourceObservation
from nexus.reconciliation.cash_flow_validation import validate_cash_flow_result
from nexus.reconciliation.cash_flows import CashFlowSyncResult, NormalizedCashFlow
from nexus.reconciliation.persistence_errors import BrokerSyncPersistenceError

NOW = datetime(2026, 8, 23, 4, tzinfo=UTC)


def result_with_fact(
    *,
    quality: Literal["PASS", "PARTIAL"] = "PASS",
    identity_quality: Literal["STABLE", "FALLBACK_PARTIAL"] = "STABLE",
    currency: str = "USD",
    source_request_id: str = "missing-source-page",
) -> CashFlowSyncResult:
    return CashFlowSyncResult(
        facts=(
            NormalizedCashFlow(
                provider_cash_flow_id="synthetic-flow",
                identity_version=(
                    "provider_cash_flow_id_v1"
                    if identity_quality == "STABLE"
                    else "broker_cash_flow_fallback_v1"
                ),
                identity_quality=identity_quality,
                currency=currency,
                amount=Decimal("1"),
                occurred_at=NOW,
                provider_business_date=date(2026, 8, 23),
                kind="deposit",
                source_request_id=source_request_id,
                response_as_of=NOW,
            ),
        ),
        quality=quality,
        pagination_complete=quality == "PASS",
        watermark_date=date(2026, 8, 23) if quality == "PASS" else None,
        source_observations=(
            SourceObservation(
                source_request_id="different-source-page",
                response_as_of=NOW,
                quality="PASS",
            ),
        ),
    )


def test_cash_flow_pass_requires_page_observation_even_when_no_facts_exist() -> None:
    result = CashFlowSyncResult((), "PASS", True, date(2026, 8, 23), ())
    with pytest.raises(BrokerSyncPersistenceError, match="PASS evidence"):
        validate_cash_flow_result(result)


def test_cash_flow_pass_binds_every_fact_to_a_source_observation() -> None:
    with pytest.raises(BrokerSyncPersistenceError, match="PASS evidence"):
        validate_cash_flow_result(result_with_fact())


def test_partial_without_source_evidence_cannot_carry_a_watermark() -> None:
    result = CashFlowSyncResult((), "PARTIAL", True, date(2026, 8, 23), ())
    with pytest.raises(BrokerSyncPersistenceError, match="watermark evidence"):
        validate_cash_flow_result(result)


def test_partial_watermark_binds_every_fact_to_a_complete_source_observation() -> None:
    result = result_with_fact(quality="PARTIAL", identity_quality="FALLBACK_PARTIAL")
    result = CashFlowSyncResult(
        result.facts,
        "PARTIAL",
        True,
        date(2026, 8, 23),
        result.source_observations,
    )
    with pytest.raises(BrokerSyncPersistenceError, match="watermark evidence"):
        validate_cash_flow_result(result)


def test_partial_without_evidence_remains_diagnostic_when_watermark_is_absent() -> None:
    validate_cash_flow_result(CashFlowSyncResult((), "PARTIAL", False, None, ()))


@pytest.mark.parametrize("currency", ("U1D", "USÉ", "ＵSD"))
def test_persistence_rejects_non_ascii_or_non_letter_currency(currency: str) -> None:
    with pytest.raises(BrokerSyncPersistenceError, match="facts are invalid"):
        validate_cash_flow_result(result_with_fact(quality="PARTIAL", currency=currency))
