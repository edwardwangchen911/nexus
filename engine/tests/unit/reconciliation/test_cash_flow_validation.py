from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal

import pytest
from nexus.providers.broker.base import SourceObservation
from nexus.reconciliation.cash_flow_validation import validate_cash_flow_result
from nexus.reconciliation.cash_flows import CashFlowSyncResult, NormalizedCashFlow
from nexus.reconciliation.persistence_errors import BrokerSyncPersistenceError
from pydantic import TypeAdapter

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


@pytest.mark.parametrize(
    "context",
    [
        {"business_type": True},
        {"business_type": "1"},
        {"business_type": 1.0},
        {"business_type": 2**63},
        {"product_code": "synthetic private memo"},
        {"product_code": "A" * 129},
        {"product_code": 123},
        {"product_code": ""},
    ],
)
def test_persistence_revalidates_context_on_dataclass_inputs(context):
    result = result_with_fact(source_request_id="different-source-page")
    result = replace(result, facts=(replace(result.facts[0], **context),))
    with pytest.raises(BrokerSyncPersistenceError, match="context is invalid") as error:
        validate_cash_flow_result(result)
    assert "private" not in str(error.value)


def test_legacy_cash_flow_archive_without_context_can_still_be_read():
    result = result_with_fact(source_request_id="different-source-page")
    adapter = TypeAdapter(CashFlowSyncResult)
    payload = adapter.dump_python(result, mode="json")
    del payload["facts"][0]["business_type"]
    del payload["facts"][0]["product_code"]
    replayed = adapter.validate_python(payload)
    validate_cash_flow_result(replayed)
    assert replayed.facts[0].business_type is None
    assert replayed.facts[0].product_code is None
    assert replayed == result
