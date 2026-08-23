from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.diagnostics.market_risk import correlation_cluster_scope
from nexus.diagnostics.models import DiagnosticItem, InstrumentIdentity, MarketEvidence
from nexus.diagnostics.scope import assert_fosun_read_only_scope
from nexus.domain.evidence import EvidenceRef
from nexus.providers.broker.base import BrokerPosition, PortfolioSnapshot

INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000013")


@pytest.mark.parametrize("identity_mode", ("fosun", "missing"))
def test_fosun_guard_rejects_malicious_prefixed_code(identity_mode: str) -> None:
    identities = (
        {INSTRUMENT_ID: InstrumentIdentity.fosun_read_only(INSTRUMENT_ID)}
        if identity_mode == "fosun"
        else {}
    )
    market = MarketEvidence(quality="PASS", identities=identities)
    malicious = _item("CONCENTRATION_FUNDAMENTAL_BUY")

    with pytest.raises(ValueError, match="00656.HK"):
        assert_fosun_read_only_scope(_snapshot(), market, [malicious])


def test_fosun_guard_accepts_only_exact_supported_code_families() -> None:
    market = MarketEvidence(
        quality="PASS",
        identities={INSTRUMENT_ID: InstrumentIdentity.fosun_read_only(INSTRUMENT_ID)},
    )
    items = [
        _item("CONCENTRATION_TOP1"),
        _item(f"LIQUIDITY_DAYS_TO_EXIT:{INSTRUMENT_ID}"),
        _item(f"VOLATILITY_20:{INSTRUMENT_ID}"),
        _item(f"DRAWDOWN_252:{INSTRUMENT_ID}"),
        _item("DATA_COMPLETENESS_TRADING_CALENDAR"),
    ]

    assert_fosun_read_only_scope(_snapshot(), market, items)


def test_market_evidence_rejects_caller_symbol_claims() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        MarketEvidence.model_validate(
            {"quality": "PASS", "symbols": {str(INSTRUMENT_ID): "00656.HK"}}
        )


def test_correlation_cluster_scope_is_currency_specific_and_component_stable() -> None:
    first = UUID("0198d500-1000-7000-8000-000000000021")
    second = UUID("0198d500-1000-7000-8000-000000000022")
    third = UUID("0198d500-1000-7000-8000-000000000023")

    left = correlation_cluster_scope("currency:USD", (first, second))

    assert left.startswith("currency:USD:correlation-cluster:")
    assert left == correlation_cluster_scope("currency:USD", (second, first))
    assert left != correlation_cluster_scope("currency:USD", (first, third))
    assert left != correlation_cluster_scope("currency:HKD", (first, second))


def _snapshot() -> PortfolioSnapshot:
    return PortfolioSnapshot(
        id=UUID("0198d500-1000-7000-8000-000000000011"),
        portfolio_id=UUID("0198d500-1000-7000-8000-000000000012"),
        status="READY",
        cash=(),
        positions=(
            BrokerPosition(
                instrument_id=INSTRUMENT_ID,
                currency="HKD",
                quantity=Decimal(1),
                available_quantity=Decimal(1),
                frozen_quantity=Decimal(0),
                avg_cost=Decimal(1),
                diluted_cost=Decimal(1),
                current_price=Decimal(1),
                provider_as_of=datetime(2026, 8, 23, tzinfo=UTC),
            ),
        ),
        open_orders=(),
        as_of=datetime(2026, 8, 23, tzinfo=UTC),
    )


def _item(code: str) -> DiagnosticItem:
    return DiagnosticItem(
        diagnostic_code=code,
        metric_value="1",
        unit="ratio",
        reference_limit=None,
        policy_scope="currency:HKD",
        severity="info",
        actionability="monitor",
        unknown_reason="TEST_SCOPE_ONLY",
        evidence_refs=(
            EvidenceRef(
                source="fixture",
                object_id="scope",
                available_at=datetime(2026, 8, 23, tzinfo=UTC),
                location="fixture://scope",
                content_hash="a" * 64,
            ),
        ),
    )
