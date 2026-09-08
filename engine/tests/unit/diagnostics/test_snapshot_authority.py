from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.diagnostics.assembler import DiagnosticAssembly, _requires_dexhkus
from nexus.diagnostics.input_hash import diagnostic_input_hash
from nexus.diagnostics.items import unknown_item
from nexus.diagnostics.models import (
    DiagnosticEvidence,
    DiagnosticHistory,
    DiagnosticItem,
    EvidenceBinding,
    MarketEvidence,
    QualifiedFxValuation,
)
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.diagnostics.service import diagnose_portfolio
from nexus.domain.evidence import EvidenceRef
from nexus.providers.broker.base import CashBalance, PortfolioSnapshot
from pydantic import ValidationError

SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
PORTFOLIO_ID = UUID("0198d500-1000-7000-8000-000000000011")
SNAPSHOT_AT = datetime(2026, 8, 24, tzinfo=UTC)


def test_unknown_diagnostic_item_requires_immutable_evidence() -> None:
    with pytest.raises(ValidationError, match="at least 1 item"):
        DiagnosticItem(
            diagnostic_code="UNKNOWN",
            metric_value=None,
            unit="status",
            reference_limit=None,
            policy_scope="portfolio",
            severity="info",
            actionability="research",
            unknown_reason="MISSING",
            evidence_refs=(),
        )


def test_unknown_constructor_falls_back_to_explicit_snapshot_authority() -> None:
    snapshot_ref = _ref("snapshot", "a")
    market = MarketEvidence(
        quality="PARTIAL",
        evidence=DiagnosticEvidence(snapshot=_binding(snapshot_ref)),
    )

    item = unknown_item("MARKET_EVIDENCE", "status", "MISSING", "portfolio", market)

    assert item.evidence_refs == (snapshot_ref,)


def test_snapshot_source_provenance_changes_diagnostic_input_hash() -> None:
    snapshot = PortfolioSnapshot(
        id=SNAPSHOT_ID,
        portfolio_id=PORTFOLIO_ID,
        status="READY",
        cash=(),
        positions=(),
        open_orders=(),
        as_of=SNAPSHOT_AT,
    )
    canonical_inputs = {
        "snapshot": {
            "id": str(SNAPSHOT_ID),
            "source_request_ids": ("request-a", "request-b"),
            "source_observations": ({"source_request_id": "request-a"},),
        }
    }
    first = DiagnosticAssembly(
        market=MarketEvidence(quality="PASS"), canonical_inputs=canonical_inputs
    )
    changed = DiagnosticAssembly(
        market=first.market,
        canonical_inputs={
            "snapshot": {
                **canonical_inputs["snapshot"],
                "source_observations": ({"source_request_id": "request-z"},),
            }
        },
    )

    first_hash = diagnostic_input_hash(
        snapshot=snapshot,
        assembly=first,
        history=DiagnosticHistory.empty(),
        policy_version="real_advisory_policy_v1",
    )
    changed_hash = diagnostic_input_hash(
        snapshot=snapshot,
        assembly=changed,
        history=DiagnosticHistory.empty(),
        policy_version="real_advisory_policy_v1",
    )

    assert first_hash != changed_hash


def test_dexhkus_is_loaded_only_for_exact_usd_hkd_scope() -> None:
    def snapshot(*currencies: str) -> PortfolioSnapshot:
        return PortfolioSnapshot(
            id=SNAPSHOT_ID,
            portfolio_id=PORTFOLIO_ID,
            status="READY",
            cash=tuple(
                CashBalance(
                    currency=currency,
                    cash_balance=Decimal("1"),
                    available_cash=Decimal("1"),
                    provider_as_of=SNAPSHOT_AT,
                )
                for currency in currencies
            ),
            positions=(),
            open_orders=(),
            as_of=SNAPSHOT_AT,
        )

    assert _requires_dexhkus(snapshot("USD", "HKD"))
    assert not _requires_dexhkus(snapshot("USD"))
    assert not _requires_dexhkus(snapshot("USD", "HKD", "CNY"))


def test_unqualified_fx_unknown_binds_snapshot_and_policy_authority() -> None:
    snapshot_ref = _ref("snapshot", "a")
    policy_ref = _ref("policy", "b")
    cash_refs = {
        currency: _ref(f"cash-{currency}", character)
        for currency, character in (
            ("USD", "c"),
            ("HKD", "d"),
        )
    }
    snapshot = PortfolioSnapshot(
        id=SNAPSHOT_ID,
        portfolio_id=PORTFOLIO_ID,
        status="READY",
        cash=tuple(
            CashBalance(
                currency=currency,
                cash_balance=Decimal("100"),
                available_cash=Decimal("100"),
                provider_as_of=SNAPSHOT_AT,
            )
            for currency in ("USD", "HKD")
        ),
        positions=(),
        open_orders=(),
        as_of=SNAPSHOT_AT,
    )
    calendar_ref = _ref("calendar", "e")
    market = MarketEvidence(
        quality="PASS",
        trading_sessions=(date(2026, 8, 21),),
        evidence=DiagnosticEvidence(
            snapshot=_binding(snapshot_ref),
            cash={currency: _binding(ref) for currency, ref in cash_refs.items()},
            trading_calendar=EvidenceBinding(
                available_at=SNAPSHOT_AT,
                as_of=SNAPSHOT_AT,
                session=date(2026, 8, 21),
                refs=(calendar_ref,),
            ),
        ),
    )

    report = diagnose_portfolio(
        snapshot,
        market,
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(authority_refs=(policy_ref,)),
    )

    fx_item = next(item for item in report.items if item.diagnostic_code == "CURRENCY_EXPOSURE")
    assert fx_item.unknown_reason == "FX_QUALIFIED_VIEW_MISSING"
    assert fx_item.evidence_refs == (snapshot_ref, policy_ref)


def test_qualified_fx_produces_deterministic_max_currency_exposure() -> None:
    snapshot_ref = _ref("snapshot", "a")
    policy_ref = _ref("policy", "b")
    usd_ref = _ref("cash-USD", "c")
    hkd_ref = _ref("cash-HKD", "d")
    fx_ref = _ref("DEXHKUS", "e")
    calendar_ref = _ref("calendar", "f")
    snapshot = PortfolioSnapshot(
        id=SNAPSHOT_ID,
        portfolio_id=PORTFOLIO_ID,
        status="READY",
        cash=(
            CashBalance(
                currency="USD",
                cash_balance=Decimal("100"),
                available_cash=Decimal("100"),
                provider_as_of=SNAPSHOT_AT,
            ),
            CashBalance(
                currency="HKD",
                cash_balance=Decimal("785"),
                available_cash=Decimal("785"),
                provider_as_of=SNAPSHOT_AT,
            ),
        ),
        positions=(),
        open_orders=(),
        as_of=SNAPSHOT_AT,
    )
    market = MarketEvidence(
        quality="PASS",
        trading_sessions=(date(2026, 8, 21),),
        evidence=DiagnosticEvidence(
            snapshot=_binding(snapshot_ref),
            cash={"USD": _binding(usd_ref), "HKD": _binding(hkd_ref)},
            trading_calendar=EvidenceBinding(
                available_at=SNAPSHOT_AT,
                as_of=SNAPSHOT_AT,
                session=date(2026, 8, 21),
                refs=(calendar_ref,),
            ),
        ),
        fx_valuations=(
            QualifiedFxValuation(
                quote_per_base=Decimal("7.85"),
                observed_on=date(2026, 8, 21),
                vintage=date(2026, 8, 21),
                received_at=SNAPSHOT_AT,
                evidence=EvidenceBinding(
                    available_at=SNAPSHOT_AT,
                    as_of=SNAPSHOT_AT,
                    session=date(2026, 8, 21),
                    refs=(fx_ref, calendar_ref),
                ),
            ),
        ),
    )

    report = diagnose_portfolio(
        snapshot,
        market,
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(authority_refs=(policy_ref,)),
    )

    fx_item = next(item for item in report.items if item.diagnostic_code == "CURRENCY_EXPOSURE")
    assert fx_item.metric_value == "0.5"
    assert fx_item.reference_limit == "0.80"
    assert fx_item.policy_scope == "portfolio:USD"
    assert fx_item.unknown_reason is None
    assert fx_item.evidence_refs == (
        snapshot_ref,
        fx_ref,
        calendar_ref,
        usd_ref,
        hkd_ref,
        policy_ref,
    )


def test_fx_received_after_snapshot_remains_unknown_for_pure_diagnostic() -> None:
    snapshot_ref = _ref("snapshot", "a")
    policy_ref = _ref("policy", "b")
    cash_refs = {
        currency: _ref(f"cash-{currency}", character)
        for currency, character in (
            ("USD", "c"),
            ("HKD", "d"),
        )
    }
    snapshot = PortfolioSnapshot(
        id=SNAPSHOT_ID,
        portfolio_id=PORTFOLIO_ID,
        status="READY",
        cash=tuple(
            CashBalance(
                currency=currency,
                cash_balance=Decimal("100"),
                available_cash=Decimal("100"),
                provider_as_of=SNAPSHOT_AT,
            )
            for currency in ("USD", "HKD")
        ),
        positions=(),
        open_orders=(),
        as_of=SNAPSHOT_AT,
    )
    fx_ref = _ref("DEXHKUS", "e")
    calendar_ref = _ref("calendar", "f")
    market = MarketEvidence(
        quality="PASS",
        trading_sessions=(date(2026, 8, 21),),
        evidence=DiagnosticEvidence(
            snapshot=_binding(snapshot_ref),
            cash={currency: _binding(ref) for currency, ref in cash_refs.items()},
            trading_calendar=EvidenceBinding(
                available_at=SNAPSHOT_AT,
                as_of=SNAPSHOT_AT,
                session=date(2026, 8, 21),
                refs=(calendar_ref,),
            ),
        ),
        fx_valuations=(
            QualifiedFxValuation(
                quote_per_base=Decimal("7.85"),
                observed_on=date(2026, 8, 21),
                vintage=date(2026, 8, 21),
                received_at=SNAPSHOT_AT + timedelta(seconds=1),
                evidence=EvidenceBinding(
                    available_at=SNAPSHOT_AT,
                    as_of=SNAPSHOT_AT,
                    session=date(2026, 8, 21),
                    refs=(fx_ref, calendar_ref),
                ),
            ),
        ),
    )

    report = diagnose_portfolio(
        snapshot,
        market,
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(authority_refs=(policy_ref,)),
    )

    fx_item = next(item for item in report.items if item.diagnostic_code == "CURRENCY_EXPOSURE")
    assert fx_item.metric_value is None
    assert fx_item.unknown_reason == "FX_QUALIFIED_VIEW_MISSING"


def _ref(name: str, content: str) -> EvidenceRef:
    return EvidenceRef(
        source="fixture",
        object_id=name,
        available_at=SNAPSHOT_AT,
        location=f"fixture://{name}",
        content_hash=content * 64,
    )


def _binding(ref: EvidenceRef) -> EvidenceBinding:
    return EvidenceBinding(available_at=SNAPSHOT_AT, as_of=SNAPSHOT_AT, refs=(ref,))
