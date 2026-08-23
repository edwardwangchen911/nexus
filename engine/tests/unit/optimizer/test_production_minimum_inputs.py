"""Evidence-qualified production inputs for the minimum-change heuristic."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from nexus.diagnostics.models import DiagnosticItem, PortfolioDiagnosticReport
from nexus.domain.evidence import EvidenceRef
from nexus.optimizer.production import _minimum_change_inputs
from nexus.providers.broker.base import BrokerPosition, CashBalance, PortfolioSnapshot

_NOW = datetime(2026, 8, 24, tzinfo=UTC)
_FIRST = UUID("0198d500-1000-7000-8000-000000000c01")
_SECOND = UUID("0198d500-1000-7000-8000-000000000c02")


def _ref(name: str) -> EvidenceRef:
    return EvidenceRef(
        source="fixture", object_id=name, available_at=_NOW,
        location=f"db://{name}", content_hash="a" * 64,
    )


def _item(
    code: str, value: str, limit: str | None, severity: str, actionability: str,
) -> DiagnosticItem:
    return DiagnosticItem(
        diagnostic_code=code, metric_value=value, unit="ratio",
        reference_limit=limit, policy_scope="currency:USD", severity=severity,
        actionability=actionability, unknown_reason=None, evidence_refs=(_ref(code),),
    )  # type: ignore[arg-type]


def _snapshot() -> PortfolioSnapshot:
    def position(instrument_id: UUID, value: str) -> BrokerPosition:
        return BrokerPosition(
            instrument_id=instrument_id, currency="USD", quantity=Decimal(value),
            available_quantity=Decimal(value), frozen_quantity=Decimal("0"),
            avg_cost=Decimal("1"), diluted_cost=Decimal("1"), current_price=Decimal("1"),
        )

    return PortfolioSnapshot(
        id=UUID("0198d500-1000-7000-8000-000000000c03"),
        portfolio_id=UUID("0198d500-1000-7000-8000-000000000c04"), status="READY",
        cash=(
            CashBalance(
                currency="USD", cash_balance=Decimal("30"), available_cash=Decimal("30")
            ),
        ),
        positions=(position(_FIRST, "40"), position(_SECOND, "30")), open_orders=(), as_of=_NOW,
    )


def _report(*items: DiagnosticItem) -> PortfolioDiagnosticReport:
    return PortfolioDiagnosticReport(
        portfolio_snapshot_id=_snapshot().id, snapshot_as_of=_NOW,
        policy_version="real_advisory_policy_v1", optimization_need="OPTIMIZATION_CANDIDATE",
        items=items,
    )


def test_concentration_uses_only_constraint_contributors_and_proven_liquidity() -> None:
    result = _minimum_change_inputs(
        _snapshot(),
        _report(
            _item("CONCENTRATION_TOP1", "0.40", "0.30", "critical", "reduce_only"),
            _item(f"LIQUIDITY_DAYS_TO_EXIT:{_FIRST}", "1", "2", "info", "monitor"),
            _item(f"LIQUIDITY_DAYS_TO_EXIT:{_SECOND}", "5", "2", "warning", "research"),
        ),
    )
    assert result.evidence_complete is True
    assert result.excess_weight == Decimal("0.10")
    assert tuple((row.instrument_id, row.contribution) for row in result.candidates) == (
        (_FIRST, Decimal("40") / Decimal("70")),
        (_SECOND, Decimal("0")),
    )
    assert result.candidates[0].severity_rank == 2
    assert result.candidates[0].liquidity_score > result.candidates[1].liquidity_score


def test_sector_or_missing_liquidity_proof_fails_closed() -> None:
    liquidity = _item(f"LIQUIDITY_DAYS_TO_EXIT:{_FIRST}", "1", "2", "info", "monitor")
    for items in (
        (
            _item(
                "RISK_BUCKET_EXPOSURE:technology", "0.60", "0.50", "critical", "reduce_only"
            ),
            liquidity,
        ),
        (_item("CONCENTRATION_TOP1", "0.40", "0.30", "critical", "reduce_only"),),
    ):
        result = _minimum_change_inputs(_snapshot(), _report(*items))
        assert (result.candidates, result.excess_weight, result.evidence_complete) == (
            (), Decimal("0"), False,
        )
