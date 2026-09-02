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
        source="fixture",
        object_id=name,
        available_at=_NOW,
        location=f"db://{name}",
        content_hash="a" * 64,
    )


def _item(
    code: str,
    value: str,
    limit: str | None,
    severity: str,
    actionability: str,
) -> DiagnosticItem:
    return DiagnosticItem(
        diagnostic_code=code,
        metric_value=value,
        unit="ratio",
        reference_limit=limit,
        policy_scope="currency:USD",
        severity=severity,
        actionability=actionability,
        unknown_reason=None,
        evidence_refs=(_ref(code),),
    )  # type: ignore[arg-type]


def _snapshot() -> PortfolioSnapshot:
    def position(instrument_id: UUID, value: str) -> BrokerPosition:
        return BrokerPosition(
            instrument_id=instrument_id,
            currency="USD",
            quantity=Decimal(value),
            available_quantity=Decimal(value),
            frozen_quantity=Decimal("0"),
            avg_cost=Decimal("1"),
            diluted_cost=Decimal("1"),
            current_price=Decimal("1"),
        )

    return PortfolioSnapshot(
        id=UUID("0198d500-1000-7000-8000-000000000c03"),
        portfolio_id=UUID("0198d500-1000-7000-8000-000000000c04"),
        status="READY",
        cash=(
            CashBalance(currency="USD", cash_balance=Decimal("30"), available_cash=Decimal("30")),
        ),
        positions=(position(_FIRST, "40"), position(_SECOND, "30")),
        open_orders=(),
        as_of=_NOW,
    )


def _report(*items: DiagnosticItem) -> PortfolioDiagnosticReport:
    return PortfolioDiagnosticReport(
        portfolio_snapshot_id=_snapshot().id,
        snapshot_as_of=_NOW,
        policy_version="real_advisory_policy_v1",
        optimization_need="OPTIMIZATION_CANDIDATE",
        items=items,
    )


def test_sale_cost_uses_traded_notional_and_real_cost_stays_unknown() -> None:
    from nexus.optimizer.models import OptimizationPolicy, build_proposal_group
    from nexus.optimizer.production import MinimumChangeInputs, _comparison_inputs

    snapshot, report = _snapshot(), _report()
    for scope in ("internal_paper", "real_read_only"):
        group = build_proposal_group(
            snapshot, report, (), OptimizationPolicy.v1(), portfolio_scope=scope,
        )
        minimum = group.proposals[1]
        sale = minimum.positions[0].model_copy(
            update={"target_weight": Decimal("0.3"), "action": "reduce"},
        )
        minimum = minimum.model_copy(update={
            "positions": (sale, minimum.positions[1]), "cash_target_weight": Decimal("0.4"),
        })
        group = group.model_copy(
            update={"proposals": (group.proposals[0], minimum, group.proposals[2])}
        )
        compared = _comparison_inputs(
            group, report, MinimumChangeInputs((), Decimal(0), True), snapshot,
            {"commission_bps": "0", "slippage_bps": "10"},
        )[1]
        if scope == "internal_paper":
            # A sale of 10% of NAV at 10 bps costs 0.01% of NAV, not half that.
            assert compared.cost == Decimal("0.0001")
        else:
            assert compared.cost is None and not compared.cost_complete


def test_sector_or_missing_liquidity_proof_fails_closed() -> None:
    liquidity = _item(f"LIQUIDITY_DAYS_TO_EXIT:{_FIRST}", "1", "2", "info", "monitor")
    for items in (
        (
            _item("RISK_BUCKET_EXPOSURE:technology", "0.60", "0.50", "critical", "reduce_only"),
            liquidity,
        ),
        (_item("CONCENTRATION_TOP1", "0.40", "0.30", "critical", "reduce_only"),),
    ):
        result = _minimum_change_inputs(_snapshot(), _report(*items))
        assert (result.candidates, result.excess_weight, result.evidence_complete) == (
            (),
            Decimal("0"),
            False,
        )


def test_minimum_reduction_recomputes_security_denominator_with_cash() -> None:
    snapshot = _snapshot()
    positions = tuple(
        snapshot.positions[0].model_copy(
            update={
                "instrument_id": UUID(int=900 + index),
                "quantity": Decimal(value),
                "available_quantity": Decimal(value),
            }
        )
        for index, value in enumerate(("60", "10", "10", "10", "10"))
    )
    snapshot = snapshot.model_copy(
        update={
            "positions": positions,
            "cash": (
                CashBalance(
                    currency="USD", cash_balance=Decimal("100"), available_cash=Decimal("100")
                ),
            ),
        }
    )
    report = _report(
        _item("CONCENTRATION_TOP1", "0.60", "0.30", "critical", "reduce_only"),
        *(
            _item(f"LIQUIDITY_DAYS_TO_EXIT:{p.instrument_id}", "1", "2", "info", "monitor")
            for p in positions
        ),
    )
    result = _minimum_change_inputs(snapshot, report)
    assert result.evidence_complete
    # Sell about 42.86 of 200 NAV, not the raw 30 percentage-point diagnostic gap.
    assert Decimal("0.2142") < result.excess_weight <= Decimal("0.2143")
    assert result.reduction is not None
    weights = [p.target_weight for p in result.reduction.positions]
    assert max(weights) <= Decimal("0.30") * sum(weights)
    assert all(p.target_weight <= p.current_weight for p in result.reduction.positions)
    assert sum(weights) + Decimal("0.5") + result.reduction.cash_released == 1


def test_two_names_cannot_meet_security_top1_thirty_percent_by_selling() -> None:
    report = _report(
        _item(
            "CONCENTRATION_TOP1",
            str(Decimal("40") / Decimal("70")),
            "0.30",
            "critical",
            "reduce_only",
        ),
        _item(f"LIQUIDITY_DAYS_TO_EXIT:{_FIRST}", "1", "2", "info", "monitor"),
        _item(f"LIQUIDITY_DAYS_TO_EXIT:{_SECOND}", "5", "2", "warning", "research"),
    )
    result = _minimum_change_inputs(_snapshot(), report)
    assert not result.evidence_complete
    assert result.excess_weight == 0
