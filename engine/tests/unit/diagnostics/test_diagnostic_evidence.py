from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.diagnostics.models import (
    DiagnosticEvidence,
    DiagnosticHistory,
    DiagnosticItem,
    EvidenceBinding,
    InstrumentIdentity,
    MarketEvidence,
)
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.diagnostics.service import diagnose_portfolio
from nexus.domain.evidence import EvidenceRef
from nexus.providers.base import PriceBar, PriceSeries
from nexus.providers.broker.base import BrokerPosition, CashBalance, PortfolioSnapshot

INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000013")
SNAPSHOT_AT = datetime(2026, 8, 23, tzinfo=UTC)


@pytest.mark.parametrize("price", (None, Decimal(0)))
@pytest.mark.parametrize("include_priced_holding", (False, True))
def test_snapshot_only_diagnosis_does_not_turn_unpriced_holdings_into_cash_only(
    price: Decimal | None,
    include_priced_holding: bool,
) -> None:
    snapshot = _snapshot()
    priced_id = UUID("0198d500-1000-7000-8000-000000000014")
    priced = snapshot.positions[0].model_copy(update={"instrument_id": priced_id})
    snapshot = snapshot.model_copy(
        update={
            "positions": (snapshot.positions[0].model_copy(update={"current_price": price}),)
            + ((priced,) if include_priced_holding else ()),
        }
    )
    market = _market(snapshot).model_copy(update={"quality": "PARTIAL"})
    if include_priced_holding:
        market = market.model_copy(
            update={
                "identities": {
                    **market.identities,
                    priced_id: InstrumentIdentity.standard(priced_id),
                },
                "evidence": market.evidence.model_copy(
                    update={
                        "position_prices": {
                            **market.evidence.position_prices,
                            priced_id: _binding("priced", "f"),
                        },
                    }
                ),
            }
        )
    report = diagnose_portfolio(
        snapshot, market, DiagnosticHistory.empty(), RealAdvisoryPolicy.v1()
    )
    cash = next(item for item in report.items if item.diagnostic_code == "CASH_RATIO")
    assert cash.metric_value is None
    assert cash.unknown_reason is not None
    assert not any(item.diagnostic_code == "CONCENTRATION_TOP1" for item in report.items)


def test_known_diagnostic_item_requires_immutable_evidence() -> None:
    with pytest.raises(ValueError, match="evidence_refs"):
        DiagnosticItem(
            diagnostic_code="KNOWN",
            metric_value="1",
            unit="ratio",
            reference_limit=None,
            policy_scope="portfolio",
            severity="info",
            actionability="monitor",
            unknown_reason=None,
        )


def test_known_diagnostics_retain_only_their_actual_input_evidence() -> None:
    snapshot = _snapshot()
    refs = {
        name: _ref(name, character)
        for name, character in zip(
            ("snapshot", "cash", "position", "adv", "bucket", "prices", "benchmark", "calendar"),
            "12345678",
            strict=True,
        )
    }
    report = diagnose_portfolio(
        snapshot,
        _market(snapshot, refs),
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(),
    )
    items = {item.diagnostic_code: item for item in report.items}

    assert items["CONCENTRATION_TOP1"].evidence_refs == (
        refs["snapshot"],
        refs["position"],
    )
    assert items[f"LIQUIDITY_DAYS_TO_EXIT:{INSTRUMENT_ID}"].evidence_refs == (
        refs["snapshot"],
        refs["position"],
        refs["adv"],
    )
    assert items["RISK_BUCKET_EXPOSURE:equity"].evidence_refs == (
        refs["snapshot"],
        refs["position"],
        refs["bucket"],
    )
    assert items[f"VOLATILITY_20:{INSTRUMENT_ID}"].evidence_refs == (refs["prices"],)
    assert items[f"BETA_126:{INSTRUMENT_ID}"].evidence_refs == (
        refs["prices"],
        refs["benchmark"],
    )
    assert items["CASH_RATIO"].evidence_refs == (refs["snapshot"], refs["position"], refs["cash"])
    assert items["DATA_COMPLETENESS_TRADING_CALENDAR"].evidence_refs == (refs["calendar"],)
    used_object_ids = {ref.object_id for item in report.items for ref in item.evidence_refs}
    assert "unrelated" not in used_object_ids


def test_market_evidence_rejects_a_global_ref_bag() -> None:
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        MarketEvidence.model_validate(
            {
                "quality": "PASS",
                "evidence_refs": [_ref("unrelated", "f").model_dump()],
            }
        )


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    (
        ("missing", "ADV20_EVIDENCE_MISSING"),
        ("future", "ADV20_EVIDENCE_FUTURE"),
        ("inconsistent", "ADV20_EVIDENCE_INCONSISTENT"),
    ),
)
def test_adv_evidence_gaps_are_explicit_unknowns(case: str, expected_reason: str) -> None:
    snapshot = _snapshot()
    market = _market(snapshot)
    bindings = {
        "missing": {},
        "future": {INSTRUMENT_ID: _binding("adv", "d", as_of=SNAPSHOT_AT + timedelta(seconds=1))},
        "inconsistent": {
            INSTRUMENT_ID: _binding(
                "adv",
                "d",
                available_at=SNAPSHOT_AT - timedelta(days=1),
                ref_available_at=SNAPSHOT_AT,
            )
        },
    }
    evidence_update = {"adv20": bindings[case]}
    evidence = market.evidence.model_copy(update=evidence_update)

    report = diagnose_portfolio(
        snapshot,
        market.model_copy(update={"evidence": evidence}),
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(),
    )

    item = next(
        item
        for item in report.items
        if item.diagnostic_code == f"LIQUIDITY_DAYS_TO_EXIT:{INSTRUMENT_ID}"
    )
    assert item.metric_value is None
    assert item.unknown_reason == expected_reason
    assert tuple(ref.object_id for ref in item.evidence_refs) == ("snapshot", "position")
    assert report.optimization_need == "RESEARCH_REQUIRED"


@pytest.mark.parametrize(
    ("evidence_update", "expected_code", "expected_reason"),
    (
        ({"snapshot": None}, "SNAPSHOT_EVIDENCE", "SNAPSHOT_EVIDENCE_MISSING"),
        ({"cash": {}}, "CASH_RATIO", "CASH_EVIDENCE_MISSING"),
        ({"position_prices": {}}, "POSITION_MARKET_VALUE", "POSITION_PRICE_EVIDENCE_MISSING"),
        ({"risk_buckets": {}}, "RISK_BUCKET_EXPOSURE", "RISK_BUCKET_EVIDENCE_MISSING"),
        ({"price_series": {}}, "VOLATILITY_20", "PRICE_SERIES_EVIDENCE_MISSING"),
        ({"benchmark": None}, "BETA_126", "BENCHMARK_EVIDENCE_MISSING"),
        (
            {"trading_calendar": None},
            "DATA_COMPLETENESS_TRADING_CALENDAR",
            "TRADING_CALENDAR_EVIDENCE_MISSING",
        ),
    ),
)
def test_every_required_input_has_a_local_missing_evidence_path(
    evidence_update: dict[str, object], expected_code: str, expected_reason: str
) -> None:
    snapshot = _snapshot()
    market = _market(snapshot)
    evidence = market.evidence.model_copy(update=evidence_update)
    report = diagnose_portfolio(
        snapshot,
        market.model_copy(update={"evidence": evidence}),
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(),
    )

    assert any(
        item.diagnostic_code.startswith(expected_code) and item.unknown_reason == expected_reason
        for item in report.items
    )
    assert report.optimization_need == "RESEARCH_REQUIRED"


def test_future_calendar_binding_is_research_required_not_source_conflict() -> None:
    snapshot = _snapshot()
    market = _market(snapshot)
    evidence = market.evidence.model_copy(
        update={
            "trading_calendar": _binding(
                "calendar", "8", session=snapshot.as_of.date() + timedelta(days=1)
            )
        }
    )

    report = diagnose_portfolio(
        snapshot,
        market.model_copy(update={"evidence": evidence}),
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(),
    )

    assert report.optimization_need == "RESEARCH_REQUIRED"
    assert any(item.unknown_reason == "TRADING_CALENDAR_EVIDENCE_FUTURE" for item in report.items)


def test_authoritative_partial_and_derived_unknowns_retain_relevant_refs() -> None:
    snapshot = _snapshot()
    market = _market(snapshot)
    partial_ref = _ref("partial-series", "a")
    partial = diagnose_portfolio(
        snapshot,
        market.model_copy(update={"quality": "PARTIAL", "incomplete_refs": (partial_ref,)}),
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(),
    )
    assert partial.items[0].evidence_refs == (partial_ref,)

    derived = diagnose_portfolio(
        snapshot,
        market.model_copy(update={"adv20": {}, "risk_buckets": {}}),
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(),
    )
    by_code = {item.diagnostic_code: item for item in derived.items}
    assert by_code[f"LIQUIDITY_DAYS_TO_EXIT:{INSTRUMENT_ID}"].evidence_refs
    assert by_code["RISK_BUCKET_EXPOSURE"].evidence_refs


def _snapshot() -> PortfolioSnapshot:
    return PortfolioSnapshot(
        id=UUID("0198d500-1000-7000-8000-000000000011"),
        portfolio_id=UUID("0198d500-1000-7000-8000-000000000012"),
        status="READY",
        cash=(
            CashBalance(
                currency="USD",
                cash_balance=Decimal("100"),
                available_cash=Decimal("100"),
                provider_as_of=SNAPSHOT_AT,
            ),
        ),
        positions=(
            BrokerPosition(
                instrument_id=INSTRUMENT_ID,
                currency="USD",
                quantity=Decimal("1"),
                available_quantity=Decimal("1"),
                frozen_quantity=Decimal(0),
                avg_cost=Decimal("100"),
                diluted_cost=Decimal("100"),
                current_price=Decimal("100"),
                provider_as_of=SNAPSHOT_AT,
            ),
        ),
        open_orders=(),
        as_of=SNAPSHOT_AT,
    )


def _market(
    snapshot: PortfolioSnapshot, refs: dict[str, EvidenceRef] | None = None
) -> MarketEvidence:
    refs = refs or {
        name: _ref(name, character)
        for name, character in zip(
            ("snapshot", "cash", "position", "adv", "bucket", "prices", "benchmark", "calendar"),
            "12345678",
            strict=True,
        )
    }
    series = _series(snapshot, 253)
    return MarketEvidence(
        prices={INSTRUMENT_ID: series},
        adv20={INSTRUMENT_ID: Decimal("1000")},
        quality="PASS",
        risk_buckets={INSTRUMENT_ID: {"equity": Decimal(1)}},
        identities={INSTRUMENT_ID: InstrumentIdentity.standard(INSTRUMENT_ID)},
        trading_sessions=tuple(bar.session for bar in series.bars),
        benchmark=series,
        evidence=DiagnosticEvidence(
            snapshot=_binding_from_ref(refs["snapshot"]),
            cash={"USD": _binding_from_ref(refs["cash"])},
            position_prices={INSTRUMENT_ID: _binding_from_ref(refs["position"])},
            adv20={INSTRUMENT_ID: _binding_from_ref(refs["adv"])},
            risk_buckets={INSTRUMENT_ID: _binding_from_ref(refs["bucket"])},
            price_series={
                INSTRUMENT_ID: _binding_from_ref(refs["prices"], session=series.bars[-1].session)
            },
            benchmark=_binding_from_ref(refs["benchmark"], session=series.bars[-1].session),
            trading_calendar=_binding_from_ref(refs["calendar"], session=series.bars[-1].session),
        ),
        incomplete_refs=(refs["snapshot"],),
    )


def _series(snapshot: PortfolioSnapshot, count: int) -> PriceSeries:
    bars = tuple(
        PriceBar(
            instrument_id=INSTRUMENT_ID,
            session=snapshot.as_of.date() - timedelta(days=count - index),
            open=Decimal("100") + Decimal(index % 2),
            high=Decimal("101") + Decimal(index % 2),
            low=Decimal("99") + Decimal(index % 2),
            close=Decimal("100") + Decimal(index % 2),
            volume=Decimal("1000"),
            available_at=SNAPSHOT_AT,
        )
        for index in range(count)
    )
    return PriceSeries(
        source="fixture",
        feed="fixture",
        adjustment="all",
        bars=bars,
        mode="fixture",
        delivery_mode="fixture",
        quality="PASS",
    )


def _binding(
    object_id: str,
    character: str,
    *,
    available_at: datetime = SNAPSHOT_AT,
    as_of: datetime = SNAPSHOT_AT,
    session: date | None = None,
    ref_available_at: datetime = SNAPSHOT_AT,
) -> EvidenceBinding:
    return EvidenceBinding(
        available_at=available_at,
        as_of=as_of,
        session=session,
        refs=(_ref(object_id, character, available_at=ref_available_at),),
    )


def _binding_from_ref(ref: EvidenceRef, *, session: date | None = None) -> EvidenceBinding:
    return EvidenceBinding(
        available_at=SNAPSHOT_AT, as_of=SNAPSHOT_AT, session=session, refs=(ref,)
    )


def _ref(object_id: str, character: str, *, available_at: datetime = SNAPSHOT_AT) -> EvidenceRef:
    return EvidenceRef(
        source="fixture",
        object_id=object_id,
        available_at=available_at,
        location=f"fixture://{object_id}",
        content_hash=character * 64,
    )
