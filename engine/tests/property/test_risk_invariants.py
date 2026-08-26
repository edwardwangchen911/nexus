from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

from hypothesis import given, settings
from hypothesis import strategies as st
from nexus.risk.evaluate import evaluate_order_batch
from nexus.risk.models import (
    AuthorityQueryProof,
    OrderCandidate,
    PaperPortfolioProjection,
    PaperPositionBalance,
    RiskMarketSnapshot,
    RiskPrice,
    RiskSectorExposure,
)
from nexus.risk.policy import RiskPolicy

AT = datetime(2026, 8, 21, 20, tzinfo=UTC)
ACCOUNT_ID = UUID(int=1)
PORTFOLIO_ID = UUID(int=2)
FIRST_ID = UUID(int=101)
SECOND_ID = UUID(int=102)


def _sector(instrument_id: UUID, sector: str) -> RiskSectorExposure:
    return RiskSectorExposure(
        instrument_id=instrument_id,
        authority_id=UUID(int=instrument_id.int + 1000),
        sector=sector,
        weight=Decimal("1"),
        content_hash="a" * 64,
    )


def _market(items: tuple[tuple[UUID, Decimal, str], ...]) -> RiskMarketSnapshot:
    ids = tuple(item[0] for item in items)
    return RiskMarketSnapshot(
        decision_session=date(2026, 8, 21),
        execution_session=date(2026, 8, 24),
        decision_time=AT,
        source_policy_id=UUID(int=3),
        calendar_authority_id=UUID(int=4),
        universe_snapshot_id=UUID(int=5),
        quality_report_id=UUID(int=6),
        required_instrument_ids=ids,
        whitelisted_instrument_ids=ids,
        prices=tuple(
            RiskPrice(
                instrument_id=instrument_id,
                authority_id=UUID(int=instrument_id.int + 500),
                market="US",
                currency="USD",
                asset_type="common_stock",
                close=close,
                source="fixture",
                delivery_mode="fixture",
                available_at=AT,
                content_hash="b" * 64,
            )
            for instrument_id, close, _ in items
        ),
        sector_exposures=tuple(
            _sector(instrument_id, sector) for instrument_id, _, sector in items
        ),
        query_proofs=tuple(
            AuthorityQueryProof(
                kind=kind,
                authority_id=UUID(int=index),
                complete=True,
                blocked=False,
                available_at=AT,
                content_hash=character * 64,
            )
            for kind, index, character in (
                ("corporate_action", 20, "a"),
                ("suspension", 21, "b"),
                ("delisting", 22, "c"),
            )
        ),
        authority_hash="d" * 64,
    )


def _projection(
    *,
    positions: tuple[PaperPositionBalance, ...] = (),
) -> PaperPortfolioProjection:
    nav = Decimal("10000") + sum(
        (position.quantity * position.reference_close for position in positions),
        start=Decimal(0),
    )
    return PaperPortfolioProjection(
        account_id=ACCOUNT_ID,
        portfolio_id=PORTFOLIO_ID,
        as_of=AT,
        currency="USD",
        account_model="cash",
        position_model="long_only",
        available_cash=Decimal("10000"),
        reserved_cash=Decimal("0"),
        positions=positions,
        open_orders=(),
        nav=nav,
        peak_nav=nav,
        drawdown_state="unlocked",
        source_facts_hash="f" * 64,
        projection_hash="e" * 64,
    )


def _candidate(
    instrument_id: UUID,
    *,
    quantity: int,
    close: Decimal,
    target_weight: Decimal,
    sector: str,
) -> OrderCandidate:
    return OrderCandidate(
        instrument_id=instrument_id,
        side="buy",
        desired_quantity=Decimal(quantity),
        target_weight=target_weight,
        strategy_priority=2,
        reference_close=close,
        asset_type="common_stock",
        currency="USD",
        sector_exposures=(_sector(instrument_id, sector),),
    )


@settings(max_examples=100, deadline=None)
@given(
    first_quantity=st.integers(min_value=1, max_value=200),
    second_quantity=st.integers(min_value=1, max_value=200),
    first_close=st.decimals(
        min_value=Decimal("5"),
        max_value=Decimal("500"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ),
    second_close=st.decimals(
        min_value=Decimal("5"),
        max_value=Decimal("500"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ),
)
def test_approved_buys_never_break_hard_limits_and_permutation_is_stable(
    first_quantity: int,
    second_quantity: int,
    first_close: Decimal,
    second_close: Decimal,
) -> None:
    first = _candidate(
        FIRST_ID,
        quantity=first_quantity,
        close=first_close,
        target_weight=Decimal("0.125"),
        sector="technology",
    )
    second = _candidate(
        SECOND_ID,
        quantity=second_quantity,
        close=second_close,
        target_weight=Decimal("0.10"),
        sector="health_care",
    )
    market = _market(
        (
            (FIRST_ID, first_close, "technology"),
            (SECOND_ID, second_close, "health_care"),
        )
    )
    portfolio = _projection()

    forward = evaluate_order_batch((first, second), portfolio, market, RiskPolicy.v1())
    reverse = evaluate_order_batch((second, first), portfolio, market, RiskPolicy.v1())

    assert forward == reverse
    for result in forward:
        assert result.approved_quantity == result.approved_quantity.to_integral_value()
        assert Decimal(0) <= result.approved_quantity <= result.desired_quantity
        if result.outcome == "rejected":
            assert result.approved_quantity == 0
            assert result.reserve_amount == 0
            continue
        assert result.projected_available_cash >= 0
        assert result.projected_cash_ratio >= Decimal("0.10")
        assert result.projected_name_ratio <= Decimal("0.15")
        assert all(ratio <= Decimal("0.30") for _, ratio in result.projected_sector_ratios)


@settings(max_examples=75, deadline=None)
@given(
    quantity=st.integers(min_value=1, max_value=200),
    reserved=st.integers(min_value=0, max_value=200),
    requested=st.integers(min_value=1, max_value=300),
    close=st.decimals(
        min_value=Decimal("5"),
        max_value=Decimal("500"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ),
)
def test_sell_never_exceeds_unreserved_position(
    quantity: int,
    reserved: int,
    requested: int,
    close: Decimal,
) -> None:
    bounded_reserved = min(quantity, reserved)
    position = PaperPositionBalance(
        instrument_id=FIRST_ID,
        quantity=Decimal(quantity),
        reserved_quantity=Decimal(bounded_reserved),
        cost_basis=Decimal(quantity) * close,
        reference_close=close,
    )
    candidate = OrderCandidate(
        instrument_id=FIRST_ID,
        side="sell",
        desired_quantity=Decimal(requested),
        target_weight=Decimal("0"),
        strategy_priority=0,
        reference_close=close,
        asset_type="common_stock",
        currency="USD",
        sector_exposures=(_sector(FIRST_ID, "technology"),),
    )

    result = evaluate_order_batch(
        (candidate,),
        _projection(positions=(position,)),
        _market(((FIRST_ID, close, "technology"),)),
        RiskPolicy.v1(),
    )[0]

    assert result.approved_quantity <= Decimal(quantity - bounded_reserved)
    assert result.approved_quantity == result.approved_quantity.to_integral_value()
