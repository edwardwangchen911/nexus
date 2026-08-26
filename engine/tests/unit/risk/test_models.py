from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.risk.models import (
    AuthorityQueryProof,
    OrderCandidate,
    PaperOpenOrder,
    PaperPortfolioProjection,
    PaperPositionBalance,
    RiskMarketSnapshot,
    RiskPrice,
    RiskSectorExposure,
)
from pydantic import ValidationError

ACCOUNT_ID = UUID("00000000-0000-0000-0000-000000000001")
PORTFOLIO_ID = UUID("00000000-0000-0000-0000-000000000002")
INSTRUMENT_ID = UUID("00000000-0000-0000-0000-000000000003")
SECOND_INSTRUMENT_ID = UUID("00000000-0000-0000-0000-000000000004")
DECISION_TIME = datetime(2026, 8, 21, 20, tzinfo=UTC)


def _price(instrument_id: UUID = INSTRUMENT_ID) -> RiskPrice:
    return RiskPrice(
        instrument_id=instrument_id,
        authority_id=UUID(int=instrument_id.int + 100),
        close=Decimal("100"),
        source="fosun",
        delivery_mode="delayed",
        available_at=DECISION_TIME,
    )


def _sector(instrument_id: UUID = INSTRUMENT_ID) -> RiskSectorExposure:
    return RiskSectorExposure(
        instrument_id=instrument_id,
        authority_id=UUID(int=instrument_id.int + 200),
        sector="technology",
        weight=Decimal("1"),
    )


def _proof(kind: str, *, blocked: bool = False) -> AuthorityQueryProof:
    return AuthorityQueryProof(
        kind=kind,
        authority_id=UUID(int=300 + len(kind)),
        complete=True,
        blocked=blocked,
        available_at=DECISION_TIME,
        content_hash="a" * 64,
    )


def _market(**overrides: object) -> RiskMarketSnapshot:
    values: dict[str, object] = {
        "decision_session": date(2026, 8, 21),
        "execution_session": date(2026, 8, 24),
        "decision_time": DECISION_TIME,
        "source_policy_id": UUID(int=10),
        "calendar_authority_id": UUID(int=11),
        "universe_snapshot_id": UUID(int=12),
        "quality_report_id": UUID(int=13),
        "required_instrument_ids": (INSTRUMENT_ID,),
        "whitelisted_instrument_ids": (INSTRUMENT_ID,),
        "prices": (_price(),),
        "sector_exposures": (_sector(),),
        "query_proofs": (
            _proof("corporate_action"),
            _proof("suspension"),
            _proof("delisting"),
        ),
        "authority_hash": "b" * 64,
    }
    values.update(overrides)
    return RiskMarketSnapshot(**values)


def test_market_snapshot_requires_an_exact_price_set() -> None:
    with pytest.raises(ValidationError, match="prices must exactly cover required instruments"):
        _market(
            required_instrument_ids=(INSTRUMENT_ID, SECOND_INSTRUMENT_ID),
            whitelisted_instrument_ids=(INSTRUMENT_ID, SECOND_INSTRUMENT_ID),
            sector_exposures=(_sector(), _sector(SECOND_INSTRUMENT_ID)),
        )


def test_market_snapshot_requires_full_sector_weight() -> None:
    with pytest.raises(ValidationError, match="sector exposures must sum to one"):
        _market(
            sector_exposures=(
                RiskSectorExposure(
                    instrument_id=INSTRUMENT_ID,
                    authority_id=UUID(int=203),
                    sector="technology",
                    weight=Decimal("0.9"),
                ),
            )
        )


def test_market_snapshot_rejects_late_authority() -> None:
    with pytest.raises(ValidationError, match="available after decision time"):
        _market(
            prices=(
                RiskPrice(
                    instrument_id=INSTRUMENT_ID,
                    authority_id=UUID(int=103),
                    close=Decimal("100"),
                    source="fosun",
                    delivery_mode="delayed",
                    available_at=datetime(2026, 8, 21, 20, 0, 1, tzinfo=UTC),
                ),
            )
        )


def test_market_snapshot_rejects_incomplete_negative_proof() -> None:
    with pytest.raises(ValidationError, match="query proofs must be complete"):
        _market(
            query_proofs=(
                AuthorityQueryProof(
                    kind="corporate_action",
                    authority_id=UUID(int=301),
                    complete=False,
                    blocked=False,
                    available_at=DECISION_TIME,
                    content_hash="a" * 64,
                ),
                _proof("suspension"),
                _proof("delisting"),
            )
        )


def test_projection_rejects_non_usd_account() -> None:
    with pytest.raises(ValidationError, match="USD"):
        PaperPortfolioProjection(
            account_id=ACCOUNT_ID,
            portfolio_id=PORTFOLIO_ID,
            as_of=DECISION_TIME,
            currency="HKD",
            account_model="cash",
            position_model="long_only",
            available_cash=Decimal("10000"),
            reserved_cash=Decimal("0"),
            positions=(),
            open_orders=(),
            nav=Decimal("10000"),
            peak_nav=Decimal("10000"),
            drawdown_state="unlocked",
            projection_hash="c" * 64,
        )


def test_projection_rejects_duplicate_positions() -> None:
    position = PaperPositionBalance(
        instrument_id=INSTRUMENT_ID,
        quantity=Decimal("2"),
        reserved_quantity=Decimal("0"),
        cost_basis=Decimal("150"),
        reference_close=Decimal("100"),
    )
    with pytest.raises(ValidationError, match="positions must be unique"):
        PaperPortfolioProjection(
            account_id=ACCOUNT_ID,
            portfolio_id=PORTFOLIO_ID,
            as_of=DECISION_TIME,
            currency="USD",
            account_model="cash",
            position_model="long_only",
            available_cash=Decimal("9600"),
            reserved_cash=Decimal("0"),
            positions=(position, position),
            open_orders=(),
            nav=Decimal("10000"),
            peak_nav=Decimal("10000"),
            drawdown_state="unlocked",
            projection_hash="c" * 64,
        )


def test_projection_rejects_reserved_position_above_quantity() -> None:
    with pytest.raises(ValidationError, match="reserved quantity"):
        PaperPositionBalance(
            instrument_id=INSTRUMENT_ID,
            quantity=Decimal("2"),
            reserved_quantity=Decimal("3"),
            cost_basis=Decimal("150"),
            reference_close=Decimal("100"),
        )


def test_projection_rejects_duplicate_open_orders() -> None:
    order = PaperOpenOrder(
        order_id=UUID(int=500),
        instrument_id=INSTRUMENT_ID,
        side="buy",
        quantity=Decimal("1"),
        worst_price=Decimal("101"),
        reserved_cash=Decimal("101"),
        reserved_quantity=Decimal("0"),
        status="submitted",
    )
    with pytest.raises(ValidationError, match="open orders must be unique"):
        PaperPortfolioProjection(
            account_id=ACCOUNT_ID,
            portfolio_id=PORTFOLIO_ID,
            as_of=DECISION_TIME,
            currency="USD",
            account_model="cash",
            position_model="long_only",
            available_cash=Decimal("9798"),
            reserved_cash=Decimal("202"),
            positions=(),
            open_orders=(order, order),
            nav=Decimal("10000"),
            peak_nav=Decimal("10000"),
            drawdown_state="unlocked",
            projection_hash="c" * 64,
        )


def test_order_candidate_requires_whole_shares() -> None:
    with pytest.raises(ValidationError, match="whole"):
        OrderCandidate(
            instrument_id=INSTRUMENT_ID,
            side="buy",
            desired_quantity=Decimal("1.5"),
            target_weight=Decimal("0.125"),
            strategy_priority=2,
            reference_close=Decimal("100"),
            asset_type="common_stock",
            currency="USD",
            sector_exposures=(_sector(),),
        )


def test_financial_boundary_rejects_binary_float_and_over_precision() -> None:
    with pytest.raises(ValidationError, match="binary float"):
        OrderCandidate(
            instrument_id=INSTRUMENT_ID,
            side="buy",
            desired_quantity=Decimal("1"),
            target_weight=0.125,
            strategy_priority=2,
            reference_close=Decimal("100"),
            asset_type="common_stock",
            currency="USD",
            sector_exposures=(_sector(),),
        )
    with pytest.raises(ValidationError, match="12 decimal places"):
        OrderCandidate(
            instrument_id=INSTRUMENT_ID,
            side="buy",
            desired_quantity=Decimal("1"),
            target_weight=Decimal("0.1234567890123"),
            strategy_priority=2,
            reference_close=Decimal("100"),
            asset_type="common_stock",
            currency="USD",
            sector_exposures=(_sector(),),
        )
