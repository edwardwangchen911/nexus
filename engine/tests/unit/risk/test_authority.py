from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.risk.authority import build_risk_market_snapshot, canonical_query_authority_hash
from nexus.risk.canonical import canonical_risk_market_authority_hash
from nexus.risk.models import AuthorityQueryProof, RiskPrice, RiskSectorExposure
from nexus.strategy.forward import ForwardStrategyDecision, ForwardStrategyTarget

NOW = datetime(2026, 8, 21, 20, tzinfo=UTC)
FIRST_ID = UUID(int=10)
SECOND_ID = UUID(int=11)


def _decision() -> ForwardStrategyDecision:
    return ForwardStrategyDecision(
        account_id=UUID(int=1),
        portfolio_id=UUID(int=2),
        strategy_id="baseline",
        strategy_version="baseline_v1",
        decision_session=date(2026, 8, 21),
        decision_time=NOW,
        execution_session=date(2026, 8, 24),
        source_policy_id=UUID(int=3),
        calendar_authority_id=UUID(int=4),
        universe_snapshot_id=UUID(int=5),
        quality_report_id=UUID(int=6),
        portfolio_projection_hash="a" * 64,
        authority_hash="b" * 64,
        strategy_parameters_hash="c" * 64,
        decision_hash="d" * 64,
        cash_weight=Decimal("0.75"),
        reason="WEEKLY_REBALANCE",
        targets=(
            ForwardStrategyTarget(
                instrument_id=FIRST_ID,
                target_weight=Decimal("0.125"),
                exit=False,
                strategy_priority=1,
                reason="CURRENT",
            ),
            ForwardStrategyTarget(
                instrument_id=SECOND_ID,
                target_weight=Decimal("0.125"),
                exit=False,
                strategy_priority=2,
                reason="NEW",
            ),
        ),
    )


def _price(instrument_id: UUID) -> RiskPrice:
    return RiskPrice(
        instrument_id=instrument_id,
        authority_id=UUID(int=100 + instrument_id.int),
        market="US",
        currency="USD",
        asset_type="common_stock",
        close=Decimal("100"),
        source="fosun",
        delivery_mode="delayed",
        available_at=NOW,
        content_hash="a" * 64,
    )


def _sector(instrument_id: UUID) -> RiskSectorExposure:
    return RiskSectorExposure(
        instrument_id=instrument_id,
        authority_id=UUID(int=200 + instrument_id.int),
        sector="technology",
        weight=Decimal(1),
        content_hash="b" * 64,
    )


def _proof(kind: str, index: int) -> AuthorityQueryProof:
    return AuthorityQueryProof(
        kind=kind,
        authority_id=UUID(int=300 + index),
        complete=True,
        blocked=False,
        available_at=NOW,
        content_hash=f"{index}" * 64,
    )


def _build(**overrides: object):
    values: dict[str, object] = {
        "decision": _decision(),
        "required_instrument_ids": (FIRST_ID, SECOND_ID),
        "whitelisted_instrument_ids": (FIRST_ID, SECOND_ID),
        "prices": (_price(FIRST_ID), _price(SECOND_ID)),
        "sector_exposures": (_sector(FIRST_ID), _sector(SECOND_ID)),
        "query_proofs": (
            _proof("corporate_action", 1),
            _proof("suspension", 2),
            _proof("delisting", 3),
        ),
    }
    values.update(overrides)
    return build_risk_market_snapshot(**values)


def test_authority_builds_exact_stable_snapshot_and_hash() -> None:
    snapshot = _build()
    reversed_snapshot = _build(
        required_instrument_ids=(SECOND_ID, FIRST_ID),
        whitelisted_instrument_ids=(SECOND_ID, FIRST_ID),
        prices=tuple(reversed((_price(FIRST_ID), _price(SECOND_ID)))),
        sector_exposures=tuple(reversed((_sector(FIRST_ID), _sector(SECOND_ID)))),
        query_proofs=tuple(
            reversed(
                (
                    _proof("corporate_action", 1),
                    _proof("suspension", 2),
                    _proof("delisting", 3),
                )
            )
        ),
    )

    assert snapshot.authority_hash == canonical_risk_market_authority_hash(snapshot)
    assert reversed_snapshot.authority_hash == snapshot.authority_hash


def test_query_authority_hash_rejects_non_utc_timestamp() -> None:
    with pytest.raises(ValueError, match="UTC"):
        canonical_query_authority_hash(
            source_policy_id=UUID(int=3),
            universe_snapshot_id=UUID(int=5),
            decision_session=date(2026, 8, 21),
            kind="suspension",
            required_instrument_ids=(FIRST_ID,),
            blocked_instrument_ids=(),
            complete=True,
            source_watermark_hash="a" * 64,
            available_at=datetime(
                2026,
                8,
                22,
                4,
                tzinfo=timezone(timedelta(hours=8)),
            ),
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"prices": (_price(FIRST_ID),)}, "prices must exactly cover"),
        (
            {"whitelisted_instrument_ids": (FIRST_ID,)},
            "whitelist must exactly cover",
        ),
        (
            {
                "sector_exposures": (
                    _sector(FIRST_ID).model_copy(update={"weight": Decimal("0.5")}),
                    _sector(SECOND_ID),
                )
            },
            "sector exposures must sum to one",
        ),
        (
            {
                "prices": (
                    _price(FIRST_ID).model_copy(
                        update={"available_at": datetime(2026, 8, 21, 21, tzinfo=UTC)}
                    ),
                    _price(SECOND_ID),
                )
            },
            "available after decision time",
        ),
        (
            {
                "query_proofs": (
                    _proof("corporate_action", 1),
                    _proof("corporate_action", 2),
                    _proof("delisting", 3),
                )
            },
            "query proofs must exactly cover",
        ),
    ),
)
def test_incomplete_or_mismatched_authority_fails_closed(
    overrides: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        _build(**overrides)
