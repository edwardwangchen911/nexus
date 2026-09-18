from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.risk.authority import (
    RiskAuthorityInput,
    build_risk_market_snapshot,
    canonical_query_authority_hash,
    require_schwab_execution_scope_capacity,
)
from nexus.risk.canonical import canonical_risk_market_authority_hash
from nexus.risk.models import AuthorityQueryProof, RiskPrice, RiskSectorExposure
from nexus.strategy.forward import ForwardStrategyDecision, ForwardStrategyTarget

NOW = datetime(2026, 8, 21, 20, tzinfo=UTC)
FIRST_ID = UUID(int=10)
SECOND_ID = UUID(int=11)


@pytest.mark.parametrize(
    ("required_count", "max_instruments", "error"),
    [
        (10, 10, None),
        (11, 10, "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED"),
        (30, 30, None),
        (31, 30, "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED"),
        (1, 31, "AGENT_PAPER_PROVIDER_CAPACITY_INVALID"),
    ],
)
def test_schwab_execution_scope_uses_explicit_provider_capacity(
    required_count: int, max_instruments: int, error: str | None
) -> None:
    required = {UUID(int=index + 1) for index in range(required_count)}

    if error is None:
        require_schwab_execution_scope_capacity(
            required, max_instruments=max_instruments
        )
        return

    with pytest.raises(ValueError, match=f"^{error}$"):
        require_schwab_execution_scope_capacity(
            required, max_instruments=max_instruments
        )


def _decision() -> ForwardStrategyDecision:
    return ForwardStrategyDecision(
        account_id=UUID(int=1),
        portfolio_id=UUID(int=2),
        strategy_id="baseline",
        strategy_version="baseline_v1",
        strategy_definition_id=UUID(int=7),
        registry_event_id=UUID(int=8),
        champion_generation=1,
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


def _bound_proofs():
    return tuple(
        AuthorityQueryProof(
            **{
                **_proof(kind, index).model_dump(),
                "authority_id": UUID(int=instrument.int * 100 + index),
                "required_instrument_ids": (instrument,),
            }
        )
        for instrument in (FIRST_ID, SECOND_ID)
        for index, kind in enumerate(("corporate_action", "delisting", "suspension"), 1)
    )


def test_bound_proofs_cover_every_candidate_and_have_stable_hash():
    proofs = _bound_proofs()
    snapshot = _build(query_proofs=proofs)
    reversed_snapshot = _build(query_proofs=tuple(reversed(proofs)))
    assert snapshot == reversed_snapshot
    assert snapshot.query_proofs == proofs
    swapped = tuple(
        proof.model_copy(
            update={
                "required_instrument_ids": (
                    SECOND_ID if proof.required_instrument_ids == (FIRST_ID,) else FIRST_ID,
                ),
            }
        )
        for proof in proofs
    )
    assert _build(query_proofs=swapped).authority_hash != snapshot.authority_hash


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "foreign", "mixed", "multi"])
def test_bound_proofs_reject_inexact_cross_candidate_coverage(mutation):
    proofs = list(_bound_proofs())
    if mutation == "missing":
        proofs.pop()
    elif mutation == "duplicate":
        proofs[-1] = proofs[0]
    elif mutation == "foreign":
        proofs[-1] = proofs[-1].model_copy(update={"required_instrument_ids": (UUID(int=99),)})
    elif mutation == "mixed":
        proofs[-1] = _proof("suspension", 3)
    else:
        proofs[-1] = proofs[-1].model_copy(
            update={"required_instrument_ids": (FIRST_ID, SECOND_ID)}
        )
    with pytest.raises(ValueError):
        _build(query_proofs=proofs)


def test_legacy_proof_serialization_and_hash_are_unchanged():
    proof = _proof("corporate_action", 1)
    assert set(proof.model_dump()) == {
        "kind",
        "authority_id",
        "complete",
        "blocked",
        "available_at",
        "content_hash",
    }
    assert (
        _build().authority_hash
        == "7282987e5cde558d03b104b1eae6bdb55a646d468a0e71a0bd76b9391c06467f"
    )


def test_risk_snapshot_value_allows_same_session_authority() -> None:
    same_session = RiskAuthorityInput(
        decision_session=date(2026, 8, 21),
        execution_session=date(2026, 8, 21),
        decision_time=NOW,
        source_policy_id=UUID(int=3),
        calendar_authority_id=UUID(int=4),
        universe_snapshot_id=UUID(int=5),
        quality_report_id=UUID(int=6),
    )

    snapshot = _build(decision=same_session)

    assert snapshot.execution_session == snapshot.decision_session
    assert snapshot.authority_hash == canonical_risk_market_authority_hash(snapshot)


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
