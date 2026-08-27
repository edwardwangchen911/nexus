import hashlib
import json
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.models import LedgerEntry, LedgerTransaction
from nexus.risk.canonical import (
    canonical_forward_strategy_decision_hash,
    canonical_legacy_forward_strategy_decision_hash,
    canonical_paper_ledger_transaction_hash,
    canonical_paper_order_batch_hash,
    canonical_paper_projection_hash,
    canonical_risk_batch_input_hash,
    canonical_risk_evaluation_result_hash,
)
from nexus.risk.models import (
    AuthorityQueryProof,
    OrderCandidate,
    PaperOpenOrder,
    PaperPortfolioProjection,
    RiskEvaluation,
    RiskMarketSnapshot,
    RiskPrice,
    RiskRuleEvidence,
    RiskSectorExposure,
)
from nexus.strategy.forward import ForwardStrategyDecision, ForwardStrategyTarget

AT = datetime(2026, 8, 21, 20, tzinfo=UTC)
ACCOUNT_ID = UUID(int=1)
PORTFOLIO_ID = UUID(int=2)
INSTRUMENT_ID = UUID(int=3)


def _digest(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _target() -> ForwardStrategyTarget:
    return ForwardStrategyTarget(
        instrument_id=INSTRUMENT_ID,
        target_weight=Decimal("0.125"),
        exit=False,
        strategy_priority=2,
        reason="BASELINE_SELECTED",
    )


def _decision(**overrides: object) -> ForwardStrategyDecision:
    values: dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "portfolio_id": PORTFOLIO_ID,
        "strategy_id": "baseline",
        "strategy_version": "baseline_v1",
        "strategy_definition_id": UUID(int=8),
        "registry_event_id": UUID(int=9),
        "champion_generation": 1,
        "decision_session": date(2026, 8, 21),
        "decision_time": AT,
        "execution_session": date(2026, 8, 24),
        "source_policy_id": UUID(int=4),
        "calendar_authority_id": UUID(int=5),
        "universe_snapshot_id": UUID(int=6),
        "quality_report_id": UUID(int=7),
        "portfolio_projection_hash": "a" * 64,
        "authority_hash": "b" * 64,
        "strategy_parameters_hash": "c" * 64,
        "decision_hash": "d" * 64,
        "cash_weight": Decimal("0.875"),
        "reason": "WEEKLY_REBALANCE",
        "targets": (_target(),),
    }
    values.update(overrides)
    return ForwardStrategyDecision(**values)


def _sector() -> RiskSectorExposure:
    return RiskSectorExposure(
        instrument_id=INSTRUMENT_ID,
        authority_id=UUID(int=30),
        sector="technology",
        weight=Decimal("1"),
        content_hash="a" * 64,
    )


def _market() -> RiskMarketSnapshot:
    return RiskMarketSnapshot(
        decision_session=date(2026, 8, 21),
        execution_session=date(2026, 8, 24),
        decision_time=AT,
        source_policy_id=UUID(int=4),
        calendar_authority_id=UUID(int=5),
        universe_snapshot_id=UUID(int=6),
        quality_report_id=UUID(int=7),
        required_instrument_ids=(INSTRUMENT_ID,),
        whitelisted_instrument_ids=(INSTRUMENT_ID,),
        prices=(
            RiskPrice(
                instrument_id=INSTRUMENT_ID,
                authority_id=UUID(int=20),
                market="US",
                currency="USD",
                asset_type="common_stock",
                close=Decimal("100"),
                source="fosun",
                delivery_mode="delayed",
                available_at=AT,
                content_hash="b" * 64,
            ),
        ),
        sector_exposures=(_sector(),),
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
                ("corporate_action", 40, "e"),
                ("suspension", 41, "f"),
                ("delisting", 42, "1"),
            )
        ),
        authority_hash="b" * 64,
    )


def _projection(*, order_id: UUID = UUID(int=50)) -> PaperPortfolioProjection:
    return PaperPortfolioProjection(
        account_id=ACCOUNT_ID,
        portfolio_id=PORTFOLIO_ID,
        as_of=AT,
        currency="USD",
        account_model="cash",
        position_model="long_only",
        available_cash=Decimal("9899"),
        reserved_cash=Decimal("101"),
        positions=(),
        open_orders=(
            PaperOpenOrder(
                order_id=order_id,
                instrument_id=INSTRUMENT_ID,
                side="buy",
                quantity=Decimal("1"),
                worst_price=Decimal("101"),
                reserved_cash=Decimal("101"),
                reserved_quantity=Decimal("0"),
                status="submitted",
            ),
        ),
        nav=Decimal("10000"),
        peak_nav=Decimal("10000"),
        drawdown_state="unlocked",
        source_facts_hash="3" * 64,
        projection_hash="2" * 64,
    )


def _candidate() -> OrderCandidate:
    return OrderCandidate(
        instrument_id=INSTRUMENT_ID,
        side="buy",
        desired_quantity=Decimal("1"),
        target_weight=Decimal("0.125"),
        strategy_priority=2,
        reference_close=Decimal("100"),
        asset_type="common_stock",
        currency="USD",
        sector_exposures=(_sector(),),
    )


def _evaluation(**overrides: object) -> RiskEvaluation:
    values: dict[str, object] = {
        "instrument_id": INSTRUMENT_ID,
        "side": "buy",
        "outcome": "approved",
        "desired_quantity": Decimal("1"),
        "approved_quantity": Decimal("1"),
        "target_weight": Decimal("0.125"),
        "strategy_priority": 2,
        "reference_close": Decimal("100"),
        "worst_price": Decimal("101"),
        "reserve_amount": Decimal("101"),
        "projected_available_cash": Decimal("9798"),
        "projected_cash_ratio": Decimal("0.9798"),
        "projected_name_ratio": Decimal("0.01"),
        "projected_sector_ratios": (("technology", Decimal("0.01")),),
        "rules": (
            RiskRuleEvidence(
                rule_code="CASH_FLOOR",
                passed=True,
                limit=Decimal("0.10"),
                observed=Decimal("0.9798"),
                remaining=Decimal("0.8798"),
                authority_ids=(UUID(int=20),),
            ),
        ),
        "result_hash": "3" * 64,
    }
    values.update(overrides)
    return RiskEvaluation(**values)


def _transaction(*, transaction_id: UUID = UUID(int=60)) -> LedgerTransaction:
    return LedgerTransaction(
        transaction_id=transaction_id,
        account_id=ACCOUNT_ID,
        transaction_type="cash_reservation",
        reference_type="cash_reservation",
        reference_hash="f" * 64,
        occurred_at=AT,
        entries=(
            LedgerEntry(
                entry_id=UUID(int=61),
                entry_seq=1,
                account_code="asset.cash.reserved.usd",
                currency="USD",
                debit_amount=Decimal("101"),
                credit_amount=Decimal("0"),
            ),
            LedgerEntry(
                entry_id=UUID(int=62),
                entry_seq=2,
                account_code="asset.cash.available.usd",
                currency="USD",
                debit_amount=Decimal("0"),
                credit_amount=Decimal("101"),
            ),
        ),
        transaction_hash="4" * 64,
    )


def test_forward_decision_hash_matches_the_v2_registry_bound_golden_payload() -> None:
    expected = _digest(
        {
            "account_id": str(ACCOUNT_ID),
            "authority_hash": "b" * 64,
            "calendar_authority_id": str(UUID(int=5)),
            "cash_weight": "0.875000000000",
            "decision_session": "2026-08-21",
            "decision_time": "2026-08-21T20:00:00.000000Z",
            "execution_session": "2026-08-24",
            "champion_generation": 1,
            "portfolio_id": str(PORTFOLIO_ID),
            "portfolio_projection_hash": "a" * 64,
            "quality_report_id": str(UUID(int=7)),
            "reason": "WEEKLY_REBALANCE",
            "registry_event_id": str(UUID(int=9)),
            "schema": "forward_strategy_decision_sha256_v2",
            "source_policy_id": str(UUID(int=4)),
            "strategy_id": "baseline",
            "strategy_definition_id": str(UUID(int=8)),
            "strategy_parameters_hash": "c" * 64,
            "strategy_version": "baseline_v1",
            "targets": [
                {
                    "exit": False,
                    "instrument_id": str(INSTRUMENT_ID),
                    "reason": "BASELINE_SELECTED",
                    "strategy_priority": 2,
                    "target_weight": "0.125000000000",
                }
            ],
            "universe_snapshot_id": str(UUID(int=6)),
        }
    )

    assert canonical_forward_strategy_decision_hash(_decision()) == expected


def test_legacy_forward_hash_remains_recomputable_for_migrated_rows() -> None:
    expected = _digest(
        {
            "account_id": str(ACCOUNT_ID),
            "authority_hash": "b" * 64,
            "calendar_authority_id": str(UUID(int=5)),
            "cash_weight": "0.875000000000",
            "decision_session": "2026-08-21",
            "decision_time": "2026-08-21T20:00:00.000000Z",
            "execution_session": "2026-08-24",
            "portfolio_id": str(PORTFOLIO_ID),
            "portfolio_projection_hash": "a" * 64,
            "quality_report_id": str(UUID(int=7)),
            "reason": "WEEKLY_REBALANCE",
            "schema": "forward_strategy_decision_sha256_v1",
            "source_policy_id": str(UUID(int=4)),
            "strategy_id": "baseline",
            "strategy_parameters_hash": "c" * 64,
            "strategy_version": "baseline_v1",
            "targets": [
                {
                    "exit": False,
                    "instrument_id": str(INSTRUMENT_ID),
                    "reason": "BASELINE_SELECTED",
                    "strategy_priority": 2,
                    "target_weight": "0.125000000000",
                }
            ],
            "universe_snapshot_id": str(UUID(int=6)),
        }
    )

    assert canonical_legacy_forward_strategy_decision_hash(_decision()) == expected


def test_projection_hash_excludes_random_order_ids() -> None:
    assert canonical_paper_projection_hash(
        _projection(order_id=UUID(int=500))
    ) == canonical_paper_projection_hash(_projection(order_id=UUID(int=900)))


def test_risk_input_hash_is_independent_of_candidate_input_order() -> None:
    second_sector = RiskSectorExposure(
        instrument_id=UUID(int=8),
        authority_id=UUID(int=31),
        sector="health_care",
        weight=Decimal("1"),
        content_hash="c" * 64,
    )
    second = OrderCandidate(
        instrument_id=UUID(int=8),
        side="buy",
        desired_quantity=Decimal("2"),
        target_weight=Decimal("0.10"),
        strategy_priority=2,
        reference_close=Decimal("50"),
        asset_type="common_stock",
        currency="USD",
        sector_exposures=(second_sector,),
    )

    first_hash = canonical_risk_batch_input_hash(
        candidates=(_candidate(), second),
        portfolio=_projection(),
        market=_market(),
        policy_version="risk_policy_v1",
        policy_hash="5" * 64,
    )
    second_hash = canonical_risk_batch_input_hash(
        candidates=(second, _candidate()),
        portfolio=_projection(),
        market=_market(),
        policy_version="risk_policy_v1",
        policy_hash="5" * 64,
    )

    assert first_hash == second_hash


def test_evaluation_hash_excludes_stored_result_hash() -> None:
    assert canonical_risk_evaluation_result_hash(
        (_evaluation(result_hash="6" * 64),)
    ) == canonical_risk_evaluation_result_hash((_evaluation(result_hash="7" * 64),))


def test_ledger_hash_excludes_random_transaction_and_entry_ids() -> None:
    first = _transaction(transaction_id=UUID(int=60))
    second = LedgerTransaction(
        transaction_id=UUID(int=999),
        account_id=first.account_id,
        transaction_type=first.transaction_type,
        reference_type=first.reference_type,
        reference_hash=first.reference_hash,
        occurred_at=first.occurred_at,
        entries=tuple(
            entry.model_copy(update={"entry_id": UUID(int=900 + entry.entry_seq)})
            for entry in first.entries
        ),
        transaction_hash=first.transaction_hash,
    )

    first_hash = canonical_paper_ledger_transaction_hash(first)
    second_hash = canonical_paper_ledger_transaction_hash(second)

    assert first_hash == second_hash


def test_batch_hash_is_stable_and_binds_every_upstream_hash() -> None:
    first = canonical_paper_order_batch_hash(
        account_id=ACCOUNT_ID,
        decision_hash="8" * 64,
        risk_input_hash="9" * 64,
        evaluations=(_evaluation(result_hash="a" * 64),),
    )
    replay = canonical_paper_order_batch_hash(
        account_id=ACCOUNT_ID,
        decision_hash="8" * 64,
        risk_input_hash="9" * 64,
        evaluations=(_evaluation(result_hash="a" * 64),),
    )
    changed = canonical_paper_order_batch_hash(
        account_id=ACCOUNT_ID,
        decision_hash="8" * 64,
        risk_input_hash="b" * 64,
        evaluations=(_evaluation(result_hash="a" * 64),),
    )

    assert first == replay
    assert first != changed


def test_all_v1_hashes_match_reviewed_golden_vectors() -> None:
    assert canonical_paper_projection_hash(_projection()) == (
        "9796faf0063504eb905307e8c6a2cb14224a3d59cc3af4d72049802810cdb315"
    )
    assert (
        canonical_risk_batch_input_hash(
            candidates=(_candidate(),),
            portfolio=_projection(),
            market=_market(),
            policy_version="risk_policy_v1",
            policy_hash="5" * 64,
        )
        == "25d1ee99b3126b99822f582739a4188603f13b9f6f7b7697f6a08a1cde15a871"
    )
    assert canonical_risk_evaluation_result_hash((_evaluation(),)) == (
        "c394b59e8c38829912d0f70f11f172e67808d23938976f72c915c6baf0172c83"
    )
    assert canonical_paper_ledger_transaction_hash(_transaction()) == (
        "3bded4107fa16459d4c14caf3289f3920baf15279deb5f36e8c9920baa0e2761"
    )
    assert (
        canonical_paper_order_batch_hash(
            account_id=ACCOUNT_ID,
            decision_hash="8" * 64,
            risk_input_hash="9" * 64,
            evaluations=(_evaluation(result_hash="a" * 64),),
        )
        == "ac8b4afe4b7ae861db975fb3154916c9078b17b4d6561a183f7e3efbc441a202"
    )


def test_canonical_hash_rejects_non_utc_time() -> None:
    non_utc = timezone(timedelta(hours=8))
    with pytest.raises(ValueError, match="UTC"):
        canonical_forward_strategy_decision_hash(
            _decision(decision_time=datetime(2026, 8, 22, 4, tzinfo=non_utc))
        )
