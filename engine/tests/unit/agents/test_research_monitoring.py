"""Price monitoring is a review trigger, independent of execution permission."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.monitoring.contracts import PriceObservation, PriceReviewCondition
from nexus.monitoring.evaluation import EpisodeState, advance_episode, evaluate_price

NOW = datetime(2026, 10, 23, 15, tzinfo=UTC)


def condition(**changes):
    return PriceReviewCondition.model_validate({
        "signal": "schwab.l1.bid_ask_midpoint", "operator": "gte",
        "threshold": "100", "unit": "currency", "currency": "USD",
        "purpose": "review", "source": "schwab", "feed": "l1",
        "price_basis": "unadjusted_bid_ask_midpoint", "max_age_seconds": 60,
        **changes,
    })


def observation(**changes):
    return PriceObservation.model_validate({
        "instrument_id": UUID(int=1), "quote_id": UUID(int=2),
        "artifact_id": UUID(int=3), "content_hash": "a" * 64,
        "source": "schwab", "feed": "l1", "currency": "USD",
        "price_basis": "unadjusted_bid_ask_midpoint", "delivery_mode": "real_time",
        "evidence_status": "verified", "bid": Decimal("99"), "ask": Decimal("101"),
        "observed_at": NOW, "available_at": NOW, "received_at": NOW,
        **changes,
    })


@pytest.mark.parametrize("change", [
    {"signal": "committee.evidence_valid"}, {"threshold": "0"},
    {"threshold": "NaN"}, {"threshold": 100.0}, {"currency": "HKD"},
    {"unit": "ratio"}, {"source": "fixture"}, {"max_age_seconds": 300},
    {"price_basis": "adjusted_close"}, {"purpose": "sell"},
])
def test_unregistered_or_unbound_condition_cannot_execute(change):
    with pytest.raises(ValueError):
        condition(**change)


@pytest.mark.parametrize("operator,threshold,expected", [
    ("gte", "100", "true"), ("gte", "100.000000000001", "false"),
    ("lte", "100", "true"), ("lte", "99.999999999999", "false"),
])
def test_exact_midpoint_does_not_use_rounding_or_trade_price(operator, threshold, expected):
    result = evaluate_price(condition(operator=operator, threshold=threshold), observation(),
                            instrument_id=UUID(int=1), now=NOW)
    assert result.result == expected
    assert result.reason is None


@pytest.mark.parametrize("changes", [
    {"observed_at": NOW - timedelta(seconds=61)},
    {"received_at": NOW + timedelta(seconds=1)},
    {"instrument_id": UUID(int=4)}, {"currency": "HKD"},
    {"delivery_mode": "delayed"}, {"evidence_status": "legacy_unverified"},
])
def test_unknown_observations_are_never_false(changes):
    result = evaluate_price(condition(), observation(**changes), instrument_id=UUID(int=1), now=NOW)
    assert result.result == "unknown"
    assert result.reason is not None


def test_missing_observation_is_unknown():
    assert evaluate_price(condition(), None, instrument_id=UUID(int=1), now=NOW).result == "unknown"


def test_episode_latch_survives_unknown_and_plan_replacement():
    state = EpisodeState()
    state, cause = advance_episode(state, "true")
    assert cause == "initial_match" and state.sequence == 1
    # This durable state belongs to the semantic condition, not a plan generation.
    state = EpisodeState.model_validate_json(state.model_dump_json())
    for value in ("true", "unknown", "unknown", "true"):
        state, cause = advance_episode(state, value)
        assert cause is None and state.sequence == 1 and state.latched
    state, cause = advance_episode(state, "false")
    assert cause is None and not state.latched
    state, cause = advance_episode(state, "true")
    assert cause == "crossed" and state.sequence == 2


def test_numeric_equivalence_preserves_semantic_identity():
    assert condition(threshold="100").semantic_hash == condition(threshold="100.00").semantic_hash
    assert condition(threshold="101").semantic_hash != condition().semantic_hash


def test_unknown_first_does_not_fabricate_a_crossing():
    state, cause = advance_episode(EpisodeState(), "unknown")
    assert cause is None
    state, cause = advance_episode(state, "true")
    assert cause == "initial_match"


@pytest.mark.parametrize("veto", ["bear", "evidence_audit", "wrong_hash", "future"])
def test_compilation_requires_independent_reviews_and_visible_original_evidence(veto):
    from nexus.domain.evidence import EvidenceRef
    from nexus.monitoring.contracts import PriceMonitoringProposal, PriceMonitoringReview
    from nexus.monitoring.evaluation import compile_proposal

    evidence = EvidenceRef(source="fixture", object_id="price-thesis", location="fixture",
                           content_hash="b" * 64, available_at=NOW)
    proposal = PriceMonitoringProposal(condition=condition(), rationale="复核原观点。",
        evidence_refs=(evidence,), review_due_at=NOW + timedelta(hours=1),
        valid_until=NOW + timedelta(hours=2))
    reviews = {role: PriceMonitoringReview(proposal_hash=proposal.content_hash,
        decision="accepted", rationale="限定价格复核，不授予交易。", evidence_refs=(evidence,))
        for role in ("bear", "evidence_audit")}
    assert compile_proposal(proposal, reviews, now=NOW, evidence_refs=(evidence,)) == proposal
    if veto in reviews:
        reviews[veto] = reviews[veto].model_copy(update={"decision": "rejected"})
    elif veto == "wrong_hash":
        reviews["evidence_audit"] = reviews["evidence_audit"].model_copy(
            update={"proposal_hash": "c" * 64})
    else:
        evidence = evidence.model_copy(update={"available_at": NOW + timedelta(seconds=1)})
    with pytest.raises(ValueError, match="MONITOR"):
        compile_proposal(proposal, reviews, now=NOW, evidence_refs=(evidence,))
