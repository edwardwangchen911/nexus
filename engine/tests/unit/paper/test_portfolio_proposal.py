from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.portfolio_synthesis_contracts import PortfolioSynthesisIntentV1
from nexus.paper.portfolio_proposal import PortfolioBaselineV1, build_portfolio_proposal

A, B, C, D = (UUID(int=i) for i in range(1, 5))


def intent(*choices):
    statement = {
        "text": "Evidence supports this comparison.",
        "references": [{"research_output_id": UUID(int=20), "evidence_ref_id": UUID(int=21)}],
    }
    return PortfolioSynthesisIntentV1.model_validate(
        {
            "schema_version": "portfolio_synthesis_intent_v1",
            "input_hash": "a" * 64,
            "compared_instrument_ids": [item[0] for item in choices],
            "choices": [
                {
                    "instrument_id": item[0],
                    "action": item[1],
                    "role": item[2],
                    "rank": item[3],
                    "replaces_instrument_id": None,
                    "depends_on_instrument_ids": [],
                    "thesis": statement,
                    "counter_thesis": statement,
                    "invalidation": statement,
                }
                for item in choices
            ],
            "cash_rationale": statement,
        }
    )


def baseline(weights, cash):
    return PortfolioBaselineV1(
        weights=weights,
        cash_weight=Decimal(cash),
        target_version_id=UUID(int=50),
        content_hash="b" * 64,
    )


def build(base, choices):
    return build_portfolio_proposal(
        active_target=base,
        synthesis=choices,
        synthesis_output_id=UUID(int=30),
        synthesis_output_hash="c" * 64,
        expires_at=datetime(2026, 9, 18, 20, tzinfo=UTC),
    )


def with_dependencies(choices, dependencies, replacements=None):
    document = choices.model_dump()
    for row in document["choices"]:
        row["depends_on_instrument_ids"] = dependencies.get(row["instrument_id"], ())
        row["replaces_instrument_id"] = (replacements or {}).get(row["instrument_id"])
    return PortfolioSynthesisIntentV1.model_validate(document)


@pytest.mark.parametrize(
    "action,old,want",
    [("reduce", "0.025", "0"), ("reduce", "0.05", "0.05"), ("exit", "0.025", "0.05")],
)
def test_replacement_requires_the_declared_reduction_to_apply(action, old, want):
    choices = with_dependencies(
        intent((A, action, "core", None), (B, "include", "tactical", 1)), {}, {B: A}
    )
    result = build(baseline({A: Decimal(old)}, str(1 - Decimal(old))), choices)
    assert result.weights[B] == Decimal(want)
    assert result.reasons[B] == ("DEPENDENCY_NOT_APPLIED" if want == "0" else "INCLUDE")
    assert result.synthesis.choices[1].replaces_instrument_id == A
    assert build(result.baseline, choices) == result


@pytest.mark.parametrize(
    "weights,cash,choices,blocked,reason",
    [
        (
            {A: Decimal("0.15")},
            "0.85",
            ((A, "increase", "core", 1), (B, "include", "tactical", 2)),
            A,
            "SINGLE_NAME_CAP",
        ),
        (
            {C: Decimal("0.775")},
            "0.225",
            ((A, "include", "core", 1), (B, "include", "tactical", 2)),
            A,
            "CASH_FLOOR",
        ),
        (
            {},
            "1",
            (
                (C, "include", "core", 1),
                (A, "include", "tactical", 2),
                (B, "include", "tactical", 3),
            ),
            A,
            "TURNOVER_LIMIT",
        ),
    ],
)
def test_dependent_increase_does_not_ignore_constraint_deferred_predecessor(
    weights, cash, choices, blocked, reason
):
    result = build(baseline(weights, cash), with_dependencies(intent(*choices), {B: (blocked,)}))
    assert result.reasons[blocked] == reason
    assert result.weights[B] == 0
    assert result.reasons[B] == "DEPENDENCY_NOT_APPLIED"


def test_transitive_dependency_deferral_and_maintain_semantics():
    choices = with_dependencies(
        intent(
            (A, "reduce", "core", None),
            (B, "include", "tactical", 1),
            (C, "include", "tactical", 2),
        ),
        {B: (A,), C: (B,)},
    )
    result = build(baseline({A: Decimal("0.025")}, "0.975"), choices)
    assert result.weights == {A: Decimal("0.025"), B: Decimal(0), C: Decimal(0)}
    assert result.reasons[B] == result.reasons[C] == "DEPENDENCY_NOT_APPLIED"
    satisfied = build(baseline({A: Decimal("0.05")}, "0.95"), choices)
    assert satisfied.weights == {A: Decimal(0), B: Decimal("0.05"), C: Decimal("0.05")}
    assert satisfied.cash_weight == Decimal("0.90")
    assert build(satisfied.baseline, choices) == satisfied
    maintained = with_dependencies(
        intent((A, "maintain", "core", None), (B, "include", "tactical", 1)), {B: (A,)}
    )
    assert build(baseline({A: Decimal("0.025")}, "0.975"), maintained).weights[B] == Decimal("0.05")


def test_replacement_cannot_bypass_exit_turnover_rejection():
    choices = with_dependencies(
        intent((A, "exit", "none", None), (B, "include", "tactical", 1)), {}, {B: A}
    )
    with pytest.raises(ValueError, match="^EXIT_TURNOVER_CONFLICT$"):
        build(baseline({A: Decimal("0.15")}, "0.85"), choices)


@pytest.mark.parametrize("kind", ["cycle", "rank_inversion", "missing"])
def test_unsafe_dependency_graph_is_a_known_rejection(kind):
    source = intent((A, "include", "tactical", 1), (B, "include", "tactical", 2))
    if kind == "missing":
        # An unchecked in-process object must not bypass the proposal boundary.
        source = source.model_copy(
            update={
                "choices": (
                    source.choices[0].model_copy(update={"depends_on_instrument_ids": (C,)}),
                    source.choices[1],
                )
            }
        )
    else:
        source = with_dependencies(source, {A: (B,), **({B: (A,)} if kind == "cycle" else {})})
    with pytest.raises(ValueError, match="^PORTFOLIO_DEPENDENCY_CONFLICT$"):
        build(baseline({}, "1"), source)


def test_unmet_exit_dependency_rejects_instead_of_silently_retaining_exit():
    choices = with_dependencies(
        intent((A, "defer", "none", None), (B, "exit", "none", None)), {B: (A,)}
    )
    with pytest.raises(ValueError, match="^PORTFOLIO_DEPENDENCY_CONFLICT$"):
        build(baseline({A: Decimal("0.05"), B: Decimal("0.05")}, "0.90"), choices)


@pytest.mark.parametrize(
    "action,role,old,cash,want,want_cash",
    [
        ("include", "core", "0", "1", "0.10", "0.90"),
        ("include", "tactical", "0", "1", "0.05", "0.95"),
        ("increase", "core", "0.075", "0.925", "0.125", "0.875"),
        ("reduce", "core", "0.075", "0.925", "0.025", "0.975"),
        ("exit", "none", "0.075", "0.925", "0", "1"),
        ("maintain", "core", "0.075", "0.925", "0.075", "0.925"),
        ("defer", "none", "0.075", "0.925", "0.075", "0.925"),
    ],
)
def test_actions_preserve_exact_inherited_baseline(action, role, old, cash, want, want_cash):
    base = baseline({A: Decimal(old)} if old != "0" else {}, cash)
    result = build(
        base, intent((A, action, role, 1 if action in {"include", "increase"} else None))
    )
    assert result.weights[A] == Decimal(want)
    assert result.cash_weight == Decimal(want_cash)
    assert build(base, result.synthesis) == result


def test_omissions_inherit_and_rank_decides_budget():
    result = build(
        baseline({A: Decimal("0.10")}, "0.90"),
        intent((B, "include", "tactical", 2), (C, "include", "core", 1)),
    )
    assert result.weights == {A: Decimal("0.10"), B: Decimal(0), C: Decimal("0.10")}
    assert result.cash_weight == Decimal("0.80")
    assert result.reasons[B] == "TURNOVER_LIMIT"


def test_two_increases_cap_and_grandfathered_breaches():
    result = build(
        baseline({A: Decimal("0.16")}, "0.84"),
        intent(
            (A, "increase", "core", 1),
            (B, "include", "tactical", 2),
            (C, "include", "tactical", 3),
            (D, "include", "tactical", 4),
        ),
    )
    assert result.weights == {
        A: Decimal("0.16"),
        B: Decimal("0.05"),
        C: Decimal("0.05"),
        D: Decimal(0),
    }
    assert result.reasons[A] == "SINGLE_NAME_CAP"
    assert result.reasons[D] == "MAX_INCREASES"


def test_cash_floor_does_not_normalize_inherited_breach():
    result = build(
        baseline({A: Decimal("0.85")}, "0.15"),
        intent((A, "reduce", "core", None), (B, "include", "tactical", 1)),
    )
    assert result.weights == {A: Decimal("0.80"), B: Decimal(0)}
    assert result.cash_weight == Decimal("0.20")
    assert result.reasons[B] == "CASH_FLOOR"


def test_exit_cannot_be_silently_clipped_to_turnover():
    with pytest.raises(ValueError, match="EXIT_TURNOVER_CONFLICT"):
        build(baseline({A: Decimal("0.15")}, "0.85"), intent((A, "exit", "none", None)))


def test_partial_step_is_deferred_and_policy_mutation_rejected():
    result = build(baseline({A: Decimal("0.13")}, "0.87"), intent((A, "increase", "core", 1)))
    assert result.weights[A] == Decimal("0.13")
    assert result.reasons[A] == "SINGLE_NAME_CAP"
    from nexus.paper.portfolio_policy import PortfolioAllocationPolicyV1

    with pytest.raises(ValueError, match="POLICY"):
        build_portfolio_proposal(
            active_target=result.baseline,
            synthesis=result.synthesis,
            synthesis_output_id=result.synthesis_output_id,
            synthesis_output_hash=result.synthesis_output_hash,
            expires_at=result.expires_at,
            policy=PortfolioAllocationPolicyV1(max_increases=3),
        )


def test_initial_ratios_round_each_security_without_redistribution():
    from nexus.paper.portfolio_proposal import initial_projection_baseline
    from nexus.risk.models import PaperPortfolioProjection, PaperPositionBalance

    positions = tuple(
        PaperPositionBalance(
            instrument_id=key,
            quantity=Decimal(1),
            reserved_quantity=Decimal(0),
            cost_basis=Decimal(1),
            reference_close=Decimal(1),
        )
        for key in (A, B)
    )
    projection = PaperPortfolioProjection(
        account_id=A,
        portfolio_id=B,
        as_of=datetime(2026, 9, 18, tzinfo=UTC),
        currency="USD",
        account_model="cash",
        position_model="long_only",
        available_cash=Decimal(1),
        reserved_cash=Decimal(0),
        positions=positions,
        open_orders=(),
        nav=Decimal(3),
        peak_nav=Decimal(3),
        drawdown_state="unlocked",
        source_facts_hash="a" * 64,
        projection_hash="b" * 64,
    )
    result = initial_projection_baseline(projection, approval_id=C, approval_hash="c" * 64)
    assert result.weights == {A: Decimal("0.333333333333"), B: Decimal("0.333333333333")}
    assert result.cash_weight == Decimal("0.333333333334")
    assert sum(result.weights.values(), result.cash_weight) == 1


def test_target_content_ignores_research_identity_and_zero_candidates():
    from nexus.paper.portfolio_proposal import canonical_portfolio_target_content_hash

    first = build(baseline({A: Decimal("0.10")}, "0.90"), intent((A, "maintain", "core", None)))
    second = build(first.baseline, intent((B, "defer", "none", None)))
    assert first.decision_hash != second.decision_hash
    assert canonical_portfolio_target_content_hash(
        first
    ) == canonical_portfolio_target_content_hash(second)


@pytest.mark.parametrize(
    "count,cash,expected", [(40, "1", "0.024390243920"), (6, "0", None), (3, "0", "0.000000000001")]
)
def test_initial_rounding_bounds_do_not_redistribute_securities(count, cash, expected):
    from nexus.paper.portfolio_proposal import initial_projection_baseline
    from nexus.risk.models import PaperPortfolioProjection, PaperPositionBalance

    positions = tuple(
        PaperPositionBalance(
            instrument_id=UUID(int=i + 1),
            quantity=Decimal(1),
            reserved_quantity=Decimal(0),
            cost_basis=Decimal(1),
            reference_close=Decimal(1),
        )
        for i in range(count)
    )
    projection = PaperPortfolioProjection(
        account_id=A,
        portfolio_id=B,
        as_of=datetime(2026, 9, 18, tzinfo=UTC),
        currency="USD",
        account_model="cash",
        position_model="long_only",
        available_cash=Decimal(cash),
        reserved_cash=Decimal(0),
        positions=positions,
        open_orders=(),
        nav=Decimal(count) + Decimal(cash),
        peak_nav=Decimal(count) + Decimal(cash),
        drawdown_state="unlocked",
        source_facts_hash="a" * 64,
        projection_hash="b" * 64,
    )
    if expected is None:
        with pytest.raises(ValueError, match="INITIAL_PROJECTION_WEIGHT_PRECISION_UNSUPPORTED"):
            initial_projection_baseline(projection, approval_id=C, approval_hash="c" * 64)
    else:
        result = initial_projection_baseline(projection, approval_id=C, approval_hash="c" * 64)
        assert result.cash_weight == Decimal(expected)
        assert len(set(result.weights.values())) == 1
        assert sum(result.weights.values(), result.cash_weight) == 1
        assert (
            initial_projection_baseline(projection, approval_id=C, approval_hash="c" * 64) == result
        )
