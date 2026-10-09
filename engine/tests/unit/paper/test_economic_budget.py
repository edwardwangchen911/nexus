from decimal import Decimal

import pytest
from nexus.paper.economic_budget import BudgetRequest, calculate_budget


def request(**changes):
    data = dict(
        business_date_utc="2026-10-08",
        global_limit_usd="10",
        research_cost_usd="1",
        synthesis_cost_usd="1",
        reservations=[],
        formal_tasks=[{"key": "formal-1", "ceiling_usd": "3"}],
        formal_inventory_complete=True,
    )
    return BudgetRequest.model_validate(data | changes)


def test_both_lanes_fit_only_after_protecting_formal_tasks():
    result = calculate_budget(request())
    assert result.status == "both_fit_arithmetic"
    assert result.uncovered_formal_usd == Decimal(3)
    assert result.per_lane_ceiling_usd == Decimal(3)
    assert result.both_lane_ceiling_usd == Decimal(6)
    assert result.reserved is False and result.activation_permitted is False


def test_only_one_lane_fits_neither_may_start():
    result = calculate_budget(request(global_limit_usd="8"))
    assert result.status == "insufficient_for_both"
    assert result.allowed_model_jobs == 0


def test_formal_reservation_not_counted_twice():
    result = calculate_budget(
        request(
            global_limit_usd="9",
            reservations=[{"key": "r1", "state": "reserved", "reserved_usd": "3"}],
            formal_tasks=[{"key": "formal-1", "ceiling_usd": "3", "reservation_key": "r1"}],
        )
    )
    assert result.used_or_committed_usd == Decimal(3)
    assert result.uncovered_formal_usd == 0
    assert result.status == "both_fit_arithmetic"


def test_partial_formal_coverage_protects_remainder():
    result = calculate_budget(
        request(
            reservations=[{"key": "r1", "state": "reserved", "reserved_usd": "1"}],
            formal_tasks=[{"key": "formal-1", "ceiling_usd": "3", "reservation_key": "r1"}],
        )
    )
    assert result.used_or_committed_usd == 1
    assert result.uncovered_formal_usd == 2


@pytest.mark.parametrize("state", ["reserved", "usage_unknown", "settled"])
def test_unknown_usage_keeps_full_reservation(state):
    result = calculate_budget(
        request(
            global_limit_usd="9",
            formal_tasks=[],
            reservations=[{"key": "r1", "state": state, "reserved_usd": "4"}],
        )
    )
    assert result.used_or_committed_usd == 4
    assert result.status == "insufficient_for_both"


def test_known_settlement_and_release_preserve_actual_accounting():
    result = calculate_budget(
        request(
            formal_tasks=[],
            reservations=[
                {"key": "r1", "state": "settled", "reserved_usd": "4", "settled_usd": "1"},
                {"key": "r2", "state": "released", "reserved_usd": "4"},
            ],
        )
    )
    assert result.used_or_committed_usd == 1


def test_unknown_formal_inventory_is_not_zero_even_with_zero_model_cost():
    result = calculate_budget(
        request(
            research_cost_usd="0",
            synthesis_cost_usd="0",
            formal_tasks=[],
            formal_inventory_complete=False,
        )
    )
    assert result.status == "formal_inventory_unknown"
    assert result.allowed_model_jobs == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"reservations": [{"key": "r1", "state": "reserved", "reserved_usd": "1"}] * 2},
        {"formal_tasks": [{"key": "f1", "ceiling_usd": "1", "reservation_key": "missing"}]},
        {
            "formal_tasks": [
                {"key": "f1", "ceiling_usd": "1", "reservation_key": "r1"},
                {"key": "f2", "ceiling_usd": "1", "reservation_key": "r1"},
            ],
            "reservations": [{"key": "r1", "state": "reserved", "reserved_usd": "2"}],
        },
        {
            "reservations": [
                {"key": "r1", "state": "released", "reserved_usd": "1", "settled_usd": "0.5"}
            ]
        },
        {"research_cost_usd": "NaN"},
        {"synthesis_cost_usd": "-1"},
    ],
)
def test_ambiguous_or_invalid_budget_claim_rejected(changes):
    with pytest.raises(ValueError):
        request(**changes)


def test_settled_or_released_usage_does_not_cover_pending_formal_work():
    result = calculate_budget(
        request(
            reservations=[
                {"key": "r1", "state": "settled", "reserved_usd": "3", "settled_usd": "1"}
            ],
            formal_tasks=[{"key": "formal-1", "ceiling_usd": "3", "reservation_key": "r1"}],
            global_limit_usd="9",
        )
    )
    assert result.used_or_committed_usd == 1
    assert result.uncovered_formal_usd == 3
    assert result.status == "insufficient_for_both"


@pytest.mark.parametrize("limit,code", [("10", 0), ("8", 1)])
def test_normal_cli_preserves_non_activation_and_hides_inventory_keys(
    tmp_path, monkeypatch, capsys, limit, code
):
    import json

    from nexus.ops.economic_budget import main

    path = tmp_path / "claims.json"
    path.write_text(request(global_limit_usd=limit).model_dump_json())
    monkeypatch.setattr("sys.argv", ["budget-preflight", "--input", str(path)])
    assert main() == code
    output = capsys.readouterr().out
    data = json.loads(output)
    assert data["evidence_kind"] == "planning_claims"
    assert data["allowed_model_jobs"] == 0
    assert data["reserved"] is False
    assert "formal-1" not in output


def test_normal_cli_bad_input_only_emits_fixed_code(tmp_path, monkeypatch, capsys):
    from nexus.ops.economic_budget import main

    path = tmp_path / "claims.json"
    path.write_text('{"private-field":"not-for-output"}')
    monkeypatch.setattr("sys.argv", ["budget-preflight", "--input", str(path)])
    assert main() == 1
    output = capsys.readouterr().out
    assert "ECONOMIC_BUDGET_CLAIMS_INVALID" in output
    assert "not-for-output" not in output
