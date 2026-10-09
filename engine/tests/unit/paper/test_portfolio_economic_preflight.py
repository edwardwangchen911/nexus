"""Zero-call planning must never certify business registration or activation."""

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.portfolio_economic_preflight import (
    EconomicPlanRequest,
    compile_plan,
    save_plan,
)
from pydantic import ValidationError


def request(**updates):
    values = {
        "evaluation_key": "isolated-contract-case",
        "instruments": [
            {"instrument_id": UUID(int=1), "symbol": "AVGO"},
            {"instrument_id": UUID(int=2), "symbol": "NVDA"},
        ],
        "start_session": date(2026, 10, 9),
        "research_cost_usd": Decimal("1"),
        "synthesis_cost_usd": Decimal("2"),
        "global_daily_limit_usd": Decimal("20"),
    }
    return EconomicPlanRequest.model_validate(values | updates)


NOW = datetime(2026, 10, 8, 16, tzinfo=UTC)


def test_exact_protocol_and_actual_synthesis_entrypoint():
    result = compile_plan(request(), now=NOW)
    assert result.status == "compiled_not_registered"
    assert result.execution_locked is True
    assert result.economic_status == "not_evaluated"
    assert result.policy.min_excess_over_hold_percentage_points == "0"
    assert result.policy.cash_usd_per_lane == "10000"
    assert result.policy.fixed_baseline_required is True
    assert result.policy.auto_promote is False
    assert len(result.slots) == 24
    assert [s.phase for s in result.slots].count("training") == 4
    assert [s.phase for s in result.slots].count("oos") == 20
    assert result.daily_model_jobs == 6
    assert result.daily_sdk_calls == 14  # Four three-stage research, two one-turn synthesis.
    assert result.total_model_jobs == 144
    assert result.total_sdk_calls == 336
    assert result.daily_cost_ceiling_usd == Decimal("8")
    assert result.daily_model_input_cap == 576000
    assert result.daily_wire_input_cap == 512000
    assert result.daily_output_cap == 49152
    assert "COMMON_EXECUTION_WINDOW_UNVERIFIED" in result.activation_blockers
    assert "ATOMIC_GLOBAL_BUDGET_RESERVATION_UNIMPLEMENTED" in result.activation_blockers
    assert "FIXED_STRATEGY_BASELINE_UNIMPLEMENTED" in result.activation_blockers


def test_slots_respect_dst_and_weekends():
    result = compile_plan(request(), now=NOW)
    assert result.slots[0].decision_at.hour == 14
    assert next(s for s in result.slots if s.session == date(2026, 11, 2)).decision_at.hour == 15
    assert all(s.session.weekday() < 5 for s in result.slots)
    assert len({s.session for s in result.slots}) == 24


@pytest.mark.parametrize(
    "updates",
    [
        {"start_session": date(2026, 10, 10)},
        {"start_session": date(2026, 10, 8)},
    ],
)
def test_non_session_or_past_slot_is_rejected(updates):
    with pytest.raises(ValueError, match="ECONOMIC_PLAN_SESSION_INVALID"):
        compile_plan(request(**updates), now=NOW)


@pytest.mark.parametrize(
    "updates",
    [
        {"research_cost_usd": Decimal("-1")},
        {"research_cost_usd": Decimal("NaN")},
        {"cash_usd": "9000"},
        {"training_days": 5},
        {"auto_promote": True},
    ],
)
def test_unapproved_parameters_and_invalid_costs_are_rejected(updates):
    with pytest.raises(ValidationError):
        request(**updates)


def test_duplicate_identity_rejected():
    pair = [{"instrument_id": UUID(int=1), "symbol": "AVGO"}] * 2
    with pytest.raises(ValidationError):
        request(instruments=pair)


def test_zero_cost_still_requires_budget_and_authority():
    result = compile_plan(request(research_cost_usd=0, synthesis_cost_usd=0), now=NOW)
    assert result.daily_cost_ceiling_usd == 0
    assert "ATOMIC_GLOBAL_BUDGET_RESERVATION_UNIMPLEMENTED" in result.activation_blockers
    assert "MODEL_AND_CANDIDATE_AUTHORITIES_UNBOUND" in result.activation_blockers


def test_budget_insufficient_adds_blocker_without_reducing_one_lane():
    result = compile_plan(request(global_daily_limit_usd=7), now=NOW)
    assert result.daily_cost_ceiling_usd == 8
    assert "DAILY_LIMIT_BELOW_EVALUATION_CEILING" in result.activation_blockers
    assert result.daily_model_jobs == 6


def test_immutable_receipt_exact_replay_conflict_and_corruption(tmp_path):
    path = tmp_path / "preflight.json"
    result = compile_plan(request(), now=NOW)
    assert save_plan(path, result) == "created"
    assert path.stat().st_mode & 0o777 == 0o600
    assert save_plan(path, result) == "existing"
    different = compile_plan(request(evaluation_key="different"), now=NOW)
    with pytest.raises(ValueError, match="ECONOMIC_PLAN_IDEMPOTENCY_CONFLICT"):
        save_plan(path, different)
    path.write_text("broken")
    with pytest.raises(ValueError, match="ECONOMIC_PLAN_EXISTING_INVALID"):
        save_plan(path, result)


def test_symlink_is_never_written(tmp_path):
    target = tmp_path / "target"
    target.write_text("keep")
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="ECONOMIC_PLAN_EXISTING_INVALID"):
        save_plan(link, compile_plan(request(), now=NOW))
    assert target.read_text() == "keep"


def test_holiday_and_early_close_are_not_normalized_away():
    result = compile_plan(request(start_session=date(2026, 11, 25)), now=NOW)
    assert date(2026, 11, 26) not in {s.session for s in result.slots}
    early_close = next(s for s in result.slots if s.session == date(2026, 11, 27))
    assert early_close.market_close_at == datetime(2026, 11, 27, 18, tzinfo=UTC)


def test_unsafe_parent_is_rejected_without_creating_file(tmp_path):
    tmp_path.chmod(0o755)
    with pytest.raises(ValueError, match="ECONOMIC_PLAN_DIRECTORY_INVALID"):
        save_plan(tmp_path / "receipt.json", compile_plan(request(), now=NOW))
    assert not (tmp_path / "receipt.json").exists()


def test_normal_cli_exact_replay_and_safe_failure(tmp_path, monkeypatch, capsys):
    import json
    import sys

    from nexus.ops.portfolio_economic_preflight import main

    input_path, output_path = tmp_path / "input.json", tmp_path / "output.json"
    input_path.write_text(request(start_session=date(2027, 1, 4)).model_dump_json())
    monkeypatch.setattr(
        sys, "argv", ["preflight", "--input", str(input_path), "--output", str(output_path)]
    )
    assert main() == 0
    assert json.loads(capsys.readouterr().out)["receipt"] == "created"
    before = output_path.read_bytes()
    assert main() == 0
    assert json.loads(capsys.readouterr().out)["receipt"] == "existing"
    assert output_path.read_bytes() == before
    input_path.write_text('{"secret":"never-print-this"}')
    assert main() == 1
    output = capsys.readouterr().out
    assert "never-print-this" not in output
    assert json.loads(output)["code"] == "ECONOMIC_PLAN_INPUT_OR_RECEIPT_INVALID"
