"""CLI validation rejects ambiguous clocks, invented protocols, and unsafe error output."""

import sys
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from nexus.ops import backtest_once
from pydantic import ValidationError


def request_values() -> dict[str, object]:
    return {
        "strategy_version": "baseline_v1", "authority_bundle_id": str(uuid4()),
        "data_snapshot_id": str(uuid4()), "source_policy_id": str(uuid4()),
        "cost_policy_id": str(uuid4()), "start": "2021-01-08", "end": "2021-01-11",
        "initial_nav": "10000", "as_of": datetime.now(UTC).isoformat(),
    }


@pytest.mark.parametrize("changes", [
    {"as_of": "2026-09-03T00:00:00"}, {"initial_nav": 1.2},
    {"initial_nav": "NaN"}, {"initial_nav": "-1"},
    {"strategy_version": "unpublished"}, {"walk_forward": False},
])
def test_request_requires_explicit_supported_parameters(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        backtest_once.BacktestOnceRequest.model_validate({**request_values(), **changes})


def test_cli_failure_does_not_print_connection_or_raw_exception(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    values = request_values()
    args = [part for key, value in values.items() for part in (
        f"--{key.replace('_', '-')}", str(value)
    )]
    monkeypatch.setattr(sys, "argv", ["backtest_once", *args])

    async def fail(*_args: object) -> dict[str, object]:
        raise RuntimeError("postgresql://private-connection-do-not-print")

    monkeypatch.setattr(backtest_once, "run_once", fail)
    with pytest.raises(SystemExit, match="1"):
        backtest_once.main()
    captured = capsys.readouterr()
    assert "BACKTEST_RUN_FAILED" in captured.err
    assert "private-connection" not in captured.err
    assert captured.out == ""
