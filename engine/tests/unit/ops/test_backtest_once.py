"""CLI validation rejects ambiguous clocks, invented protocols, and unsafe error output."""

import json
import sys
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
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


@pytest.mark.parametrize("invalid_case", ["choice", "unknown", "missing"])
def test_cli_parse_errors_are_fixed_and_do_not_load_settings_or_run(
    invalid_case: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    values = request_values()
    canary = "/synthetic-private/DO_NOT_ECHO"
    if invalid_case == "choice":
        values["strategy_version"] = canary
    elif invalid_case == "missing":
        del values["initial_nav"]
    args = [part for key, value in values.items() for part in (
        f"--{key.replace('_', '-')}", str(value)
    )]
    if invalid_case == "unknown":
        args.extend(["--unexpected", canary])
    settings = Mock(side_effect=AssertionError("settings must not load"))
    run = AsyncMock(side_effect=AssertionError("backtest must not run"))
    monkeypatch.setattr(backtest_once, "EngineSettings", settings)
    monkeypatch.setattr(backtest_once, "run_once", run)
    monkeypatch.setattr(sys, "argv", ["backtest_once", *args])

    with pytest.raises(SystemExit) as exited:
        backtest_once.main()

    captured = capsys.readouterr()
    settings.assert_not_called()
    run.assert_not_called()
    assert captured.out == ""
    assert canary not in captured.err
    assert json.loads(captured.err) == {"status": "failed", "code": "BACKTEST_REQUEST_INVALID"}
    assert exited.value.code == 1


def test_cli_help_remains_successful_without_settings_or_execution(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    settings = Mock(side_effect=AssertionError("settings must not load"))
    run = AsyncMock(side_effect=AssertionError("backtest must not run"))
    monkeypatch.setattr(backtest_once, "EngineSettings", settings)
    monkeypatch.setattr(backtest_once, "run_once", run)
    monkeypatch.setattr(sys, "argv", ["backtest_once", "--help"])

    with pytest.raises(SystemExit) as exited:
        backtest_once.main()

    captured = capsys.readouterr()
    assert exited.value.code == 0
    assert "--authority-bundle-id" in captured.out
    assert captured.err == ""
    settings.assert_not_called()
    run.assert_not_called()


def test_cli_valid_request_runs_once_and_prints_result(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    values = request_values()
    args = [part for key, value in values.items() for part in (
        f"--{key.replace('_', '-')}", str(value)
    )]
    settings_value = object()
    settings = Mock(return_value=settings_value)
    output = {"status": "BACKTEST_COMPLETED", "execution_locked": True}
    run = AsyncMock(return_value=output)
    monkeypatch.setattr(backtest_once, "EngineSettings", settings)
    monkeypatch.setattr(backtest_once, "run_once", run)
    monkeypatch.setattr(sys, "argv", ["backtest_once", *args])

    backtest_once.main()

    captured = capsys.readouterr()
    assert json.loads(captured.out) == output
    assert captured.err == ""
    settings.assert_called_once_with()
    run.assert_awaited_once_with(
        settings_value, backtest_once.BacktestOnceRequest.model_validate(values)
    )
