"""Universe preflight CLI is explicit and does not expose identity gaps."""

import json
import sys
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import uuid4

import pytest
from nexus.ops import backtest_once
from nexus.settings import EngineSettings


def test_universe_preflight_cli_reaches_database_guard(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_once",
            "--preflight-universe",
            "--effective-session",
            "2026-09-08",
        ],
    )
    settings = EngineSettings().model_copy(update={"database_url": None})
    monkeypatch.setattr(backtest_once, "EngineSettings", lambda: settings)

    with pytest.raises(SystemExit, match="1"):
        backtest_once.main()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "status": "failed",
        "code": "BACKTEST_DATABASE_UNCONFIGURED",
    }


@pytest.mark.parametrize(
    "extra",
    [
        ["--prepare-calendar", "--start", "2026-09-08", "--end", "2026-09-08"],
        ["--prepare-request-file", "/synthetic-private-canary"],
        ["--strategy-version", "baseline_v1"],
        ["--as-of", "2026-09-05T00:00:00Z"],
        ["--start", "2026-09-08"],
    ],
)
def test_universe_preflight_cli_rejects_mixed_modes_before_settings(
    extra, monkeypatch, capsys
) -> None:
    settings = Mock(side_effect=AssertionError("invalid request loaded settings"))
    monkeypatch.setattr(backtest_once, "EngineSettings", settings)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_once",
            "--preflight-universe",
            "--effective-session",
            "2026-09-08",
            *extra,
        ],
    )

    with pytest.raises(SystemExit, match="1"):
        backtest_once.main()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "synthetic-private-canary" not in captured.err
    assert json.loads(captured.err) == {
        "status": "failed",
        "code": "BACKTEST_REQUEST_INVALID",
    }
    settings.assert_not_called()


@pytest.mark.parametrize("value", [None, "private-canary", "2026-09-05"])
def test_invalid_universe_session_never_loads_settings(value, monkeypatch, capsys) -> None:
    settings = Mock(side_effect=AssertionError("invalid request loaded settings"))
    monkeypatch.setattr(backtest_once, "EngineSettings", settings)
    args = ["backtest_once", "--preflight-universe"]
    if value is not None:
        args.extend(["--effective-session", value])
    monkeypatch.setattr(sys, "argv", args)

    with pytest.raises(SystemExit, match="1"):
        backtest_once.main()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "private-canary" not in captured.err
    assert json.loads(captured.err) == {
        "status": "failed",
        "code": "BACKTEST_REQUEST_INVALID",
    }
    settings.assert_not_called()


def test_effective_session_is_rejected_outside_preflight_mode(monkeypatch, capsys) -> None:
    values = {
        "strategy-version": "baseline_v1",
        "authority-bundle-id": str(uuid4()),
        "data-snapshot-id": str(uuid4()),
        "source-policy-id": str(uuid4()),
        "cost-policy-id": str(uuid4()),
        "start": "2021-01-08",
        "end": "2021-01-11",
        "initial-nav": "10000",
        "as-of": datetime.now(UTC).isoformat(),
        "effective-session": "2026-09-08",
    }
    args = [part for key, value in values.items() for part in (f"--{key}", value)]
    settings = Mock(side_effect=AssertionError("mixed request loaded settings"))
    monkeypatch.setattr(backtest_once, "EngineSettings", settings)
    monkeypatch.setattr(sys, "argv", ["backtest_once", *args])

    with pytest.raises(SystemExit, match="1"):
        backtest_once.main()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "status": "failed",
        "code": "BACKTEST_REQUEST_INVALID",
    }
    settings.assert_not_called()
