"""Calendar preparation is explicit, bounded and never invokes a backtest."""

import json
import sys
from datetime import UTC, date, datetime
from importlib.metadata import version

import pytest
from nexus.ops import backtest_once
from nexus.settings import EngineSettings


def test_calendar_cli_reaches_database_guard_without_running(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_once",
            "--prepare-calendar",
            "--start",
            "2024-11-27",
            "--end",
            "2024-11-29",
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
        ["--prepare-request-file", "/private-canary"],
        ["--as-of", "2024-01-01T00:00:00Z"],
        ["--initial-nav", "10000"],
        ["--strategy-version", "baseline_v1"],
        ["--data-snapshot-id", "private-canary"],
    ],
)
def test_calendar_cli_rejects_mixed_modes_before_settings(extra, monkeypatch, capsys):
    def no_settings():
        pytest.fail("invalid request loaded settings")

    monkeypatch.setattr(backtest_once, "EngineSettings", no_settings)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "backtest_once",
            "--prepare-calendar",
            "--start",
            "2024-11-27",
            "--end",
            "2024-11-29",
            *extra,
        ],
    )
    with pytest.raises(SystemExit, match="1"):
        backtest_once.main()
    captured = capsys.readouterr()
    assert captured.out == "" and "private-canary" not in captured.err
    assert json.loads(captured.err)["code"] == "BACKTEST_REQUEST_INVALID"


@pytest.mark.parametrize(
    "start,end,sessions,clocks",
    [
        (
            "2024-11-27",
            "2024-11-29",
            ["2024-11-27", "2024-11-29"],
            ["2024-11-27T21:30:00", "2024-11-29T18:30:00"],
        ),
        (
            "2024-03-08",
            "2024-03-11",
            ["2024-03-08", "2024-03-11"],
            ["2024-03-08T21:30:00", "2024-03-11T20:30:00"],
        ),
    ],
)
def test_installed_calendar_preserves_holiday_early_close_and_dst(start, end, sessions, clocks):
    from nexus.backtest.calendar import calendar_definition

    calendar = calendar_definition(date.fromisoformat(start), date.fromisoformat(end))
    assert calendar.sessions == tuple(date.fromisoformat(value) for value in sessions)
    assert tuple(calendar.decision_times.values()) == tuple(
        datetime.fromisoformat(value).replace(tzinfo=UTC) for value in clocks
    )
    package_version = version("exchange-calendars")
    assert calendar.version == f"exchange_calendars_{package_version}_XNYS"
    assert calendar.query_parameters == {"exchange": "XNYS", "package_version": package_version}
    import hashlib

    payload = [
        {"session": value, "decision_at": clock + "+00:00"}
        for value, clock in zip(sessions, clocks, strict=True)
    ]
    assert (
        calendar.content_hash
        == hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()
    )


@pytest.mark.parametrize(
    "start,end",
    [
        ("2024-11-29", "2024-11-27"),
        ("2024-11-28", "2024-11-29"),
        ("2024-11-27", "2024-11-30"),
        ("1800-01-01", "1800-01-02"),
        ("9999-01-01", "9999-01-02"),
    ],
)
def test_calendar_never_normalizes_or_truncates_invalid_endpoints(start, end):
    from nexus.backtest.calendar import calendar_definition

    with pytest.raises(ValueError, match="BACKTEST_CALENDAR_RANGE_INVALID"):
        calendar_definition(date.fromisoformat(start), date.fromisoformat(end))


@pytest.mark.parametrize(
    "dates,code",
    [
        ([], "BACKTEST_REQUEST_INVALID"),
        (["--start", "2024-11-27"], "BACKTEST_REQUEST_INVALID"),
        (["--start", "private-canary", "--end", "2024-11-29"], "BACKTEST_REQUEST_INVALID"),
        (["--start", "2024-11-29", "--end", "2024-11-27"], "BACKTEST_CALENDAR_RANGE_INVALID"),
        (["--start", "2024-11-28", "--end", "2024-11-29"], "BACKTEST_CALENDAR_RANGE_INVALID"),
        (["--start", "1800-01-01", "--end", "2024-11-29"], "BACKTEST_CALENDAR_RANGE_INVALID"),
    ],
)
def test_bad_calendar_dates_never_load_settings(dates, code, monkeypatch, capsys):
    def no_settings():
        pytest.fail("invalid dates loaded settings")

    monkeypatch.setattr(backtest_once, "EngineSettings", no_settings)
    monkeypatch.setattr(sys, "argv", ["backtest_once", "--prepare-calendar", *dates])
    with pytest.raises(SystemExit, match="1"):
        backtest_once.main()
    captured = capsys.readouterr()
    assert captured.out == "" and "private-canary" not in captured.err
    assert json.loads(captured.err) == {"status": "failed", "code": code}
