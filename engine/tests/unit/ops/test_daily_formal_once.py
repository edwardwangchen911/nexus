"""Explicit formal proposal CLI requests remain bounded and safely redacted."""

import json
from uuid import UUID

import pytest
from nexus.ops.daily_once import main


def _request():
    def identifier(value):
        return str(UUID(int=value))
    return {
        "portfolio_snapshot_id": identifier(1),
        "diagnostic_report_id": identifier(11),
        "decision_cutoff_at": "2026-08-28T21:00:00Z",
        "idempotency_key": "synthetic-formal-once",
        "research_outputs": [{"run_id": identifier(2), "output_id": identifier(3)}],
        "strategy_references": {
            "market": {
                "decision_session": "2026-08-28", "execution_session": "2026-08-31",
                "decision_time": "2026-08-28T21:00:00Z",
                "source_policy_id": identifier(4), "calendar_authority_id": identifier(5),
                "universe_snapshot_id": identifier(6), "quality_report_id": identifier(7),
            },
            "raw_bar_set_ids": [identifier(8)], "adjusted_bar_set_ids": [identifier(9)],
            "sector_exposure_ids": [],
        },
    }


@pytest.mark.parametrize("action", ["formal-proposal-admit", "formal-proposal-run"])
def test_formal_cli_reaches_local_database_guard_after_validating_request(
    monkeypatch, tmp_path, capsys, action,
):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:secret@example.invalid/db")
    if action == "formal-proposal-admit":
        request_file = tmp_path / "formal.json"
        request_file.write_text(json.dumps(_request()), encoding="utf-8")
        arguments = ["--request-file", str(request_file)]
    else:
        arguments = ["--job-id", str(UUID(int=10))]
    monkeypatch.setattr("sys.argv", ["daily_once", action, *arguments])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "status": "failed", "code": "DAILY_ONCE_DATABASE_NOT_LOCAL",
    }
    assert "secret" not in captured.err


@pytest.mark.parametrize("case", [
    "invalid_json", "extra_flags", "naive_time", "oversized", "large_integer", "deep_json",
])
def test_formal_cli_invalid_file_never_echoes_private_content(
    monkeypatch, tmp_path, capsys, case,
):
    request = _request()
    if case == "extra_flags":
        request["cost_complete"] = True
        request["private"] = "account-canary"
    if case == "naive_time":
        request["decision_cutoff_at"] = "2026-08-28T21:00:00"
    content = (
        "account-canary" if case == "invalid_json" else
        "account-canary" * 100_000 if case == "oversized" else json.dumps(request)
    )
    if case == "large_integer":
        content = '{"private":"account-canary","value":' + "9" * 5_000 + "}"
    if case == "deep_json":
        content = '{"private":"account-canary","value":' + "[" * 20_000 + "0" + "]" * 20_000 + "}"
    request_file = tmp_path / "private-request.json"
    request_file.write_text(content, encoding="utf-8")
    monkeypatch.setattr("sys.argv", [
        "daily_once", "formal-proposal-admit", "--request-file", str(request_file),
    ])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "status": "failed", "code": "DAILY_ONCE_REQUEST_INVALID",
    }
    assert "account-canary" not in captured.err and str(request_file) not in captured.err
