"""Explicit local paper command validation and safe failure output."""

import json
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from nexus.ops import paper_once
from nexus.settings import EngineSettings
from pydantic import ValidationError


def admit_values() -> dict[str, object]:
    return {
        "action": "admit",
        "account_id": str(uuid4()),
        "strategy_decision_id": str(uuid4()),
        "request_time": "2026-08-27T12:00:00Z",
        "idempotency_key": "explicit-paper-request",
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"request_time": "2026-08-27T12:00:00"},
        {"request_time": "2026-08-27T12:00:00+08:00"},
        {"idempotency_key": ""},
        {"idempotency_key": " "},
        {"idempotency_key": "x" * 256},
        {"account_id": "invalid"},
        {"targets": []},
        {"action": "advance"},
    ],
)
def test_admit_requires_exact_explicit_request(changes: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        paper_once.PaperAdmitRequest.model_validate({**admit_values(), **changes})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        None,
        "postgresql+asyncpg://db.example.test/paper",
        "postgresql+asyncpg://localhost/paper?host=remote.test",
        "sqlite+aiosqlite:///paper.db",
    ],
)
async def test_nonlocal_or_missing_database_is_rejected_before_connection(url, monkeypatch) -> None:
    def unexpected(*args, **kwargs):
        raise AssertionError("database must not be opened")

    monkeypatch.setattr(paper_once, "create_async_engine", unexpected)
    request = paper_once.PaperAdmitRequest.model_validate(admit_values())
    with pytest.raises(paper_once.PaperOnceError, match="PAPER_ONCE_DATABASE"):
        await paper_once.run_once(EngineSettings(database_url=url), request)


@pytest.mark.asyncio
async def test_future_clock_is_rejected_before_connection(monkeypatch) -> None:
    def unexpected(*args, **kwargs):
        raise AssertionError("database must not be opened")

    monkeypatch.setattr(paper_once, "create_async_engine", unexpected)
    request = paper_once.PaperAdmitRequest.model_validate(
        {
            **admit_values(),
            "request_time": datetime.now(UTC) + timedelta(days=1),
        }
    )
    with pytest.raises(paper_once.PaperOnceError, match="PAPER_ONCE_TIME_IN_FUTURE"):
        await paper_once.run_once(
            EngineSettings(database_url="postgresql+asyncpg://localhost/paper"), request
        )


@pytest.mark.parametrize(
    "argv",
    [
        ["unexpected-private-value"],
        ["admit", "--account-id", "private-account-value"],
    ],
)
def test_parser_errors_never_echo_input(argv, monkeypatch, capsys) -> None:
    monkeypatch.setattr(sys, "argv", ["paper_once", *argv])
    with pytest.raises(SystemExit, match="1"):
        paper_once.main()
    captured = capsys.readouterr()
    assert json.loads(captured.err)["code"] == "PAPER_ONCE_REQUEST_INVALID"
    assert "private" not in captured.err and captured.out == ""


def test_uncertain_failure_is_redacted_and_never_retried(monkeypatch, capsys) -> None:
    values = admit_values()
    argv = ["paper_once", "admit"]
    for key, value in values.items():
        if key != "action":
            argv.extend((f"--{key.replace('_', '-')}", str(value)))
    monkeypatch.setattr(sys, "argv", argv)
    calls = []

    async def fail(*args):
        calls.append(1)
        raise RuntimeError("postgresql://private-credential-do-not-print")

    monkeypatch.setattr(paper_once, "run_once", fail)
    with pytest.raises(SystemExit, match="1"):
        paper_once.main()
    captured = capsys.readouterr()
    assert json.loads(captured.err) == {
        "status": "unknown",
        "code": "PAPER_ONCE_RESULT_UNKNOWN",
    }
    assert calls == [1] and captured.out == ""
