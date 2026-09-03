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


def forward_values():
    from nexus.quality.source_policy import SOURCE_POLICY_V2_ID

    return {
        "action": "forward",
        "account_id": str(uuid4()),
        "idempotency_key": "forward-one",
        "request_time": "2026-08-28T21:01:00Z",
        "strategy_definition_id": str(uuid4()),
        "champion_generation": 1,
        "references": {
            "market": {
                "decision_session": "2026-08-28",
                "execution_session": "2026-08-31",
                "decision_time": "2026-08-28T21:00:00Z",
                "source_policy_id": str(SOURCE_POLICY_V2_ID),
                "calendar_authority_id": str(uuid4()),
                "universe_snapshot_id": str(uuid4()),
                "quality_report_id": str(uuid4()),
            },
            "raw_bar_set_ids": [str(uuid4())],
            "adjusted_bar_set_ids": [str(uuid4())],
            "sector_exposure_ids": [str(uuid4())],
        },
    }


@pytest.mark.parametrize(
    "changes",
    [{"champion_generation": 0}, {"request_time": "2026-08-28T20:59:00Z"}, {"targets": []}],
)
def test_forward_rejects_missing_or_conflicting_frozen_request(changes):
    with pytest.raises(ValidationError):
        paper_once.PaperForwardRequest.model_validate({**forward_values(), **changes})


def test_forward_json_cli_preserves_exact_source_rejection(tmp_path, monkeypatch, capsys):
    path = tmp_path / "forward.json"
    path.write_text(json.dumps(forward_values()))
    seen = []

    async def fail(settings, request):
        seen.append(request)
        raise paper_once.PaperOnceError("STRATEGY_SNAPSHOT_ADJUSTED_AUTHORITY_UNAVAILABLE")

    monkeypatch.setattr(paper_once, "run_once", fail)
    monkeypatch.setattr(sys, "argv", ["paper_once", "forward", "--request-file", str(path)])
    with pytest.raises(SystemExit, match="1"):
        paper_once.main()
    captured = capsys.readouterr()
    assert json.loads(captured.err) == {
        "status": "failed",
        "code": "STRATEGY_SNAPSHOT_ADJUSTED_AUTHORITY_UNAVAILABLE",
    }
    assert len(seen) == 1 and isinstance(seen[0], paper_once.PaperForwardRequest)
    assert captured.out == ""


def test_frozen_reference_identity_is_replayable_and_order_independent():
    from nexus.strategy.baseline import StrategyDataSnapshot
    from nexus.strategy.rank_checkpoint import _Observations, _snapshot_hash
    from nexus.strategy.snapshot_reader import FrozenStrategyReferences

    references = FrozenStrategyReferences.model_validate(forward_values()["references"])
    other = references.model_copy(update={"adjusted_bar_set_ids": (uuid4(),)})
    snapshot = StrategyDataSnapshot(
        decision_session=references.market.decision_session,
        next_session=references.market.execution_session,
        instruments=(),
    )
    assert _snapshot_hash(snapshot, references.content_hash) != _snapshot_hash(
        snapshot, other.content_hash
    )
    saved = _Observations(held_rank=(), continuity=(), input_references=references).as_payload()
    assert saved["input_references"] == references.canonical_payload
    assert _Observations(held_rank=(), continuity=()).as_payload() == {
        "held_rank": [],
        "continuity": [],
    }
    replayed = _Observations.model_validate(saved)
    assert replayed.input_references == references
    multiple = references.model_copy(update={"sector_exposure_ids": (uuid4(), uuid4())})
    reversed_ids = multiple.model_copy(
        update={"sector_exposure_ids": multiple.sector_exposure_ids[::-1]}
    )
    assert multiple.canonical_payload == reversed_ids.canonical_payload
    assert multiple.content_hash == reversed_ids.content_hash
