"""Local boundary tests for the Agent-version evaluation CLI."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.ops import agent_version_evaluation as cli
from nexus.ops.agent_version_evaluation import RegisterRequest, StatusResult, main, parser
from pydantic import ValidationError

EVALUATION_ID = UUID("71000000-0000-0000-0000-000000000099")


def test_parser_rejects_database_and_credential_overrides() -> None:
    with pytest.raises(SystemExit):
        parser().parse_args(
            [
                "status",
                "--evaluation-id",
                str(EVALUATION_ID),
                "--database-url",
                "postgresql+asyncpg://remote/nexus",
            ]
        )


def test_parser_accepts_bounded_legacy_invalidation() -> None:
    values = vars(
        parser().parse_args(
            ["invalidate", "--evaluation-id", str(EVALUATION_ID)]
        )
    )

    assert cli._request(values) == cli.EvaluationRequest(
        action="invalidate",
        evaluation_id=EVALUATION_ID,
    )


def test_register_requires_exact_frozen_window() -> None:
    with pytest.raises(ValidationError):
        RegisterRequest(
            action="register",
            instrument_id=UUID(int=1),
            incumbent_version_source_run_id=UUID(int=2),
            challenger_prompt_version_id=UUID(int=3),
            challenger_model_config_id=UUID(int=4),
            initial_capital="10000",
            train_slot=(datetime(2026, 10, 1, 14, tzinfo=UTC),),
            oos_slot=tuple(datetime(2026, 10, day, 14, tzinfo=UTC) for day in range(2, 22)),
            idempotency_key="register-v1",
        )


def test_nonlocal_database_fails_before_any_evaluation_work() -> None:
    request = cli.EvaluationRequest(action="status", evaluation_id=EVALUATION_ID)
    settings = cli.EngineSettings(database_url="postgresql+asyncpg://user:pass@example.com/nexus")

    with pytest.raises(ValueError, match="AGENT_PAPER_DATABASE_NOT_LOCAL"):
        cli.asyncio.run(cli.execute(settings, request))


def test_status_command_emits_bounded_json_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "load_local_environment",
        lambda *_args: {"DATABASE_URL": "postgresql+asyncpg://user:pass@127.0.0.1/nexus"},
    )

    async def status(*_args: object, **_kwargs: object) -> StatusResult:
        return StatusResult(
            evaluation_id=EVALUATION_ID,
            status="observing",
            train_completed=2,
            oos_completed=0,
            reason_code="EVALUATION_OBSERVING",
        )

    monkeypatch.setattr(cli, "execute", status)

    assert main(["status", "--evaluation-id", str(EVALUATION_ID)]) == 0
    assert capsys.readouterr().out == (
        '{"broker_write":false,"evaluation_id":"71000000-0000-0000-0000-000000000099",'
        '"execution_locked":true,"live_write":false,"oos_completed":0,'
        '"reason_code":"EVALUATION_OBSERVING","status":"observing",'
        '"train_completed":2}\n'
    )
