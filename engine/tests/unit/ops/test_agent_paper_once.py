from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.ops.agent_paper_once import (
    AgentPaperCollectWindowRequest,
    AgentPaperRunRequest,
    main,
    parser,
)
from nexus.settings import EngineSettings


def test_request_requires_explicit_utc_time() -> None:
    with pytest.raises(ValueError, match="explicit UTC time is required"):
        AgentPaperRunRequest(
            action="run",
            account_id=UUID(int=1),
            decision_time=datetime(2026, 8, 26, 21),
            idempotency_key="cycle-1",
        )


def test_disabled_command_emits_only_fixed_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("NEXUS_AGENT_PAPER_CYCLE_ENABLED", "false")

    exit_code = main(
        [
            "run",
            "--account-id",
            str(UUID(int=1)),
            "--decision-time",
            datetime(2026, 8, 26, 21, tzinfo=UTC).isoformat(),
            "--idempotency-key",
            "cycle-1",
        ]
    )

    assert exit_code == 1
    assert capsys.readouterr().out == '{"error_code":"AGENT_PAPER_CYCLE_DISABLED"}\n'


def test_feature_flag_is_disabled_by_default_and_can_be_explicitly_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NEXUS_AGENT_PAPER_CYCLE_ENABLED", raising=False)
    assert EngineSettings().agent_paper_cycle_enabled is False
    monkeypatch.setenv("NEXUS_AGENT_PAPER_CYCLE_ENABLED", "true")
    assert EngineSettings().agent_paper_cycle_enabled is True


def test_parser_rejects_provider_and_credential_overrides() -> None:
    with pytest.raises(SystemExit):
        parser().parse_args(
            [
                "run",
                "--account-id",
                str(UUID(int=1)),
                "--decision-time",
                datetime(2026, 8, 26, 21, tzinfo=UTC).isoformat(),
                "--idempotency-key",
                "cycle-1",
                "--provider-url",
                "https://example.invalid",
            ]
        )


def test_collect_window_requires_explicit_utc_window_start() -> None:
    with pytest.raises(ValueError, match="explicit UTC time is required"):
        AgentPaperCollectWindowRequest(
            action="collect-window",
            cycle_id=UUID(int=1),
            instrument_id=UUID(int=2),
            window_start=datetime(2026, 8, 27, 13, 30),
        )


def test_collect_window_parser_has_no_provider_or_credential_override() -> None:
    parsed = parser().parse_args(
        [
            "collect-window",
            "--cycle-id",
            str(UUID(int=1)),
            "--instrument-id",
            str(UUID(int=2)),
            "--window-start",
            datetime(2026, 8, 27, 13, 30, tzinfo=UTC).isoformat(),
        ]
    )
    assert parsed.action == "collect-window"
    with pytest.raises(SystemExit):
        parser().parse_args(
            [
                "collect-window",
                "--cycle-id",
                str(UUID(int=1)),
                "--instrument-id",
                str(UUID(int=2)),
                "--window-start",
                datetime(2026, 8, 27, 13, 30, tzinfo=UTC).isoformat(),
                "--provider-url",
                "https://example.invalid",
            ]
        )
