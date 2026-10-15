from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.main import create_engine_app
from nexus.paper.manual_request import ManualRequestInput
from nexus.settings import EngineSettings
from pydantic import ValidationError


def test_manual_request_api_is_explicit_authenticated_and_cannot_choose_scope():
    paths = create_engine_app(EngineSettings.for_test()).openapi()["paths"]
    path = "/internal/v1/paper/manual-requests"
    assert "post" in paths[path]
    assert "get" in paths[path + "/{request_id}"]
    for extra in ("account_id", "candidate_batch_id", "model_config_id", "decision_time"):
        with pytest.raises(ValidationError):
            ManualRequestInput.model_validate({"mode": "research_only", extra: str(UUID(int=1))})
    assert EngineSettings.for_test().agent_paper_manual_enabled is False


@pytest.mark.parametrize(
    "bad", ["stale", "future", "version", "schema", "protocol", "missing-sha", "scope"]
)
def test_manual_host_requires_fresh_same_release_and_capable_protocol(bad):
    from nexus.paper.manual_entry import MANUAL_HOST_PROTOCOL, require_manual_host
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

    now = datetime(2026, 10, 12, 14, tzinfo=UTC)
    sha = "a" * 40
    row = SimpleNamespace(
        worker_id=f"agent-paper-loop-{UUID(int=1)}:manual:{'c' * 64}",
        seen_at=now,
        started_at=now - timedelta(minutes=1),
        process_version=f"{MANUAL_HOST_PROTOCOL}+{sha}",
        migration_head=CURRENT_MIGRATION_HEAD,
    )
    require_manual_host([row], now=now, release_revision=sha, scope_hash="c" * 64)
    if bad == "stale":
        row.seen_at = now - timedelta(seconds=31)
    elif bad == "future":
        row.seen_at = now + timedelta(seconds=1)
    elif bad == "version":
        row.process_version = f"{MANUAL_HOST_PROTOCOL}+{'b' * 40}"
    elif bad == "schema":
        row.migration_head = "0074_portfolio_economic_evidence"
    elif bad == "protocol":
        row.process_version = f"nexus-agent-paper-v1+{sha}"
    elif bad == "scope":
        row.worker_id = f"agent-paper-loop-{UUID(int=1)}:manual:{'d' * 64}"
    else:
        sha = None
    with pytest.raises(ValueError, match="MANUAL_HOST_NOT_READY"):
        require_manual_host([row], now=now, release_revision=sha, scope_hash="c" * 64)


def test_manual_host_multiple_fresh_processes_are_not_one_consumer():
    from nexus.paper.manual_entry import MANUAL_HOST_PROTOCOL, require_manual_host
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

    now = datetime(2026, 10, 12, 14, tzinfo=UTC)
    row = SimpleNamespace(
        worker_id=f"agent-paper-loop-{UUID(int=1)}:manual:{'c' * 64}",
        seen_at=now,
        started_at=now,
        process_version=f"{MANUAL_HOST_PROTOCOL}+{'a' * 40}",
        migration_head=CURRENT_MIGRATION_HEAD,
    )
    with pytest.raises(ValueError, match="MANUAL_HOST_NOT_READY"):
        require_manual_host([row, row], now=now, release_revision="a" * 40, scope_hash="c" * 64)


def test_manual_cli_cannot_choose_scope_or_emit_raw_configuration_error(monkeypatch, capsys):
    from nexus.ops import manual_paper

    with pytest.raises(SystemExit):
        manual_paper.parser().parse_args(
            [
                "start",
                "--mode",
                "research_only",
                "--idempotency-key",
                "test",
                "--account-id",
                str(UUID(int=1)),
            ]
        )
    capsys.readouterr()

    def fail(*args, **kwargs):
        raise ValueError("sensitive-config-canary")

    monkeypatch.setattr(manual_paper, "load_agent_paper_service_environment", fail)
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda _root, path: path)
    assert manual_paper.main(["start", "--mode", "research_only", "--idempotency-key", "test"]) == 1
    output = capsys.readouterr().out
    assert "MANUAL_RESULT_UNKNOWN" in output and "sensitive-config-canary" not in output


def test_manual_cli_explicit_file_does_not_inherit_process_access(monkeypatch):
    from nexus.ops.manual_paper import _FileEngineSettings

    monkeypatch.setenv("NEXUS_AGENT_PAPER_MANUAL_ENABLED", "true")
    monkeypatch.setenv("NEXUS_AGENT_PAPER_ACCOUNT_ID", str(UUID(int=1)))
    settings = _FileEngineSettings.model_validate({"NEXUS_ENV": "test"})
    assert settings.agent_paper_manual_enabled is False and settings.agent_paper_account_id is None


def test_preparation_disposition_is_read_metadata_not_a_terminal_or_execution_status():
    from nexus.paper.manual_request import ManualRequestView

    data = {
        "request_id": UUID(int=1),
        "mode": "research_only",
        "requested_at": datetime(2026, 10, 15, 14, tzinfo=UTC),
        "status": "unknown",
        "code": "MANUAL_RESULT_UNKNOWN",
        "cycle_id": None,
    }
    old = ManualRequestView.model_validate(data)
    assert set(old.model_dump()) == {
        "request_id",
        "mode",
        "requested_at",
        "status",
        "code",
        "cycle_id",
        "execution_locked",
    }
    accepted = ManualRequestView.model_validate(
        data | {"preparation_disposition": "unknown_acknowledged"}
    )
    assert accepted.status == "unknown" and accepted.execution_locked is True
    for changed in [
        {"status": "queued"},
        {"cycle_id": UUID(int=2)},
        {"preparation_disposition": True},
        {"preparation_disposition": "all_unknowns_retriable"},
    ]:
        with pytest.raises(ValidationError):
            ManualRequestView.model_validate(
                data | {"preparation_disposition": "unknown_acknowledged"} | changed
            )
