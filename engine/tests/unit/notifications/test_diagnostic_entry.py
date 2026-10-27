"""The test-notification CLI cannot accept business text or a new destination."""

from uuid import UUID

import pytest
from nexus.ops import notification_test


def test_parser_requires_explicit_identity_and_excludes_payload_or_destination() -> None:
    identifier = "0199d500-1000-7000-8000-000000000001"
    args = notification_test.parser().parse_args(["enqueue", "--test-id", identifier])
    assert args.test_id == UUID(identifier)
    assert args.action == "enqueue"
    for extra in (["--body", "buy"], ["--url", "https://elsewhere.invalid"], ["--retry"]):
        with pytest.raises(SystemExit):
            notification_test.parser().parse_args(["enqueue", "--test-id", identifier, *extra])
    with pytest.raises(SystemExit):
        notification_test.parser().parse_args(["enqueue"])


def test_failed_submission_returns_safe_same_identity_read_instruction(monkeypatch, capsys) -> None:
    def fail(*args, **kwargs):
        raise RuntimeError("secret credential and private response")

    monkeypatch.setattr(notification_test, "_validate_local_env", fail)
    identifier = "0199d500-1000-7000-8000-000000000001"
    assert notification_test.main(["enqueue", "--test-id", identifier]) == 1
    output = capsys.readouterr().out
    assert identifier in output and "read_same_test_id" in output and "unknown" in output
    assert "secret" not in output and "private" not in output


def test_typed_pre_submission_rejection_is_not_unknown(monkeypatch, capsys, tmp_path) -> None:
    import json

    from nexus.notifications.diagnostic import DiagnosticNotificationRejected

    monkeypatch.setattr(notification_test, "_validate_local_env", lambda *args: tmp_path)
    monkeypatch.setattr(
        notification_test, "load_agent_paper_service_environment", lambda *args, **kwargs: {}
    )

    async def rejected(*args, **kwargs):
        raise DiagnosticNotificationRejected("DIAGNOSTIC_NOTIFICATION_DISABLED")

    monkeypatch.setattr(notification_test, "run", rejected)
    identifier = "0199d500-1000-7000-8000-000000000001"
    assert notification_test.main(["enqueue", "--test-id", identifier]) == 1
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["status"] == "failed"
    assert receipt["submission"] == "not_started"
    assert receipt["error_code"] == "DIAGNOSTIC_NOTIFICATION_DISABLED"


def test_untyped_commit_error_keeps_unknown_even_with_known_text(
    monkeypatch, capsys, tmp_path
) -> None:
    import json

    monkeypatch.setattr(notification_test, "_validate_local_env", lambda *args: tmp_path)
    monkeypatch.setattr(
        notification_test, "load_agent_paper_service_environment", lambda *args, **kwargs: {}
    )

    async def ambiguous(*args, **kwargs):
        raise RuntimeError("DIAGNOSTIC_NOTIFICATION_DISABLED")

    monkeypatch.setattr(notification_test, "run", ambiguous)
    identifier = "0199d500-1000-7000-8000-000000000001"
    assert notification_test.main(["enqueue", "--test-id", identifier]) == 1
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["status"] == "unknown"
    assert receipt["next_action"] == "read_same_test_id"
