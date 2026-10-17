"""Identity rejection keeps its cause through the normal safe CLI envelope."""

import json
import os
import subprocess
from types import SimpleNamespace

import pytest
from nexus.ops import manual_paper
from nexus.paper import history_seal
from nexus.paper import manual_preparation_close as close

_DESKTOP = (
    "/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex"
)
_BIRTH = "Mon Oct 12 10:00:00 2026"


@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize(
    "fault,reason",
    [
        ("missing", "ANCESTOR_MISSING"),
        ("cycle", "ANCESTOR_CYCLE"),
        ("owner", "OWNER_MISMATCH"),
        ("executable", "EXECUTABLE_MISMATCH"),
    ],
)
def test_identity_cause_survives_guard_and_cli_without_host_details(
    monkeypatch, tmp_path, capsys, wrapped, fault, reason
):
    def execute(command, **kwargs):
        if "status" in command:
            return subprocess.CompletedProcess(command, 0, '{"status":"inactive"}')
        if command[0] == "/bin/ps":
            parent = 200 if fault == "cycle" else 999
            uid = os.getuid() + 1 if fault == "owner" else os.getuid()
            executable = "/private/credential-marker/codex" if fault == "executable" else _DESKTOP
            raw = f"200 {parent} {uid} {_BIRTH} {executable}\n"
        elif command[0] == "/usr/sbin/lsof":
            raw = f"p200\nftxt\nn{_DESKTOP}\nfcwd\nn/\n"
        else:
            raise AssertionError("Identity rejection must precede container inspection")
        return subprocess.CompletedProcess(command, 0, raw)

    fake = SimpleNamespace(
        run=execute,
        SubprocessError=subprocess.SubprocessError,
        TimeoutExpired=subprocess.TimeoutExpired,
    )
    monkeypatch.setattr(history_seal, "subprocess", fake)
    monkeypatch.setattr(close, "subprocess", fake)

    async def run(*_):
        if wrapped:
            close.require_preparation_producers_stopped(tmp_path, tmp_path / "env")
        else:
            history_seal.require_research_processes_quiescent()

    monkeypatch.setattr(manual_paper, "run", run)
    monkeypatch.setattr(manual_paper, "_validate_local_env", lambda root, path: path)
    monkeypatch.setattr(manual_paper, "load_agent_paper_service_environment", lambda *a, **k: {})
    monkeypatch.setattr(manual_paper, "_checkout_revision", lambda root: "a" * 40)
    args = ["--repo-root", str(tmp_path)]
    if wrapped:
        args += [
            "check-preparation-acceptance",
            "--request-id",
            "00000000-0000-0000-0000-000000000001",
        ]
    else:
        args += [
            "seal-failed-research",
            "--cycle-id",
            "00000000-0000-0000-0000-000000000001",
            "--run-id",
            "00000000-0000-0000-0000-000000000002",
            "--accept-unknown-usage",
        ]
    assert manual_paper.main(args) == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out) == {
        "status": "blocked",
        "error_code": "MANUAL_PREPARATION_CLOSE_BLOCKED"
        if wrapped
        else "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
        "execution_locked": True,
        "diagnostic_stage": "research_processes",
        "diagnostic_code": "HOST_IDENTITY_UNCONFIRMED",
        "diagnostic_reason": reason,
    }
    assert "credential-marker" not in captured.out and _DESKTOP not in captured.out


@pytest.mark.parametrize("reason", [None, "private secret", ["OWNER_MISMATCH"]])
@pytest.mark.parametrize("wrapped", [False, True])
def test_untrusted_identity_reason_is_omitted(monkeypatch, reason, wrapped):
    error = history_seal.ResearchProcessDiagnosticError("HOST_IDENTITY_UNCONFIRMED", reason=reason)
    if wrapped:
        error = close.PreparationCloseDiagnosticError(
            "research_processes", "HOST_IDENTITY_UNCONFIRMED", reason=reason
        )
    assert error.safe_diagnostics() == {
        "diagnostic_stage": "research_processes",
        "diagnostic_code": "HOST_IDENTITY_UNCONFIRMED",
    }


@pytest.mark.parametrize("wrapped", [False, True])
def test_identity_reason_cannot_be_attached_to_another_boundary(wrapped):
    if wrapped:
        error = close.PreparationCloseDiagnosticError(
            "host_status", "INSPECTION_FAILED", reason="OWNER_MISMATCH"
        )
    else:
        error = history_seal.ResearchProcessDiagnosticError(
            "HOST_MAPPING_UNCONFIRMED", reason="OWNER_MISMATCH"
        )
    assert "diagnostic_reason" not in error.safe_diagnostics()
