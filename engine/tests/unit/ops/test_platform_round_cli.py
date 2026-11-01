"""Normal CLI receipts preserve exact safe reasons and unknown registration state."""

import json
import sys
from uuid import UUID

import pytest
from nexus.ops import research_comparison
from sqlalchemy.exc import SQLAlchemyError

IDENTITY = UUID("11111111-2222-4333-8444-555555555555")


def argv(tmp_path):
    args = ["nexus-engine-research-comparison", "register-round", "--experiment-id", str(IDENTITY)]
    for flag in (
        "repo-root",
        "source",
        "template",
        "task-policy",
        "continuation-evidence",
        "output",
    ):
        args += ["--" + flag, str(tmp_path / flag)]
    for flag in ("source-sha256", "task-policy-sha256", "implementation-sha"):
        args += ["--" + flag, "a" * (40 if flag == "implementation-sha" else 64)]
    return args


@pytest.mark.parametrize(
    "code",
    [
        "PLATFORM_ROUND_TEMPLATE_CHANGED",
        "PLATFORM_ROUND_FILE_CONFLICT",
        "PLATFORM_ROUND_BUNDLE_CONFLICT",
        "PLATFORM_ROUND_LOCAL_BUNDLE_REQUIRED",
        "PLATFORM_ROUND_REGISTRATION_REQUIRED",
        "PLATFORM_ROUND_PUBLICATION_BUSY",
        "PLATFORM_ROUND_TEMPLATE_CHANGED PRIVATE_SENTINEL",
    ],
)
def test_normal_registration_preserves_only_allowlisted_safe_reason(
    tmp_path, monkeypatch, capsys, code
):
    async def reject(args):
        raise ValueError(code)

    monkeypatch.setattr(research_comparison, "native_command", reject)
    monkeypatch.setattr(sys, "argv", argv(tmp_path))
    assert research_comparison.main() == 1
    output = capsys.readouterr().out
    assert "PRIVATE_SENTINEL" not in output
    result = json.loads(output)
    assert result["reason"] == ("COMPARISON_PREPARATION_FAILED" if " " in code else code)
    assert result["replay_allowed"] is False


def test_registration_commit_read_failure_keeps_original_identity(tmp_path, monkeypatch, capsys):
    async def reject(args):
        raise SQLAlchemyError("PRIVATE_SENTINEL")

    monkeypatch.setattr(research_comparison, "native_command", reject)
    monkeypatch.setattr(sys, "argv", argv(tmp_path))
    assert research_comparison.main() == 1
    output = capsys.readouterr().out
    result = json.loads(output)
    assert "PRIVATE_SENTINEL" not in output
    assert (
        result["status"] == "unknown"
        and result["reason"] == "PLATFORM_ROUND_REGISTRATION_UNCONFIRMED"
    )
    assert result["experiment_id"] == str(IDENTITY) and not result["replay_allowed"]


def test_unconfirmed_registration_is_not_successful_shell_exit(tmp_path, monkeypatch, capsys):
    async def unknown(args):
        return {"status": "registration_unconfirmed", "dispatch_authority": False}

    monkeypatch.setattr(research_comparison, "native_command", unknown)
    monkeypatch.setattr(sys, "argv", argv(tmp_path))
    assert research_comparison.main() == 1
    assert json.loads(capsys.readouterr().out)["status"] == "registration_unconfirmed"
