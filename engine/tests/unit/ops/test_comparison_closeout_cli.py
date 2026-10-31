"""Public expiry closeout is identity-only, bounded and never a dispatch command."""

import json
from uuid import uuid4

import pytest
from nexus.ops import research_comparison as cli
from sqlalchemy.exc import SQLAlchemyError


@pytest.mark.parametrize("failure", [None, "lease", "commit", "database", "unexpected"])
def test_close_expired_cli_preserves_unknown_and_safe_errors(
    monkeypatch, capsys, tmp_path, failure
):
    from nexus.agents.comparison_closeout import ComparisonCloseoutUnknown

    identity = uuid4()
    called = []

    async def command(args):
        called.append(args)
        assert args.command == "close-expired-position"
        assert args.experiment_id == identity and args.position == 1
        assert not hasattr(args, "credentials_dir") and not hasattr(args, "source")
        if failure == "lease":
            raise ValueError("COMPARISON_LEASE_ACTIVE")
        if failure == "commit":
            raise ComparisonCloseoutUnknown() from RuntimeError("private transport detail")
        if failure == "database":
            raise SQLAlchemyError("private transport detail")
        if failure == "unexpected":
            raise RuntimeError("private transport detail")
        return {
            "status": "position_registered",
            "replay_allowed": False,
            "terminal_proof": {"known": False},
        }

    monkeypatch.setattr(cli, "native_command", command)
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-engine-research-comparison",
            "close-expired-position",
            "--repo-root",
            str(tmp_path),
            "--experiment-id",
            str(identity),
            "--position",
            "1",
        ],
    )
    assert cli.main() == (0 if failure is None else 1)
    output = capsys.readouterr().out
    assert "private transport detail" not in output
    result = json.loads(output)
    assert result["replay_allowed"] is False and len(called) == 1
    if failure == "lease":
        assert result == {
            "status": "rejected",
            "reason": "COMPARISON_LEASE_ACTIVE",
            "replay_allowed": False,
        }
    elif failure is not None:
        assert result == {
            "status": "unknown",
            "reason": "COMPARISON_CLOSEOUT_RESULT_UNKNOWN",
            "replay_allowed": False,
        }
