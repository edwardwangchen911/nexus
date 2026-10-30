"""The registered API profile describes the actual transport, without activating it."""

import json
from unittest.mock import AsyncMock
from uuid import uuid4

from nexus.ops import research_comparison


def test_normal_profile_command_is_registration_only(monkeypatch, capsys, tmp_path):
    native = AsyncMock(return_value={"status": "profile_registered", "dispatch_authority": False})
    source = uuid4()
    monkeypatch.setattr(research_comparison, "native_command", native)
    monkeypatch.setattr(
        "sys.argv",
        [
            "comparison",
            "register-profile",
            "--repo-root",
            str(tmp_path),
            "--sdk-model-config-id",
            str(source),
        ],
    )
    assert research_comparison.main() == 0
    args = native.call_args.args[0]
    assert args.sdk_model_config_id == source and args.command == "register-profile"
    assert json.loads(capsys.readouterr().out)["dispatch_authority"] is False


def test_api_profile_matches_wire_and_preserves_limits():
    from nexus.agents.codex_bounded_budget import bounded_stage_config
    from nexus.agents.comparison_manifest import SDK_HASH
    from nexus.agents.comparison_profile import api_profile, api_profile_hash
    from nexus.agents.research_stages.http_lifecycle import http_stage_wire
    from pydantic import BaseModel

    class Output(BaseModel):
        result: str

    profile = api_profile()
    wire = json.loads(http_stage_wire({"stage": "research"}, output_model=Output, forbidden=()))
    assert profile["model_id"] == wire["model"] == "gpt-5.6-terra"
    assert profile["reasoning_effort"] == wire["reasoning"]["effort"] == "low"
    assert profile["stream"] == wire["stream"] is True
    assert profile["store"] == wire["store"] is False
    assert profile["endpoint"] == "https://api.openai.com/v1/responses"
    assert profile["source_sdk_config_hash"] == SDK_HASH != api_profile_hash()
    for key in (
        "max_input_tokens",
        "max_output_tokens",
        "max_tool_calls",
        "max_wire_bytes",
        "stage_time_budget",
        "pricing_available_at",
        "pricing_expires_at",
        "input_usd_per_million",
        "output_usd_per_million",
        "billing_mode",
    ):
        assert profile[key] == bounded_stage_config()[key]
    assert "max_output_tokens" not in wire


def test_registration_commit_unknown_is_safe_and_has_read_only_entry(monkeypatch, capsys, tmp_path):
    from sqlalchemy.exc import SQLAlchemyError

    native = AsyncMock(side_effect=SQLAlchemyError("private response must not escape"))
    monkeypatch.setattr(research_comparison, "native_command", native)
    monkeypatch.setattr(
        "sys.argv",
        [
            "comparison",
            "register-profile",
            "--repo-root",
            str(tmp_path),
            "--sdk-model-config-id",
            str(uuid4()),
        ],
    )
    assert research_comparison.main() == 1
    assert json.loads(capsys.readouterr().out) == {
        "status": "unknown",
        "reason": "COMPARISON_PROFILE_REGISTRATION_UNKNOWN",
    }
    native.side_effect = None
    native.return_value = {"status": "profile_not_registered", "dispatch_authority": False}
    monkeypatch.setattr("sys.argv", ["comparison", "inspect-profile", "--repo-root", str(tmp_path)])
    assert research_comparison.main() == 0
    assert native.call_args.args[0].command == "inspect-profile"
