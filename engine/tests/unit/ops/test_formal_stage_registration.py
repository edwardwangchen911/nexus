"""Explicit pending formal profile registration must fail before connection on ambiguity."""

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from nexus.agents.codex_candidate import candidate_config_hash
from nexus.agents.codex_formal_stage import formal_stage_config, formal_stage_config_hash
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION, SDK_CONFIG_VERSIONS
from nexus.ops import openai_local
from nexus.settings import EngineSettings

OPTIONS = dict(
    actionable_paper=True,
    structured_synthesis=True,
    aggregate_input_tokens=96000,
    max_wire_bytes=64000,
    inline_citations=True,
    stage_time_budget=True,
    allow_stage_time_budget_registration=True,
    formal_stage_capacity=True,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"formal_stage_capacity": 1},
        {"stage_time_budget": False},
        {"allow_stage_time_budget_registration": False},
        {"candidate_stage_capacity": True},
    ],
)
async def test_formal_registration_rejects_ambiguous_profile_before_connection(monkeypatch, change):
    def forbidden(*args, **kwargs):
        raise AssertionError("registration reached connection before validation")

    monkeypatch.setattr(openai_local, "session_factory", forbidden)
    settings = EngineSettings(
        database_url="postgresql+asyncpg://127.0.0.1/nexus_diag_formal",
        agent_runtime_mode="codex_sdk",
    )
    with pytest.raises(ValueError, match="CODEX_FORMAL_STAGE_REGISTRATION_ARGUMENT_INVALID"):
        await openai_local.prepare_database(settings, **(OPTIONS | change))


def test_pending_identity_freezes_capacity_and_remains_outside_runtime_versions():
    config = formal_stage_config()
    budget = json.loads(str(config["stage_time_budget"]))
    assert config["version"] == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
    assert FORMAL_STAGE_CAPACITY_CONFIG_VERSION not in SDK_CONFIG_VERSIONS
    assert formal_stage_config_hash() != candidate_config_hash()
    assert (
        budget["job_absolute_seconds"],
        budget["handler_absolute_seconds"],
        budget["lease_seconds"],
    ) == (180, 195, 210)
    assert budget["stage_caps_seconds"] == {"research": 60, "critique": 45, "synthesis": 45}
    config["max_input_tokens"] = 1
    assert formal_stage_config()["max_input_tokens"] == 96000


@pytest.mark.parametrize("action", ["status", "admit-research", "run-once"])
def test_cli_pending_flag_cannot_activate_research(monkeypatch, action):
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-openai-local",
            action,
            "--formal-stage-capacity",
            "--stage-time-budget",
            "--allow-stage-time-budget-registration",
        ],
    )
    with pytest.raises(SystemExit) as error:
        openai_local.main()
    assert error.value.code == 2


def test_cli_setup_dispatches_only_explicit_pending_registration(monkeypatch):
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus_diag_formal",
        agent_runtime_mode="codex_sdk",
    )
    prepare = AsyncMock()
    monkeypatch.chdir(Path(__file__).resolve().parents[4])
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-openai-local",
            "setup",
            "--backend",
            "codex_sdk",
            "--formal-stage-capacity",
            "--stage-time-budget",
            "--allow-stage-time-budget-registration",
            "--structured-synthesis",
            "--aggregate-input-tokens",
            "96000",
            "--max-wire-bytes",
            "64000",
            "--inline-citations",
            "--actionable-paper",
            "--skip-key",
        ],
    )
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    monkeypatch.setattr(openai_local, "prepare_database", prepare)
    monkeypatch.setattr(openai_local, "status", lambda _: {"execution_locked": True})
    assert openai_local.main() == 0
    prepare.assert_awaited_once_with(settings, staged=False, strategy_rnd=False, **OPTIONS)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"formal_bounded_stage_budget": 1},
        {"formal_flexible_stage_budget": True},
        {"flexible_stage_budget": True},
        {"full_input_capacity": False},
        {"formal_stage_capacity": False},
    ],
)
async def test_v11_registration_rejects_ambiguity_before_connection(monkeypatch, change):
    def forbidden(*args, **kwargs):
        raise AssertionError("must validate before connection")

    monkeypatch.setattr(openai_local, "session_factory", forbidden)
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus_test_v11", agent_runtime_mode="codex_sdk"
    )
    options = (
        OPTIONS
        | {
            "max_wire_bytes": 100479,
            "full_input_capacity": True,
            "formal_bounded_stage_budget": True,
        }
        | change
    )
    with pytest.raises(ValueError, match="CODEX_FORMAL_BOUNDED_ARGUMENT_INVALID"):
        await openai_local.prepare_database(settings, **options)
