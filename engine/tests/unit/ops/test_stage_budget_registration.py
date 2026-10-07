"""Business registration requires a deliberate offline setup argument."""

from unittest.mock import AsyncMock

import pytest
from nexus.ops import openai_local
from nexus.settings import EngineSettings


@pytest.mark.parametrize("allow", [False, True])
def test_cli_dispatches_business_registration_only_when_explicit(monkeypatch, allow):
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus", agent_runtime_mode="codex_sdk"
    )
    prepare = AsyncMock()
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **k: settings)
    monkeypatch.setattr(openai_local, "prepare_database", prepare)
    monkeypatch.setattr(openai_local, "status", lambda _: {"execution_locked": True})
    argv = [
        "openai-local",
        "setup",
        "--actionable-paper",
        "--structured-synthesis",
        "--aggregate-input-tokens",
        "96000",
        "--max-wire-bytes",
        "64000",
        "--inline-citations",
        "--stage-time-budget",
        "--skip-key",
    ]
    if allow:
        argv.append("--allow-stage-time-budget-registration")
    monkeypatch.setattr("sys.argv", argv)
    assert openai_local.main() == 0
    options = prepare.await_args.kwargs
    assert options["stage_time_budget"] is True
    if allow:
        assert options["allow_stage_time_budget_registration"] is True
    else:
        assert "allow_stage_time_budget_registration" not in options


@pytest.mark.asyncio
@pytest.mark.parametrize("allow", [True, 1, "yes"])
async def test_business_registration_argument_requires_budget_and_strict_boolean(allow):
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus", agent_runtime_mode="codex_sdk"
    )
    with pytest.raises(ValueError, match="CODEX_STAGE_REGISTRATION_ARGUMENT_INVALID"):
        await openai_local.prepare_database(settings, allow_stage_time_budget_registration=allow)


def test_registration_cli_rejects_flag_without_budget_before_loading_settings(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        ["openai-local", "setup", "--allow-stage-time-budget-registration", "--skip-key"],
    )
    settings = AsyncMock()
    monkeypatch.setattr(openai_local, "local_settings", settings)
    with pytest.raises(SystemExit) as error:
        openai_local.main()
    assert error.value.code == 2
    settings.assert_not_called()
    assert "requires setup --stage-time-budget" in capsys.readouterr().err
