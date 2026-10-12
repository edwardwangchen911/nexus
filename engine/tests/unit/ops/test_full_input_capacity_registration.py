"""v9 registration is explicit pending setup; numbers alone cannot select it."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from nexus.ops import openai_local
from nexus.settings import EngineSettings
from unit.ops.test_formal_stage_registration import OPTIONS

V9_OPTIONS = OPTIONS | {"max_wire_bytes": 100479, "full_input_capacity": True}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"formal_stage_capacity": False},
        {"full_input_capacity": 1},
        {"max_wire_bytes": 64000},
        {"candidate_stage_capacity": True},
        {"full_input_capacity": False},
    ],
)
async def test_v9_registration_refuses_ambiguous_selection_before_connection(monkeypatch, change):
    def forbidden(*args, **kwargs):
        raise AssertionError("invalid v9 setup reached a database")

    monkeypatch.setattr(openai_local, "session_factory", forbidden)
    with pytest.raises(ValueError, match="CODEX_CAPACITY_REGISTRATION_ARGUMENT_INVALID"):
        await openai_local.prepare_database(
            EngineSettings(
                database_url="postgresql+asyncpg://localhost/nexus_diag_v9",
                agent_runtime_mode="codex_sdk",
            ),
            **(V9_OPTIONS | change),
        )


def test_cli_v9_setup_dispatches_only_pending_exact_profile(monkeypatch):
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus_diag_v9", agent_runtime_mode="codex_sdk"
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
            "--full-input-capacity",
            "--stage-time-budget",
            "--allow-stage-time-budget-registration",
            "--structured-synthesis",
            "--aggregate-input-tokens",
            "96000",
            "--max-wire-bytes",
            "100479",
            "--inline-citations",
            "--actionable-paper",
            "--skip-key",
        ],
    )
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    monkeypatch.setattr(openai_local, "prepare_database", prepare)
    monkeypatch.setattr(openai_local, "status", lambda _: {"execution_locked": True})
    assert openai_local.main() == 0
    prepare.assert_awaited_once_with(settings, staged=False, strategy_rnd=False, **V9_OPTIONS)


@pytest.mark.parametrize("action", ["status", "admit-research", "run-once"])
def test_cli_v9_flag_never_starts_research(monkeypatch, action):
    monkeypatch.setattr("sys.argv", ["nexus-openai-local", action, "--full-input-capacity"])
    with pytest.raises(SystemExit) as error:
        openai_local.main()
    assert error.value.code == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query",
    [
        "host=remote.invalid",
        "hostaddr=192.0.2.1",
        "port=9999",
        "dsn=other",
        "database=other",
        "dbname=other",
    ],
)
async def test_v9_registration_rejects_connection_overrides_before_factory(monkeypatch, query):
    def forbidden(*args, **kwargs):
        raise AssertionError("redirected v9 setup reached a database")

    monkeypatch.setattr(openai_local, "session_factory", forbidden)
    with pytest.raises(ValueError, match="OPENAI_LOCAL_DATABASE_REQUIRED"):
        await openai_local.prepare_database(
            EngineSettings(
                database_url="postgresql+asyncpg://localhost/nexus_diag_v9?" + query,
                agent_runtime_mode="codex_sdk",
            ),
            **V9_OPTIONS,
        )
