"""Price monitoring is an explicit formal setup option, never an execution switch."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from nexus.agents.committee import (
    DAILY_PENDING_SCOPE_PROMPT_VERSION,
    INFERENCE_REVIEW_SCOPE_PROMPT_VERSION,
)
from nexus.ops import openai_local
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentPromptVersionRecord
from nexus.settings import EngineSettings

OPTIONS = dict(
    actionable_paper=True,
    structured_synthesis=True,
    aggregate_input_tokens=96000,
    max_wire_bytes=100479,
    inline_citations=True,
    stage_time_budget=True,
    allow_stage_time_budget_registration=True,
    formal_stage_capacity=True,
    full_input_capacity=True,
    formal_bounded_stage_budget=True,
)
SETTINGS = EngineSettings(
    database_url="postgresql+asyncpg://localhost/nexus_test_monitor_registration",
    agent_runtime_mode="codex_sdk",
)
FLAGS = [
    "--actionable-paper",
    "--structured-synthesis",
    "--aggregate-input-tokens",
    "96000",
    "--max-wire-bytes",
    "100479",
    "--inline-citations",
    "--stage-time-budget",
    "--allow-stage-time-budget-registration",
    "--formal-stage-capacity",
    "--full-input-capacity",
    "--formal-bounded-stage-budget",
    "--skip-key",
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"price_monitoring": 1},
        {"formal_bounded_stage_budget": False},
        {"full_input_capacity": False},
        {"formal_stage_capacity": False},
        {"allow_stage_time_budget_registration": False},
        {"stage_time_budget": False},
        {"actionable_paper": False},
        {"structured_synthesis": False},
        {"aggregate_input_tokens": 64000},
        {"max_wire_bytes": 64000},
        {"inline_citations": False},
        {"candidate_stage_capacity": True},
    ],
)
async def test_monitor_registration_rejects_incomplete_contract_before_connection(
    monkeypatch, change
):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("invalid monitoring setup reached the database")

    monkeypatch.setattr(openai_local, "session_factory", forbidden)
    with pytest.raises(ValueError):
        await openai_local.prepare_database(
            SETTINGS, **(OPTIONS | {"price_monitoring": True} | change)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("monitoring", [False, True])
async def test_prepare_registers_only_explicit_prompt_and_existing_model_contract(
    monkeypatch, monitoring
):
    records = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def begin(self):
            return self

        async def scalar(self, statement):
            return (
                "nexus_test_monitor_registration" if "current_database" in str(statement) else None
            )

        def add(self, record):
            records.append(record)

    monkeypatch.setattr(openai_local, "session_factory", lambda _settings: Session)
    await openai_local.prepare_database(
        SETTINGS, **OPTIONS, **({"price_monitoring": True} if monitoring else {})
    )
    assert all(
        isinstance(row, AgentPromptVersionRecord | AgentModelConfigRecord) for row in records
    )
    portfolio = [
        row
        for row in records
        if isinstance(row, AgentPromptVersionRecord) and row.role == "portfolio"
    ]
    assert portfolio[-1].version == (
        DAILY_PENDING_SCOPE_PROMPT_VERSION if monitoring else INFERENCE_REVIEW_SCOPE_PROMPT_VERSION
    )
    models = [row for row in records if isinstance(row, AgentModelConfigRecord)]
    assert len(models) == 2
    assert models[0].version == "codex-sdk-chatgpt-v11-bounded-stage-budget"
    assert models[0].max_input_tokens == 96000 and models[0].max_output_tokens == 8192


@pytest.mark.parametrize("action", ["status", "admit-research", "run-once", "setup"])
def test_cli_monitor_flag_rejects_non_setup_or_missing_formal_budget(monkeypatch, action):
    monkeypatch.setattr("sys.argv", ["nexus-openai-local", action, "--price-monitoring"])
    monkeypatch.setattr(
        openai_local, "local_settings", lambda *_a, **_kw: pytest.fail("read settings")
    )
    with pytest.raises(SystemExit) as error:
        openai_local.main()
    assert error.value.code == 2


@pytest.mark.parametrize("monitoring", [False, True])
def test_cli_dispatches_explicit_setup_without_starting_research(monkeypatch, monitoring):
    prepare = AsyncMock()
    monkeypatch.chdir(Path(__file__).resolve().parents[4])
    monkeypatch.setattr(
        "sys.argv",
        ["nexus-openai-local", "setup", *FLAGS, *(["--price-monitoring"] if monitoring else [])],
    )
    monkeypatch.setattr(openai_local, "local_settings", lambda *_a, **_kw: SETTINGS)
    monkeypatch.setattr(openai_local, "prepare_database", prepare)
    monkeypatch.setattr(openai_local, "status", lambda _: {"execution_locked": True})
    assert openai_local.main() == 0
    prepare.assert_awaited_once_with(
        SETTINGS,
        staged=False,
        strategy_rnd=False,
        **OPTIONS,
        **({"price_monitoring": True} if monitoring else {}),
    )
