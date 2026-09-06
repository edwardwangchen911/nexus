"""The new input view is opt-in and retains the existing request budgets."""

from unittest.mock import AsyncMock

import pytest
from nexus.agents import codex_versions
from nexus.ops import openai_local
from nexus.settings import EngineSettings

VERSION = "codex-sdk-chatgpt-v3-input96k-wire64k-cite1"


def test_citation_version_uses_existing_structured_limits():
    assert VERSION in codex_versions.STRUCTURED_CONFIG_VERSIONS
    assert codex_versions.aggregate_input_limit(VERSION) == 96000
    assert codex_versions.wire_byte_limit(VERSION) == 64000
    assert codex_versions.aggregate_input_limit("codex-sdk-chatgpt-v3") == 64000
    assert codex_versions.wire_byte_limit("codex-sdk-chatgpt-v3") == 55808


def test_normal_cli_passes_explicit_citation_setting(monkeypatch, capsys):
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus", agent_runtime_mode="codex_sdk"
    )
    prepare = AsyncMock()
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **k: settings)
    monkeypatch.setattr(openai_local, "prepare_database", prepare)
    monkeypatch.setattr(openai_local, "status", lambda _: {"execution_locked": True})
    monkeypatch.setattr(
        "sys.argv",
        [
            "openai-local",
            "setup",
            "--structured-synthesis",
            "--aggregate-input-tokens",
            "96000",
            "--max-wire-bytes",
            "64000",
            "--inline-citations",
            "--skip-key",
        ],
    )
    assert openai_local.main() == 0
    prepare.assert_awaited_once_with(
        settings,
        staged=False,
        strategy_rnd=False,
        structured_synthesis=True,
        aggregate_input_tokens=96000,
        max_wire_bytes=64000,
        inline_citations=True,
    )
    assert "registered" in capsys.readouterr().out


@pytest.mark.parametrize(
    "args",
    [
        ["status"],
        ["setup"],
        ["setup", "--structured-synthesis"],
        ["setup", "--structured-synthesis", "--aggregate-input-tokens", "96000"],
        ["setup", "--backend", "responses"],
    ],
)
def test_invalid_inline_mode_rejects_before_loading_settings(monkeypatch, capsys, args):
    def forbidden(*a, **k):
        raise AssertionError("invalid mode must not load runtime configuration")

    monkeypatch.setattr(openai_local, "local_settings", forbidden)
    monkeypatch.setattr("sys.argv", ["openai-local", *args, "--inline-citations"])
    assert openai_local.main() == 1
    assert "CODEX_CITATION_SETUP_INVALID" in capsys.readouterr().out


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [True, 1, "true"])
async def test_direct_setup_rejects_unsupported_citation_config(value):
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus", agent_runtime_mode="codex_sdk"
    )
    with pytest.raises(ValueError, match="CODEX_CITATION_SETUP_INVALID"):
        await openai_local.prepare_database(settings, inline_citations=value)
