"""Local OpenAI setup must not expose credentials or silently call a provider."""

from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from openai import RateLimitError


@pytest.mark.parametrize("flag", ["--strategy-rnd", "--structured-synthesis"])
def test_strategy_rnd_setup_dispatches_explicit_offline_registration(
    monkeypatch, capsys, flag
) -> None:
    from nexus.ops import openai_local
    from nexus.settings import EngineSettings

    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus",
        environment="development",
        agent_runtime_mode="codex_sdk",
    )
    prepare = AsyncMock()
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-openai-local",
            "setup",
            "--backend",
            "codex_sdk",
            flag,
            "--skip-key",
        ],
    )
    monkeypatch.setattr(openai_local, "local_settings", lambda *args, **kwargs: settings)
    monkeypatch.setattr(openai_local, "prepare_database", prepare)
    monkeypatch.setattr(
        openai_local,
        "status",
        lambda _settings: {
            "ready_for_model_requests": True,
            "execution_locked": True,
        },
    )

    assert openai_local.main() == 0
    prepare.assert_awaited_once_with(
        settings,
        staged=False,
        strategy_rnd=flag == "--strategy-rnd",
        **({"structured_synthesis": True} if flag == "--structured-synthesis" else {}),
    )
    assert "registered" in capsys.readouterr().out


@pytest.mark.parametrize(
    "argv,code",
    [
        (
            ["setup", "--backend", "responses", "--strategy-rnd", "--skip-key"],
            "CODEX_STRATEGY_RND_SETUP_ONLY",
        ),
        (
            [
                "setup",
                "--backend",
                "codex_sdk",
                "--strategy-rnd",
                "--staged",
                "--skip-key",
            ],
            "CODEX_STRATEGY_RND_STAGED_CONFLICT",
        ),
        (
            ["status", "--backend", "codex_sdk", "--strategy-rnd"],
            "CODEX_STRATEGY_RND_SETUP_ONLY",
        ),
    ],
)
def test_strategy_rnd_setup_rejects_ambiguous_or_non_setup_modes(
    monkeypatch, capsys, argv: list[str], code: str
) -> None:
    from nexus.ops import openai_local

    monkeypatch.setattr("sys.argv", ["nexus-openai-local", *argv])
    assert openai_local.main() == 1
    assert code in capsys.readouterr().out


def test_local_setup_reports_missing_key_without_enabling_responses(
    tmp_path: Path, monkeypatch
) -> None:
    from nexus.ops.openai_local import local_settings, status

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    settings = local_settings(tmp_path)
    assert settings.agent_runtime_mode == "fixture"
    assert status(settings)["ready_for_model_requests"] is False


def test_offline_preparation_ignores_model_credentials(tmp_path: Path, monkeypatch) -> None:
    from nexus.ops.openai_local import local_settings

    monkeypatch.setenv("OPENAI_API_KEY", "invalid whitespace key")
    key = tmp_path / ".secrets/nexus-openai-local/api-key"
    key.parent.mkdir(parents=True)
    key.write_text("invalid whitespace key")
    key.chmod(0o644)
    settings = local_settings(tmp_path, load_model_key=False)
    assert settings.openai_api_key is None
    assert settings.agent_runtime_mode == "fixture"


def test_private_key_file_is_loaded_but_never_returned(tmp_path: Path, monkeypatch) -> None:
    from nexus.ops.openai_local import local_settings, status

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    key = tmp_path / ".secrets/nexus-openai-local/api-key"
    key.parent.mkdir(parents=True, mode=0o700)
    key.write_text("test-only-openai-key\n")
    key.chmod(0o600)
    settings = local_settings(tmp_path)
    assert settings.agent_runtime_mode == "responses"
    assert settings.openai_api_key.get_secret_value() == "test-only-openai-key"
    assert "test-only-openai-key" not in str(status(settings))
    key.chmod(0o644)
    with pytest.raises(ValueError, match="OPENAI_KEY_FILE_UNSAFE"):
        local_settings(tmp_path)


@pytest.mark.asyncio
async def test_openai_verification_sends_only_synthetic_input_and_never_retries(
    monkeypatch, tmp_path: Path
) -> None:
    from nexus.ops.openai_local import local_settings, verify_connection

    monkeypatch.setenv("OPENAI_API_KEY", "test-only-openai-key")
    requests = []

    async def reject(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            429, json={"error": {"message": "rate limited", "type": "rate_limit_error"}}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(reject)) as transport:
        with pytest.raises(RateLimitError):
            await verify_connection(local_settings(tmp_path), http_client=transport)
    assert len(requests) == 1
    assert requests[0].url == "https://api.openai.com/v1/responses"
    import json

    payload = json.loads(requests[0].content)
    assert payload["store"] is False
    assert payload["max_output_tokens"] == 128
    assert "tools" not in payload
    assert (
        payload["input"]
        == "Reply exactly NEXUS_OPENAI_OK. This is a connectivity check, not financial research."
    )


def test_key_entry_is_private_and_never_overwrites_an_existing_key(
    tmp_path: Path, monkeypatch
) -> None:
    from nexus.ops.openai_local import _save_key

    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr("getpass.getpass", lambda prompt: "test-only-openai-key")
    _save_key(tmp_path)
    key = tmp_path / ".secrets/nexus-openai-local/api-key"
    assert key.stat().st_mode & 0o777 == 0o600
    assert key.read_text() == "test-only-openai-key\n"
    with pytest.raises(FileExistsError):
        _save_key(tmp_path)


@pytest.mark.asyncio
async def test_verification_accepts_a_real_sdk_shape_without_claiming_research(
    tmp_path: Path, monkeypatch
) -> None:
    from nexus.ops.openai_local import local_settings, verify_connection

    monkeypatch.setenv("OPENAI_API_KEY", "test-only-openai-key")

    async def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "resp_fixture",
                "object": "response",
                "created_at": 0,
                "status": "completed",
                "model": "gpt-5.4-mini-2026-03-17",
                "error": None,
                "incomplete_details": None,
                "instructions": None,
                "metadata": {},
                "parallel_tool_calls": False,
                "temperature": 1,
                "tool_choice": "none",
                "tools": [],
                "top_p": 1,
                "output": [
                    {
                        "id": "msg_fixture",
                        "type": "message",
                        "status": "completed",
                        "role": "assistant",
                        "content": [
                            {"type": "output_text", "text": "NEXUS_OPENAI_OK", "annotations": []}
                        ],
                    }
                ],
                "usage": {
                    "input_tokens": 20,
                    "output_tokens": 6,
                    "total_tokens": 26,
                    "input_tokens_details": {"cached_tokens": 0},
                    "output_tokens_details": {"reasoning_tokens": 0},
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as transport:
        result = await verify_connection(local_settings(tmp_path), http_client=transport)
    assert result["status"] == "OPENAI_CONNECTED"
    assert result["research_completed"] is False
    assert result["physical_requests"] == 1
    assert "test-only-openai-key" not in str(result)


@pytest.mark.asyncio
async def test_research_preparation_rejects_remote_database_before_connection() -> None:
    from datetime import UTC, datetime
    from uuid import UUID

    from nexus.ops.openai_local import prepare_local_research
    from nexus.settings import EngineSettings

    with pytest.raises(ValueError, match="OPENAI_LOCAL_DATABASE_REQUIRED"):
        await prepare_local_research(
            EngineSettings(database_url="postgresql+asyncpg://remote.invalid/nexus"),
            UUID(int=42),
            as_of=datetime(2026, 9, 2, tzinfo=UTC),
        )


@pytest.mark.parametrize("extra", [["--backend", "responses"], ["--staged"], ["--strategy-rnd"]])
def test_structured_setup_rejects_ambiguous_backend_or_version(monkeypatch, capsys, extra):
    from nexus.ops import openai_local

    monkeypatch.setattr(
        "sys.argv", ["nexus-openai-local", "setup", "--structured-synthesis", *extra]
    )
    assert openai_local.main() == 1
    assert "CODEX_STRUCTURED_SETUP_" in capsys.readouterr().out


def test_explicit_research_admission_uses_normal_configuration(monkeypatch, tmp_path, capsys):
    from nexus.ops import openai_local
    from nexus.settings import EngineSettings

    request = tmp_path / "request.json"
    request.write_text("{}")
    settings = EngineSettings(database_url="postgresql+asyncpg://localhost/nexus")
    admit = AsyncMock(return_value={"status": "prepared", "model_requests": 0})
    monkeypatch.setattr(openai_local, "admit_local_research", admit, raising=False)
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    monkeypatch.setattr(
        "sys.argv", ["nexus-openai-local", "admit-research", "--research-request", str(request)]
    )
    assert openai_local.main() == 0
    admit.assert_awaited_once_with(settings, request)
    assert '"model_requests": 0' in capsys.readouterr().out


def test_explicit_portfolio_run_once_dispatches_only_the_dedicated_worker(
    monkeypatch, capsys
) -> None:
    from nexus.ops import openai_local
    from nexus.settings import EngineSettings

    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus",
        environment="development",
        agent_runtime_mode="codex_sdk",
    )
    run = AsyncMock(return_value=True)
    monkeypatch.setattr(
        "sys.argv",
        ["nexus-openai-local", "run-portfolio-once", "--backend", "codex_sdk"],
    )
    monkeypatch.chdir(Path(__file__).resolve().parents[4])
    monkeypatch.setattr(openai_local, "local_settings", lambda *args, **kwargs: settings)
    monkeypatch.setattr(
        openai_local,
        "status",
        lambda _settings: {"ready_for_model_requests": True, "execution_locked": True},
    )
    monkeypatch.setattr("nexus.worker.run_portfolio_decision_worker_once", run)

    assert openai_local.main() == 0
    run.assert_awaited_once_with(settings)
    assert '"job_processed": true' in capsys.readouterr().out


def test_paid_portfolio_transport_binds_the_complete_registered_configuration(
    monkeypatch,
) -> None:
    from decimal import Decimal

    from nexus.agents.responses import ResponsesModelConfig
    from nexus.ops.openai_local import MODEL_ID
    from nexus.settings import EngineSettings
    from nexus.worker import _openai_portfolio_responses_client

    class FakeAsyncOpenAI:
        def __init__(self, **kwargs: object) -> None:
            self.max_retries = kwargs["max_retries"]
            self.responses = object()

    monkeypatch.setattr("nexus.worker.AsyncOpenAI", FakeAsyncOpenAI)
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus",
        environment="development",
        agent_runtime_mode="responses",
        openai_api_key="fixture-key",
        openai_extraction_model=MODEL_ID,
        openai_reasoning_model=MODEL_ID,
    )
    client = _openai_portfolio_responses_client(settings)
    expected = ResponsesModelConfig(
        model_id=MODEL_ID,
        pricing_version="openai-standard-2026-09-02",
        max_input_tokens=64_000,
        max_output_tokens=8_192,
        max_tool_calls=1,
        absolute_timeout_seconds=Decimal("105"),
        input_usd_per_million=Decimal("0.75"),
        output_usd_per_million=Decimal("4.50"),
    )

    assert client.supports_model_config(expected) is True
    for update in (
        {"pricing_version": "codex-sdk-portfolio-decision-v1"},
        {"max_input_tokens": 128_000},
        {"max_output_tokens": 16_384},
        {"input_usd_per_million": Decimal("0")},
    ):
        assert client.supports_model_config(expected.model_copy(update=update)) is False


def test_api_can_select_responses_authority_without_holding_provider_credentials() -> None:
    from nexus.settings import AgentRuntimeMode, EngineSettings

    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus",
        environment="development",
        portfolio_decision_runtime_mode="responses",
    )

    assert settings.agent_runtime_mode is AgentRuntimeMode.FIXTURE
    assert settings.portfolio_decision_runtime_mode is AgentRuntimeMode.RESPONSES
    assert settings.openai_api_key is None
