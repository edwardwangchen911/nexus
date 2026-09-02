"""Local OpenAI setup must not expose credentials or silently call a provider."""

from pathlib import Path

import httpx
import pytest
from openai import RateLimitError


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
