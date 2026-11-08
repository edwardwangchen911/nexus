"""A missing test double must stop before a live SDK process or model send."""

from unittest.mock import AsyncMock

import pytest
from nexus.agents import codex_sdk
from openai_codex import AsyncCodex


@pytest.mark.asyncio
async def test_direct_test_function_without_autouse_stops_before_sdk_account(monkeypatch):
    account = AsyncMock(side_effect=AssertionError("LIVE_BOUNDARY_REACHED"))
    monkeypatch.setattr(AsyncCodex, "account", account)
    with pytest.raises(codex_sdk.CodexInvocationError) as error:
        await codex_sdk.invoke_codex({})
    assert account.await_count == 0
    assert error.value.reason == "CODEX_OFFLINE_TRANSPORT_REQUIRED"
    assert error.value.provider_call_started is False
    assert error.value.usage.provider_calls == 0


@pytest.mark.asyncio
async def test_explicit_offline_sdk_double_still_completes(monkeypatch):
    from unit.agents.test_codex_sdk import _sdk

    client, thread, _, _ = _sdk(monkeypatch)
    raw, usage = await codex_sdk.invoke_codex({})
    assert raw and usage.input_tokens == 300
    thread.run.assert_awaited_once()
    client.close.assert_awaited_once()


def test_isolated_worker_scope_in_child_without_pytest_cannot_launch_live_sdk(tmp_path):
    import os
    import subprocess
    import sys

    source = """
import asyncio
from types import SimpleNamespace
from nexus.agents import codex_sdk
from nexus.agents.model_io_guard import guard_worker_model_io
from openai_codex import AsyncCodex
calls = []
async def blocked(self, **kw):
    calls.append(1)
    raise AssertionError("LIVE_BOUNDARY_REACHED")
AsyncCodex.account = blocked
@guard_worker_model_io
async def worker(settings):
    return await codex_sdk.invoke_codex({})
async def main():
    try:
        await worker(SimpleNamespace(database_url="postgresql+asyncpg://localhost/nexus_test_child"))
    except codex_sdk.CodexInvocationError as error:
        assert error.reason == "CODEX_OFFLINE_TRANSPORT_REQUIRED", error.reason
    else:
        raise AssertionError("NOT_BLOCKED")
    assert calls == []
asyncio.run(main())
"""
    env = dict(os.environ)
    env.pop("PYTEST_CURRENT_TEST", None)
    result = subprocess.run([sys.executable, "-c", source], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_http_real_transport_rejected_but_mock_remains_available():
    import httpx
    from nexus.agents.model_io_guard import require_offline_http_transport

    async with httpx.AsyncClient() as client:
        with pytest.raises(ValueError, match="MODEL_OFFLINE_TRANSPORT_REQUIRED"):
            require_offline_http_transport(client)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200))
    ) as client:
        require_offline_http_transport(client)
        assert (await client.get("https://example.invalid")).status_code == 200


@pytest.mark.asyncio
async def test_http_guard_precedes_send_marker_and_callback(monkeypatch):
    import asyncio

    import httpx
    from nexus.agents.responses_request import send_response_stage

    boundary = AsyncMock(side_effect=AssertionError("LIVE_BOUNDARY_REACHED"))
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", boundary)
    callbacks = []
    loop = asyncio.get_running_loop()
    client = httpx.AsyncClient()
    result = await send_response_stage(
        client,
        httpx.Request("POST", "https://api.openai.com/v1/responses"),
        work_deadline=loop.time() + 30,
        cleanup_deadline=loop.time() + 35,
        before_send=lambda: callbacks.append(1),
    )
    assert str(result.error) == "MODEL_OFFLINE_TRANSPORT_REQUIRED"
    assert result.owner.send_started is False
    assert callbacks == [] and boundary.await_count == 0
