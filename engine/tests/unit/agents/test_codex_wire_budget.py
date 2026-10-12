"""Versioned wire bytes are independent of estimated tokens and cumulative usage."""

import json

import pytest
from unit.agents.test_codex_sdk import _authority, _sdk, _staged_batch, _tools


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "limit,size,allowed",
    [
        (55808, 55809, False),
        (64000, 64000, True),
        (64000, 64001, False),
        (100479, 100479, True),
        (100479, 100480, False),
    ],
)
async def test_actual_invocation_enforces_selected_wire_bytes(monkeypatch, limit, size, allowed):
    from nexus.agents import codex_sdk

    _, thread, _, configs = _sdk(monkeypatch)
    monkeypatch.setattr(codex_sdk, "codex_request_wire", lambda payload: "x" * size)
    monkeypatch.setattr(codex_sdk, "codex_input_allowance", lambda *args: 30000)
    if allowed:
        await codex_sdk.invoke_codex({}, max_wire_bytes=limit)
        thread.run.assert_awaited_once()
    else:
        with pytest.raises(ValueError, match="CODEX_INPUT_TOO_LARGE"):
            await codex_sdk.invoke_codex({}, max_wire_bytes=limit)
        assert not configs
        thread.run.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [64000, 100479])
async def test_wider_wire_keeps_single_request_token_preflight(monkeypatch, limit):
    from nexus.agents import codex_sdk

    _, thread, _, configs = _sdk(monkeypatch)
    monkeypatch.setattr(codex_sdk, "codex_input_allowance", lambda *args: 64001)
    with pytest.raises(ValueError, match="CODEX_INPUT_TOO_LARGE"):
        await codex_sdk.invoke_codex({}, max_wire_bytes=limit)
    assert not configs
    thread.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_staged_wire_limit_reaches_real_invocation_preflight(monkeypatch):
    from nexus.agents import codex_sdk, codex_staged
    from nexus.agents.committee import ROLE_ORDER

    request, evidence, prompts = _authority()
    _, thread, result, _ = _sdk(monkeypatch)
    result.final_response = json.dumps(_staged_batch("research", ROLE_ORDER[:5]))
    monkeypatch.setattr(codex_sdk, "codex_request_wire", lambda payload: "x" * 64000)
    monkeypatch.setattr(codex_staged, "codex_request_wire", lambda payload: "x" * 64000)
    monkeypatch.setattr(codex_sdk, "codex_input_allowance", lambda *args: 30000)
    monkeypatch.setattr(codex_staged, "codex_input_allowance", lambda *args: 30000)
    model = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts, tools=_tools(), aggregate_input_tokens=96000, max_wire_bytes=64000
    )
    await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    thread.run.assert_awaited_once()
    assert model.usage.provider_calls == 1
