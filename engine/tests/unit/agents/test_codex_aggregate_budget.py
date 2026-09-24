"""A new cumulative allowance never enlarges a single request or grants research authority."""

import json
from unittest.mock import AsyncMock

import pytest
from nexus.agents.responses_budget import UsageSnapshot
from unit.agents.test_codex_sdk import _authority, _staged_batch, _tools


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [64000, 96000])
async def test_frozen_aggregate_limit_controls_third_stage(limit, monkeypatch):
    from nexus.agents import codex_staged
    from nexus.agents.committee import ROLE_ORDER

    request, evidence, prompts = _authority()

    async def invoke(payload, *, output_model):
        return json.dumps(
            _staged_batch(payload["stage"], [p["role"] for p in payload["prompts"]])
        ), UsageSnapshot(input_tokens=30000, output_tokens=80)

    called = AsyncMock(side_effect=invoke)
    monkeypatch.setattr(codex_staged, "invoke_codex", called)
    monkeypatch.setattr(codex_staged, "codex_input_allowance", lambda *args: 30000)
    model = codex_staged.StagedCodexCommittee(
        prompts=prompts, tools=_tools(), aggregate_input_tokens=limit
    )
    for role in ROLE_ORDER[:-1]:
        await model.analyze(role, request, evidence, prompts[role])
    if limit == 64000:
        with pytest.raises(ValueError, match="CODEX_AGGREGATE_ALLOWANCE_EXHAUSTED"):
            await model.analyze("portfolio", request, evidence, prompts["portfolio"])
        assert called.await_count == 2
        assert model.failure_reason == "model_stage_input_allowance_exhausted"
    else:
        await model.analyze("portfolio", request, evidence, prompts["portfolio"])
        assert model.usage.input_tokens == 90000 and called.await_count == 3


@pytest.mark.asyncio
async def test_96k_actual_overrun_still_stops(monkeypatch):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    invoked = AsyncMock(
        return_value=(
            json.dumps(_staged_batch("research", tuple(prompts)[:5])),
            UsageSnapshot(input_tokens=96001, output_tokens=80),
        )
    )
    monkeypatch.setattr(codex_staged, "invoke_codex", invoked)
    model = codex_staged.StagedCodexCommittee(
        prompts=prompts, tools=_tools(), aggregate_input_tokens=96000
    )
    with pytest.raises(ValueError, match="CODEX_TOKEN_LIMIT_EXCEEDED"):
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert model.usage.input_tokens == 96001 and invoked.await_count == 1


@pytest.mark.asyncio
async def test_96k_keeps_wire_cap_before_provider_call(monkeypatch):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    called = AsyncMock()
    monkeypatch.setattr(codex_staged, "invoke_codex", called)
    monkeypatch.setattr(codex_staged, "codex_request_wire", lambda payload: "x" * 55809)
    model = codex_staged.StagedCodexCommittee(
        prompts=prompts, tools=_tools(), aggregate_input_tokens=96000
    )
    with pytest.raises(ValueError, match="CODEX_AGGREGATE_ALLOWANCE_EXHAUSTED"):
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    called.assert_not_awaited()
    assert model.failure_reason == "model_stage_wire_bytes_exceeded"


@pytest.mark.asyncio
async def test_expired_stage_deadline_records_safe_reason_without_provider_call(monkeypatch):
    from nexus.agents import codex_staged

    request, evidence, prompts = _authority()
    called = AsyncMock()
    monkeypatch.setattr(codex_staged, "invoke_codex", called)
    model = codex_staged.StagedCodexCommittee(
        prompts=prompts, tools=_tools(), deadline=0
    )
    with pytest.raises(ValueError, match="CODEX_AGGREGATE_ALLOWANCE_EXHAUSTED"):
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    called.assert_not_awaited()
    assert model.failure_reason == "model_stage_deadline_elapsed"


@pytest.mark.parametrize("limit", [True, 64000.0, 80000, 96001])
def test_arbitrary_aggregate_limits_are_rejected(limit):
    from nexus.agents.codex_staged import StagedCodexCommittee

    _, _, prompts = _authority()
    with pytest.raises(ValueError, match="CODEX_AGGREGATE_INPUT_INVALID"):
        StagedCodexCommittee(prompts=prompts, tools=_tools(), aggregate_input_tokens=limit)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["hash", "version", "input_limit", "output_limit"])
async def test_worker_rejects_budget_that_does_not_bind_frozen_version(monkeypatch, damage):
    from types import SimpleNamespace

    from nexus import worker
    from nexus.agents.codex_versions import STRUCTURED_96K_CONFIG_VERSION
    from nexus.agents.price_authority import FrozenPriceAuthorityStatus
    from nexus.settings import EngineSettings

    model = SimpleNamespace(
        version=STRUCTURED_96K_CONFIG_VERSION,
        model_id="gpt-5.4-mini",
        config_hash="a" * 64,
        max_input_tokens=96000,
        max_output_tokens=8192,
        input_usd_per_million=0,
        output_usd_per_million=0,
    )
    run = SimpleNamespace(
        model_config_id=None, pricing_version=model.version, model_hash=model.config_hash
    )
    if damage == "hash":
        run.model_hash = "b" * 64
    elif damage == "version":
        run.pricing_version = "codex-sdk-chatgpt-v3"
    elif damage == "input_limit":
        model.max_input_tokens = 64000
    else:
        model.max_output_tokens = 16384
    session = SimpleNamespace(get=AsyncMock(return_value=model))
    monkeypatch.setattr(worker, "load_frozen_evidence", AsyncMock(return_value=(object(),)))
    monkeypatch.setattr(
        worker,
        "revalidate_frozen_price_authority",
        AsyncMock(return_value=FrozenPriceAuthorityStatus.MISSING),
    )
    monkeypatch.setattr(worker, "load_frozen_prompts", AsyncMock(return_value={}))
    result = await worker._committee(
        session,
        EngineSettings(agent_runtime_mode="codex_sdk"),
        run,
        responses_client=None,
        tools=_tools(),
    )
    assert isinstance(result, worker._UnavailableResponsesCommittee)


@pytest.mark.asyncio
async def test_worker_builds_actionable_committee_with_exact_frozen_limits(monkeypatch):
    from types import SimpleNamespace

    from nexus import worker
    from nexus.agents.codex_sdk import MODEL_ID
    from nexus.agents.codex_staged import StructuredStagedCodexCommittee
    from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION
    from nexus.agents.price_authority import FrozenPriceAuthorityStatus
    from nexus.settings import EngineSettings

    _, _, prompts = _authority()
    model = SimpleNamespace(
        version=ACTIONABLE_CONFIG_VERSION,
        model_id=MODEL_ID,
        config_hash="a" * 64,
        max_input_tokens=96000,
        max_output_tokens=8192,
        input_usd_per_million=0,
        output_usd_per_million=0,
    )
    run = SimpleNamespace(
        model_config_id=None, pricing_version=model.version, model_hash=model.config_hash
    )
    session = SimpleNamespace(get=AsyncMock(return_value=model))
    monkeypatch.setattr(worker, "load_frozen_evidence", AsyncMock(return_value=(object(),)))
    monkeypatch.setattr(
        worker,
        "revalidate_frozen_price_authority",
        AsyncMock(return_value=FrozenPriceAuthorityStatus.MISSING),
    )
    monkeypatch.setattr(worker, "load_frozen_prompts", AsyncMock(return_value=prompts))
    result = await worker._committee(
        session,
        EngineSettings(agent_runtime_mode="codex_sdk"),
        run,
        responses_client=None,
        tools=_tools(),
    )
    assert isinstance(result, StructuredStagedCodexCommittee)
    assert result._aggregate_input_tokens == 96000
    assert result._max_wire_bytes == 64000
    assert result._inline_citations is True


@pytest.mark.parametrize(
    "args",
    [
        ["run-once"],
        ["setup"],
        ["setup", "--staged"],
        ["setup", "--structured-synthesis", "--backend", "responses"],
    ],
)
def test_new_budget_is_only_registered_by_explicit_structured_setup(args, monkeypatch, capsys):
    from nexus.ops import openai_local

    def forbidden(*args, **kwargs):
        raise AssertionError("configuration must not load")

    monkeypatch.setattr(openai_local, "local_settings", forbidden)
    monkeypatch.setattr(
        "sys.argv", ["nexus-openai-local", *args, "--aggregate-input-tokens", "96000"]
    )
    assert openai_local.main() == 1
    assert json.loads(capsys.readouterr().out)["code"] == "CODEX_AGGREGATE_SETUP_INVALID"
