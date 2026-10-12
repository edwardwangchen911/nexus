"""Isolated composition uses v9 identity, never a widened v8 identity."""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus import worker
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.price_authority import FrozenPriceAuthorityStatus
from nexus.settings import EngineSettings
from unit.agents.test_codex_sdk import _authority, _tools


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid", [None, "hash", "run_hash", "tokens", "deadline", "runtime", "mixed"]
)
async def test_formal_capacity_factory_preserves_v9_profile_and_pool_gate(monkeypatch, invalid):
    from nexus.agents.codex_capacity import capacity_config, capacity_config_hash

    config = capacity_config()
    model = SimpleNamespace(
        **{
            k: config[k]
            for k in (
                "version",
                "model_id",
                "max_input_tokens",
                "max_output_tokens",
                "max_tool_calls",
            )
        }
    )
    model.config_hash = capacity_config_hash()
    model.input_usd_per_million = model.output_usd_per_million = 0
    for key in ("pricing_available_at", "pricing_expires_at"):
        setattr(model, key, datetime.fromisoformat(config[key]))
    run = SimpleNamespace(
        model_config_id=uuid4(), pricing_version=model.version, model_hash=model.config_hash
    )
    session = SimpleNamespace(get=AsyncMock(return_value=model))
    _, _, prompts = _authority()
    monkeypatch.setattr(worker, "load_frozen_evidence", AsyncMock(return_value=(object(),)))
    monkeypatch.setattr(worker, "load_frozen_prompts", AsyncMock(return_value=prompts))
    monkeypatch.setattr(
        worker,
        "revalidate_frozen_price_authority",
        AsyncMock(return_value=FrozenPriceAuthorityStatus.MISSING),
    )
    if invalid == "hash":
        model.config_hash = "wrong"
    if invalid == "run_hash":
        run.model_hash = "wrong"
    if invalid == "tokens":
        model.max_input_tokens = 96001
    settings = EngineSettings(agent_runtime_mode="fixture" if invalid == "runtime" else "codex_sdk")
    result = await worker._committee(
        session,
        settings,
        run,
        responses_client=None,
        tools=_tools(),
        deadline=None if invalid == "deadline" else 1000,
        formal_stage_capacity=True,
        full_input_capacity=invalid == "mixed",
    )
    if invalid is not None:
        assert isinstance(result, worker._UnavailableResponsesCommittee)
        return
    assert isinstance(result, StructuredStagedCodexCommittee)
    assert result._max_wire_bytes == 100479
    assert result._stage_budget_version == config["version"]
    assert result._deadline == 980  # Handler grace 15s plus existing terminal reserve 5s.
    assert result._capacity_pool_manifest is None
    request, evidence, _ = _authority()
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_REQUIRED$"):
        await result.analyze("orchestrator", request, evidence, result._prompts["orchestrator"])

    import asyncio
    import json

    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
    from nexus.agents.committee import ROLE_ORDER
    from unit.agents.test_codex_sdk import _assessment_payload, _sdk, _staged_batch

    _, thread, response, _ = _sdk(monkeypatch)
    result._deadline = asyncio.get_running_loop().time() + 175
    monkeypatch.setattr(
        result,
        "_inputs",
        AsyncMock(
            return_value={"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
        ),
    )
    await preflight_capacity_pool(
        [CapacityPoolMember(result, request, evidence)],
        expected_instrument_ids=(request.instrument_id,),
    )

    async def invoke(wire, **options):
        payload = json.loads(wire)
        response.final_response = json.dumps(
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        return response

    thread.run.side_effect = invoke
    for role in ROLE_ORDER:
        assert (await result.analyze(role, request, evidence, result._prompts[role])).role == role
    assert result.usage.provider_calls == 3 and thread.run.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [None, "hash", "v8", "main", "runtime", "deadline", "run_hash", "run_version", "tokens"],
)
async def test_capacity_factory_requires_isolation_and_exact_identity(monkeypatch, invalid):
    from nexus.agents.codex_capacity import capacity_config, capacity_config_hash

    config = capacity_config()
    model = SimpleNamespace(
        **{
            k: config[k]
            for k in (
                "version",
                "model_id",
                "max_input_tokens",
                "max_output_tokens",
                "max_tool_calls",
            )
        }
    )
    model.config_hash = capacity_config_hash()
    model.input_usd_per_million = model.output_usd_per_million = 0
    for key in ("pricing_available_at", "pricing_expires_at"):
        setattr(model, key, datetime.fromisoformat(config[key]))
    run = SimpleNamespace(
        model_config_id=uuid4(), pricing_version=model.version, model_hash=model.config_hash
    )
    session = SimpleNamespace(
        get=AsyncMock(return_value=model), scalar=AsyncMock(return_value="nexus_test_capacity")
    )
    _, _, prompts = _authority()
    monkeypatch.setattr(worker, "load_frozen_evidence", AsyncMock(return_value=(object(),)))
    monkeypatch.setattr(worker, "load_frozen_prompts", AsyncMock(return_value=prompts))
    monkeypatch.setattr(
        worker,
        "revalidate_frozen_price_authority",
        AsyncMock(return_value=FrozenPriceAuthorityStatus.MISSING),
    )
    if invalid == "hash":
        model.config_hash = "wrong"
    if invalid == "run_hash":
        run.model_hash = "wrong"
    if invalid == "run_version":
        run.pricing_version = "codex-sdk-chatgpt-v8-stage-capacity"
    if invalid == "tokens":
        model.max_input_tokens = 96001
    if invalid == "v8":
        model.version = "codex-sdk-chatgpt-v8-stage-capacity"
    if invalid == "main":
        session.scalar.return_value = "nexus"
    kwargs = dict(responses_client=None, tools=_tools(), deadline=1000, full_input_capacity=True)
    if invalid == "deadline":
        kwargs["deadline"] = None
    settings = EngineSettings(agent_runtime_mode="fixture" if invalid == "runtime" else "codex_sdk")
    if invalid == "main":
        with pytest.raises(ValueError, match="CODEX_CANDIDATE_ISOLATION_REQUIRED"):
            await worker._committee(session, settings, run, **kwargs)
        return
    result = await worker._committee(session, settings, run, **kwargs)
    if invalid is not None:
        assert isinstance(result, worker._UnavailableResponsesCommittee)
        return
    assert isinstance(result, StructuredStagedCodexCommittee)
    assert result._max_wire_bytes == 100479
    assert result._stage_budget_version == model.version
    assert result._capacity_pool_manifest is None
    normal = await worker._committee(session, settings, run, responses_client=None, tools=_tools())
    assert isinstance(normal, worker._UnavailableResponsesCommittee)
    # The factory itself does not grant model execution. Bind the full fixture
    # pool, then exercise the normal SDK schema and all eight role outputs.
    import asyncio
    import json

    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
    from nexus.agents.committee import ROLE_ORDER
    from unit.agents.test_codex_sdk import _assessment_payload, _sdk, _staged_batch

    _, thread, response, _ = _sdk(monkeypatch)
    request, evidence, _ = _authority()
    result._deadline = asyncio.get_running_loop().time() + 180
    monkeypatch.setattr(
        result,
        "_inputs",
        AsyncMock(
            return_value={
                "security_snapshot": {"items": []},
                "financial_facts": {"items": []},
            }
        ),
    )
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_REQUIRED$"):
        await result.analyze("orchestrator", request, evidence, result._prompts["orchestrator"])
    thread.run.assert_not_awaited()
    await preflight_capacity_pool(
        [CapacityPoolMember(result, request, evidence)],
        expected_instrument_ids=(request.instrument_id,),
    )

    async def invoke(wire, **options):
        payload = json.loads(wire)
        response.final_response = json.dumps(
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        return response

    thread.run.side_effect = invoke
    for role in ROLE_ORDER:
        assert (await result.analyze(role, request, evidence, result._prompts[role])).role == role
    assert result.usage.provider_calls == 3 and thread.run.await_count == 3
