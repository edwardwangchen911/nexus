"""Preclaim recovery consumes the saved output version, never today's default.

Storage reads are synthetic here; real route admission and SQL constraints are
separate integration gates. Compilation, pricing and preclaim checks are real.
"""

import copy
import json
from dataclasses import asdict
from datetime import UTC, datetime
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.agents import platform_native
from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_profile import sol_business_model_fields
from nexus.agents.platform_wire import (
    compile_native_platform_stage,
    freeze_native_platform_allocation,
)
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.settings import AgentRuntimeMode
from pydantic import SecretStr
from unit.agents.test_native_sol_wire import native_case
from unit.agents.test_platform_wire import prior

VERSIONS = (
    "platform-structured-http-utf8-plus8192-v2",
    "platform-compact-json-http-utf8-plus8192-v3",
)


def saved_pool(monkeypatch, version):
    _, _, compiler, inputs, price, original = native_case()
    allocation = freeze_native_platform_allocation(
        compiler, **inputs, profile=original.profile, price_record=price, units_version=version
    )
    settings = SimpleNamespace(
        agent_runtime_mode=AgentRuntimeMode.RESPONSES,
        openai_api_key=SecretStr("synthetic-preclaim-key"),
    )
    credential = platform_native.credential_for_preparation(settings)
    model = AgentModelConfigRecord(id=uuid4(), **sol_business_model_fields(price))
    run = AgentResearchRunRecord(
        id=uuid4(),
        model_config_id=model.id,
        model_hash=model.config_hash,
        pricing_version=model.version,
        status="prepared",
    )
    scope, job = uuid4(), uuid4()
    binding = dict(
        version=1,
        transport="platform_responses",
        model_config_id=str(model.id),
        model_hash=model.config_hash,
        price_version=allocation.profile.price_version,
        price_record_hash=sol_price_record(price).content_hash,
        credential_binding_hash=credential.binding_hash,
        allocation=asdict(allocation),
        allocation_hash=allocation.allocation_hash,
    )

    class Storage:
        now = datetime(2026, 11, 3, 15, tzinfo=UTC)

        async def execute(self, query, params):
            sql = str(query)
            if sql.startswith("SELECT scope_id "):
                assert params == {"run": run.id}
                return SimpleNamespace(scalar_one=lambda: scope)
            if sql.startswith("SELECT run_id,job_id "):
                assert params == {"scope": scope}
                return SimpleNamespace(all=lambda: [(run.id, job)])
            if sql.startswith("SELECT platform_binding,"):
                assert params == {"run": run.id}
                row = {"platform_binding": binding, "valid": True}
                return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))
            pytest.fail(f"unexpected storage query: {sql}")

        async def get(self, cls, identity):
            if cls is AgentResearchRunRecord and identity == run.id:
                return run
            if cls is AgentModelConfigRecord and identity == model.id:
                return model
            pytest.fail("unexpected identity read")

        async def scalar(self, query):
            assert str(query) == "SELECT clock_timestamp()"
            return self.now

    storage = Storage()

    async def route(session, job_id):
        assert session is storage and job_id == job
        return {"kind": "increment", "platform_binding": binding}

    async def material(session, *, run, model):
        assert session is storage
        return compiler, inputs["request"], inputs["evidence_refs"], inputs["inputs"]

    monkeypatch.setattr("nexus.agents.research_stages.routes.selected_route", route)
    monkeypatch.setattr(platform_native, "native_prepared_material", material)
    return storage, settings, run, inputs, price, allocation, binding


@pytest.mark.asyncio
@pytest.mark.parametrize("version", VERSIONS)
async def test_recovered_pool_rebuilds_all_three_stages_with_saved_version(monkeypatch, version):
    storage, settings, run, inputs, price, allocation, binding = saved_pool(monkeypatch, version)
    before = copy.deepcopy(binding)
    restored = await platform_native.prepare_native_pool(
        storage, settings, run, deadline=monotonic() + 120
    )
    assert restored._platform_allocation == allocation
    usage = []
    for index, stage in enumerate(("research", "critique", "synthesis")):
        pair = restored.build_stage_request(
            stage=stage,
            request=inputs["request"],
            evidence_refs=inputs["evidence_refs"],
            frozen_inputs=inputs["inputs"],
            previous=[],
        )
        wire = compile_native_platform_stage(
            input_hash=allocation.input_hash,
            allocation=restored._platform_allocation,
            expected_allocation_hash=before["allocation_hash"],
            stage=stage,
            compiled_request=pair,
            prior_usage=usage,
            price_record=price,
        )
        body = json.loads(wire.wire)
        instruction = json.loads(body["input"][1]["content"])["instructions"]
        assert ("Emit one compact JSON object." in instruction) is (version == VERSIONS[1])
        assert body["max_output_tokens"] == 8192 - 900 * index
        assert wire.input_units <= allocation.stage_input_upper_bounds[index] <= 272000
        observed = prior(allocation, stage=index + 1)
        observed.response["model"] = "gpt-6.1-sol"
        usage.append(observed)
    assert binding == before


@pytest.mark.asyncio
@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize(
    "change,code",
    [
        ("input", "PLATFORM_NATIVE_INPUT_CHANGED"),
        ("credential", "PLATFORM_HTTP_CREDENTIAL_CHANGED"),
        ("expired_price", "PLATFORM_PRICE_WINDOW_INVALID"),
        ("version_without_hash", "PLATFORM_NATIVE_ROUTE_INVALID"),
    ],
)
async def test_recovered_version_keeps_existing_authority_checks(
    monkeypatch, version, change, code
):
    storage, settings, run, inputs, _, _, binding = saved_pool(monkeypatch, version)
    if change == "input":
        inputs["inputs"]["financial_facts"]["items"][0]["value"] = "9.87"
    elif change == "credential":
        settings.openai_api_key = SecretStr("synthetic-different-key")
    elif change == "expired_price":
        storage.now = datetime(2026, 11, 10, tzinfo=UTC)
    else:
        binding["allocation"]["units_version"] = next(v for v in VERSIONS if v != version)
    with pytest.raises(ValueError, match=f"^{code}$"):
        await platform_native.prepare_native_pool(
            storage, settings, run, deadline=monotonic() + 120
        )
