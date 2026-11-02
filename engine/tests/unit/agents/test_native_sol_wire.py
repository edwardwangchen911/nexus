"""Native business inputs use the production compiler without comparison aliases."""

import copy
import json
from dataclasses import replace
from time import monotonic

import pytest
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.agents.committee import PromptAsset
from nexus.agents.models import ResearchRequest
from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_wire import (
    PlatformProfile,
    compile_native_platform_stage,
    freeze_native_platform_allocation,
    native_platform_input_hash,
    platform_wire_profile,
)
from nexus.agents.tools import ReadOnlyTools
from nexus.domain.evidence import EvidenceRef
from unit.agents.test_comparison_inputs import source_member
from unit.agents.test_platform_wire import prior
from unit.agents.test_sol_pricing import sol_price_record as record_fixture


def native_case():
    source = source_member()
    options = dict(
        prompts={role: PromptAsset.model_validate(p) for role, p in source["prompts"].items()},
        tools=ReadOnlyTools(executors={}),
        deadline=monotonic() + 300,
        aggregate_input_tokens=96000,
        max_wire_bytes=100479,
        inline_citations=True,
        paper_stage_scope=True,
        stage_time_budget=True,
        stage_budget_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
    )
    compiler = StructuredStagedCodexCommittee(**options)
    inputs = dict(
        request=ResearchRequest.model_validate(source["request"]),
        evidence_refs=tuple(EvidenceRef.model_validate(e) for e in source["evidence"]),
        inputs=source["inputs"],
    )
    price = record_fixture()
    allocation = freeze_native_platform_allocation(
        compiler,
        **inputs,
        profile=platform_wire_profile(sol_price_record(price).version, price_record=price),
        price_record=price,
    )
    return source, options, compiler, inputs, price, allocation


def test_native_three_stage_compilation_never_uses_comparison_authority(monkeypatch):
    import nexus.agents.platform_wire as module

    def forbidden(*args, **kwargs):
        pytest.fail("native business must not compile a comparison input")

    monkeypatch.setattr(module, "_compiler", forbidden)
    monkeypatch.setattr(module, "compile_comparison_stage", forbidden)
    source, options, _, inputs, price, allocation = native_case()
    compiler = StructuredStagedCodexCommittee(
        **(
            options
            | dict(
                platform_allocation=allocation,
                platform_price_record=price,
                aggregate_input_tokens=sum(allocation.stage_input_upper_bounds),
                max_wire_bytes=max(allocation.stage_input_upper_bounds) - 8192,
            )
        )
    )
    frozen = copy.deepcopy(source)
    usage = []
    for index, stage in enumerate(("research", "critique", "synthesis")):
        payload, schema = compiler.build_stage_request(
            stage=stage,
            request=inputs["request"],
            evidence_refs=inputs["evidence_refs"],
            frozen_inputs=inputs["inputs"],
            previous=[],
        )
        wire = compile_native_platform_stage(
            input_hash=native_platform_input_hash(compiler, **inputs),
            allocation=allocation,
            expected_allocation_hash=allocation.allocation_hash,
            stage=stage,
            compiled_request=(payload, schema),
            prior_usage=usage,
            price_record=price,
        )
        body = json.loads(wire.wire)
        assert body["model"] == "gpt-6.1-sol"
        assert body["reasoning"] == {"effort": "medium"}
        assert body["max_output_tokens"] == 8192 - 900 * index
        assert str(inputs["request"].data_snapshot_id) in wire.wire.decode()
        assert wire.input_units <= allocation.stage_input_upper_bounds[index]
        observation = prior(allocation, stage=index + 1)
        observation.response["model"] = "gpt-6.1-sol"
        usage.append(observation)
    assert source == frozen


def test_native_input_binding_detects_changed_tool_material():
    _, _, compiler, inputs, price, allocation = native_case()
    changed = copy.deepcopy(inputs)
    changed["inputs"]["native_material_change"] = "new evidence must have a new identity"
    digest = native_platform_input_hash(compiler, **changed)
    assert digest != allocation.input_hash
    payload = compiler.build_stage_request(
        stage="research",
        request=inputs["request"],
        evidence_refs=inputs["evidence_refs"],
        frozen_inputs=inputs["inputs"],
        previous=[],
    )
    with pytest.raises(ValueError, match="PLATFORM_ALLOCATION_CHANGED"):
        compile_native_platform_stage(
            input_hash=digest,
            allocation=allocation,
            expected_allocation_hash=allocation.allocation_hash,
            stage="research",
            compiled_request=payload,
            prior_usage=(),
            price_record=price,
        )


def test_native_input_binding_includes_compiler_semantics_and_exact_request():
    _, _, compiler, inputs, _, allocation = native_case()
    changed = copy.copy(compiler)
    changed._paper_stage_scope = False
    assert native_platform_input_hash(changed, **inputs) != allocation.input_hash
    changed = inputs | {
        "request": inputs["request"].model_copy(
            update={
                "data_snapshot_id": inputs["request"].instrument_id,
            }
        )
    }
    assert native_platform_input_hash(compiler, **changed) != allocation.input_hash


def test_native_constructor_requires_exact_price_and_allocation():
    _, options, _, _, price, allocation = native_case()
    bound = options | dict(
        platform_allocation=allocation,
        aggregate_input_tokens=sum(allocation.stage_input_upper_bounds),
        max_wire_bytes=max(allocation.stage_input_upper_bounds) - 8192,
    )
    with pytest.raises(ValueError, match="PLATFORM_PROFILE_INVALID"):
        StructuredStagedCodexCommittee(**bound)
    with pytest.raises(ValueError, match="PLATFORM_PROFILE_INVALID"):
        StructuredStagedCodexCommittee(
            **bound, platform_price_record=price | {"source_evidence_sha256": "b" * 64}
        )
    with pytest.raises(ValueError, match="PLATFORM_ALLOCATION_INVALID"):
        StructuredStagedCodexCommittee(**options, platform_price_record=price)
    with pytest.raises(ValueError, match="PLATFORM_ALLOCATION_INVALID"):
        StructuredStagedCodexCommittee(
            **(bound | {"platform_allocation": replace(allocation, output_limit=8193)}),
            platform_price_record=price,
        )


def test_native_business_does_not_admit_the_historical_terra_price_profile():
    _, _, compiler, inputs, _, _ = native_case()
    with pytest.raises(ValueError, match="PLATFORM_PRICE_RECORD_INVALID"):
        freeze_native_platform_allocation(
            compiler, **inputs, profile=PlatformProfile(), price_record=None
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "family,rejected",
    [
        ("daily", False),
        ("daily", True),
        ("price", False),
        ("price", True),
        ("disclosure", False),
        ("disclosure", True),
    ],
)
async def test_native_full_three_stage_payload_keeps_proposals_and_audit_veto(
    monkeypatch, family, rejected
):
    """Exercise real normal producers/schema/synthesis; only provider output is synthetic.

    This deliberately does not claim native SQL admission/HTTP dispatch coverage.
    The harness supplies the separately frozen capacity and observed prior usage.
    """
    from nexus.agents import codex_staged
    from nexus.agents.codex_sdk import codex_request_hash
    from unit.agents import test_daily_pending_producer, test_monitoring_producer

    bodies = []
    price = record_fixture()
    base = StructuredStagedCodexCommittee

    class NativeCommittee(base):
        async def analyze(self, role, request, evidence_refs, prompt):
            if self._platform_allocation is None:
                inputs = await self._inputs(request)
                options = dict(
                    prompts=self._prompts,
                    tools=self._tools,
                    deadline=monotonic() + 180,
                    aggregate_input_tokens=96000,
                    max_wire_bytes=100479,
                    inline_citations=True,
                    paper_stage_scope=True,
                    stage_time_budget=True,
                    stage_budget_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
                )
                preview = base(**options)
                allocation = freeze_native_platform_allocation(
                    preview,
                    request=request,
                    evidence_refs=evidence_refs,
                    inputs=inputs,
                    profile=platform_wire_profile(
                        sol_price_record(price).version, price_record=price
                    ),
                    price_record=price,
                )
                base.__init__(
                    self,
                    **(
                        options
                        | dict(
                            platform_allocation=allocation,
                            platform_price_record=price,
                            aggregate_input_tokens=sum(allocation.stage_input_upper_bounds),
                            max_wire_bytes=max(allocation.stage_input_upper_bounds) - 8192,
                        )
                    ),
                )
                initial = self.build_stage_request(
                    stage="research",
                    request=request,
                    evidence_refs=evidence_refs,
                    frozen_inputs=inputs,
                    previous=[],
                )
                self._capacity_pool_manifest = allocation.allocation_hash
                self._capacity_pool_request = request
                self._capacity_pool_evidence = evidence_refs
                self._capacity_research_hash = codex_request_hash(*initial)
                observed = []

                async def invoke(payload, *, output_model, **kwargs):
                    wire = compile_native_platform_stage(
                        input_hash=native_platform_input_hash(
                            self, request=request, evidence_refs=evidence_refs, inputs=inputs
                        ),
                        allocation=allocation,
                        expected_allocation_hash=allocation.allocation_hash,
                        stage=payload["stage"],
                        compiled_request=(payload, output_model),
                        prior_usage=observed,
                        price_record=price,
                    )
                    bodies.append(json.loads(wire.wire))
                    value, usage = await codex_staged.invoke_codex(
                        payload, output_model=output_model, **kwargs
                    )
                    row = prior(allocation, output=usage.output_tokens, stage=len(observed) + 1)
                    row.response["model"] = "gpt-6.1-sol"
                    row.response["usage"]["input_tokens"] = usage.input_tokens
                    row.response["usage"]["total_tokens"] = usage.input_tokens + usage.output_tokens
                    observed.append(row)
                    return value, usage

                self.stage_invoker = invoke
            return await super().analyze(role, request, evidence_refs, prompt)

    monkeypatch.setattr(codex_staged, "StructuredStagedCodexCommittee", NativeCommittee)
    if family == "daily":
        intent, outputs, _, _ = await test_daily_pending_producer.produce(
            monkeypatch,
            inline=True,
            v2=True,
            mutation="audit_vote" if rejected else None,
        )
        assert (intent.daily_pending_proposal is not None) is (not rejected)
    else:
        intent, outputs, _ = await test_monitoring_producer.produce(
            monkeypatch,
            inline=True,
            disclosure=family == "disclosure",
            rejected="evidence_audit" if rejected else None,
        )
        assert (intent.monitoring_proposal is not None) is (not rejected)
    assert len(outputs) == 8 and len(bodies) == 3
    assert [b["max_output_tokens"] for b in bodies] == [8192, 8092, 7992]
    assert all(b["model"] == "gpt-6.1-sol" and b["tools"] == [] for b in bodies)
    assert intent.stance == "watch" and intent.exposure_action == "none"
