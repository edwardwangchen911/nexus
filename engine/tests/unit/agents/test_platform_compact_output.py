"""Future-only output guidance must preserve evidence, limits and frozen identity."""

import copy
import hashlib
import json
from dataclasses import replace

import httpx
import pytest
from nexus.agents.platform_wire import (
    _hash,
    _wire,
    compile_native_platform_stage,
    freeze_native_platform_allocation,
    freeze_platform_allocation,
)
from nexus.agents.research_stages.http_lifecycle import HttpStageFailure
from unit.agents.test_http_stage_lifecycle import Answer, Body, Protocol, terminal
from unit.agents.test_native_sol_wire import native_case
from unit.agents.test_platform_http_stage_lifecycle import call
from unit.agents.test_platform_http_stage_lifecycle import credential as make_credential
from unit.agents.test_platform_http_stage_lifecycle import frozen as make_wire
from unit.agents.test_platform_wire import prior

COMPACT = "platform-compact-json-http-utf8-plus8192-v3"
GUIDANCE = (
    "Emit one compact JSON object. Do not pretty-print or indent. "
    "Do not add whitespace between JSON tokens or pad the output. "
    "Preserve whitespace inside natural-language string values. "
    "Stop after the complete JSON object."
)


@pytest.mark.parametrize("alias", [False, True])
def test_compact_guidance_preserves_lossless_independent_input_view(alias):
    from nexus.agents.independent_input_view import (
        project_independent_payload,
        restore_independent_input,
    )

    shared = "Preserve all evidence and all original audit requirements. " * 12
    payload = {
        "instructions": shared + ("Only this field receives output guidance." if not alias else ""),
        "prompts": [{"content": shared}],
        "frozen_inputs": {"facts": {"value": "1.23", "unit": "USD/shares"}},
        "previous_claims": [{"text": "literal ~ and whitespace\n"}],
    }
    projected = project_independent_payload(payload)
    assert isinstance(projected["instructions"], dict) or str(projected["instructions"]).startswith(
        "$v:"
    )
    before = copy.deepcopy(projected)
    _, _, _, _, old, _ = allocations()
    body = json.loads(
        _wire(
            projected,
            Answer,
            output_remaining=8192,
            profile=old.profile,
            forbidden=(),
            units_version=COMPACT,
        )
    )
    sent = json.loads(body["input"][1]["content"])
    restored = restore_independent_input(sent)
    assert restored.pop("instructions") == payload["instructions"] + " " + GUIDANCE
    assert restored == {k: v for k, v in payload.items() if k != "instructions"}
    assert {k: v for k, v in sent.items() if k != "instructions"} == {
        k: v for k, v in before.items() if k != "instructions"
    }
    assert projected == before


def allocations():
    source, _, compiler, inputs, price, old = native_case()
    new = freeze_native_platform_allocation(
        compiler,
        **inputs,
        profile=old.profile,
        price_record=price,
        units_version=COMPACT,
    )
    return source, compiler, inputs, price, old, new


def compile_wire(compiler, inputs, price, allocation, stage, usage):
    pair = compiler.build_stage_request(
        stage=stage,
        request=inputs["request"],
        evidence_refs=inputs["evidence_refs"],
        frozen_inputs=inputs["inputs"],
        previous=[],
    )
    before = copy.deepcopy(pair[0])
    wire = compile_native_platform_stage(
        input_hash=allocation.input_hash,
        allocation=allocation,
        expected_allocation_hash=allocation.allocation_hash,
        stage=stage,
        compiled_request=pair,
        prior_usage=usage,
        price_record=price,
    )
    assert pair[0] == before, "compilation must not mutate frozen material"
    return wire, json.loads(wire.wire)


def test_all_three_stages_add_only_guidance_and_account_for_its_full_wire_size():
    source, compiler, inputs, price, old, new = allocations()
    before = copy.deepcopy(source)
    assert old.units_version == "platform-structured-http-utf8-plus8192-v2"
    assert new.input_hash == old.input_hash
    assert new.profile == old.profile
    assert new.stage_schema_envelope_hashes == old.stage_schema_envelope_hashes
    assert new.allocation_hash != old.allocation_hash
    usage = []
    for index, stage in enumerate(("research", "critique", "synthesis")):
        original, original_body = compile_wire(compiler, inputs, price, old, stage, usage)
        candidate, body = compile_wire(compiler, inputs, price, new, stage, usage)
        payload = json.loads(body["input"][1]["content"])
        original_payload = json.loads(original_body["input"][1]["content"])
        assert payload.pop("instructions") == original_payload.pop("instructions") + " " + GUIDANCE
        assert payload == original_payload
        assert {k: v for k, v in body.items() if k != "input"} == {
            k: v for k, v in original_body.items() if k != "input"
        }
        assert body["input"][0] == original_body["input"][0]
        assert body["max_output_tokens"] == 8192 - index * 900
        added = len(candidate.wire) - len(original.wire)
        assert added == len((" " + GUIDANCE).encode())
        assert new.stage_input_upper_bounds[index] - old.stage_input_upper_bounds[index] == added
        assert candidate.input_units == len(candidate.wire) + 8192
        assert candidate.input_units <= new.stage_input_upper_bounds[index] <= 272000
        with pytest.raises(ValueError, match="PLATFORM_WIRE_CHANGED"):
            candidate.verify_wire(original.wire)
        observation = prior(new, stage=index + 1)
        observation.response["model"] = "gpt-6.1-sol"
        usage.append(observation)
    assert source == before


def test_changed_version_cannot_use_the_original_allocation_authority():
    _, compiler, inputs, price, old, new = allocations()
    pair = compiler.build_stage_request(
        stage="research",
        request=inputs["request"],
        evidence_refs=inputs["evidence_refs"],
        frozen_inputs=inputs["inputs"],
        previous=[],
    )
    with pytest.raises(ValueError, match="PLATFORM_ALLOCATION_CHANGED"):
        compile_native_platform_stage(
            input_hash=old.input_hash,
            allocation=replace(old, units_version=COMPACT),
            expected_allocation_hash=old.allocation_hash,
            stage="research",
            compiled_request=pair,
            prior_usage=[],
            price_record=price,
        )
    assert new.units_version == COMPACT


def test_compact_guidance_does_not_borrow_capacity_from_an_old_exact_bound():
    _, compiler, inputs, price, old, new = allocations()
    old_wire, _ = compile_wire(compiler, inputs, price, old, "research", [])
    short = replace(
        new, stage_input_upper_bounds=(old_wire.input_units, *new.stage_input_upper_bounds[1:])
    )
    with pytest.raises(ValueError, match="PLATFORM_INPUT_TOO_LARGE"):
        compile_wire(compiler, inputs, price, short, "research", [])


def test_compact_native_version_does_not_expand_legacy_qualification_authority():
    from uuid import uuid4

    from nexus.agents.comparison_inputs import prepare_comparison_input
    from unit.agents.test_comparison_inputs import source_member

    prepared = prepare_comparison_input(source_member(), experiment_id=uuid4())
    with pytest.raises(ValueError, match="PLATFORM_ALLOCATION_CHANGED"):
        freeze_platform_allocation(prepared, units_version=COMPACT)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,known", [("completed", True), ("incomplete", True), ("incomplete", False)]
)
async def test_compact_wire_keeps_normal_whitespace_valid_and_incomplete_usage_protected(
    status, known
):
    _, _, _, _, old, _ = allocations()
    frozen, credential = make_wire.__wrapped__(), make_credential.__wrapped__()
    wire = _wire(
        {"stage": "research", "instructions": "Return the requested JSON."},
        Answer,
        output_remaining=8192,
        profile=old.profile,
        forbidden=(),
        units_version=COMPACT,
    )
    digest = hashlib.sha256(wire).hexdigest()
    frozen = replace(
        frozen,
        wire=wire,
        body_sha256=digest,
        binding_hash=_hash(
            dict(allocation_hash=frozen.allocation_hash, stage="research", body_sha256=digest)
        ),
        input_units=len(wire) + 8192,
        input_upper_bound=len(wire) + 8192,
    )
    event = terminal(status=status, usage=known)
    event["response"]["model"] = "gpt-6.1-sol"
    output = json.dumps({"answer": "normal internal spaces"}, indent=2)
    if status == "incomplete":
        event["response"]["incomplete_details"] = {"reason": "max_output_tokens"}
        output = '{"answer":' + " " * 112783
    event["response"]["output"][0]["content"][0]["text"] = output
    body = Body(("data: " + json.dumps(event) + "\n\n").encode())
    sent = []

    def handle(request):
        sent.append(request.content)
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=body)

    async def check():
        return credential

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    protocol = Protocol()
    if status == "completed":
        raw, usage = await call(protocol, client, frozen, credential, check)
        assert raw == output and usage.output_tokens == 9
    else:
        with pytest.raises(HttpStageFailure) as raised:
            await call(protocol, client, frozen, credential, check)
        if known:
            assert raised.value.usage.output_tokens == 9
        else:
            assert raised.value.usage is None
    assert sent == [wire] and client.is_closed and body.closed == 1
    observations = [row for kind, row in protocol.events if kind == "observe"]
    if known:
        assert len(observations) == 1
        assert observations[0]["valid"] is (status == "completed")
        assert json.loads(observations[0]["payload"]) == event["response"]
    else:
        assert observations == []
