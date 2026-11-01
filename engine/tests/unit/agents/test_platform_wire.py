"""No dispatch: full Platform envelope and frozen per-chain allocation."""

import copy
import hashlib
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from nexus.agents.codex_sdk import (
    BASE_INSTRUCTIONS,
    DEVELOPER_INSTRUCTIONS,
    codex_output_schema,
    codex_request_wire,
)
from nexus.agents.comparison_compiler import compile_comparison_stage
from nexus.agents.comparison_inputs import prepare_comparison_input
from nexus.agents.platform_schema import LEGACY_PLATFORM_UNITS_VERSION
from nexus.agents.platform_wire import (
    PlatformProfile,
    PriorPlatformUsage,
    compile_platform_stage,
    freeze_platform_allocation,
)
from nexus.agents.research_stages.http_lifecycle import http_stage_wire
from unit.agents.test_comparison_inputs import source_member


@pytest.fixture
def prepared():
    return prepare_comparison_input(source_member(), experiment_id=uuid4())


def prior(prepared, output=900, *, stage=1):
    return PriorPlatformUsage(
        stage=stage,
        input_hash=prepared.input_hash,
        verified=True,
        response={
            "status": "completed",
            "model": "gpt-5.6-terra",
            "service_tier": "default",
            "usage": {
                "input_tokens": 20,
                "output_tokens": output,
                "total_tokens": 20 + output,
                "input_tokens_details": {
                    "cached_tokens": 0,
                    "cache_write_tokens": 0,
                    "cache_write_tokens_details": {"ttl_30m_tokens": 0, "ttl_12h_tokens": 0},
                },
                "output_tokens_details": {"reasoning_tokens": output - 1},
            },
        },
    )


def compile_stage(prepared, *, stage="research", prior_usage=(), previous=None, allocation=None):
    allocation = allocation or freeze_platform_allocation(prepared)
    return compile_platform_stage(
        prepared,
        allocation=allocation,
        expected_allocation_hash=allocation.allocation_hash,
        stage=stage,
        previous=previous or [],
        prior_usage=prior_usage,
    )


def test_legacy_normal_compiler_real_schema_and_sdk_http_bytes_are_unchanged(prepared):
    payload, schema = compile_comparison_stage(
        prepared, expected_input_hash=prepared.input_hash, stage="research", previous=[]
    )
    sdk_before = codex_request_wire(payload)
    http_before = http_stage_wire(payload, output_model=schema, forbidden=prepared.forbidden)
    original = copy.deepcopy(prepared)
    allocation = freeze_platform_allocation(prepared, units_version=LEGACY_PLATFORM_UNITS_VERSION)
    result = compile_stage(prepared, allocation=allocation)
    body = json.loads(result.wire)
    assert body["input"] == [
        {"role": "developer", "content": DEVELOPER_INSTRUCTIONS},
        {"role": "user", "content": sdk_before},
    ]
    assert body["instructions"] == BASE_INSTRUCTIONS
    assert body["text"]["format"]["schema"] == codex_output_schema(schema)
    assert body["model"] == "gpt-5.6-terra" and body["reasoning"] == {"effort": "low"}
    assert body["service_tier"] == "default"
    assert body["prompt_cache_options"] == {"mode": "implicit", "ttl": "30m"}
    assert body["max_output_tokens"] == 8192
    assert body["stream"] is True and body["store"] is False and body["tools"] == []
    assert result.input_units == len(result.wire) + 8192
    assert result.input_units <= allocation.stage_input_upper_bounds[0]
    assert result.body_sha256 == hashlib.sha256(result.wire).hexdigest()
    assert result.credential_kind == "platform_project_key"
    assert result.endpoint == "https://api.openai.com/v1/responses"
    result.verify_wire(result.wire)
    assert prepared == original
    assert codex_request_wire(payload) == sdk_before
    assert (
        http_stage_wire(payload, output_model=schema, forbidden=prepared.forbidden) == http_before
    )
    assert (
        freeze_platform_allocation(prepared, units_version=LEGACY_PLATFORM_UNITS_VERSION)
        == allocation
    )


@pytest.mark.parametrize("text", ['中文𠀀"\\\n' * 50, "\x00" * 400, "𠀀" * 400, '"' * 400])
def test_final_utf8_size_includes_schema_and_second_escaping(prepared, text):
    previous = [
        dict(
            claim_id="market_state:0", role="market_state", vote="abstain", text=text, citations=[0]
        )
    ]
    result = compile_stage(
        prepared, stage="critique", prior_usage=(prior(prepared),), previous=previous
    )
    payload, schema = compile_comparison_stage(
        prepared, expected_input_hash=prepared.input_hash, stage="critique", previous=previous
    )
    assert json.loads(result.wire)["input"][1]["content"] == codex_request_wire(payload)
    assert json.loads(result.wire)["text"]["format"]["schema"] == codex_output_schema(schema)
    assert len(result.wire) > len(codex_request_wire(payload).encode())
    assert result.input_units == len(result.wire) + 8192


def test_cumulative_output_uses_full_reasoning_and_does_not_reset_each_stage(prepared):
    result = compile_stage(
        prepared,
        stage="synthesis",
        prior_usage=(
            prior(prepared, 4000),
            prior(prepared, 3000, stage=2),
        ),
    )
    assert json.loads(result.wire)["max_output_tokens"] == 1192


@pytest.mark.parametrize("spent,allowed", [(8177, False), (8176, True), (8193, False)])
def test_remaining_output_boundary_is_not_rounded_up(prepared, spent, allowed):
    if allowed:
        result = compile_stage(prepared, stage="critique", prior_usage=(prior(prepared, spent),))
        assert json.loads(result.wire)["max_output_tokens"] == 16
    else:
        with pytest.raises(ValueError, match="^PLATFORM_OUTPUT_EXHAUSTED$"):
            compile_stage(prepared, stage="critique", prior_usage=(prior(prepared, spent),))


@pytest.mark.parametrize(
    "fault",
    [
        "missing",
        "unverified",
        "wrong_stage",
        "wrong_input",
        "missing_usage",
        "missing_total",
        "contradictory",
        "bool_output",
        "reasoning_conflict",
        "incomplete",
        "unknown_tier",
        "cache_12h",
    ],
)
def test_untrusted_predecessor_cannot_grant_output_budget(prepared, fault):
    receipt = prior(prepared)
    response = copy.deepcopy(receipt.response)
    if fault == "missing":
        receipts = ()
    else:
        if fault == "unverified":
            receipt = replace(receipt, verified=False)
        elif fault == "wrong_stage":
            receipt = replace(receipt, stage=2)
        elif fault == "wrong_input":
            receipt = replace(receipt, input_hash="0" * 64)
        elif fault == "missing_usage":
            del response["usage"]
        elif fault == "missing_total":
            del response["usage"]["total_tokens"]
        elif fault == "contradictory":
            response["usage"]["total_tokens"] -= 1
        elif fault == "bool_output":
            response["usage"]["output_tokens"] = True
        elif fault == "reasoning_conflict":
            response["usage"]["output_tokens_details"]["reasoning_tokens"] = 9999
        elif fault == "incomplete":
            response["status"] = "incomplete"
        elif fault == "unknown_tier":
            del response["service_tier"]
        elif fault == "cache_12h":
            response["usage"]["input_tokens_details"]["cache_write_tokens"] = 1
            response["usage"]["input_tokens_details"]["cache_write_tokens_details"][
                "ttl_12h_tokens"
            ] = 1
        receipts = (replace(receipt, response=response),)
    with pytest.raises(ValueError, match="^PLATFORM_PRIOR_USAGE_INVALID$"):
        compile_stage(prepared, stage="critique", prior_usage=receipts)


@pytest.mark.parametrize(
    "changes",
    [
        {"model": "gpt-5.6-sol"},
        {"effort": "high"},
        {"cache_ttl": "12h"},
        {"cache_mode": "explicit"},
        {"service_tier": "priority"},
        {"stream": False},
        {"stream": 1},
        {"store": True},
        {"credential_kind": "chatgpt_grant"},
        {"endpoint": "https://example.invalid/responses"},
        {"price_version": "old-zero-price"},
    ],
)
def test_profile_drift_rejected_before_freezing(prepared, changes):
    with pytest.raises(ValueError, match="^PLATFORM_PROFILE_INVALID$"):
        freeze_platform_allocation(prepared, profile=replace(PlatformProfile(), **changes))


def test_allocation_hash_binds_exact_bounds_and_final_bytes_cannot_change(prepared):
    allocation = freeze_platform_allocation(prepared)
    changed = replace(
        allocation,
        stage_input_upper_bounds=tuple(n + 1 for n in allocation.stage_input_upper_bounds),
    )
    assert changed.allocation_hash != allocation.allocation_hash
    with pytest.raises(ValueError, match="^PLATFORM_ALLOCATION_CHANGED$"):
        compile_platform_stage(
            prepared,
            allocation=changed,
            expected_allocation_hash=allocation.allocation_hash,
            stage="research",
            previous=[],
            prior_usage=(),
        )
    result = compile_stage(prepared, allocation=allocation)
    changed_body = json.loads(result.wire)
    changed_body["max_output_tokens"] -= 1
    with pytest.raises(ValueError, match="^PLATFORM_WIRE_CHANGED$"):
        result.verify_wire(json.dumps(changed_body).encode())
    limited = replace(
        allocation,
        stage_input_upper_bounds=(result.input_units - 1, *allocation.stage_input_upper_bounds[1:]),
    )
    with pytest.raises(ValueError, match="^PLATFORM_INPUT_TOO_LARGE$"):
        compile_stage(prepared, allocation=limited)


def test_preflight_and_final_body_do_not_reuse_old_common_wire_limit():
    member = source_member()
    member["inputs"]["financial_facts"]["padding"] = "x" * 105000
    prepared = prepare_comparison_input(member, experiment_id=uuid4())
    allocation = freeze_platform_allocation(prepared)
    result = compile_stage(prepared, allocation=allocation)
    payload, schema = compile_comparison_stage(
        prepared, expected_input_hash=prepared.input_hash, stage="research", previous=[]
    )
    assert len(codex_request_wire(payload).encode()) > 100479
    assert result.input_units <= allocation.stage_input_upper_bounds[0] <= 272000
    with pytest.raises(ValueError, match="CODEX_INPUT_TOO_LARGE"):
        http_stage_wire(payload, output_model=schema, forbidden=prepared.forbidden)


def test_all_stages_must_fit_reviewed_price_tier_before_allocation():
    member = source_member()
    member["inputs"]["financial_facts"]["padding"] = "x" * 272000
    prepared = prepare_comparison_input(member, experiment_id=uuid4())
    with pytest.raises(ValueError, match="^PLATFORM_INPUT_TOO_LARGE$"):
        freeze_platform_allocation(prepared)


def test_verified_prior_usage_exceeding_its_frozen_input_bound_stops_next_stage(prepared):
    allocation = freeze_platform_allocation(prepared)
    receipt = prior(prepared)
    response = copy.deepcopy(receipt.response)
    response["usage"]["input_tokens"] = allocation.stage_input_upper_bounds[0] + 1
    response["usage"]["total_tokens"] = response["usage"]["input_tokens"] + 900
    with pytest.raises(ValueError, match="^PLATFORM_PRIOR_USAGE_INVALID$"):
        compile_stage(
            prepared,
            stage="critique",
            allocation=allocation,
            prior_usage=(replace(receipt, response=response),),
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"input_hash": "f" * 64},
        {"stage_order": ("critique", "research", "synthesis")},
        {"output_limit": 8193},
        {"framing_units": 0},
    ],
)
def test_every_frozen_contract_dimension_changes_allocation_identity(prepared, changes):
    allocation = freeze_platform_allocation(prepared)
    changed = replace(allocation, **changes)
    assert changed.allocation_hash != allocation.allocation_hash
    with pytest.raises(ValueError, match="^PLATFORM_ALLOCATION_CHANGED$"):
        compile_platform_stage(
            prepared,
            allocation=changed,
            expected_allocation_hash=allocation.allocation_hash,
            stage="research",
            previous=[],
            prior_usage=(),
        )
