"""Real envelope compiler with synthetic input, no dispatch/comparison authority."""

import json
from dataclasses import replace
from uuid import uuid4

import pytest
from nexus.agents.comparison_inputs import prepare_comparison_input
from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_profile import platform_profile_hash
from nexus.agents.platform_usage import (
    CURRENT_PLATFORM_PRICE_VERSION,
    MEDIUM_PLATFORM_PRICE_VERSION,
    PLATFORM_PRICE_VERSION,
)
from nexus.agents.platform_wire import (
    compile_platform_stage,
    freeze_platform_allocation,
    platform_wire_profile,
)
from unit.agents.test_comparison_inputs import source_member
from unit.agents.test_platform_wire import prior
from unit.agents.test_sol_pricing import sol_price_record as record_fixture


@pytest.fixture
def prepared():
    return prepare_comparison_input(source_member(), experiment_id=uuid4())


def allocation(prepared, record):
    version = sol_price_record(record).version
    profile = platform_wire_profile(version, price_record=record)
    return freeze_platform_allocation(prepared, profile=profile, price_record=record)


def compile_stage(prepared, frozen, record, stage="research", previous_usage=()):
    return compile_platform_stage(
        prepared,
        allocation=frozen,
        expected_allocation_hash=frozen.allocation_hash,
        stage=stage,
        previous=[],
        prior_usage=previous_usage,
        price_record=record,
    )


def test_sol_three_stage_wire_binds_price_and_consumes_full_prior_output(prepared):
    record = record_fixture()
    frozen = allocation(prepared, record)
    spent = []
    for index, stage in enumerate(("research", "critique", "synthesis")):
        wire = compile_stage(prepared, frozen, record, stage, spent)
        body = json.loads(wire.wire)
        assert body["model"] == "gpt-6.1-sol"
        assert body["reasoning"] == {"effort": "medium"}
        assert body["service_tier"] == "default"
        assert body["prompt_cache_options"] == {"mode": "implicit", "ttl": "30m"}
        assert body["max_output_tokens"] == (8192, 7292, 6392)[index]
        assert body["text"]["format"]["strict"] is True
        assert body["tools"] == [] and body["stream"] is True and body["store"] is False
        assert not {"temperature", "top_p", "top_logprobs"}.intersection(body)
        assert len(wire.wire) + 8192 == wire.input_units <= frozen.stage_input_upper_bounds[index]
        row = prior(prepared, stage=index + 1)
        row.response["model"] = "gpt-6.1-sol"
        spent.append(row)


def test_sol_price_renewal_cannot_reinterpret_frozen_allocation(prepared):
    record = record_fixture()
    frozen = allocation(prepared, record)
    changed = record | {"source_evidence_sha256": "b" * 64}
    with pytest.raises(ValueError, match="PLATFORM_PROFILE_INVALID"):
        compile_stage(prepared, frozen, changed)
    with pytest.raises(ValueError, match="PLATFORM_PROFILE_INVALID"):
        compile_stage(prepared, frozen, None)
    assert allocation(prepared, changed).allocation_hash != frozen.allocation_hash
    assert compile_stage(prepared, frozen, record).allocation_hash == frozen.allocation_hash


def test_sol_cannot_consume_terra_response_or_silently_fall_back(prepared):
    record = record_fixture()
    frozen = allocation(prepared, record)
    with pytest.raises(ValueError, match="PLATFORM_PRIOR_USAGE_INVALID"):
        compile_stage(prepared, frozen, record, "critique", [prior(prepared)])
    with pytest.raises(ValueError, match="PLATFORM_PROFILE_INVALID"):
        freeze_platform_allocation(
            prepared, profile=replace(frozen.profile, effort="low"), price_record=record
        )


def test_original_profiles_keep_their_exact_frozen_hashes():
    assert (
        platform_profile_hash(PLATFORM_PRICE_VERSION)
        == "64ced6b0149a9b80139e62a007390562485107f903d230041697149f84cf487a"
    )
    assert (
        platform_profile_hash(CURRENT_PLATFORM_PRICE_VERSION)
        == "e5a3e02ebd9b5276352224056c95880cef99e6230ed7219eef617551f6642f5d"
    )
    assert (
        platform_profile_hash(MEDIUM_PLATFORM_PRICE_VERSION)
        == "bccfe955523567268db8daa6a9d56a187db776066927e3b5075763cbeaa12099"
    )
