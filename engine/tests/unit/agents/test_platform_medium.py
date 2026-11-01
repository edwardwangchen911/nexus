"""The opt-in diagnostic changes effort, not task, limits or old identities."""

import json
from dataclasses import replace
from uuid import uuid4

import pytest
from nexus.agents.comparison_inputs import prepare_comparison_input
from nexus.agents.platform_profile import platform_profile, platform_profile_hash
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION, PLATFORM_PRICE_VERSION
from nexus.agents.platform_wire import PlatformProfile, freeze_platform_allocation
from unit.agents.test_comparison_inputs import source_member
from unit.agents.test_platform_wire import compile_stage, prior

MEDIUM = "platform-terra-medium-implicit30m-20261101-v1"


@pytest.fixture
def prepared():
    return prepare_comparison_input(source_member(), experiment_id=uuid4())


def test_medium_has_distinct_identity_but_same_prices_expiry_and_limits():
    low = platform_profile(CURRENT_PLATFORM_PRICE_VERSION)
    medium = platform_profile(MEDIUM)
    assert medium["effort"] == "medium" and low["effort"] == "low"
    assert {k: v for k, v in medium.items() if k not in {"version", "price_version", "effort"}} == {
        k: v for k, v in low.items() if k not in {"version", "price_version", "effort"}
    }
    assert platform_profile_hash(MEDIUM) != platform_profile_hash(CURRENT_PLATFORM_PRICE_VERSION)
    assert (
        platform_profile_hash()
        == "64ced6b0149a9b80139e62a007390562485107f903d230041697149f84cf487a"
    )
    assert platform_profile_hash(CURRENT_PLATFORM_PRICE_VERSION) == (
        "e5a3e02ebd9b5276352224056c95880cef99e6230ed7219eef617551f6642f5d"
    )


@pytest.mark.parametrize(
    "stage,used", [("research", ()), ("critique", (3000,)), ("synthesis", (3000, 2000))]
)
def test_each_stage_changes_only_effort_and_counts_full_reasoning_output(prepared, stage, used):
    low = freeze_platform_allocation(
        prepared, profile=PlatformProfile(price_version=CURRENT_PLATFORM_PRICE_VERSION)
    )
    medium = freeze_platform_allocation(
        prepared, profile=PlatformProfile(price_version=MEDIUM, effort="medium")
    )
    previous = tuple(prior(prepared, output=v, stage=i) for i, v in enumerate(used, 1))
    old_wire = compile_stage(prepared, stage=stage, prior_usage=previous, allocation=low)
    new_wire = compile_stage(prepared, stage=stage, prior_usage=previous, allocation=medium)
    old, new = json.loads(old_wire.wire), json.loads(new_wire.wire)
    assert old.pop("reasoning") == {"effort": "low"}
    assert new.pop("reasoning") == {"effort": "medium"}
    assert new == old
    assert new["max_output_tokens"] == 8192 - sum(used)
    assert medium.output_limit == low.output_limit == 8192
    assert all(v <= 272000 for v in medium.stage_input_upper_bounds)
    assert low.allocation_hash != medium.allocation_hash


@pytest.mark.parametrize(
    "version,effort",
    [
        (PLATFORM_PRICE_VERSION, "medium"),
        (CURRENT_PLATFORM_PRICE_VERSION, "medium"),
        (MEDIUM, "low"),
        (MEDIUM, "high"),
    ],
)
def test_crossed_effort_and_profile_are_rejected_before_send(prepared, version, effort):
    with pytest.raises(ValueError, match="PLATFORM_PROFILE_INVALID"):
        freeze_platform_allocation(
            prepared, profile=PlatformProfile(price_version=version, effort=effort)
        )


def test_medium_does_not_reset_exhausted_cumulative_output(prepared):
    a = freeze_platform_allocation(
        prepared, profile=PlatformProfile(price_version=MEDIUM, effort="medium")
    )
    with pytest.raises(ValueError, match="PLATFORM_OUTPUT_EXHAUSTED"):
        compile_stage(
            prepared, stage="critique", allocation=a, prior_usage=(prior(prepared, output=8192),)
        )
    with pytest.raises(ValueError, match="PLATFORM_PROFILE_INVALID"):
        freeze_platform_allocation(prepared, profile=replace(a.profile, service_tier="priority"))


def test_medium_manifest_rejects_legacy_purpose_and_mixed_allocations():
    import copy

    from nexus.agents.comparison_manifest import (
        PLATFORM_QUALIFICATION_PURPOSE,
        parse_comparison_manifest,
    )
    from nexus.agents.platform_manifest import ThirdPlatformManifest
    from unit.agents.test_platform_manifest import platform_fixture

    _, _, data = platform_fixture(MEDIUM, purpose=PLATFORM_QUALIFICATION_PURPOSE)
    manifest = parse_comparison_manifest(data)
    assert manifest.price_version == MEDIUM and manifest.stage_limit == 9
    legacy = {**data, "platform_contract": ThirdPlatformManifest.purpose}
    with pytest.raises(ValueError, match="PLATFORM_PROFILE_INVALID"):
        ThirdPlatformManifest.model_validate(legacy)
    mixed = copy.deepcopy(data)
    mixed["allocations"][1]["profile"]["effort"] = "low"
    with pytest.raises(ValueError, match="PLATFORM_ALLOCATION_INVALID"):
        parse_comparison_manifest(mixed)
    old_schema = copy.deepcopy(data)
    old_schema["allocations"][0]["units_version"] = "platform-full-http-utf8-plus8192-v1"
    with pytest.raises(ValueError, match="PLATFORM_ALLOCATION_INVALID"):
        parse_comparison_manifest(old_schema)
