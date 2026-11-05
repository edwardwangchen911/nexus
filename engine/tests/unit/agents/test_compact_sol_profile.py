"""Existing immutable model identity freezes presentation without changing price."""

from copy import deepcopy
from types import SimpleNamespace

import pytest
from nexus.agents import platform_native, platform_profile
from nexus.agents.platform_schema import (
    COMPACT_PLATFORM_UNITS_VERSION,
    CURRENT_PLATFORM_UNITS_VERSION,
)
from unit.agents.test_native_sol_wire import native_case
from unit.agents.test_sol_pricing import sol_price_record


def test_compact_profile_is_distinct_but_preserves_original_economic_and_model_capability():
    old = platform_profile.sol_business_model_fields(sol_price_record())
    frozen = deepcopy(old)
    new = platform_profile.sol_business_model_fields(
        sol_price_record(), units_version=COMPACT_PLATFORM_UNITS_VERSION
    )
    assert old == frozen
    assert old["config_hash"] != new["config_hash"] and old["version"] != new["version"]
    profile = dict(new["platform_profile"])
    assert profile.pop("output_units_version") == COMPACT_PLATFORM_UNITS_VERSION
    assert profile == old["platform_profile"]
    for field in old.keys() - {"platform_profile", "version", "config_hash"}:
        assert old[field] == new[field]
    for fields in (old, new):
        assert platform_profile.sol_business_model_matches(SimpleNamespace(**fields))
    new["platform_profile"]["output_units_version"] = CURRENT_PLATFORM_UNITS_VERSION
    assert not platform_profile.sol_business_model_matches(SimpleNamespace(**new))


@pytest.mark.parametrize("bad", [None, [], True, "unknown-output-version"])
def test_malformed_output_version_is_a_rejected_profile(bad):
    fields = platform_profile.sol_business_model_fields(sol_price_record())
    fields["platform_profile"]["output_units_version"] = bad
    assert platform_profile.sol_business_model_matches(SimpleNamespace(**fields)) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version", [CURRENT_PLATFORM_UNITS_VERSION, COMPACT_PLATFORM_UNITS_VERSION]
)
async def test_native_producer_uses_original_model_format_not_registration_default(
    monkeypatch, version
):
    _, _, compiler, inputs, price, original = native_case()
    model = SimpleNamespace(
        **platform_profile.sol_business_model_fields(price, units_version=version)
    )

    async def material(*args, **kwargs):
        return compiler, inputs["request"], inputs["evidence_refs"], inputs["inputs"]

    monkeypatch.setattr(platform_native, "native_prepared_material", material)
    result = await platform_native.native_run_allocation(None, run=None, model=model)
    assert result.units_version == version
    assert result.profile == original.profile and result.output_limit == 8192
    assert result.stage_schema_envelope_hashes == original.stage_schema_envelope_hashes
    if version == CURRENT_PLATFORM_UNITS_VERSION:
        assert result == original
    else:
        assert result.allocation_hash != original.allocation_hash
        assert all(
            new == old + 218
            for new, old in zip(
                result.stage_input_upper_bounds, original.stage_input_upper_bounds, strict=True
            )
        )
