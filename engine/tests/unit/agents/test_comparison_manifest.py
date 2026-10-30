from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.agents.comparison_manifest import PURPOSE, ComparisonManifest, require_business_run
from pydantic import ValidationError


def manifest_data():
    return {
        "experiment_id": str(uuid4()),
        "sdk_config_hash": "cdf5e7d3b0cfd2a3e6fda267f74bc9b3d135f28e33f965431f17175b9f2a8720",
        "api_config_hash": "a" * 64,
        "implementation_sha": "b" * 40,
        "source_manifest_hash": "c" * 64,
        "identity_equivalence_hash": "d" * 64,
        "outbound_safety_hash": "e" * 64,
        "scoring_hash": "f" * 64,
        "blinding_hash": "1" * 64,
        "input_hashes": ("2" * 64, "3" * 64, "4" * 64),
        "instrument_ids": tuple(str(uuid4()) for _ in range(3)),
    }


def test_comparison_has_exact_fixed_nonresetting_positions():
    manifest = ComparisonManifest.model_validate(manifest_data())
    assert len(manifest.positions) == 12
    assert manifest.stage_limit == 36
    assert manifest.positions[:2] == ((1, "AAPL", "sdk"), (1, "AAPL", "responses"))
    assert manifest.positions[-2:] == ((2, "AAPL", "responses"), (2, "AAPL", "sdk"))
    assert (
        manifest.digest == ComparisonManifest.model_validate_json(manifest.model_dump_json()).digest
    )


@pytest.mark.parametrize(
    "field", ["identity_equivalence_hash", "outbound_safety_hash", "scoring_hash"]
)
def test_missing_preregistered_authority_is_rejected(field):
    data = manifest_data()
    del data[field]
    with pytest.raises(ValidationError):
        ComparisonManifest.model_validate(data)


def test_cannot_increase_positions_or_reuse_sdk_config_for_http():
    data = manifest_data()
    with pytest.raises(ValidationError):
        ComparisonManifest.model_validate({**data, "stage_limit": 39})
    with pytest.raises(ValidationError):
        ComparisonManifest.model_validate({**data, "api_config_hash": data["sdk_config_hash"]})
    with pytest.raises(ValidationError):
        ComparisonManifest.model_validate({**data, "sdk_config_hash": "0" * 64})


def test_input_identity_and_hash_change_are_detectable():
    data = manifest_data()
    manifest = ComparisonManifest.model_validate(data)
    assert manifest.input_hash_for(1) == manifest.input_hash_for(2) == manifest.input_hash_for(11)
    changed = ComparisonManifest.model_validate(
        {**data, "input_hashes": ("5" * 64, "3" * 64, "4" * 64)}
    )
    assert changed.digest != manifest.digest
    for invalid in (0, 13):
        with pytest.raises(ValueError, match="COMPARISON_POSITION_INVALID"):
            manifest.input_hash_for(invalid)


@pytest.mark.parametrize("marker", ["source", "schema_version", "purpose"])
def test_snapshot_marker_cannot_be_consumed_under_a_different_actor(marker):
    snapshot = SimpleNamespace(source="business", schema_version="1", payload={})
    if marker == "purpose":
        snapshot.payload["purpose"] = PURPOSE
    else:
        setattr(
            snapshot,
            marker,
            "nexus_research_comparison" if marker == "source" else "comparison-input-v1",
        )
    with pytest.raises(ValueError, match="COMPARISON_BUSINESS_CONSUMPTION_FORBIDDEN"):
        require_business_run("ordinary-business-actor", snapshot)
