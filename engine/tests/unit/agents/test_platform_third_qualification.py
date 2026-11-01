"""The separately approved third round cannot inherit either consumed identity."""

import hashlib
import json
from decimal import Decimal
from uuid import uuid4

import pytest
from nexus.agents.comparison_manifest import parse_comparison_manifest, require_business_run
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION
from nexus.ops.research_comparison import prepare_file
from unit.agents.test_platform_manifest import platform_fixture

PURPOSE = "platform-terra-v3-20261101"


@pytest.mark.parametrize("purpose", [PURPOSE, "platform-qualification-v1"])
def test_normal_prepare_and_manifest_keep_current_prices_and_new_identity(tmp_path, purpose):
    source, _, original = platform_fixture(CURRENT_PLATFORM_PRICE_VERSION)
    original_manifest = parse_comparison_manifest(original)
    path = tmp_path / "source.json"
    path.write_bytes(source)
    output = tmp_path / "prepared.json"
    prepare_file(
        source=path,
        expected_source_hash=hashlib.sha256(source).hexdigest(),
        experiment_id=uuid4(),
        output=output,
        purpose=purpose,
    )
    prepared = json.loads(output.read_bytes())
    data = dict(original)
    for key in (
        "experiment_id",
        "source_manifest_hash",
        "input_hashes",
        "instrument_ids",
        "identity_equivalence_hash",
        "outbound_safety_hash",
        "allocations",
    ):
        data[key] = prepared[key]
    data["platform_contract"] = purpose
    manifest = parse_comparison_manifest(data)
    assert manifest.purpose == purpose and manifest.stage_limit == 9
    assert manifest.positions == original_manifest.positions
    assert manifest.price_version == CURRENT_PLATFORM_PRICE_VERSION
    assert manifest.api_config_hash == original_manifest.api_config_hash
    assert manifest.digest != original_manifest.digest
    assert manifest.total_reservation_usd <= Decimal("6.42")
    assert parse_comparison_manifest(original).digest == original_manifest.digest
    with pytest.raises(ValueError, match="COMPARISON_BUSINESS_CONSUMPTION_FORBIDDEN"):
        require_business_run(manifest.actor)
