import copy
import hashlib
import json
from dataclasses import asdict
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.agents.comparison_inputs import PreparedComparisonInput
from nexus.agents.comparison_manifest import (
    PLATFORM_RENEWED_PURPOSE,
    ComparisonManifest,
    parse_comparison_manifest,
    require_business_run,
)
from nexus.agents.comparison_preparation import prepare_archive
from nexus.agents.platform_manifest import PLATFORM_PURPOSE, PlatformManifest
from nexus.agents.platform_profile import platform_profile_hash
from nexus.agents.platform_schema import (
    CURRENT_PLATFORM_UNITS_VERSION,
    LEGACY_PLATFORM_UNITS_VERSION,
)
from nexus.agents.platform_usage import (
    CURRENT_PLATFORM_PRICE_VERSION,
    PLATFORM_PRICE_VERSION,
    platform_reservation_usd,
)
from nexus.agents.platform_wire import freeze_platform_allocation
from unit.agents.test_comparison_inputs import archive_source
from unit.agents.test_comparison_manifest import manifest_data


def platform_fixture(
    price_version=PLATFORM_PRICE_VERSION,
    *,
    purpose=None,
    units_version=CURRENT_PLATFORM_UNITS_VERSION,
):
    purpose = purpose or (
        PLATFORM_RENEWED_PURPOSE
        if price_version == CURRENT_PLATFORM_PRICE_VERSION
        else PLATFORM_PURPOSE
    )
    source = json.dumps(archive_source()).encode()
    data = manifest_data()
    prepared = prepare_archive(
        source,
        expected_source_hash=hashlib.sha256(source).hexdigest(),
        experiment_id=uuid4(),
        purpose=purpose,
        platform_price_version=price_version,
        platform_units_version=units_version,
    )
    data.update(
        experiment_id=prepared["experiment_id"],
        source_manifest_hash=prepared["source_manifest_hash"],
        api_config_hash=platform_profile_hash(price_version),
        input_hashes=prepared["input_hashes"],
        instrument_ids=prepared["instrument_ids"],
        identity_equivalence_hash=prepared["identity_equivalence_hash"],
        outbound_safety_hash=prepared["outbound_safety_hash"],
        platform_contract=purpose,
        allocations=prepared["allocations"],
        funding_proof_hash="a" * 64,
        credential_binding_hash="b" * 64,
    )
    return source, prepared, data


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [LEGACY_PLATFORM_UNITS_VERSION, CURRENT_PLATFORM_UNITS_VERSION])
async def test_execution_rebuilds_the_manifest_version_instead_of_current_defaults(version):
    import time

    from nexus.agents.comparison_execution import prepare_execution_pool
    from nexus.agents.platform_wire import compile_platform_stage
    from nexus.domain.evidence import EvidenceRef

    source, prepared, data = platform_fixture(units_version=version)
    manifest = parse_comparison_manifest(data)
    pool = await prepare_execution_pool(
        source=source, prepared=prepared, manifest=manifest, deadline=time.monotonic() + 300
    )
    c = pool[0]
    payload, model = c.build_stage_request(
        stage="research",
        request=c.frozen_request,
        evidence_refs=tuple(
            EvidenceRef.model_validate(r) for r in c.prepared_input.payload["evidence"]
        ),
        frozen_inputs=c.prepared_input.payload["inputs"],
        previous=[],
    )
    a = manifest.allocations[0]
    wire = compile_platform_stage(
        c.prepared_input,
        allocation=a,
        expected_allocation_hash=a.allocation_hash,
        stage="research",
        previous=[],
        prior_usage=(),
        compiled_request=(payload, model),
    )
    forms = json.loads(wire.wire)["text"]["format"]["schema"]["$defs"]["DisclosureReviewCondition"][
        "properties"
    ]["forms"]
    assert ("prefixItems" in forms) is (version == LEGACY_PLATFORM_UNITS_VERSION)
    assert ("items" in forms) is (version == CURRENT_PLATFORM_UNITS_VERSION)
    assert wire.input_units <= a.stage_input_upper_bounds[0]


def test_manifest_cannot_mix_legacy_and_current_schema_allocations():
    _, _, data = platform_fixture()
    data["allocations"][1]["units_version"] = LEGACY_PLATFORM_UNITS_VERSION
    with pytest.raises(ValueError, match="PLATFORM_ALLOCATION_INVALID"):
        parse_comparison_manifest(data)


@pytest.mark.asyncio
async def test_execution_rejects_version_switch_after_preparation():
    import time

    from nexus.agents.comparison_execution import prepare_execution_pool

    source, prepared, data = platform_fixture()
    for allocation in data["allocations"]:
        allocation["units_version"] = LEGACY_PLATFORM_UNITS_VERSION
    manifest = parse_comparison_manifest(data)
    with pytest.raises(ValueError, match="COMPARISON_PREPARATION_CHANGED"):
        await prepare_execution_pool(
            source=source, prepared=prepared, manifest=manifest, deadline=time.monotonic() + 300
        )


def test_three_positions_and_full_http_allocations_are_bound():
    _, prepared, data = platform_fixture()
    m = PlatformManifest.model_validate(data)
    assert m.positions == (
        (1, "AAPL", "responses"),
        (1, "AVGO", "responses"),
        (1, "NVDA", "responses"),
    )
    assert m.stage_limit == 9 and m.purpose == PLATFORM_PURPOSE
    assert parse_comparison_manifest(m.model_dump()).digest == m.digest
    expected = Decimal(0)
    for ordinal, member in enumerate(prepared["members"], 1):
        allocation = freeze_platform_allocation(
            PreparedComparisonInput(member["payload"], member["proof"], tuple(member["forbidden"]))
        )
        assert asdict(m.allocations[ordinal - 1]) == asdict(allocation)
        assert m.input_hash_for(ordinal) == member["input_hash"]
        assert m.reservation_for(ordinal) == platform_reservation_usd(
            allocation.stage_input_upper_bounds, 8192
        )
        expected += m.reservation_for(ordinal)
    assert m.total_reservation_usd == expected
    for ordinal in (0, 4, True):
        with pytest.raises(ValueError, match="POSITION_INVALID"):
            m.reservation_for(ordinal)


@pytest.mark.parametrize(
    "change",
    ["input", "price", "tier", "bounds", "output", "framing", "stages", "positions", "funding"],
)
def test_changed_allocation_or_authority_is_rejected(change):
    _, _, data = platform_fixture()
    data = copy.deepcopy(data)
    a = data["allocations"][0]
    if change == "input":
        a["input_hash"] = "0" * 64
    elif change == "price":
        a["profile"]["price_version"] = "zero"
    elif change == "tier":
        a["profile"]["service_tier"] = "flex"
    elif change == "bounds":
        a["stage_input_upper_bounds"][0] = 272001
    elif change == "output":
        a["output_limit"] = 8193
    elif change == "framing":
        a["framing_units"] = 0
    elif change == "stages":
        a["stage_order"] = ["research"]
    elif change == "positions":
        data["positions"] = [[1, "AAPL", "sdk"]]
    elif change == "funding":
        del data["funding_proof_hash"]
    with pytest.raises(ValueError):
        PlatformManifest.model_validate(data)


def test_old_manifest_hash_payload_positions_are_unchanged_and_new_is_not_business():
    old = ComparisonManifest.model_validate(manifest_data())
    assert len(old.positions) == 12 and old.stage_limit == 36
    assert "platform_contract" not in old.model_dump()
    assert parse_comparison_manifest(old.model_dump()).digest == old.digest
    _, _, data = platform_fixture()
    new = PlatformManifest.model_validate(data)
    with pytest.raises(ValueError):
        ComparisonManifest.model_validate(new.model_dump())
    with pytest.raises(ValueError, match="BUSINESS_CONSUMPTION_FORBIDDEN"):
        require_business_run(new.actor)
    with pytest.raises(ValueError, match="BUSINESS_CONSUMPTION_FORBIDDEN"):
        require_business_run(
            "business",
            SimpleNamespace(
                source="other", schema_version="1", payload={"purpose": PLATFORM_PURPOSE}
            ),
        )


def test_normal_prepare_cli_freezes_three_full_chains_without_db_or_network(
    tmp_path, monkeypatch, capsys
):
    from nexus.ops import openai_local, research_comparison

    source = json.dumps(archive_source()).encode()
    source_path, output = tmp_path / "source.json", tmp_path / "prepared.json"
    source_path.write_bytes(source)

    def forbidden(*args, **kwargs):
        raise AssertionError("offline preparation must not load settings or credentials")

    monkeypatch.setattr(openai_local, "local_settings", forbidden)
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-research-comparison",
            "prepare",
            "--source",
            str(source_path),
            "--source-sha256",
            hashlib.sha256(source).hexdigest(),
            "--experiment-id",
            str(uuid4()),
            "--output",
            str(output),
            "--purpose",
            PLATFORM_RENEWED_PURPOSE,
        ],
    )
    assert research_comparison.main() == 0
    result = json.loads(capsys.readouterr().out)
    artifact = json.loads(output.read_bytes())
    assert result["dispatch_enabled"] is False and result["model_calls"] == 0
    assert artifact["purpose"] == PLATFORM_RENEWED_PURPOSE and len(artifact["allocations"]) == 3
    assert all(
        a["profile"]["price_version"] == "platform-terra-default-implicit30m-20261101-v2"
        for a in artifact["allocations"]
    )
    assert output.stat().st_mode & 0o777 == 0o600
    assert source_path.read_bytes() == source


def test_normal_cli_retains_safe_platform_funding_reason(monkeypatch, capsys, tmp_path):
    from nexus.ops import research_comparison
    from nexus.ops.platform_credentials import PlatformCredentialError

    async def rejected(_):
        raise PlatformCredentialError("PLATFORM_BALANCE_INSUFFICIENT")

    monkeypatch.setattr(research_comparison, "native_command", rejected)
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-engine-research-comparison",
            "admit-position",
            "--repo-root",
            str(tmp_path),
            "--position",
            "1",
            "--model-config-id",
            str(uuid4()),
            "--source",
            str(tmp_path / "s"),
            "--prepared",
            str(tmp_path / "p"),
            "--manifest",
            str(tmp_path / "m"),
        ],
    )
    assert research_comparison.main() == 1
    assert json.loads(capsys.readouterr().out) == {
        "status": "rejected",
        "reason": "PLATFORM_BALANCE_INSUFFICIENT",
    }


@pytest.mark.asyncio
async def test_platform_normal_execution_pool_uses_complete_allocation_without_sdk_fallback():
    import asyncio

    from nexus.agents.comparison_execution import prepare_execution_pool
    from nexus.agents.platform_wire import freeze_platform_allocation

    source, prepared, data = platform_fixture()
    m = PlatformManifest.model_validate(data)
    pool = await prepare_execution_pool(
        source=source,
        prepared=prepared,
        manifest=m,
        deadline=asyncio.get_running_loop().time() + 300,
    )
    assert len(pool) == 3
    for c, a in zip(pool, m.allocations, strict=True):
        assert c._aggregate_input_tokens == sum(a.stage_input_upper_bounds)
        assert c._capacity_pool_manifest == m.digest
        assert freeze_platform_allocation(c.prepared_input) == a
