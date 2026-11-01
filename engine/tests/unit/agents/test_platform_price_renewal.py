"""A renewed window never renews old identities or drops historical accounting."""

from decimal import Decimal
from types import SimpleNamespace

import pytest
from nexus.agents import platform_profile as profiles
from nexus.agents.comparison_manifest import parse_comparison_manifest
from nexus.agents.platform_stage_accounting import require_platform_stage_cost
from nexus.agents.platform_usage import PLATFORM_PRICE_VERSION
from unit.agents.test_platform_manifest import platform_fixture
from unit.agents.test_platform_stage_accounting import row

RENEWED = "platform-terra-default-implicit30m-20261101-v2"


def test_old_profile_is_immutable_and_new_window_is_separate():
    old = profiles.platform_model_fields()
    new = profiles.platform_model_fields(RENEWED)
    assert old["version"] == PLATFORM_PRICE_VERSION
    assert old["config_hash"] == "64ced6b0149a9b80139e62a007390562485107f903d230041697149f84cf487a"
    assert old["pricing_expires_at"].isoformat() == "2026-11-01T00:00:00+00:00"
    assert new["pricing_available_at"].isoformat() == "2026-11-01T00:00:00+00:00"
    assert new["pricing_expires_at"].isoformat() == "2026-11-02T00:00:00+00:00"
    assert new["config_hash"] != old["config_hash"]
    for version, fields in ((PLATFORM_PRICE_VERSION, old), (RENEWED, new)):
        assert profiles.platform_model_matches(SimpleNamespace(**fields))
        assert profiles.platform_profile_hash(version) == fields["config_hash"]
    altered = {**old, "pricing_expires_at": new["pricing_expires_at"]}
    assert not profiles.platform_model_matches(SimpleNamespace(**altered))
    assert not profiles.platform_model_matches(SimpleNamespace(**{**new, "version": "unknown"}))


@pytest.mark.parametrize("version", [PLATFORM_PRICE_VERSION, RENEWED])
def test_manifest_reads_both_versions_but_rejects_cross_version_allocation(version):
    _, _, data = platform_fixture(version)
    _, _, old = platform_fixture()
    for allocation in data["allocations"]:
        allocation["profile"]["price_version"] = version
    data["api_config_hash"] = profiles.platform_profile_hash(version)
    manifest = parse_comparison_manifest(data)
    assert manifest.price_version == version
    assert parse_comparison_manifest(manifest.model_dump()).digest == manifest.digest
    data["allocations"][1]["profile"]["price_version"] = (
        RENEWED if version == PLATFORM_PRICE_VERSION else PLATFORM_PRICE_VERSION
    )
    with pytest.raises(ValueError):
        parse_comparison_manifest(data)
    assert parse_comparison_manifest(old).price_version == PLATFORM_PRICE_VERSION


@pytest.mark.parametrize("version", [PLATFORM_PRICE_VERSION, RENEWED])
def test_settlement_requires_stage_price_version_to_match_frozen_model(version):
    model = SimpleNamespace(**profiles.platform_model_fields(version))
    rows = [row()]
    rows[0]["platform_binding"]["price_version"] = version
    assert require_platform_stage_cost(model, rows, 12000, 1000) == Decimal("0.0323")
    rows[0]["platform_binding"]["price_version"] = (
        RENEWED if version == PLATFORM_PRICE_VERSION else PLATFORM_PRICE_VERSION
    )
    with pytest.raises(ValueError, match="PLATFORM_USAGE_ACCOUNTING_UNCONFIRMED"):
        require_platform_stage_cost(model, rows, 12000, 1000)


@pytest.mark.asyncio
async def test_normal_prepare_roundtrips_through_real_worker_pool(tmp_path):
    import hashlib
    import json
    import time
    from uuid import UUID

    from nexus.agents.comparison_execution import prepare_execution_pool
    from nexus.ops.research_comparison import prepare_file

    source, _, data = platform_fixture(RENEWED)
    original = tmp_path / "source.json"
    original.write_bytes(source)
    output = tmp_path / "prepared.json"
    result = prepare_file(
        source=original,
        expected_source_hash=hashlib.sha256(source).hexdigest(),
        experiment_id=UUID(str(data["experiment_id"])),
        output=output,
        purpose=data["platform_contract"],
    )
    assert result["model_calls"] == result["database_writes"] == 0
    prepared = json.loads(output.read_bytes())
    data.update(
        allocations=prepared["allocations"], api_config_hash=profiles.platform_profile_hash(RENEWED)
    )
    manifest = parse_comparison_manifest(data)
    pool = await prepare_execution_pool(
        source=source, prepared=prepared, manifest=manifest, deadline=time.monotonic() + 300
    )
    assert len(pool) == 3 and manifest.price_version == RENEWED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version,at,accepted",
    [
        (PLATFORM_PRICE_VERSION, "2026-11-01T00:00:00+00:00", False),
        (RENEWED, "2026-10-31T23:59:59+00:00", False),
        (RENEWED, "2026-11-01T00:00:00+00:00", True),
        (RENEWED, "2026-11-01T23:59:59+00:00", True),
        (RENEWED, "2026-11-02T00:00:00+00:00", False),
    ],
)
async def test_registration_uses_db_time_and_never_renews_old_profile(version, at, accepted):
    from datetime import datetime

    class Session:
        writes = 0
        reads = 0

        async def scalar(self, statement):
            self.reads += 1
            if self.reads == 1:
                return datetime.fromisoformat(at)
            return SimpleNamespace(**profiles.platform_model_fields(version))

        async def execute(self, statement):
            self.writes += 1

    session = Session()
    if accepted:
        result = await profiles.register_platform_profile(session, version=version)
        assert result.version == version and session.writes == 1
    else:
        with pytest.raises(ValueError, match="PLATFORM_PRICE_WINDOW_INVALID"):
            await profiles.register_platform_profile(session, version=version)
        assert session.writes == 0
