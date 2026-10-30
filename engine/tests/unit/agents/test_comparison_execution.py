import asyncio
import hashlib
import json
from uuid import uuid4

import pytest
from nexus.agents.comparison_preparation import prepare_archive
from nexus.agents.models import ResearchRequest
from nexus.domain.evidence import EvidenceRef
from unit.agents.test_comparison_inputs import archive_source


@pytest.mark.asyncio
async def test_real_committee_pool_uses_exact_frozen_inputs_and_capacity_gate():
    from nexus.agents.codex_sdk import codex_request_wire
    from nexus.agents.comparison_execution import prepare_execution_pool
    from nexus.agents.comparison_manifest import SDK_HASH, ComparisonManifest

    source = json.dumps(archive_source()).encode()
    prepared = prepare_archive(
        source, expected_source_hash=hashlib.sha256(source).hexdigest(), experiment_id=uuid4()
    )
    manifest = ComparisonManifest(
        experiment_id=prepared["experiment_id"],
        sdk_config_hash=SDK_HASH,
        api_config_hash="a" * 64,
        implementation_sha="b" * 40,
        source_manifest_hash=prepared["source_manifest_hash"],
        identity_equivalence_hash=prepared["identity_equivalence_hash"],
        outbound_safety_hash=prepared["outbound_safety_hash"],
        scoring_hash="c" * 64,
        blinding_hash="d" * 64,
        input_hashes=prepared["input_hashes"],
        instrument_ids=prepared["instrument_ids"],
    )
    pool = await prepare_execution_pool(
        source=source,
        prepared=prepared,
        manifest=manifest,
        deadline=asyncio.get_running_loop().time() + 300,
    )
    assert len(pool) == 3
    for member, compiler in zip(prepared["members"], pool, strict=True):
        request = ResearchRequest.model_validate(member["payload"]["request"])
        refs = tuple(EvidenceRef.model_validate(r) for r in member["payload"]["evidence"])
        assert compiler._capacity_pool_manifest is not None
        assert await compiler._inputs(request) == member["payload"]["inputs"]
        payload, _ = compiler.build_stage_request(
            stage="research",
            request=request,
            evidence_refs=refs,
            frozen_inputs=await compiler._inputs(request),
            previous=[],
        )
        assert (
            hashlib.sha256(codex_request_wire(payload).encode()).hexdigest()
            == member["first_stage_wire_hash"]
        )
        assert compiler.usage.provider_calls == 0
    prepared["members"][0]["payload"]["inputs"]["security_snapshot"]["changed"] = True
    with pytest.raises(ValueError, match="COMPARISON_PREPARATION_CHANGED"):
        await prepare_execution_pool(
            source=source,
            prepared=prepared,
            manifest=manifest,
            deadline=asyncio.get_running_loop().time() + 300,
        )


@pytest.mark.parametrize("database", [False, True])
def test_run_command_does_not_report_unconfirmed_execution_as_unclaimed(
    monkeypatch, capsys, tmp_path, database
):
    import json

    from nexus.ops.research_comparison import main
    from sqlalchemy.exc import SQLAlchemyError

    async def unknown(args):
        raise (SQLAlchemyError if database else RuntimeError)("private-provider-canary")

    monkeypatch.setattr("nexus.ops.research_comparison.native_command", unknown)
    monkeypatch.setattr(
        "sys.argv",
        [
            "comparison",
            "run-position",
            "--repo-root",
            str(tmp_path),
            "--position",
            "1",
            "--source",
            str(tmp_path / "source"),
            "--prepared",
            str(tmp_path / "prepared"),
            "--manifest",
            str(tmp_path / "manifest"),
            "--worker-id",
            "test-offline",
        ],
    )
    assert main() == 1
    output = capsys.readouterr().out
    assert "private-provider-canary" not in output
    assert json.loads(output)["status"] == "unknown"
    assert json.loads(output)["reason"] == "COMPARISON_EXECUTION_RESULT_UNKNOWN"
