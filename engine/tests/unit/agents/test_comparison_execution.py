import asyncio
import hashlib
import json
from uuid import uuid4

import pytest
from nexus.agents.comparison_preparation import prepare_archive
from nexus.agents.models import ResearchRequest
from nexus.domain.evidence import EvidenceRef
from unit.agents.test_comparison_inputs import archive_source


def test_preclaim_cli_commit_unknown_is_safe_and_never_replayed(monkeypatch, tmp_path, capsys):
    from nexus.ops import research_comparison as cli

    calls = 0

    async def lost_ack(_):
        nonlocal calls
        calls += 1
        try:
            raise OSError("private-driver-ack-canary")
        except OSError as cause:
            raise cli.PreclaimCommitUnknown() from cause

    monkeypatch.setattr(cli, "native_command", lost_ack)
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-research-comparison",
            "close-preclaim",
            "--repo-root",
            str(tmp_path),
            "--source",
            str(tmp_path / "source.json"),
            "--prepared",
            str(tmp_path / "prepared.json"),
            "--manifest",
            str(tmp_path / "manifest.json"),
        ],
    )
    assert cli.main() == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        "status": "unknown",
        "reason": "COMPARISON_PRECLOSE_RESULT_UNKNOWN",
        "replay_allowed": False,
    }
    assert calls == 1 and "private-driver" not in captured.out + captured.err


@pytest.mark.asyncio
@pytest.mark.parametrize("member_index", [0, 2])
async def test_old_first_stage_artifact_is_rejected_before_any_native_write(
    monkeypatch, member_index
):
    from decimal import Decimal
    from unittest.mock import MagicMock

    from nexus.agents import comparison_preparation
    from nexus.agents.committee import INDEPENDENT_MONITOR_PROMPT_VERSION
    from nexus.agents.comparison_manifest import SDK_HASH, ComparisonManifest
    from nexus.agents.comparison_producer import admit_comparison_position
    from sqlalchemy.ext.asyncio import AsyncSession

    archive = archive_source()
    member = archive["members"][member_index]
    for prompt in member["prompts"].values():
        prompt["version"] = INDEPENDENT_MONITOR_PROMPT_VERSION
    member["inputs"]["filing_disclosures"]["items"] = [{"excerpt": "x" * 50000}]
    source = json.dumps(archive).encode()
    # Reconstruct the old first-stage-only artifact; admission below uses real gates.
    with monkeypatch.context() as legacy:
        legacy.setattr(comparison_preparation, "validate_comparison_capacity", lambda _: None)
        prepared = prepare_archive(
            source, expected_source_hash=hashlib.sha256(source).hexdigest(), experiment_id=uuid4()
        )
    assert all(row["first_stage_wire_bytes"] < 100479 for row in prepared["members"])
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
    original = json.dumps(prepared, sort_keys=True)
    session = MagicMock(spec=AsyncSession)
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_WIRE_EXCEEDED$"):
        await admit_comparison_position(
            session,
            source=source,
            prepared=prepared,
            manifest=manifest,
            ordinal=1,
            model_config_id=uuid4(),
            daily_limit_usd=Decimal("100"),
        )
    assert session.mock_calls == []
    assert json.dumps(prepared, sort_keys=True) == original


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


@pytest.mark.parametrize(
    "command,status", [("admit-position", "rejected"), ("run-position", "unknown")]
)
def test_comparison_busy_is_distinct_and_preserves_post_claim_uncertainty(
    monkeypatch, capsys, tmp_path, command, status
):
    import json

    from nexus.agents.budget import ResearchAccountingBusy
    from nexus.ops.research_comparison import main

    calls = []

    async def busy(args):
        calls.append(args.command)
        raise ResearchAccountingBusy()

    monkeypatch.setattr("nexus.ops.research_comparison.native_command", busy)
    argv = [
        "comparison",
        command,
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
    ]
    argv += (
        ["--worker-id", "offline-test"]
        if command == "run-position"
        else ["--model-config-id", "00000000-0000-4000-8000-000000000001"]
    )
    monkeypatch.setattr("sys.argv", argv)
    assert main() == 1
    assert json.loads(capsys.readouterr().out) == {
        "status": status,
        "reason": "RESEARCH_ACCOUNTING_BUSY",
        "replay_allowed": False,
    }
    assert calls == [command]
