"""Archive consumption preserves quality and separates evidence/observation/import time."""

import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from unit.agents.test_historical_nvda_observation import invoke
from unit.agents.test_historical_pool_completion import accepted_parent
from unit.agents.test_historical_pool_diagnostic import install_transport


def assessment(tmp_path, monkeypatch, capsys):
    root, _, _, _, case = accepted_parent(tmp_path, monkeypatch, capsys)
    install_transport(monkeypatch, [], rejection="unresolved")
    from nexus.agents import codex_staged

    underlying = codex_staged.invoke_codex

    async def watch(payload, **options):
        raw, usage = await underlying(payload, **options)
        body = json.loads(raw)
        if payload["stage"] == "synthesis":
            body["synthesis_assessment"]["stance"] = "watch"
        return json.dumps(body), usage

    monkeypatch.setattr(codex_staged, "invoke_codex", watch)
    assert (
        invoke(
            monkeypatch,
            capsys,
            "prepare-pool-assessment",
            "--repo-root",
            root,
            "--source-cycle-id",
            case["source_cycle_id"],
        )[0]
        == 0
    )
    directory = (
        root / ".secrets/nexus-historical-three-stock" / f"{case['source_cycle_id']}-assessment-v1"
    )
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 0
    return directory / "case.sqlite3"


def test_source_projection_keeps_partial_and_rejects_backdated_availability(
    tmp_path, monkeypatch, capsys
):
    from nexus.paper.historical_assessment_source import project_archive, read_archive

    path = assessment(tmp_path, monkeypatch, capsys)
    before = path.read_bytes()
    archive = read_archive(path)
    now = datetime.now(UTC)
    projection = project_archive(archive, cycle_id=uuid4(), model_id=uuid4(), imported_at=now)
    assert path.read_bytes() == before
    assert len(projection["research"]) == 3
    assert any(row["intent"]["quality_status"] == "partial" for row in projection["research"])
    assert all(c["action"] == "defer" for c in projection["synthesis"]["intent"]["choices"])
    for original, derived in zip(archive["outputs"][:3], projection["research"], strict=True):
        assert {k: v for k, v in original["intent"].items() if k != "decision_id"} == {
            k: v for k, v in derived["intent"].items() if k != "decision_id"
        }
        assert original["output_id"] != derived["output_id"]
        assert derived["source_output_id"] == original["output_id"]
    with pytest.raises(ValueError, match="ARCHIVE_AVAILABILITY_INVALID"):
        project_archive(
            archive,
            cycle_id=uuid4(),
            model_id=uuid4(),
            imported_at=datetime.fromisoformat(archive["observed_finished_at"])
            - timedelta(seconds=1),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message,expected",
    [
        ("ARCHIVE_EVIDENCE_BINDING_INVALID", "ARCHIVE_EVIDENCE_BINDING_INVALID"),
        ("ARCHIVE_secret_token_do_not_print", "ARCHIVE_HANDLER_FAILED"),
    ],
)
async def test_worker_failure_preserves_only_closed_safe_codes(monkeypatch, message, expected):
    from nexus.jobs.errors import UnsafeJobError
    from nexus.paper import historical_assessment

    async def fail(_context):
        raise ValueError(message)

    monkeypatch.setattr(historical_assessment, "consume_archive", fail)
    with pytest.raises(UnsafeJobError) as result:
        await historical_assessment.guarded_archive_consumer(None)
    assert result.value.error_code == expected


@pytest.mark.parametrize("wrong_id", [False, True])
def test_completed_synthesis_requires_exact_reserved_output(
    tmp_path, monkeypatch, capsys, wrong_id
):
    from types import SimpleNamespace

    from nexus.agents.portfolio_synthesis_contracts import (
        canonical_portfolio_synthesis_hash as digest,
    )
    from nexus.paper.historical_assessment import validate_source
    from nexus.paper.historical_assessment_source import (
        MODEL_HASH,
        PROFILE,
        project_archive,
        read_archive,
    )

    archive = read_archive(assessment(tmp_path, monkeypatch, capsys))
    now = datetime.now(UTC)
    cycle_id, model_id = uuid4(), uuid4()
    projection = project_archive(archive, cycle_id=cycle_id, model_id=model_id, imported_at=now)
    payload = {"archive": archive, "projection": projection}
    from uuid import UUID

    source = SimpleNamespace(
        imported_at=now,
        content_hash=digest(payload),
        payload=payload,
        cycle_id=cycle_id,
        archive_hash=archive["archive_hash"],
        source_cycle_id=UUID(archive["case"]["source_cycle_id"]),
        evidence_at=datetime.fromisoformat(archive["case"]["evidence_as_of"]),
        observed_started_at=datetime.fromisoformat(archive["observed_started_at"]),
        observed_finished_at=datetime.fromisoformat(archive["observed_finished_at"]),
    )
    synth = projection["synthesis"]
    run = SimpleNamespace(
        id=UUID(synth["run_id"]),
        cycle_id=cycle_id,
        frozen_input=synth["frozen_input"],
        input_hash=synth["input_hash"],
        model_hash=MODEL_HASH,
        pricing_version=PROFILE,
        created_at=now,
        status="completed",
    )

    class Session:
        async def get(self, *_):
            return run

        async def scalars(self, *_):
            return [SimpleNamespace(id=uuid4())] if wrong_id else []

    with pytest.raises(ValueError, match="ARCHIVE_SYNTHESIS_BINDING_INVALID"):
        import asyncio

        asyncio.run(validate_source(Session(), source, at=now))
