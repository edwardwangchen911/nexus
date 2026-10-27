"""Opt-in real image readers against synthetic, isolated PostgreSQL contracts."""

import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from integration import test_accounting_adoption as accounting_fixture
from integration.test_accounting_adoption import designation, seed_case
from integration.test_daily_v2_readback_pg import partial_daily
from integration.test_sec_local import INSTRUMENT, seed_instrument
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.accounting_adoption_storage import adopt_review, register_designation
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.engine import make_url

pytestmark = pytest.mark.skipif(
    not os.environ.get("NEXUS_COMPAT_ENGINE_IMAGE"),
    reason="explicit NEXUS_COMPAT_ENGINE_IMAGE is required for real image compatibility",
)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


async def image_readback(url, manifest, private, *, image=None):
    parsed = make_url(url)
    assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", parsed.database or "")
    image = image or os.environ["NEXUS_COMPAT_ENGINE_IMAGE"]
    identity = await asyncio.to_thread(
        subprocess.check_output,
        ["docker", "image", "inspect", image, "--format", "{{.Id}}"],
        text=True,
    )
    identity = identity.strip()
    private.mkdir(mode=0o700, parents=True, exist_ok=True)
    private.chmod(0o700)
    shutil.copyfile(
        Path(__file__).parents[1] / "helpers/monitor_image_readback.py", private / "readback.py"
    )
    (private / "manifest.json").write_text(json.dumps(manifest, sort_keys=True))
    for path in private.iterdir():
        path.chmod(0o600)
    isolated_url = parsed.set(host="127.0.0.1", port=5432).render_as_string(hide_password=False)
    result = await asyncio.to_thread(
        subprocess.run,
        [
            "docker",
            "run",
            "--rm",
            "--read-only",
            "-i",
            "--network",
            "container:nexus-monitor-tests-20261024",
            "--mount",
            f"type=bind,source={private.resolve()},target=/compat,readonly",
            "--entrypoint",
            "/app/.venv/bin/python",
            identity,
            "-I",
            "/compat/readback.py",
        ],
        input=json.dumps({"database_url": isolated_url}),
        text=True,
        capture_output=True,
        timeout=75,
    )
    # Never persist connection material, even when Docker or a reader fails.
    output = result.stdout.replace(parsed.password or "<no-password>", "[REDACTED]")
    error = result.stderr.replace(parsed.password or "<no-password>", "[REDACTED]")
    (private / "consumer.stdout.json").write_text(output)
    (private / "consumer.stderr.log").write_text(error)
    (private / "image-id.txt").write_text(identity + "\n")
    for path in private.iterdir():
        path.chmod(0o600)
    assert result.returncode == 0, {"exit": result.returncode, "stdout": output, "stderr": error}
    report = json.loads(output)
    assert report["status"] == "passed" and report["transaction_read_only"] == "on"
    assert report["module_path"].startswith("/app/engine/src/nexus/")
    return report


async def persisted_facts(factory):
    models = (
        MonitorTrigger,
        MonitorIncrement,
        AgentResearchRunRecord,
        AgentDailyBudgetReservationRecord,
        AgentResearchOutputRecord,
        DataSnapshot,
        ResearchEvidenceRef,
    )
    async with factory() as session:
        return {
            model.__tablename__: digest(
                sorted(
                    [
                        dict(row)
                        for row in (await session.execute(select(model.__table__))).mappings()
                    ],
                    key=lambda row: str(row.get("id")),
                )
            )
            for model in models
        }


@pytest.mark.asyncio
async def test_image_reads_0086_adoption_and_same_data_after_0097_upgrade(
    migrated_database_url,
    monkeypatch,
    tmp_path,
):
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    await asyncio.to_thread(command.downgrade, config, "0086_accounting_adoption")
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await seed_instrument(settings)
    factory = session_factory(settings)
    try:
        (tmp_path / "sources").mkdir()
        async with factory() as session, session.begin():
            # The normal receipt validates against PG time. Generate synthetic
            # review timestamps from that same clock, not the host wall clock.
            review_clock = await session.scalar(select(func.clock_timestamp()))

            class ReviewClock(datetime):
                @classmethod
                def now(cls, tz=None):
                    return review_clock.astimezone(tz) if tz else review_clock.replace(tzinfo=None)

            with monkeypatch.context() as review_fixture:
                review_fixture.setattr(accounting_fixture, "datetime", ReviewClock)
                material, opinion, _, _, _, _ = await seed_case(session, tmp_path / "sources")
            before = await session.scalar(select(func.clock_timestamp()))
            old = await prepare_research_snapshot(session, INSTRUMENT, as_of=before)
            authorization = await register_designation(
                session, opinion.submission_id, designation(material, opinion)
            )
            evidence = await adopt_review(
                session, opinion.submission_id, UUID(int=20), source_root=tmp_path / "sources"
            )
            after = await session.scalar(select(func.clock_timestamp()))
            new = await prepare_research_snapshot(session, INSTRUMENT, as_of=after)
            manifest = {
                "mode": "adoption",
                "instrument_id": str(INSTRUMENT),
                "before": before.isoformat(),
                "after": after.isoformat(),
                "evidence_id": str(evidence.id),
                "adoption_hash": evidence.content_hash,
                "designation_hash": authorization.revision,
                "old_snapshot": str(old.id),
                "new_snapshot": str(new.id),
                "old_payload_hash": digest(old.payload),
                "new_payload_hash": digest(new.payload),
            }
        first = await image_readback(migrated_database_url, manifest, tmp_path / "image-0086")
        async with factory() as session:
            before_upgrade = {
                row.id: (row.revision, digest(row.payload))
                for row in await session.scalars(select(DataSnapshot))
            }
        await factory.kw["bind"].dispose()
        await asyncio.to_thread(command.upgrade, config, "head")
        second = await image_readback(migrated_database_url, manifest, tmp_path / "image-0097")
        assert first["adoption_hash"] == second["adoption_hash"] == evidence.content_hash
        assert first["evidence_ids"] == second["evidence_ids"] == [str(evidence.id)]
        async with factory() as session:
            assert {
                row.id: (row.revision, digest(row.payload))
                for row in await session.scalars(select(DataSnapshot))
            } == before_upgrade
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_images_read_partial_daily_proof_and_fence_old_schema_writer(
    migrated_database_url,
    monkeypatch,
    tmp_path,
):
    old_image = os.environ.get("NEXUS_COMPAT_OLD_ENGINE_IMAGE")
    if not old_image:
        pytest.skip("explicit NEXUS_COMPAT_OLD_ENGINE_IMAGE is required")
    (tmp_path / "partial").mkdir()
    (factory, _, captured, pointer, batch_id, _, _, _, daily, proof, _) = await partial_daily(
        migrated_database_url, monkeypatch, tmp_path / "partial"
    )
    try:
        before = await persisted_facts(factory)
        manifest = {
            "mode": "monitor",
            "now": captured["clock"][0].isoformat(),
            "parent_run_id": str(pointer.source_run_id),
            "daily_run_id": str(daily.id),
            "proof_id": str(proof.id),
            "batch_id": str(batch_id),
        }
        report = await image_readback(migrated_database_url, manifest, tmp_path / "image-monitor")
        assert report["mixed"]["unresolved_event_count"] == 2
        current = await image_readback(
            migrated_database_url,
            {
                "mode": "schema",
                "expected_image_schema": "0098_independent_monitor",
                "expect_mismatch": False,
            },
            tmp_path / "image-current-schema",
        )
        old = await image_readback(
            migrated_database_url,
            {
                "mode": "schema",
                "expected_image_schema": "0086_accounting_adoption",
                "expect_mismatch": True,
            },
            tmp_path / "image-old-schema",
            image=old_image,
        )
        assert current["schema_guard"] == "ready"
        assert old["schema_guard"] == "DAILY_PIPELINE_DATABASE_HEAD_MISMATCH"
        assert await persisted_facts(factory) == before
    finally:
        await factory.kw["bind"].dispose()
