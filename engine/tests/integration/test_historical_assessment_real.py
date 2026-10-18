"""Opt-in acceptance using an unchanged private real archive and read-only DB snapshot."""

import asyncio
import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from helpers.paper_api import headers, settings
from nexus.main import create_engine_app
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_backup import LocalBackup
from nexus.ops.local_recovery import write_private
from nexus.paper.historical_assessment import source_for_cycle, validate_research
from nexus.paper.manual_request import _require_cycle_jobs_settled, require_manual_account_available
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import JobLease
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.mark.asyncio
async def test_real_archive_normal_jobs_pages_closeout_and_next_admission(monkeypatch):
    raw = os.environ.get("NEXUS_REAL_ASSESSMENT_ARCHIVE")
    if not raw:
        pytest.skip("explicit protected real archive required; never synthesizes its contents")
    path = Path(raw)
    original = hashlib.sha256(path.read_bytes()).hexdigest()
    root = Path(__file__).resolve().parents[3]
    repo = Path(os.environ["NEXUS_REAL_SOURCE_REPO"])
    destination = Path(os.environ["NEXUS_REAL_ACCEPTANCE_OUTPUT"])
    destination.mkdir(mode=0o700)
    owner = LocalBackup(destination, repo)
    owner.output = destination
    owner.environment = load_local_environment(repo, {})
    target = None
    receipt = {"stage": "snapshot", "status": "unknown", "model_calls": 0}
    try:
        target = await owner.start()
        manifest, _ = await owner.capture_database()
        write_private(destination / "manifest.json", json.dumps(manifest).encode())
        await owner.restore_database(manifest, target)
        receipt["stage"] = "migration"
        process = await asyncio.to_thread(
            subprocess.run,
            [
                "uv",
                "run",
                "--project",
                "engine",
                "alembic",
                "-c",
                "engine/alembic.ini",
                "upgrade",
                "head",
            ],
            cwd=root,
            env={**os.environ, "DATABASE_URL": target},
            capture_output=True,
            text=True,
            timeout=90,
        )
        write_private(destination / "migration.log", (process.stdout + process.stderr).encode())
        assert process.returncode == 0, "ISOLATED_MIGRATION_FAILED"
        receipt["stage"] = "archive_jobs"
        from nexus.ops.openai_local import local_settings

        runtime = local_settings(repo, load_model_key=False).model_copy(
            update={"database_url": target}
        )
        process = await asyncio.to_thread(
            subprocess.run,
            [
                "uv",
                "run",
                "--project",
                "engine",
                "python",
                "-m",
                "nexus.ops.historical_assessment",
                "--case-dir",
                str(path.parent),
                "--source-repo",
                str(repo),
            ],
            cwd=root,
            env={**os.environ, "DATABASE_URL": target},
            capture_output=True,
            text=True,
            timeout=180,
        )
        write_private(destination / "consumer.log", (process.stdout + process.stderr).encode())
        result = json.loads(process.stdout.strip().splitlines()[-1])
        if process.returncode != 0:
            receipt.update(result)
        assert process.returncode == 0, result.get("code", "ARCHIVE_CLI_FAILED")
        receipt.update(result)
        receipt["status"] = "checking"
        cycle_id = UUID(result["cycle_id"])
        engine = create_async_engine(target)
        sessions = async_sessionmaker(
            engine, expire_on_commit=False, info={"engine_settings": runtime}
        )
        try:
            async with sessions.begin() as session:
                source = await source_for_cycle(session, cycle_id)
                projection = await validate_research(session, source, at=datetime.now(UTC))
                assert [row["intent"]["quality_status"] for row in projection["research"]].count(
                    "partial"
                ) == 2
                cycle = await session.get(AgentPaperCycleRecord, cycle_id)
                await _require_cycle_jobs_settled(
                    session, cycle_id=cycle_id, now=datetime.now(UTC), require_no_live_lease=False
                )
                count = await session.scalar(
                    text("SELECT count(*) FROM strategy_decisions WHERE agent_paper_cycle_id=:id"),
                    {"id": cycle_id},
                )
                assert count == 0
                expiry = await session.scalar(select(func.max(JobLease.expires_at)))
            from datetime import timedelta
            from uuid import uuid4

            from nexus.paper.historical_assessment import (
                admit_archive,
                copy_columns,
                validate_source,
            )
            from nexus.paper.historical_assessment_source import read_archive
            from nexus.persistence.models.agents import AgentResearchOutputRecord
            from sqlalchemy.exc import DBAPIError

            with pytest.raises(ValueError, match="ARCHIVE_ALREADY_CONSUMED"):
                async with sessions.begin() as session:
                    await admit_archive(session, read_archive(path))
            async with sessions.begin() as session:
                with pytest.raises(ValueError, match="ARCHIVE_SOURCE_BINDING_INVALID"):
                    await validate_source(
                        session, source, at=source.imported_at - timedelta(seconds=1)
                    )
            with pytest.raises(DBAPIError, match="HISTORICAL_ASSESSMENT_OUTPUT_BINDING_INVALID"):
                async with sessions.begin() as session:
                    original_output = await session.get(
                        AgentResearchOutputRecord, UUID(projection["research"][0]["output_id"])
                    )
                    session.add(
                        AgentResearchOutputRecord(
                            **copy_columns(original_output, id=uuid4(), sequence=2)
                        )
                    )
                    await session.flush()
            receipt["duplicate_consumption_rejected"] = True
            receipt["backdated_visibility_rejected"] = True
            receipt["extra_output_rejected"] = True
            receipt["stage"] = "api"
            api_settings = runtime.model_copy(
                update={
                    "engine_hmac_current_key_id": settings(target).engine_hmac_current_key_id,
                    "engine_hmac_current_secret": settings(target).engine_hmac_current_secret,
                }
            )
            app = create_engine_app(api_settings)
            endpoint = f"/internal/v1/paper/management/cycles/{cycle_id}"
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://testserver"
            ) as client:
                response = await client.get(endpoint, headers=headers("GET", endpoint))
                assert response.status_code == 200, "ARCHIVE_API_READBACK_FAILED"
                data = response.json()["data"]
                assert data["historical_source"]["execution_allowed"] is False
                assert data["cycle"]["result_code"] == "PORTFOLIO_ASSESSMENT_DEFERRED"
                assert data["proposal"] is None and data["orders"] == []
            receipt["stage"] = "pages"
            from helpers.controlled_browser import verify_controlled_browser

            await verify_controlled_browser(
                target,
                api_now=datetime.now(UTC),
                cycle_id=cycle_id,
                order_ids=(),
                candidate_count=3,
                deferred_closeout=True,
                historical_archive=True,
                manual_settings=api_settings,
            )
            # Wait for real lease expiry, never use a fabricated future admission clock.
            remaining = (expiry - datetime.now(UTC)).total_seconds() if expiry else 0
            if 0 < remaining <= 65:
                while remaining > 0:
                    await asyncio.sleep(min(remaining + 0.05, 30))
                    remaining = (expiry - datetime.now(UTC)).total_seconds()
            receipt["stage"] = "next_admission"
            async with sessions.begin() as session:
                now = await session.scalar(select(func.clock_timestamp()))
                await _require_cycle_jobs_settled(session, cycle_id=cycle_id, now=now)
                try:
                    await require_manual_account_available(
                        session, account_id=cycle.account_id, now=now
                    )
                    receipt["next_account_admission"] = "ready"
                except ValueError as error:
                    receipt["next_account_admission"] = (
                        str(error) if str(error) == "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN" else "UNKNOWN"
                    )
            receipt.update(
                status="verified",
                stage="complete",
                archive_unchanged=True,
                new_cycle_jobs_settled=True,
                real_trading_session_admission=False,
            )
        finally:
            await engine.dispose()
    finally:
        assert hashlib.sha256(path.read_bytes()).hexdigest() == original
        write_private(destination / "acceptance.json", json.dumps(receipt).encode())
        await owner.cleanup()
