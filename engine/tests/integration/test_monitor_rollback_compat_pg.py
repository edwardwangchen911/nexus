"""Opt-in actual older image writes new daily proof using unchanged frozen originals."""

import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from integration import test_monitor_mixed_host_pg as host_fixture
from integration.test_daily_v2_readback_pg import partial_daily
from nexus.persistence.models.data import DataSnapshot
from sqlalchemy.engine import make_url


@pytest.mark.asyncio
@pytest.mark.skipif(
    not os.environ.get("NEXUS_ROLLBACK_ENGINE_IMAGE"),
    reason="explicit isolated rollback image is required",
)
async def test_older_image_writes_daily_proof_and_serves_authenticated_reads(
    migrated_database_url, monkeypatch, tmp_path
):
    (tmp_path / "database-name.txt").write_text(make_url(migrated_database_url).database + "\n")
    saved = {}
    prepare = host_fixture.prepared_materials

    async def capture(*args, **kwargs):
        result = await prepare(*args, **kwargs)
        saved["store"] = result[2]
        return result

    monkeypatch.setattr(host_fixture, "prepared_materials", capture)

    async def before_reconcile(factory, settings, captured, pointer, batch_id, target, now):
        private = tmp_path / "rollback-image"
        private.mkdir(mode=0o700)
        objects = private / "objects"
        objects.mkdir(mode=0o700)
        bindings = {}
        for key, raw in saved["store"].objects.items():
            digest = hashlib.sha256(raw).hexdigest()
            (objects / digest).write_bytes(raw)
            (objects / digest).chmod(0o600)
            bindings[key] = digest
        async with factory() as session:
            snapshot = await session.get(DataSnapshot, target.data_snapshot_id)
            context = snapshot.payload["agent_paper_context"]["pending_monitoring_v2"]
        manifest = {
            "now": now.isoformat(),
            "account_id": str(pointer.account_id),
            "candidate_batch_id": str(settings.agent_paper_candidate_batch_id),
            "parent_run_id": str(pointer.source_run_id),
            "daily_run_id": str(target.id),
            "original_run_id": context["trigger_checks"][0]["original_run_id"],
            "price_generation": context["predecessors"]["price"]["generation"],
            "objects": bindings,
        }
        (private / "manifest.json").write_text(json.dumps(manifest, sort_keys=True))
        shutil.copyfile(
            Path(__file__).parents[1] / "helpers/monitor_rollback_consumer.py",
            private / "consumer.py",
        )
        parsed = make_url(migrated_database_url)
        assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", parsed.database or "")
        # Only the synthetic DB identity is persisted; credentials travel on stdin.
        (private / "database-name.txt").write_text(parsed.database + "\n")
        identity = (
            await asyncio.to_thread(
                subprocess.check_output,
                [
                    "docker",
                    "image",
                    "inspect",
                    os.environ["NEXUS_ROLLBACK_ENGINE_IMAGE"],
                    "--format",
                    "{{.Id}}",
                ],
                text=True,
            )
        ).strip()
        (private / "image-id.txt").write_text(identity + "\n")
        for path in private.iterdir():
            if path.is_file():
                path.chmod(0o600)
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
                "/compat/consumer.py",
            ],
            input=json.dumps(
                {
                    "database_url": parsed.set(host="127.0.0.1", port=5432).render_as_string(
                        hide_password=False
                    )
                }
            ),
            text=True,
            capture_output=True,
            timeout=240,
        )
        stdout = result.stdout.replace(parsed.password or "<no-password>", "[REDACTED]")
        stderr = result.stderr.replace(parsed.password or "<no-password>", "[REDACTED]")
        (private / "consumer.stdout.json").write_text(stdout)
        (private / "consumer.stderr.log").write_text(stderr)
        assert result.returncode == 0, {
            "exit": result.returncode,
            "stdout": stdout,
            "stderr": stderr,
        }
        report = json.loads(stdout)
        assert report["status"] == "passed"
        assert report["source_objects"] == len(bindings) > 0
        assert report["nonce_rows_added"] == 2
        saved["report"] = report

    (tmp_path / "partial").mkdir()
    result = await partial_daily(
        migrated_database_url,
        monkeypatch,
        tmp_path / "partial",
        before_reconcile=before_reconcile,
    )
    try:
        assert saved["report"]["status"] == "passed"
    finally:
        await result[0].kw["bind"].dispose()
