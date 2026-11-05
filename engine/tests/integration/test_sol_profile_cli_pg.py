"""Registered local CLI + isolated PG, no Key, model request or business job."""

import asyncio
import json
from uuid import UUID

import pytest
from integration.test_sol_profile_pg import SOURCE, current_record
from nexus.ops import platform_credentials as cli
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSessionTransaction


@pytest.mark.asyncio
@pytest.mark.parametrize("output_format", ["structured-v2", "compact-v3"])
@pytest.mark.parametrize(
    "lost_commit_ack,failed_disposal", [(False, False), (True, False), (True, True)]
)
async def test_normal_sol_profile_cli_readback_without_keys_dispatch_or_duplicate_audit(
    migrated_database_url,
    tmp_path,
    monkeypatch,
    capsys,
    lost_commit_ack,
    failed_disposal,
    output_format,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    record = await current_record(factory)
    (tmp_path / "engine").mkdir()
    (tmp_path / "engine/pyproject.toml").write_text("# synthetic local root")
    price_path, source_path = tmp_path / "price.json", tmp_path / "source.txt"
    price_path.write_text(json.dumps(record))
    source_path.write_bytes(SOURCE)
    price_path.chmod(0o600)
    source_path.chmod(0o600)
    monkeypatch.chdir(tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("profile registration must not load a Key")

    monkeypatch.setattr(cli, "resolve_platform_credential", forbidden)
    monkeypatch.setattr(cli, "_resolved_key", forbidden)
    commit = AsyncSessionTransaction.commit
    lost = False

    async def commit_with_lost_ack(transaction):
        nonlocal lost
        await commit(transaction)
        if lost_commit_ack and not lost:
            lost = True
            raise ConnectionError("synthetic private commit diagnostic")

    monkeypatch.setattr(AsyncSessionTransaction, "commit", commit_with_lost_ack)
    dispose = AsyncEngine.dispose
    disposed_failure = False

    async def dispose_with_failure(engine, *args, **kwargs):
        nonlocal disposed_failure
        await dispose(engine, *args, **kwargs)
        if failed_disposal and not disposed_failure:
            disposed_failure = True
            raise RuntimeError("synthetic private disposal diagnostic")

    monkeypatch.setattr(AsyncEngine, "dispose", dispose_with_failure)
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-platform-local",
            "register-sol-profile",
            "--price-record",
            str(price_path),
            "--source-evidence",
            str(source_path),
            "--output-format",
            output_format,
        ],
    )
    code = await asyncio.to_thread(cli.main)
    result = json.loads(capsys.readouterr().out)
    assert code == int(lost_commit_ack)
    assert result["status"] == ("unknown" if lost_commit_ack else "profile_registered")
    assert result["model_requests"] == 0 and result["dispatch_authority"] is False
    assert "synthetic private" not in json.dumps(result)
    model_id = UUID(result["model_config_id"])
    monkeypatch.setattr(
        "sys.argv",
        ["nexus-platform-local", "inspect-sol-profile", "--model-config-id", str(model_id)],
    )
    assert await asyncio.to_thread(cli.main) == 0
    readback = json.loads(capsys.readouterr().out)
    assert readback["model_config_id"] == str(model_id)
    assert readback["config_hash"] == result["config_hash"]
    assert readback["output_format"] == result["output_format"] == output_format
    assert readback["database_writes"] == 0 and readback["inspection_only"] is True
    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "platform.sol_profile_registered")
            )
            == 1
        )
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
