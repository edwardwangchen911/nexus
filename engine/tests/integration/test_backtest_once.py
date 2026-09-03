"""First-run CLI path starts with frozen authorities, never a pre-existing run."""

import asyncio
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from helpers.portfolio_api import app, headers
from nexus.backtest.authority_loader import load_point_in_time_dataset
from nexus.backtest.cost_policy import BACKTEST_COST_POLICY_ID
from nexus.backtest.engine import run_backtest
from nexus.ops import backtest_once
from nexus.ops.backtest_once import BacktestOnceError, BacktestOnceRequest, run_once
from nexus.persistence.models.backtest import BacktestRunRecord
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text

from engine.tests.integration.test_backtest_writer import (
    AUTHORITY_BUNDLE_ID,
    COMPLETED_AT,
    DATA_SNAPSHOT_ID,
    _dataset,
    _raw_inputs,
    _seed_data_snapshot,
    _seed_upstream_authorities,
)


async def prepare(database_url: str) -> BacktestOnceRequest:
    factory = session_factory(EngineSettings(database_url=database_url))
    strategy, bars, sessions, protocol = _raw_inputs()
    price_authorities = await _seed_upstream_authorities(factory, bars, sessions)
    data = _dataset(bars, sessions, price_authorities)
    await _seed_data_snapshot(factory, data, protocol, run_backtest(strategy, data, protocol))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(BacktestRunRecord)) == 0
    return BacktestOnceRequest(
        strategy_version="baseline_v1", authority_bundle_id=AUTHORITY_BUNDLE_ID,
        data_snapshot_id=DATA_SNAPSHOT_ID, source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID, start=protocol.start, end=protocol.end,
        initial_nav=protocol.initial_nav, as_of=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_first_run_persists_once_and_is_readable_without_promoting(
    migrated_database_url: str,
) -> None:
    request = await prepare(migrated_database_url)
    settings = EngineSettings(database_url=migrated_database_url)
    args = [
        part for key, value in request.model_dump(mode="json").items()
        for part in (f"--{key.replace('_', '-')}", str(value))
    ]
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "nexus.ops.backtest_once", *args,
        env={**os.environ, "DATABASE_URL": migrated_database_url},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
    except TimeoutError:
        process.kill()
        await process.wait()
        raise
    assert process.returncode == 0, stderr.decode()
    first = json.loads(stdout)
    replay = await run_once(settings, request)
    assert first["run_id"] == replay["run_id"]
    assert first["result_hash"] == replay["result_hash"]
    assert first["status"] == "BACKTEST_COMPLETED"
    assert replay["status"] == "BACKTEST_REPLAYED"
    assert first["benchmarks"] == ["champion", "equal_weight", "spy"]
    assert first["promotable"] is False
    assert first["history_requirement_met"] is False
    factory = session_factory(settings)
    async with factory() as session:
        records = list(await session.scalars(select(BacktestRunRecord)))
        assert len(records) == 1 and records[0].graph_sealed
        loaded = await load_point_in_time_dataset(session, records[0].id, datetime.now(UTC))
        assert loaded.stored_result.result_hash == first["result_hash"]
        assert loaded.protocol.cost_model.spread_bps == 5
        assert loaded.protocol.cost_model.slippage_bps == 10
    path = "/internal/v1/strategies"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    summary = response.json()["data"][0]["latest_backtest"]
    assert summary["result_hash"] == first["result_hash"]
    assert summary["data_mode"] == "fixture"


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["cutoff", "dataset", "cost", "range", "source"])
async def test_first_run_rejects_wrong_binding_without_partial_graph(
    migrated_database_url: str, invalid: str,
) -> None:
    request = await prepare(migrated_database_url)
    updates = {
        "cutoff": {"as_of": COMPLETED_AT - timedelta(seconds=1)},
        "dataset": {"data_snapshot_id": uuid4()},
        "cost": {"cost_policy_id": uuid4()},
        "source": {"source_policy_id": uuid4()},
        "range": {"start": request.start - timedelta(days=1)},
    }
    request = request.model_copy(update=updates[invalid])
    with pytest.raises(BacktestOnceError):
        await run_once(EngineSettings(database_url=migrated_database_url), request)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(BacktestRunRecord)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", ["corporate_action", "mixed_price", "late_collection"])
async def test_inconsistent_source_or_action_members_do_not_create_a_run(
    migrated_database_url: str, corrupt: str,
) -> None:
    request = await prepare(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        if corrupt == "corporate_action":
            await session.execute(text(
                "ALTER TABLE backtest_corporate_action_authority_members DISABLE TRIGGER USER"
            ))
            await session.execute(text("DELETE FROM backtest_corporate_action_authority_members"))
            await session.execute(text(
                "ALTER TABLE backtest_corporate_action_authority_members ENABLE TRIGGER USER"
            ))
        elif corrupt == "mixed_price":
            await session.execute(text("ALTER TABLE price_series_manifests DISABLE TRIGGER USER"))
            await session.execute(text(
                "UPDATE price_series_manifests SET source = 'alpaca' WHERE adjustment = 'all'"
            ))
            await session.execute(text("ALTER TABLE price_series_manifests ENABLE TRIGGER USER"))
        else:
            await session.execute(text(
                "ALTER TABLE backtest_authority_collections DISABLE TRIGGER USER"
            ))
            await session.execute(text(
                "UPDATE backtest_authority_collections SET created_at = '9999-01-01T00:00:00Z'"
            ))
            await session.execute(text(
                "ALTER TABLE backtest_authority_collections ENABLE TRIGGER USER"
            ))
    with pytest.raises(BacktestOnceError):
        await run_once(EngineSettings(database_url=migrated_database_url), request)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(BacktestRunRecord)) == 0


@pytest.mark.asyncio
async def test_failure_after_writer_rolls_back_the_whole_graph(
    migrated_database_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = await prepare(migrated_database_url)
    writer = backtest_once.persist_backtest_result

    async def fail_after_writer(*args: object, **kwargs: object) -> BacktestRunRecord:
        await writer(*args, **kwargs)
        raise ValueError("synthetic failure before transaction commit")

    monkeypatch.setattr(backtest_once, "persist_backtest_result", fail_after_writer)
    with pytest.raises(BacktestOnceError):
        await run_once(EngineSettings(database_url=migrated_database_url), request)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        for table in ("backtest_runs", "backtest_nav_points", "backtest_fills", "backtest_metrics"):
            assert await session.scalar(text(f"SELECT count(*) FROM {table}")) == 0


@pytest.mark.asyncio
async def test_concurrent_first_requests_share_one_sealed_run(migrated_database_url: str) -> None:
    request = await prepare(migrated_database_url)
    settings = EngineSettings(database_url=migrated_database_url)
    results = await asyncio.wait_for(
        asyncio.gather(run_once(settings, request), run_once(settings, request)), timeout=30
    )
    assert len({item["run_id"] for item in results}) == 1
    assert {item["status"] for item in results} == {"BACKTEST_COMPLETED", "BACKTEST_REPLAYED"}
