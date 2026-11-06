"""Corrupt saved origin projections fail closed in scanner, refresh and read view."""

from datetime import timedelta
from hashlib import sha256

import pytest
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_monitor_disclosure_refresh_pg import setup
from nexus.api.research_disclosure_monitoring import read_disclosure_monitoring_view
from nexus.monitoring import disclosure_inheritance
from nexus.monitoring.batches import scan_stock_batch
from nexus.monitoring.disclosure_forward import SOURCE, ForwardOrigin
from nexus.monitoring.disclosure_observations import read_disclosure_observation
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan
from sqlalchemy import select


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["hash", "capture", "duplicate"])
async def test_invalid_origin_never_falls_back_or_republishes(
    migrated_database_url, monkeypatch, tmp_path, corruption
):
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path, listed_history=True
    )
    try:
        assert (await invoke())["status"] == "refreshed"
        captured["clock"][0] = AT + timedelta(minutes=10)
        await _set_test_insert_clock(factory, captured["clock"][0])
        assert (await invoke())["status"] == "refreshed"
        async with factory() as s, s.begin():
            plan = await s.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            scan = await scan_stock_batch(
                s,
                account_id=plan.account_id,
                instrument_id=plan.instrument_id,
                now=captured["clock"][0],
            )
            assert scan["false"] == 1
            run = await s.get(AgentResearchRunRecord, plan.source_run_id)
            page = await read_disclosure_monitoring_view(s, run, now=captured["clock"][0])
            assert page.latest_check.result == "false"
            saved = tuple(
                await s.scalars(select(DataSnapshot).where(DataSnapshot.source == SOURCE))
            )
            old = [(r.id, r.revision, r.payload) for r in saved]
        original = disclosure_inheritance._origins

        async def invalid(*args, **kwargs):
            rows = await original(*args, **kwargs)
            if not rows:
                return rows
            if corruption == "duplicate":
                return rows + rows
            # Transient projection only: immutable originals in the DB are untouched.
            row = DataSnapshot(
                **{c.name: getattr(rows[0], c.name) for c in DataSnapshot.__table__.columns}
            )
            if corruption == "hash":
                row.revision = "0" * 64
            else:
                row.payload = {**row.payload, "capture_hash": "0" * 64}
                row.revision = sha256(
                    ForwardOrigin.model_validate(row.payload).canonical()
                ).hexdigest()
            return (row,)

        with monkeypatch.context() as bad:
            bad.setattr(disclosure_inheritance, "_origins", invalid)
            async with factory() as s:
                view = await read_disclosure_observation(s, plan=plan, now=captured["clock"][0])
                assert (
                    view.result == "unknown" and view.reason == "SEC_INDEX_FORWARD_EVIDENCE_INVALID"
                )
                page = await read_disclosure_monitoring_view(s, run, now=captured["clock"][0])
                assert page.latest_check.result == "unknown"
                assert page.latest_check.reason == "observation_unverified"
                assert page.latest_check.comparison_from is None
            count = len(calls)
            captured["clock"][0] += timedelta(minutes=5)
            await _set_test_insert_clock(factory, captured["clock"][0])
            await invoke()
            assert len(calls) == count
        async with factory() as s:
            saved = tuple(
                await s.scalars(select(DataSnapshot).where(DataSnapshot.source == SOURCE))
            )
            assert [(r.id, r.revision, r.payload) for r in saved] == old
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()
