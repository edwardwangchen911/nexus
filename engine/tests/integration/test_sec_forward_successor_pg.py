"""Normal archived sources, SDK substitute, settlement and atomic SEC successor."""

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from integration import test_monitor_mixed_increment_pg as mixed_increment
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_sec_forward_origin_pg import listed
from nexus.monitoring.batches import scan_stock_batch
from nexus.monitoring.disclosure_forward import SOURCE, ForwardObservation, read_origin
from nexus.monitoring.disclosure_observations import read_disclosure_observation
from nexus.persistence.models.agents import AgentResearchOutputRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan, MonitorTrigger
from nexus.persistence.models.runtime import OutboxEvent
from sqlalchemy import func, select


async def install_child(factory, captured, child_id, *, predecessor=None, rollback=False):
    from nexus.monitoring.disclosure_installation import _install_mixed_or_legacy_disclosure

    async with factory() as s, s.begin():
        output_id = await s.scalar(
            select(AgentResearchOutputRecord.id).where(
                AgentResearchOutputRecord.run_id == child_id,
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status == "completed",
            )
        )
        child = await _install_mixed_or_legacy_disclosure(
            s,
            account_id=captured["account_id"],
            output_id=output_id,
            expected_predecessor_id=predecessor or captured["forward_prior_id"],
            now=captured["clock"][0],
        )
        origin = await read_origin(s, child, captured["clock"][0])
        assert origin is not None
        if rollback:
            raise RuntimeError("controlled after inheritance, before commit")
        return child.id, origin[0].id


async def assert_installation_atomic(factory, captured, child_id):
    with pytest.raises(ValueError, match="MONITOR_PLAN_CONFLICT"):
        await install_child(factory, captured, child_id, predecessor=UUID(int=1))
    with pytest.raises(RuntimeError, match="controlled after inheritance"):
        await install_child(factory, captured, child_id, rollback=True)
    async with factory() as s:
        assert (
            await s.scalar(select(MonitorPlan.id).where(MonitorPlan.source_run_id == child_id))
            is None
        )
        assert (
            await s.scalar(
                select(func.count()).select_from(DataSnapshot).where(DataSnapshot.source == SOURCE)
            )
            == 1
        )


async def verify_successor(factory, settings, store, captured, root, child_id, monkeypatch):
    async with factory() as s:
        successor = await s.scalar(
            select(MonitorPlan).where(
                MonitorPlan.source_run_id == child_id, MonitorPlan.kind == "disclosure"
            )
        )
        assert successor is not None
        assert successor.predecessor_id == captured["forward_prior_id"]
        inherited = await read_origin(s, successor, captured["clock"][0])
        assert inherited is not None, "normal replacement must atomically inherit comparison origin"
        row, value, baseline, first = inherited
        assert row.id != captured["forward_origin_id"]
        assert value.capture_id == captured["forward_first_id"]
        assert value.effective_at == captured["forward_comparison_from"]
        assert (
            value.baseline_snapshot_id != successor.baseline_snapshot_id
            or value.baseline_as_of != successor.baseline_as_of
        )
        view = await read_disclosure_observation(s, plan=successor, now=captured["clock"][0])
        assert isinstance(view, ForwardObservation) and view.result == "true"
        assert any(e.accession_number.endswith("20-000009") for e in view.added)
        assert view.comparison_from == captured["forward_comparison_from"]
        old_origin = await s.get(DataSnapshot, captured["forward_origin_id"])
        assert old_origin.revision == captured["forward_origin_hash"]
        before = {
            m: await s.scalar(select(func.count()).select_from(m))
            for m in (MonitorTrigger, OutboxEvent)
        }

    twins = await asyncio.gather(*(install_child(factory, captured, child_id) for _ in range(2)))
    assert twins == [(successor.id, row.id)] * 2
    captured["clock"][0] += timedelta(seconds=1)
    await listed(
        settings, store, factory, captured, root, "successor-late", recent_added=2, late=True
    )
    async with factory() as s, s.begin():
        view = await read_disclosure_observation(s, plan=successor, now=captured["clock"][0])
        assert isinstance(view, ForwardObservation)
        assert view.result == "true" and len(view.added) == 3
        assert view.comparison_from == captured["forward_comparison_from"]
        assert view.old_history_gap == "unknown"
        scanned = await scan_stock_batch(
            s,
            account_id=successor.account_id,
            instrument_id=successor.instrument_id,
            now=captured["clock"][0],
        )
        assert scanned["new_triggers"] == scanned["notifications"] == 1
    captured["clock"][0] += timedelta(seconds=1)
    await _set_test_insert_clock(factory, captured["clock"][0])
    async with factory() as s, s.begin():
        scanned = await scan_stock_batch(
            s,
            account_id=successor.account_id,
            instrument_id=successor.instrument_id,
            now=captured["clock"][0],
        )
        assert scanned["new_triggers"] == scanned["notifications"] == 0
        assert (
            await s.scalar(
                select(func.count()).select_from(DataSnapshot).where(DataSnapshot.source == SOURCE)
            )
            == 2
        )
        for model, count in before.items():
            assert await s.scalar(select(func.count()).select_from(model)) == count + 1

    import os

    if os.environ.get("NEXUS_TEST_SEC_FORMAL_IMAGE"):
        from helpers.sec_forward_images import verify_images

        await verify_images(factory, store, captured, successor)
    await verify_successor_page(factory, captured, successor, monkeypatch)
    await assert_invalid_inheritance(factory, captured, successor, child_id, monkeypatch)

    captured["clock"][0] += timedelta(seconds=1)
    # The stable accession is unchanged but its verified metadata now conflicts.
    # Cross-plan dedup must reject it, not silently call it an already handled event.
    prior_form = next(e.form for e in view.added if e.accession_number.endswith("20-000009"))
    await listed(
        settings,
        store,
        factory,
        captured,
        root,
        "conflicting-late-event",
        recent_added=2,
        late=True,
        late_form="8-K/A" if prior_form == "8-K" else "8-K",
    )
    with pytest.raises(ValueError, match="MONITOR_DISCLOSURE_IDENTITY_CONFLICT"):
        async with factory() as s, s.begin():
            await scan_stock_batch(
                s,
                account_id=successor.account_id,
                instrument_id=successor.instrument_id,
                now=captured["clock"][0],
            )
    async with factory() as s:
        for model, count in before.items():
            assert await s.scalar(select(func.count()).select_from(model)) == count + 1


async def assert_invalid_inheritance(factory, captured, successor, child_id, monkeypatch):
    from hashlib import sha256

    from nexus.api.research_disclosure_monitoring import read_disclosure_monitoring_view
    from nexus.monitoring import disclosure_inheritance as inheritance
    from nexus.persistence.models.agents import AgentResearchRunRecord

    original = inheritance._origins

    # A self-consistent new root must not bypass the prior comparison origin.
    async def reset_root(session, plan, now):
        from nexus.monitoring.disclosure_forward import POLICY, ForwardOrigin
        from nexus.monitoring.disclosure_observations import _bound_proposal
        from nexus.research.sec_listed_storage import SOURCE as LISTED
        from nexus.research.sec_listed_storage import ListedCapture

        rows = await original(session, plan, now)
        if plan.id != successor.id:
            return rows
        proposal = await _bound_proposal(session, plan, now)
        current = await session.scalar(
            select(DataSnapshot)
            .where(DataSnapshot.source == LISTED)
            .order_by(DataSnapshot.available_at.desc())
            .limit(1)
        )
        value = ForwardOrigin(
            plan_id=plan.id,
            account_id=plan.account_id,
            instrument_id=plan.instrument_id,
            source_hash=plan.source_hash,
            condition_hash=plan.condition_hash,
            baseline_snapshot_id=proposal.baseline_snapshot_id,
            baseline_hash=proposal.baseline_hash,
            baseline_as_of=proposal.baseline_as_of,
            capture_id=current.id,
            capture_hash=current.revision,
            original_hashes=tuple(
                x.sha256 for x in ListedCapture.model_validate(current.payload).originals
            ),
            effective_at=now,
        )
        row = DataSnapshot(
            **{c.name: getattr(rows[0], c.name) for c in DataSnapshot.__table__.columns}
        )
        row.schema_version = POLICY
        row.payload = value.model_dump(mode="json")
        row.revision = sha256(value.canonical()).hexdigest()
        row.created_at = row.available_at = row.observed_at = row.received_at = now
        return (row,)

    with monkeypatch.context() as reset:
        reset.setattr(inheritance, "_origins", reset_root)
        async with factory() as s:
            with pytest.raises(ValueError, match="SEC_INDEX_FORWARD_ORIGIN_INVALID"):
                await inheritance.read_binding(s, successor, captured["clock"][0])

    # Having a predecessor alone must not prohibit a first comparison origin.
    # This reader projection keeps the normal installed chain and valid capture,
    # but represents ancestors that had no forward origin at publication time.
    async def first_root(session, plan, now):
        return await reset_root(session, plan, now) if plan.id == successor.id else ()

    with monkeypatch.context() as first:
        first.setattr(inheritance, "_origins", first_root)
        async with factory() as s:
            binding = await inheritance.read_binding(s, successor, captured["clock"][0])
            assert binding is not None
            assert binding[2].id == successor.id
            assert binding[3].effective_at == captured["clock"][0]
    for field in (
        "predecessor_origin_hash",
        "root_origin_hash",
        "issuer_cik",
        "predecessor_plan_id",
    ):

        async def invalid(session, plan, now):
            rows = await original(session, plan, now)
            if plan.id != successor.id:
                return rows
            row = DataSnapshot(
                **{c.name: getattr(rows[0], c.name) for c in DataSnapshot.__table__.columns}
            )
            replacement = (
                "0000000001"
                if field == "issuer_cik"
                else str(UUID(int=1))
                if field == "predecessor_plan_id"
                else "0" * 64
            )
            row.payload = {**row.payload, field: replacement}
            row.revision = sha256(
                inheritance.ForwardInheritance.model_validate(row.payload).canonical()
            ).hexdigest()
            return (row,)

        with monkeypatch.context() as bad:
            bad.setattr(inheritance, "_origins", invalid)
            async with factory() as s:
                view = await read_disclosure_observation(
                    s, plan=successor, now=captured["clock"][0]
                )
                assert (
                    view.result == "unknown" and view.reason == "SEC_INDEX_FORWARD_EVIDENCE_INVALID"
                )
                child = await s.get(AgentResearchRunRecord, child_id)
                page = await read_disclosure_monitoring_view(s, child, now=captured["clock"][0])
                assert (
                    page.latest_check.result == "unknown"
                    and page.latest_check.comparison_from is None
                )
            with pytest.raises(ValueError, match="SEC_INDEX_FORWARD_ORIGIN_INVALID"):
                await install_child(factory, captured, child_id)
        async with factory() as s:
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(DataSnapshot)
                    .where(DataSnapshot.source == SOURCE)
                )
                == 2
            )


async def verify_successor_page(factory, captured, successor, monkeypatch):
    import json
    import os
    from pathlib import Path

    import httpx
    from helpers.monitoring_readview_browser import stage_facts, verify_monitoring_browser
    from helpers.paper_api import app, headers
    from nexus.api.research_disclosure_monitoring import read_disclosure_monitoring_view
    from nexus.persistence.models.agents import AgentResearchRunRecord

    now = captured["clock"][0]
    async with factory() as s:
        child = await s.get(AgentResearchRunRecord, successor.source_run_id)
        view = await read_disclosure_monitoring_view(s, child, now=now)
        assert view.latest_check.comparison_from == captured["forward_comparison_from"]
        assert view.latest_check.prior_history_unknown is True
        assert view.latest_check.prior_history_from < successor.baseline_as_of
        assert view.latest_check.result == "true"
    destination = os.environ.get("NEXUS_TEST_FORWARD_SUCCESSOR_BROWSER_DIR")
    if not destination:
        return
    destination = Path(destination).resolve()
    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: now)
    application = app(factory.kw["bind"].url.render_as_string(hide_password=False), now=now)
    path = f"/internal/v1/research-runs/{successor.source_run_id}"
    async with factory() as s:
        protected = await stage_facts(s)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200, response.text
    saved = response.json()["data"]["disclosure_monitoring"]
    assert (
        saved["latest_check"]["comparison_from"]
        == view.model_dump(mode="json")["latest_check"]["comparison_from"]
    )
    assert saved["latest_check"]["prior_history_unknown"] is True
    assert saved["trigger_count"] == 1
    destination.mkdir(parents=True, exist_ok=False)
    (destination / "ready.json").write_text(
        json.dumps(
            dict(
                run_id=str(successor.source_run_id),
                instrument_id=str(successor.instrument_id),
                expected_result="true",
                expected_event_count=1,
                response=response.json(),
            )
        )
    )
    await verify_monitoring_browser(
        application, directory=destination, spec_path="tests/e2e/sec-forward-readview.spec.ts"
    )
    async with factory() as s:
        assert await stage_facts(s) == protected


@pytest.mark.asyncio
async def test_normal_single_stock_successor_keeps_forward_history(
    migrated_database_url, monkeypatch, tmp_path
):
    normal = (
        mixed_increment.test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls
    )
    await normal(migrated_database_url, monkeypatch, tmp_path, "sec_only", forward=True)
