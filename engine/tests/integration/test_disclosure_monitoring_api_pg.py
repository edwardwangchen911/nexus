"""Normal archived filings and authenticated read-only SEC event projection."""

from datetime import timedelta

import httpx
import pytest
from helpers.monitor_materials import prepared_materials
from helpers.paper_api import app, headers
from nexus.api.research_disclosure_monitoring import read_disclosure_monitoring_view
from nexus.persistence.models.agents import AgentResearchRunRecord


@pytest.mark.asyncio
async def test_normal_two_sec_events_are_separate_from_price_and_time_filtered(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, store, captured, pointer, batch_id, _ = await prepared_materials(
        migrated_database_url, monkeypatch, tmp_path
    )
    from integration.test_monitor_view_pg import business_facts

    before_facts = await business_facts(factory)
    now = captured["clock"][0]
    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: now)
    application = app(migrated_database_url, now=now)
    path = f"/internal/v1/research-runs/{pointer.source_run_id}"
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test"
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200, response.text
        body = response.json()["data"]
        view = body["disclosure_monitoring"]
        assert view["state"] == "valid", view
        assert view["condition"]["signal"] == "sec.issuer_disclosure_added"
        assert view["trigger_count"] == 2 and len(view["triggers"]) == 2
        assert {t["batch_id"] for t in view["triggers"]} == {str(batch_id)}
        assert all(t["event_key"].startswith("sec:") for t in view["triggers"])
        assert all(
            t["event_review"] == "not_performed" and t["usage_status"] == "not_started"
            for t in view["triggers"]
        )
        assert body["monitoring"]["trigger_count"] == 1
        assert view["execution_locked"] and body["monitoring"]["execution_locked"]
        async with factory() as session:
            source = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            before = await read_disclosure_monitoring_view(
                session, source, now=now - timedelta(seconds=3)
            )
        assert before.trigger_count == 0 and before.latest_check is None
        assert await business_facts(factory) == before_facts
    finally:
        await application.state.session_factory.kw["bind"].dispose()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [False, "unresolved", "sec_unresolved", "veto", "rollback"])
async def test_native_review_proofs_not_completion_control_sec_view(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    from integration import test_monitor_mixed_increment_pg as native
    from nexus.api import research_disclosure_monitoring as reader
    from nexus.api.research_monitoring import read_monitoring_view
    from nexus.persistence.models.monitoring import MonitorMixedEventReceipt
    from sqlalchemy import select

    if mode == "sec_unresolved":
        from helpers import mixed_stage

        add_fields = mixed_stage.add_mixed_fields

        def sec_first(body, wire):
            add_fields(body, wire)
            if wire["stage"] == "research":
                proposal = next(
                    row["mixed_event_proposal"]
                    for row in body["roles"]
                    if row["role"] == "market_state"
                )
                proposal["items"].sort(key=lambda row: row["kind"] != "disclosure")
            return body

        monkeypatch.setattr(mixed_stage, "add_mixed_fields", sec_first)

    captured_fixture = []
    prepare = native.prepared_materials

    async def capture(*args, **kwargs):
        fixture = await prepare(*args, **kwargs)
        captured_fixture.append(fixture)
        return fixture

    monkeypatch.setattr(native, "prepared_materials", capture)
    await native.test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "unresolved" if mode == "sec_unresolved" else mode,
    )
    factory, _, _, captured, pointer, batch_id, _ = captured_fixture[0]
    qualified = reader.load_qualified_source
    calls = []

    async def tracked(*args, **kwargs):
        calls.append(kwargs["output_id"])
        return await qualified(*args, **kwargs)

    monkeypatch.setattr(reader, "load_qualified_source", tracked)
    try:
        async with factory() as session:
            source = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            view = await reader.read_disclosure_monitoring_view(
                session, source, now=captured["clock"][0]
            )
            assert view.trigger_count == 2
            assert all(t.batch_id == batch_id and t.usage_status == "known" for t in view.triggers)
            if mode in (False, "unresolved"):
                assert {t.event_review for t in view.triggers} == {"addressed"}, view
                assert view.state == "superseded"
                assert len(calls) == 1  # One exact qualified source for both SEC events.
                proof = await session.scalar(
                    select(MonitorMixedEventReceipt).where(
                        MonitorMixedEventReceipt.trigger_id == view.triggers[0].trigger_id
                    )
                )
                original_hash = proof.review_hash
                original_child = view.triggers[0].run_id
                # Simulate a damaged persisted projection without bypassing immutable DB guards.
                with session.no_autoflush:
                    proof.review_hash = "0" * 64
                    damaged = await reader.read_disclosure_monitoring_view(
                        session, source, now=captured["clock"][0]
                    )
                    item = next(t for t in damaged.triggers if t.trigger_id == proof.trigger_id)
                    assert (item.status, item.event_review, item.run_id, item.usage_status) == (
                        "unknown",
                        "unverified",
                        original_child,
                        "known",
                    )
                    proof.review_hash = original_hash
                if mode == "unresolved":
                    price = await read_monitoring_view(session, source, now=captured["clock"][0])
                    assert price.coverage_reason == "events_unresolved"
            elif mode == "sec_unresolved":
                assert {t.event_review for t in view.triggers} == {"addressed", "unresolved"}
                assert view.state == "withdrawn" and view.coverage_reason == "events_unresolved"
                assert len(calls) == 1
            else:
                assert {t.event_review for t in view.triggers} == {"unverified"}
                assert view.state == "withdrawn"
                assert view.coverage_reason == (
                    "review_unverified" if mode == "veto" else "replacement_rejected"
                )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_daily_sec_proof_overrides_only_when_original_proof_valid(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration import test_monitor_daily_v2_native_pg as native
    from nexus.api import research_disclosure_monitoring as reader
    from nexus.persistence.models.monitoring import MonitorDailyEventReceipt
    from sqlalchemy import select

    fixtures = []
    prepare = native.unresolved_then_daily

    async def captured(*args, **kwargs):
        result = await prepare(*args, **kwargs)
        fixtures.append(result)
        return result

    monkeypatch.setattr(native, "unresolved_then_daily", captured)
    await native.test_new_daily_native_consumes_only_qualified_historical_events(
        migrated_database_url, monkeypatch, tmp_path, "addressed"
    )
    factory, _, pointer, _, _, old_run, _, now, _, _ = fixtures[0]
    try:
        async with factory() as session:
            parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            view = await reader.read_disclosure_monitoring_view(session, parent, now=now)
            assert view.trigger_count == 2
            assert all(
                t.status == "covered"
                and t.event_review == "addressed"
                and t.usage_status == "known"
                and t.run_id != old_run
                for t in view.triggers
            )
            target = view.triggers[0]
            proof = await session.scalar(
                select(MonitorDailyEventReceipt).where(
                    MonitorDailyEventReceipt.trigger_id == target.trigger_id
                )
            )
            validate = reader.validate_daily_event_receipt

            async def damaged(session, receipt_id):
                return False if receipt_id == proof.id else await validate(session, receipt_id)

            monkeypatch.setattr(reader, "validate_daily_event_receipt", damaged)
            broken = await reader.read_disclosure_monitoring_view(session, parent, now=now)
            damaged_item = next(t for t in broken.triggers if t.trigger_id == target.trigger_id)
            assert (
                damaged_item.status,
                damaged_item.event_review,
                damaged_item.run_id,
                damaged_item.usage_status,
            ) == ("unknown", "unverified", old_run, "known")
            assert sum(t.status == "covered" for t in broken.triggers) == 1
    finally:
        await factory.kw["bind"].dispose()
