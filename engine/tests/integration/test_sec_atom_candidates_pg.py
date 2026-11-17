"""Normal source entry owns one bucket; hints never create paid or notice effects."""

import argparse
import asyncio
import json
import traceback
from datetime import datetime, timedelta
from uuid import UUID

import httpx
import pytest
from helpers.database_clock import is_fixture_clock_read
from helpers.durable_test_clock import DisposableClock
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_monitor_disclosure_refresh_pg import setup
from nexus.monitoring import durable_sec_storage as durable
from nexus.ops import research_monitor
from nexus.ops import sec_atom_candidates as atom
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pause_during_read,cross_bucket", [(False, False), (True, False), (False, True)]
)
async def test_normal_candidate_entry_custody_once_and_no_effects(
    migrated_database_url, monkeypatch, tmp_path, pause_during_read, cross_bucket
):
    factory, store, captured, pointer, source_calls, client, _ = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    monkeypatch.setattr(atom, "configured_exhibit_store", lambda session: store)
    monkeypatch.setattr(durable, "configured_exhibit_store", lambda session: store)
    monkeypatch.setattr(
        "nexus.monitoring.durable_sec_host.configured_exhibit_store", lambda session: store
    )
    normal_scalar = AsyncSession.scalar

    async def synchronized_clock(session, statement, *args, **kwargs):
        if is_fixture_clock_read(statement):
            return captured["clock"][0]
        return await normal_scalar(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", synchronized_clock)
    controller = DisposableClock(migrated_database_url)
    try:
        async with factory() as s, s.begin():
            await controller.set(s, captured["clock"][0], running=False)
            plan = await s.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            rule = await durable.enable_rule(s, account_id=pointer.account_id, plan_id=plan.id)
            run = await s.get(AgentResearchRunRecord, pointer.source_run_id)
            cycle = await s.scalar(
                select(AgentPaperCycleRecord)
                .where(AgentPaperCycleRecord.account_id == pointer.account_id)
                .order_by(AgentPaperCycleRecord.created_at.desc())
                .limit(1)
            )
            counts = [
                await s.scalar(text(f"SELECT count(*) FROM {t}"))
                for t in (
                    "agent_research_runs",
                    "jobs",
                    "outbox_events",
                    "agent_daily_budget_reservations",
                    "sec_discovery_events",
                )
            ]
        settings = EngineSettings(
            database_url=migrated_database_url,
            object_store_endpoint="http://127.0.0.1:9000",
            agent_paper_cycle_enabled=True,
            agent_paper_account_id=pointer.account_id,
            agent_paper_candidate_batch_id=cycle.candidate_batch_id,
            agent_paper_formal_model_config_id=run.model_config_id,
        )
        captured["clock"][0] = max(captured["clock"][0], rule.effective_at) + timedelta(seconds=2)
        await _set_test_insert_clock(factory, captured["clock"][0])
        async with factory() as s, s.begin():
            await controller.set(s, captured["clock"][0], running=False)

        class ClockMeta(type):
            def __instancecheck__(cls, obj):
                return isinstance(obj, datetime)

        class Clock(datetime, metaclass=ClockMeta):
            @classmethod
            def now(cls, tz=None):
                return captured["clock"][0].astimezone(tz)

        monkeypatch.setattr(atom, "datetime", Clock)
        from nexus.ops import durable_sec_index_local as local_index
        from nexus.ops.monitor_disclosure_refresh import refresh_disclosure_observations
        from nexus.research.durable_sec_index_storage import read_durable_index_capture

        monkeypatch.setattr(local_index, "_store", lambda settings, supplied: store)
        real_collect = local_index.collect_durable_index

        async def diagnose_collect(*args, **kwargs):
            try:
                return await real_collect(*args, **kwargs)
            except Exception as error:
                captured["index_error"] = (type(error).__name__, str(error), traceback.format_exc())
                raise

        monkeypatch.setattr(local_index, "collect_durable_index", diagnose_collect)
        await local_index.import_durable_index(
            settings,
            identity_snapshot_id=captured["identity"].snapshot_id,
            archive_dir=tmp_path / "baseline",
            store=store,
        )
        calls = []

        async def request(req):
            calls.append(str(req.url))
            assert req.url.host == "www.sec.gov" and req.url.params["CIK"] == rule.issuer_cik
            if pause_during_read:
                async with factory() as s, s.begin():
                    await durable.change_state(
                        s, account_id=rule.account_id, rule_id=rule.id, state="paused"
                    )
            at = (captured["clock"][0] - timedelta(seconds=1)).isoformat()
            xml = f"""<feed xmlns="http://www.w3.org/2005/Atom"><company-info><cik>{rule.issuer_cik}</cik></company-info><updated>{at}</updated><entry><content><accession-number>0001193125-26-000099</accession-number><filing-type>8-K</filing-type><filing-date>{captured["clock"][0].date()}</filing-date><filing-href>https://www.sec.gov/Archives/edgar/data/{int(rule.issuer_cik)}/000119312526000099/0001193125-26-000099-index.htm</filing-href></content><updated>{at}</updated></entry></feed>"""
            return httpx.Response(200, content=xml.encode())

        monkeypatch.setattr(
            atom, "_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(request))
        )
        args = argparse.Namespace(
            action="observe-sec-candidates",
            repo_root=tmp_path,
            calendar_authority_id=CALENDAR_ID,
            _observation_environment={"SEC_CONTACT": "Synthetic test test@example.invalid"},
        )
        if cross_bucket:
            normal_identity = atom.current_identity

            async def late_identity(*args, **kwargs):
                result = await normal_identity(*args, **kwargs)
                captured["clock"][0] += timedelta(minutes=5)
                async with factory() as clock_db, clock_db.begin():
                    await controller.set(clock_db, captured["clock"][0], running=False)
                return result

            monkeypatch.setattr(atom, "current_identity", late_identity)
            with pytest.raises(ValueError, match="MONITOR_REFRESH_BUCKET_UNAVAILABLE"):
                await research_monitor.run(settings, args)
            assert calls == []
            async with factory() as s:
                assert (
                    await s.scalar(
                        text("SELECT count(*) FROM audit_events WHERE action=:action"),
                        {"action": atom.CLAIM},
                    )
                    == 0
                )
            return
        result = await research_monitor.run(settings, args)
        assert result["negative_conclusion_authorized"] is False
        assert len(calls) == 1
        if pause_during_read:
            assert result["items"][0]["status"] == "unknown"
        else:
            item = result["items"][0]
            assert item["candidate_hints"] == ["0001193125-26-000099"]
            read = await research_monitor.run(
                settings,
                argparse.Namespace(
                    action="inspect-sec-candidates", snapshot_id=UUID(item["snapshot_id"])
                ),
            )
            assert read["status"] == "verified" and read["primary_confirmation_required"]
            assert read["negative_conclusion_authorized"] is False
        repeated = await research_monitor.run(settings, args)
        assert repeated["items"] == [] and len(calls) == 1
        async with factory() as s:
            after = [
                await s.scalar(text(f"SELECT count(*) FROM {t}"))
                for t in (
                    "agent_research_runs",
                    "jobs",
                    "outbox_events",
                    "agent_daily_budget_reservations",
                    "sec_discovery_events",
                )
            ]
            assert after == counts
        if not pause_during_read:
            kwargs = dict(
                account_id=pointer.account_id,
                calendar_authority_id=CALENDAR_ID,
                scheduled_at=atom._bucket(captured["clock"][0]),
                environment={"SEC_CONTACT": "Synthetic test test@example.invalid"},
                repo_root=tmp_path,
            )
            # The unchanged recent B would normally be reused without a new GET.
            before_requests = len(source_calls)
            reused = await refresh_disclosure_observations(settings, **kwargs)
            assert reused["status"] == "unchanged" and len(source_calls) == before_requests
            # A newly visible official hint defeats reuse, not the normal bucket once gate.
            path = tmp_path / "baseline/AAPL-submissions.json"
            document = json.loads(path.read_bytes())
            rows = document["filings"]["recent"]
            for key, value in dict(
                accessionNumber="0001193125-26-000099",
                form="8-K",
                acceptanceDateTime=(captured["clock"][0] - timedelta(seconds=1)).isoformat(),
                filingDate=captured["clock"][0].date().isoformat(),
                primaryDocument="new.htm",
            ).items():
                rows[key].append(value)
            path.write_text(json.dumps(document))
            fresh = await refresh_disclosure_observations(
                settings,
                **kwargs,
                candidate_hints={rule.instrument_id: tuple(item["candidate_hints"])},
            )
            assert fresh["status"] == "refreshed" and len(source_calls) > before_requests, (
                fresh,
                captured.get("index_error"),
            )
            snapshot_id = UUID(fresh["items"][0]["snapshot_id"])
            async with factory() as s:
                _, capture = await read_durable_index_capture(
                    s, store, snapshot_id=snapshot_id, as_of=captured["clock"][0]
                )
                assert "0001193125-26-000099" in {e.accession_number for e in capture.index.entries}
            held = await refresh_disclosure_observations(
                settings, **kwargs, candidate_hints={rule.instrument_id: ("0001193125-26-000100",)}
            )
            assert held["items"] == [{"status": "already_claimed"}]
            from nexus.ops import monitor_disclosure_refresh, monitor_price_refresh

            cancelled = asyncio.Event()
            primary_calls = []

            async def slow_optional(*args, **kwargs):
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

            async def primary(settings, **kwargs):
                primary_calls.append(kwargs)
                return {"status": "blocked", "provider_requests": 0}

            async def no_reconcile(*args, **kwargs):
                return {"recorded": 0}

            monkeypatch.setattr(atom, "refresh_candidates", slow_optional)
            monkeypatch.setattr(
                monitor_disclosure_refresh, "refresh_disclosure_observations", primary
            )
            monkeypatch.setattr(monitor_price_refresh, "refresh_price_observations", primary)
            monkeypatch.setattr(research_monitor, "run", no_reconcile)
            observed = await research_monitor.refresh_configured_observations(
                settings,
                calendar_authority_id=CALENDAR_ID,
                scheduled_at=kwargs["scheduled_at"],
                environment=kwargs["environment"],
                repo_root=tmp_path,
            )
            assert cancelled.is_set() and len(primary_calls) == 2
            assert observed["official_candidates"]["status"] == "unknown"

    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()
