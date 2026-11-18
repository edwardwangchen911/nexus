"""One foundation SEC event traverses ordinary native daily research and receipts."""

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID, uuid4

import httpx
import pytest
from helpers.database_clock import is_fixture_clock_read
from helpers.durable_session_inputs import (
    append_session_fresh_inputs,
)
from helpers.durable_test_clock import DisposableClock
from helpers.paper_batch import CALENDAR_ID
from integration import test_candidate_multistock_path as candidate
from integration import test_monitor_batches_pg as batches
from integration import test_monitor_disclosure_refresh_pg as refresh
from integration import test_monitor_filing_content_pg as filing
from nexus.monitoring import durable_sec_research as bridge
from nexus.monitoring import durable_sec_storage as durable
from nexus.ops.research_monitor import run
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import (
    MonitorIncrement,
    MonitorPlan,
)
from nexus.persistence.models.runtime import Job, OutboxEvent
from nexus.persistence.models.sec_monitor import SecDiscoveryEvent, SecNotificationClaim
from nexus.persistence.models.sec_research import (
    SecEventResearchBinding,
)
from nexus.research.durable_sec_index_storage import persist_durable_index_archive
from nexus.settings import AgentRuntimeMode
from pydantic import SecretStr
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resolution", ("addressed", "unresolved", "absent", "issue", "adopted_graphic")
)
async def test_unbound_foundation_normal_sol_daily_addressed_receipt(
    migrated_database_url, monkeypatch, tmp_path, resolution
):
    adopted_graphic = resolution == "adopted_graphic"
    if adopted_graphic:
        resolution = "addressed"
    from helpers import disclosure_observations as reader

    async def forbidden_real_http(*args, **kwargs):
        raise AssertionError("Only controlled local MockTransport is permitted")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden_real_http)

    at = datetime(2026, 11, 11, 15, tzinfo=UTC)
    sessions = (datetime(2026, 11, 12, 15, tzinfo=UTC), datetime(2026, 11, 13, 15, tzinfo=UTC))
    assert all(t < datetime.now(UTC) and t.weekday() < 5 for t in sessions)
    controller = DisposableClock(migrated_database_url)
    url = controller.uncached_url(migrated_database_url)
    setup = create_async_engine(url)
    async with async_sessionmaker(setup).begin() as db:
        await controller.set(db, at.replace(hour=21) - timedelta(days=1, seconds=30), running=False)
    await setup.dispose()
    scalar = AsyncSession.scalar
    for module in (candidate, batches, refresh, filing, reader):
        monkeypatch.setattr(module, "AT", at)
    for module in tuple(sys.modules.values()):
        if module is not None and getattr(module, "__name__", "").startswith(
            ("integration.", "helpers.")
        ):
            old = getattr(module, "_set_test_insert_clock", None)
            if old is not None:

                async def synchronized(factory, at, _old=old):
                    await _old(factory, at)
                    async with factory.begin() as db:
                        await controller.set(db, at, running=False, refresh_defaults=True)

                monkeypatch.setattr(module, "_set_test_insert_clock", synchronized)
    from nexus import worker
    from nexus.persistence.session import session_factory

    original_worker = worker.run_agent_worker_once

    async def synchronized_worker(settings, *args, **kwargs):
        temporary = session_factory(settings)
        try:
            async with temporary() as db, db.begin():
                await controller.set(db, worker.datetime.now(UTC), running=True)
        finally:
            await temporary.kw["bind"].dispose()
        return await original_worker(settings, *args, **kwargs)

    for module in tuple(sys.modules.values()):
        if module is not None and getattr(module, "__name__", "").startswith(
            ("integration.", "helpers.")
        ):
            if getattr(module, "run_agent_worker_once", None) is original_worker:
                monkeypatch.setattr(module, "run_agent_worker_once", synchronized_worker)

    async def dbclock(self, statement, *args, **kwargs):
        if is_fixture_clock_read(statement):
            return controller.now()
        return await scalar(self, statement, *args, **kwargs)

    from nexus.paper import research_request

    normal_prepare = research_request.prepare_research_request

    async def synchronized_prepare(db, settings, **kwargs):
        from nexus.research import current_identity_storage, observation_identity_storage

        monkeypatch.setattr(current_identity_storage, "func", func)
        monkeypatch.setattr(observation_identity_storage, "func", func)
        async with session_factory(settings).begin() as setup_db:
            await controller.set(setup_db, worker.datetime.now(UTC), running=True)
        monkeypatch.setattr(AsyncSession, "scalar", dbclock)
        return await normal_prepare(db, settings, **kwargs)

    monkeypatch.setattr(research_request, "prepare_research_request", synchronized_prepare)
    factory, settings, store, captured, pointer = await batches.installed(
        url,
        monkeypatch,
        tmp_path,
        kinds=("disclosure",),
        independent=True,
    )

    monkeypatch.setattr(AsyncSession, "scalar", dbclock)

    class WorkerClock(datetime):
        @classmethod
        def now(cls, tz=None):
            t = controller.now()
            return t if tz is None else t.astimezone(tz)

    monkeypatch.setattr("nexus.worker.datetime", WorkerClock)
    for module in (durable, bridge):
        monkeypatch.setattr(module, "configured_exhibit_store", lambda _: store)
    monkeypatch.setattr("nexus.monitoring.filing_archive.configured_exhibit_store", lambda _: store)
    from integration.test_sol_native_stage_pg import install_sol_http
    from integration.test_sol_profile_pg import current_record, register
    from nexus.agents.research_stages.proof import read_terminal_proof
    from nexus.monitoring import durable_sec_host as host
    from nexus.monitoring import filing_archive
    from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
    from nexus.paper.agent_cycle_authority import load_quasi_realtime_cycle_authority
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
    from nexus.persistence.models.sec_daily import SecDurableDailyEventReceipt

    try:
        monkeypatch.setattr(host, "configured_exhibit_store", lambda _: store)
        async with factory.begin() as db:
            plan = await db.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            rule = await durable.enable_rule(db, account_id=pointer.account_id, plan_id=plan.id)
        day = at + timedelta(minutes=20)
        captured["clock"][0] = day
        async with factory.begin() as db:
            await controller.set(db, day, running=False, refresh_defaults=True)
        async with factory() as read_batch:
            from nexus.paper.candidate_batch import load_agent_paper_candidate_batch

            candidate_scope = await load_agent_paper_candidate_batch(
                read_batch, batch_id=settings.agent_paper_candidate_batch_id
            )
        for instrument in candidate_scope.instrument_ids:
            await append_session_fresh_inputs(factory, instrument, day, decision_session=day.date())
        price = await current_record(factory)
        model_id = await register(factory, price)
        settings = settings.model_copy(
            update={
                "agent_paper_formal_model_config_id": model_id,
                "agent_runtime_mode": AgentRuntimeMode.RESPONSES,
                "openai_api_key": SecretStr("sk-proj-" + "synthetic-only" * 5),
                "openai_extraction_model": "gpt-6.1-sol",
                "openai_reasoning_model": "gpt-6.1-sol",
                "agent_daily_budget_usd": Decimal("13"),
                "agent_paper_cycle_enabled": True,
            }
        )
        entry = dict(
            accession_number="0000123456-26-000097",
            form="8-K",
            accepted_at=day.isoformat(),
            filed_at=day.replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
            primary_document="foundation.htm",
        )
        document = json.loads((tmp_path / "baseline/AAPL-submissions.json").read_bytes())
        document["filings"]["recent"] = {
            "accessionNumber": [entry["accession_number"]],
            "form": ["8-K"],
            "acceptanceDateTime": [day.isoformat()],
            "filingDate": [day.date().isoformat()],
            "primaryDocument": ["foundation.htm"],
        }
        if resolution == "issue":
            recent = document["filings"]["recent"]
            for key, value in {
                "accessionNumber": "0000123456-26-000098",
                "form": "8-K",
                "acceptanceDateTime": day.isoformat(),
                "filingDate": day.date().isoformat(),
                "primaryDocument": "unavailable.htm",
            }.items():
                recent[key].append(value)
        directory = (tmp_path / "baseline/directory.json").read_bytes()
        raw = json.dumps(document).encode()

        def receipt(body, url):
            return json.dumps(
                dict(
                    source_url=url,
                    received_at=day.isoformat(),
                    sha256=sha256(body).hexdigest(),
                    byte_length=len(body),
                    http_status=200,
                )
            ).encode()

        async with factory.begin() as db:
            capture, _ = await persist_durable_index_archive(
                db,
                store,
                identity_snapshot_id=rule.identity_snapshot_id,
                directory=(
                    directory,
                    receipt(directory, "https://www.sec.gov/files/company_tickers_exchange.json"),
                ),
                submissions=(
                    raw,
                    receipt(raw, f"https://data.sec.gov/submissions/CIK{rule.issuer_cik}.json"),
                ),
                now=day,
            )
        async with factory.begin() as db:
            discovered = await durable.record_discovery(
                db,
                account_id=pointer.account_id,
                rule_id=rule.id,
                capture_id=capture.id,
                scheduled_at=day.replace(minute=day.minute // 5 * 5),
            )
            assert discovered.result == "true", discovered.reason
            event = await db.scalar(
                select(SecDiscoveryEvent).where(
                    SecDiscoveryEvent.rule_id == rule.id,
                    SecDiscoveryEvent.event_key
                    == f"sec:{rule.issuer_cik}:{entry['accession_number']}",
                )
            )
        if resolution == "addressed":
            # Rejected before FK/audit completion; no fake receipt is persisted.
            insert_attempt = text("""INSERT INTO sec_durable_daily_event_receipts
                (id,event_id,cycle_id,run_id,output_id,snapshot_id,context_hash,
                 proposal_hash,review_hash,created_at,observed_at)
                SELECT :id,:event,:scope,:scope,:scope,:scope,
                 repeat('a',64),repeat('a',64),repeat('a',64),
                 clock_timestamp()-CAST(:lag AS interval),clock_timestamp()-CAST(:lag AS interval)
            """)
            for isolation, lag, code in (
                ("REPEATABLE READ", timedelta(0), "SEC_DAILY_ISOLATION_INVALID"),
                ("READ COMMITTED", timedelta(days=1), "SEC_DAILY_TIME_INVALID"),
            ):
                async with factory.kw["bind"].connect() as connection:
                    connection = await connection.execution_options(isolation_level=isolation)
                    async with connection.begin():
                        with pytest.raises(DBAPIError) as rejected:
                            await connection.execute(
                                insert_attempt,
                                {
                                    "id": uuid4(),
                                    "event": event.id,
                                    "scope": uuid4(),
                                    "lag": lag,
                                },
                            )
                        assert rejected.value.orig.sqlstate == "23514"
                        assert code in str(rejected.value)
        monkeypatch.setattr(filing, "AT", day - timedelta(seconds=8))
        if adopted_graphic:
            from helpers.filing_relations import assert_relation_cli

            imported = await assert_relation_cli(
                factory,
                settings,
                store,
                str(capture.id),
                tmp_path / "graphic-originals",
                monkeypatch,
                target_accession=entry["accession_number"],
                keep_policy=True,
            )
        else:
            imported = await run(
                settings,
                argparse.Namespace(
                    action="import-content",
                    index_snapshot_id=capture.id,
                    accession=entry["accession_number"],
                    archive_dir=filing.archive(tmp_path / "foundation-content", event.entry),
                ),
            )
        async with factory() as db:
            assert (
                await filing_archive.read_filing_content(
                    db, snapshot_id=UUID(imported["snapshot_id"]), as_of=day
                )
                is not None
            ), "FROZEN_FILING_READER_REFUSED"
            await host.current_identity(db, rule, day)
            assert not tuple(await db.scalars(select(SecEventResearchBinding)))
            assert not tuple(await db.scalars(select(SecDurableDailyEventReceipt)))
            batch = await load_agent_paper_candidate_batch(
                db, batch_id=settings.agent_paper_candidate_batch_id
            )
            notices = await db.scalar(select(func.count()).select_from(OutboxEvent))
            claims = await db.scalar(select(func.count()).select_from(SecNotificationClaim))
        async with factory.begin() as db:
            authority = await load_quasi_realtime_cycle_authority(
                db,
                account_id=pointer.account_id,
                now=day,
                expected_calendar_authority_id=CALENDAR_ID,
                expected_universe_snapshot_id=batch.universe_snapshot_id,
            )
            cycle = await start_schwab_agent_paper_cycle(
                db,
                authority=authority,
                decision_slot_at=day.replace(second=0, microsecond=0),
                idempotency_key="foundation-daily-normal-native",
                requested_instrument_ids=batch.instrument_ids,
                candidate_batch_id=batch.batch_id,
                daily_limit_usd=settings.agent_daily_budget_usd,
                formal_stage_capacity=True,
                formal_model_config_id=model_id,
                platform_credential=__import__(
                    "nexus.agents.platform_native", fromlist=["credential_for_preparation"]
                ).credential_for_preparation(settings),
            )
            if cycle.result_code != "RESEARCH_JOBS_PREPARED":
                from nexus.paper.agent_cycle import _events

                facts = (await _events(db, cycle.cycle_id))[-1].fact_ids
                print(
                    "DAILY_ADMISSION_FIXED",
                    {k: v for k, v in facts.items() if k in ("failure_phase", "failure_code")},
                )
            assert cycle.result_code == "RESEARCH_JOBS_PREPARED", cycle.result_code
        async with factory() as db:
            target = await db.scalar(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_(cycle.research_run_ids),
                    AgentResearchRunRecord.instrument_id == pointer.instrument_id,
                )
            )
            snapshot = await db.get(
                __import__("nexus.persistence.models.data", fromlist=["DataSnapshot"]).DataSnapshot,
                target.data_snapshot_id,
            )
            paper = snapshot.payload["agent_paper_context"]
            assert paper.get("pending_monitoring_v2") is None
            assert paper.get("pending_durable_sec") is not None, paper.get(
                "pending_durable_sec_gap"
            )
            assert [x["event_id"] for x in paper["pending_durable_sec"]["events"]] == [
                str(event.id)
            ]
            if resolution == "issue":
                assert [x["event_key"] for x in paper["pending_durable_sec"]["issues"]] == [
                    f"sec:{rule.issuer_cik}:0000123456-26-000098"
                ]
            assert not tuple(await db.scalars(select(MonitorIncrement)))
            jobs = tuple(
                await db.scalars(
                    select(Job).where(
                        Job.payload["run_id"].astext.in_([str(x) for x in cycle.research_run_ids])
                    )
                )
            )
        seen = []

        def model_response(body, payload):
            has_event = payload.get("durable_sec_pending_context_hash") is not None
            if has_event:
                seen.append(payload["stage"])
            for role in body.get("roles", ()):
                role["vote"] = "support"
            reference = next(
                x["index"]
                for x in payload["evidence"]
                if x["source"] == "nexus_agent_paper_context"
            )
            if payload["stage"] == "synthesis":
                for review in body["synthesis_assessment"]["reviews"]:
                    review.update(
                        status="accepted",
                        resolved=True,
                        missing_evidence=[],
                        prevented_conclusion=None,
                    )
                body["synthesis_assessment"].update(
                    stance="watch", review_conclusion="sufficient", uncertainties=[]
                )
            if not has_event or resolution == "absent":
                return
            if payload["stage"] == "research":
                for role in body["roles"]:
                    if role["role"] == "orchestrator":
                        role["durable_sec_pending_proposal"] = {
                            "context_hash": payload["durable_sec_pending_context_hash"],
                            "items": [
                                {
                                    "event_id": str(event.id),
                                    "status": "addressed" if resolution == "issue" else resolution,
                                    "rationale": "Review the exact frozen filing and metadata.",
                                    "citations": [reference],
                                }
                            ],
                        }
            elif payload["stage"] == "critique":
                for role in body["roles"]:
                    role["durable_sec_pending_review"] = {
                        "proposal_hash": payload["durable_sec_pending_proposal"]["content_hash"],
                        "decision": "accepted",
                        "rationale": "Independently reviewed every frozen event.",
                        "citations": [reference],
                    }
            else:
                for review in body["synthesis_assessment"]["reviews"]:
                    review.update(
                        status="accepted",
                        resolved=True,
                        missing_evidence=[],
                        prevented_conclusion=None,
                    )
                body["synthesis_assessment"].update(
                    stance="watch", review_conclusion="sufficient", uncertainties=[]
                )
                body["durable_sec_pending_adoption_hash"] = payload["durable_sec_pending_proposal"][
                    "content_hash"
                ]

        calls, _ = install_sol_http(monkeypatch, store, model_response=model_response)
        normal_mock = httpx.MockTransport.handle_async_request

        async def headers(self, request):
            trace = request.extensions.get("trace")
            if trace is not None:
                await trace("http11.send_request_headers.started", {})
                await trace("http11.send_request_headers.complete", {})
            return await normal_mock(self, request)

        monkeypatch.setattr(httpx.MockTransport, "handle_async_request", headers)
        try:
            for job in jobs:
                assert await synchronized_worker(
                    settings, job_ids=(job.id,), formal_stage_capacity=True
                )
        except BaseException:
            async with factory() as diagnostic:
                print(
                    "DAILY_NATIVE_STAGE_DIAGNOSTIC",
                    calls,
                    seen,
                    [
                        r.status
                        for r in await diagnostic.scalars(
                            select(AgentResearchRunRecord).where(
                                AgentResearchRunRecord.id.in_(cycle.research_run_ids)
                            )
                        )
                    ],
                )
            raise
        async with factory() as db:
            finished = await db.scalar(select(func.clock_timestamp()))
            child = await db.get(AgentResearchRunRecord, target.id)
            assert child.status == "completed", child.status
            hold = await db.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == child.id
                )
            )
            assert hold.state == "settled" and hold.settled_usd == child.settled_cost_usd
            proof = await read_terminal_proof(db, run_id=child.id, now=finished)
            assert (
                proof is not None
                and proof.known
                and proof.budget_settled
                and proof.cleanup_confirmed
            )
        await run(settings, argparse.Namespace(action="reconcile"))
        async with factory() as db:
            from nexus.api.durable_sec import read_event_research_status

            read_at = await db.scalar(select(func.clock_timestamp()))
            rows = tuple(await db.scalars(select(SecDurableDailyEventReceipt)))
            if resolution in {"addressed", "issue"}:
                assert (
                    len(rows) == 1 and rows[0].event_id == event.id and rows[0].run_id == child.id
                )
                assert await read_event_research_status(db, event, read_at) == "addressed_daily"
            else:
                assert not rows
                assert await read_event_research_status(db, event, read_at) == "no_receipt"
            from nexus.monitoring.durable_sec_daily import freeze_durable_sec_pending

            pending, gap = await freeze_durable_sec_pending(
                db, cycle_id=cycle.cycle_id, instrument_id=pointer.instrument_id, as_of=finished
            )
            if resolution == "issue":
                assert pending is None and gap is not None and gap.pending_count == 1
                assert tuple(x.event_key for x in gap.issues) == (
                    f"sec:{rule.issuer_cik}:0000123456-26-000098",
                )
            else:
                assert gap is None
                assert (pending is None) is (resolution == "addressed")
            if pending is not None:
                assert tuple(x.event_id for x in pending.events) == (event.id,)
            assert not tuple(await db.scalars(select(MonitorIncrement)))
            assert await db.scalar(select(func.count()).select_from(OutboxEvent)) == notices
            assert await db.scalar(select(func.count()).select_from(SecNotificationClaim)) == claims
        assert calls == ["research", "critique", "synthesis"] * 3 and seen == [
            "research",
            "critique",
            "synthesis",
        ]
    finally:
        await factory.kw["bind"].dispose()
