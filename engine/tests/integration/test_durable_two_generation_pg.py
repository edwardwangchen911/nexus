"""Two legal past NY sessions use ordinary source, native jobs and exact event receipts."""

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
    append_prior_session_daily,
    append_session_fresh_inputs,
    candidate_input_boundary,
)
from helpers.durable_test_clock import DisposableClock
from helpers.mixed_stage import add_mixed_fields
from helpers.paper_batch import CALENDAR_ID
from integration import test_candidate_multistock_path as candidate
from integration import test_monitor_batches_pg as batches
from integration import test_monitor_disclosure_refresh_pg as refresh
from integration import test_monitor_filing_content_pg as filing
from nexus.monitoring import durable_sec_research as bridge
from nexus.monitoring import durable_sec_storage as durable
from nexus.monitoring.auto_increment_policy import enable_scope
from nexus.ops.research_monitor import run
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import (
    MonitorCoverageReceipt,
    MonitorIncrement,
    MonitorMixedEventReceipt,
    MonitorPlan,
)
from nexus.persistence.models.runtime import Job, OutboxEvent
from nexus.persistence.models.sec_monitor import SecDiscoveryEvent, SecNotificationClaim
from nexus.persistence.models.sec_research import (
    SecEventConsumptionReceipt,
    SecEventResearchBinding,
)
from nexus.research.durable_sec_index_storage import persist_durable_index_archive
from nexus.settings import AgentRuntimeMode
from pydantic import SecretStr
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "budget_pressure",
    [
        False,
        True,
        "allocator",
        "send_refusal",
        "stage2_refusal",
        "host_handoff",
        "sec_expired_before_material",
        "sec_conflicting_before_material",
        "sec_expired",
        "sec_refresh_on_dispatch",
        "sec_refresh_unavailable",
        "sec_refresh_slot_guard",
        "sec_refresh_dispatch_fenced",
        "sec_refresh_crosses_cutoff",
    ],
    ids=[
        "two-generations",
        "natural-budget-protection",
        "shared-allocation",
        "send-refusal",
        "stage2-refusal",
        "host-open-handoff",
        "sec-expires-before-material",
        "sec-conflicting-before-material",
        "sec-expires-in-queue",
        "sec-refresh-before-first-claim",
        "sec-refresh-unavailable-no-claim",
        "sec-refresh-respects-slot",
        "sec-refresh-refuses-prior-dispatch",
        "sec-refresh-rolls-back-marker-at-cutoff",
    ],
)
async def test_two_ordinary_ny_sessions_complete_native_settlement_and_successor(
    migrated_database_url, monkeypatch, tmp_path, budget_pressure
):
    from helpers import disclosure_observations as reader

    async def forbidden_real_http(*args, **kwargs):
        raise AssertionError("Only controlled local MockTransport is permitted")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden_real_http)

    at = datetime(2026, 11, 11, 15, tzinfo=UTC)
    sessions = (datetime(2026, 11, 12, 15, tzinfo=UTC), datetime(2026, 11, 13, 15, tzinfo=UTC))
    host_handoff = budget_pressure == "host_handoff"
    if host_handoff:
        sessions = (sessions[0].replace(hour=14, minute=24),)
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

    factory.kw.setdefault("info", {})["sec_exhibit_store"] = store
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
    try:
        async with factory() as db, db.begin():
            plan = await db.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            source = await db.get(AgentResearchRunRecord, plan.source_run_id)
            assert source.actor_id != "local-single-monitor-sol-v1"
            assert (
                await db.scalar(text("SELECT single_monitor_root_request(:id)"), {"id": source.id})
                is None
            )
            await enable_scope(
                db,
                account_id=pointer.account_id,
                candidate_batch_id=settings.agent_paper_candidate_batch_id,
            )
            rule = await durable.enable_rule(db, account_id=pointer.account_id, plan_id=plan.id)
            for day in (*sessions, datetime(2026, 11, 16, 15, tzinfo=UTC)):
                prior = await db.scalar(
                    select(ExchangeCalendarSessionRecord).where(
                        ExchangeCalendarSessionRecord.calendar_authority_id == CALENDAR_ID,
                        ExchangeCalendarSessionRecord.session == day.date(),
                    )
                )
                assert prior is None, day
                db.add(
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=CALENDAR_ID,
                        session=day.date(),
                        decision_at=day.replace(hour=21),
                        market_open_at=day.replace(hour=14, minute=30),
                        market_close_at=day.replace(hour=21),
                    )
                )
        settings = settings.model_copy(
            update={
                "agent_paper_formal_model_config_id": source.model_config_id,
                "agent_runtime_mode": AgentRuntimeMode.RESPONSES,
                "openai_api_key": SecretStr("sk-proj-" + "synthetic-only" * 5),
                "openai_extraction_model": "gpt-6.1-sol",
                "openai_reasoning_model": "gpt-6.1-sol",
                "agent_paper_cycle_enabled": True,
                "agent_daily_budget_usd": Decimal("13"),
            }
        )
        from integration.test_sol_native_stage_pg import install_sol_http
        from integration.test_sol_profile_pg import current_record, register
        from nexus.monitoring.disclosure_contracts import disclosure_from_inputs

        baseline_states = []
        install_errors = []
        admission_errors = []
        from nexus.monitoring import mixed_admission

        normal_admit = mixed_admission.admit_batch

        async def capture_admission(*args, **kwargs):
            try:
                return await normal_admit(*args, **kwargs)
            except ValueError as error:
                import re
                import traceback

                code = str(error)
                admission_errors.append(
                    dict(
                        code=code
                        if re.fullmatch(r"[A-Z0-9_]+", code)
                        else "UNCLASSIFIED_VALUE_ERROR",
                        frames=[
                            f"{f.filename}:{f.lineno}"
                            for f in traceback.extract_tb(error.__traceback__)
                        ],
                    )
                )
                raise

        monkeypatch.setattr(mixed_admission, "admit_batch", capture_admission)
        from nexus.monitoring import disclosure_installation

        normal_disclosure_install = disclosure_installation._install_mixed_or_legacy_disclosure

        async def capture_install(*args, **kwargs):
            try:
                return await normal_disclosure_install(*args, **kwargs)
            except ValueError as error:
                install_errors.append(str(error))
                raise

        monkeypatch.setattr(
            disclosure_installation, "_install_mixed_or_legacy_disclosure", capture_install
        )

        def model_response(body, payload):
            for role in body.get("roles", ()):
                role["vote"] = "support"
            add_mixed_fields(body, payload)
            context = disclosure_from_inputs(payload["frozen_inputs"])
            baseline = context.get("baseline") if isinstance(context, dict) else None
            baseline_states.append((payload["stage"], context))
            reference = next(
                row["index"]
                for row in payload["evidence"]
                if row["source"] == "nexus_monitor_increment_v2"
            )
            if payload["stage"] == "research":
                for row in body["roles"]:
                    row["disclosure_proposal"] = None
                    if row["role"] == "market_state" and baseline is not None:
                        row["disclosure_proposal"] = dict(
                            condition=dict(
                                signal="sec.issuer_disclosure_added",
                                operator="exists_new",
                                threshold=True,
                                unit="filing",
                                purpose="review",
                                source="sec",
                                forms=["8-K", "8-K/A"],
                                max_age_seconds=300,
                                time_authority="frozen_index_vs_local_receipt",
                            ),
                            baseline_hash=baseline["snapshot_hash"],
                            rationale="Review each frozen original and the current thesis.",
                            citations=[reference],
                            review_due_at=(day + timedelta(days=1)).isoformat(),
                            valid_until=(day + timedelta(days=2)).isoformat(),
                        )
            elif payload["stage"] == "critique":
                for row in body["roles"]:
                    row["disclosure_review"] = (
                        None
                        if payload.get("disclosure_proposal") is None
                        else dict(
                            proposal_hash=payload["disclosure_proposal"]["content_hash"],
                            decision="accepted",
                            rationale="Review exact baseline, source and time.",
                            citations=[reference],
                        )
                    )
            else:
                body["disclosure_adoption_hash"] = (
                    payload["disclosure_proposal"]["content_hash"]
                    if payload.get("disclosure_proposal") is not None
                    else None
                )

        calls, clients = install_sol_http(monkeypatch, store, model_response=model_response)
        normal_mock = httpx.MockTransport.handle_async_request

        async def synthetic_headers(self, request):
            if request.url.host in {"www.sec.gov", "data.sec.gov"}:
                return await normal_mock(self, request)
            trace = request.extensions.get("trace")
            assert trace is not None, "Active SEC request must have a send fence"
            await trace("http11.send_request_headers.started", {})
            await trace("http11.send_request_headers.complete", {})
            return await normal_mock(self, request)

        if not host_handoff:
            monkeypatch.setattr(httpx.MockTransport, "handle_async_request", synthetic_headers)
        directory = (tmp_path / "baseline/directory.json").read_bytes()
        entries = []
        run_ids = []
        plan_ids = [plan.id]
        event_ids = []
        for index, day in enumerate(sessions):
            monkeypatch.setattr(candidate, "AT", day)
            captured["clock"][0] = day
            async with factory() as db, db.begin():
                await controller.set(db, day, running=False)
            price = await current_record(factory)
            model_id = await register(factory, price)
            settings = settings.model_copy(update={"agent_paper_formal_model_config_id": model_id})
            if index:
                async with factory() as historical:
                    prior_run = await historical.get(AgentResearchRunRecord, run_ids[-1])
                    original_model = await historical.get(
                        AgentModelConfigRecord, prior_run.model_config_id
                    )
                    assert original_model.pricing_expires_at <= controller.now()
                    assert prior_run.model_config_id != model_id
                    assert (
                        prior_run.status == "completed"
                        and prior_run.settled_cost_usd == Decimal("0.0897")
                    )
            if not host_handoff:
                await append_session_fresh_inputs(
                    factory, pointer.instrument_id, controller.now(), decision_session=day.date()
                )
                boundary_before = await candidate_input_boundary(
                    factory, settings, pointer.instrument_id, controller.now(), CALENDAR_ID
                )
                if index:
                    assert boundary_before == dict(
                        candidate_count=0,
                        prior_session=sessions[0].date(),
                        daily_authorities=0,
                        quote_present=True,
                        same_quote=False,
                    ), boundary_before
                    await append_prior_session_daily(
                        factory,
                        monkeypatch,
                        pointer.instrument_id,
                        boundary_before["prior_session"],
                    )
                    boundary_after = await candidate_input_boundary(
                        factory, settings, pointer.instrument_id, controller.now(), CALENDAR_ID
                    )
                    assert boundary_after == dict(
                        candidate_count=1,
                        prior_session=sessions[0].date(),
                        daily_authorities=1,
                        quote_present=True,
                        same_quote=True,
                    ), boundary_after
                    print(
                        "E2_CURRENT_INPUT_BOUNDARY="
                        + json.dumps(
                            dict(before=boundary_before, after=boundary_after), default=str
                        ),
                        flush=True,
                    )
                else:
                    assert (
                        boundary_before["candidate_count"] == 1 and boundary_before["same_quote"]
                    ), boundary_before
            accepted = controller.now()
            entry = dict(
                accession_number=f"0000123456-26-00009{index + 1}",
                form="8-K",
                accepted_at=accepted.isoformat(),
                filed_at=accepted.replace(hour=0, minute=0, second=0, microsecond=0).isoformat(),
                primary_document=f"event-{index + 1}.htm",
            )
            entries.append(entry)
            if budget_pressure == "sec_conflicting_before_material":
                entries.append(
                    {
                        **entry,
                        "accession_number": "0000123456-26-000099",
                        "primary_document": "second-event.htm",
                    }
                )
            document = json.loads((tmp_path / "baseline/AAPL-submissions.json").read_bytes())
            document["filings"]["recent"] = {
                "accessionNumber": [x["accession_number"] for x in entries],
                "form": [x["form"] for x in entries],
                "acceptanceDateTime": [x["accepted_at"] for x in entries],
                "filingDate": [x["filed_at"][:10] for x in entries],
                "primaryDocument": [x["primary_document"] for x in entries],
            }
            raw = json.dumps(document).encode()
            received = controller.now()

            def receipt(body, url):
                return json.dumps(
                    dict(
                        source_url=url,
                        received_at=received.isoformat(),
                        sha256=sha256(body).hexdigest(),
                        byte_length=len(body),
                        http_status=200,
                    )
                ).encode()

            async def save_capture(db, submissions):
                stored, _ = await persist_durable_index_archive(
                    db,
                    store,
                    identity_snapshot_id=rule.identity_snapshot_id,
                    directory=(
                        directory,
                        receipt(
                            directory, "https://www.sec.gov/files/company_tickers_exchange.json"
                        ),
                    ),
                    submissions=(
                        submissions,
                        receipt(
                            submissions,
                            f"https://data.sec.gov/submissions/CIK{rule.issuer_cik}.json",
                        ),
                    ),
                    now=controller.now(),
                )
                return stored

            async with factory() as db, db.begin():
                capture = await save_capture(db, raw)
            async with factory() as db, db.begin():
                check = await durable.record_discovery(
                    db,
                    account_id=pointer.account_id,
                    rule_id=rule.id,
                    capture_id=capture.id,
                    scheduled_at=day.replace(minute=day.minute // 5 * 5),
                )
                assert check.result == "true", check.reason
                event = await db.scalar(
                    select(SecDiscoveryEvent).where(
                        SecDiscoveryEvent.event_key
                        == f"sec:{rule.issuer_cik}:{entry['accession_number']}"
                    )
                )
                event_ids.append(event.id)
                notices = await db.scalar(select(func.count()).select_from(OutboxEvent))
                claims = await db.scalar(select(func.count()).select_from(SecNotificationClaim))
                batch = await bridge.prepare_durable_sec_research_batch(
                    db, account_id=pointer.account_id, rule_id=rule.id
                )
                assert batch is not None
                binding = await db.scalar(
                    select(SecEventResearchBinding).where(
                        SecEventResearchBinding.batch_id == batch.id,
                        SecEventResearchBinding.event_id == event.id,
                    )
                )
                assert binding.event_id == event.id and binding.plan_id == plan_ids[-1]
            monkeypatch.setattr(filing, "AT", controller.now() - timedelta(seconds=8))
            await run(
                settings,
                argparse.Namespace(
                    action="import-content",
                    index_snapshot_id=capture.id,
                    accession=entry["accession_number"],
                    archive_dir=filing.archive(tmp_path / f"event-{index}", event.entry),
                ),
            )
            if budget_pressure in {
                "sec_expired_before_material",
                "sec_conflicting_before_material",
            }:
                from nexus.ops import monitor_batch_prepare as preparation
                from nexus.ops.monitor_dispatch import dispatch_configured_once
                from nexus.persistence.models.monitoring import MonitorTrigger
                from nexus.persistence.models.runtime import AuditEvent

                conflicting = budget_pressure == "sec_conflicting_before_material"
                if conflicting:
                    assert len(batch.trigger_ids) == 2
                    received += timedelta(seconds=1)
                    async with factory.begin() as db:
                        await controller.set(db, received, running=False, refresh_defaults=True)
                        later = await db.get(MonitorTrigger, UUID(batch.trigger_ids[1]))
                        assert later is not None
                        altered = json.loads(raw)
                        recent = altered["filings"]["recent"]
                        changed = recent["accessionNumber"].index(later.event_key.rsplit(":", 1)[1])
                        recent["primaryDocument"][changed] = "different.htm"
                        await save_capture(db, json.dumps(altered).encode())
                expired_at = received + timedelta(seconds=301)
                async with factory.begin() as db:
                    await controller.set(db, expired_at, running=False, refresh_defaults=True)
                    jobs = await db.scalar(select(func.count()).select_from(Job))
                options = dict(
                    trigger_id=UUID(batch.trigger_ids[0]),
                    calendar_authority_id=CALENDAR_ID,
                    repo_root=tmp_path,
                    prepare_materials_only=True,
                )
                declined = await dispatch_configured_once(settings, **options)
                assert declined["status"] == "blocked", declined
                assert declined["provider_requests"] == declined["model_requests"] == 0
                if conflicting:
                    assert declined["reason"] == "MONITOR_PREPARATION_INELIGIBLE"
                    assert "deferred_before_collection" not in declined
                    async with factory.begin() as db:
                        assert (
                            await preparation.inspect_batch_preparation(db, batch.id) == "blocked"
                        )
                        assert await db.scalar(select(func.count()).select_from(Job)) == jobs
                    return
                assert declined["reason"] == "DURABLE_SEC_CURRENT_CAPTURE_EXPIRED", declined
                assert "deferred_before_collection" not in declined
                assert (await dispatch_configured_once(settings, **options))["status"] == "checked"
                async with factory.begin() as db:
                    assert await preparation.inspect_batch_preparation(db, batch.id) == "pending"
                    received = expired_at + timedelta(minutes=5)
                    await controller.set(db, received, running=False, refresh_defaults=True)
                async with factory.begin() as db:
                    latest = await save_capture(db, raw)
                    checked = await durable.record_discovery(
                        db,
                        account_id=pointer.account_id,
                        rule_id=rule.id,
                        capture_id=latest.id,
                        scheduled_at=received.replace(minute=received.minute // 5 * 5, second=0),
                    )
                    assert checked.result != "true"

                async def account_unavailable(*args, **kwargs):
                    raise ValueError("MANUAL_ACCOUNT_BUSY_OR_UNKNOWN")

                with monkeypatch.context() as hold:
                    hold.setattr(
                        preparation, "require_manual_account_available", account_unavailable
                    )
                    blocked = await dispatch_configured_once(settings, **options)
                assert blocked["status"] == "blocked"
                assert blocked["reason"] == "MONITOR_ACCOUNT_WAIT_AFTER_SEC_EXPIRY"
                assert "deferred_before_collection" not in blocked
                assert blocked["provider_requests"] == blocked["model_requests"] == 0
                async with factory.begin() as db:
                    assert await preparation.inspect_batch_preparation(db, batch.id) == "pending"
                    next_bucket = (received + timedelta(minutes=5)).replace(second=0, microsecond=0)
                    await controller.set(db, next_bucket, running=False, refresh_defaults=True)
                prepared = await dispatch_configured_once(settings, **options)
                assert prepared["status"] == "prepared", prepared
                assert prepared["provider_requests"] == prepared["model_requests"] == 0
                async with factory() as db:
                    assert await db.scalar(select(func.count()).select_from(Job)) == jobs
                    assert await db.scalar(select(func.count()).select_from(OutboxEvent)) == notices
                    assert (
                        await db.scalar(select(func.count()).select_from(SecNotificationClaim))
                        == claims
                    )
                    assert not await db.scalar(
                        select(AuditEvent.id).where(
                            AuditEvent.action == preparation.CLAIM,
                            AuditEvent.payload["batch_id"].astext == str(batch.id),
                        )
                    )
                    assert not await db.scalar(
                        select(MonitorIncrement.id).where(MonitorIncrement.batch_id == batch.id)
                    )
                return
            material = await run(
                settings, argparse.Namespace(action="prepare-batch", batch_id=batch.id)
            )
            assert material["status"] == "prepared", material
            if host_handoff:
                from helpers.durable_host_open import verify_open_handoff

                await verify_open_handoff(
                    monkeypatch,
                    tmp_path,
                    factory,
                    settings,
                    store,
                    captured,
                    pointer,
                    controller,
                    rule,
                    batch.id,
                    UUID(material["snapshot_id"]),
                    directory,
                    raw,
                    calls,
                )
                return
            if index:

                async def row_counts():
                    async with factory() as counts_db:
                        return tuple(
                            [
                                await counts_db.scalar(select(func.count()).select_from(model))
                                for model in (
                                    AgentResearchRunRecord,
                                    Job,
                                    AgentDailyBudgetReservationRecord,
                                    MonitorIncrement,
                                )
                            ]
                        )

                before_expired = await row_counts()
                expired = await run(
                    settings.model_copy(
                        update={"agent_paper_formal_model_config_id": original_model.id}
                    ),
                    argparse.Namespace(
                        action="admit-batch",
                        batch_id=batch.id,
                        materials_snapshot_id=UUID(material["snapshot_id"]),
                        calendar_authority_id=CALENDAR_ID,
                    ),
                )
                assert expired["status"] == "blocked", expired
                assert await row_counts() == before_expired
                print(
                    "EXPIRED_CHILD_PRICE_ZERO_ROWS="
                    + json.dumps(dict(result=expired, counts=before_expired)),
                    flush=True,
                )
            if budget_pressure is True:
                # Two unrelated original reservations occupy this UTC day. They
                # never receive a route or transport; the isolated ledger is real.
                async with factory.begin() as db:
                    base = {
                        column.name: getattr(source, column.name)
                        for column in source.__table__.columns
                    }
                    model = await db.get(AgentModelConfigRecord, model_id)
                    other_members = await db.scalar(
                        text(
                            "SELECT candidate_instrument_ids FROM agent_paper_candidate_batches "
                            "WHERE id=:id"
                        ),
                        {"id": settings.agent_paper_candidate_batch_id},
                    )
                    other_instrument = next(i for i in other_members if i != pointer.instrument_id)
                    for _ in range(2):
                        identity = uuid4()
                        db.add(
                            AgentResearchRunRecord(
                                **(
                                    base
                                    | dict(
                                        id=identity,
                                        actor_id="unrelated-budget-fixture",
                                        instrument_id=other_instrument,
                                        idempotency_key=str(identity),
                                        input_hash=identity.hex * 2,
                                        decision_time=controller.now(),
                                        created_at=controller.now(),
                                        status="prepared",
                                        model_config_id=model.id,
                                        model_hash=model.config_hash,
                                        pricing_version=model.version,
                                        reserved_cost_usd=model.worst_case_cost_usd,
                                        settled_cost_usd=None,
                                        input_tokens=None,
                                        output_tokens=None,
                                    )
                                )
                            )
                        )
                        await db.flush()
                        db.add(
                            AgentDailyBudgetReservationRecord(
                                run_id=identity,
                                business_date=day.date(),
                                state="reserved",
                                reserved_usd=model.worst_case_cost_usd,
                                settled_usd=None,
                                pricing_version=model.version,
                            )
                        )
                assert calls == []
            admitted = await run(
                settings,
                argparse.Namespace(
                    action="admit-batch",
                    batch_id=batch.id,
                    materials_snapshot_id=UUID(material["snapshot_id"]),
                    calendar_authority_id=CALENDAR_ID,
                ),
            )
            if budget_pressure is True:
                assert admitted["status"] == "blocked", admitted
                assert admitted["reason"] == "MONITOR_BUDGET_UNAVAILABLE", dict(
                    result=admitted, errors=admission_errors
                )
                assert calls == []
                async with factory() as db:
                    assert await db.scalar(select(func.count()).select_from(MonitorIncrement)) == 0
                return
            assert admitted["status"] == "admitted", json.dumps(
                dict(result=admitted, errors=admission_errors), default=str
            )
            if budget_pressure == "allocator":
                from helpers.routine_budget_assertions import assert_allocations

                await assert_allocations(settings, UUID(admitted["run_id"]), controller.now())
                from helpers.routine_budget_assertions import assert_price_updates

                await assert_price_updates(settings, UUID(admitted["run_id"]), controller.now())
                assert calls == []
                return
            if budget_pressure in {
                "sec_expired",
                "sec_refresh_on_dispatch",
                "sec_refresh_unavailable",
                "sec_refresh_slot_guard",
                "sec_refresh_dispatch_fenced",
                "sec_refresh_crosses_cutoff",
            }:
                from nexus.agents.research_stages.protocol import stage_evidence
                from nexus.monitoring import durable_sec_permission
                from nexus.persistence.models.runtime import AuditEvent

                run_id = UUID(admitted["run_id"])
                job_id = UUID(admitted["job_id"])
                async with factory.begin() as db:
                    assert await durable_sec_permission.require_new_send(db, run_id)
                    assert (await db.get(Job, job_id)).attempt_count == 0
                # Model the real ordering: custody was fresh when admitted,
                # but no new official receipt arrives before the first claim.
                expired_at = received + timedelta(seconds=301)
                assert expired_at > controller.now()
                async with factory.begin() as db:
                    await controller.set(db, expired_at, running=False, refresh_defaults=True)

                if budget_pressure != "sec_expired":
                    from helpers.durable_host_open import install_durable_refresh_mock
                    from nexus.agents.research_stages.proof import read_terminal_proof
                    from nexus.ops.agent_paper_loop import (
                        AgentPaperLoopSnapshot,
                        choose_loop_action,
                    )
                    from nexus.ops.monitor_dispatch import (
                        SEC_DEFERRED_ACTION,
                        WORKER_ACTION,
                        confirm_monitor_shutdown,
                        dispatch_configured_once,
                        inspect_monitor_queue,
                    )

                    failed_source = budget_pressure == "sec_refresh_unavailable"
                    sec_calls = install_durable_refresh_mock(
                        monkeypatch,
                        store,
                        controller,
                        directory,
                        raw,
                        unavailable=failed_source,
                    )
                    with pytest.raises(ValueError, match="SEC_RESEARCH_PERMISSION_INVALID"):
                        async with factory.begin() as db:
                            await durable_sec_permission.require_new_send(db, run_id)
                    monkeypatch.setattr(worker, "run_agent_worker_once", synchronized_worker)
                    options = dict(
                        trigger_id=UUID(batch.trigger_ids[0]),
                        calendar_authority_id=CALENDAR_ID,
                        repo_root=tmp_path,
                        environment={"SEC_CONTACT": "nexus test@example.invalid"},
                    )
                    if budget_pressure == "sec_refresh_slot_guard":
                        options["refresh_not_after"] = controller.now() + timedelta(seconds=89)
                    dispatched = budget_pressure == "sec_refresh_dispatch_fenced"
                    if dispatched:
                        from nexus.audit.service import append_audit_event

                        async with factory.begin() as db:
                            await append_audit_event(
                                db,
                                actor_id="research-monitor",
                                action=WORKER_ACTION,
                                schema_version="monitor_dispatch_v1",
                                payload={
                                    "trigger_id": str(options["trigger_id"]),
                                    "job_id": str(job_id),
                                },
                                occurred_at=controller.now(),
                            )

                    crosses_cutoff = budget_pressure == "sec_refresh_crosses_cutoff"
                    if crosses_cutoff:
                        from nexus.audit import service as audit

                        cutoff = controller.now() + timedelta(seconds=180)
                        options["refresh_not_after"] = cutoff
                        normal_append = audit.append_audit_event

                        async def append_at_cutoff(db, **kwargs):
                            event = await normal_append(db, **kwargs)
                            if kwargs["action"] == WORKER_ACTION:
                                await controller.set(db, cutoff, running=False)
                            return event

                        monkeypatch.setattr(audit, "append_audit_event", append_at_cutoff)

                    result = await dispatch_configured_once(settings, **options)
                    if budget_pressure != "sec_refresh_on_dispatch":
                        assert result["status"] == ("unconfirmed" if dispatched else "deferred"), (
                            result
                        )
                        assert not calls and len(sec_calls) == (
                            2 if crosses_cutoff else int(failed_source)
                        )
                        async with factory() as db:
                            job = await db.get(Job, job_id)
                            assert (job.status, job.attempt_count) == ("queued", 0)
                            sent = await db.scalar(
                                select(AuditEvent.id).where(
                                    AuditEvent.action == WORKER_ACTION,
                                    AuditEvent.payload["job_id"].astext == str(job_id),
                                )
                            )
                            assert bool(sent) is dispatched
                            receipt = await db.scalar(
                                select(AuditEvent).where(
                                    AuditEvent.action == SEC_DEFERRED_ACTION,
                                    AuditEvent.payload["job_id"].astext == str(job_id),
                                )
                            )
                            assert (receipt is None) is dispatched
                        if not dispatched:
                            now = controller.now()
                            snapshot = AgentPaperLoopSnapshot(
                                now=now,
                                calendar_authority_id=CALENDAR_ID,
                                decision_window_start_at=now - timedelta(hours=2),
                                decision_window_end_at=now - timedelta(minutes=1),
                                next_decision_window_start_at=now + timedelta(days=1),
                            )
                            selected = await inspect_monitor_queue(
                                settings, account_id=pointer.account_id, snapshot=snapshot
                            )
                            assert selected.monitor_preclaim_deferred
                            assert selected.monitor_trigger_id is None
                            assert choose_loop_action(selected) == choose_loop_action(snapshot)
                            # The completed HTTP 503 records its request count.
                            assert receipt.payload["source_confirmed"] is True
                            assert await confirm_monitor_shutdown(
                                settings,
                                account_id=pointer.account_id,
                                trigger_id=options["trigger_id"],
                                action_started_at=expired_at,
                            )
                    else:
                        assert result["status"] == "completed", result
                        assert result["run_id"] == str(run_id)
                        assert calls == ["research", "critique", "synthesis"]
                        assert len(sec_calls) == 2
                        async with factory() as db:
                            job = await db.get(Job, job_id)
                            assert (job.status, job.attempt_count) == ("succeeded", 1)
                            proof = await read_terminal_proof(
                                db, run_id=run_id, now=controller.now()
                            )
                            assert proof is not None and proof.known and proof.budget_settled
                            assert proof.cleanup_confirmed
                    before = len(calls), len(sec_calls)
                    await dispatch_configured_once(settings, **options)
                    assert (len(calls), len(sec_calls)) == before
                    return

                refusals = []
                native_send = durable_sec_permission.require_new_send

                async def observed_send(db, selected):
                    try:
                        return await native_send(db, selected)
                    except ValueError as error:
                        assert selected == run_id
                        refusals.append(str(error))
                        raise

                monkeypatch.setattr(durable_sec_permission, "require_new_send", observed_send)
                assert await synchronized_worker(
                    settings, job_ids=(job_id,), formal_stage_capacity=True
                )
                assert refusals == ["SEC_RESEARCH_PERMISSION_INVALID"] and calls == []
                async with factory() as db:
                    job = await db.get(Job, job_id)
                    assert (job.status, job.attempt_count, job.manual_retry_count) == (
                        "failed",
                        1,
                        0,
                    )
                    terminal = await db.scalar(
                        text("SELECT known FROM research_stage_terminal WHERE run_id=:run"),
                        {"run": run_id},
                    )
                    assert terminal is False and await stage_evidence(db, run_id) == []
                    failure = await db.scalar(
                        select(AuditEvent).where(
                            AuditEvent.action == "research.stage_failure",
                            AuditEvent.payload["run_id"].astext == str(run_id),
                        )
                    )
                    assert failure.payload["failure_phase"] == "authorization"
                    hold = await db.scalar(
                        select(AgentDailyBudgetReservationRecord).where(
                            AgentDailyBudgetReservationRecord.run_id == run_id
                        )
                    )
                    assert hold.state == "reserved" and hold.settled_usd is None
                assert not await synchronized_worker(
                    settings, job_ids=(job_id,), formal_stage_capacity=True
                )
                from helpers.original_monitor_occupancy import assert_disposition

                async with factory.begin() as db:
                    expiry = await db.scalar(
                        text("SELECT expires_at FROM job_leases WHERE job_id=:job"), {"job": job_id}
                    )
                    closed_at = expiry + timedelta(seconds=1)
                    await controller.set(db, closed_at, running=False, refresh_defaults=True)
                await assert_disposition(
                    factory,
                    monkeypatch,
                    pointer.account_id,
                    run_id,
                    job_id,
                    failure.event_hash,
                    closed_at,
                )
                assert calls == []
                return
            if budget_pressure == "stage2_refusal":
                from nexus.agents.budget import BudgetExhausted
                from nexus.agents.research_stages.proof import read_terminal_proof
                from nexus.paper import routine_budget

                normal_send = routine_budget.require_new_send

                async def refuse_after_first(*args, **kwargs):
                    if calls:
                        raise BudgetExhausted("ROUTINE_NATURAL_BUDGET_PROTECTED")
                    return await normal_send(*args, **kwargs)

                monkeypatch.setattr(routine_budget, "require_new_send", refuse_after_first)
                assert await synchronized_worker(
                    settings, job_ids=(UUID(admitted["job_id"]),), formal_stage_capacity=True
                )
                assert calls == ["research"]
                async with factory() as db:
                    identity = UUID(admitted["run_id"])
                    proof = await read_terminal_proof(db, run_id=identity, now=controller.now())
                    assert (
                        proof is not None
                        and proof.known
                        and proof.budget_settled
                        and proof.cleanup_confirmed
                    )
                    assert (
                        await db.scalar(
                            text(
                                "SELECT count(*) FROM research_stage_authorization "
                                "WHERE run_id=:run"
                            ),
                            {"run": identity},
                        )
                        == 1
                    )
                    assert (
                        await db.scalar(
                            text(
                                "SELECT state FROM agent_daily_budget_reservations "
                                "WHERE run_id=:run"
                            ),
                            {"run": identity},
                        )
                        == "settled"
                    )
                return
            if budget_pressure == "send_refusal":
                from nexus.agents.budget import BudgetExhausted
                from nexus.paper import routine_budget

                async def refuse_new_send(*args, **kwargs):
                    raise BudgetExhausted("ROUTINE_NATURAL_BUDGET_PROTECTED")

                monkeypatch.setattr(routine_budget, "require_new_send", refuse_new_send)
                assert await synchronized_worker(
                    settings, job_ids=(UUID(admitted["job_id"]),), formal_stage_capacity=True
                )
                assert calls == []
                async with factory() as db:
                    job = await db.get(Job, UUID(admitted["job_id"]))
                    terminal = (
                        await db.execute(
                            text("SELECT known FROM research_stage_terminal WHERE run_id=:run"),
                            {"run": UUID(admitted["run_id"])},
                        )
                    ).one_or_none()
                    assert (
                        job.status == "skipped"
                        and job.result_code == "AGENT_ABSTAINED"
                        and job.last_error_code != "AUTHORIZATION_COMMIT_UNCONFIRMED"
                    )
                    from nexus.paper.research_failure import (
                        require_claimed_uncalled_research_closed,
                    )

                    await require_claimed_uncalled_research_closed(
                        db, run_id=UUID(admitted["run_id"]), job_id=job.id, now=controller.now()
                    )
                    assert terminal is None
                    assert (
                        await db.scalar(
                            text("SELECT count(*) FROM research_stage_member WHERE run_id=:run"),
                            {"run": UUID(admitted["run_id"])},
                        )
                        == 0
                    )
                    assert (
                        await db.scalar(
                            text(
                                "SELECT count(*) FROM research_stage_authorization "
                                "WHERE run_id=:run"
                            ),
                            {"run": UUID(admitted["run_id"])},
                        )
                        == 0
                    )
                return
            run_ids.append(UUID(admitted["run_id"]))
            assert await synchronized_worker(
                settings, job_ids=(UUID(admitted["job_id"]),), formal_stage_capacity=True
            )
            async with factory() as diagnostic:
                actual_job = await diagnostic.get(Job, UUID(admitted["job_id"]))
                actual_run = await diagnostic.get(AgentResearchRunRecord, run_ids[-1])
                terminal = (
                    (
                        await diagnostic.execute(
                            text(
                                "SELECT known,closed_at,observed_stages "
                                "FROM research_stage_terminal WHERE run_id=:run"
                            ),
                            {"run": run_ids[-1]},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                stages = (
                    await diagnostic.execute(
                        text(
                            "SELECT stage,valid,eligible,observed_at "
                            "FROM research_stage_observation WHERE run_id=:run ORDER BY stage"
                        ),
                        {"run": run_ids[-1]},
                    )
                ).all()
                assert actual_run.status == "completed", dict(
                    job=actual_job.status,
                    error=actual_job.last_error_code,
                    result=actual_job.result_code,
                    run=actual_run.status,
                    calls=calls,
                    terminal=terminal,
                    stages=stages,
                )
                from nexus.agents.research_stages.proof import read_terminal_proof

                now = await diagnostic.scalar(select(func.clock_timestamp()))
                proof = await read_terminal_proof(diagnostic, run_id=run_ids[-1], now=now)
                assert proof is not None and proof.cleanup_confirmed and proof.budget_settled, dict(
                    proof=proof, now=now, terminal=terminal
                )
            finished = await run(
                settings, argparse.Namespace(action="finish", trigger_id=UUID(batch.trigger_ids[0]))
            )
            assert finished["status"] == "completed", finished
            await run(settings, argparse.Namespace(action="reconcile"))
            from nexus.ops.monitor_dispatch import confirm_monitor_shutdown

            assert await confirm_monitor_shutdown(
                settings, account_id=pointer.account_id, trigger_id=UUID(batch.trigger_ids[0])
            )
            async with factory() as db:
                child = await db.get(AgentResearchRunRecord, run_ids[-1])
                assert child.status == "completed"
                assert child.settled_cost_usd == Decimal("0.0897")
                job = await db.get(Job, UUID(admitted["job_id"]))
                assert (
                    job.status == "succeeded"
                    and job.attempt_count == 1
                    and job.manual_retry_count == 0
                )
                reservation = await db.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == child.id
                    )
                )
                assert reservation.state == "settled"
                member = await db.scalar(
                    select(MonitorIncrement).where(MonitorIncrement.run_id == child.id)
                )
                assert member.session_date == day.date() and member.reserved_sdk_calls == 3
                coverage = await db.scalar(
                    select(MonitorCoverageReceipt).where(
                        MonitorCoverageReceipt.run_id == child.id,
                        MonitorCoverageReceipt.kind == "disclosure",
                    )
                )
                assert coverage.status == "installed", json.dumps(
                    dict(
                        status=coverage.status,
                        reason=coverage.reason,
                        install_errors=install_errors,
                        baseline_states=baseline_states,
                    ),
                    default=str,
                )
                successor = await db.get(MonitorPlan, coverage.active_plan_id)
                assert successor.source_run_id == child.id and successor.generation == index + 2
                plan_ids.append(successor.id)
                proof = await db.scalar(
                    select(SecEventConsumptionReceipt).where(
                        SecEventConsumptionReceipt.event_id == event.id
                    )
                )
                assert proof is not None
                native_receipt = await db.get(MonitorMixedEventReceipt, proof.mixed_receipt_id)
                assert native_receipt.run_id == child.id
                from nexus.api.durable_sec import read_event_research_status

                read_at = await db.scalar(select(func.clock_timestamp()))
                assert await read_event_research_status(db, event, read_at) == "addressed_increment"
                assert await db.scalar(select(func.count()).select_from(OutboxEvent)) == notices
                assert (
                    await db.scalar(select(func.count()).select_from(SecNotificationClaim))
                    == claims
                )
        assert calls == ["research", "critique", "synthesis"] * 2
        assert len(clients) == 6 and all(client.is_closed for client in clients)
        assert len(set(run_ids)) == 2 and len(set(plan_ids)) == 3 and len(set(event_ids)) == 2
    finally:
        await factory.kw["bind"].dispose()
