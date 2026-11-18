"""Isolated normal-host handoff only; no incremental model stages or external I/O."""

import argparse
import asyncio
import json
from contextlib import asynccontextmanager, nullcontext
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
from helpers.durable_session_inputs import append_session_fresh_inputs
from helpers.monitor_host_process import write_environment
from helpers.paper_batch import CALENDAR_ID
from nexus.ops import agent_paper_loop as loop
from nexus.ops import agent_paper_once, monitor_disclosure_refresh, research_identity_local
from nexus.ops.research_monitor import run
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorPlan, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent, Job, OutboxEvent
from nexus.persistence.models.sec_monitor import SecNotificationClaim
from pydantic import SecretStr
from sqlalchemy import func, select


async def verify_open_handoff(
    monkeypatch,
    root,
    factory,
    settings,
    store,
    captured,
    pointer,
    controller,
    rule,
    batch_id,
    materials_id,
    directory,
    submissions,
    model_calls,
):
    """Keep real queue/selector/refresh/admission; replace provider and worker edges."""
    from nexus import worker
    from nexus.ops import sec_index_local

    async def move(at, *, refresh_defaults=False):
        captured["clock"][0] = at
        async with factory.begin() as db:
            await controller.set(db, at, running=False, refresh_defaults=refresh_defaults)

    closed = await run(
        settings,
        argparse.Namespace(
            action="admit-batch",
            batch_id=batch_id,
            materials_snapshot_id=materials_id,
            calendar_authority_id=CALENDAR_ID,
        ),
    )
    assert closed["status"] == "blocked" and closed["error_code"] == "MONITOR_MARKET_CLOSED"

    async def counts():
        async with factory() as db:
            rows = [
                await db.scalar(select(func.count()).select_from(m))
                for m in (AgentResearchRunRecord, Job)
            ]
            rows.append(
                await db.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(
                        OutboxEvent.event_type.in_(
                            (
                                "research.sec.discovered",
                                "research.monitor.triggered",
                                "research.monitor.batch_triggered",
                            )
                        ),
                    )
                )
            )
            rows.append(await db.scalar(select(func.count()).select_from(SecNotificationClaim)))
            return tuple(rows)

    before = await counts()
    async with factory() as db:
        trigger = await db.scalar(select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id))
        original_capture = await db.scalar(
            select(DataSnapshot)
            .where(
                DataSnapshot.source == "sec_durable_disclosure_index",
            )
            .order_by(DataSnapshot.received_at.desc())
            .limit(1)
        )
        old_quote = await db.scalar(
            select(LatestQuote)
            .where(
                LatestQuote.instrument_id == pointer.instrument_id,
                LatestQuote.event_kind == "quote",
            )
            .order_by(LatestQuote.received_at.desc())
            .limit(1)
        )
        assert trigger.status == "blocked" and trigger.research_run_id is None
    opened = controller.now().replace(hour=14, minute=30, second=1, microsecond=0)
    daily_slot = opened.replace(hour=15, minute=0, second=0)
    from nexus.monitoring.host import (
        BATCH_PREPARATION_HEADROOM_SECONDS,
        INCREMENT_ACTION_HEADROOM_SECONDS,
    )

    assert daily_slot - opened > timedelta(
        seconds=(BATCH_PREPARATION_HEADROOM_SECONDS + INCREMENT_ACTION_HEADROOM_SECONDS)
    )
    assert (opened - original_capture.received_at).total_seconds() > 300
    assert (opened - old_quote.received_at).total_seconds() > 300

    sec_requests, quote_requests, worker_calls, actions = [], [], [], []
    completed_scans = []
    action_active = False
    from nexus.ops import monitor_batch_prepare, research_monitor

    normal_scan = research_monitor.scan_host_once
    normal_material = monitor_batch_prepare.prepare_batch_inputs

    async def observed_scan(*args, **kwargs):
        try:
            return await normal_scan(*args, **kwargs)
        finally:
            completed_scans.append(controller.now())

    async def observed_material(*args, **kwargs):
        try:
            return await normal_material(*args, **kwargs)
        except ValueError as error:
            print("HOST_MATERIAL_REJECTED=" + str(error), flush=True)
            raise

    monkeypatch.setattr(research_monitor, "scan_host_once", observed_scan)
    monkeypatch.setattr(monitor_batch_prepare, "prepare_batch_inputs", observed_material)

    def sec_response(request):
        assert request.method == "GET" and request.url.host in {"www.sec.gov", "data.sec.gov"}
        sec_requests.append({"at": controller.now(), "path": request.url.path})
        content = directory if request.url.host == "www.sec.gov" else submissions
        return httpx.Response(200, content=content)

    monkeypatch.setattr(
        monitor_disclosure_refresh,
        "_client",
        lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(sec_response),
            follow_redirects=False,
        ),
    )
    monkeypatch.setattr(sec_index_local, "_store", lambda _settings, supplied: supplied or store)

    class ClockMeta(type):
        def __instancecheck__(cls, value):
            return isinstance(value, datetime)

    class SourceClock(datetime, metaclass=ClockMeta):
        @classmethod
        def now(cls, tz=None):
            return controller.now().astimezone(tz or UTC)

    monkeypatch.setattr(research_identity_local, "datetime", SourceClock)

    async def unrelated_opening_refresh(*_args, **_kwargs):
        # These ordinary opening refreshes have their own acceptance. Preserve
        # their documented unavailable result, never invent ready source facts.
        return "unavailable"

    for phase in (
        "prepare_prior_daily_once",
        "prepare_agent_paper_identity_once",
        "prepare_macro_once",
        "prepare_current_trends_once",
    ):
        monkeypatch.setattr(loop, phase, unrelated_opening_refresh)

    @asynccontextmanager
    async def broker_client(**_kwargs):
        # A normal market producer receives the explicit isolated client; no socket opens.
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: (_ for _ in ()).throw(AssertionError("unexpected broker HTTP"))
            )
        ) as client:
            yield client

    async def qualified_market_input(db, evidence, configured, **kwargs):
        # Reuse the qualified quote/risk fixture, then let the real market preflight
        # prove the new quote matches the actual candidate's decision quote.
        assert kwargs["identities"][0].instrument_id == pointer.instrument_id
        assert kwargs["manual_market_window"] is True and kwargs["expand_execution_scope"] is False
        quote_requests.append(controller.now())
        async with factory() as read:
            price_plan = await read.scalar(
                select(MonitorPlan)
                .where(
                    MonitorPlan.instrument_id == pointer.instrument_id,
                    MonitorPlan.kind == "price",
                )
                .order_by(MonitorPlan.generation.desc())
                .limit(1)
            )
            midpoint = None
            if price_plan is not None:
                threshold = Decimal(str(price_plan.condition["threshold"]))
                midpoint = (
                    threshold / 2 if price_plan.condition["operator"] == "gte" else threshold * 2
                )
        await append_session_fresh_inputs(
            factory,
            pointer.instrument_id,
            controller.now(),
            midpoint=midpoint,
            decision_session=kwargs["decision_session"],
        )

    monkeypatch.setattr(agent_paper_once, "schwab_http_client", broker_client)
    monkeypatch.setattr(agent_paper_once, "S3ProviderEvidenceStore", lambda *a, **kw: store)
    monkeypatch.setattr(
        agent_paper_once, "prepare_schwab_decision_authority", qualified_market_input
    )

    async def handoff_only(configured, *, job_ids, formal_stage_capacity):
        assert configured.database_url == settings.database_url and formal_stage_capacity
        async with factory() as db:
            increment = await db.scalar(
                select(MonitorIncrement).where(
                    MonitorIncrement.batch_id == batch_id,
                )
            )
            assert increment is not None and job_ids == (increment.job_id,)
            worker_calls.append(
                {
                    "at": controller.now(),
                    "job_id": str(increment.job_id),
                    "run_id": str(increment.run_id),
                }
            )
        return False  # Stop at the persisted handoff; deliberately do not run model stages.

    monkeypatch.setattr(worker, "run_agent_worker_once", handoff_only)
    monkeypatch.setattr(loop, "acquire_agent_paper_lock", lambda: nullcontext())
    normal_execute = loop.execute_agent_paper_loop_action

    async def observed_execute(*args, **kwargs):
        nonlocal action_active
        action_active = True
        actions.append({"at": controller.now(), "action": kwargs["action"].value})
        print("HOST_OPEN_ACTION=" + json.dumps(actions[-1], default=str), flush=True)
        try:
            return await normal_execute(*args, **kwargs)
        finally:
            action_active = False

    monkeypatch.setattr(loop, "execute_agent_paper_loop_action", observed_execute)
    settings = settings.model_copy(
        update={
            "object_store_region": "us-east-1",
            "object_store_bucket": "synthetic",
            "object_store_prefix": "synthetic",
            "object_store_access_key_id": SecretStr("synthetic"),
            "object_store_secret_access_key": SecretStr("synthetic"),
        }
    )
    env_file = root / "host-open.env"
    write_environment(
        settings,
        env_file,
        extras={
            "SEC_CONTACT": "synthetic test@example.invalid",
            "NEXUS_SCHWAB_APP_KEY": "synthetic-key",
            "NEXUS_SCHWAB_APP_SECRET": "synthetic-secret",
            "NEXUS_SCHWAB_CALLBACK_URL": "https://127.0.0.1",
            "ALPACA_API_KEY_ID": "synthetic",
            "ALPACA_API_SECRET_KEY": "synthetic",
        },
    )

    async def sleep(_seconds):
        # Advance only after the real source/scan and foreground action end;
        # never jump across a bucket while either normal operation is in flight.
        if not action_active and completed_scans and completed_scans[-1] == controller.now():
            if controller.now() < opened:
                await move(opened)
            elif not worker_calls:
                assert controller.now() < daily_slot - timedelta(minutes=17)
                await move(controller.now() + timedelta(minutes=5))
        await asyncio.sleep(0.01)

    # Install fixture defaults before the host starts. Advancing its clock while
    # background reads are active must not ALTER TABLE or take exclusive locks.
    await move(opened - timedelta(minutes=1), refresh_defaults=True)
    async with asyncio.timeout(90):
        first = await loop.run_local_agent_paper_loop(
            settings,
            account_id=pointer.account_id,
            repo_root=root,
            env_file=env_file,
            clock=controller.now,
            monotonic=lambda: (controller.now() - opened).total_seconds(),
            sleep=sleep,
        )
    print(
        "HOST_OPEN_FIRST="
        + json.dumps(
            {
                "result": first.value,
                "actions": actions,
                "sec_requests": sec_requests,
                "quote_requests": quote_requests,
                "worker_calls": worker_calls,
            },
            default=str,
        ),
        flush=True,
    )
    assert first == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    assert len(worker_calls) == 1
    assert opened <= worker_calls[0]["at"] <= daily_slot - timedelta(minutes=17)
    assert len(quote_requests) == 1 and quote_requests[0] <= worker_calls[0]["at"]
    assert len(sec_requests) >= 2 and all(x["at"] >= opened for x in sec_requests)
    assert not model_calls
    after_first = await counts()
    assert after_first == (before[0] + 1, before[1] + 1, before[2], before[3]), {
        "before": before,
        "after": after_first,
    }
    async with factory() as db:
        latest_capture = await db.scalar(
            select(DataSnapshot)
            .where(
                DataSnapshot.source == "sec_durable_disclosure_index",
            )
            .order_by(DataSnapshot.received_at.desc())
            .limit(1)
        )
        assert latest_capture.id != original_capture.id
        assert (
            timedelta(0)
            <= worker_calls[0]["at"] - latest_capture.received_at
            <= timedelta(seconds=300)
        )
        increment = await db.scalar(
            select(MonitorIncrement).where(MonitorIncrement.batch_id == batch_id)
        )
        quote = await db.scalar(
            select(LatestQuote)
            .where(
                LatestQuote.instrument_id == pointer.instrument_id,
                LatestQuote.event_kind == "quote",
            )
            .order_by(LatestQuote.received_at.desc())
            .limit(1)
        )
        assert quote.id != old_quote.id
        assert timedelta(0) <= worker_calls[0]["at"] - quote.received_at <= timedelta(seconds=300)
        assert (
            await db.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(
                    AuditEvent.action == "research.monitor.worker_dispatched",
                    AuditEvent.payload["job_id"].astext == str(increment.job_id),
                )
            )
            == 1
        )
    await move(worker_calls[0]["at"] + timedelta(seconds=15))
    async with asyncio.timeout(90):
        second = await loop.run_local_agent_paper_loop(
            settings,
            account_id=pointer.account_id,
            repo_root=root,
            env_file=env_file,
            clock=controller.now,
            sleep=sleep,
        )
    assert second == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN
    assert len(worker_calls) == 1 and not model_calls and await counts() == after_first
    print(
        "HOST_OPEN_HANDOFF="
        + json.dumps(
            {
                "opened_at": str(opened),
                "first_dispatch_at": str(worker_calls[0]["at"]),
                "daily_slot_at": str(daily_slot),
                "headroom_seconds": 1020,
                "official_fixture_requests": sec_requests,
                "quote_fixture_requests": len(quote_requests),
                "worker_calls": worker_calls,
                "actions": actions,
                "second_tick_no_replay": True,
                "model_requests": 0,
                "external_http_requests": 0,
                "settlement_tested": False,
            },
            default=str,
        ),
        flush=True,
    )
