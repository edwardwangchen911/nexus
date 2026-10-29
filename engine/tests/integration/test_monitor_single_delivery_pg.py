"""New monitor events never authorize a second HTTP send after uncertain delivery."""

import asyncio
from contextlib import ExitStack
from datetime import timedelta
from uuid import UUID

import httpx
import pytest
from helpers.commit_ack_fault import CommitAcknowledgementLost, lose_commit_ack
from integration.test_monitor_batches_pg import installed
from integration.test_research_monitor_notification_pg import captured_trigger
from nexus.monitoring.batches import scan_stock_batch
from nexus.notifications.bark import BarkPublisher
from nexus.outbox.dispatcher import dispatch_outbox_once
from nexus.outbox.models import PublisherSpec
from nexus.outbox.registry import PublisherRegistry
from nexus.persistence.models.monitoring import MonitorObservationBatch
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


async def capture(database_url, monkeypatch, root, kind):
    if kind == "single":
        factory, trigger, _ = await captured_trigger(database_url)
        event_id = trigger.outbox_id
    else:
        factory, _, _, captured, pointer = await installed(database_url, monkeypatch, root)
        async with factory() as session, session.begin():
            result = await scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=captured["clock"][0],
            )
        async with factory() as session, session.begin():
            repeated = await scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=captured["clock"][0],
            )
            assert repeated["batch_id"] == result["batch_id"]
            assert repeated["notifications"] == 0 and repeated["new_triggers"] == 0
            batch = await session.get(MonitorObservationBatch, UUID(result["batch_id"]))
            event_id = batch.outbox_id
    async with factory() as session:
        event = await session.get(OutboxEvent, event_id)
    return factory, event, max(event.available_at, event.created_at) + timedelta(seconds=1)


async def protected_facts(factory):
    async with factory() as session:
        return {
            name: sorted(
                (await session.scalars(text(f"SELECT to_jsonb(t)::text FROM {name} t"))).all()
            )
            for name in (
                "agent_research_runs",
                "agent_research_outputs",
                "jobs",
                "job_leases",
                "agent_daily_budget_reservations",
                "agent_portfolio_synthesis_budget_reservations",
                "portfolio_decision_budget_reservations",
                "research_monitor_triggers",
            )
        }


def registry(client, event):
    publisher = BarkPublisher(
        client,
        "http://localhost:8080",
        "test-device-key",
        b"0123456789abcdef",
        read_only_url="https://nexus.local/paper",
    )
    return PublisherRegistry(
        [
            PublisherSpec(
                event_type=event.event_type,
                schema_version=event.schema_version,
                consumer_idempotent=True,
                publisher=publisher,
            )
        ]
    )


async def saved(factory, event):
    async with factory() as session:
        row = await session.get(OutboxEvent, event.id)
        attempts = tuple(
            await session.scalars(
                select(OutboxDeliveryAttempt).where(
                    OutboxDeliveryAttempt.outbox_event_id == event.id
                )
            )
        )
    return row, attempts


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["single", "batch"])
@pytest.mark.parametrize("failure", ["http_rejected", "read_timeout"])
async def test_new_monitor_failure_does_not_send_twice(
    migrated_database_url, monkeypatch, tmp_path, kind, failure
):
    factory, event, now = await capture(migrated_database_url, monkeypatch, tmp_path, kind)
    before = await protected_facts(factory)
    sends = []

    async def transport(request):
        sends.append(request.content)
        if failure == "read_timeout":
            raise httpx.ReadTimeout("controlled timeout", request=request)
        return httpx.Response(503, text="controlled refusal")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        options = dict(
            registry=registry(client, event),
            worker_id="single-attempt-test",
            claim_registered_only=True,
        )
        assert await dispatch_outbox_once(factory, clock=lambda: now, **options)
        for seconds in (4, 62, 124):
            assert not await dispatch_outbox_once(
                factory, clock=lambda: now + timedelta(seconds=seconds), **options
            )
    row, attempts = await saved(factory, event)
    assert len(sends) == len(attempts) == row.attempt_count == row.max_attempts == 1
    assert row.status == "dead_letter" and row.published_at is None
    assert attempts[0].outcome == "failed" and attempts[0].error_code == "PUBLISHER_FAILED"
    assert await protected_facts(factory) == before
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["single", "batch"])
@pytest.mark.parametrize("phase", ["claim", "ack"])
@pytest.mark.parametrize("server_commits", [False, True])
async def test_monitor_commit_confirmation_loss_preserves_single_send(
    migrated_database_url, monkeypatch, tmp_path, kind, phase, server_commits
):
    factory, event, now = await capture(migrated_database_url, monkeypatch, tmp_path, kind)
    before = await protected_facts(factory)
    fault_engine = create_async_engine(migrated_database_url)
    fault_factory = async_sessionmaker(fault_engine, expire_on_commit=False)
    sends = []
    fault = None

    async def transport(request):
        sends.append(request.content)
        return httpx.Response(200, json={"code": 200})

    with ExitStack() as stack:

        async def after_publish(_message):
            nonlocal fault
            fault = stack.enter_context(
                lose_commit_ack(fault_engine, server_commits=server_commits)
            )

        if phase == "claim":
            fault = stack.enter_context(
                lose_commit_ack(fault_engine, server_commits=server_commits)
            )
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            options = dict(
                registry=registry(client, event),
                worker_id="commit-ack-test",
                claim_registered_only=True,
            )
            with pytest.raises(CommitAcknowledgementLost, match="COMMIT_OUTCOME_UNCONFIRMED"):
                await dispatch_outbox_once(
                    fault_factory,
                    clock=lambda: now,
                    after_publish=after_publish if phase == "ack" else None,
                    **options,
                )
            assert fault.boundary_calls == 1
            assert fault.server_commit_completed is server_commits
            # Independent engine reads the PostgreSQL result, not the errored Session.
            row, attempts = await saved(factory, event)
            if phase == "claim":
                assert not sends
                assert row.status == ("delivering" if server_commits else "queued")
                assert len(attempts) == int(server_commits)
            else:
                assert len(sends) == 1 and len(attempts) == 1
                assert row.status == ("published" if server_commits else "delivering")
            first_claim_rolled_back = phase == "claim" and not server_commits
            assert (
                await dispatch_outbox_once(
                    factory, clock=lambda: now + timedelta(seconds=62), **options
                )
                is first_claim_rolled_back
            )
            assert not await dispatch_outbox_once(
                factory, clock=lambda: now + timedelta(seconds=124), **options
            )
    row, attempts = await saved(factory, event)
    published = first_claim_rolled_back or (phase == "ack" and server_commits)
    assert row.status == ("published" if published else "dead_letter")
    assert (row.published_at is not None) is published
    assert row.attempt_count == row.max_attempts == len(attempts) == 1
    assert len(sends) == (0 if phase == "claim" and server_commits else 1)
    if not published:
        assert (attempts[0].outcome, attempts[0].error_code) == ("lost", "LEASE_EXPIRED")
    assert await protected_facts(factory) == before
    await fault_engine.dispose()
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["single", "batch"])
async def test_expired_monitor_lease_blocks_concurrent_send_and_late_ack(
    migrated_database_url, monkeypatch, tmp_path, kind
):
    factory, event, now = await capture(migrated_database_url, monkeypatch, tmp_path, kind)
    before = await protected_facts(factory)
    clock = [now]
    entered, release = asyncio.Event(), asyncio.Event()
    sends = []

    async def transport(request):
        sends.append(request.content)
        entered.set()
        await release.wait()
        return httpx.Response(200, json={"code": 200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        options = dict(
            registry=registry(client, event), clock=lambda: clock[0], claim_registered_only=True
        )
        first = asyncio.create_task(dispatch_outbox_once(factory, worker_id="first", **options))
        try:
            await asyncio.wait_for(entered.wait(), timeout=10)
            assert not await dispatch_outbox_once(factory, worker_id="second", **options)
            clock[0] += timedelta(seconds=62)
            assert not await dispatch_outbox_once(factory, worker_id="reclaimer", **options)
            terminal, attempts = await saved(factory, event)
            frozen = (terminal.status, terminal.dead_lettered_at, attempts[0].finished_at)
        finally:
            release.set()
        assert await first
        assert not await dispatch_outbox_once(factory, worker_id="late-poll", **options)
    row, attempts = await saved(factory, event)
    assert len(sends) == row.attempt_count == row.max_attempts == len(attempts) == 1
    assert frozen == (row.status, row.dead_lettered_at, attempts[0].finished_at)
    assert row.status == "dead_letter" and row.published_at is None
    assert (attempts[0].outcome, attempts[0].error_code) == ("lost", "LEASE_EXPIRED")
    assert await protected_facts(factory) == before
    await factory.kw["bind"].dispose()
