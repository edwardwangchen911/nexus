"""Short committed collection claims protect exact sealed batches across processes."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_monitor_batches_pg import installed
from integration.test_monitor_increment_pg import append_fresh_inputs
from nexus.monitoring.batches import scan_stock_batch
from nexus.persistence.models.runtime import AuditEvent, Job, JobLease
from sqlalchemy import func, select


async def setup(database_url, monkeypatch, tmp_path, *, added=2, kinds=("price", "disclosure")):
    factory, settings, store, captured, pointer = await installed(
        database_url, monkeypatch, tmp_path, added=added, kinds=kinds
    )
    monkeypatch.setattr(
        "nexus.monitoring.filing_archive.configured_exhibit_store", lambda session: store
    )
    monkeypatch.setattr(store, "read_evidence", store.read_raw, raising=False)
    async with factory() as session, session.begin():
        scanned = await scan_stock_batch(
            session,
            account_id=pointer.account_id,
            instrument_id=pointer.instrument_id,
            now=captured["clock"][0],
        )
    async with factory() as session:
        later = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
        before = await session.scalar(select(func.count()).select_from(Job))
    captured["clock"][0] = later
    await _set_test_insert_clock(factory, later)
    await append_fresh_inputs(factory, pointer.instrument_id, later)
    settings = settings.model_copy(update={"agent_paper_cycle_enabled": True})
    return factory, settings, store, captured, UUID(scanned["batch_id"]), before


def mocked_collector(monkeypatch, store, captured, *, fail=False, wait=None):
    from nexus.ops import monitor_batch_prepare as preparation
    from nexus.ops import monitor_content_local

    real = monitor_content_local.collect_filing_content
    calls = []

    class ClockMeta(type):
        def __instancecheck__(cls, value):
            return isinstance(value, datetime)

    class Clock(datetime, metaclass=ClockMeta):
        @classmethod
        def now(cls, tz=None):
            return captured["clock"][0].astimezone(tz or UTC)

    monkeypatch.setattr(monitor_content_local, "datetime", Clock)

    def respond(request):
        calls.append(str(request.url))
        if fail:
            return httpx.Response(503)
        name = request.url.path.rsplit("/", 1)[-1]
        body = b"<p>Exact synthetic financial disclosure requiring investment review.</p>"
        if name.endswith("-index.html"):
            # The frozen fixture source uses new2.htm and new3.htm.
            number = int(name.split("-")[-2])
            primary = f"new{number}.htm"
            raw = (
                '<table summary="Document Format Files"><tr><td>1</td><td>Report</td>'
                f'<td><a href="{primary}">{primary}</a></td>'
                f"<td>8-K</td><td>{len(body)}</td></tr></table>"
            ).encode()
            return httpx.Response(200, content=raw)
        return httpx.Response(200, content=body)

    async def collect(settings, **kwargs):
        if wait:
            entered, release = wait
            entered.set()
            await release.wait()
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            return await real(settings, **kwargs, http_client=client, store=store)

    monkeypatch.setattr(preparation, "collect_filing_content", collect)
    return calls


@pytest.mark.asyncio
async def test_one_exact_filing_per_operation_then_zero_http_prepared_reuse(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, store, captured, batch_id, jobs = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    from nexus.ops.monitor_batch_prepare import (
        confirm_batch_preparation,
        inspect_batch_preparation,
        prepare_pending_batch,
    )

    calls = mocked_collector(monkeypatch, store, captured)
    first = await prepare_pending_batch(
        settings, batch_id=batch_id, repo_root=tmp_path, sec_contact="test@example.invalid"
    )
    assert first["status"] == "preparing", first
    assert first["provider_requests"] == 2 and len(calls) == 2
    assert await confirm_batch_preparation(
        settings, account_id=settings.agent_paper_account_id, batch_id=batch_id
    )
    second = await prepare_pending_batch(
        settings, batch_id=batch_id, repo_root=tmp_path, sec_contact="test@example.invalid"
    )
    assert second["status"] == "prepared", second
    assert second["provider_requests"] == 2 and len(calls) == 4
    third = await prepare_pending_batch(settings, batch_id=batch_id, repo_root=tmp_path)
    assert third["status"] == "prepared" and third["snapshot_id"] == second["snapshot_id"]
    assert third["provider_requests"] == 0 and len(calls) == 4
    async with factory() as session:
        assert await inspect_batch_preparation(session, batch_id) == "prepared"
        assert await session.scalar(select(func.count()).select_from(Job)) == jobs
        claims = list(
            await session.scalars(
                select(AuditEvent).where(
                    AuditEvent.action == "research.monitor.material_collection_claimed"
                )
            )
        )
        assert len(claims) == 2
        assert {c.payload["accession"] for c in claims} == {
            "0000123456-26-000002",
            "0000123456-26-000003",
        }
        assert len({c.payload["archive_dir"] for c in claims}) == 2


@pytest.mark.asyncio
async def test_committed_claim_blocks_second_process_without_holding_network_transaction(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, store, captured, batch_id, _ = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    from nexus.ops.monitor_batch_prepare import (
        confirm_batch_preparation,
        inspect_batch_preparation,
        prepare_pending_batch,
    )

    entered, release = asyncio.Event(), asyncio.Event()
    calls = mocked_collector(monkeypatch, store, captured, wait=(entered, release))
    task = asyncio.create_task(
        prepare_pending_batch(
            settings, batch_id=batch_id, repo_root=tmp_path, sec_contact="test@example.invalid"
        )
    )
    await entered.wait()
    try:
        async with factory() as session:
            assert await inspect_batch_preparation(session, batch_id) == "unknown"
        assert not await confirm_batch_preparation(
            settings, account_id=settings.agent_paper_account_id, batch_id=batch_id
        )
        second = await prepare_pending_batch(
            settings, batch_id=batch_id, repo_root=tmp_path, sec_contact="test@example.invalid"
        )
        assert second["status"] == "unknown" and calls == []
    finally:
        release.set()
    result = await task
    assert result["status"] == "preparing", result
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_failed_collection_is_terminal_for_automatic_preparation(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, store, captured, batch_id, _ = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    from nexus.ops.monitor_batch_prepare import (
        confirm_batch_preparation,
        inspect_batch_preparation,
        prepare_pending_batch,
    )

    calls = mocked_collector(monkeypatch, store, captured, fail=True)
    first = await prepare_pending_batch(
        settings, batch_id=batch_id, repo_root=tmp_path, sec_contact="test@example.invalid"
    )
    assert first["status"] == "blocked" and first["provider_requests"] == 1, first
    again = await prepare_pending_batch(
        settings, batch_id=batch_id, repo_root=tmp_path, sec_contact="test@example.invalid"
    )
    assert again["status"] == "blocked" and len(calls) == 1
    assert await confirm_batch_preparation(
        settings, account_id=settings.agent_paper_account_id, batch_id=batch_id
    )
    async with factory() as session:
        assert await inspect_batch_preparation(session, batch_id) == "blocked"


@pytest.mark.asyncio
async def test_cancelled_claim_survives_restart_and_never_replays(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, store, captured, batch_id, _ = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    from nexus.ops.monitor_batch_prepare import (
        confirm_batch_preparation,
        inspect_batch_preparation,
        prepare_pending_batch,
    )

    entered, release = asyncio.Event(), asyncio.Event()
    calls = mocked_collector(monkeypatch, store, captured, wait=(entered, release))
    task = asyncio.create_task(
        prepare_pending_batch(
            settings, batch_id=batch_id, repo_root=tmp_path, sec_contact="test@example.invalid"
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with factory() as session:
        assert await inspect_batch_preparation(session, batch_id) == "unknown"
    assert not await confirm_batch_preparation(
        settings, account_id=settings.agent_paper_account_id, batch_id=batch_id
    )
    again = await prepare_pending_batch(
        settings, batch_id=batch_id, repo_root=tmp_path, sec_contact="test@example.invalid"
    )
    assert again["status"] == "unknown" and calls == []


@pytest.mark.asyncio
async def test_other_account_cannot_record_or_collect_this_batch(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, store, captured, batch_id, _ = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    from nexus.ops.monitor_batch_prepare import confirm_batch_preparation, prepare_pending_batch

    calls = mocked_collector(monkeypatch, store, captured)
    wrong = settings.model_copy(update={"agent_paper_account_id": UUID(int=999)})
    with pytest.raises(ValueError, match="MONITOR_BATCH_UNAVAILABLE"):
        await prepare_pending_batch(
            wrong, batch_id=batch_id, repo_root=tmp_path, sec_contact="test@example.invalid"
        )
    assert calls == []
    assert not await confirm_batch_preparation(wrong, account_id=UUID(int=999), batch_id=batch_id)
    async with factory() as session:
        events = list(
            await session.scalars(
                select(AuditEvent).where(
                    AuditEvent.action.in_(
                        (
                            "research.monitor.material_collection_claimed",
                            "research.monitor.material_preparation_result",
                        )
                    )
                )
            )
        )
        assert events == []


@pytest.mark.asyncio
async def test_price_only_batch_needs_neither_contact_nor_archive_directory(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, store, captured, batch_id, _ = await setup(
        migrated_database_url, monkeypatch, tmp_path, kinds=("price",)
    )
    from nexus.ops.monitor_batch_prepare import prepare_pending_batch

    calls = mocked_collector(monkeypatch, store, captured)
    result = await prepare_pending_batch(
        settings, batch_id=batch_id, repo_root=tmp_path / "not-created"
    )
    assert result["status"] == "prepared", result
    assert result["provider_requests"] == 0 and calls == []
    assert not (tmp_path / "not-created").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["overflow", "account_unknown"])
async def test_source_overflow_and_existing_unknown_account_cannot_claim_collection(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    factory, settings, store, captured, batch_id, _ = await setup(
        migrated_database_url, monkeypatch, tmp_path, added=33 if mode == "overflow" else 2
    )
    from nexus.ops import monitor_batch_prepare as preparation

    if mode == "account_unknown":

        async def unavailable(*args, **kwargs):
            raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")

        monkeypatch.setattr(preparation, "require_manual_account_available", unavailable)
    calls = mocked_collector(monkeypatch, store, captured)
    result = await preparation.prepare_pending_batch(
        settings, batch_id=batch_id, repo_root=tmp_path, sec_contact="test@example.invalid"
    )
    assert result["status"] == "blocked", result
    assert result["reason"] == (
        "MONITOR_PENDING_OVERFLOW" if mode == "overflow" else "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"
    )
    assert calls == []
    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == preparation.CLAIM)
            )
            == 0
        )
        assert await preparation.inspect_batch_preparation(session, batch_id) == "blocked"
