"""Actual httpcore trace; immutable inputs/DB clock are controlled components, not PG proof."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
from nexus.monitoring import durable_sec_permission as permission
from nexus.monitoring import durable_sec_send_inputs as inputs
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.research.sec_disclosure_index import SecDisclosureIndex, SecIndexEntry


@pytest.mark.asyncio
@pytest.mark.parametrize("expires", ("source", "plan", "neither"))
async def test_final_source_read_wait_cannot_send_expired_headers(monkeypatch, expires):
    initial = datetime(2026, 11, 13, 15, tzinfo=UTC)
    clock = [initial]
    instrument, identity, snapshot, plan_id, run_id = (UUID(int=x) for x in range(1, 6))
    entry = SecIndexEntry(
        accession_number="0000123456-26-000001",
        form="8-K",
        accepted_at=initial - timedelta(seconds=10),
        filed_at=initial.replace(hour=0),
        primary_document="filing.htm",
    )
    index = SecDisclosureIndex(
        instrument_id=instrument,
        identity_snapshot_id=identity,
        issuer_cik="0000123456",
        received_at=initial,
        source_url="https://data.sec.gov/submissions/CIK0000123456.json",
        raw_content_hash="a" * 64,
        entries=(entry,),
        coverage_start_at=entry.accepted_at,
        history_complete=True,
    )
    plan = SimpleNamespace(
        effective_at=initial - timedelta(seconds=60),
        valid_until=initial + timedelta(seconds=1 if expires == "plan" else 600),
    )
    scope = dict(
        account_id=UUID(int=6),
        instrument_id=instrument,
        plan_id=plan_id,
        event_key="sec:0000123456:0000123456-26-000001",
        entry=entry.model_dump(mode="json"),
    )
    sessions, source_reads, received, handlers = [], [], [], []

    class Session:
        closed = False

        async def begin(self):
            pass

        async def rollback(self):
            pass

        async def close(self):
            self.closed = True

        async def execute(self, statement, params=None):
            if statement is inputs._SCOPES:
                return SimpleNamespace(mappings=lambda: (scope,))
            assert "sec_research_require_send_run" in str(statement)
            return None

        async def scalar(self, statement, params=None):
            sql = str(statement)
            if statement is inputs._LATEST:
                return snapshot
            if "clock_timestamp" in sql:
                return clock[0]
            assert any(
                x in sql
                for x in (
                    "to_regclass",
                    "sec_research_run_has_dependencies",
                    "pg_try_advisory_xact_lock",
                )
            ), sql
            return True

        async def get(self, model, key):
            if model is DataSnapshot:
                assert key == snapshot
                return SimpleNamespace(source=inputs.SOURCE)
            assert model is MonitorPlan and key == plan_id
            return plan

    def factory():
        session = Session()
        sessions.append(session)
        return session

    async def strong_reader(session, store, *, snapshot_id, as_of):
        assert snapshot_id == snapshot and 0 <= (as_of - index.received_at).total_seconds() <= 300
        return None, SimpleNamespace(index=index, issues=())

    async def current_source(session, value, as_of, *, exclude_run_id):
        assert value is plan and exclude_run_id == run_id
        source_reads.append(as_of)
        if len(source_reads) == 2:
            # The final source-current query began while qualified but awaited past expiry.
            await asyncio.sleep(0)
            if expires == "source":
                clock[0] += timedelta(seconds=301)
            elif expires == "plan":
                clock[0] += timedelta(seconds=2)
        return True

    monkeypatch.setattr(inputs, "configured_exhibit_store", lambda session: object())
    monkeypatch.setattr(inputs, "read_durable_index_capture", strong_reader)
    monkeypatch.setattr(inputs, "disclosure_source_current", current_source)

    async def handler(reader, writer):
        handlers.append(asyncio.current_task())
        try:
            header = await reader.readuntil(b"\r\n\r\n")
            received.append(header.split(b" ", 1)[0])
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    fence = permission.SecRequestFence(factory, run_id)
    fence.work_deadline = asyncio.get_running_loop().time() + 30
    try:
        async with httpx.AsyncClient(trust_env=False, timeout=5) as client:
            request = client.build_request("POST", f"http://127.0.0.1:{port}/responses", json={})
            await fence.acquire(request)
            assert len(source_reads) == 1 and clock[0] == initial
            error = None
            try:
                response = await client.send(request)
                await response.aclose()
            except ValueError as exc:
                error = exc
            finally:
                await fence.release()
            assert len(source_reads) == 2
            if expires == "neither":
                assert error is None and received == [b"POST"]
            else:
                assert received == [], (
                    "final source-query await must not admit expired POST headers"
                )
                assert str(error) == "SEC_RESEARCH_PERMISSION_INVALID"
            assert all(session.closed for session in sessions)
    finally:
        await fence.release()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers, return_exceptions=True)
