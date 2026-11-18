import asyncio

import httpx
import pytest


def test_optional_time_preserves_original_preparation_and_scan():
    from nexus.ops.research_monitor import _candidate_deadline

    assert _candidate_deadline(340, 400, 100) == 110
    assert _candidate_deadline(309, 400, 100) is None
    assert _candidate_deadline(340, 317, 100) is None
    assert _candidate_deadline(311, 400, 100) == 101


@pytest.mark.asyncio
async def test_coordinator_exposes_and_resets_existing_preparation_deadline():
    from datetime import UTC, datetime

    from nexus.monitoring.host import PriceMonitorCoordinator, preparation_deadline

    observed = []

    async def prepare(bucket):
        observed.append(preparation_deadline.get())

    async def scan():
        assert preparation_deadline.get() is None

    monitor = PriceMonitorCoordinator(scan, prepare=prepare)
    before = asyncio.get_running_loop().time()
    assert monitor.kick(datetime(2026, 11, 17, 18, tzinfo=UTC))
    await monitor.close()
    assert before + 240 <= observed[0] <= asyncio.get_running_loop().time() + 240
    assert preparation_deadline.get() is None


@pytest.mark.asyncio
async def test_real_pool_wait_cannot_send_expired_candidate_headers():
    from nexus.ops.sec_atom_candidates import _SendDeadline

    held, released = asyncio.Event(), asyncio.Event()
    requests, handlers = [], []

    async def receiver(reader, writer):
        handlers.append(asyncio.current_task())
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            requests.append(head.split(b" ", 2)[1])
            if requests[-1] == b"/hold":
                held.set()
                await released.wait()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(receiver, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    checked = asyncio.Event()

    async def permit():
        checked.set()

    fence = _SendDeadline(asyncio.get_running_loop().time() + 300, permit)
    client = httpx.AsyncClient(
        trust_env=False,
        timeout=None,
        limits=httpx.Limits(max_connections=1),
        event_hooks={"request": [fence.attach]},
    )
    hold = asyncio.create_task(client.get(f"http://127.0.0.1:{port}/hold"))
    candidate = None
    try:
        await asyncio.wait_for(held.wait(), 5)
        checked.clear()
        candidate = asyncio.create_task(client.get(f"http://127.0.0.1:{port}/candidate"))
        await asyncio.wait_for(checked.wait(), 5)
        assert not candidate.done()
        fence.deadline = asyncio.get_running_loop().time() - 1
        released.set()
        await asyncio.wait_for(hold, 5)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(candidate, 5)
        assert requests == [b"/hold"]
    finally:
        released.set()
        for task in (hold, candidate):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(t for t in (hold, candidate) if t is not None), return_exceptions=True
        )
        await client.aclose()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers, return_exceptions=True)


@pytest.mark.parametrize(
    "kind,message,expected",
    [
        ("value", "SEC_ATOM_SOURCE_INVALID", "SEC_ATOM_SOURCE_INVALID"),
        ("value", "SEC_ATOM_SOURCE_INVALID private-text", "unverified"),
        ("http", "STATUS_REJECTED", "STATUS_REJECTED"),
        ("http", "private-text", "unverified"),
    ],
)
def test_candidate_diagnostic_never_logs_unlisted_error_text(kind, message, expected):
    from nexus.ops.sec_atom_candidates import _failure_code
    from nexus.providers.http import HttpPolicyError

    error = HttpPolicyError(message) if kind == "http" else ValueError(message)
    assert _failure_code(error) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("issuer_count", [2, 3])
async def test_equal_priority_candidates_take_first_turn_over_consecutive_buckets(
    monkeypatch, tmp_path, issuer_count
):
    from contextlib import asynccontextmanager
    from datetime import UTC, datetime, timedelta
    from types import SimpleNamespace
    from uuid import UUID

    from nexus.ops import sec_atom_candidates as atom
    from nexus.settings import EngineSettings

    class StopBeforeClaim(Exception):
        pass

    rules = [
        SimpleNamespace(instrument_id=UUID(int=number), issuer_cik=f"{number:010d}")
        for number in (2, 1, 3)[:issuer_count]
    ]

    @asynccontextmanager
    async def clean_turn(*_args):
        yield

    class Reader:
        def begin(self):
            return clean_turn()

        async def scalars(self, _statement):
            return rules

    reader = Reader()

    async def clean_dispose():
        pass

    class Factory:
        kw = {"bind": SimpleNamespace(dispose=clean_dispose)}

        def __call__(self):
            return self

        async def __aenter__(self):
            return reader

        async def __aexit__(self, *_exc):
            pass

    async def scope(_session, _settings, _account_id, _calendar_id, scheduled):
        return scheduled, tuple(rule.instrument_id for rule in rules)

    async def host_admitting(_session, _account_id):
        pass

    first_rules = []

    async def stop_before_claim(_session, rule):
        first_rules.append(rule.issuer_cik)
        raise StopBeforeClaim

    monkeypatch.setattr(atom, "session_factory", lambda _settings: Factory())
    monkeypatch.setattr(atom, "account_transaction_turn", clean_turn)
    monkeypatch.setattr(atom, "_scope", scope)
    monkeypatch.setattr(atom, "require_host_admitting", host_admitting)
    monkeypatch.setattr(atom, "_lock", stop_before_claim)
    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus_test_atom_order",
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=UUID(int=11),
        agent_paper_formal_model_config_id=UUID(int=12),
    )
    start = datetime(2026, 11, 18, 20, tzinfo=UTC)
    for i in range(2 * issuer_count):
        if i == issuer_count:
            rules.reverse()
        with pytest.raises(StopBeforeClaim):
            await atom.refresh_candidates(
                settings,
                calendar_authority_id=UUID(int=13),
                scheduled_at=start + timedelta(minutes=5 * i),
                environment={},
                repo_root=tmp_path,
            )
    assert len(set(first_rules[:issuer_count])) == issuer_count
    assert first_rules[:issuer_count] == first_rules[issuer_count:]
