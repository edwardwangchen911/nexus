import asyncio

import httpx
import pytest


def test_optional_time_preserves_original_preparation_and_scan():
    from nexus.ops.research_monitor import _candidate_deadline

    assert _candidate_deadline(340, 400, 100) == 103
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
