"""Real loopback httpcore traces release PostgreSQL send locks before body/usage work."""

import asyncio
import json
from uuid import UUID

import httpx
import pytest
from nexus.agents.responses_request import send_response_stage
from nexus.monitoring import durable_sec_permission as permission
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from unit.agents.test_responses_stream import event


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_before_headers", [False, True])
async def test_real_headers_or_cancel_release_send_lock(
    migrated_database_url, monkeypatch, cancel_before_headers
):
    engine = create_async_engine(migrated_database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    acquired, send_allowed, connected, body_allowed = (asyncio.Event() for _ in range(4))
    key = 1273419
    traces = []
    handlers = []

    async def require(session, run_id):
        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
        acquired.set()
        await send_allowed.wait()
        return True

    monkeypatch.setattr(permission, "require_new_send", require)

    async def handler(reader, writer):
        handlers.append(asyncio.current_task())
        try:
            if cancel_before_headers:
                await reader.read(
                    1
                )  # Actual TLS ClientHello; the loopback server never answers it.
                connected.set()
                await body_allowed.wait()
            else:
                await reader.readuntil(b"\r\n\r\n")
                connected.set()
                payload = ("data: " + json.dumps(event("response.completed")) + "\n\n").encode()
                writer.write(
                    (
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                        f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n"
                    ).encode()
                )
                await writer.drain()
                await body_allowed.wait()
                writer.write(payload)
                await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = httpx.AsyncClient(transport=httpx.AsyncHTTPTransport(), trust_env=False, timeout=None)
    request = httpx.Request(
        "POST",
        f"{'https' if cancel_before_headers else 'http'}://127.0.0.1:{port}/responses",
        json={"stream": True},
    )

    async def traced(name, info):
        traces.append(name)

    request.extensions["trace"] = traced
    fence = permission.SecRequestFence(factory, UUID(int=1))
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(
        send_response_stage(
            client,
            request,
            work_deadline=loop.time() + 20,
            cleanup_deadline=loop.time() + 25,
            request_fence=fence,
        )
    )
    try:
        await asyncio.wait_for(acquired.wait(), 5)
        async with factory() as check, check.begin():
            assert (
                await check.scalar(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": key})
                is False
            )
        send_allowed.set()
        await asyncio.wait_for(connected.wait(), 5)
        if cancel_before_headers:
            task.cancel("controlled pre-header cancellation")
            result = await asyncio.wait_for(task, 5)
            assert isinstance(result.error, asyncio.CancelledError)
            assert result.stage is None
            assert not any(name.endswith("send_request_headers.complete") for name in traces)
        else:
            # Header completion releases the database lock while the response body is withheld.
            async with factory() as check, check.begin():
                await asyncio.wait_for(
                    check.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key}), 5
                )
            assert "http11.send_request_headers.complete" in traces
            assert not task.done() and fence.session is None
            body_allowed.set()
            result = await asyncio.wait_for(task, 5)
            assert result.error is None and result.stage.facts.usage == (7, 3)
        async with factory() as check, check.begin():
            assert (
                await check.scalar(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": key})
                is True
            )
        assert fence.session is None
    finally:
        send_allowed.set()
        body_allowed.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await client.aclose()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers, return_exceptions=True)
        await engine.dispose()
