"""Real httpcore pool wait; qualification is a clock component, not SQL evidence."""

import asyncio
from uuid import UUID

import httpx
import pytest
from nexus.monitoring import durable_sec_permission as permission


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", ["permission", "work_deadline", "original_callback"])
async def test_real_pool_wait_cannot_send_headers_after_fence_expiry(monkeypatch, expired):
    loop = asyncio.get_running_loop()
    hold_entered, release_hold, acquired = (asyncio.Event() for _ in range(3))
    requests, handlers, qualifications, sessions = [], [], [], []
    expires_at = loop.time() + 2

    class Session:
        closed = False

        async def begin(self):
            pass

        async def rollback(self):
            pass

        async def close(self):
            self.closed = True

    def factory():
        value = Session()
        sessions.append(value)
        return value

    async def qualify(session, run_id):
        qualifications.append(loop.time())
        if expired == "permission" and loop.time() >= expires_at:
            raise ValueError("SEC_RESEARCH_PERMISSION_INVALID")
        acquired.set()
        return True

    monkeypatch.setattr(permission, "require_new_send", qualify)

    async def handler(reader, writer):
        handlers.append(asyncio.current_task())
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            method = headers.split(b" ", 1)[0]
            requests.append(method)
            if method == b"GET":
                hold_entered.set()
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nConnection: close\r\n\r\n")
                await writer.drain()
                await release_hold.wait()
                writer.write(b"x")
            else:
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = httpx.AsyncClient(
        transport=httpx.AsyncHTTPTransport(limits=httpx.Limits(max_connections=1)),
        trust_env=False,
        timeout=None,
    )
    hold = asyncio.create_task(client.get(f"http://127.0.0.1:{port}/hold"))
    fence = permission.SecRequestFence(factory, UUID(int=1))
    fence.work_deadline = (
        expires_at if expired in ("work_deadline", "original_callback") else loop.time() + 30
    )
    request = client.build_request(
        "POST", f"http://127.0.0.1:{port}/responses", json={"stream": True}
    )
    traces = []

    async def traced(name, info):
        traces.append(name)
        if expired == "original_callback" and name == "http11.send_request_headers.started":
            await asyncio.sleep(max(0, expires_at - loop.time()) + 0.02)

    request.extensions["trace"] = traced

    async def protected_post():
        try:
            await fence.acquire(request)
            response = await client.send(request, stream=True)
            await response.aclose()
            return None
        except Exception as error:
            return error
        finally:
            await fence.release()

    post = None
    try:
        await asyncio.wait_for(hold_entered.wait(), 5)
        post = asyncio.create_task(protected_post())
        await asyncio.wait_for(acquired.wait(), 5)
        await asyncio.sleep(0)
        assert not post.done() and requests == [b"GET"]
        # Actual two-second expiry while httpcore's one-slot pool is occupied.
        if expired != "original_callback":
            await asyncio.sleep(max(0, expires_at - loop.time()) + 0.02)
        release_hold.set()
        assert (await asyncio.wait_for(hold, 5)).content == b"x"
        result = await asyncio.wait_for(post, 5)
        assert requests == [b"GET"], "expired protected POST must send zero headers"
        assert isinstance(result, ValueError if expired == "permission" else TimeoutError)
        assert len(qualifications) == (2 if expired == "permission" else 1)
        assert "http11.send_request_headers.complete" not in traces
        assert fence.session is None and all(value.closed for value in sessions)
    finally:
        release_hold.set()
        for task in (hold, post):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (hold, post) if task is not None), return_exceptions=True
        )
        await client.aclose()
        server.close()
        await server.wait_closed()
        await asyncio.gather(*handlers, return_exceptions=True)
