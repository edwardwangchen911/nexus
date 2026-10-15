"""A failed browser check retains process facts before fixture cleanup."""

import asyncio
import json
from contextlib import nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from helpers import controlled_browser as browser


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "web_exit,timeout,api_done,archive_blocked",
    [
        (None, False, False, False),
        (-9, False, False, False),
        (None, True, False, False),
        (None, False, True, False),
        (None, False, False, True),
    ],
)
async def test_failure_archives_pre_cleanup_process_state(
    tmp_path, monkeypatch, web_exit, timeout, api_done, archive_blocked
):
    executable = tmp_path / "node_modules/.bin/vinext"
    executable.parent.mkdir(parents=True)
    executable.touch()
    monkeypatch.setattr(
        browser, "__file__", str(tmp_path / "engine/tests/helpers/controlled_browser.py")
    )
    monkeypatch.setattr(
        browser.subprocess, "run", Mock(return_value=SimpleNamespace(stdout="a" * 40))
    )
    monkeypatch.setattr(browser, "heartbeat_agent_paper_loop", AsyncMock())
    monkeypatch.setattr(browser, "app", Mock(return_value=object()))
    monkeypatch.setattr(browser.uvicorn, "Config", Mock())
    monkeypatch.setattr(browser, "_web_project_root", lambda root: nullcontext(root))
    monkeypatch.setattr(browser, "_wait_for_web", AsyncMock())
    server_entered = asyncio.Event()

    class Server:
        started = True
        should_exit = False

        async def serve(self, *, sockets):
            try:
                server_entered.set()
                if api_done:
                    return
                while not self.should_exit:
                    await asyncio.sleep(0)
            finally:
                for listener in sockets:
                    listener.close()

    server = Server()
    monkeypatch.setattr(browser.uvicorn, "Server", Mock(return_value=server))

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, *args, **kwargs):
            return SimpleNamespace(status_code=200)

    monkeypatch.setattr(browser.httpx, "AsyncClient", lambda **kwargs: Client())

    class Process:
        def __init__(self, code):
            self.returncode = code
            self.terminated = False

        def terminate(self):
            self.terminated = True
            self.returncode = -15

        async def wait(self):
            return self.returncode

        async def communicate(self):
            await server_entered.wait()
            if timeout:
                raise TimeoutError("private timeout detail")
            return b"private response and headers", None

    web = Process(web_exit)
    test_process = Process(None if timeout else 1)
    monkeypatch.setattr(
        browser.asyncio, "create_subprocess_exec", AsyncMock(side_effect=[web, test_process])
    )
    expected_error = TimeoutError if timeout else AssertionError
    if archive_blocked:
        (tmp_path / "test-results").write_text("preserve existing file")
    with pytest.raises(expected_error) as failure:
        await browser.verify_controlled_browser(
            "postgresql+asyncpg://test:test@127.0.0.1:55440/test",
            api_now=datetime(2026, 10, 15, 14, tzinfo=UTC),
            cycle_id=UUID(int=1),
            order_ids=(),
        )
    assert web.terminated is (web_exit is None)
    assert server.should_exit is True
    if archive_blocked:
        assert (tmp_path / "test-results").read_text() == "preserve existing file"
        assert failure.value.__notes__ == ["CONTROLLED_BROWSER_DIAGNOSTIC_WRITE_FAILED"]
        return
    files = list((tmp_path / "test-results").glob("controlled-browser-failure-*.json"))
    assert len(files) == 1
    result = json.loads(files[0].read_text())
    assert result == {
        "revision": "a" * 40,
        "stage": "browser_readback",
        "web_returncode": web_exit,
        "browser_returncode": None if timeout else 1,
        "api_task_status": "completed" if api_done else "running",
        "api_started": True,
        "error_type": "TimeoutError" if timeout else "AssertionError",
    }
    assert "private" not in files[0].read_text()
