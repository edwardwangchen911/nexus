"""Owned TCP API and normal BFF/browser over one disposable monitoring database."""

import asyncio
import json
import os
import socket
import time
from pathlib import Path

import uvicorn
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET


async def verify_monitoring_browser(
    application,
    *,
    directory: Path,
    spec_path: str = "tests/e2e/monitoring-readview-live.spec.ts",
) -> None:
    for port in (8129, 4193):
        with socket.socket() as probe:
            # Match the server's normal TCP restart semantics; TIME_WAIT is not
            # an active owner. A listening socket still prevents this bind.
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
    receipts = []

    async def observed(scope, receive, send):
        started = time.monotonic()
        status = None

        async def observed_send(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await application(scope, receive, observed_send)
        finally:
            if scope["type"] == "http":
                receipts.append(
                    {
                        "method": scope["method"],
                        "path": scope["path"],
                        "status": status,
                        "elapsed": time.monotonic() - started,
                    }
                )

    server = uvicorn.Server(
        uvicorn.Config(
            observed,
            host="127.0.0.1",
            port=8129,
            access_log=False,
            log_level="warning",
        )
    )
    task = asyncio.create_task(server.serve())
    process = None
    try:
        for _ in range(100):
            if server.started:
                break
            if task.done():
                await task
                raise AssertionError("owned monitoring API did not start")
            await asyncio.sleep(0.05)
        assert server.started
        environment = {
            key: os.environ[key]
            for key in ("PATH", "LANG", "LC_ALL", "TZ", "TMPDIR", "TMP", "TEMP")
            if key in os.environ
        }
        environment.update(
            {
                "NEXUS_ENV": "test",
                "NEXUS_E2E_PORT": "4193",
                "NEXUS_TEST_MONITOR_BROWSER_DIR": str(directory.resolve()),
                "NEXUS_ENGINE_BASE_URL": "http://127.0.0.1:8129",
                "NEXUS_ENGINE_HMAC_CURRENT_KEY_ID": TEST_HMAC_KEY_ID,
                "NEXUS_ENGINE_HMAC_CURRENT_SECRET": TEST_HMAC_SECRET,
                "CLOUDFLARE_LOAD_DEV_VARS_FROM_DOT_ENV": "false",
                "CLOUDFLARE_INCLUDE_PROCESS_ENV": "false",
            }
        )
        process = await asyncio.create_subprocess_exec(
            "npx",
            "playwright",
            "test",
            spec_path,
            "--workers=1",
            "--reporter=line",
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await asyncio.wait_for(process.communicate(), timeout=180)
        (directory / "browser.log").write_bytes(output)
        assert process.returncode == 0, "inspect protected monitoring browser log"
        assert receipts and all(row["method"] == "GET" for row in receipts)
        assert all(row["elapsed"] < 10 for row in receipts)
    finally:
        if process is not None and process.returncode is None:
            process.terminate()
            await process.wait()
        (directory / "http.json").write_text(json.dumps(receipts))
        server.should_exit = True
        await asyncio.wait_for(task, timeout=15)
