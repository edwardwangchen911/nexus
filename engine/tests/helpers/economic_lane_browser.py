"""Owned loopback API plus normal Playwright/Vinext; isolated test DB only."""

import asyncio
import json
import os
import socket
import time
from pathlib import Path

import uvicorn
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET


async def verify_live_economic_pages(application, *, case: str) -> None:
    for port in (55881, 55882):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
    receipts = []

    async def observed_application(scope, receive, send):
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
                        "status": status,
                        "elapsed": time.monotonic() - started,
                    }
                )

    server = uvicorn.Server(
        uvicorn.Config(
            observed_application,
            host="127.0.0.1",
            port=55881,
            access_log=False,
            log_level="warning",
        )
    )
    server_task = asyncio.create_task(server.serve())
    process = None
    try:
        for _ in range(100):
            if server.started:
                break
            if server_task.done():
                await server_task
                raise AssertionError("isolated API did not start")
            await asyncio.sleep(0.05)
        if not server.started:
            raise AssertionError("isolated API startup timeout")
        environment = os.environ.copy()
        environment.update(
            {
                "NEXUS_E2E_PORT": "55882",
                "NEXUS_E2E_ECONOMIC_LANE_LIVE_CASE": case,
                "NEXUS_E2E_ECONOMIC_LANE_FIXTURE_DIR": os.environ[
                    "NEXUS_TEST_ECONOMIC_LANE_EXPORT_DIR"
                ],
                "NEXUS_ENGINE_BASE_URL": "http://127.0.0.1:55881",
                "NEXUS_ENGINE_HMAC_CURRENT_KEY_ID": TEST_HMAC_KEY_ID,
                "NEXUS_ENGINE_HMAC_CURRENT_SECRET": TEST_HMAC_SECRET,
            }
        )
        process = await asyncio.create_subprocess_exec(
            "npx",
            "playwright",
            "test",
            "tests/e2e/economic-context.spec.ts",
            "--workers=1",
            "--reporter=line",
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await asyncio.wait_for(process.communicate(), timeout=240)
        log = Path(f".secrets/economic-pair-lifecycle/lane-browser-{case}.log")
        log.write_bytes(output)
        log.chmod(0o600)
        assert process.returncode == 0, f"live browser {case} failed; inspect protected log"
    finally:
        if process is not None and process.returncode is None:
            process.terminate()
            await process.wait()
        receipt = Path(f".secrets/economic-pair-lifecycle/lane-browser-{case}-http.json")
        receipt.write_text(json.dumps(receipts))
        receipt.chmod(0o600)
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=15)
