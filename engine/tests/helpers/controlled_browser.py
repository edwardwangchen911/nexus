"""Read an isolated controlled Paper cycle through the real local BFF and browser."""

import asyncio
import os
import socket
import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import httpx
import uvicorn
from helpers.hmac import TEST_HMAC_KEY_ID, TEST_HMAC_SECRET
from helpers.paper_api import app, headers
from helpers.paper_api import settings as paper_api_settings
from nexus.ops.agent_paper_loop import heartbeat_agent_paper_loop


def _listen_socket() -> socket.socket:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    return listener


@contextmanager
def _web_project_root(root: Path) -> Iterator[Path]:
    """Keep a controlled Web instance separate from an existing local dev server."""
    if not (root / ".vinext/dev/lock.json").is_file():
        yield root
        return
    web_paths = (
        "app",
        "worker",
        "public",
        "package.json",
        "package-lock.json",
        "vite.config.ts",
        "next.config.ts",
        "tsconfig.json",
        "postcss.config.mjs",
        "playwright.controlled-paper.config.ts",
        "tests/e2e-local/portfolio-controlled.spec.ts",
        "tests/e2e-local/agent-paper-readonly.spec.ts",
    )
    dirty = subprocess.run(
        ("git", "-C", str(root), "status", "--porcelain", "--", *web_paths),
        check=True,
        capture_output=True,
        text=True,
    )
    if dirty.stdout.strip():
        raise AssertionError("CONTROLLED_WEB_CHECKOUT_DIRTY")
    with tempfile.TemporaryDirectory(prefix="nexus-controlled-web-") as directory:
        checkout = Path(directory) / "checkout"
        created = subprocess.run(
            ("git", "-C", str(root), "worktree", "add", "--detach", str(checkout), "HEAD"),
            check=False,
            capture_output=True,
            text=True,
        )
        if created.returncode != 0:
            raise AssertionError("CONTROLLED_WEB_ISOLATION_FAILED")
        try:
            (checkout / "node_modules").symlink_to(root / "node_modules", target_is_directory=True)
            yield checkout
        finally:
            (checkout / "node_modules").unlink(missing_ok=True)
            removed = subprocess.run(
                ("git", "-C", str(root), "worktree", "remove", "--force", str(checkout)),
                check=False,
                capture_output=True,
                text=True,
            )
            if removed.returncode != 0:
                raise AssertionError("CONTROLLED_WEB_CLEANUP_FAILED")


async def _wait_for_web(url: str, process: asyncio.subprocess.Process) -> None:
    async with httpx.AsyncClient(timeout=2) as client:
        for _ in range(200):
            if process.returncode is not None:
                raise AssertionError("controlled Web process exited before readiness")
            try:
                response = await client.get(f"{url}/__debug")
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.1)
    raise AssertionError("controlled Web readiness timed out")


async def verify_controlled_browser(
    database_url: str,
    *,
    api_now: datetime,
    cycle_id: UUID,
    order_ids: tuple[UUID, UUID],
    candidate_count: int = 2,
    first_partial: bool = False,
    candidate_stage_capacity_reads: bool = False,
    expected_defer_count: int | None = None,
) -> None:
    root = Path(__file__).resolve().parents[3]
    if not (root / "node_modules/.bin/vinext").exists():
        raise AssertionError("controlled browser dependencies are unavailable")
    revision_result = subprocess.run(
        ("git", "-C", str(root), "rev-parse", "--verify", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    )
    revision = revision_result.stdout.strip()
    if len(revision) != 40 or any(character not in "0123456789abcdef" for character in revision):
        raise AssertionError("controlled browser release revision is invalid")
    heartbeat_at = datetime.now(UTC)
    await heartbeat_agent_paper_loop(
        paper_api_settings(database_url).model_copy(update={"release_revision": revision}),
        worker_id=f"agent-paper-loop-{UUID(int=1)}",
        started_at=heartbeat_at - timedelta(minutes=1),
        seen_at=heartbeat_at,
    )
    api_socket = _listen_socket()
    api_port = api_socket.getsockname()[1]
    web_socket = _listen_socket()
    web_port = web_socket.getsockname()[1]
    web_socket.close()
    server = uvicorn.Server(
        uvicorn.Config(
            app(
                database_url,
                now=api_now,
                release_revision=revision,
                candidate_stage_capacity_reads=candidate_stage_capacity_reads,
            ),
            log_config=None,
            access_log=False,
        )
    )
    server_task = asyncio.create_task(server.serve(sockets=[api_socket]))
    web_process: asyncio.subprocess.Process | None = None
    web_log = tempfile.TemporaryFile(mode="w+b")
    isolation = ExitStack()
    try:
        for _ in range(100):
            if server.started:
                break
            if server_task.done():
                raise AssertionError("controlled Engine API stopped before readiness")
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("controlled Engine API readiness timed out")
        order_path = "/internal/v1/paper/orders?limit=50"
        async with httpx.AsyncClient(timeout=5) as client:
            direct = await client.get(
                f"http://127.0.0.1:{api_port}{order_path}",
                headers=headers("GET", order_path),
            )
            assert direct.status_code == 200, f"controlled Engine API: {direct.status_code}"
        environment = {
            key: value
            for key in ("PATH", "HOME", "LANG", "TMPDIR")
            if (value := os.environ.get(key)) is not None
        }
        environment.update(
            NEXUS_ENV="test",
            NEXUS_RELEASE_REVISION=revision,
            AGENT_RUNTIME_MODE="fixture",
            NEXUS_PROVIDER_JOBS_ENABLED="false",
            NEXUS_ENGINE_BASE_URL=f"http://127.0.0.1:{api_port}",
            NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
            NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
            CLOUDFLARE_LOAD_DEV_VARS_FROM_DOT_ENV="false",
            CLOUDFLARE_INCLUDE_PROCESS_ENV="false",
            NEXUS_CONTROLLED_WEB_URL=f"http://127.0.0.1:{web_port}",
            NEXUS_CONTROLLED_CYCLE_ID=str(cycle_id),
            NEXUS_CONTROLLED_ORDER_IDS=",".join(str(order_id) for order_id in order_ids),
            NEXUS_CONTROLLED_CANDIDATE_COUNT=str(candidate_count),
            NEXUS_CONTROLLED_FIRST_PARTIAL="1" if first_partial else "0",
            NEXUS_CONTROLLED_DEFER_COUNT=str(
                candidate_count - 2 if expected_defer_count is None else expected_defer_count
            ),
            NEXUS_LOCAL_PAPER_READONLY="1",
            WRANGLER_LOG_PATH=".wrangler/wrangler.log",
        )
        if candidate_stage_capacity_reads:
            import stat

            screenshots = root / ".secrets"
            for component in (
                "candidate-page-readback-20261010",
                revision,
                f"count-{candidate_count}",
            ):
                screenshots = screenshots / component
                screenshots.mkdir(mode=0o700, exist_ok=True)
                info = screenshots.stat()
                if stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != os.getuid():
                    raise AssertionError("CONTROLLED_SCREENSHOT_DIRECTORY_REJECTED")
            environment["NEXUS_CONTROLLED_SCREENSHOT_DIR"] = str(screenshots)
        web_root = isolation.enter_context(_web_project_root(root))
        web_process = await asyncio.create_subprocess_exec(
            str(web_root / "node_modules/.bin/vinext"),
            "dev",
            "--hostname",
            "127.0.0.1",
            "--port",
            str(web_port),
            cwd=web_root,
            env=environment,
            stdout=web_log,
            stderr=asyncio.subprocess.STDOUT,
        )
        await _wait_for_web(environment["NEXUS_CONTROLLED_WEB_URL"], web_process)
        browser = await asyncio.create_subprocess_exec(
            str(web_root / "node_modules/.bin/playwright"),
            "test",
            "--config",
            "playwright.controlled-paper.config.ts",
            *(("portfolio-controlled.spec.ts",) if first_partial else ()),
            cwd=web_root,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await asyncio.wait_for(browser.communicate(), timeout=120)
        if browser.returncode != 0:
            web_log.seek(0)
            raise AssertionError(
                output.decode(errors="replace")[-3000:]
                + "\nWeb log:\n"
                + web_log.read().decode(errors="replace")[-3000:]
            )
    finally:
        if web_process is not None and web_process.returncode is None:
            web_process.terminate()
            try:
                await asyncio.wait_for(web_process.wait(), timeout=5)
            except TimeoutError:
                web_process.kill()
                await web_process.wait()
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=10)
        web_log.close()
        isolation.close()
