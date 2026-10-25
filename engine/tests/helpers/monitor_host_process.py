"""Fresh isolated product-host interpreters with explicit synthetic transports."""

import asyncio
import json
import os
import re
import signal
import sys
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from enum import Enum
from hashlib import sha256
from pathlib import Path

import httpx
from nexus.ops import agent_paper_loop as loop
from nexus.ops import research_monitor
from pydantic import SecretStr


def write_environment(settings, path: Path, extras=None):
    values = {}
    for name, field in type(settings).model_fields.items():
        value = getattr(settings, name)
        if value is None:
            continue
        alias = field.validation_alias
        if not isinstance(alias, str):
            alias = name
        if isinstance(value, SecretStr):
            value = value.get_secret_value()
        elif isinstance(value, Enum):
            value = value.value
        elif isinstance(value, bool):
            value = str(value).lower()
        values[alias] = str(value)
    values.update(extras or {})
    path.write_text("".join(f"{key}={json.dumps(value)}\n" for key, value in values.items()))
    path.chmod(0o600)


class DiskEvidence:
    """Existing fixture URI contracts backed by shared immutable local bytes."""

    def __init__(self, *args, **kwargs):
        self.root = Path(os.environ["NEXUS_TEST_HOST_ROOT"])

    async def archive_raw(self, key, payload):
        self.put("host-evidence", key, payload)
        return f"memory://{key}"

    def put(self, folder, key, payload):
        path = self.root / folder / sha256(key.encode()).hexdigest()
        path.parent.mkdir(exist_ok=True)
        if path.exists():
            assert path.read_bytes() == payload
        else:
            path.write_bytes(payload)

    async def put_if_absent(self, *, key, content):
        self.put("host-sec-evidence", key, content)
        return "s3://synthetic/" + key

    async def read_raw(self, key, *, uri):
        folder = "host-evidence" if uri == f"memory://{key}" else "host-sec-evidence"
        assert uri in {f"memory://{key}", "s3://synthetic/" + key}
        return (self.root / folder / sha256(key.encode()).hexdigest()).read_bytes()

    async def read_evidence(self, key, *, uri):
        return await self.read_raw(key, uri=uri)


async def child_main(root, at, signal_stop=True):
    from integration.test_schwab_market_stream_import import CREDENTIALS, Socket, _frames
    from nexus.agents import codex_staged
    from nexus.market import object_store
    from nexus.monitoring import disclosure_context, disclosure_observations, filing_archive
    from nexus.ops import monitor_disclosure_refresh, research_identity_local, schwab_market_stream
    from nexus.providers.market import schwab_oauth
    from sqlalchemy.ext.asyncio import AsyncSession

    fixture = json.loads((root / "host-fixture.json").read_text())
    original_scalar = AsyncSession.scalar

    async def fixed_clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return at
        return await original_scalar(self, statement, *args, **kwargs)

    AsyncSession.scalar = fixed_clock

    class ClockMeta(type):
        def __instancecheck__(cls, value):
            return isinstance(value, datetime)

    class Clock(datetime, metaclass=ClockMeta):
        @classmethod
        def now(cls, tz=None):
            return at.astimezone(tz or UTC)

    research_identity_local.datetime = Clock
    counts = [0, 0]
    actions = []
    native_execute = loop.execute_agent_paper_loop_action

    async def observed_execute(*args, **kwargs):
        actions.append(kwargs["action"].value)
        return await native_execute(*args, **kwargs)

    loop.execute_agent_paper_loop_action = observed_execute
    original_capture = schwab_market_stream.capture_and_import_schwab_level_one_batch

    async def capture(arguments, client, **kwargs):
        counts[0] += 1
        frames = _frames(at)
        for packet in frames:
            for data in packet.get("data", []):
                original = data["content"][0]
                data["content"] = [
                    {
                        **original,
                        "key": symbol,
                        "1": float(
                            fixture["midpoint"] if symbol == "AAPL" else fixture["other_midpoint"]
                        ),
                        "2": float(
                            fixture["midpoint"] if symbol == "AAPL" else fixture["other_midpoint"]
                        ),
                    }
                    for symbol in arguments.symbol
                ]
        result = await schwab_market_stream.collect_schwab_stream(
            symbols=tuple(arguments.symbol),
            directory=arguments.output_dir,
            streamer_url="wss://streamer-api.schwab.com/ws",
            credentials=CREDENTIALS,
            max_frames=3,
            connector=lambda *args, **kwargs: Socket(frames),
        )
        path = arguments.output_dir / "receipt.json"
        receipt = json.loads(path.read_text())
        from datetime import timedelta

        receipt["started_at"] = (at - timedelta(seconds=2)).isoformat()
        receipt["ended_at"] = at.isoformat()
        for frame in receipt["frames"]:
            frame["received_at"] = at.isoformat()
        path.write_text(json.dumps(receipt, sort_keys=True))
        return result

    async def collect(*args, **kwargs):
        return await original_capture(*args, **kwargs, capture=capture)

    schwab_market_stream.capture_and_import_schwab_level_one_batch = collect

    @asynccontextmanager
    async def broker_client(**kwargs):
        def deny(request):
            raise AssertionError("unexpected fixture broker HTTP")

        async with httpx.AsyncClient(transport=httpx.MockTransport(deny)) as client:
            yield client

    schwab_oauth.schwab_http_client = broker_client
    object_store.S3ProviderEvidenceStore = DiskEvidence
    monitor_disclosure_refresh.configured_exhibit_store = lambda session: DiskEvidence()
    disclosure_context.configured_exhibit_store = lambda session: DiskEvidence()
    disclosure_observations.configured_exhibit_store = lambda session: DiskEvidence()
    filing_archive.configured_exhibit_store = lambda session: DiskEvidence()

    def sec_response(request):
        counts[1] += 1
        name = "directory.json" if request.url.host == "www.sec.gov" else "AAPL-submissions.json"
        folder = "baseline" if name == "directory.json" else "current"
        return httpx.Response(200, content=(root / folder / name).read_bytes())

    monitor_disclosure_refresh._client = lambda: httpx.AsyncClient(
        transport=httpx.MockTransport(sec_response), follow_redirects=False
    )

    async def forbidden_model(*args, **kwargs):
        raise AssertionError("unexpected model invocation in host reconstruction")

    codex_staged.invoke_codex = forbidden_model
    errors = []
    native_prepare = research_monitor.refresh_host_observations

    async def observed_prepare(*args, **kwargs):
        try:
            return await native_prepare(*args, **kwargs)
        except Exception as error:
            code = str(error)
            errors.append(
                code if re.fullmatch(r"[A-Z][A-Z0-9_]{2,100}", code) else type(error).__name__
            )
            raise

    research_monitor.refresh_host_observations = observed_prepare
    native_scan = research_monitor.scan_host_once

    async def observed_scan(*args, **kwargs):
        result = await native_scan(*args, **kwargs)
        print(
            json.dumps(
                {
                    "phase": "scan",
                    "result": result,
                    "pid": os.getpid(),
                    "preparation_errors": errors,
                }
            ),
            flush=True,
        )
        assert await asyncio.to_thread(sys.stdin.readline) == "stop\n"
        if signal_stop:
            os.kill(os.getpid(), signal.SIGTERM)
        return result

    research_monitor.scan_host_once = observed_scan
    native_lock = loop.acquire_agent_paper_lock
    loop.acquire_agent_paper_lock = lambda: native_lock(runtime_root=root / "isolated-runtime")
    settings = loop._FileEngineSettings.model_validate(
        loop.load_agent_paper_service_environment(root, {}, env_file=root / "host.env")
    )
    # No provider, model, MinIO or broker connection can escape a missing stub.
    import socket

    from sqlalchemy.engine import make_url

    database = make_url(settings.database_url)
    assert database.host in {"127.0.0.1", "localhost", "::1"}
    assert database.database.startswith("nexus_test_")
    port = database.port
    native_connect = socket.socket.connect

    def postgres_only(sock, address):
        assert (
            isinstance(address, tuple) and address[0] in {"127.0.0.1", "::1"} and address[1] == port
        ), "unexpected host fixture network connection"
        return native_connect(sock, address)

    socket.socket.connect = postgres_only
    action = await loop.run_local_agent_paper_loop(
        settings,
        account_id=settings.agent_paper_account_id,
        repo_root=root,
        env_file=root / "host.env",
        clock=lambda: at,
    )
    print(
        json.dumps(
            {
                "phase": "exit",
                "action": action.value,
                "pid": os.getpid(),
                "source_calls": counts,
                "actions": actions,
            }
        ),
        flush=True,
    )


async def run_process(settings, root, at, *, expect_scan=True, signal_stop=True):
    """Start the real host core in a fresh interpreter, using no parent Python state."""
    environment = os.environ.copy()
    repository = Path(__file__).resolve().parents[3]
    environment["PYTHONPATH"] = os.pathsep.join(
        str(repository / part) for part in ("engine/tests", "engine/src", ".")
    )
    environment["NEXUS_TEST_HOST_ROOT"] = str(root)
    error_path = root / ("host-stderr-" + str(len(tuple(root.glob("host-stderr-*")))) + ".log")
    receipts = []
    with error_path.open("wb") as errors:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "helpers.monitor_host_process",
            "--child",
            str(root),
            at.isoformat(),
            "signal" if signal_stop else "natural",
            env=environment,
            cwd=repository,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=errors,
        )
        try:
            line = await asyncio.wait_for(process.stdout.readline(), 45)
            assert line, "isolated host produced no receipt"
            receipt = json.loads(line)
            receipts.append(receipt)
            if expect_scan:
                assert receipt["phase"] == "scan" and not receipt["preparation_errors"], receipt
                assert process.returncode is None and receipt["pid"] == process.pid != os.getpid()
                try:
                    with loop.acquire_agent_paper_lock(runtime_root=root / "isolated-runtime"):
                        raise AssertionError("second host acquired an owned process lock")
                except RuntimeError as error:
                    assert str(error) == "AGENT_PAPER_LOOP_ALREADY_RUNNING"
                process.stdin.write(b"stop\n")
                await process.stdin.drain()
                receipt = json.loads(await asyncio.wait_for(process.stdout.readline(), 30))
                receipts.append(receipt)
            assert receipt["phase"] == "exit", receipt
            assert await asyncio.wait_for(process.wait(), 30) == 0
            with loop.acquire_agent_paper_lock(runtime_root=root / "isolated-runtime"):
                pass
            return receipts
        finally:
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 10)
                except TimeoutError:
                    process.kill()
                    await process.wait()


async def assert_reconstruction(
    factory, settings, root, captured, environment, midpoint, threshold, sec_store
):
    from datetime import timedelta

    from integration.test_candidate_multistock_path import _set_test_insert_clock
    from integration.test_monitor_session_budget_pg import preserved_rows
    from sqlalchemy import text

    tables = (
        "research_monitor_plans",
        "research_monitor_checks",
        "research_monitor_episodes",
        "research_monitor_triggers",
        "research_monitor_batches",
        "research_monitor_increments",
        "research_monitor_coverage",
        "outbox_events",
        "outbox_delivery_attempts",
        "agent_research_runs",
        "agent_research_outputs",
        "agent_daily_budget_reservations",
        "formal_budget_bindings",
        "agent_portfolio_synthesis_budget_reservations",
        "portfolio_decision_budget_reservations",
        "jobs",
        "job_leases",
        "data_snapshots",
    )

    async def saved():
        async with factory() as session:
            return {
                table: tuple(
                    await session.scalars(
                        text(f'SELECT to_jsonb(t)::text AS row FROM "{table}" t ORDER BY row')
                    )
                )
                for table in tables
            }

    async def workers():
        async with factory() as session:
            return set(
                await session.scalars(
                    text(
                        "SELECT worker_id FROM worker_heartbeats "
                        "WHERE worker_id LIKE 'agent-paper-loop-%'"
                    )
                )
            )

    write_environment(settings, root / "host.env", environment)
    (root / "host-fixture.json").write_text(
        json.dumps({"midpoint": float(midpoint), "other_midpoint": float(threshold * 2)})
    )
    for key, content in sec_store.objects.items():
        path = root / "host-sec-evidence" / sha256(key.encode()).hexdigest()
        path.parent.mkdir(exist_ok=True)
        path.write_bytes(content)
    previous = await saved()
    protected = await preserved_rows(factory)
    before_workers = await workers()
    pool = factory.kw["bind"]
    receipts = []
    for _ in range(2):
        await pool.dispose()
        rows = await run_process(settings, root, captured["clock"][0])
        assert rows[-1]["action"] == loop.AgentPaperLoopAction.STOP_REQUESTED.value, rows
        assert rows[0]["result"]["model_requests"] == 0
        # The parent already made its second normal observe/scan: its newer SEC
        # capture is false while the original stale-index unknown remains saved.
        assert rows[0]["result"]["false"] == 2
        assert rows[0]["result"]["unknown"] == 2
        assert rows[0]["result"]["checks"] == 4
        assert rows[0]["result"].get("new_triggers", 0) == 0
        assert rows[0]["result"].get("notifications", 0) == 0
        assert rows[-1]["source_calls"] == [0, 0], rows
        current = await saved()
        assert current == previous
        receipts.append(rows)
    assert receipts[0][-1]["pid"] != receipts[1][-1]["pid"]
    assert len((await workers()) - before_workers) == 2
    captured["clock"][0] += timedelta(minutes=5)
    await _set_test_insert_clock(factory, captured["clock"][0])
    await pool.dispose()
    following = await run_process(settings, root, captured["clock"][0])
    assert following[-1]["action"] == loop.AgentPaperLoopAction.STOP_REQUESTED.value, following
    assert following[-1]["source_calls"] == [1, 0], following
    assert following[0]["result"]["model_requests"] == 0
    assert following[0]["result"].get("new_triggers", 0) == 0
    assert following[0]["result"].get("notifications", 0) == 0
    after = await saved()
    assert all(
        set(old) <= set(after[table])
        for table, old in previous.items()
        if table != "research_monitor_episodes"
    ), {
        table: [json.loads(row) for row in set(old) - set(after[table])]
        for table, old in previous.items()
        if not set(old) <= set(after[table])
    }
    assert all(
        after[table] == previous[table]
        for table in tables
        if table
        not in {
            "research_monitor_checks",
            "research_monitor_batches",
            "data_snapshots",
            "research_monitor_episodes",
        }
    )
    new_checks = [
        json.loads(row)
        for row in set(after["research_monitor_checks"]) - set(previous["research_monitor_checks"])
    ]
    assert len(new_checks) == 4
    assert sorted(row["kind"] for row in new_checks) == ["disclosure", "price", "price", "price"]
    assert len({row["instrument_id"] for row in new_checks if row["kind"] == "price"}) == 3
    assert {row["scheduled_at"] for row in new_checks} == {
        captured["clock"][0]
        .replace(minute=captured["clock"][0].minute // 5 * 5, second=0, microsecond=0)
        .isoformat()
    }
    price_results = {
        row["instrument_id"]: row["result"] for row in new_checks if row["kind"] == "price"
    }
    old_episodes = {
        json.loads(row)["instrument_id"]: json.loads(row)
        for row in previous["research_monitor_episodes"]
    }
    new_episodes = {
        json.loads(row)["instrument_id"]: json.loads(row)
        for row in after["research_monitor_episodes"]
    }
    assert old_episodes.keys() == new_episodes.keys()
    for instrument, old in old_episodes.items():
        new = new_episodes[instrument]
        assert {k: v for k, v in new.items() if k not in {"checked_at", "observed_false"}} == {
            k: v for k, v in old.items() if k not in {"checked_at", "observed_false"}
        }
        assert new["checked_at"] == captured["clock"][0].isoformat()
        assert new["observed_false"] == (
            old["observed_false"] or price_results[instrument] == "false"
        )
    assert len((await workers()) - before_workers) == 3
    # A fourth independent process consumes the new bucket's persisted result.
    await pool.dispose()
    repeated = await run_process(settings, root, captured["clock"][0])
    assert repeated[-1]["action"] == loop.AgentPaperLoopAction.STOP_REQUESTED.value, repeated
    assert repeated[-1]["source_calls"] == [0, 0], repeated
    assert await saved() == after
    assert len((await workers()) - before_workers) == 4
    latest = await preserved_rows(factory)
    assert all(old <= latest[table] for table, old in protected.items())


if __name__ == "__main__":
    try:
        asyncio.run(
            child_main(
                Path(sys.argv[2]), datetime.fromisoformat(sys.argv[3]), sys.argv[4] == "signal"
            )
        )
    except BaseException as error:
        code = str(error)
        print(
            json.dumps(
                {
                    "phase": "error",
                    "type": type(error).__name__,
                    "code": code
                    if re.fullmatch(r"[A-Z][A-Z0-9_]{2,100}", code)
                    else "FIXTURE_HOST_FAILED",
                }
            ),
            flush=True,
        )
        raise SystemExit(1) from None
