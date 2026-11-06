"""Real isolated host lifecycle, with the product readiness and heartbeat writers."""

import asyncio
import json
import os
import signal
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from nexus.ops import agent_paper_loop as loop
from nexus.ops.agent_paper_recovery import inspect_recovery_readiness
from nexus.paper.manual_host_exit import record_host_exit
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.persistence.session import session_factory
from sqlalchemy import select, text
from sqlalchemy.engine import make_url


async def append_late_fact(factory, run_id):
    from uuid import UUID

    from nexus.agents.execution import _append_event
    from nexus.persistence.models.agents import AgentResearchRunRecord

    async with factory.begin() as db:
        run = await db.get(AgentResearchRunRecord, UUID(str(run_id)))
        sequence = await db.scalar(
            text("SELECT max(sequence)+1 FROM agent_research_events WHERE run_id=:id"),
            {"id": run.id},
        )
        await _append_event(db, run, sequence, "failed", {"reason": "isolated_late_fact"})


async def main(root):
    settings = loop._FileEngineSettings.model_validate(
        loop.load_agent_paper_service_environment(root, {}, env_file=root / "host.env")
    )
    url = make_url(settings.database_url)
    assert url.host == "127.0.0.1" and url.database.startswith("nexus_test_")
    factory = session_factory(settings)
    async with factory() as db:
        at = await db.scalar(text("SELECT clock_timestamp()"))
    async with factory() as db:
        offset = await db.scalar(text("SELECT pg_catalog.clock_timestamp()-clock_timestamp()"))
    delta = timedelta(seconds=round(offset.total_seconds()))
    import pytest
    from integration.test_sol_native_stage_pg import install_sol_http
    from nexus import worker
    from nexus.agents import snapshots
    from nexus.paper import independent_history_scope

    independent_history_scope.POLICY_AT = at - timedelta(days=1)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return (datetime.now(UTC) - delta).astimezone(tz or UTC)

    worker.datetime = snapshots.datetime = Clock
    worker._ONCE_STARTED_AT = at
    os.environ["NEXUS_TEST_HOST_ROOT"] = str(root)
    patch = pytest.MonkeyPatch()
    import base64

    from nexus.market.object_store import S3ProviderEvidenceStore
    from unit.market.test_object_store import FakeS3Client

    fixture = json.loads((root / "objects.json").read_text())
    client = FakeS3Client()
    client.objects = {key: base64.b64decode(content) for key, content in fixture["objects"].items()}
    client.versions = fixture["versions"]

    fault_enabled = False
    authorization_refusals = []
    if fixture["case"] == "policy_refusal":
        from contextvars import ContextVar

        from nexus.agents.research_stages.protocol import StagedProtocol
        from nexus.paper import independent_priority as priority
        from nexus.paper import independent_unknown_acceptance as acceptance

        inside_predecessor = ContextVar("inside_predecessor", default=False)
        original_capture = priority.capture_predecessor
        original_validate = acceptance._validate
        original_authorize = StagedProtocol.authorize

        async def capture(*args, **kwargs):
            token = inside_predecessor.set(True)
            try:
                return await original_capture(*args, **kwargs)
            finally:
                inside_predecessor.reset(token)

        async def validate(*args, **kwargs):
            if fault_enabled and inside_predecessor.get():
                raise ValueError(acceptance.BLOCKED)
            return await original_validate(*args, **kwargs)

        async def authorize(self, *args, **kwargs):
            try:
                return await original_authorize(self, *args, **kwargs)
            except Exception as error:
                authorization_refusals.append([type(error).__name__, str(error), kwargs["stage"]])
                raise

        patch.setattr(priority, "capture_predecessor", capture)
        patch.setattr(acceptance, "_validate", validate)
        patch.setattr(StagedProtocol, "authorize", authorize)
    stage_calls = {}
    first_transports = []
    overlap = asyncio.Event()

    async def on_request(payload):
        nonlocal fault_enabled
        if fixture["case"] == "policy_refusal":
            fault_enabled = True
        stage_calls.setdefault(str(payload["request"]["instrument_id"]), []).append(
            payload["stage"]
        )
        if fixture["case"] == "normal" and payload["stage"] == "research":
            first_transports.append(payload["request"]["instrument_id"])
            if len(first_transports) == 2:
                overlap.set()
            if len(first_transports) <= 2:
                # Both real dispatch members must reach transport before either returns.
                await asyncio.wait_for(overlap.wait(), 30)
        if fixture["case"] == "late_stage" and payload["stage"] == "research":
            await append_late_fact(factory, fixture["old_run"])

    calls, clients = install_sol_http(
        patch,
        S3ProviderEvidenceStore(
            settings.model_copy(
                update={
                    "object_store_bucket": fixture["bucket"],
                    "object_store_prefix": fixture["prefix"],
                }
            ),
            client=client,
        ),
        on_request=on_request,
    )
    dispatched = False
    native_execute = loop.execute_agent_paper_loop_action

    async def execute(*args, **kwargs):
        nonlocal dispatched, fault_enabled
        result = await native_execute(*args, **kwargs)
        if kwargs["action"] == loop.AgentPaperLoopAction.TRIGGER_MANUAL:
            fault_enabled = False
            print(
                json.dumps(
                    dict(
                        phase="dispatched",
                        authorization_refusals=authorization_refusals,
                        stages=calls,
                        stage_calls=stage_calls,
                        parallel_http=overlap.is_set(),
                        clients_closed=all(c.is_closed for c in clients),
                    )
                ),
                flush=True,
            )
            dispatched = True
        return result

    loop.execute_agent_paper_loop_action = execute
    native_lock = loop.acquire_agent_paper_lock
    loop.acquire_agent_paper_lock = lambda: native_lock(runtime_root=root / "isolated-host")
    native_heartbeat = loop.heartbeat_agent_paper_loop
    observations = []

    async def guard(current):
        result = await inspect_recovery_readiness(current)
        assert result["status"] == "ready", result

    async def heartbeat(*args, **kwargs):
        await native_heartbeat(*args, **kwargs)
        observations.append(kwargs["seen_at"])
        if dispatched:
            os.kill(os.getpid(), signal.SIGTERM)
        if len(observations) == 2:
            assert observations[1] > observations[0]
            print(json.dumps(dict(phase="ready", pid=os.getpid(), heartbeats=2)), flush=True)
            assert await asyncio.to_thread(sys.stdin.readline) == "run\n"

    loop.heartbeat_agent_paper_loop = heartbeat

    # Only synthetic PostgreSQL; a missing fixture cannot call any provider.
    def audit(event, values):
        if event == "socket.connect":
            address = values[1]
            assert isinstance(address, tuple) and address[:2] == (url.host, url.port)
        if event in {"subprocess.Popen", "os.system", "os.posix_spawn", "os.fork"}:
            raise AssertionError("host must wait without external work")

    sys.addaudithook(audit)
    try:
        from nexus.agents.research_stages.protocol import ProtocolRejected

        try:
            result = await loop.run_local_agent_paper_loop(
                settings,
                account_id=settings.agent_paper_account_id,
                repo_root=root,
                env_file=root / "host.env",
                clock=lambda: datetime.now(UTC) - delta,
                recovery_guard=guard,
            )
        except ProtocolRejected as error:
            assert fixture["case"] == "late_stage" and dispatched
            assert str(error) == "STAGED_TERMINAL_PROOF_INVALID"
            assert calls == ["research"] and all(c.is_closed for c in clients)
            print(json.dumps(dict(phase="blocked", code=str(error), pid=os.getpid())), flush=True)
            raise
        assert result == loop.AgentPaperLoopAction.STOP_REQUESTED, result
        async with factory.begin() as db:
            hosts = list(
                await db.scalars(
                    select(WorkerHeartbeat).where(
                        WorkerHeartbeat.queue == "agent_paper",
                        WorkerHeartbeat.started_at >= at,
                    )
                )
            )
            assert len(hosts) == 1
            host = hosts[0]
            await record_host_exit(
                db,
                worker_id=host.worker_id,
                started_at=host.started_at,
                process_version=host.process_version,
            )
        print(json.dumps(dict(phase="stopped", pid=os.getpid())), flush=True)
    finally:
        await factory.kw["bind"].dispose()


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1])))
