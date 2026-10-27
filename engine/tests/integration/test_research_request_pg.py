"""Normal pure-research producer, complete batch, durable close and next admission."""

import asyncio
import importlib
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from helpers.current_identity import archives
from helpers.paper_batch import seed_paper_batch
from helpers.research_snapshots import seed_filing
from integration.test_research_identity_context import seed, source_archive
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.openai_local import prepare_database
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.manual_request import (
    admit_manual_request,
    claim_manual_request,
    read_manual_request,
)
from nexus.paper.research_request import finish_research_request, prepare_research_request
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.manual_paper import (
    ManualPaperRequest,
    ResearchRequestManifest,
    ResearchRequestMember,
)
from nexus.persistence.models.market import UniverseMember, UniverseSnapshot
from nexus.persistence.models.paper import AgentPaperCycleRecord, PaperOrderRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.research.current_identity_storage import import_identity_binding
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from unit.market.test_object_store import FakeS3Client
from unit.ops.test_formal_stage_registration import OPTIONS


@pytest.fixture(autouse=True)
def isolated_host_lock(monkeypatch, tmp_path):
    # Keep the actual process lock checks; never share the resident app's lock.
    from nexus.ops.agent_paper_loop import acquire_agent_paper_lock

    monkeypatch.setattr(
        "nexus.ops.agent_paper_loop.acquire_agent_paper_lock",
        lambda: acquire_agent_paper_lock(runtime_root=tmp_path / "host-runtime"),
    )


@pytest.fixture(autouse=True)
def research_snapshot_uses_database_clock(monkeypatch):
    # The producer's authoritative clock is PostgreSQL. Use that same clock for
    # the snapshot guard in isolated Docker tests, instead of depending on host/VM
    # subsecond synchronization. Future timestamps still fail the original guard.
    import nexus.agents.snapshots as snapshots

    original = snapshots.prepare_research_snapshot

    async def prepare(session, *args, **kwargs):
        now = await session.scalar(select(func.clock_timestamp()))

        class DatabaseClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return now.astimezone(tz)

        with monkeypatch.context() as scoped:
            scoped.setattr(snapshots, "datetime", DatabaseClock)
            return await original(session, *args, **kwargs)

    monkeypatch.setattr(snapshots, "prepare_research_snapshot", prepare)


async def prepare_scope(url, tmp_path, *, bounded=False):
    account = await seed_paper_batch(url)
    settings = EngineSettings(database_url=url, agent_runtime_mode="codex_sdk").model_copy(
        update={
            "object_store_endpoint": "http://127.0.0.1:9000",
            "object_store_bucket": "synthetic-identity",
            "object_store_prefix": "synthetic-identity",
            "agent_paper_account_id": account.account_id,
        }
    )
    factory = session_factory(settings)
    store = S3ProviderEvidenceStore(settings, client=FakeS3Client())
    await prepare_database(
        settings,
        **(
            OPTIONS
            | (
                {
                    "max_wire_bytes": 100479,
                    "full_input_capacity": True,
                    "formal_bounded_stage_budget": True,
                }
                if bounded
                else {}
            )
        ),
    )
    ids = []
    for index, symbol in enumerate(("AAPL", "NVDA", "AVGO"), 1):
        instrument = await seed(factory, symbol=symbol)
        ids.append(instrument)
        fm, sc = archives(tmp_path / f"base-{symbol}", symbol=symbol)
        base = await importlib.import_module("nexus.ops.instrument_identity_local").revise_listing(
            settings, symbol=symbol, fmp_archive=fm, sec_archive=sc, store=store
        )
        # The source helper defaults to now minus two minutes, which crosses NY
        # midnight during CI. Synthetic originals belong to the DB clock's NY day;
        # use its start, preserving the real same-session current-identity guard.
        async with factory.begin() as session:
            database_time = await session.scalar(select(func.clock_timestamp()))
        source_time = (
            database_time.astimezone(ZoneInfo("America/New_York"))
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .astimezone(UTC)
        )
        source = tmp_path / f"source-{symbol}"
        source_archive(source)
        for name in ("directory", "AAPL-submissions"):
            document = (
                {
                    "fields": ["cik", "name", "ticker", "exchange"],
                    "data": [
                        [123456, "Synthetic", ticker, "NYSE"] for ticker in ("AAPL", "NVDA", "AVGO")
                    ],
                }
                if name == "directory"
                else {
                    "cik": "123456",
                    "tickers": ["AAPL", "NVDA", "AVGO"],
                    "exchanges": ["NYSE"] * 3,
                }
            )
            raw = json.dumps(document).encode()
            receipt = json.loads((source / f"{name}-receipt.json").read_text())
            receipt.update(
                sha256=sha256(raw).hexdigest(),
                byte_length=len(raw),
                received_at=source_time.isoformat(),
            )
            (source / f"{name}.json").write_bytes(raw)
            (source / f"{name}-receipt.json").write_text(json.dumps(receipt))
        if symbol != "AAPL":
            (source / "AAPL-submissions.json").rename(source / f"{symbol}-submissions.json")
            (source / "AAPL-submissions-receipt.json").rename(
                source / f"{symbol}-submissions-receipt.json"
            )
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            await seed_filing(
                session,
                instrument_id=instrument,
                seed_instrument=False,
                provider_object_id=f"filing:{symbol}",
                accession_number=f"000000000{index}-26-000001",
            )
            await import_identity_binding(
                session, store, identity_snapshot_id=base.snapshot_id, collection_dir=source
            )
    async with factory.begin() as session:
        at = await session.scalar(select(func.clock_timestamp()))
        universe = UniverseSnapshot(
            id=uuid4(),
            key="agent_paper_research",
            version="reviewed_v1_" + "c" * 16,
            source="nexus_local_reviewed_research",
            content_hash="c" * 64,
            effective_from=at.date(),
            published_at=None,
        )
        session.add(universe)
        await session.flush()
        for instrument in ids:
            session.add(
                UniverseMember(
                    universe_snapshot_id=universe.id,
                    instrument_id=instrument,
                    inclusion_reason="reviewed",
                )
            )
        await session.flush()
        universe.published_at = at
        await session.flush()
        batch = await create_agent_paper_candidate_batch(
            session,
            account_id=account.account_id,
            universe_snapshot_id=universe.id,
            instrument_ids=tuple(ids),
            idempotency_key="pure-scope",
            created_at=at,
        )
        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version
                == (
                    "codex-sdk-chatgpt-v11-bounded-stage-budget"
                    if bounded
                    else "codex-sdk-chatgpt-v8-stage-capacity"
                )
            )
        )
    settings = settings.model_copy(
        update={
            "agent_paper_candidate_batch_id": batch.batch_id,
            "agent_paper_formal_model_config_id": model.id,
            "agent_paper_manual_enabled": True,
            "agent_paper_cycle_enabled": True,
        }
    )
    return factory, settings, store, ids


@pytest.mark.asyncio
async def test_three_stock_atomic_manifest_readback_cancel_and_next_admission(
    migrated_database_url, tmp_path
):
    factory, settings, store, ids = await prepare_scope(migrated_database_url, tmp_path)

    async def admit(key):
        async with factory.begin() as session:
            return await admit_manual_request(
                session,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                model_config_id=settings.agent_paper_formal_model_config_id,
                mode="research_only",
                idempotency_key=key,
                contract_version=2,
            )

    try:
        first, replay = await asyncio.gather(admit("one"), admit("one"))
        assert first == replay and first.research_contract == "research_only_v2"
        async with factory.begin() as session:
            _, claimed = await claim_manual_request(session, request_id=first.request_id)
            assert claimed
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            active = await prepare_research_request(session, settings, request_id=first.request_id)
            assert active.status == "active" and active.cycle_id is None
            assert {m.instrument_id for m in active.research.members} == set(ids)
            assert len({m.run_id for m in active.research.members}) == 3
            assert len({m.job_id for m in active.research.members}) == 3
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            reread = await prepare_research_request(session, settings, request_id=first.request_id)
            assert reread == active
            assert (
                await session.scalar(select(func.count()).select_from(ResearchRequestManifest)) == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(ResearchRequestMember)) == 3
            )
            assert (
                await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 3
            )
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 0
            )
            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            await admit("blocked-before-close")
        async with factory.begin() as session:
            row = await session.get(ManualPaperRequest, first.request_id)
            closed = await finish_research_request(session, row=row, cancel_unused=True)
            assert closed.status == "unavailable"
            assert all(m.status == "abstained" and m.usage_known for m in closed.research.members)
        async with factory.begin() as session:
            row = await session.get(ManualPaperRequest, first.request_id)
            assert await read_manual_request(session, row) == closed
            jobs = list(
                await session.scalars(
                    select(Job).where(Job.id.in_([m.job_id for m in closed.research.members]))
                )
            )
            assert all(j.status == "skipped" and j.attempt_count == 1 for j in jobs)
        next_request = await admit("independent-next")
        assert next_request.request_id != first.request_id and next_request.status == "queued"
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,contract_version", [("research_only", 2), ("research_and_monitor", 3)]
)
async def test_normal_v11_bounded_host_three_stages_each_without_calendar_or_scheduling(
    migrated_database_url, tmp_path, monkeypatch, mode, contract_version
):
    import subprocess
    import sys
    from types import SimpleNamespace

    from nexus.agents.codex_sdk import invoke_codex as real_invoke
    from nexus.ops.manual_paper_host import run_pure_research_once
    from nexus.persistence.models.runtime import ManualHostExit
    from unit.agents.test_codex_sdk import _assessment_payload, _sdk, _staged_batch

    factory, settings, store, ids = await prepare_scope(
        migrated_database_url, tmp_path, bounded=True
    )
    if mode == "research_and_monitor":
        await prepare_database(
            settings,
            **(
                OPTIONS
                | {
                    "max_wire_bytes": 100479,
                    "full_input_capacity": True,
                    "formal_bounded_stage_budget": True,
                    "price_monitoring": True,
                    "disclosure_monitoring": True,
                }
            ),
        )
    settings = settings.model_copy(update={"release_revision": "a" * 40})
    calls = []
    reuse_states = []
    client, thread, result, _ = _sdk(monkeypatch)
    processes = []
    client._client._sync = SimpleNamespace(_proc=None)

    async def account(**_):
        # Model SDK initialization with an owned local child, not an invented
        # successful poll(). No network, Codex process or model is invoked.
        assert client._client._sync._proc is None
        process = subprocess.Popen(
            [sys.executable, "-c", "import sys; sys.stdin.buffer.read()"],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes.append(process)
        client._client._sync._proc = process
        return client.account.return_value

    async def close():
        process = client._client._sync._proc
        assert process is not None
        # The SDK clears its handle before waiting. The production close guard
        # must retain and observe the original child across this transition.
        client._client._sync._proc = None
        process.stdin.close()
        await asyncio.to_thread(process.wait, timeout=5)

    client.account.side_effect = account
    client.close.side_effect = close

    async def invoke(payload, **options):
        from nexus.agents.preparation_reuse import current_reuse

        reuse = current_reuse()
        assert reuse is not None and reuse.active
        reuse_states.append(reuse)
        calls.append(payload["stage"])
        response = (
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        if mode == "research_and_monitor":
            if payload["stage"] == "synthesis":
                # This synthetic model acknowledges missing inputs: it cannot
                # claim a buy conclusion and still qualify as a safe watch.
                response["synthesis_assessment"]["stance"] = "watch"
                response.update(
                    monitoring_adoption_hash=None,
                    disclosure_adoption_hash=None,
                    daily_pending_adoption_hash=None,
                )
            else:
                suffix = "proposal" if payload["stage"] == "research" else "review"
                for role in response["roles"]:
                    role.update(
                        {
                            f"monitoring_{suffix}": None,
                            f"disclosure_{suffix}": None,
                            f"daily_pending_{suffix}": None,
                        }
                    )
            options["output_model"].model_validate(response)
        result.final_response = json.dumps(response)
        return await real_invoke(payload, **options)

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )

    async def no_calendar(*a, **kw):
        raise AssertionError("pure research reached formal calendar")

    monkeypatch.setattr(
        "nexus.paper.manual_entry.select_agent_paper_service_calendar_authority", no_calendar
    )
    monkeypatch.setattr(
        "nexus.ops.manual_paper_host.select_agent_paper_service_calendar_authority", no_calendar
    )
    try:
        # Existing four-field requests retain their serial execution policy.
        # The separate two-flight test exercises today's normal new admission.
        async with factory.begin() as session:
            await admit_manual_request(
                session,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                model_config_id=settings.agent_paper_formal_model_config_id,
                mode=mode,
                idempotency_key="normal-v11",
                contract_version=contract_version,
            )
        view = await run_pure_research_once(settings, idempotency_key="normal-v11", mode=mode)
        from nexus.agents.preparation_reuse import current_reuse

        assert current_reuse() is None
        assert len(reuse_states) == 9 and len({id(state) for state in reuse_states}) == 1
        reused = reuse_states[0]
        assert reused.counts["projection_hits"] > 0
        assert not reused.active and not reused.projections and not reused.encodings
        assert view.status == "completed" and view.cycle_id is None
        assert {m.instrument_id for m in view.research.members} == set(ids)
        assert all(m.status == "completed" and m.usage_known for m in view.research.members)
        assert calls == ["research", "critique", "synthesis"] * 3
        assert len(processes) == 9 and all(p.poll() == 0 for p in processes)
        assert client.close.await_count == 9
        if mode == "research_and_monitor":
            from nexus.agents.research_only import require_formal_research
            from nexus.monitoring.batches import disclosure_source_current
            from nexus.monitoring.source_qualification import load_qualified_source
            from nexus.persistence.models.agents import AgentResearchOutputRecord

            async with factory.begin() as session:
                session.info["sec_exhibit_store"] = store
                now = await session.scalar(select(func.clock_timestamp()))
                for member in view.research.members:
                    terminal = await session.scalar(
                        select(AgentResearchOutputRecord).where(
                            AgentResearchOutputRecord.run_id == member.run_id,
                            AgentResearchOutputRecord.role.is_(None),
                            AgentResearchOutputRecord.status == "completed",
                        )
                    )
                    qualified = await load_qualified_source(
                        session,
                        account_id=settings.agent_paper_account_id,
                        output_id=terminal.id,
                        now=now,
                    )
                    assert qualified.context.request_id == view.request_id
                    with pytest.raises(
                        ValueError, match="INDEPENDENT_MONITOR_FORMAL_CONSUMPTION_FORBIDDEN"
                    ):
                        await require_formal_research(session, qualified.run)
                    # The source-current query must discover native independent roots,
                    # even without a fabricated PaperCycle or MonitorIncrement parent.
                    pointer = SimpleNamespace(
                        id=member.run_id,
                        account_id=settings.agent_paper_account_id,
                        instrument_id=member.instrument_id,
                        source_run_id=member.run_id,
                    )
                    assert await disclosure_source_current(session, pointer, now)
        async with factory.begin() as session:
            assert await session.scalar(select(func.count()).select_from(ManualHostExit)) == 1
        import httpx
        from helpers.paper_api import headers
        from helpers.paper_api import settings as api_settings
        from nexus.main import create_engine_app

        auth = api_settings(migrated_database_url)
        api_config = settings.model_copy(
            update={
                "env": "test",
                "engine_hmac_current_key_id": auth.engine_hmac_current_key_id,
                "engine_hmac_current_secret": auth.engine_hmac_current_secret,
            }
        )
        app = create_engine_app(api_config)
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                path = f"/internal/v1/paper/manual-requests/{view.request_id}"
                response = await client.get(path, headers=headers("GET", path))
                assert response.status_code == 200 and response.json()["data"] == view.model_dump(
                    mode="json"
                )
                path = "/internal/v1/paper/manual-requests"
                body = json.dumps({"mode": mode}).encode()
                response = await client.post(
                    path,
                    content=body,
                    headers={**headers("POST", path, body), "Idempotency-Key": "normal-v11"},
                )
                assert response.status_code == 200 and response.json()["data"] == view.model_dump(
                    mode="json"
                )
                # The completed foreground host has retired. A new API request
                # needs a genuinely live host, not the old host's recent heartbeat.
                from nexus.ops.agent_paper_loop import heartbeat_agent_paper_loop

                async with factory.begin() as session:
                    host_now = await session.scalar(select(func.clock_timestamp()))
                await heartbeat_agent_paper_loop(
                    settings,
                    worker_id=f"agent-paper-loop-{uuid4()}",
                    started_at=host_now,
                    seen_at=host_now,
                    account_id=settings.agent_paper_account_id,
                )
                response = await client.post(
                    path,
                    content=body,
                    headers={
                        **headers("POST", path, body),
                        "Idempotency-Key": "independent-v11-next",
                    },
                )
                assert response.status_code == 200 and response.json()["data"]["status"] == "queued"
                assert calls == ["research", "critique", "synthesis"] * 3
        finally:
            await app.state.session_factory.kw["bind"].dispose()
        async with factory.begin() as session:
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord)) == 0
            )
            assert await session.scalar(select(func.count()).select_from(PaperOrderRecord)) == 0
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_queued_profile_drift_closes_without_runs(migrated_database_url, tmp_path):
    from decimal import Decimal

    from nexus.ops.manual_paper_host import claim_configured_manual_request

    factory, settings, store, ids = await prepare_scope(migrated_database_url, tmp_path)
    try:
        async with factory.begin() as session:
            receipt = await admit_manual_request(
                session,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                model_config_id=settings.agent_paper_formal_model_config_id,
                mode="research_only",
                idempotency_key="queued-profile",
                contract_version=2,
            )
        async with factory.begin() as session:
            view, claimed = await claim_configured_manual_request(
                session,
                settings.model_copy(update={"agent_daily_budget_usd": Decimal("4")}),
                request_id=receipt.request_id,
            )
            assert not claimed and view.status == "unavailable"
            assert (
                await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_secondary_close_failure_preserves_primary_and_committed_unused_cancellation(
    migrated_database_url, tmp_path, monkeypatch
):
    from nexus.ops.manual_paper_host import run_pure_research_once

    factory, settings, store, ids = await prepare_scope(migrated_database_url, tmp_path)
    settings = settings.model_copy(update={"release_revision": "a" * 40})
    primary = RuntimeError("primary-test-canary")

    async def worker(*a, **kw):
        raise primary

    async def close(*a, **kw):
        raise ValueError("secondary-test-canary")

    monkeypatch.setattr("nexus.ops.agent_paper_once.run_cycle_job_once", worker)
    monkeypatch.setattr("nexus.paper.research_request.finish_research_request", close)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    try:
        with pytest.raises(RuntimeError) as captured:
            await run_pure_research_once(settings, idempotency_key="new-primary")
        assert captured.value is primary
        async with factory.begin() as session:
            runs = list(await session.scalars(select(AgentResearchRunRecord)))
            assert len(runs) == 3 and all(
                r.status == "abstained" and r.settled_cost_usd == 0 for r in runs
            )
            jobs = list(
                await session.scalars(
                    select(Job).where(
                        Job.payload["run_id"].as_string().in_([str(r.id) for r in runs])
                    )
                )
            )
            assert len(jobs) == 3 and all(j.status == "skipped" for j in jobs)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_interrupted_atomic_preparation_closes_without_replay(
    migrated_database_url, tmp_path
):
    from nexus.ops.manual_paper_host import dispatch_pure_research_request

    factory, settings, store, ids = await prepare_scope(migrated_database_url, tmp_path)
    try:
        async with factory.begin() as session:
            first = await admit_manual_request(
                session,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                model_config_id=settings.agent_paper_formal_model_config_id,
                mode="research_only",
                idempotency_key="interrupted-before-manifest",
                contract_version=2,
            )
            _, claimed = await claim_manual_request(session, request_id=first.request_id)
            assert claimed
        assert await dispatch_pure_research_request(settings, request_id=first.request_id)
        async with factory.begin() as session:
            row = await session.get(ManualPaperRequest, first.request_id)
            view = await read_manual_request(session, row)
            assert view.status == "unavailable" and view.research is None
            assert (
                await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
            )
            next_view = await admit_manual_request(
                session,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                model_config_id=settings.agent_paper_formal_model_config_id,
                mode="research_only",
                idempotency_key="independent-after-interruption",
                contract_version=2,
            )
            assert next_view.status == "queued"
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,contract_version", [("research_only", 2), ("research_and_monitor", 3)]
)
@pytest.mark.parametrize("typed_after_claim", [False, True])
async def test_pure_expired_native_attempt_preserves_unknown_budget_and_blocks_next(
    migrated_database_url, tmp_path, monkeypatch, mode, contract_version, typed_after_claim
):
    from datetime import timedelta

    from nexus.jobs import worker
    from nexus.ops.manual_paper_host import run_pure_research_once
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from nexus.persistence.models.runtime import JobLease

    factory, settings, store, ids = await prepare_scope(
        migrated_database_url, tmp_path, bounded=mode == "research_and_monitor"
    )
    if mode == "research_and_monitor":
        await prepare_database(
            settings,
            **(
                OPTIONS
                | {
                    "max_wire_bytes": 100479,
                    "full_input_capacity": True,
                    "formal_bounded_stage_budget": True,
                    "price_monitoring": True,
                    "disclosure_monitoring": True,
                }
            ),
        )
        async with factory.begin() as session:
            await admit_manual_request(
                session,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                model_config_id=settings.agent_paper_formal_model_config_id,
                mode=mode,
                contract_version=contract_version,
                idempotency_key="unknown-new",
            )
    settings = settings.model_copy(update={"release_revision": "a" * 40})
    calls = []

    class Vanished(BaseException):
        pass

    from nexus.paper.research_failure import CapacityPreclaimRejected

    failure = (
        CapacityPreclaimRejected("CODEX_CAPACITY_POOL_WIRE_EXCEEDED")
        if typed_after_claim
        else Vanished()
    )

    async def disappear(session, **kwargs):
        calls.append(kwargs["claim"].job_id)
        raise Vanished() if typed_after_claim else failure

    if typed_after_claim:
        from nexus.ops.agent_paper_once import run_cycle_job_once as native_worker

        async def reject_after_claim(*args, **kwargs):
            try:
                return await native_worker(*args, **kwargs)
            except Vanished:
                # Model a faulty boundary classifying an already-owned attempt as preclaim.
                # Native claim/lease remain intact, so the host must reject the classification.
                raise failure

        monkeypatch.setattr("nexus.ops.agent_paper_once.run_cycle_job_once", reject_after_claim)
    monkeypatch.setattr(worker, "execute_and_finalize_job", disappear)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    try:
        with pytest.raises(type(failure)) as captured:
            await run_pure_research_once(settings, idempotency_key="unknown-new", mode=mode)
        assert captured.value is failure
        assert len(calls) == 1
        async with factory.begin() as session:
            row = await session.scalar(select(ManualPaperRequest))
            request_id = row.id
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == calls[0]))
            expiry = lease.expires_at
            assert (await read_manual_request(session, row)).status == "active"
        async with factory.begin() as session:
            row = await session.get(ManualPaperRequest, request_id)
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await finish_research_request(session, row=row, cancel_unused=True)

        async def after_expiry(session):
            return expiry + timedelta(seconds=1)

        monkeypatch.setattr("nexus.paper.research_request.database_now", after_expiry)
        async with factory.begin() as session:
            row = await session.get(ManualPaperRequest, request_id)
            closed = await finish_research_request(session, row=row, cancel_unused=True)
            assert closed.status == "unknown" and closed.cycle_id is None
            unknown = [m for m in closed.research.members if not m.usage_known]
            assert len(unknown) == 1
            run = await session.get(AgentResearchRunRecord, unknown[0].run_id)
            assert run.status == "failed"
            assert run.input_tokens is run.output_tokens is run.settled_cost_usd is None
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert budget.state == "reserved" and budget.settled_usd is None
            job = await session.get(Job, calls[0])
            assert job.status == "dead_letter" and job.manual_retry_count == 0
            assert (
                len(
                    [
                        m
                        for m in closed.research.members
                        if m.status == "abstained" and m.usage_known
                    ]
                )
                == 2
            )
        async with factory.begin() as session:
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await admit_manual_request(
                    session,
                    account_id=settings.agent_paper_account_id,
                    batch_id=settings.agent_paper_candidate_batch_id,
                    model_config_id=settings.agent_paper_formal_model_config_id,
                    mode=mode,
                    idempotency_key="blocked-after-unknown",
                    contract_version=contract_version,
                )
        assert len(calls) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_preparation_failure_is_not_replaced_by_cleanup_failure(
    migrated_database_url, tmp_path, monkeypatch
):
    import nexus.ops.manual_paper_host as host
    import nexus.paper.research_request as request_module

    factory, settings, store, ids = await prepare_scope(migrated_database_url, tmp_path)
    primary = RuntimeError("preparation-test-canary")
    original = host.record_manual_request_event

    async def prepare(*a, **kw):
        raise primary

    async def record(*a, **kw):
        if kw.get("status") == "unavailable":
            raise ValueError("cleanup-test-canary")
        return await original(*a, **kw)

    monkeypatch.setattr(request_module, "prepare_research_request", prepare)
    monkeypatch.setattr(host, "record_manual_request_event", record)
    try:
        async with factory.begin() as session:
            view = await admit_manual_request(
                session,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                model_config_id=settings.agent_paper_formal_model_config_id,
                mode="research_only",
                idempotency_key="preparation-primary",
                contract_version=2,
            )
        with pytest.raises(RuntimeError) as captured:
            await host.dispatch_pure_research_request(settings, request_id=view.request_id)
        assert captured.value is primary
        assert captured.value.__cause__.args == ("cleanup-test-canary",)
        from nexus.persistence.models.runtime import AuditEvent

        async with factory() as session:
            failure = await session.scalar(
                select(AuditEvent).where(AuditEvent.action == "research.request_failure")
            )
            assert failure.payload["failure_phase"] == "research_preparation"
            assert failure.payload["failure_code"] == "RESEARCH_FAILURE_UNCLASSIFIED"
            assert "canary" not in str(failure.payload)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "typed,close_fails",
    [
        (True, None),
        (False, None),
        (True, "finish"),
        (True, "cancel"),
        (True, "audit"),
        (True, "audit_commit"),
        (True, "postcondition_commit"),
    ],
)
@pytest.mark.parametrize(
    "mode,contract_version", [("research_only", 2), ("research_and_monitor", 3)]
)
async def test_preclaim_refusal_records_safe_reason_and_only_continues_after_native_close(
    migrated_database_url, tmp_path, monkeypatch, typed, close_fails, mode, contract_version
):
    from nexus.ops.manual_paper_host import dispatch_pure_research_request
    from nexus.persistence.models.runtime import AuditEvent

    factory, settings, store, ids = await prepare_scope(
        migrated_database_url, tmp_path, bounded=True
    )
    if mode == "research_and_monitor":
        await prepare_database(
            settings,
            **(
                OPTIONS
                | {
                    "max_wire_bytes": 100479,
                    "full_input_capacity": True,
                    "formal_bounded_stage_budget": True,
                    "price_monitoring": True,
                    "disclosure_monitoring": True,
                }
            ),
        )
    async with factory.begin() as session:
        receipt = await admit_manual_request(
            session,
            account_id=settings.agent_paper_account_id,
            batch_id=settings.agent_paper_candidate_batch_id,
            model_config_id=settings.agent_paper_formal_model_config_id,
            mode=mode,
            contract_version=contract_version,
            idempotency_key="preclaim-failure",
            two_flight=True,
        )
    calls = []

    async def reject(*a, **kw):
        calls.append("preclaim")
        raise ValueError("CODEX_CAPACITY_POOL_WIRE_EXCEEDED")

    async def untyped(*a, **kw):
        calls.append("untyped")
        raise ValueError("CODEX_CAPACITY_POOL_WIRE_EXCEEDED")

    async def failed_close(*a, **kw):
        raise ValueError("private-close-canary")

    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    monkeypatch.setattr("nexus.agents.capacity_cycle.prepare_cycle_capacity_pool", reject)
    if not typed:
        monkeypatch.setattr("nexus.ops.agent_paper_once.run_cycle_job_once", untyped)
    commit_body_completed = []
    if close_fails in {"audit_commit", "postcondition_commit"}:
        import nexus.paper.research_failure as failure_module
        from sqlalchemy import text

        target_name = (
            "record_research_failure"
            if close_fails == "audit_commit"
            else "require_unused_request_closed"
        )
        original_boundary = getattr(failure_module, target_name)

        async def deferred_commit_failure(session, **kwargs):
            result = await original_boundary(session, **kwargs)
            # Both statements succeed; PostgreSQL rejects the deferred uniqueness
            # constraint only when the real session context attempts COMMIT.
            await session.execute(
                text(
                    "CREATE TEMP TABLE preclaim_commit_probe "
                    "(value integer UNIQUE DEFERRABLE INITIALLY DEFERRED) ON COMMIT DROP"
                )
            )
            await session.execute(text("INSERT INTO preclaim_commit_probe VALUES (1), (1)"))
            commit_body_completed.append(target_name)
            return result

        monkeypatch.setattr(failure_module, target_name, deferred_commit_failure)
    elif close_fails:
        target = (
            "nexus.paper.research_request.finish_research_request"
            if close_fails == "finish"
            else "nexus.paper.manual_cancel.cancel_unused_manual_research"
            if close_fails == "cancel"
            else "nexus.paper.research_failure.record_research_failure"
        )
        monkeypatch.setattr(target, failed_close)
    try:
        if typed and not close_fails:
            from dataclasses import replace

            from nexus.ops import agent_paper_loop as loop
            from nexus.ops.manual_paper_host import dispatch_manual_request, inspect_manual_queue

            inspections, heartbeats, dispatched = [], [], []
            host_task = asyncio.current_task()

            async def inspect_queue():
                assert asyncio.current_task() is host_task
                snapshot = await inspect_manual_queue(
                    settings,
                    account_id=settings.agent_paper_account_id,
                    snapshot=loop.AgentPaperLoopSnapshot(now=datetime.now(UTC)),
                )
                inspections.append(snapshot.manual_pending_request_id)
                # Stop this controlled host only after it has actually reinspected
                # the persisted queue; never manufacture an empty queue or retry.
                return replace(snapshot, enabled=len(inspections) == 1)

            async def execute(action, snapshot):
                assert action == loop.AgentPaperLoopAction.TRIGGER_MANUAL
                dispatched.append(snapshot.manual_pending_request_id)
                await dispatch_manual_request(
                    settings,
                    settings,
                    request_id=snapshot.manual_pending_request_id,
                    repo_root=tmp_path,
                    calendar_authority_id=None,
                    process_environment={},
                )

            async def heartbeat(at):
                assert asyncio.current_task() is host_task
                heartbeats.append(at)

            result = await loop.run_foreground_loop(
                inspect=inspect_queue,
                execute=execute,
                sleep=asyncio.sleep,
                heartbeat=heartbeat,
            )
            assert result == loop.AgentPaperLoopAction.STOP_DISABLED
            assert inspections == [receipt.request_id, None]
            assert dispatched == [receipt.request_id]
            assert len(heartbeats) >= 2  # Before action and after its successful close.
        else:
            with pytest.raises(ValueError, match="CODEX_CAPACITY_POOL_WIRE_EXCEEDED") as captured:
                await dispatch_pure_research_request(settings, request_id=receipt.request_id)
            if close_fails in {"audit_commit", "postcondition_commit"}:
                from sqlalchemy.exc import IntegrityError

                assert isinstance(captured.value.__cause__, IntegrityError)
                assert len(commit_body_completed) == 1
            elif close_fails:
                assert captured.value.__cause__.args == ("private-close-canary",)
        assert len(calls) == 1
        async with factory.begin() as session:
            audit = await session.scalar(
                select(AuditEvent).where(
                    AuditEvent.action == "research.request_failure",
                    AuditEvent.payload["failure_phase"].as_string() != "research_cleanup",
                )
            )
            if close_fails in {"audit", "audit_commit"}:
                assert audit is None
            else:
                assert audit is not None
                assert audit.payload["failure_code"] == (
                    "CODEX_CAPACITY_POOL_WIRE_EXCEEDED"
                    if typed
                    else "RESEARCH_FAILURE_UNCLASSIFIED"
                )
                assert audit.payload["failure_phase"] == (
                    "research_preclaim" if typed else "research_dispatch"
                )
                assert "private" not in str(audit.payload)
            if close_fails in {"finish", "cancel"}:
                cleanup_audit = await session.scalar(
                    select(AuditEvent).where(
                        AuditEvent.action == "research.request_failure",
                        AuditEvent.payload["failure_phase"].as_string() == "research_cleanup",
                    )
                )
                assert cleanup_audit is not None
                assert cleanup_audit.payload["failure_code"] == "RESEARCH_FAILURE_UNCLASSIFIED"
                assert "private" not in str(cleanup_audit.payload)
            row = await session.get(ManualPaperRequest, receipt.request_id)
            view = await read_manual_request(session, row)
            assert view.status == (
                "active" if close_fails in {"finish", "cancel"} else "unavailable"
            )
            if typed and close_fails is None:
                next_view = await admit_manual_request(
                    session,
                    account_id=settings.agent_paper_account_id,
                    batch_id=settings.agent_paper_candidate_batch_id,
                    model_config_id=settings.agent_paper_formal_model_config_id,
                    mode=mode,
                    contract_version=contract_version,
                    idempotency_key="fresh-after-known-preclaim-rejection",
                    two_flight=True,
                )
                assert next_view.status == "queued"
                assert next_view.request_id != receipt.request_id

    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_shared_clock_snapshot_guard_still_rejects_future(migrated_database_url):
    from nexus.agents import snapshots

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            with pytest.raises(ValueError, match="OPENAI_RESEARCH_TIME_IN_FUTURE"):
                await snapshots.prepare_research_snapshot(
                    session, uuid4(), as_of=now + timedelta(minutes=1), research_only=True
                )
    finally:
        await factory.kw["bind"].dispose()
