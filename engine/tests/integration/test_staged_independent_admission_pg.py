"""Independent root and host read-only readiness share the staged lifecycle proof."""

from datetime import timedelta
from uuid import UUID

import pytest
from helpers.staged_public import install_public_readers
from integration.test_staged_roots_pg import (
    test_normal_three_stock_roots_use_one_staged_protocol as roots,
)
from integration.test_staged_worker_pg import ObservedCloseout
from nexus.ops.agent_paper_recovery import inspect_recovery_readiness
from nexus.paper import manual_request, research_request
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import Job, JobLease
from sqlalchemy import event, select, text
from sqlalchemy.engine import Engine


@pytest.mark.asyncio
@pytest.mark.parametrize("known", [True, False])
@pytest.mark.parametrize("cleanup", [True, False])
async def test_independent_public_close_and_host_readiness_require_same_proof(
    migrated_database_url, monkeypatch, tmp_path, known, cleanup, prior_native_close=False
):
    install_public_readers(monkeypatch)

    async def before_worker(captured, settings, options):
        captured["terminal_commit_fault"] = False
        if not known:
            captured["observation_mutation"] = "tokens"
        if not cleanup:
            captured["sdk_cleanup_failure"] = True
        captured["post_worker_hook"] = closed

    async def closed(factory, settings, captured):
        async with factory() as session:
            row = (await session.scalars(select(ManualPaperRequest))).one()
            target = UUID(captured["claim"].payload["run_id"])
            lease = await session.scalar(
                select(JobLease).where(JobLease.job_id == captured["claim"].job_id)
            )
            now = lease.expires_at + timedelta(seconds=1)
        captured["clock"][0] = now
        if not cleanup and not prior_native_close:
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                async with factory.begin() as session:
                    await research_request.finish_research_request(
                        session,
                        row=await session.get(ManualPaperRequest, row.id),
                        cancel_unused=True,
                    )
            # Whole public transaction rolls back, including orphan settlement.
            async with factory() as session:
                assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 0
                budget = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == target
                    )
                )
                assert budget.state == "reserved" and budget.settled_usd is None
        else:
            async with factory.begin() as session:
                old_marker = await session.scalar(
                    text("SELECT current_setting('nexus.stage_protocol',true)")
                )
                # This counterexample uses the original native consumer to
                # persist the very gap being fixed, not synthetic row mutation.
                from helpers.staged_public import _native_research_settled

                with monkeypatch.context() as old_consumer:
                    if prior_native_close:
                        old_consumer.setattr(
                            research_request,
                            "require_research_jobs_settled",
                            _native_research_settled,
                        )
                    closed = await research_request.finish_research_request(
                        session,
                        row=await session.get(ManualPaperRequest, row.id),
                        cancel_unused=True,
                    )
                assert closed.status == ("unavailable" if known else "unknown")
                assert (
                    await session.scalar(
                        text("SELECT current_setting('nexus.stage_protocol',true)")
                    )
                    or ""
                ) == (old_marker or "")
            async with factory() as session:
                run = await session.get(AgentResearchRunRecord, target)
                budget = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == target
                    )
                )
                assert run.status == "failed"
                assert budget.state == ("settled" if known else "reserved")
                assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 1
                assert (
                    await session.scalar(
                        text(
                            "SELECT count(*) FROM agent_daily_budget_reservations "
                            "WHERE state='released'"
                        )
                    )
                    == 2
                )
        if prior_native_close:
            from integration.test_research_orphan_seal_pg import original_facts

            async with factory.begin() as session:
                before = await original_facts(session)
                historical = await research_request.finish_research_request(
                    session, row=await session.get(ManualPaperRequest, row.id), cancel_unused=True
                )
                assert historical.status == "unavailable"
                assert await original_facts(session) == before
            # Existing terminal finish is a historical idempotent read, not a
            # current cleanup certificate; both admission gates below reject.
        runtime = settings.model_copy(
            update={"agent_paper_cycle_enabled": True, "release_revision": "a" * 40}
        )
        writes = []

        def observed(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.lstrip().split(None, 1)[0].upper() in {"INSERT", "UPDATE", "DELETE"}:
                writes.append(statement)

        event.listen(Engine, "before_cursor_execute", observed)
        try:
            if prior_native_close:
                from helpers.staged_public import _native_research_settled

                with monkeypatch.context() as original_consumer:
                    original_consumer.setattr(
                        research_request, "require_research_jobs_settled", _native_research_settled
                    )
                    assert (await inspect_recovery_readiness(runtime))["status"] == "ready"
            readiness = await inspect_recovery_readiness(runtime)
        finally:
            event.remove(Engine, "before_cursor_execute", observed)
        assert writes == []
        assert readiness["execution_locked"] and readiness["external_requests"] == 0
        assert readiness["status"] == ("ready" if known and cleanup else "blocked"), readiness
        if not (known and cleanup):
            assert readiness["blocking_codes"] == ["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"]
        await verify_activation_boundary(
            runtime, monkeypatch, tmp_path / "activation", ready=known and cleanup
        )
        async with factory.begin() as session:
            if known and cleanup:
                await manual_request.require_manual_account_available(
                    session, account_id=row.account_id, now=now
                )
                fresh = await manual_request.admit_manual_request(
                    session,
                    account_id=row.account_id,
                    batch_id=row.candidate_batch_id,
                    model_config_id=row.model_config_id,
                    mode="research_and_monitor",
                    contract_version=3,
                    two_flight=True,
                    idempotency_key="fresh-after-staged-close",
                )
                assert fresh.request_id != row.id and fresh.status == "queued"
            else:
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await manual_request.require_manual_account_available(
                        session, account_id=row.account_id, now=now
                    )
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=before_worker
        )


async def verify_activation_boundary(runtime, monkeypatch, root, *, ready):
    """Actual activation and lifetime-lock ordering, stopped before business code."""
    import json

    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import agent_paper_recovery as readiness
    from nexus.ops import agent_paper_recovery_activation as activation
    from nexus.settings import EngineSettings
    from unit.ops.test_recovery_activation import state

    root.mkdir()
    env, generation, attempt, _ = state(root)
    effective = EngineSettings(
        database_url=runtime.database_url,
        agent_runtime_mode="codex_sdk",
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=runtime.agent_paper_account_id,
        agent_paper_candidate_batch_id=runtime.agent_paper_candidate_batch_id,
        agent_paper_formal_model_config_id=runtime.agent_paper_formal_model_config_id,
        release_revision=runtime.release_revision,
    )
    native_lock = loop.acquire_agent_paper_lock
    callbacks = []
    reads = []
    inspect = readiness.inspect_recovery_readiness

    class BeforeBusiness(Exception):
        pass

    async def checked(settings):
        result = await inspect(settings)
        reads.append(result)
        return result

    async def forbidden(*args, **kwargs):
        callbacks.append(True)
        pytest.fail("activation must stop before heartbeat/calendar/business")

    with monkeypatch.context() as patch:
        patch.setattr(activation, "_boot_id", lambda: "11111111-1111-1111-1111-111111111111")
        # Isolated real mutexes, never the live manager's lock or recovery state.
        patch.setattr(
            activation,
            "_service_control_lock",
            lambda root: native_lock(runtime_root=root / "control-lock"),
        )
        patch.setattr(
            loop,
            "acquire_agent_paper_lock",
            lambda: native_lock(runtime_root=root / "lifetime-lock"),
        )
        patch.setattr(readiness, "require_recovery_process_quiescence", lambda: None)
        patch.setattr(readiness, "inspect_recovery_readiness", checked)
        patch.setattr(loop, "heartbeat_agent_paper_loop", forbidden)
        patch.setattr(loop, "_select_initial_service_calendar_authority_id", forbidden)
        guard = activation.make_recovery_guard(
            effective, repo_root=root, env_file=env, generation=generation, attempt_id=attempt
        )

        async def guarded(settings):
            # Exact native lock is already held, not a mocked null context.
            with pytest.raises(RuntimeError, match="AGENT_PAPER_LOOP_ALREADY_RUNNING"):
                with native_lock(runtime_root=root / "lifetime-lock"):
                    pytest.fail("second host lock acquired")
            await guard(settings)
            raise BeforeBusiness

        expected = BeforeBusiness if ready else ValueError
        with pytest.raises(expected):
            await loop.run_local_agent_paper_loop(
                effective,
                account_id=effective.agent_paper_account_id,
                repo_root=root,
                recovery_guard=guarded,
            )
        assert len(reads) == 1 and reads[0]["status"] == ("ready" if ready else "blocked")
        assert callbacks == []
        receipt = json.loads(guard.receipt_path.read_text())
        assert receipt["phase"] == ("activated" if ready else "rejected")


@pytest.mark.asyncio
async def test_host_rechecks_staged_cleanup_even_after_original_consumer_closed_request(
    migrated_database_url, monkeypatch, tmp_path
):
    await test_independent_public_close_and_host_readiness_require_same_proof(
        migrated_database_url, monkeypatch, tmp_path, True, False, prior_native_close=True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("database_error", [False, True])
async def test_native_unused_cancel_marker_does_not_leak_or_replace_original_error(
    migrated_database_url, monkeypatch, tmp_path, database_error
):
    from helpers import staged_public
    from sqlalchemy.exc import DBAPIError

    install_public_readers(monkeypatch)

    async def before_worker(captured, settings, options):
        factory = captured["fixture"][0]
        original = ValueError("controlled unused cancellation rejected")
        observed = []

        async def rejected(session, **kwargs):
            assert (
                await session.scalar(text("SELECT current_setting('nexus.stage_protocol',true)"))
                == "staged-experiment-v1"
            )
            if database_error:
                try:
                    await session.execute(text("SELECT * FROM missing_isolated_cancel_probe"))
                except DBAPIError as error:
                    observed.append(error)
                    raise
            raise original

        monkeypatch.setattr(staged_public, "_native_cancel_unused", rejected)
        if database_error:
            with pytest.raises(DBAPIError) as raised:
                async with factory.begin() as session:
                    await staged_public._cancel_unused(session)
            assert raised.value is observed[0]
            assert "PROTOCOL_MARKER_RESTORE_UNCONFIRMED" in raised.value.__notes__
        else:
            async with factory.begin() as session:
                old = await session.scalar(
                    text("SELECT current_setting('nexus.stage_protocol',true)")
                )
                with pytest.raises(ValueError) as raised:
                    await staged_public._cancel_unused(session)
                assert raised.value is original
                assert (
                    await session.scalar(
                        text("SELECT current_setting('nexus.stage_protocol',true)")
                    )
                    or ""
                ) == (old or "")
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT current_setting('nexus.stage_protocol',true)"))
                != "staged-experiment-v1"
            )
            assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 0
        assert captured.get("sdk_run_calls", 0) == 0
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=before_worker
        )


@pytest.mark.asyncio
async def test_independent_normal_success_uses_same_public_consumers(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers import staged_public

    install_public_readers(monkeypatch)
    read_proof = staged_public.read_terminal_proof
    seen = set()
    state = {}

    async def prove(session, **kwargs):
        # One atomic closeout has one timestamp even with an advancing clock.
        terminal_at = await session.scalar(
            text("SELECT closed_at FROM stage_proto_terminal WHERE run_id=:run"),
            {"run": kwargs["run_id"]},
        )
        job_at = await session.scalar(
            select(Job.finished_at).where(Job.payload["run_id"].astext == str(kwargs["run_id"]))
        )
        assert terminal_at == job_at
        proof = await read_proof(session, **kwargs)
        if proof is not None:
            assert proof.known and proof.cleanup_confirmed and not proof.orphan
            seen.add(proof.run_id)
        return proof

    async def before(captured, settings, options):
        state.update(factory=captured["fixture"][0], captured=captured, settings=settings)

    monkeypatch.setattr(staged_public, "read_terminal_proof", prove)
    await roots(migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=before)
    assert len(seen) == 3 and state["captured"]["sdk_run_calls"] == 9
    async with state["factory"]() as session:
        row = (await session.scalars(select(ManualPaperRequest))).one()
        view = await manual_request.read_manual_request(session, row)
        assert view.status == "completed"
        assert await session.scalar(text("SELECT count(*) FROM jobs WHERE status='succeeded'")) == 3
        assert (
            await session.scalar(
                text("SELECT count(*) FROM agent_daily_budget_reservations WHERE state='settled'")
            )
            == 3
        )
    runtime = state["settings"].model_copy(
        update={"agent_paper_cycle_enabled": True, "release_revision": "a" * 40}
    )
    result = await inspect_recovery_readiness(runtime)
    assert result["status"] == "ready", result
    assert result["execution_locked"] and result["external_requests"] == 0
    await verify_activation_boundary(
        runtime, monkeypatch, tmp_path / "normal-activation", ready=True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup", [True, False])
async def test_host_formal_cycle_history_also_checks_staged_cleanup(
    migrated_database_url, monkeypatch, tmp_path, cleanup
):
    from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
    from nexus.persistence.models.paper import AgentPaperCycleRecord

    install_public_readers(monkeypatch)

    async def before_worker(captured, settings, options):
        captured["terminal_commit_fault"] = False
        if not cleanup:
            captured["sdk_cleanup_failure"] = True
        captured["post_worker_hook"] = closed

    async def closed(factory, settings, captured):
        async with factory() as session:
            cycle = (await session.scalars(select(AgentPaperCycleRecord))).one()
            target = await session.get(
                AgentResearchRunRecord, UUID(captured["claim"].payload["run_id"])
            )
            lease = await session.scalar(
                select(JobLease).where(JobLease.job_id == captured["claim"].job_id)
            )
            now = lease.expires_at + timedelta(seconds=1)
        captured["clock"][0] = now
        from helpers.staged_public import _native_cycle_settled

        # Build the original consumer's persisted counterexample, then require
        # the current host reader to reject its missing cleanup evidence.
        with monkeypatch.context() as prior_consumer:
            if not cleanup:
                prior_consumer.setattr(
                    manual_request, "_require_cycle_jobs_settled", _native_cycle_settled
                )
            async with factory.begin() as session:
                result = await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=cycle.id,
                    idempotency_key="formal-known-orphan",
                    now=now,
                    formal_stage_capacity=True,
                )
                assert result.status == "unavailable"
        runtime = settings.model_copy(
            update={
                "agent_paper_cycle_enabled": True,
                "release_revision": "a" * 40,
                "agent_paper_account_id": cycle.account_id,
                "agent_paper_candidate_batch_id": cycle.candidate_batch_id,
                "agent_paper_formal_model_config_id": target.model_config_id,
            }
        )
        result = await inspect_recovery_readiness(runtime)
        assert result["status"] == ("ready" if cleanup else "blocked"), result
        if not cleanup:
            assert result["blocking_codes"] == ["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"]
        await verify_activation_boundary(
            runtime, monkeypatch, tmp_path / "formal-activation", ready=cleanup
        )
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, False, False, before_worker=before_worker
        )
