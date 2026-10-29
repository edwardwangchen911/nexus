"""Public lifecycle boundaries on real PostgreSQL; no live models or services."""

import asyncio
from datetime import timedelta

import pytest
from helpers.commit_ack_fault import CommitAcknowledgementLost, lose_commit_ack
from helpers.staged_protocol import ProtocolRejected, StagedProtocol
from helpers.staged_public import install_public_readers, read_terminal_proof
from integration import test_formal_research_orphan_pg as orphan_case
from integration import test_research_orphan_seal_pg as seal_module
from integration.test_staged_public_seal_pg import (
    test_staged_root_uses_normal_recovery_seal_audit_and_new_admission as root_case,
)
from integration.test_staged_worker_pg import ObservedCloseout, exercise
from nexus.monitoring.admission import finish_increment, require_monitor_account_available
from nexus.paper import history_seal
from nexus.persistence.models.monitoring import MonitorIncrement
from nexus.persistence.models.runtime import AuditEvent, JobLease
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


async def terminal_row(session, run_id):
    return dict(
        (
            await session.execute(
                text("SELECT * FROM stage_proto_terminal WHERE run_id=:run"), {"run": run_id}
            )
        )
        .mappings()
        .one()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("confirmed", [True, False])
async def test_cleanup_asof_and_complete_stage_membership(
    migrated_database_url, monkeypatch, tmp_path, confirmed
):
    install_public_readers(monkeypatch)
    cleanup = StagedProtocol.cleanup
    held = {}

    async def hold_last(self, ticket, **kwargs):
        if ticket.stage == 3:
            held.update(protocol=self, ticket=ticket)
        else:
            await cleanup(self, ticket, **kwargs)

    monkeypatch.setattr(StagedProtocol, "cleanup", hold_last)

    async def closed(factory, settings, captured):
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == member.job_id))
            now = lease.expires_at + timedelta(seconds=1)
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 3
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == 2
        assert await captured["protocol"].close(captured["claim"], now=now, orphan=True)
        async with factory() as session:
            before = await seal_module.original_facts(session)
            terminal = await terminal_row(session, member.run_id)
            proof = await read_terminal_proof(session, run_id=member.run_id, now=now)
            assert proof.known and not proof.cleanup_confirmed
        ticket = held["ticket"]
        async with factory() as session:
            authorized_at = await session.scalar(
                text(
                    "SELECT authorized_at FROM stage_proto_authorization "
                    "WHERE run_id=:run AND stage=3"
                ),
                {"run": member.run_id},
            )
        with pytest.raises(ProtocolRejected, match="CLEANUP_BINDING_INVALID"):
            await cleanup(
                held["protocol"], ticket, now=authorized_at - timedelta(seconds=1), confirmed=True
            )
        observed_at = now + timedelta(seconds=5)
        await cleanup(held["protocol"], ticket, now=observed_at, confirmed=confirmed)
        for asof, ready in ((now, False), (observed_at, confirmed)):
            async with factory.begin() as session:
                proof = await read_terminal_proof(session, run_id=member.run_id, now=asof)
                assert proof.known and proof.cleanup_confirmed is ready
                result = await finish_increment(
                    session, account_id=member.account_id, trigger_id=member.trigger_id, now=asof
                )
                assert result.status == ("failed" if ready else "admitted")
                if ready:
                    await require_monitor_account_available(
                        session, account_id=member.account_id, now=asof
                    )
                else:
                    with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                        await require_monitor_account_available(
                            session, account_id=member.account_id, now=asof
                        )
                assert await terminal_row(session, member.run_id) == terminal
                assert await seal_module.original_facts(session) == before
        assert captured["sdk_run_calls"] == 3
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={"terminal_commit_fault": False, "post_worker_hook": closed},
        )


@pytest.mark.asyncio
async def test_late_cleanup_seal_repeats_same_proof_after_diagnostic(
    migrated_database_url, monkeypatch, tmp_path
):
    state_ref = {}

    async def configure(state, patch):
        state_ref.update(state=state)
        recover = seal_module.recover_case

        async def after_recover(*args, **kwargs):
            await recover(*args, **kwargs)
            async with state["factory"]() as session:
                terminal = await terminal_row(session, state["target"])
                state["terminal"] = terminal
                state["facts"] = await seal_module.original_facts(session)
                proof = await read_terminal_proof(
                    session, run_id=state["target"], now=terminal["closed_at"]
                )
                assert not proof.known and not proof.cleanup_confirmed
            await state["protocol"].cleanup(
                state["ticket"], now=terminal["closed_at"], confirmed=True
            )

        patch.setattr(seal_module, "recover_case", after_recover)

    async def after_seal(state):
        async with state["factory"]() as session:
            audit = (
                await session.scalars(
                    select(AuditEvent).where(AuditEvent.action == "paper.legacy_research.sealed")
                )
            ).one()
            original_payload = dict(audit.payload)
            now = audit.occurred_at
            proof = await read_terminal_proof(session, run_id=state["target"], now=now)
            assert original_payload["cleanup_hash"] == proof.cleanup_hash
            assert original_payload["stage_chain_hash"] == proof.evidence_hash
            assert await seal_module.original_facts(session) == state["facts"]
        await state["protocol"].observe(
            state["ticket"],
            now=now + timedelta(seconds=2),
            payload=b"late diagnostic only",
            tokens=(50, 60),
        )
        for when in (now, now + timedelta(days=1)):
            async with state["factory"]() as session:
                assert await history_seal.legacy_research_sealed(
                    session, cycle_id=state["scope"], run_id=state["target"], now=when
                )
                audits = tuple(
                    await session.scalars(
                        select(AuditEvent).where(
                            AuditEvent.action == "paper.legacy_research.sealed"
                        )
                    )
                )
                assert len(audits) == 1 and audits[0].payload == original_payload
                assert await terminal_row(session, state["target"]) == state["terminal"]
                assert await seal_module.original_facts(session) == state["facts"]
                assert (
                    await session.scalar(
                        text("SELECT eligible FROM stage_proto_observation WHERE run_id=:run"),
                        {"run": state["target"]},
                    )
                    is False
                )
                assert (
                    await session.scalar(
                        text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                        {"run": state["target"]},
                    )
                    == "reserved"
                )

    await root_case(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "same_day",
        controls={"late_cleanup": True, "configure": configure, "after_seal": after_seal},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("server_commits", [False, True])
async def test_public_recovery_outer_commit_uncertainty_is_atomic(
    migrated_database_url, monkeypatch, tmp_path, server_commits
):
    state_ref = {}

    async def configure(state, patch):
        state_ref["state"] = state
        resume = orphan_case.resume_schwab_agent_paper_cycle
        state["resume_calls"] = 0

        async def uncertain(session, **kwargs):
            state["resume_calls"] += 1
            result = await resume(session, **kwargs)
            assert result.status == "unavailable"
            # Capture the complete outer transaction result before COMMIT, not
            # just two status columns; another engine must see all or none.
            state["after_recovery"] = await seal_module.original_facts(session)
            state["after_terminal"] = await terminal_row(session, state["target"])
            state["recovery_at"] = kwargs["now"]
            state["fault"] = lose_commit_ack(session.bind, server_commits=server_commits)
            state["fault_evidence"] = state["fault"].__enter__()
            return result

        patch.setattr(orphan_case, "resume_schwab_agent_paper_cycle", uncertain)

    try:
        with pytest.raises(CommitAcknowledgementLost, match="COMMIT_OUTCOME_UNCONFIRMED"):
            await root_case(
                migrated_database_url,
                monkeypatch,
                tmp_path,
                "same_day",
                controls={"configure": configure},
            )
    finally:
        state = state_ref["state"]
        if "fault" in state:
            state["fault"].__exit__(None, None, None)
    assert state["resume_calls"] == 1
    assert state["fault_evidence"].boundary_calls == 1
    assert state["fault_evidence"].server_commit_completed is server_commits
    engine = create_async_engine(migrated_database_url)
    try:
        async with async_sessionmaker(engine)() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == int(
                server_commits
            )
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
            assert (
                await session.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                    {"run": state["target"]},
                )
                == "reserved"
            )
            if not server_commits:
                assert await seal_module.original_facts(session) == state["before_recovery"]
            else:
                assert await seal_module.original_facts(session) == state["after_recovery"]
                assert await terminal_row(session, state["target"]) == state["after_terminal"]
                proof = await read_terminal_proof(
                    session, run_id=state["target"], now=state["recovery_at"]
                )
                assert not proof.known and proof.cleanup_confirmed and proof.orphan
                assert (
                    await session.scalar(
                        text("SELECT status FROM jobs WHERE id=:job"),
                        {"job": state["claim"].job_id},
                    )
                    == "dead_letter"
                )
                assert (
                    await session.scalar(
                        text("SELECT status FROM agent_research_runs WHERE id=:run"),
                        {"run": state["target"]},
                    )
                    == "failed"
                )
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='paper.legacy_research.sealed'"
                    )
                )
                == 0
            )
    finally:
        await engine.dispose()


async def wait_for_scope_block(factory, *, waiter, holder):
    """Wait for PostgreSQL's lock fact, never infer order from a small sleep."""
    async with asyncio.timeout(10):
        while True:
            async with factory() as observer:
                row = (
                    (
                        await observer.execute(
                            text(
                                "SELECT pg_blocking_pids(pid) AS blockers,query "
                                "FROM pg_stat_activity WHERE pid=:pid"
                            ),
                            {"pid": waiter},
                        )
                    )
                    .mappings()
                    .one()
                )
                if holder in row["blockers"]:
                    assert "stage_proto_scope" in row["query"]
                    return
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["recovery", "observation"])
async def test_public_recovery_serializes_with_late_stage_writer(
    migrated_database_url, monkeypatch, tmp_path, first
):
    async def configure(state, patch):
        from nexus.paper import research_orphan

        resume = orphan_case.resume_schwab_agent_paper_cycle
        lock = research_orphan._lock_research_orphan_rows
        member = StagedProtocol._member
        recover = seal_module.recover_case
        entered = asyncio.Event()
        release = asyncio.Event()
        state["tasks"] = []

        async def observed_member(self, session, claim):
            is_observer = asyncio.current_task() is state.get("observer")
            if is_observer:
                state["observer_pid"] = await session.scalar(text("SELECT pg_backend_pid()"))
                if first == "recovery":
                    entered.set()
            result = await member(self, session, claim)
            if is_observer and first == "observation":
                entered.set()
                await release.wait()
            return result

        patch.setattr(StagedProtocol, "_member", observed_member)

        def start_observer(now):
            state["observer"] = asyncio.create_task(
                state["protocol"].observe(
                    state["ticket"], now=now, payload=b"concurrent late diagnostic", tokens=(4, 7)
                )
            )
            state["tasks"].append(state["observer"])

        async def recovery_lock(session, **kwargs):
            rows = await lock(session, **kwargs)
            if first == "recovery" and rows and "observer" not in state:
                start_observer(kwargs["now"])
                await asyncio.wait_for(entered.wait(), 10)
                await wait_for_scope_block(
                    state["factory"], waiter=state["observer_pid"], holder=state["recovery_pid"]
                )
            return rows

        patch.setattr(research_orphan, "_lock_research_orphan_rows", recovery_lock)

        async def competing_resume(session, **kwargs):
            if "recovery_pid" not in state:
                state["recovery_pid"] = await session.scalar(text("SELECT pg_backend_pid()"))
                if first == "observation":
                    start_observer(kwargs["now"])
                    await asyncio.wait_for(entered.wait(), 10)

                    async def release_after_real_wait():
                        try:
                            await wait_for_scope_block(
                                state["factory"],
                                waiter=state["recovery_pid"],
                                holder=state["observer_pid"],
                            )
                        finally:
                            release.set()

                    state["tasks"].append(asyncio.create_task(release_after_real_wait()))
            result = await resume(session, **kwargs)
            if "frozen" not in state:
                state["frozen"] = await terminal_row(session, state["target"])
            return result

        patch.setattr(orphan_case, "resume_schwab_agent_paper_cycle", competing_resume)

        async def after_recover(*args, **kwargs):
            try:
                await recover(*args, **kwargs)
                await asyncio.wait_for(asyncio.gather(*state["tasks"]), 10)
                async with state["factory"]() as session:
                    assert await terminal_row(session, state["target"]) == state["frozen"]
                    assert state["frozen"]["known"] is False
                    assert (
                        await session.scalar(
                            text("SELECT eligible FROM stage_proto_observation WHERE run_id=:run"),
                            {"run": state["target"]},
                        )
                        is False
                    )
                    assert (
                        await session.scalar(
                            text(
                                "SELECT state FROM agent_daily_budget_reservations "
                                "WHERE run_id=:run"
                            ),
                            {"run": state["target"]},
                        )
                        == "reserved"
                    )
                    frozen_observed = state["frozen"]["observed_stages"]
                    assert frozen_observed == ([1] if first == "observation" else [])
            finally:
                release.set()
                for task in state["tasks"]:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*state["tasks"], return_exceptions=True)

        patch.setattr(seal_module, "recover_case", after_recover)

    await root_case(
        migrated_database_url, monkeypatch, tmp_path, "same_day", controls={"configure": configure}
    )


async def corrupt_isolated_stage_fact(factory, run_id, corruption):
    """First prove immutability; then inject corruption only in this disposable DB."""
    from sqlalchemy.exc import IntegrityError

    table, change = {
        "chain": ("stage_proto_terminal", "chain_hash=repeat('0',64)"),
        "known": ("stage_proto_terminal", "known=true"),
        "cleanup": ("stage_proto_cleanup", "confirmed=false"),
    }[corruption]
    async with factory.begin() as session:
        with pytest.raises(IntegrityError, match="stage fact immutable"):
            async with session.begin_nested():
                await session.execute(
                    text(f"UPDATE {table} SET {change} WHERE run_id=:run"), {"run": run_id}
                )
        await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
        await session.execute(
            text(f"UPDATE {table} SET {change} WHERE run_id=:run"), {"run": run_id}
        )
        await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["chain", "known", "cleanup"])
@pytest.mark.parametrize("phase", ["before_seal", "readback"])
async def test_public_seal_and_readback_reject_damaged_stage_proof(
    migrated_database_url, monkeypatch, tmp_path, corruption, phase
):
    state_ref = {}

    async def configure(state, patch):
        state_ref["state"] = state
        recover = seal_module.recover_case

        async def after_recover(*args, **kwargs):
            await recover(*args, **kwargs)
            async with state["factory"]() as session:
                state["native_before"] = await seal_module.original_facts(session)
            if phase == "before_seal":
                await corrupt_isolated_stage_fact(state["factory"], state["target"], corruption)

        patch.setattr(seal_module, "recover_case", after_recover)

    if phase == "before_seal":
        with pytest.raises(
            ValueError, match="STAGED_TERMINAL_PROOF_INVALID|MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
        ):
            await root_case(
                migrated_database_url,
                monkeypatch,
                tmp_path,
                "same_day",
                controls={"configure": configure},
            )
    else:
        await root_case(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "same_day",
            controls={"configure": configure},
        )
        state = state_ref["state"]
        await corrupt_isolated_stage_fact(state["factory"], state["target"], corruption)
        async with state["factory"]() as session:
            audit = (
                await session.scalars(
                    select(AuditEvent).where(AuditEvent.action == "paper.legacy_research.sealed")
                )
            ).one()
            with pytest.raises(
                ValueError, match="STAGED_TERMINAL_PROOF_INVALID|MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
            ):
                await history_seal.legacy_research_sealed(
                    session,
                    cycle_id=state["scope"],
                    run_id=state["target"],
                    now=audit.occurred_at + timedelta(days=1),
                )
    state = state_ref["state"]
    async with state["factory"]() as session:
        assert await seal_module.original_facts(session) == state["native_before"]
        assert await session.scalar(
            text("SELECT count(*) FROM audit_events WHERE action='paper.legacy_research.sealed'")
        ) == (0 if phase == "before_seal" else 1)
        assert (
            await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                {"run": state["target"]},
            )
            == "reserved"
        )


@pytest.mark.asyncio
async def test_monitor_finish_and_admission_do_not_consume_damaged_terminal(
    migrated_database_url, monkeypatch, tmp_path
):
    install_public_readers(monkeypatch)

    async def closed(factory, settings, captured):
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == member.job_id))
            now = lease.expires_at + timedelta(seconds=1)
        assert await captured["protocol"].close(captured["claim"], now=now, orphan=True)
        async with factory() as session:
            before = await seal_module.original_facts(session)
        await corrupt_isolated_stage_fact(factory, member.run_id, "chain")
        async with factory.begin() as session:
            with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
                await read_terminal_proof(session, run_id=member.run_id, now=now)
            result = await finish_increment(
                session, account_id=member.account_id, trigger_id=member.trigger_id, now=now
            )
            assert result.status == "admitted"
            with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_monitor_account_available(
                    session, account_id=member.account_id, now=now
                )
            assert await seal_module.original_facts(session) == before
        assert captured["sdk_run_calls"] == 3
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={"terminal_commit_fault": False, "post_worker_hook": closed},
        )
