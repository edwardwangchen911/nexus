"""Explicit disposition is distinct from cleanup and keeps every unknown reserve."""

from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from integration.test_independent_history_scope_pg import (
    test_accepted_history_three_stock_preparation_worker_and_close as exercise,
)
from nexus import worker
from nexus.agents.research_stages import executor
from nexus.ops import manual_paper
from nexus.paper import manual_preparation_close
from nexus.paper.research_request import finish_research_request
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "commit_unknown",
        "rollback",
        "maintenance",
        "late_before",
        "late_stage",
        "successor",
        "successor_late",
        "successor_ack_lost",
        "successor_root_ack_lost",
        "successor_rollback",
        "successor_policy_refusal",
    ],
)
async def test_zero_stage_disposition_normal_entry(
    migrated_database_url, monkeypatch, tmp_path, request, case
):
    is_successor = case.startswith("successor")
    late_case = case in {"late_stage", "successor_late"}

    async def check(factory, live, prepared, receipt, calls, clients, old):
        async def fail(*args, **kwargs):
            raise ValueError("frozen preauthorization failure")

        monkeypatch.setattr(executor.ShortTools, "execute", fail)
        first = prepared.research.members[0]
        assert await worker.run_agent_worker_once(
            live, job_ids=(first.job_id,), formal_stage_capacity=True
        )
        assert calls == [] and clients == []
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            result = await finish_research_request(db, row=row, cancel_unused=True)
            assert result.status == "unknown"
            # Retire the setup host through the normal exact heartbeat exit writer.
            from nexus.paper.manual_host_exit import record_host_exit
            from nexus.persistence.models.runtime import WorkerHeartbeat
            from sqlalchemy import select

            for host in await db.scalars(
                select(WorkerHeartbeat).where(WorkerHeartbeat.queue == "agent_paper")
            ):
                await record_host_exit(
                    db,
                    worker_id=host.worker_id,
                    started_at=host.started_at,
                    process_version=host.process_version,
                )

        monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_: None)
        monkeypatch.setattr(
            manual_paper, "preparation_service_control_lock", lambda *_: nullcontext()
        )
        monkeypatch.setattr(
            manual_preparation_close, "require_preparation_producers_stopped", lambda *_: None
        )
        args = SimpleNamespace(
            action="check-independent-acceptance",
            request_id=receipt.request_id,
            repo_root=tmp_path,
            env_file=tmp_path / "env",
        )
        checked = await manual_paper.run(live, args)
        assert checked.status == "eligible_at_check"
        async with factory() as db:
            assert (
                await db.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='paper.independent_unknown.accepted'"
                    )
                )
                == 0
            )
            held = (
                await db.execute(
                    text(
                        "SELECT state,settled_usd FROM agent_daily_budget_reservations "
                        "WHERE run_id=:id"
                    ),
                    {"id": first.run_id},
                )
            ).one()
            assert tuple(held) == ("reserved", None)

        from nexus.agents.research_stages.proof import read_terminal_proof
        from nexus.paper import independent_unknown_acceptance as acceptance
        from nexus.paper.independent_unknown_proof import FACT_FILTERS, independent_unknown_proof
        from nexus.paper.manual_request import require_manual_account_available
        from sqlalchemy.exc import DBAPIError
        from sqlalchemy.ext.asyncio import AsyncSession

        async def facts():
            async with factory() as db:
                return {
                    table: tuple(
                        await db.scalars(
                            text(
                                f"SELECT to_jsonb(t)::text FROM {table} t "
                                "ORDER BY to_jsonb(t)::text"
                            )
                        )
                    )
                    for table in FACT_FILTERS
                }

        before = await facts()
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            now = await db.scalar(text("SELECT clock_timestamp()"))
            with pytest.raises(ValueError):
                await independent_unknown_proof(db, row=row, now=now)
            proof = await read_terminal_proof(
                db, run_id=first.run_id, now=now, close_empty_unknown=True
            )
            assert not proof.cleanup_confirmed and not proof.known and not proof.budget_settled
            with pytest.raises(ValueError):
                await require_manual_account_available(
                    db, account_id=row.account_id, now=now, read_only=True
                )
        if is_successor:
            from nexus.paper.independent_priority import authorize

            async with factory.begin() as db:
                with pytest.raises(ValueError):
                    await authorize(
                        db,
                        live,
                        idempotency_key="not-accepted",
                        accept_priority=True,
                        predecessor_request_id=receipt.request_id,
                    )
        args.action = "accept-independent-unknown"
        args.accept_unknown_effects = False
        args.expected_proof_hash = checked.proof_hash
        with pytest.raises(ValueError):
            await manual_paper.run(live, args)
        args.accept_unknown_effects = True
        args.expected_proof_hash = "0" * 64
        with pytest.raises(ValueError):
            await manual_paper.run(live, args)
        args.expected_proof_hash = checked.proof_hash
        if case == "maintenance":
            from nexus.agents import research_maintenance

            monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
            with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
                await manual_paper.run(live, args)
            args.action = "check-independent-acceptance"
            assert (await manual_paper.run(live, args)).status == "eligible_at_check"
            assert before == await facts()
            return
        if case == "rollback":
            async with factory() as owner:
                await owner.begin()
                accepted = await acceptance.inspect_or_accept_independent_unknown(
                    owner,
                    settings=live,
                    request_id=receipt.request_id,
                    repo_root=tmp_path,
                    env_file=tmp_path / "env",
                    accept=True,
                    accept_unknown_effects=True,
                    expected_proof_hash=checked.proof_hash,
                )
                assert not accepted.replayed
                async with factory.begin() as writer:
                    with pytest.raises(DBAPIError) as busy:
                        await writer.execute(
                            text(
                                "SELECT id FROM research_stage_scope WHERE id=:id FOR UPDATE NOWAIT"
                            ),
                            {"id": receipt.request_id},
                        )
                    assert busy.value.orig.sqlstate == "55P03"
                    await writer.rollback()
                with pytest.raises(ValueError):
                    await manual_paper.run(live, args)
                await owner.rollback()
            assert before == await facts()
            args.action = "check-independent-acceptance"
            assert (await manual_paper.run(live, args)).status == "eligible_at_check"
            return
        if case == "commit_unknown":
            commit = AsyncSession.commit

            async def lost_ack(db):
                await commit(db)
                raise ConnectionError("synthetic lost acknowledgement")

            with monkeypatch.context() as m:
                m.setattr(AsyncSession, "commit", lost_ack)
                with pytest.raises(ConnectionError):
                    await manual_paper.run(live, args)
        else:
            accepted = await manual_paper.run(live, args)
            assert accepted.status == "independent_unknown_acknowledged" and not accepted.replayed
        args.action = "check-independent-acceptance"
        assert (await manual_paper.run(live, args)).status == "already_acknowledged"
        assert before == await facts()
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            now = await db.scalar(text("SELECT clock_timestamp()"))
            assert await acceptance.independent_unknown_accepted(db, row, now=now)
            await require_manual_account_available(
                db, account_id=row.account_id, now=now, read_only=True
            )
            with pytest.raises(ValueError):
                await acceptance.require_independent_execution_open(db, request_id=row.id)
            audit = (
                await db.execute(
                    text(
                        "SELECT schema_version,payload FROM audit_events "
                        "WHERE action='paper.independent_unknown.accepted'"
                    )
                )
            ).one()
            assert audit.schema_version == acceptance.ZERO_STAGE_SCHEMA
            assert audit.payload["zero_stage_disposition"]["cleanup_confirmed"] is False
            assert audit.payload["zero_stage_disposition"]["usage_unknown"] is True
            point = await db.begin_nested()
            try:
                await db.execute(
                    text("ALTER TABLE agent_daily_budget_reservations DISABLE TRIGGER USER")
                )
                await db.execute(
                    text(
                        "UPDATE agent_daily_budget_reservations SET state='released' "
                        "WHERE run_id=:run"
                    ),
                    {"run": first.run_id},
                )
                with pytest.raises(ValueError):
                    await acceptance.independent_unknown_accepted(db, row, now=now)
                with pytest.raises(ValueError):
                    await acceptance.require_independent_execution_open(db, request_id=row.id)
            finally:
                await point.rollback()
        assert before == await facts()
        assert calls == [] and clients == []

        if is_successor:
            from uuid import uuid4

            from nexus.paper.independent_priority import authorize, read_priority

            async with factory.begin() as db:
                with pytest.raises(ValueError, match="^INDEPENDENT_PRIORITY_INVALID$"):
                    await authorize(
                        db,
                        live,
                        idempotency_key="next-after-zero-disposition",
                        accept_priority=True,
                    )
            async with factory.begin() as db:
                with pytest.raises(ValueError):
                    await authorize(
                        db,
                        live,
                        idempotency_key="wrong-parent",
                        accept_priority=True,
                        predecessor_request_id=uuid4(),
                    )
            from nexus.paper.independent_priority import capture_predecessor

            unexpected = ValueError("synthetic non-policy read failure")

            async def unrelated_error(*_args, **_kwargs):
                raise unexpected

            with monkeypatch.context() as m:
                m.setattr(acceptance, "_validate", unrelated_error)
                async with factory.begin() as db:
                    with pytest.raises(ValueError) as failure:
                        await capture_predecessor(
                            db,
                            request_id=receipt.request_id,
                            account=live.agent_paper_account_id,
                            day=(await db.scalar(text("SELECT clock_timestamp()"))).date(),
                            now=await db.scalar(text("SELECT clock_timestamp()")),
                        )
                    assert failure.value is unexpected
            grant_args = manual_paper.parser().parse_args(
                [
                    "authorize-independent-priority",
                    "--idempotency-key",
                    "next-after-zero-disposition",
                    "--accept-priority",
                    "--predecessor-request-id",
                    str(receipt.request_id),
                ]
            )
            if case == "successor_rollback":
                async with factory() as holder:
                    await holder.begin()
                    await authorize(
                        holder,
                        live,
                        idempotency_key="rolled-back",
                        accept_priority=True,
                        predecessor_request_id=receipt.request_id,
                    )
                    await holder.rollback()
                async with factory() as db:
                    with pytest.raises(ValueError):
                        await read_priority(db, live, idempotency_key="rolled-back")
                assert before == await facts()
                return
            if case == "successor_ack_lost":
                original_commit = AsyncSession.commit

                async def lost_grant_ack(db):
                    await original_commit(db)
                    raise ConnectionError("synthetic grant acknowledgement lost")

                with monkeypatch.context() as m:
                    m.setattr(AsyncSession, "commit", lost_grant_ack)
                    with pytest.raises(ConnectionError):
                        await manual_paper.run(live, grant_args)
                renewed = await manual_paper.run(
                    live,
                    manual_paper.parser().parse_args(
                        [
                            "read-independent-priority",
                            "--idempotency-key",
                            "next-after-zero-disposition",
                        ]
                    ),
                )
            else:
                # The contender uses a separate connection while the winner's
                # account/day locks are still held; registration is the fence.
                from nexus.agents.budget import ResearchAccountingBusy

                async with factory.begin() as holder:
                    renewed = await authorize(
                        holder,
                        live,
                        idempotency_key="next-after-zero-disposition",
                        accept_priority=True,
                        predecessor_request_id=receipt.request_id,
                    )
                    async with factory.begin() as contender:
                        with pytest.raises(ResearchAccountingBusy):
                            await authorize(
                                contender,
                                live,
                                idempotency_key="racing-successor",
                                accept_priority=True,
                                predecessor_request_id=receipt.request_id,
                            )
            repeated = await manual_paper.run(live, grant_args)
            assert renewed == repeated and renewed.grant_unconsumed
            async with factory.begin() as db:
                with pytest.raises(ValueError):
                    await authorize(
                        db,
                        live,
                        idempotency_key="another-successor",
                        accept_priority=True,
                        predecessor_request_id=receipt.request_id,
                    )
            async with factory.begin() as db:
                historical = await read_priority(db, live, idempotency_key="full-new-root")
                assert historical.consumed_request_id == receipt.request_id
            assert before == await facts()

        import os

        if os.environ.get("NEXUS_TEST_HISTORY_FORMAL_IMAGE") and case == "commit_unknown":
            from helpers.independent_history_images import verify_images

            await verify_images(
                factory,
                migrated_database_url,
                receipt.request_id,
                old,
                expected_new="unknown",
                zero_disposition=True,
            )

        from datetime import timedelta

        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            now = await db.scalar(text("SELECT clock_timestamp()"))
            assert await acceptance.independent_unknown_accepted(
                db, row, now=now + timedelta(days=1)
            )
        if case == "late_before":
            from helpers.zero_stage_recovery_host import append_late_fact
            from nexus.ops.agent_paper_recovery import inspect_recovery_readiness

            await append_late_fact(factory, first.run_id)
            result = await inspect_recovery_readiness(live)
            assert result["status"] == "blocked" and result["failed_stage"] == "account_occupancy"
            async with factory.begin() as db:
                row = await db.get(ManualPaperRequest, receipt.request_id)
                now = await db.scalar(text("SELECT clock_timestamp()"))
                assert (
                    await acceptance.read_independent_disposition(db, row, now=now)
                    == "recheck_required"
                )
                with pytest.raises(ValueError):
                    await acceptance.require_independent_execution_open(db, request_id=row.id)
            return
        if case in {"normal", "late_stage"} or is_successor:
            from decimal import Decimal

            from nexus.monitoring.disclosure_context import configured_exhibit_store
            from nexus.ops.agent_paper_loop import (
                acquire_agent_paper_lock,
            )
            from nexus.ops.agent_paper_recovery import inspect_recovery_readiness

            ready = await inspect_recovery_readiness(live)
            assert ready["status"] == "ready", ready
            assert ready["external_requests"] == 0
            import asyncio
            import base64
            import json
            import os
            import sys
            from pathlib import Path

            from helpers.monitor_host_process import write_environment

            store_client = configured_exhibit_store(None)._client
            (tmp_path / "objects.json").write_text(
                json.dumps(
                    {
                        "objects": {
                            key: base64.b64encode(content).decode()
                            for key, content in store_client.objects.items()
                        },
                        "versions": store_client.versions,
                        "case": "policy_refusal"
                        if case == "successor_policy_refusal"
                        else "late_stage"
                        if late_case
                        else "normal"
                        if is_successor
                        else case,
                        "old_run": str(first.run_id),
                        "bucket": configured_exhibit_store(None)._bucket,
                        "prefix": configured_exhibit_store(None)._prefix,
                    }
                )
            )
            write_environment(
                live,
                tmp_path / "host.env",
                {"ALPACA_API_KEY_ID": "synthetic", "ALPACA_API_SECRET_KEY": "synthetic"},
            )
            env = os.environ.copy()
            env["PYTHONPATH"] = (
                str(Path("engine/src").resolve()) + os.pathsep + str(Path("engine/tests").resolve())
            )
            error_file = (tmp_path / "recovery-host-error.log").open("wb")
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "helpers.zero_stage_recovery_host",
                str(tmp_path),
                env=env,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=error_file,
            )
            try:
                line = await asyncio.wait_for(process.stdout.readline(), 35)
                assert line, (tmp_path / "recovery-host-error.log").read_text()
                signal = json.loads(line)
                assert (
                    signal["phase"] == "ready"
                    and signal["heartbeats"] == 2
                    and process.returncode is None
                )
                with pytest.raises(RuntimeError, match="AGENT_PAPER_LOOP_ALREADY_RUNNING"):
                    with acquire_agent_paper_lock(runtime_root=tmp_path / "isolated-host"):
                        pass
                if is_successor:
                    ready = await manual_paper.run(
                        live,
                        manual_paper.parser().parse_args(
                            [
                                "check-independent-admission",
                                "--idempotency-key",
                                "next-after-zero-disposition",
                            ]
                        ),
                    )
                    assert (
                        ready.status == "independent_request_queueable"
                        and not ready.budget_reserved
                    )
                start = manual_paper.parser().parse_args(
                    [
                        "start",
                        "--mode",
                        "research_and_monitor",
                        "--idempotency-key",
                        "next-after-zero-disposition",
                    ]
                )
                with pytest.raises(
                    ValueError,
                    match="INDEPENDENT_PRIORITY_INVALID"
                    if is_successor
                    else "INDEPENDENT_NATURAL_BUDGET_PROTECTED",
                ):
                    await manual_paper.run(
                        live.model_copy(update={"agent_daily_budget_usd": Decimal("1")}), start
                    )
                if case == "successor_root_ack_lost":
                    original_commit = AsyncSession.commit

                    async def lost_root_ack(db):
                        await original_commit(db)
                        raise ConnectionError("synthetic root acknowledgement lost")

                    with monkeypatch.context() as m:
                        m.setattr(AsyncSession, "commit", lost_root_ack)
                        with pytest.raises(ConnectionError):
                            await manual_paper.run(live, start)
                    known_grant = await manual_paper.run(
                        live,
                        manual_paper.parser().parse_args(
                            [
                                "read-independent-priority",
                                "--idempotency-key",
                                "next-after-zero-disposition",
                            ]
                        ),
                    )
                    successor = await manual_paper.run(
                        live,
                        manual_paper.parser().parse_args(
                            [
                                "read",
                                "--request-id",
                                str(known_grant.consumed_request_id),
                            ]
                        ),
                    )
                else:
                    successor = await manual_paper.run(live, start)
                assert successor.status == "queued" and successor.request_id != receipt.request_id
                assert (await manual_paper.run(live, start)).request_id == successor.request_id
                process.stdin.write(b"run\n")
                await process.stdin.drain()
                dispatched_line = await asyncio.wait_for(process.stdout.readline(), 90)
                assert dispatched_line, (tmp_path / "recovery-host-error.log").read_text()
                dispatched = json.loads(dispatched_line)
                assert dispatched["phase"] == "dispatched"
                if late_case:
                    assert dispatched["stages"] == ["research"]
                elif case == "successor_policy_refusal":
                    assert dispatched["stages"] == ["research"]
                    # The next stock's first stage and the first stock's critique
                    # both recheck the predecessor before any transport can open.
                    assert sorted(dispatched["authorization_refusals"]) == [
                        ["ProtocolRejected", "INDEPENDENT_PRIORITY_INVALID", 1],
                        ["ProtocolRejected", "INDEPENDENT_PRIORITY_INVALID", 2],
                    ]
                else:
                    assert dispatched["parallel_http"]
                    assert len(dispatched["stage_calls"]) == 3
                    assert all(
                        stages == ["research", "critique", "synthesis"]
                        for stages in dispatched["stage_calls"].values()
                    )
                    assert len(dispatched["stages"]) == 9
                assert dispatched["clients_closed"]
                stopped_line = await asyncio.wait_for(process.stdout.readline(), 35)
                assert stopped_line, (tmp_path / "recovery-host-error.log").read_text()
                stopped = json.loads(stopped_line)
                assert stopped["pid"] == signal["pid"]
                if late_case:
                    assert stopped["phase"] == "blocked"
                    assert stopped["code"] == "STAGED_TERMINAL_PROOF_INVALID"
                    assert await asyncio.wait_for(process.wait(), 10) == 1
                else:
                    assert stopped["phase"] == "stopped"
                    assert await asyncio.wait_for(process.wait(), 10) == 0
                async with factory.begin() as db:
                    row = await db.get(ManualPaperRequest, successor.request_id)
                    complete = await finish_research_request(db, row=row)
                    if case == "successor_policy_refusal":
                        # The parallel stock failed before its first authorization.
                        # Existing zero-stage policy retains unknown usage and money;
                        # a fixed refusal code does not certify a zero-cost terminal.
                        assert complete.status == "unknown"
                        assert complete.research is not None
                        uncertain = [m for m in complete.research.members if not m.usage_known]
                        assert len(uncertain) == 1 and uncertain[0].status == "failed"
                        unknown_run = uncertain[0].run_id
                        from nexus.agents.research_stages.protocol import ProtocolRejected

                        with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
                            await read_terminal_proof(
                                db,
                                run_id=unknown_run,
                                now=await db.scalar(text("SELECT clock_timestamp()")),
                            )
                        proof = await read_terminal_proof(
                            db,
                            run_id=unknown_run,
                            now=await db.scalar(text("SELECT clock_timestamp()")),
                            close_empty_unknown=True,
                        )
                        assert proof.empty_authorizations and not proof.known
                        assert not proof.budget_settled and not proof.cleanup_confirmed
                        unknown_hold = (
                            await db.execute(
                                text(
                                    "SELECT state,reserved_usd,settled_usd "
                                    "FROM agent_daily_budget_reservations WHERE run_id=:id"
                                ),
                                {"id": unknown_run},
                            )
                        ).one()
                        from decimal import Decimal

                        assert tuple(unknown_hold) == ("reserved", Decimal("2.12192"), None)
                        assert sorted(
                            (m.status, m.usage_known) for m in complete.research.members
                        ) == [("abstained", True), ("failed", False), ("failed", True)]
                    else:
                        assert complete.status == ("unknown" if late_case else "completed")
                    assert await finish_research_request(db, row=row) == complete
                    held = (
                        await db.execute(
                            text(
                                "SELECT state,settled_usd FROM agent_daily_budget_reservations "
                                "WHERE run_id=:id"
                            ),
                            {"id": first.run_id},
                        )
                    ).one()
                    assert tuple(held) == ("reserved", None)
                    if late_case or case == "successor_policy_refusal":
                        saved = (
                            (
                                await db.execute(
                                    text("""
                            SELECT a.run_id,a.stage,o.input_tokens,o.output_tokens,c.confirmed,
                                   t.known,b.state,b.settled_usd
                            FROM research_request_manifests f
                            JOIN research_request_members m ON m.manifest_id=f.id
                            JOIN research_stage_authorization a ON a.run_id=m.run_id
                            JOIN research_stage_observation o
                                ON o.run_id=a.run_id AND o.stage=a.stage
                            JOIN research_stage_cleanup c ON c.run_id=a.run_id AND c.stage=a.stage
                            JOIN research_stage_terminal t ON t.run_id=a.run_id
                            JOIN agent_daily_budget_reservations b ON b.run_id=a.run_id
                            WHERE f.request_id=:id
                        """),
                                    {"id": row.id},
                                )
                            )
                            .mappings()
                            .all()
                        )
                        assert len(saved) == 1 and saved[0]["stage"] == 1
                        usage = saved[0]
                        assert usage["input_tokens"] > 0 and usage["output_tokens"] > 0
                        assert usage["confirmed"] and usage["known"]
                        assert usage["state"] == "settled" and usage["settled_usd"] > 0
                        terminal = await read_terminal_proof(
                            db,
                            run_id=usage["run_id"],
                            now=await db.scalar(text("SELECT clock_timestamp()")),
                        )
                        assert (
                            terminal.known
                            and terminal.budget_settled
                            and terminal.cleanup_confirmed
                        )
                assert calls == [] and clients == []
            finally:
                if process.returncode is None:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), 10)
                error_file.close()
            if is_successor and not late_case and os.environ.get("NEXUS_TEST_HISTORY_FORMAL_IMAGE"):
                from helpers.independent_history_images import verify_images

                await verify_images(
                    factory,
                    migrated_database_url,
                    successor.request_id,
                    receipt.request_id,
                    priority_key="next-after-zero-disposition",
                )

    await exercise(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "priority" if is_successor else None,
        request,
        zero_stage_check=check,
    )
