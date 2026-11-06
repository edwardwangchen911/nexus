"""Frozen zero-stage failure shape through the real producer, Worker and closeout."""

import json

import pytest
from integration.test_independent_history_scope_pg import (
    test_accepted_history_three_stock_preparation_worker_and_close as exercise,
)
from nexus import worker
from nexus.agents.research_stages import executor
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import ProtocolRejected, StagedProtocol
from nexus.paper.independent_unknown_proof import independent_unknown_proof
from nexus.paper.research_request import finish_research_request, require_research_jobs_settled
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import AuditEvent
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase", ["input_preparation", "compile", "authorization", "audit_failure", "safe_compile_code"]
)
async def test_zero_stage_unknown_closes_without_acceptance(
    migrated_database_url, monkeypatch, tmp_path, request, phase
):
    async def check(factory, live, prepared, receipt, calls, clients, old):
        async def fail(*args, **kwargs):
            raise ValueError("SECRET-MARKER-do-not-persist")

        if phase in {"input_preparation", "audit_failure"}:
            monkeypatch.setattr(executor.ShortTools, "execute", fail)
        elif phase == "authorization":

            async def denied(*args, **kwargs):
                raise ProtocolRejected("SECRET-MARKER-do-not-persist")

            monkeypatch.setattr(StagedProtocol, "authorize", denied)
        else:
            from nexus.agents import platform_wire

            original = StagedProtocol.install

            async def install(*args, **kwargs):
                result = await original(*args, **kwargs)

                def compile_failure(*args, **kwargs):
                    raise ValueError(
                        "PLATFORM_INPUT_TOO_LARGE"
                        if phase == "safe_compile_code"
                        else "SECRET-MARKER-do-not-persist"
                    )

                monkeypatch.setattr(platform_wire, "compile_native_platform_stage", compile_failure)
                return result

            monkeypatch.setattr(StagedProtocol, "install", install)
        member = prepared.research.members[0]
        if phase == "audit_failure":
            from nexus.agents.research_stages.executor import StagedFinalizationFailed
            from nexus.audit import service

            original_audit = service.append_audit_event

            async def audit_failure(session, **kwargs):
                if kwargs["action"] == "research.stage_failure":
                    raise RuntimeError("SECRET-MARKER-audit-failure")
                return await original_audit(session, **kwargs)

            monkeypatch.setattr(service, "append_audit_event", audit_failure)
            with pytest.raises(StagedFinalizationFailed, match="TERMINAL_TRANSACTION_FAILED"):
                await worker.run_agent_worker_once(
                    live, job_ids=(member.job_id,), formal_stage_capacity=True
                )
            assert calls == [] and clients == []
            async with factory() as db:
                assert (
                    await db.scalar(
                        text("SELECT count(*) FROM research_stage_terminal WHERE run_id=:run"),
                        {"run": member.run_id},
                    )
                    == 0
                )
                hold = (
                    await db.execute(
                        text(
                            "SELECT state,settled_usd FROM agent_daily_budget_reservations "
                            "WHERE run_id=:run"
                        ),
                        {"run": member.run_id},
                    )
                ).one()
                assert tuple(hold) == ("reserved", None)
                assert (
                    await db.scalar(
                        text("SELECT status FROM jobs WHERE id=:job"), {"job": member.job_id}
                    )
                    == "running"
                )
            return
        assert await worker.run_agent_worker_once(
            live, job_ids=(member.job_id,), formal_stage_capacity=True
        )
        assert calls == [] and clients == []
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            before = await db.scalar(
                text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE run_id=:run"),
                {"run": member.run_id},
            )
            assert before["state"] == "reserved" and before["settled_usd"] is None
            audits = list(
                await db.scalars(
                    select(AuditEvent).where(AuditEvent.action == "research.stage_failure")
                )
            )
            assert len(audits) == 1
            diagnostic = audits[0].payload
            assert diagnostic["failure_phase"] == (
                "compile" if phase == "safe_compile_code" else phase
            )
            assert diagnostic["stage"] == 1
            assert diagnostic["failure_code"] == (
                "PLATFORM_INPUT_TOO_LARGE"
                if phase == "safe_compile_code"
                else "RESEARCH_FAILURE_UNCLASSIFIED"
            )
            assert "SECRET-MARKER" not in json.dumps(diagnostic)
            now = await db.scalar(text("SELECT clock_timestamp()"))
            with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
                await read_terminal_proof(db, run_id=member.run_id, now=now)
            if phase == "input_preparation":
                proof = await read_terminal_proof(
                    db, run_id=member.run_id, now=now, close_empty_unknown=True
                )
                assert proof.empty_authorizations
                assert not proof.known and not proof.budget_settled and not proof.cleanup_confirmed
                # A terminal first member never licenses cancellation of active siblings.
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await finish_research_request(db, row=row)
                from helpers.zero_stage_guards import corruptions_rejected

                await corruptions_rejected(db, member, now)
                await db.refresh(row)
            frozen = {}
            for table in (
                "agent_research_runs",
                "agent_research_outputs",
                "research_stage_terminal",
            ):
                column = "id" if table == "agent_research_runs" else "run_id"
                frozen[table] = list(
                    await db.scalars(
                        text(f"SELECT to_jsonb(t)::text FROM {table} t WHERE {column}=:run"),
                        {"run": member.run_id},
                    )
                )
            closed = await finish_research_request(db, row=row, cancel_unused=True)
            assert closed.status == "unknown" and closed.code == "MANUAL_RESULT_UNKNOWN"
            if phase == "input_preparation":
                from sqlalchemy.exc import DBAPIError

                async with factory.begin() as writer:
                    with pytest.raises(DBAPIError) as locked:
                        await writer.execute(
                            text(
                                "SELECT id FROM research_stage_scope WHERE id=:scope "
                                "FOR UPDATE NOWAIT"
                            ),
                            {"scope": row.id},
                        )
                    assert locked.value.orig.sqlstate == "55P03"
                    await writer.rollback()
            count = await db.scalar(
                text("SELECT count(*) FROM manual_paper_request_events WHERE request_id=:id"),
                {"id": row.id},
            )
            assert await finish_research_request(db, row=row) == closed
            assert count == await db.scalar(
                text("SELECT count(*) FROM manual_paper_request_events WHERE request_id=:id"),
                {"id": row.id},
            )
            for table, facts in frozen.items():
                column = "id" if table == "agent_research_runs" else "run_id"
                assert facts == list(
                    await db.scalars(
                        text(f"SELECT to_jsonb(t)::text FROM {table} t WHERE {column}=:run"),
                        {"run": member.run_id},
                    )
                )
            assert "SECRET-MARKER" not in json.dumps(frozen)
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await independent_unknown_proof(db, row=row, now=now)
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_research_jobs_settled(db, row=row, view=closed.research, now=now)
            after = await db.scalar(
                text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE run_id=:run"),
                {"run": member.run_id},
            )
            assert before == after
            assert (
                await db.scalar(
                    text("SELECT count(*) FROM research_stage_authorization WHERE run_id=:run"),
                    {"run": member.run_id},
                )
                == 0
            )

        import os

        if os.environ.get("NEXUS_TEST_HISTORY_FORMAL_IMAGE"):
            from helpers.independent_history_images import verify_images

            await verify_images(
                factory, migrated_database_url, receipt.request_id, old, expected_new="unknown"
            )

    await exercise(
        migrated_database_url, monkeypatch, tmp_path, None, request, zero_stage_check=check
    )
