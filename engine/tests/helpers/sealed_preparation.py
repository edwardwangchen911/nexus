"""Synthetic closed preparation preceding a real isolated Worker/seal scenario."""

from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest
from nexus.ops import manual_paper
from nexus.paper import manual_preparation_close as close
from nexus.paper import manual_request
from nexus.paper.manual_entry import manual_host_scope
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, WorkerHeartbeat
from nexus.settings import EngineSettings
from sqlalchemy import func, select


async def seed_closed_preparation(session, monkeypatch, account_id, batch, model, *, at):
    old_at = at - timedelta(minutes=10)
    row = ManualPaperRequest(
        account_id=account_id,
        candidate_batch_id=batch.batch_id,
        model_config_id=model.id,
        model_hash=model.config_hash,
        batch_hash=batch.batch_hash,
        mode="research_and_paper",
        key_hash="c" * 64,
        request_hash=manual_request.manual_request_hash(
            account_id=account_id, batch_id=batch.batch_id, mode="research_and_paper"
        ),
        requested_at=old_at,
    )
    session.add(row)
    await session.flush()
    for status, code in (
        ("queued", "MANUAL_REQUEST_QUEUED"),
        ("preparing", "MANUAL_PREPARATION_STARTED"),
        ("unknown", "MANUAL_RESULT_UNKNOWN"),
    ):
        await manual_request._append(session, row, status=status, code=code, at=old_at)
    scope = manual_host_scope(
        EngineSettings(
            agent_runtime_mode="codex_sdk",
            agent_paper_account_id=account_id,
            agent_paper_candidate_batch_id=batch.batch_id,
            agent_paper_formal_model_config_id=model.id,
            agent_paper_manual_enabled=True,
            formal_budget_protection_enabled=False,
        )
    )
    session.add(
        WorkerHeartbeat(
            queue="agent_paper",
            worker_id=f"agent-paper-loop-{UUID(int=991)}:manual:{scope}",
            process_version="nexus-manual-paper-v1+" + close.QUALIFIED_ORIGINAL_REVISION,
            migration_head="0078_formal_v10_output_binding",
            started_at=old_at,
            seen_at=old_at,
        )
    )
    await session.flush()
    monkeypatch.setattr(close, "require_preparation_producers_stopped", lambda *a: None)
    receipt = await close.close_unstarted_preparation(
        session,
        account_id=account_id,
        request_id=row.id,
        now=at - timedelta(minutes=1),
        repo_root=Path.cwd(),
        env_file=Path("unused-isolated.env"),
    )
    assert not receipt.replayed
    assert await close.preparation_closed(session, row, now=at)
    assert (await manual_request.read_manual_request(session, row)).status == "unknown"
    return row.id


async def check_seal_with_preparation(
    sessions,
    runtime,
    args,
    historical_id,
    fault,
    *,
    run_id,
    lower_bound,
    now,
):
    async with sessions.begin() as session:
        historical = await session.get(ManualPaperRequest, historical_id)
        assert await close.preparation_closed(session, historical, now=now)
        if fault == "valid":
            return False
        if fault == "late-claim":
            session.add(
                IdempotencyRecord(
                    actor_id="local-agent-paper-schwab:" + str(historical.account_id),
                    route="agent-paper-explicit-manual-v1",
                    idempotency_key=str(historical.id),
                    request_hash="a" * 64,
                    schema_version="agent_paper_explicit_manual_v1",
                    response_payload={},
                )
            )
        elif fault in {"unclosed", "preparing", "active"}:
            # An independent unhandled request must still block the seal.
            row = ManualPaperRequest(
                account_id=historical.account_id,
                candidate_batch_id=historical.candidate_batch_id,
                model_config_id=historical.model_config_id,
                model_hash=historical.model_hash,
                batch_hash=historical.batch_hash,
                mode=historical.mode,
                key_hash="d" * 64,
                request_hash=historical.request_hash,
                requested_at=now,
            )
            session.add(row)
            await session.flush()
            for status, code in [
                ("queued", "MANUAL_REQUEST_QUEUED"),
                ("preparing", "MANUAL_PREPARATION_STARTED"),
            ]:
                await manual_request._append(session, row, status=status, code=code, at=now)
            if fault != "preparing":
                cycle_id = None
                if fault == "active":
                    original = await session.get(AgentPaperCycleRecord, args.cycle_id)
                    other = AgentPaperCycleRecord(
                        **{
                            column.name: getattr(original, column.name)
                            for column in original.__table__.columns
                            if column.name
                            not in {"id", "created_at", "decision_slot_at", "decision_time"}
                        },
                        decision_slot_at=now + timedelta(hours=1),
                        decision_time=now,
                    )
                    session.add(other)
                    await session.flush()
                    from nexus.paper import agent_cycle

                    await agent_cycle._append_event(
                        session,
                        other,
                        None,
                        "prepared",
                        "CYCLE_PREPARED",
                        {
                            "manual_request_id": str(row.id),
                            "manual_mode": row.mode,
                            "formal_model_config_id": str(row.model_config_id),
                            "formal_model_hash": row.model_hash,
                        },
                    )
                    cycle_id = other.id
                await manual_request._append(
                    session,
                    row,
                    status="active" if fault == "active" else "unknown",
                    code="MANUAL_CYCLE_CREATED" if fault == "active" else "MANUAL_RESULT_UNKNOWN",
                    cycle_id=cycle_id,
                    at=now,
                )
        elif fault == "audit-invalid":
            # Corrupt the loaded proof only; never disable append-only database guards.
            pass
        else:
            raise AssertionError(fault)
    from unittest.mock import patch

    original = close._proof

    async def changed_proof(session, row, *, at):
        proof = await original(session, row, at=at)
        return proof | {"fact_hash": "0" * 64}

    from contextlib import nullcontext

    with (
        patch.object(close, "_proof", changed_proof) if fault == "audit-invalid" else nullcontext()
    ):
        with pytest.raises(
            ValueError, match="MANUAL_(ACCOUNT_BUSY_OR_UNKNOWN|PREPARATION_CLOSE_BLOCKED)"
        ):
            await manual_paper.run(runtime, args)
    async with sessions() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "paper.legacy_research.sealed")
            )
            == 0
        )
        run = await session.get(AgentResearchRunRecord, run_id)
        assert run.status == "failed" and run.settled_cost_usd is None
        assert (run.input_tokens, run.output_tokens) == lower_bound
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == run_id
            )
        )
        assert reservation.state == "reserved" and reservation.settled_usd is None
        historical = await session.get(ManualPaperRequest, historical_id)
        assert (await manual_request.read_manual_request(session, historical)).status == "unknown"
    return True
