"""Isolated corrupt-record probes; every mutation rolls back its savepoint."""

import pytest
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import ProtocolRejected
from sqlalchemy import text


async def corruptions_rejected(db, member, now):
    cases = [
        ("agent_research_runs", "status='completed'", "id=:run"),
        ("agent_research_runs", "input_tokens=1", "id=:run"),
        (
            "jobs",
            "status='running',finished_at=NULL,result_code=NULL,last_error_code=NULL",
            "id=:job",
        ),
        ("job_attempts", "outcome='lost'", "job_id=:job"),
        ("research_stage_terminal", "known=true", "run_id=:run"),
        ("research_stage_terminal", "chain_hash=repeat('0',64)", "run_id=:run"),
        ("agent_daily_budget_reservations", "state='released'", "run_id=:run"),
        (
            "research_stage_scope",
            "stopped=false",
            "id=(SELECT scope_id FROM research_stage_member WHERE run_id=:run)",
        ),
    ]
    for table, change, predicate in cases:
        point = await db.begin_nested()
        try:
            await db.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
            await db.execute(
                text(f"UPDATE {table} SET {change} WHERE {predicate}"),
                {"run": member.run_id, "job": member.job_id},
            )
            db.expire_all()
            with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
                await read_terminal_proof(
                    db, run_id=member.run_id, now=now, close_empty_unknown=True
                )
        finally:
            await point.rollback()
            db.expire_all()
    point = await db.begin_nested()
    try:
        await db.execute(text("ALTER TABLE research_stage_authorization DISABLE TRIGGER USER"))
        await db.execute(
            text("""INSERT INTO research_stage_authorization
            (run_id,stage,job_id,attempt_no,worker_id,lease_token,fence_token,reserved_id,
             wire_hash,schema_hash,prior_hash,authorized_at)
            SELECT r.id,1,j.id,a.attempt_no,a.worker_id,a.lease_token,a.fence_token,b.id,
              repeat('0',64),r.schema_hash,NULL,j.started_at
            FROM agent_research_runs r JOIN jobs j ON j.payload->>'run_id'=r.id::text
            JOIN job_attempts a ON a.job_id=j.id
            JOIN agent_daily_budget_reservations b ON b.run_id=r.id WHERE r.id=:run"""),
            {"run": member.run_id},
        )
        with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
            await read_terminal_proof(db, run_id=member.run_id, now=now, close_empty_unknown=True)
    finally:
        await point.rollback()
        db.expire_all()
