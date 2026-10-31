"""Read native comparison proofs or close one expired original attempt; never dispatch."""

from dataclasses import asdict
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.agents.comparison_manifest import parse_comparison_manifest
from nexus.agents.comparison_routes import comparison_owner
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import ProtocolRejected, StagedProtocol
from nexus.agents.research_stages.routes import selected_route
from nexus.jobs.repository import ClaimedJob
from nexus.paper.research_request import database_now
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobDependency, JobLease


class ComparisonCloseoutUnknown(RuntimeError):
    """Commit outcome is unknown; inspect persisted evidence and do not retry."""

    def __init__(self) -> None:
        super().__init__("COMPARISON_CLOSEOUT_RESULT_UNKNOWN")


def _safe_protocol_error(error: ProtocolRejected) -> ValueError:
    code = {
        "NO_ZERO_CALL_PROOF": "COMPARISON_STAGE_AUTHORIZATION_REQUIRED",
        "ORPHAN_NOT_QUALIFIED": "COMPARISON_ORPHAN_NOT_QUALIFIED",
        "STAGED_BINDING_MISSING": "COMPARISON_STAGE_BINDING_INVALID",
        "STAGED_ROUTE_BINDING_INVALID": "COMPARISON_STAGE_BINDING_INVALID",
        "STAGED_TERMINAL_PROOF_INVALID": "COMPARISON_TERMINAL_PROOF_INVALID",
    }.get(str(error), "COMPARISON_NATIVE_PROOF_INVALID")
    return ValueError(code)


async def _position(
    session: AsyncSession, experiment_id: UUID, ordinal: int
) -> dict[str, Any] | None:
    if type(ordinal) is not int or ordinal < 1:
        raise ValueError("COMPARISON_POSITION_INVALID")
    registration = (
        (
            await session.execute(
                text(
                    "SELECT payload,manifest_hash,purpose "
                    "FROM research_comparison_manifest WHERE id=:id"
                ),
                {"id": experiment_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if registration is None:
        return None
    try:
        manifest = parse_comparison_manifest(registration["payload"])
    except ValueError:
        raise ValueError("COMPARISON_REGISTRATION_CONFLICT") from None
    if (manifest.experiment_id, manifest.digest, manifest.purpose) != (
        experiment_id,
        registration["manifest_hash"],
        registration["purpose"],
    ):
        raise ValueError("COMPARISON_REGISTRATION_CONFLICT")
    # Dynamic manifest positions, including the three-chain purpose.
    manifest.input_hash_for(ordinal)
    row = (
        (
            await session.execute(
                text("""SELECT c.run_id,c.job_id,c.reserved_id,
              r.status AS run_status,j.status AS job_status,j.attempt_count,
              b.state AS budget_state FROM research_comparison_chain c
              JOIN agent_research_runs r ON r.id=c.run_id
              JOIN jobs j ON j.id=c.job_id
              JOIN agent_daily_budget_reservations b ON b.id=c.reserved_id
              WHERE c.experiment_id=:id AND c.ordinal=:ordinal"""),
                {"id": experiment_id, "ordinal": ordinal},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    run = await session.get(AgentResearchRunRecord, row["run_id"])
    job = await session.get(Job, row["job_id"])
    if run is None or job is None or await comparison_owner(session, run, job) != experiment_id:
        raise ValueError("COMPARISON_NATIVE_BINDING_INVALID")
    route = await selected_route(session, row["job_id"])
    if route is not None and (route["run_id"], route["scope_id"], route["kind"]) != (
        row["run_id"],
        experiment_id,
        "comparison",
    ):
        raise ProtocolRejected("STAGED_ROUTE_BINDING_INVALID")
    return {
        **row,
        "stage_route": None if route is None else route["kind"],
        "stage_version": None if route is None else route["version"],
    }


async def inspect_comparison_terminal(
    session: AsyncSession, experiment_id: UUID, ordinal: int
) -> dict[str, Any]:
    """Read the original frozen proof. Absence grants neither zero-call nor replay rights."""
    try:
        return await _inspect(session, experiment_id, ordinal)
    except ProtocolRejected as error:
        raise _safe_protocol_error(error) from None


async def _inspect(session: AsyncSession, experiment_id: UUID, ordinal: int) -> dict[str, Any]:
    with session.no_autoflush:
        row = await _position(session, experiment_id, ordinal)
        if row is None:
            return {
                "status": "position_not_admitted",
                "inspection_only": True,
                "dispatch_authority": False,
                "replay_allowed": False,
                "stage_route": None,
                "stage_version": None,
                "stage_scope_stopped": None,
                "stage_chain_stopped": None,
                "terminal_proof": None,
            }
        terminal = await session.scalar(
            text("SELECT EXISTS(SELECT 1 FROM research_stage_terminal WHERE run_id=:run)"),
            {"run": row["run_id"]},
        )
        proof = None
        if terminal:
            if row["stage_route"] is None:
                raise ProtocolRejected("STAGED_ROUTE_BINDING_INVALID")
            proof = await read_terminal_proof(
                session, run_id=row["run_id"], now=await database_now(session)
            )
            if proof is None:
                raise ProtocolRejected("STAGED_TERMINAL_PROOF_INVALID")
        scope_stopped = (
            None
            if row["stage_route"] is None
            else await session.scalar(
                text("SELECT stopped FROM research_stage_scope WHERE id=:id"), {"id": experiment_id}
            )
        )
        chain_stopped = await session.scalar(
            text("SELECT EXISTS(SELECT 1 FROM research_comparison_chain_stop WHERE run_id=:run)"),
            {"run": row["run_id"]},
        )
        return {
            "status": "position_registered",
            "inspection_only": True,
            "dispatch_authority": False,
            "replay_allowed": False,
            **{k: str(v) if isinstance(v, UUID) else v for k, v in row.items()},
            "stage_scope_stopped": scope_stopped,
            "stage_chain_stopped": chain_stopped,
            "terminal_proof": None
            if proof is None
            else {k: str(v) if isinstance(v, UUID) else v for k, v in asdict(proof).items()},
        }


async def close_expired_comparison_position(
    factory: async_sessionmaker[AsyncSession], experiment_id: UUID, ordinal: int
) -> dict[str, Any]:
    """One atomic native orphan closeout; uncertain commit requires inspection, not retry."""
    try:
        return await _close_expired(factory, experiment_id, ordinal)
    except ProtocolRejected as error:
        raise _safe_protocol_error(error) from None


async def _close_expired(
    factory: async_sessionmaker[AsyncSession], experiment_id: UUID, ordinal: int
) -> dict[str, Any]:
    async with factory() as session:
        await session.begin()
        # Same serialization point as authorize/observe/normal close. No scope creation.
        scope = await session.scalar(
            text("SELECT comparison_id FROM research_stage_scope WHERE id=:id FOR UPDATE"),
            {"id": experiment_id},
        )
        if scope != experiment_id:
            raise ValueError("COMPARISON_REGISTRATION_REQUIRED")
        receipt = await inspect_comparison_terminal(session, experiment_id, ordinal)
        if receipt["status"] == "position_not_admitted":
            raise ValueError("COMPARISON_POSITION_NOT_ADMITTED")
        if receipt["terminal_proof"] is not None:
            return receipt  # No commit, new facts, replacement claim or restored send rights.
        run_id, job_id = UUID(receipt["run_id"]), UUID(receipt["job_id"])
        run = await session.get(AgentResearchRunRecord, run_id, with_for_update=True)
        job = await session.get(Job, job_id, with_for_update=True)
        leases = tuple(
            await session.scalars(
                select(JobLease).where(JobLease.job_id == job_id).with_for_update()
            )
        )
        attempts = tuple(
            await session.scalars(
                select(JobAttempt).where(JobAttempt.job_id == job_id).with_for_update()
            )
        )
        if run is None or job is None or len(leases) != 1 or len(attempts) != 1:
            raise ProtocolRejected("ORPHAN_NOT_QUALIFIED")
        lease, attempt = leases[0], attempts[0]
        now = await database_now(session)
        if lease.expires_at > now:
            raise ValueError("COMPARISON_LEASE_ACTIVE")
        if (
            job.status != "running"
            or job.attempt_count != 1
            or attempt.attempt_no != 1
            or attempt.outcome != "running"
            or attempt.finished_at is not None
            or (attempt.worker_id, attempt.lease_token, attempt.fence_token, attempt.started_at)
            != (lease.worker_id, lease.lease_token, lease.fence_token, job.started_at)
            or lease.acquired_at != job.started_at
        ):
            raise ProtocolRejected("ORPHAN_NOT_QUALIFIED")
        # The original comparison claim forbids dependencies; do not fabricate their result.
        if await session.scalar(select(JobDependency.id).where(JobDependency.job_id == job_id)):
            raise ProtocolRejected("ORPHAN_NOT_QUALIFIED")
        authorized = await session.scalar(
            text("SELECT EXISTS(SELECT 1 FROM research_stage_authorization WHERE run_id=:run)"),
            {"run": run_id},
        )
        if not authorized:
            raise ProtocolRejected("NO_ZERO_CALL_PROOF")
        member = (
            await session.execute(
                text("SELECT run_id,scope_id FROM research_stage_member WHERE job_id=:job"),
                {"job": job_id},
            )
        ).one_or_none()
        if member is None or tuple(member) != (run_id, experiment_id):
            raise ProtocolRejected("STAGED_BINDING_MISSING")
        claim = ClaimedJob(
            job_id=job.id,
            job_type=job.job_type,
            schema_version=job.schema_version,
            payload=job.payload,
            queue=job.queue,
            priority=job.priority,
            retry_policy=job.retry_policy,
            max_attempts=job.max_attempts,
            attempt_no=attempt.attempt_no,
            effective_at=job.effective_at,
            worker_id=lease.worker_id,
            lease_token=lease.lease_token,
            fence_token=lease.fence_token,
            expires_at=lease.expires_at,
            version=job.version,
            dependencies_satisfied=True,
            dependency_result_code=None,
        )
        await StagedProtocol(factory).close_in_transaction(session, claim, now=now, orphan=True)
        await session.flush()
        receipt = await inspect_comparison_terminal(session, experiment_id, ordinal)
        if receipt["terminal_proof"] is None:
            raise ProtocolRejected("STAGED_TERMINAL_PROOF_INVALID")
        try:
            await session.commit()
        except BaseException:
            # The transaction may already have committed. Never replay or disclose driver detail.
            try:
                await session.rollback()
            except BaseException:
                pass
            raise ComparisonCloseoutUnknown() from None
        return receipt
