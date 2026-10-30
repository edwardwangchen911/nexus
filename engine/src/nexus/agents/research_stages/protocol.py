"""Durable research-stage authorization, evidence and fail-closed terminalization.

Migrated from the reviewed isolated mechanism. This module does not select old
Jobs, create schema, retry a request or release unknown reservations.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.agents.execution import _persist_terminal, _settle_or_hold_usage
from nexus.agents.persistence import require_reserved_daily_budget
from nexus.agents.research_dispatch import dispatch_failure_boundary
from nexus.agents.responses_budget import UsageSnapshot
from nexus.jobs.repository import (
    ClaimedJob,
    JobCompletion,
    LeaseLostError,
    _lock_active_claim,
    execute_and_finalize_job,
)
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease


class ProtocolRejected(ValueError):
    pass


def required[T](value: T | None) -> T:
    """Missing durable binding fails closed with a safe reason."""
    if value is None:
        raise ProtocolRejected("STAGED_BINDING_MISSING")
    return value


def research_run_id(job: Job) -> UUID:
    value = job.payload.get("run_id")
    if not isinstance(value, str):
        raise ProtocolRejected("STAGED_JOB_BINDING_INVALID")
    return UUID(value)


@dataclass(frozen=True)
class Ticket:
    run_id: UUID
    stage: int
    claim: ClaimedJob


async def stage_evidence(
    session: AsyncSession, run_id: UUID, snapshot: Sequence[Mapping[str, Any]] | None = None
) -> list[dict[str, Any]]:
    """One durable evidence projection for normal, orphan and proof readers.

    A terminal snapshot fixes membership, including absent facts. Later writes
    at the same timestamp cannot retroactively change its accounting evidence.
    """
    params: dict[str, Any] = {"run": run_id}
    joins = []
    for table, alias, field in (
        ("observation", "o", "response_hash"),
        ("verification", "v", "verified"),
        ("cleanup", "c", "confirmed"),
    ):
        condition = ""
        if snapshot is not None:
            params[alias] = [r["stage"] for r in snapshot if r[field] is not None]
            condition = f" AND {alias}.stage=ANY(:{alias})"
        joins.append(
            f"LEFT JOIN research_stage_{table} {alias} ON a.run_id={alias}.run_id "
            f"AND a.stage={alias}.stage{condition}"
        )
    result = await session.execute(
        text(
            "SELECT a.stage,a.wire_hash,a.schema_hash,o.response_hash,o.input_tokens,"
            "o.output_tokens,o.eligible,o.valid,v.verified,c.confirmed "
            "FROM research_stage_authorization a "
            + " ".join(joins)
            + " WHERE a.run_id=:run ORDER BY a.stage"
        ),
        params,
    )
    return [dict(r) for r in result.mappings()]


def evidence_digest(rows: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(
        json.dumps({"version": 1, "stages": rows}, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def usage_known(rows: Sequence[Mapping[str, Any]]) -> bool:
    return bool(rows) and all(
        r["verified"] is True and r["eligible"] and r["input_tokens"] is not None for r in rows
    )


def known_usage_lower_bound(rows: Sequence[Mapping[str, Any]]) -> UsageSnapshot:
    """Preserve individually verified usage without declaring the whole run known."""
    verified = [row for row in rows if usage_known((row,))]
    return UsageSnapshot(
        input_tokens=sum(row["input_tokens"] for row in verified),
        output_tokens=sum(row["output_tokens"] for row in verified),
        provider_calls=len(rows),
    )


async def ensure_schema(session: AsyncSession) -> None:
    """Require the migrated schema; runtime code never issues DDL."""
    if await session.scalar(text("SELECT to_regclass('research_stage_route')")) is None:
        raise ProtocolRejected("RESEARCH_STAGE_SCHEMA_REQUIRED")


class StagedProtocol:
    def __init__(
        self,
        factory: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] | None = None,
        on_failure: Callable[[BaseException], None] | None = None,
    ) -> None:
        self.factory = factory
        self.on_failure = on_failure
        # Primitive matrix drivers supply explicit logical instants. The real
        # Worker adapter must supply its anchored advancing clock as well.
        self.clock = clock
        self.claims: dict[UUID, ClaimedJob] = {}
        self.local_stopped = False

    def _now(self, supplied: datetime) -> datetime:
        return supplied if self.clock is None else max(supplied, self.clock())

    async def install(
        self,
        *,
        scope_id: UUID,
        claims: Sequence[ClaimedJob],
        now: datetime,
        work_deadline: datetime,
        cleanup_deadline: datetime,
        increment: bool = False,
        independent: bool = False,
    ) -> None:
        async with self.factory.begin() as session:
            await ensure_schema(session)
            await session.execute(
                text(
                    "INSERT INTO research_stage_scope(id,cycle_id,increment_id,request_id) "
                    "VALUES (:id,:cycle,:inc,:request) ON CONFLICT(id) DO NOTHING"
                ),
                {
                    "id": scope_id,
                    "cycle": None if increment or independent else scope_id,
                    "inc": scope_id if increment else None,
                    "request": scope_id if independent else None,
                },
            )
            for claim in claims:
                job, lease, _ = await _lock_active_claim(session, claim=claim, now=self._now(now))
                now = self._now(now)
                if not now < work_deadline < cleanup_deadline <= lease.expires_at:
                    raise ProtocolRejected("DEADLINE_INVALID")
                await session.execute(
                    text("""INSERT INTO research_stage_member
                    VALUES(:run,:job,:scope,:work,:cleanup)"""),
                    {
                        "run": research_run_id(job),
                        "job": job.id,
                        "scope": scope_id,
                        "work": work_deadline,
                        "cleanup": cleanup_deadline,
                    },
                )
        # Only this first in-process native-claim owner may authorize. A newly
        # constructed reader does not acquire send rights by loading old rows.
        self.claims = {claim.job_id: claim for claim in claims}

    async def _member(self, session: AsyncSession, claim: ClaimedJob) -> Mapping[str, Any]:
        row = (
            (
                await session.execute(
                    text("""SELECT m.*,s.stopped
            FROM research_stage_member m JOIN research_stage_scope s ON s.id=m.scope_id
            WHERE m.job_id=:job FOR UPDATE OF s"""),
                    {"job": claim.job_id},
                )
            )
            .mappings()
            .one()
        )
        return dict(row)

    async def authorize(
        self,
        claim: ClaimedJob,
        *,
        stage: int,
        now: datetime,
        wire: bytes,
        not_after: datetime | None = None,
    ) -> Ticket:
        from nexus.agents.research_maintenance import require_research_open

        require_research_open()
        if self.local_stopped or self.claims.get(claim.job_id) != claim:
            raise ProtocolRejected("ORIGIN_STOPPED_OR_UNAVAILABLE")
        async with self.factory() as session:
            await session.begin()
            member = await self._member(session, claim)
            from nexus.agents.research_stages.routes import selected_route
            from nexus.paper.independent_unknown_acceptance import (
                require_independent_execution_open,
            )

            route = await selected_route(session, claim.job_id)
            if route is not None and route["kind"] == "independent":
                await require_independent_execution_open(session, request_id=route["scope_id"])
            if await session.scalar(
                text(
                    "SELECT 1 FROM research_stage_authorization WHERE run_id=:run AND stage=:stage"
                ),
                {"run": member["run_id"], "stage": stage},
            ):
                raise ProtocolRejected("ALREADY_AUTHORIZED")
            now = self._now(now)
            cutoff = (
                member["work_deadline"]
                if not_after is None
                else min(member["work_deadline"], not_after)
            )
            if member["stopped"] or self.local_stopped or now >= cutoff:
                raise ProtocolRejected("STOPPED_OR_DEADLINE")
            job, lease, _ = await _lock_active_claim(session, claim=claim, now=now)
            run = required(await session.get(AgentResearchRunRecord, member["run_id"]))
            model = required(await session.get(AgentModelConfigRecord, run.model_config_id))
            budget = await require_reserved_daily_budget(session, run_id=run.id)
            prior = await session.scalar(
                text(
                    "SELECT response_hash FROM research_stage_observation WHERE run_id=:run AND "
                    "stage=:prior"
                ),
                {"run": run.id, "prior": stage - 1},
            )

            def current_authority() -> datetime:
                checked = self._now(now)
                if self.local_stopped or checked >= cutoff:
                    raise ProtocolRejected("STOPPED_OR_DEADLINE")
                if checked >= lease.expires_at:
                    raise LeaseLostError("job lease expired during authorization")
                if not model.pricing_available_at <= checked < model.pricing_expires_at:
                    raise ProtocolRejected("PRICING_EXPIRED")
                return checked

            now = current_authority()
            await session.execute(
                text("""INSERT INTO research_stage_authorization
                VALUES(:run,:stage,:job,:attempt,:worker,:token,:fence,:budget,:wire,:schema,:prior,:now)"""),
                {
                    "run": run.id,
                    "stage": stage,
                    "job": job.id,
                    "attempt": claim.attempt_no,
                    "worker": claim.worker_id,
                    "token": claim.lease_token,
                    "fence": claim.fence_token,
                    "budget": budget.id,
                    "wire": hashlib.sha256(wire).hexdigest(),
                    "schema": run.schema_hash,
                    "prior": prior,
                    "now": now,
                },
            )
            current_authority()
            await self._commit(session, member["scope_id"])
            # A durable authorization is not a renewed send window. If COMMIT
            # returns too late, retain it and its budget without returning a ticket.
            current_authority()
        # Session exit is asynchronous too. No await may remain between this
        # last original-deadline/lease/pricing check and handing out the ticket.
        current_authority()
        return Ticket(run.id, stage, claim)

    async def observe(
        self,
        ticket: Ticket,
        *,
        now: datetime,
        payload: bytes,
        tokens: tuple[int, int] | None,
        valid: bool = True,
    ) -> None:
        async with self.factory() as session, dispatch_failure_boundary(session):
            await session.begin()
            member = await self._member(session, ticket.claim)
            auth = (
                (
                    await session.execute(
                        text(
                            "SELECT * FROM research_stage_authorization WHERE run_id=:run AND "
                            "stage=:stage"
                        ),
                        {"run": ticket.run_id, "stage": ticket.stage},
                    )
                )
                .mappings()
                .one()
            )
            if (
                auth["lease_token"],
                auth["fence_token"],
                auth["job_id"],
                auth["attempt_no"],
                auth["worker_id"],
            ) != (
                ticket.claim.lease_token,
                ticket.claim.fence_token,
                ticket.claim.job_id,
                ticket.claim.attempt_no,
                ticket.claim.worker_id,
            ) or now < auth["authorized_at"]:
                raise ProtocolRejected("OBSERVATION_BINDING_INVALID")
            now = self._now(now)
            eligible = False
            try:
                await _lock_active_claim(session, claim=ticket.claim, now=now)
                now = self._now(now)
                eligible = now < member["cleanup_deadline"]
            except LeaseLostError:
                pass
            digest = hashlib.sha256(payload).hexdigest()
            old = (
                (
                    await session.execute(
                        text(
                            "SELECT * FROM research_stage_observation WHERE run_id=:run AND "
                            "stage=:stage"
                        ),
                        {"run": ticket.run_id, "stage": ticket.stage},
                    )
                )
                .mappings()
                .one_or_none()
            )
            values = {
                "run": ticket.run_id,
                "stage": ticket.stage,
                "hash": digest,
                "input": None if tokens is None else tokens[0],
                "output": None if tokens is None else tokens[1],
                "valid": valid,
                "now": now,
                "eligible": eligible,
            }
            if old:
                if (
                    old["response_hash"],
                    old["input_tokens"],
                    old["output_tokens"],
                    old["valid"],
                ) != (digest, values["input"], values["output"], valid):
                    raise ProtocolRejected("OBSERVATION_CONFLICT")
                return
            await session.execute(
                text(
                    "INSERT INTO research_stage_observation "
                    "VALUES(:run,:stage,:hash,:input,:output,:valid,:now,:eligible)"
                ),
                values,
            )
            await self._commit(session, member["scope_id"])

    async def verify(
        self,
        ticket: Ticket,
        *,
        now: datetime,
        wire_hash: str,
        payload: bytes,
        tokens: tuple[int, int] | None,
        not_after: datetime | None = None,
    ) -> bool:
        def check_window() -> None:
            if not_after is not None and self._now(now) >= not_after:
                raise ProtocolRejected("VERIFICATION_WINDOW_CLOSED")

        check_window()
        evidence_at = now
        async with self.factory() as session, dispatch_failure_boundary(session):
            await session.begin()
            member = await self._member(session, ticket.claim)
            await _lock_active_claim(session, claim=ticket.claim, now=self._now(now))
            now = self._now(now)
            check_window()
            if now >= member["cleanup_deadline"] or await session.scalar(
                text("SELECT 1 FROM research_stage_terminal WHERE run_id=:run"),
                {"run": ticket.run_id},
            ):
                raise ProtocolRejected("VERIFICATION_WINDOW_CLOSED")
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT a.*,o.response_hash,o.input_tokens,o.output_tokens,"
                            "o.observed_at "
                            "FROM research_stage_authorization a JOIN research_stage_observation o "
                            "USING(run_id,stage) WHERE a.run_id=:run AND a.stage=:stage"
                        ),
                        {"run": ticket.run_id, "stage": ticket.stage},
                    )
                )
                .mappings()
                .one()
            )
            if (
                row["job_id"],
                row["attempt_no"],
                row["worker_id"],
                row["lease_token"],
                row["fence_token"],
            ) != (
                ticket.claim.job_id,
                ticket.claim.attempt_no,
                ticket.claim.worker_id,
                ticket.claim.lease_token,
                ticket.claim.fence_token,
            ) or evidence_at < row["observed_at"]:
                raise ProtocolRejected("VERIFICATION_BINDING_INVALID")
            matched = (
                row["wire_hash"],
                row["response_hash"],
                row["input_tokens"],
                row["output_tokens"],
            ) == (
                wire_hash,
                hashlib.sha256(payload).hexdigest(),
                None if tokens is None else tokens[0],
                None if tokens is None else tokens[1],
            )
            old = await session.scalar(
                text(
                    "SELECT verified FROM research_stage_verification "
                    "WHERE run_id=:run AND stage=:stage"
                ),
                {"run": ticket.run_id, "stage": ticket.stage},
            )
            check_window()
            if old is not None:
                if old != matched:
                    raise ProtocolRejected("VERIFICATION_CONFLICT")
            else:
                if self._now(now) >= member["cleanup_deadline"]:
                    raise ProtocolRejected("VERIFICATION_WINDOW_CLOSED")
                await session.execute(
                    text("INSERT INTO research_stage_verification VALUES(:run,:stage,:verified)"),
                    {"run": ticket.run_id, "stage": ticket.stage, "verified": matched},
                )
                check_window()
                await self._commit(session, member["scope_id"])
                check_window()
        # A commit already issued before cutoff may be found by original-identity
        # readback; a late acknowledgement does not grant new execution time.
        check_window()
        return matched

    async def cleanup(
        self, ticket: Ticket, *, now: datetime, confirmed: bool, not_after: datetime | None = None
    ) -> None:
        def check_window() -> None:
            if not_after is not None and self._now(now) >= not_after:
                raise ProtocolRejected("CLEANUP_WINDOW_CLOSED")

        check_window()
        async with self.factory() as session, dispatch_failure_boundary(session):
            async with session.begin():
                # Same immutable origin binding as observations, not current owner.
                auth = (
                    (
                        await session.execute(
                            text(
                                "SELECT * FROM research_stage_authorization WHERE run_id=:run AND "
                                "stage=:stage"
                            ),
                            {"run": ticket.run_id, "stage": ticket.stage},
                        )
                    )
                    .mappings()
                    .one()
                )
                if (
                    auth["lease_token"],
                    auth["fence_token"],
                    auth["job_id"],
                    auth["attempt_no"],
                    auth["worker_id"],
                ) != (
                    ticket.claim.lease_token,
                    ticket.claim.fence_token,
                    ticket.claim.job_id,
                    ticket.claim.attempt_no,
                    ticket.claim.worker_id,
                ) or now < auth["authorized_at"]:
                    raise ProtocolRejected("CLEANUP_BINDING_INVALID")
                now = self._now(now)
                check_window()
                await session.execute(
                    text("INSERT INTO research_stage_cleanup VALUES(:run,:stage,:confirmed,:now)"),
                    {
                        "run": ticket.run_id,
                        "stage": ticket.stage,
                        "confirmed": confirmed,
                        "now": now,
                    },
                )
                check_window()
        # A lost/late COMMIT confirmation stays unknown to this caller, even
        # when subsequent original-identity readback finds the committed row.
        check_window()

    async def stop(self, scope_id: UUID) -> None:
        self.local_stopped = True
        async with self.factory.begin() as session:
            await session.execute(
                text("UPDATE research_stage_scope SET stopped=true WHERE id=:scope"),
                {"scope": scope_id},
            )

    async def _commit(self, session: AsyncSession, scope_id: UUID) -> None:
        try:
            await session.commit()
        except BaseException as error:
            self.local_stopped = True
            if self.on_failure is not None:
                self.on_failure(error)
            try:
                await session.rollback()
                await self.stop(scope_id)
            except BaseException:
                # Preserve original ambiguity. Failure to persist the stop is
                # an explicit remaining condition, not permission to dispatch.
                error.add_note("SCOPE_STOP_UNCONFIRMED")
            raise

    async def close(self, claim: ClaimedJob, *, now: datetime, orphan: bool = False) -> bool:
        async with self.factory() as session:
            await session.begin()
            known = await self.close_in_transaction(session, claim, now=now, orphan=orphan)
            member = await self._member(session, claim)
            await self._commit(session, member["scope_id"])
            return known

    async def close_in_transaction(
        self, session: AsyncSession, claim: ClaimedJob, *, now: datetime, orphan: bool = False
    ) -> bool:
        """Caller owns the whole public recovery transaction; never commits independently."""
        member = await self._member(session, claim)
        run = required(
            await session.get(AgentResearchRunRecord, member["run_id"], with_for_update=True)
        )
        if await session.scalar(
            text("SELECT 1 FROM research_stage_terminal WHERE run_id=:run"), {"run": run.id}
        ):
            raise ProtocolRejected("ALREADY_TERMINAL")
        await require_reserved_daily_budget(session, run_id=run.id)
        rows = await stage_evidence(session, run.id)
        if not rows:
            raise ProtocolRejected("NO_ZERO_CALL_PROOF")
        known = usage_known(rows)
        chain = evidence_digest(rows)
        model = required(await session.get(AgentModelConfigRecord, run.model_config_id))

        async def terminal(session: AsyncSession) -> JobCompletion:
            await session.execute(
                text("UPDATE research_stage_scope SET stopped=true WHERE id=:id"),
                {"id": member["scope_id"]},
            )
            if known:
                usage = UsageSnapshot(
                    input_tokens=sum(r["input_tokens"] for r in rows),
                    output_tokens=sum(r["output_tokens"] for r in rows),
                )
                if not await _settle_or_hold_usage(session, run, model=model, usage=usage):
                    raise ProtocolRejected("USAGE_NOT_SETTLED")
            else:
                lower_bound = known_usage_lower_bound(rows)
                if lower_bound.input_tokens or lower_bound.output_tokens:
                    run.input_tokens = lower_bound.input_tokens
                    run.output_tokens = lower_bound.output_tokens
            await _persist_terminal(
                session,
                run,
                output_sequence=1,
                event_sequence=1,
                status="failed",
                reason="STAGED_RESEARCH_CLOSED"
                if known
                else "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN",
            )
            await session.execute(
                text(
                    "INSERT INTO research_stage_terminal "
                    "VALUES(:run,:known,:chain,:now,:observed,CAST(:evidence AS jsonb))"
                ),
                {
                    "run": run.id,
                    "known": known,
                    "chain": chain,
                    "now": now,
                    "observed": [r["stage"] for r in rows if r["response_hash"] is not None],
                    "evidence": json.dumps(rows),
                },
            )
            return JobCompletion(
                outcome="failed",
                result_code=None,
                finished_at=now,
                error_code="STAGED_RESEARCH_CLOSED",
            )

        if not orphan:
            await execute_and_finalize_job(session, claim=claim, checked_at=now, execute=terminal)
        else:
            job = required(await session.get(Job, claim.job_id, with_for_update=True))
            lease = required(
                await session.scalar(
                    select(JobLease).where(JobLease.job_id == job.id).with_for_update()
                )
            )
            attempt = required(
                await session.scalar(
                    select(JobAttempt)
                    .where(JobAttempt.job_id == job.id, JobAttempt.attempt_no == claim.attempt_no)
                    .with_for_update()
                )
            )
            if (
                job.status != "running"
                or lease.expires_at > now
                or lease.lease_token != claim.lease_token
                or lease.fence_token != claim.fence_token
                or attempt.outcome != "running"
            ):
                raise ProtocolRejected("ORPHAN_NOT_QUALIFIED")
            # Exact expired native attempt, no claim or retry is created.
            attempt.outcome, attempt.finished_at, attempt.error_code = (
                "lost",
                now,
                "LEASE_EXPIRED",
            )
            job.status, job.finished_at, job.last_error_code = (
                "dead_letter",
                now,
                "LEASE_EXPIRED",
            )
            job.version += 1
            await session.flush()
            await terminal(session)
        return known
