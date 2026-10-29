"""Disposable PostgreSQL protocol experiment; NEVER imported by production.

Uses real native claims, reservations and terminal guards. It deliberately does
not produce qualified final research or implement the SDK/Worker adapter.
"""

import hashlib
import json
import re
from dataclasses import dataclass
from uuid import UUID

from nexus.agents.execution import _persist_terminal, _settle_or_hold_usage
from nexus.agents.persistence import require_reserved_daily_budget
from nexus.agents.research_dispatch import dispatch_failure_boundary
from nexus.agents.responses_budget import UsageSnapshot
from nexus.jobs.repository import (
    JobCompletion,
    LeaseLostError,
    _lock_active_claim,
    execute_and_finalize_job,
)
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from sqlalchemy import select, text


class ProtocolRejected(ValueError):
    pass


@dataclass(frozen=True)
class Ticket:
    run_id: UUID
    stage: int
    claim: object


async def stage_evidence(session, run_id, snapshot=None):
    """One durable evidence projection for normal, orphan and proof readers.

    A terminal snapshot fixes membership, including absent facts. Later writes
    at the same timestamp cannot retroactively change its accounting evidence.
    """
    params = {"run": run_id}
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
            f"LEFT JOIN stage_proto_{table} {alias} ON a.run_id={alias}.run_id "
            f"AND a.stage={alias}.stage{condition}"
        )
    result = await session.execute(
        text(
            "SELECT a.stage,a.wire_hash,a.schema_hash,o.response_hash,o.input_tokens,"
            "o.output_tokens,o.eligible,o.valid,v.verified,c.confirmed "
            "FROM stage_proto_authorization a "
            + " ".join(joins)
            + " WHERE a.run_id=:run ORDER BY a.stage"
        ),
        params,
    )
    return [dict(r) for r in result.mappings()]


def evidence_digest(rows):
    return hashlib.sha256(
        json.dumps({"version": 1, "stages": rows}, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def usage_known(rows):
    return bool(rows) and all(
        r["verified"] is True and r["eligible"] and r["input_tokens"] is not None for r in rows
    )


DDL = (
    """CREATE TABLE stage_proto_scope (
        id uuid PRIMARY KEY, stopped boolean NOT NULL DEFAULT false,
        cycle_id uuid REFERENCES agent_paper_cycles(id),
        increment_id uuid REFERENCES research_monitor_increments(id),
        request_id uuid REFERENCES manual_paper_requests(id),
        CHECK(num_nonnulls(cycle_id,increment_id,request_id)=1),
        CHECK(id=coalesce(cycle_id,increment_id,request_id))
    )""",
    """CREATE TABLE stage_proto_member (
        run_id uuid PRIMARY KEY REFERENCES agent_research_runs(id),
        job_id uuid UNIQUE NOT NULL REFERENCES jobs(id),
        scope_id uuid NOT NULL REFERENCES stage_proto_scope(id),
        work_deadline timestamptz NOT NULL, cleanup_deadline timestamptz NOT NULL,
        CHECK(work_deadline < cleanup_deadline)
    )""",
    """CREATE TABLE stage_proto_authorization (
        run_id uuid REFERENCES stage_proto_member(run_id), stage integer CHECK(stage
        BETWEEN 1 AND 3),
        job_id uuid NOT NULL REFERENCES jobs(id), attempt_no integer NOT NULL,
        worker_id text NOT NULL, lease_token uuid NOT NULL, fence_token bigint NOT NULL,
        reserved_id uuid NOT NULL REFERENCES agent_daily_budget_reservations(id),
        wire_hash text NOT NULL CHECK(wire_hash ~ '^[0-9a-f]{64}$'),
        schema_hash text NOT NULL, prior_hash text, authorized_at timestamptz NOT NULL,
        PRIMARY KEY(run_id,stage), FOREIGN KEY(job_id,attempt_no) REFERENCES
        job_attempts(job_id,attempt_no)
    )""",
    """CREATE TABLE stage_proto_observation (
        run_id uuid, stage integer, response_hash text NOT NULL CHECK(response_hash ~
        '^[0-9a-f]{64}$'),
        input_tokens integer, output_tokens integer, valid boolean NOT NULL,
        observed_at timestamptz NOT NULL, eligible boolean NOT NULL, PRIMARY KEY(run_id,stage),
        FOREIGN KEY(run_id,stage) REFERENCES stage_proto_authorization(run_id,stage),
        CHECK((input_tokens IS NULL AND output_tokens IS NULL) OR
              (input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND
        input_tokens>=0 AND output_tokens>=0))
    )""",
    """CREATE TABLE stage_proto_verification (
        run_id uuid, stage integer, verified boolean NOT NULL,
        PRIMARY KEY(run_id,stage), FOREIGN KEY(run_id,stage) REFERENCES
        stage_proto_observation(run_id,stage)
    )""",
    """CREATE TABLE stage_proto_cleanup (
        run_id uuid, stage integer, confirmed boolean NOT NULL, observed_at timestamptz NOT NULL,
        PRIMARY KEY(run_id,stage), FOREIGN KEY(run_id,stage) REFERENCES
        stage_proto_authorization(run_id,stage)
    )""",
    """CREATE TABLE stage_proto_terminal (
        run_id uuid PRIMARY KEY REFERENCES stage_proto_member(run_id), known boolean NOT NULL,
        chain_hash text NOT NULL, closed_at timestamptz NOT NULL,
        observed_stages integer[] NOT NULL, evidence jsonb NOT NULL
    )""",
    """CREATE FUNCTION stage_proto_inflight(scope uuid) RETURNS bigint LANGUAGE sql STABLE AS $$
      SELECT count(*) FROM stage_proto_member m WHERE m.scope_id=scope AND EXISTS(
        SELECT 1 FROM stage_proto_authorization a WHERE a.run_id=m.run_id)
        AND (NOT EXISTS(SELECT 1 FROM stage_proto_terminal t WHERE t.run_id=m.run_id)
          OR EXISTS(SELECT 1 FROM stage_proto_authorization a WHERE a.run_id=m.run_id
            AND NOT EXISTS(SELECT 1 FROM stage_proto_cleanup c
              WHERE c.run_id=a.run_id AND c.stage=a.stage AND c.confirmed)))
    $$""",
    """CREATE FUNCTION stage_proto_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'stage fact immutable' USING ERRCODE='23514'; END $$""",
    """CREATE FUNCTION stage_proto_authorize_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE member stage_proto_member; stopped boolean;
    BEGIN
      SELECT * INTO STRICT member FROM stage_proto_member WHERE run_id=NEW.run_id;
      SELECT s.stopped INTO stopped FROM stage_proto_scope s WHERE s.id=member.scope_id FOR UPDATE;
      IF stopped OR NEW.authorized_at>=member.work_deadline OR EXISTS(
        SELECT 1 FROM stage_proto_terminal WHERE run_id=NEW.run_id) THEN
        RAISE EXCEPTION 'stage stopped' USING ERRCODE='23514'; END IF;
      IF NEW.stage=1 AND stage_proto_inflight(member.scope_id)>=2 THEN
        RAISE EXCEPTION 'two flight limit' USING ERRCODE='23514'; END IF;
      PERFORM 1 FROM jobs j JOIN job_leases l ON l.job_id=j.id
        JOIN job_attempts a ON a.job_id=j.id AND a.attempt_no=NEW.attempt_no
        JOIN agent_research_runs r ON r.id=NEW.run_id
        JOIN agent_daily_budget_reservations b ON b.run_id=r.id
        WHERE j.id=member.job_id AND j.id=NEW.job_id AND j.status='running'
        AND j.payload->>'run_id'=r.id::text AND r.status='prepared'
        AND l.lease_token=NEW.lease_token AND l.fence_token=NEW.fence_token
        AND l.worker_id=NEW.worker_id AND a.worker_id=l.worker_id
        AND a.lease_token=l.lease_token AND a.fence_token=l.fence_token AND a.outcome='running'
        AND j.attempt_count=NEW.attempt_no AND NEW.authorized_at>=l.acquired_at
        AND NEW.authorized_at<l.expires_at AND NEW.schema_hash=r.schema_hash
        AND b.id=NEW.reserved_id AND b.state='reserved' FOR UPDATE OF j,l,a,r,b;
      IF NOT FOUND THEN RAISE EXCEPTION 'stage binding invalid' USING ERRCODE='23514'; END IF;
      IF NEW.stage=1 AND NEW.prior_hash IS NOT NULL THEN
        RAISE EXCEPTION 'unexpected predecessor' USING ERRCODE='23514';
      ELSIF NEW.stage>1 AND NOT EXISTS(
        SELECT 1 FROM stage_proto_observation o JOIN stage_proto_cleanup c USING(run_id,stage)
        JOIN stage_proto_verification v USING(run_id,stage)
        WHERE o.run_id=NEW.run_id AND o.stage=NEW.stage-1 AND o.valid
        AND o.eligible AND o.input_tokens IS NOT NULL AND c.confirmed AND v.verified
        AND o.response_hash=NEW.prior_hash
        AND o.observed_at<=NEW.authorized_at AND c.observed_at<=NEW.authorized_at
      ) THEN RAISE EXCEPTION 'predecessor invalid' USING ERRCODE='23514'; END IF;
      RETURN NEW;
    END $$""",
    """CREATE FUNCTION stage_proto_stop_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP='DELETE' OR NEW.id<>OLD.id OR NEW.cycle_id IS DISTINCT FROM OLD.cycle_id
        OR NEW.increment_id IS DISTINCT FROM OLD.increment_id
        OR NEW.request_id IS DISTINCT FROM OLD.request_id
        OR (OLD.stopped AND NOT NEW.stopped) THEN
        RAISE EXCEPTION 'stop cannot be revoked' USING ERRCODE='23514'; END IF;
      RETURN NEW;
    END $$""",
    """CREATE FUNCTION stage_proto_member_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE inc uuid;
    BEGIN
      SELECT increment_id INTO inc FROM stage_proto_scope WHERE id=NEW.scope_id;
      IF inc IS NOT NULL AND NOT EXISTS(SELECT 1 FROM research_monitor_increments
        WHERE id=inc AND job_id=NEW.job_id AND run_id=NEW.run_id) THEN
        RAISE EXCEPTION 'increment owner invalid' USING ERRCODE='23514'; END IF;
      RETURN NEW;
    END $$""",
    "CREATE TRIGGER stage_proto_member_binding BEFORE INSERT ON stage_proto_member "
    "FOR EACH ROW EXECUTE FUNCTION stage_proto_member_guard()",
    "CREATE TRIGGER stage_proto_stop BEFORE UPDATE OR DELETE ON stage_proto_scope "
    "FOR EACH ROW EXECUTE FUNCTION stage_proto_stop_guard()",
    "CREATE TRIGGER stage_proto_authorize BEFORE INSERT ON stage_proto_authorization "
    "FOR EACH ROW EXECUTE FUNCTION stage_proto_authorize_guard()",
)


async def ensure_schema(session):
    name = await session.scalar(text("SELECT current_database()"))
    if not re.fullmatch(r"nexus_test_[0-9a-f]{32}", name):
        raise ProtocolRejected("DISPOSABLE_DATABASE_REQUIRED")
    if await session.scalar(text("SELECT to_regclass('stage_proto_scope')")) is None:
        for ddl in DDL:
            await session.execute(text(ddl))
        for table in (
            "member",
            "authorization",
            "observation",
            "verification",
            "cleanup",
            "terminal",
        ):
            await session.execute(
                text(
                    f"CREATE TRIGGER stage_proto_{table}_immutable BEFORE UPDATE OR DELETE "
                    f"ON stage_proto_{table} FOR EACH ROW EXECUTE FUNCTION stage_proto_immutable()"
                )
            )


class StagedProtocol:
    def __init__(self, factory, *, clock=None, on_failure=None):
        self.factory = factory
        self.on_failure = on_failure
        # Primitive matrix drivers supply explicit logical instants. The real
        # Worker adapter must supply its anchored advancing clock as well.
        self.clock = clock
        self.claims = {}
        self.local_stopped = False

    def _now(self, supplied):
        return supplied if self.clock is None else max(supplied, self.clock())

    async def install(
        self,
        *,
        scope_id,
        claims,
        now,
        work_deadline,
        cleanup_deadline,
        increment=False,
        independent=False,
    ):
        async with self.factory.begin() as session:
            await ensure_schema(session)
            await session.execute(
                text(
                    "INSERT INTO stage_proto_scope(id,cycle_id,increment_id,request_id) "
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
                    text("""INSERT INTO stage_proto_member
                    VALUES(:run,:job,:scope,:work,:cleanup)"""),
                    {
                        "run": UUID(job.payload["run_id"]),
                        "job": job.id,
                        "scope": scope_id,
                        "work": work_deadline,
                        "cleanup": cleanup_deadline,
                    },
                )
        # Only this first in-process native-claim owner may authorize. A newly
        # constructed reader does not acquire send rights by loading old rows.
        self.claims = {claim.job_id: claim for claim in claims}

    async def _member(self, session, claim):
        row = (
            (
                await session.execute(
                    text("""SELECT m.*,s.stopped
            FROM stage_proto_member m JOIN stage_proto_scope s ON s.id=m.scope_id
            WHERE m.job_id=:job FOR UPDATE OF s"""),
                    {"job": claim.job_id},
                )
            )
            .mappings()
            .one()
        )
        return row

    async def authorize(self, claim, *, stage, now, wire, not_after=None):
        if self.local_stopped or self.claims.get(claim.job_id) != claim:
            raise ProtocolRejected("ORIGIN_STOPPED_OR_UNAVAILABLE")
        async with self.factory() as session:
            await session.begin()
            member = await self._member(session, claim)
            if await session.scalar(
                text("SELECT 1 FROM stage_proto_authorization WHERE run_id=:run AND stage=:stage"),
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
            run = await session.get(AgentResearchRunRecord, member["run_id"])
            model = await session.get(AgentModelConfigRecord, run.model_config_id)
            budget = await require_reserved_daily_budget(session, run_id=run.id)
            prior = await session.scalar(
                text(
                    "SELECT response_hash FROM stage_proto_observation WHERE run_id=:run AND "
                    "stage=:prior"
                ),
                {"run": run.id, "prior": stage - 1},
            )

            def current_authority():
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
                text("""INSERT INTO stage_proto_authorization
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

    async def observe(self, ticket, *, now, payload, tokens, valid=True):
        # Append-only diagnostic observations may arrive after terminalization.
        # They never publish outputs, settle a budget or grant another send.
        async with self.factory() as session, dispatch_failure_boundary(session):
            await session.begin()
            member = await self._member(session, ticket.claim)
            auth = (
                (
                    await session.execute(
                        text(
                            "SELECT * FROM stage_proto_authorization WHERE run_id=:run AND "
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
                            "SELECT * FROM stage_proto_observation WHERE run_id=:run AND "
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
                    "INSERT INTO stage_proto_observation "
                    "VALUES(:run,:stage,:hash,:input,:output,:valid,:now,:eligible)"
                ),
                values,
            )
            await self._commit(session, member["scope_id"])

    async def verify(self, ticket, *, now, wire_hash, payload, tokens, not_after=None):
        # Caller supplies original transport evidence, never values read back
        # from the observation being verified. Rejection is durable too.
        def check_window():
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
                text("SELECT 1 FROM stage_proto_terminal WHERE run_id=:run"),
                {"run": ticket.run_id},
            ):
                raise ProtocolRejected("VERIFICATION_WINDOW_CLOSED")
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT a.*,o.response_hash,o.input_tokens,o.output_tokens,"
                            "o.observed_at "
                            "FROM stage_proto_authorization a JOIN stage_proto_observation o "
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
                    "SELECT verified FROM stage_proto_verification "
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
                    text("INSERT INTO stage_proto_verification VALUES(:run,:stage,:verified)"),
                    {"run": ticket.run_id, "stage": ticket.stage, "verified": matched},
                )
                check_window()
                await self._commit(session, member["scope_id"])
                check_window()
        # A commit already issued before cutoff may be found by original-identity
        # readback; a late acknowledgement does not grant new execution time.
        check_window()
        return matched

    async def cleanup(self, ticket, *, now, confirmed, not_after=None):
        def check_window():
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
                                "SELECT * FROM stage_proto_authorization WHERE run_id=:run AND "
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
                    text("INSERT INTO stage_proto_cleanup VALUES(:run,:stage,:confirmed,:now)"),
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

    async def stop(self, scope_id):
        self.local_stopped = True
        async with self.factory.begin() as session:
            await session.execute(
                text("UPDATE stage_proto_scope SET stopped=true WHERE id=:scope"),
                {"scope": scope_id},
            )

    async def _commit(self, session, scope_id):
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

    async def close(self, claim, *, now, orphan=False):
        async with self.factory() as session:
            await session.begin()
            known = await self.close_in_transaction(session, claim, now=now, orphan=orphan)
            member = await self._member(session, claim)
            await self._commit(session, member["scope_id"])
            return known

    async def close_in_transaction(self, session, claim, *, now, orphan=False):
        """Caller owns the whole public recovery transaction; never commits independently."""
        member = await self._member(session, claim)
        run = await session.get(AgentResearchRunRecord, member["run_id"], with_for_update=True)
        if await session.scalar(
            text("SELECT 1 FROM stage_proto_terminal WHERE run_id=:run"), {"run": run.id}
        ):
            raise ProtocolRejected("ALREADY_TERMINAL")
        await require_reserved_daily_budget(session, run_id=run.id)
        rows = await stage_evidence(session, run.id)
        if not rows:
            raise ProtocolRejected("NO_ZERO_CALL_PROOF")
        known = usage_known(rows)
        chain = evidence_digest(rows)
        model = await session.get(AgentModelConfigRecord, run.model_config_id)

        async def terminal(session):
            # Every result emitted by this prototype is a failed/unpublished
            # result, so failure stop must share the authorization boundary.
            await session.execute(
                text("UPDATE stage_proto_scope SET stopped=true WHERE id=:id"),
                {"id": member["scope_id"]},
            )
            if known:
                usage = UsageSnapshot(
                    input_tokens=sum(r["input_tokens"] for r in rows),
                    output_tokens=sum(r["output_tokens"] for r in rows),
                )
                if not await _settle_or_hold_usage(session, run, model=model, usage=usage):
                    raise ProtocolRejected("USAGE_NOT_SETTLED")
            await _persist_terminal(
                session,
                run,
                output_sequence=1,
                event_sequence=1,
                status="failed",
                reason="STAGED_PROTOTYPE_CLOSED"
                if known
                else "RESEARCH_LEASE_EXPIRED_RESULT_UNKNOWN",
            )
            await session.execute(
                text(
                    "INSERT INTO stage_proto_terminal "
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
                error_code="STAGED_PROTOTYPE_CLOSED",
            )

        if not orphan:
            await execute_and_finalize_job(session, claim=claim, checked_at=now, execute=terminal)
        else:
            job = await session.get(Job, claim.job_id, with_for_update=True)
            lease = await session.scalar(
                select(JobLease).where(JobLease.job_id == job.id).with_for_update()
            )
            attempt = await session.scalar(
                select(JobAttempt)
                .where(JobAttempt.job_id == job.id, JobAttempt.attempt_no == claim.attempt_no)
                .with_for_update()
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


async def require_staged_orphan_proof(session, *, run_id, now):
    # Stage-specific component only; caller still owes common cycle/model,
    # host-exit, account and explicit acceptance checks. This never seals.
    from nexus.paper.research_orphan_proof import require_expired_research_proof
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchEventRecord,
        AgentResearchOutputRecord,
    )

    terminal = (
        (
            await session.execute(
                text("SELECT * FROM stage_proto_terminal WHERE run_id=:run"), {"run": run_id}
            )
        )
        .mappings()
        .one_or_none()
    )
    if terminal is None or terminal["known"] or terminal["closed_at"] > now:
        raise ProtocolRejected("STAGED_TERMINAL_PROOF_INVALID")
    member = (
        (
            await session.execute(
                text(
                    "SELECT m.*,s.stopped FROM stage_proto_member m "
                    "JOIN stage_proto_scope s ON s.id=m.scope_id WHERE run_id=:run"
                ),
                {"run": run_id},
            )
        )
        .mappings()
        .one()
    )
    if not member["stopped"]:
        raise ProtocolRejected("STAGED_TERMINAL_PROOF_INVALID")
    run = await session.get(AgentResearchRunRecord, run_id)
    job = await session.get(Job, member["job_id"])
    lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
    attempt = await session.scalar(
        select(JobAttempt).where(
            JobAttempt.job_id == job.id, JobAttempt.attempt_no == job.attempt_count
        )
    )
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == run_id
        )
    )
    outputs = tuple(
        await session.scalars(
            select(AgentResearchOutputRecord)
            .where(AgentResearchOutputRecord.run_id == run_id)
            .order_by(AgentResearchOutputRecord.sequence)
        )
    )
    events = tuple(
        await session.scalars(
            select(AgentResearchEventRecord)
            .where(AgentResearchEventRecord.run_id == run_id)
            .order_by(AgentResearchEventRecord.sequence)
        )
    )
    await require_expired_research_proof(
        session,
        run=run,
        reservation=reservation,
        job=job,
        attempt=attempt,
        lease=lease,
        outputs=outputs,
        events=events,
        now=now,
    )
    auths = (
        (
            await session.execute(
                text("SELECT * FROM stage_proto_authorization WHERE run_id=:run ORDER BY stage"),
                {"run": run_id},
            )
        )
        .mappings()
        .all()
    )
    if not auths or [a["stage"] for a in auths] != list(range(1, len(auths) + 1)):
        raise ProtocolRejected("STAGED_TERMINAL_PROOF_INVALID")
    for auth in auths:
        if (
            auth["job_id"],
            auth["attempt_no"],
            auth["worker_id"],
            auth["lease_token"],
            auth["fence_token"],
            auth["reserved_id"],
            auth["schema_hash"],
        ) != (
            job.id,
            attempt.attempt_no,
            attempt.worker_id,
            attempt.lease_token,
            attempt.fence_token,
            reservation.id,
            run.schema_hash,
        ):
            raise ProtocolRejected("STAGED_TERMINAL_PROOF_INVALID")
    rows = await stage_evidence(session, run_id, terminal["evidence"])
    if rows != terminal["evidence"] or evidence_digest(rows) != terminal["chain_hash"]:
        raise ProtocolRejected("STAGED_TERMINAL_PROOF_INVALID")
    return {
        "run_id": run_id,
        "unknown": True,
        "authorized_stages": len(auths),
        "requires_explicit_acceptance": True,
        "common_scope_and_host_exit_still_required": True,
    }
