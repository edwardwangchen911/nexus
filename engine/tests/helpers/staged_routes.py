"""Disposable preparation-time protocol selection; never a production migration."""

from uuid import UUID

from helpers.staged_protocol import ProtocolRejected, ensure_schema
from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.paper_context import (
    FrozenAgentPaperContextStatus,
    read_agent_paper_research_context,
    revalidate_frozen_agent_paper_context,
)
from nexus.monitoring.independent_storage import read_independent_context
from nexus.paper.research_request import read_research_manifest
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.monitoring import MonitorIncrement
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job
from sqlalchemy import select, text

VERSION = "staged-experiment-v1"


async def owner(session, run, job):
    increment = await session.scalar(
        select(MonitorIncrement).where(
            MonitorIncrement.run_id == run.id, MonitorIncrement.job_id == job.id
        )
    )
    if increment is not None:
        from nexus.monitoring.admission import require_increment_binding

        await require_increment_binding(session, run)
        return increment.id, "increment"
    independent = await read_independent_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if independent is not None:
        request = await session.get(ManualPaperRequest, independent[0].request_id)
        manifest = await read_research_manifest(session, request)
        if manifest is None or not any(
            m.run_id == run.id and m.job_id == job.id for m in manifest.members
        ):
            raise ProtocolRejected("ROOT_MANIFEST_BINDING_INVALID")
        return request.id, "independent"
    paper = await read_agent_paper_research_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if paper is not None:
        cycle = await session.get(AgentPaperCycleRecord, paper[0].cycle_id)
        evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
        if (
            cycle is None
            or run.actor_id != "local-agent-paper-cycle"
            or await revalidate_frozen_agent_paper_context(
                session, run=run, evidence_records=tuple(evidence)
            )
            != FrozenAgentPaperContextStatus.VALID
        ):
            raise ProtocolRejected("ROOT_MANIFEST_BINDING_INVALID")
        return cycle.id, "formal"
    raise ProtocolRejected("RESEARCH_PROTOCOL_OWNER_UNAVAILABLE")


async def ensure_routes(session):
    await ensure_schema(session)
    if await session.scalar(text("SELECT to_regclass('stage_proto_route')")) is not None:
        return
    statements = (
        "CREATE TABLE stage_proto_prepared_job (job_id uuid PRIMARY KEY REFERENCES jobs(id))",
        """CREATE FUNCTION stage_proto_birth_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF pg_trigger_depth()<>2 OR NOT EXISTS(SELECT 1 FROM jobs j
            WHERE j.id=NEW.job_id AND pg_xact_status(j.xmin::text::xid8)='in progress')
          THEN RAISE EXCEPTION 'birth receipt requires Job insert' USING ERRCODE='23514'; END IF;
          RETURN NEW;
        END $$""",
        "CREATE TRIGGER stage_proto_birth BEFORE INSERT ON stage_proto_prepared_job "
        "FOR EACH ROW EXECUTE FUNCTION stage_proto_birth_guard()",
        "CREATE TRIGGER stage_proto_prepared_job_immutable BEFORE UPDATE OR DELETE "
        "ON stage_proto_prepared_job "
        "FOR EACH ROW EXECUTE FUNCTION stage_proto_immutable()",
        """CREATE FUNCTION stage_proto_capture_job() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO stage_proto_prepared_job VALUES(NEW.id); RETURN NEW; END $$""",
        "CREATE TRIGGER stage_proto_capture_job AFTER INSERT ON jobs "
        "FOR EACH ROW EXECUTE FUNCTION stage_proto_capture_job()",
        """CREATE TABLE stage_proto_route (
          run_id uuid PRIMARY KEY REFERENCES agent_research_runs(id),
          job_id uuid UNIQUE NOT NULL REFERENCES jobs(id),
          scope_id uuid NOT NULL REFERENCES stage_proto_scope(id),
          kind text NOT NULL CHECK(kind IN ('formal','independent','increment')),
          version text NOT NULL CHECK(version='staged-experiment-v1')
        )""",
        "CREATE TRIGGER stage_proto_route_immutable BEFORE UPDATE OR DELETE ON stage_proto_route "
        "FOR EACH ROW EXECUTE FUNCTION stage_proto_immutable()",
        """CREATE FUNCTION stage_proto_route_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NOT EXISTS(SELECT 1 FROM jobs j JOIN agent_research_runs r
            ON j.payload->>'run_id'=r.id::text JOIN stage_proto_scope s ON s.id=NEW.scope_id
            JOIN agent_daily_budget_reservations b ON b.run_id=r.id
            WHERE j.id=NEW.job_id AND r.id=NEW.run_id AND j.status='queued'
            AND j.attempt_count=0 AND r.status='prepared' AND b.state='reserved'
            AND EXISTS(SELECT 1 FROM stage_proto_prepared_job p WHERE p.job_id=j.id
              AND pg_xact_status(p.xmin::text::xid8)='in progress')
            AND ((NEW.kind='formal' AND s.cycle_id=NEW.scope_id AND EXISTS(
              SELECT 1 FROM agent_paper_cycle_events e WHERE e.cycle_id=s.cycle_id
              AND e.fact_ids->'research_run_ids' @> to_jsonb(ARRAY[r.id::text])))
            OR (NEW.kind='independent' AND s.request_id=NEW.scope_id AND EXISTS(
              SELECT 1 FROM research_request_members m JOIN research_request_manifests f
              ON f.id=m.manifest_id JOIN manual_paper_requests q ON q.id=f.request_id
              WHERE f.request_id=s.request_id AND m.run_id=r.id AND m.job_id=j.id
              AND q.contract_version=3 AND q.mode='research_and_monitor'))
            OR (NEW.kind='increment' AND s.increment_id=NEW.scope_id AND EXISTS(
              SELECT 1 FROM research_monitor_increments i WHERE i.id=s.increment_id
              AND i.run_id=r.id AND i.job_id=j.id))))
          THEN RAISE EXCEPTION 'preparation route binding invalid' USING ERRCODE='23514'; END IF;
          RETURN NEW;
        END $$""",
        "CREATE TRIGGER stage_proto_route_binding BEFORE INSERT ON stage_proto_route "
        "FOR EACH ROW EXECUTE FUNCTION stage_proto_route_guard()",
        """CREATE FUNCTION stage_proto_claim_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.attempt_count>OLD.attempt_count AND EXISTS(
            SELECT 1 FROM stage_proto_route WHERE job_id=NEW.id) AND
            current_setting('nexus.stage_protocol',true) IS DISTINCT FROM 'staged-experiment-v1'
          THEN RAISE EXCEPTION 'staged worker required' USING ERRCODE='23514'; END IF;
          RETURN NEW;
        END $$""",
        "CREATE TRIGGER stage_proto_claim BEFORE UPDATE ON jobs "
        "FOR EACH ROW EXECUTE FUNCTION stage_proto_claim_guard()",
        """CREATE FUNCTION stage_proto_route_member_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS(SELECT 1 FROM stage_proto_scope s WHERE s.id=NEW.scope_id
            AND s.increment_id IS NULL) AND NOT EXISTS(
              SELECT 1 FROM stage_proto_route r JOIN stage_proto_scope s ON s.id=r.scope_id
              WHERE r.run_id=NEW.run_id AND r.job_id=NEW.job_id AND r.scope_id=NEW.scope_id
              AND r.version='staged-experiment-v1'
              AND r.kind=CASE WHEN s.cycle_id IS NOT NULL THEN 'formal' ELSE 'independent' END)
          THEN RAISE EXCEPTION 'staged root route required' USING ERRCODE='23514'; END IF;
          IF EXISTS(SELECT 1 FROM stage_proto_scope s WHERE s.id=NEW.scope_id
            AND s.increment_id IS NOT NULL) AND NOT EXISTS(
              SELECT 1 FROM stage_proto_route r WHERE r.run_id=NEW.run_id
              AND r.job_id=NEW.job_id AND r.scope_id=NEW.scope_id
              AND r.version='staged-experiment-v1' AND r.kind='increment')
          THEN RAISE EXCEPTION 'staged increment route required' USING ERRCODE='23514'; END IF;
          IF EXISTS(SELECT 1 FROM stage_proto_route WHERE run_id=NEW.run_id) AND NOT EXISTS(
            SELECT 1 FROM stage_proto_route r WHERE r.run_id=NEW.run_id
            AND r.job_id=NEW.job_id AND r.scope_id=NEW.scope_id)
          THEN RAISE EXCEPTION 'staged owner mismatch' USING ERRCODE='23514'; END IF;
          RETURN NEW;
        END $$""",
        "CREATE TRIGGER stage_proto_route_member BEFORE INSERT ON stage_proto_member "
        "FOR EACH ROW EXECUTE FUNCTION stage_proto_route_member_guard()",
        """CREATE FUNCTION stage_proto_root_authorize_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS(SELECT 1 FROM stage_proto_member m JOIN stage_proto_scope s ON s.id=m.scope_id
            WHERE m.run_id=NEW.run_id AND s.increment_id IS NULL) AND NOT EXISTS(
              SELECT 1 FROM stage_proto_member m JOIN stage_proto_scope s ON s.id=m.scope_id
              JOIN stage_proto_route r ON r.run_id=m.run_id AND r.job_id=m.job_id
              AND r.scope_id=m.scope_id WHERE m.run_id=NEW.run_id AND r.job_id=NEW.job_id
              AND r.version='staged-experiment-v1'
              AND r.kind=CASE WHEN s.cycle_id IS NOT NULL THEN 'formal' ELSE 'independent' END)
          THEN RAISE EXCEPTION 'staged root route required' USING ERRCODE='23514'; END IF;
          IF EXISTS(SELECT 1 FROM stage_proto_member m JOIN stage_proto_scope s ON s.id=m.scope_id
            WHERE m.run_id=NEW.run_id AND s.increment_id IS NOT NULL) AND NOT EXISTS(
              SELECT 1 FROM stage_proto_member m JOIN stage_proto_route r
              ON r.run_id=m.run_id AND r.job_id=m.job_id AND r.scope_id=m.scope_id
              WHERE m.run_id=NEW.run_id AND r.job_id=NEW.job_id
              AND r.version='staged-experiment-v1' AND r.kind='increment')
          THEN RAISE EXCEPTION 'staged increment route required' USING ERRCODE='23514'; END IF;
          RETURN NEW;
        END $$""",
        "CREATE TRIGGER stage_proto_root_authorize BEFORE INSERT ON stage_proto_authorization "
        "FOR EACH ROW EXECUTE FUNCTION stage_proto_root_authorize_guard()",
    )
    for statement in statements:
        await session.execute(text(statement))


async def register_prepared_runs(session, run_ids):
    """Called inside the original preparation transaction, before Jobs become visible."""
    await ensure_routes(session)
    for run_id in run_ids:
        run = await session.get(AgentResearchRunRecord, run_id)
        job = (
            await session.scalars(
                select(Job).where(Job.payload["run_id"].astext == str(run_id)).with_for_update()
            )
        ).one()
        existing = await selected_route(session, job.id)
        if existing is not None:
            # Exact immutable selection is read-only on replay, never a new send right.
            continue
        if not await formal_stage_job_qualified(session, job):
            raise ProtocolRejected("STAGED_PREPARATION_NOT_QUALIFIED")
        scope, kind = await owner(session, run, job)
        await session.execute(
            text(
                "INSERT INTO stage_proto_scope(id,cycle_id,increment_id,request_id) "
                "VALUES(:scope,:cycle,:inc,:request) ON CONFLICT(id) DO NOTHING"
            ),
            {
                "scope": scope,
                "cycle": scope if kind == "formal" else None,
                "inc": scope if kind == "increment" else None,
                "request": scope if kind == "independent" else None,
            },
        )
        await session.execute(
            text("INSERT INTO stage_proto_route VALUES(:run,:job,:scope,:kind,:version)"),
            {"run": run.id, "job": job.id, "scope": scope, "kind": kind, "version": VERSION},
        )


async def selected_route(session, job_id):
    if await session.scalar(text("SELECT to_regclass('stage_proto_route')")) is None:
        return None
    row = (
        (
            await session.execute(
                text("SELECT * FROM stage_proto_route WHERE job_id=:job"), {"job": job_id}
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    job = await session.get(Job, job_id)
    run = await session.get(AgentResearchRunRecord, UUID(job.payload["run_id"]))
    scope, kind = await owner(session, run, job)
    if (row["run_id"], row["scope_id"], row["kind"], row["version"]) != (
        run.id,
        scope,
        kind,
        VERSION,
    ):
        raise ProtocolRejected("STAGED_ROUTE_BINDING_INVALID")
    return row


def install_increment_preparation(monkeypatch):
    """Select only newly prepared increments in the original public transaction."""
    from nexus.monitoring import admission, mixed_admission

    def wrap(native):
        async def prepare(session, **kwargs):
            # Selection failure rolls back admission too, even if the caller
            # catches the exception and commits an unrelated diagnostic.
            async with session.begin_nested():
                await ensure_routes(session)
                member = await native(session, **kwargs)
                born_here = await session.scalar(
                    text(
                        "SELECT EXISTS(SELECT 1 FROM stage_proto_prepared_job WHERE job_id=:job "
                        "AND pg_xact_status(xmin::text::xid8)='in progress')"
                    ),
                    {"job": member.job_id},
                )
                route = await selected_route(session, member.job_id)
                if born_here or route is not None:
                    await register_prepared_runs(session, (member.run_id,))
                # Existing unselected identities remain legacy. Idempotent
                # public readback must not silently grant a protocol upgrade.
                return member

        return prepare

    monkeypatch.setattr(mixed_admission, "admit_batch", wrap(mixed_admission.admit_batch))
    monkeypatch.setattr(admission, "admit_trigger", wrap(admission.admit_trigger))
