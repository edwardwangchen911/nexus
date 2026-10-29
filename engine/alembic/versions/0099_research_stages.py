"""Persist the reviewed research-stage protocol without upgrading any historical identity.

Runtime selection remains explicit. Old Workers cannot claim selected Jobs.
Empty storage can be downgraded; any selected owner/fact requires a compatible
reader/Worker rollback, never destructive schema downgrade.
"""

from alembic import op

revision = "0099_research_stages"
down_revision = "0098_independent_monitor"
branch_labels = None
depends_on = None

DDL = (
    """CREATE TABLE research_stage_scope (
        id uuid PRIMARY KEY, stopped boolean NOT NULL DEFAULT false,
        cycle_id uuid REFERENCES agent_paper_cycles(id),
        increment_id uuid REFERENCES research_monitor_increments(id),
        request_id uuid REFERENCES manual_paper_requests(id),
        CHECK(num_nonnulls(cycle_id,increment_id,request_id)=1),
        CHECK(id=coalesce(cycle_id,increment_id,request_id))
    )""",
    """CREATE TABLE research_stage_member (
        run_id uuid PRIMARY KEY REFERENCES agent_research_runs(id),
        job_id uuid UNIQUE NOT NULL REFERENCES jobs(id),
        scope_id uuid NOT NULL REFERENCES research_stage_scope(id),
        work_deadline timestamptz NOT NULL, cleanup_deadline timestamptz NOT NULL,
        CHECK(work_deadline < cleanup_deadline)
    )""",
    """CREATE TABLE research_stage_authorization (
        run_id uuid REFERENCES research_stage_member(run_id), stage integer CHECK(stage
        BETWEEN 1 AND 3),
        job_id uuid NOT NULL REFERENCES jobs(id), attempt_no integer NOT NULL,
        worker_id text NOT NULL, lease_token uuid NOT NULL, fence_token bigint NOT NULL,
        reserved_id uuid NOT NULL REFERENCES agent_daily_budget_reservations(id),
        wire_hash text NOT NULL CHECK(wire_hash ~ '^[0-9a-f]{64}$'),
        schema_hash text NOT NULL, prior_hash text, authorized_at timestamptz NOT NULL,
        PRIMARY KEY(run_id,stage), FOREIGN KEY(job_id,attempt_no) REFERENCES
        job_attempts(job_id,attempt_no)
    )""",
    """CREATE TABLE research_stage_observation (
        run_id uuid, stage integer, response_hash text NOT NULL CHECK(response_hash ~
        '^[0-9a-f]{64}$'),
        input_tokens integer, output_tokens integer, valid boolean NOT NULL,
        observed_at timestamptz NOT NULL, eligible boolean NOT NULL, PRIMARY KEY(run_id,stage),
        FOREIGN KEY(run_id,stage) REFERENCES research_stage_authorization(run_id,stage),
        CHECK((input_tokens IS NULL AND output_tokens IS NULL) OR
              (input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND
        input_tokens>=0 AND output_tokens>=0))
    )""",
    """CREATE TABLE research_stage_verification (
        run_id uuid, stage integer, verified boolean NOT NULL,
        PRIMARY KEY(run_id,stage), FOREIGN KEY(run_id,stage) REFERENCES
        research_stage_observation(run_id,stage)
    )""",
    """CREATE TABLE research_stage_cleanup (
        run_id uuid, stage integer, confirmed boolean NOT NULL, observed_at timestamptz NOT NULL,
        PRIMARY KEY(run_id,stage), FOREIGN KEY(run_id,stage) REFERENCES
        research_stage_authorization(run_id,stage)
    )""",
    """CREATE TABLE research_stage_terminal (
        run_id uuid PRIMARY KEY REFERENCES research_stage_member(run_id), known boolean NOT NULL,
        chain_hash text NOT NULL, closed_at timestamptz NOT NULL,
        observed_stages integer[] NOT NULL, evidence jsonb NOT NULL
    )""",
    """CREATE FUNCTION research_stage_inflight(scope uuid) RETURNS bigint LANGUAGE sql STABLE AS $$
      SELECT count(*) FROM research_stage_member m WHERE m.scope_id=scope AND EXISTS(
        SELECT 1 FROM research_stage_authorization a WHERE a.run_id=m.run_id)
        AND (NOT EXISTS(SELECT 1 FROM research_stage_terminal t WHERE t.run_id=m.run_id)
          OR EXISTS(SELECT 1 FROM research_stage_authorization a WHERE a.run_id=m.run_id
            AND NOT EXISTS(SELECT 1 FROM research_stage_cleanup c
              WHERE c.run_id=a.run_id AND c.stage=a.stage AND c.confirmed)))
    $$""",
    """CREATE FUNCTION research_stage_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'stage fact immutable' USING ERRCODE='23514'; END $$""",
    """CREATE FUNCTION research_stage_authorize_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE member research_stage_member; stopped boolean;
    BEGIN
      SELECT * INTO STRICT member FROM research_stage_member WHERE run_id=NEW.run_id;
      SELECT s.stopped INTO stopped FROM research_stage_scope s WHERE s.id=member.scope_id FOR UPDATE;
      IF stopped OR NEW.authorized_at>=member.work_deadline OR EXISTS(
        SELECT 1 FROM research_stage_terminal WHERE run_id=NEW.run_id) THEN
        RAISE EXCEPTION 'stage stopped' USING ERRCODE='23514'; END IF;
      IF NEW.stage=1 AND research_stage_inflight(member.scope_id)>=2 THEN
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
        SELECT 1 FROM research_stage_observation o JOIN research_stage_cleanup c USING(run_id,stage)
        JOIN research_stage_verification v USING(run_id,stage)
        WHERE o.run_id=NEW.run_id AND o.stage=NEW.stage-1 AND o.valid
        AND o.eligible AND o.input_tokens IS NOT NULL AND c.confirmed AND v.verified
        AND o.response_hash=NEW.prior_hash
        AND o.observed_at<=NEW.authorized_at AND c.observed_at<=NEW.authorized_at
      ) THEN RAISE EXCEPTION 'predecessor invalid' USING ERRCODE='23514'; END IF;
      RETURN NEW;
    END $$""",
    """CREATE FUNCTION research_stage_stop_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP='DELETE' OR NEW.id<>OLD.id OR NEW.cycle_id IS DISTINCT FROM OLD.cycle_id
        OR NEW.increment_id IS DISTINCT FROM OLD.increment_id
        OR NEW.request_id IS DISTINCT FROM OLD.request_id
        OR (OLD.stopped AND NOT NEW.stopped) THEN
        RAISE EXCEPTION 'stop cannot be revoked' USING ERRCODE='23514'; END IF;
      RETURN NEW;
    END $$""",
    """CREATE FUNCTION research_stage_member_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE inc uuid;
    BEGIN
      SELECT increment_id INTO inc FROM research_stage_scope WHERE id=NEW.scope_id;
      IF inc IS NOT NULL AND NOT EXISTS(SELECT 1 FROM research_monitor_increments
        WHERE id=inc AND job_id=NEW.job_id AND run_id=NEW.run_id) THEN
        RAISE EXCEPTION 'increment owner invalid' USING ERRCODE='23514'; END IF;
      RETURN NEW;
    END $$""",
    """CREATE TRIGGER research_stage_member_binding BEFORE INSERT ON research_stage_member FOR EACH ROW EXECUTE FUNCTION research_stage_member_guard()""",
    """CREATE TRIGGER research_stage_stop BEFORE UPDATE OR DELETE ON research_stage_scope FOR EACH ROW EXECUTE FUNCTION research_stage_stop_guard()""",
    """CREATE TRIGGER research_stage_authorize BEFORE INSERT ON research_stage_authorization FOR EACH ROW EXECUTE FUNCTION research_stage_authorize_guard()""",
    """CREATE TRIGGER research_stage_member_immutable BEFORE UPDATE OR DELETE ON research_stage_member FOR EACH ROW EXECUTE FUNCTION research_stage_immutable()""",
    """CREATE TRIGGER research_stage_authorization_immutable BEFORE UPDATE OR DELETE ON research_stage_authorization FOR EACH ROW EXECUTE FUNCTION research_stage_immutable()""",
    """CREATE TRIGGER research_stage_observation_immutable BEFORE UPDATE OR DELETE ON research_stage_observation FOR EACH ROW EXECUTE FUNCTION research_stage_immutable()""",
    """CREATE TRIGGER research_stage_verification_immutable BEFORE UPDATE OR DELETE ON research_stage_verification FOR EACH ROW EXECUTE FUNCTION research_stage_immutable()""",
    """CREATE TRIGGER research_stage_cleanup_immutable BEFORE UPDATE OR DELETE ON research_stage_cleanup FOR EACH ROW EXECUTE FUNCTION research_stage_immutable()""",
    """CREATE TRIGGER research_stage_terminal_immutable BEFORE UPDATE OR DELETE ON research_stage_terminal FOR EACH ROW EXECUTE FUNCTION research_stage_immutable()""",
    """CREATE TABLE research_stage_prepared_job (job_id uuid PRIMARY KEY REFERENCES jobs(id))""",
    """CREATE FUNCTION research_stage_birth_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF pg_trigger_depth()<>2 OR NOT EXISTS(SELECT 1 FROM jobs j
            WHERE j.id=NEW.job_id AND pg_xact_status(j.xmin::text::xid8)='in progress')
          THEN RAISE EXCEPTION 'birth receipt requires Job insert' USING ERRCODE='23514'; END IF;
          RETURN NEW;
        END $$""",
    """CREATE TRIGGER research_stage_birth BEFORE INSERT ON research_stage_prepared_job FOR EACH ROW EXECUTE FUNCTION research_stage_birth_guard()""",
    """CREATE TRIGGER research_stage_prepared_job_immutable BEFORE UPDATE OR DELETE ON research_stage_prepared_job FOR EACH ROW EXECUTE FUNCTION research_stage_immutable()""",
    """CREATE FUNCTION research_stage_capture_job() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO research_stage_prepared_job VALUES(NEW.id); RETURN NEW; END $$""",
    """CREATE TRIGGER research_stage_capture_job AFTER INSERT ON jobs FOR EACH ROW EXECUTE FUNCTION research_stage_capture_job()""",
    """CREATE TABLE research_stage_route (
          run_id uuid PRIMARY KEY REFERENCES agent_research_runs(id),
          job_id uuid UNIQUE NOT NULL REFERENCES jobs(id),
          scope_id uuid NOT NULL REFERENCES research_stage_scope(id),
          kind text NOT NULL CHECK(kind IN ('formal','independent','increment')),
          version text NOT NULL CHECK(version='research-stages-v1')
        )""",
    """CREATE TRIGGER research_stage_route_immutable BEFORE UPDATE OR DELETE ON research_stage_route FOR EACH ROW EXECUTE FUNCTION research_stage_immutable()""",
    """CREATE FUNCTION research_stage_route_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NOT EXISTS(SELECT 1 FROM jobs j JOIN agent_research_runs r
            ON j.payload->>'run_id'=r.id::text JOIN research_stage_scope s ON s.id=NEW.scope_id
            JOIN agent_daily_budget_reservations b ON b.run_id=r.id
            WHERE j.id=NEW.job_id AND r.id=NEW.run_id AND j.status='queued'
            AND j.attempt_count=0 AND r.status='prepared' AND b.state='reserved'
            AND EXISTS(SELECT 1 FROM research_stage_prepared_job p WHERE p.job_id=j.id
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
    """CREATE TRIGGER research_stage_route_binding BEFORE INSERT ON research_stage_route FOR EACH ROW EXECUTE FUNCTION research_stage_route_guard()""",
    """CREATE FUNCTION research_stage_claim_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.attempt_count>OLD.attempt_count AND EXISTS(
            SELECT 1 FROM research_stage_route WHERE job_id=NEW.id) AND
            current_setting('nexus.stage_protocol',true) IS DISTINCT FROM 'research-stages-v1'
          THEN RAISE EXCEPTION 'staged worker required' USING ERRCODE='23514'; END IF;
          RETURN NEW;
        END $$""",
    """CREATE TRIGGER research_stage_claim BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION research_stage_claim_guard()""",
    """CREATE FUNCTION research_stage_route_member_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS(SELECT 1 FROM research_stage_scope s WHERE s.id=NEW.scope_id
            AND s.increment_id IS NULL) AND NOT EXISTS(
              SELECT 1 FROM research_stage_route r JOIN research_stage_scope s ON s.id=r.scope_id
              WHERE r.run_id=NEW.run_id AND r.job_id=NEW.job_id AND r.scope_id=NEW.scope_id
              AND r.version='research-stages-v1'
              AND r.kind=CASE WHEN s.cycle_id IS NOT NULL THEN 'formal' ELSE 'independent' END)
          THEN RAISE EXCEPTION 'staged root route required' USING ERRCODE='23514'; END IF;
          IF EXISTS(SELECT 1 FROM research_stage_scope s WHERE s.id=NEW.scope_id
            AND s.increment_id IS NOT NULL) AND NOT EXISTS(
              SELECT 1 FROM research_stage_route r WHERE r.run_id=NEW.run_id
              AND r.job_id=NEW.job_id AND r.scope_id=NEW.scope_id
              AND r.version='research-stages-v1' AND r.kind='increment')
          THEN RAISE EXCEPTION 'staged increment route required' USING ERRCODE='23514'; END IF;
          IF EXISTS(SELECT 1 FROM research_stage_route WHERE run_id=NEW.run_id) AND NOT EXISTS(
            SELECT 1 FROM research_stage_route r WHERE r.run_id=NEW.run_id
            AND r.job_id=NEW.job_id AND r.scope_id=NEW.scope_id)
          THEN RAISE EXCEPTION 'staged owner mismatch' USING ERRCODE='23514'; END IF;
          RETURN NEW;
        END $$""",
    """CREATE TRIGGER research_stage_route_member BEFORE INSERT ON research_stage_member FOR EACH ROW EXECUTE FUNCTION research_stage_route_member_guard()""",
    """CREATE FUNCTION research_stage_root_authorize_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF EXISTS(SELECT 1 FROM research_stage_member m JOIN research_stage_scope s ON s.id=m.scope_id
            WHERE m.run_id=NEW.run_id AND s.increment_id IS NULL) AND NOT EXISTS(
              SELECT 1 FROM research_stage_member m JOIN research_stage_scope s ON s.id=m.scope_id
              JOIN research_stage_route r ON r.run_id=m.run_id AND r.job_id=m.job_id
              AND r.scope_id=m.scope_id WHERE m.run_id=NEW.run_id AND r.job_id=NEW.job_id
              AND r.version='research-stages-v1'
              AND r.kind=CASE WHEN s.cycle_id IS NOT NULL THEN 'formal' ELSE 'independent' END)
          THEN RAISE EXCEPTION 'staged root route required' USING ERRCODE='23514'; END IF;
          IF EXISTS(SELECT 1 FROM research_stage_member m JOIN research_stage_scope s ON s.id=m.scope_id
            WHERE m.run_id=NEW.run_id AND s.increment_id IS NOT NULL) AND NOT EXISTS(
              SELECT 1 FROM research_stage_member m JOIN research_stage_route r
              ON r.run_id=m.run_id AND r.job_id=m.job_id AND r.scope_id=m.scope_id
              WHERE m.run_id=NEW.run_id AND r.job_id=NEW.job_id
              AND r.version='research-stages-v1' AND r.kind='increment')
          THEN RAISE EXCEPTION 'staged increment route required' USING ERRCODE='23514'; END IF;
          RETURN NEW;
        END $$""",
    """CREATE TRIGGER research_stage_root_authorize BEFORE INSERT ON research_stage_authorization FOR EACH ROW EXECUTE FUNCTION research_stage_root_authorize_guard()""",
)


def upgrade() -> None:
    for statement in DDL:
        op.execute(statement)


def downgrade() -> None:
    # Existing stage work locks scope before Job. Refuse nonempty storage
    # before touching Jobs, and recheck after waiting for a preparing Job.
    # With Jobs fenced, no new route can commit; NOWAIT also refuses any
    # remaining stage reader/writer rather than creating the reverse wait.
    reject = "DO $$ BEGIN IF EXISTS(SELECT 1 FROM research_stage_scope) OR EXISTS(SELECT 1 FROM research_stage_route) OR EXISTS(SELECT 1 FROM research_stage_member) THEN RAISE EXCEPTION 'RESEARCH_STAGE_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    op.execute(reject)
    op.execute("LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE")
    op.execute(reject)
    op.execute(
        "LOCK TABLE research_stage_scope, research_stage_route, research_stage_member IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    op.execute(reject)
    op.execute("DROP TRIGGER research_stage_claim ON jobs")
    op.execute("DROP TRIGGER research_stage_capture_job ON jobs")
    op.execute("DROP FUNCTION research_stage_inflight(uuid)")
    for table in (
        "prepared_job",
        "route",
        "terminal",
        "cleanup",
        "verification",
        "observation",
        "authorization",
        "member",
        "scope",
    ):
        op.execute(f"DROP TABLE research_stage_{table}")
    for function in (
        "birth_guard",
        "root_authorize_guard",
        "capture_job",
        "route_guard",
        "claim_guard",
        "route_member_guard",
        "authorize_guard",
        "member_guard",
        "stop_guard",
        "immutable",
    ):
        op.execute(f"DROP FUNCTION research_stage_{function}()")
