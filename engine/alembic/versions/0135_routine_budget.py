"""Protect ordinary native obligations without borrowing any one-off root."""

from alembic import op
from sqlalchemy import text

revision = "0135_routine_budget"
down_revision = "0134_sec_foundation"
branch_labels = None
depends_on = None


def _definition(signature: str) -> str:
    value = op.get_bind().scalar(
        text("SELECT pg_get_functiondef(to_regprocedure(:s))"), {"s": "public." + signature}
    )
    if not isinstance(value, str):
        raise RuntimeError("ROUTINE_BUDGET_PREDECESSOR_INVALID")
    return value


def upgrade() -> None:
    op.execute("""CREATE TABLE routine_budget_scopes (
      id uuid PRIMARY KEY, budget_day date NOT NULL, sequence integer NOT NULL CHECK(sequence>0),
      previous_id uuid REFERENCES routine_budget_scopes(id),
      account_id uuid NOT NULL REFERENCES paper_accounts(id),
      batch_id uuid NOT NULL REFERENCES agent_paper_candidate_batches(id),
      policy_id uuid NOT NULL REFERENCES data_snapshots(id),
      model_id uuid NOT NULL REFERENCES agent_model_configs(id),
      calendar_id uuid NOT NULL REFERENCES exchange_calendar_authorities(id),
      slot timestamptz NOT NULL, created_at timestamptz NOT NULL,
      day_is_session boolean NOT NULL, daily_limit numeric NOT NULL CHECK(daily_limit=13),
      maximum numeric NOT NULL CHECK(maximum>0 AND maximum<=daily_limit),
      UNIQUE(budget_day,sequence), UNIQUE(id,budget_day),
      CHECK((created_at AT TIME ZONE 'UTC')::date=budget_day),
      CHECK((slot AT TIME ZONE 'UTC')::date=budget_day))""")
    op.execute("""CREATE TABLE routine_budget_credits (
      budget_day date NOT NULL, scope_id uuid NOT NULL, task text NOT NULL,
      run_id uuid NOT NULL UNIQUE, maximum numeric NOT NULL CHECK(maximum>0),
      PRIMARY KEY(budget_day,task),
      FOREIGN KEY(scope_id,budget_day) REFERENCES routine_budget_scopes(id,budget_day))""")
    op.execute("""CREATE FUNCTION routine_budget_immutable() RETURNS trigger LANGUAGE plpgsql
      SET search_path=pg_catalog,public AS $$ BEGIN RAISE EXCEPTION 'ROUTINE_BUDGET_IMMUTABLE'; END $$""")
    for table in ("routine_budget_scopes", "routine_budget_credits"):
        op.execute(
            f"CREATE TRIGGER immutable BEFORE UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION routine_budget_immutable()"
        )
    op.execute("""CREATE FUNCTION routine_budget_scope_guard() RETURNS trigger LANGUAGE plpgsql
      SET search_path=pg_catalog,public AS $$
      DECLARE p routine_budget_scopes; b agent_paper_candidate_batches; m agent_model_configs;
      pol data_snapshots; cal exchange_calendar_authorities; latest uuid;
      BEGIN
       PERFORM pg_advisory_xact_lock(hashtext('agent-budget:accounting'));
       IF NOT pg_try_advisory_xact_lock(hashtext('agent-budget:'||NEW.budget_day::text)) THEN
        RAISE EXCEPTION 'RESEARCH_ACCOUNTING_BUSY'; END IF;
       IF current_setting('transaction_isolation')<>'read committed'
        OR NEW.budget_day IS DISTINCT FROM (clock_timestamp() AT TIME ZONE 'UTC')::date
        OR NEW.created_at>clock_timestamp() OR NEW.created_at<clock_timestamp()-interval '5 minutes'
        OR NEW.slot IS DISTINCT FROM ((NEW.budget_day+time '10:00') AT TIME ZONE 'America/New_York')
        OR EXISTS(SELECT FROM single_sol_budget_scopes WHERE budget_day=NEW.budget_day)
        OR EXISTS(SELECT FROM formal_budget_bindings WHERE business_date=NEW.budget_day)
       THEN RAISE EXCEPTION 'ROUTINE_BUDGET_SCOPE_INVALID'; END IF;
       SELECT * INTO b FROM agent_paper_candidate_batches WHERE id=NEW.batch_id;
       SELECT * INTO pol FROM data_snapshots WHERE id=NEW.policy_id;
       SELECT * INTO m FROM agent_model_configs WHERE id=NEW.model_id;
       SELECT * INTO cal FROM exchange_calendar_authorities WHERE id=NEW.calendar_id;
       SELECT id INTO latest FROM routine_budget_scopes WHERE budget_day=NEW.budget_day ORDER BY sequence DESC LIMIT 1;
       SELECT * INTO p FROM routine_budget_scopes WHERE id=latest;
       IF b.id IS NULL OR b.account_id IS DISTINCT FROM NEW.account_id OR cardinality(b.candidate_instrument_ids)<>3
        OR NOT coalesce(sec_auto_increment_scoped(NEW.account_id,b.candidate_instrument_ids[1]),false)
        OR pol.source IS DISTINCT FROM 'nexus_auto_increment_policy'
        OR pol.provider_object_id IS DISTINCT FROM NEW.account_id::text
        OR pol.payload->>'candidate_batch_id' IS DISTINCT FROM b.id::text
        OR pol.payload->>'candidate_hash' IS DISTINCT FROM b.batch_hash
        OR m.id IS NULL OR NOT coalesce(nexus_sol_model_profile_valid(m),false)
        OR NEW.maximum IS DISTINCT FROM (m.max_input_tokens*m.input_usd_per_million+m.max_output_tokens*m.output_usd_per_million)/1000000
        OR NOT(m.pricing_available_at<=NEW.created_at AND m.pricing_expires_at>NEW.created_at)
        OR greatest(m.created_at,b.created_at,pol.created_at,pol.available_at,pol.received_at,pol.observed_at)>NEW.created_at
        OR cal.id IS NULL OR cal.exchange<>'XNYS' OR cal.completeness_status<>'complete'
        OR cal.range_start>NEW.budget_day OR cal.range_end<NEW.budget_day
        OR greatest(cal.created_at,cal.published_at)>NEW.created_at
        OR NEW.day_is_session IS DISTINCT FROM EXISTS(SELECT FROM exchange_calendar_sessions cs WHERE cs.calendar_authority_id=cal.id AND cs.session=NEW.budget_day AND cs.created_at<=NEW.created_at AND cs.market_open_at<=NEW.slot AND cs.market_close_at>NEW.slot)
        OR (p.id IS NULL AND (NEW.previous_id IS NOT NULL OR NEW.sequence<>1))
        OR (p.id IS NOT NULL AND m.pricing_available_at<(SELECT pricing_available_at FROM agent_model_configs WHERE id=p.model_id))
        OR (p.id IS NOT NULL AND m.pricing_available_at=(SELECT pricing_available_at FROM agent_model_configs WHERE id=p.model_id)
         AND m.platform_profile->'price_record' IS DISTINCT FROM (SELECT platform_profile->'price_record' FROM agent_model_configs WHERE id=p.model_id))
        OR (p.id IS NOT NULL AND (NEW.previous_id IS DISTINCT FROM p.id OR NEW.sequence<>p.sequence+1
          OR (NEW.account_id,NEW.batch_id,NEW.policy_id,NEW.calendar_id,NEW.slot,NEW.day_is_session,NEW.daily_limit)
          IS DISTINCT FROM (p.account_id,p.batch_id,p.policy_id,p.calendar_id,p.slot,p.day_is_session,p.daily_limit)))
       THEN RAISE EXCEPTION 'ROUTINE_BUDGET_SCOPE_INVALID'; END IF;
       RETURN NEW;
      END $$""")
    op.execute(
        "CREATE TRIGGER routine_scope_guard BEFORE INSERT ON routine_budget_scopes FOR EACH ROW EXECUTE FUNCTION routine_budget_scope_guard()"
    )
    # Keep the complete existing native owner proof; only the funding relation is new.
    inc = _definition("single_sol_increment_valid(date,uuid,uuid,uuid,uuid,numeric,uuid)")
    if inc.count("AND i.session_date=d") != 1:
        raise RuntimeError("ROUTINE_BUDGET_PREDECESSOR_INVALID")
    inc = inc.replace("single_sol_increment_valid", "routine_budget_increment_valid").replace(
        "AND i.session_date=d", ""
    )
    op.execute(inc)
    credit = _definition("single_sol_credit_valid()")
    body = credit[credit.index("DECLARE g ") :]
    body = body.replace(
        "g single_sol_budget_scopes;",
        "g routine_budget_scopes; owner_model agent_model_configs; owner_at timestamptz;",
    )
    body = body.replace(
        "SELECT * INTO STRICT g FROM single_sol_budget_scopes WHERE budget_day=NEW.budget_day;",
        """SELECT * INTO g FROM routine_budget_scopes WHERE id=owner_scope;
          IF g.id IS NULL OR NOT g.day_is_session THEN RETURN false; END IF;
          IF owner_task='synthesis' THEN
           SELECT m.* INTO owner_model FROM agent_portfolio_synthesis_runs s JOIN agent_model_configs m ON m.id=s.model_config_id WHERE s.id=owner_run;
           SELECT decision_time INTO owner_at FROM agent_portfolio_synthesis_runs WHERE id=owner_run;
          ELSE
           SELECT m.* INTO owner_model FROM agent_research_runs r JOIN agent_model_configs m ON m.id=r.model_config_id WHERE r.id=owner_run;
           SELECT decision_time INTO owner_at FROM agent_research_runs WHERE id=owner_run;
          END IF;
          IF owner_model.id IS NULL OR NOT coalesce(nexus_sol_model_profile_valid(owner_model),false)
           OR owner_maximum IS DISTINCT FROM (owner_model.max_input_tokens*owner_model.input_usd_per_million+owner_model.max_output_tokens*owner_model.output_usd_per_million)/1000000
           OR NOT(owner_model.pricing_available_at<=owner_at AND owner_at<owner_model.pricing_expires_at)
           OR (owner_at AT TIME ZONE 'UTC')::date IS DISTINCT FROM g.budget_day
          THEN RETURN false; END IF;
          g.model_id:=owner_model.id; g.maximum:=owner_maximum;""",
    )
    body = (
        body.replace("NEW.budget_day", "g.budget_day")
        .replace("NEW.task", "owner_task")
        .replace("NEW.run_id", "owner_run")
    )
    body = (
        body.replace("single_sol_increment_valid", "routine_budget_increment_valid")
        .replace("RETURN NEW;", "RETURN true;")
        .replace("RAISE EXCEPTION 'SINGLE_SOL_BUDGET_CREDIT_UNPROVEN';", "RETURN false;")
    )
    # pg_get_functiondef retains its dollar delimiter and trailing newline.
    tail = body.rfind("$function$")
    if tail < 0:
        raise RuntimeError("ROUTINE_BUDGET_PREDECESSOR_INVALID")
    body = body[:tail]
    op.execute(
        "CREATE FUNCTION routine_budget_owner_valid(owner_scope uuid,owner_task text,owner_run uuid,owner_maximum numeric) RETURNS boolean LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $function$\n"
        + body
        + "$function$"
    )
    op.execute("""CREATE FUNCTION routine_budget_complete() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
      BEGIN
       IF TG_TABLE_NAME='routine_budget_credits' THEN
        IF NOT coalesce(routine_budget_owner_valid(NEW.scope_id,NEW.task,NEW.run_id,NEW.maximum),false)
          OR NOT EXISTS(SELECT FROM routine_budget_scopes g WHERE g.id=NEW.scope_id AND g.budget_day=NEW.budget_day)
        THEN RAISE EXCEPTION 'ROUTINE_BUDGET_CREDIT_UNPROVEN'; END IF;
       ELSE
        IF NOT EXISTS(SELECT FROM audit_events a WHERE a.actor_id='research-monitor' AND a.action='research.routine_budget.scope'
          AND a.schema_version='routine_budget_scope_v1' AND a.occurred_at=NEW.created_at
          AND a.payload=jsonb_build_object('binding',(to_jsonb(NEW)-'maximum'-'daily_limit')||jsonb_build_object('maximum',NEW.maximum::text,'daily_limit',NEW.daily_limit::text)))
        THEN RAISE EXCEPTION 'ROUTINE_BUDGET_AUDIT_INVALID'; END IF;
       END IF;
       RETURN NEW;
      END $$""")
    for table in ("routine_budget_scopes", "routine_budget_credits"):
        op.execute(
            f"CREATE CONSTRAINT TRIGGER routine_complete AFTER INSERT ON {table} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION routine_budget_complete()"
        )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE routine_budget_scopes,routine_budget_credits IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT FROM routine_budget_scopes) THEN RAISE EXCEPTION 'ROUTINE_BUDGET_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    op.execute("DROP TABLE routine_budget_credits,routine_budget_scopes")
    op.execute(
        "DROP FUNCTION routine_budget_complete(),routine_budget_scope_guard(),routine_budget_immutable(),routine_budget_owner_valid(uuid,text,uuid,numeric),routine_budget_increment_valid(date,uuid,uuid,uuid,uuid,numeric,uuid)"
    )
