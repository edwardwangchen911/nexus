"""Pin a single research owner's native reservation and its same-day obligations."""

from alembic import op

revision = "0115_single_sol_budget"
down_revision = "0114_formal_intraday_intent"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE single_sol_budget_scopes (
            budget_day date PRIMARY KEY,
            source_run_id uuid NOT NULL UNIQUE REFERENCES agent_daily_budget_reservations(run_id),
            account_id uuid NOT NULL REFERENCES paper_accounts(id),
            batch_id uuid NOT NULL REFERENCES agent_paper_candidate_batches(id),
            model_id uuid NOT NULL REFERENCES agent_model_configs(id),
            selected_id uuid NOT NULL REFERENCES instruments(id),
            calendar_id uuid NOT NULL REFERENCES exchange_calendar_authorities(id),
            slot timestamptz NOT NULL, created_at timestamptz NOT NULL,
            daily_limit numeric NOT NULL CHECK(daily_limit>0),
            maximum numeric NOT NULL CHECK(maximum>0 AND maximum<=daily_limit),
            CHECK ((slot AT TIME ZONE 'UTC')::date=budget_day AND
                   (created_at AT TIME ZONE 'UTC')::date=budget_day AND created_at<slot)
        )
    """)
    op.execute("""
        CREATE TABLE single_sol_budget_credits (
            budget_day date NOT NULL REFERENCES single_sol_budget_scopes(budget_day),
            task text NOT NULL, run_id uuid NOT NULL UNIQUE, PRIMARY KEY(budget_day,task)
        )
    """)
    op.execute("""
        CREATE FUNCTION single_sol_budget_immutable() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
        BEGIN RAISE EXCEPTION 'SINGLE_SOL_BUDGET_IMMUTABLE'; END $$
    """)
    for table in ("single_sol_budget_scopes", "single_sol_budget_credits"):
        op.execute(
            f"CREATE TRIGGER immutable BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION single_sol_budget_immutable()"
        )

    # Reuse the same predicate for admission and for an obligation already funded
    # earlier in this UTC day. The immutable snapshot identifies the actual parent.
    op.execute("""
        CREATE FUNCTION single_sol_increment_valid(
            d date, account uuid, batch uuid, model uuid, cal uuid, maximum numeric, candidate uuid
        ) RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,public AS $$
          SELECT EXISTS (
            SELECT FROM research_monitor_increments i
            JOIN agent_research_runs r ON r.id=i.run_id
            JOIN agent_model_configs m ON m.id=r.model_config_id
            JOIN agent_daily_budget_reservations b ON b.run_id=r.id
            JOIN research_stage_route rt ON rt.run_id=r.id AND rt.job_id=i.job_id
            JOIN agent_paper_candidate_batches bs ON bs.id=batch AND bs.account_id=i.account_id
            JOIN data_snapshots src ON src.id=i.snapshot_id AND src.id=r.data_snapshot_id
            JOIN research_monitor_triggers t ON t.id=i.trigger_id AND t.research_run_id=r.id
            JOIN research_monitor_checks k ON k.id=t.check_id AND k.plan_id=i.plan_id
            JOIN research_monitor_plans p ON p.id=i.plan_id
            WHERE i.run_id=candidate AND i.account_id=account AND i.session_date=d
            AND i.research_as_of=r.decision_time AND i.instrument_id=r.instrument_id
            AND r.instrument_id=ANY(bs.candidate_instrument_ids)
            AND r.model_config_id=model AND r.model_hash=m.config_hash AND r.pricing_version=m.version
            AND b.business_date=d AND b.pricing_version=m.version AND b.reserved_usd=maximum
            AND rt.kind='increment' AND rt.scope_id=i.id AND rt.platform_binding IS NOT NULL
            AND t.account_id=i.account_id AND t.instrument_id=i.instrument_id
            AND t.batch_id IS NOT DISTINCT FROM i.batch_id AND k.result='true'
            AND p.account_id=i.account_id AND p.instrument_id=i.instrument_id
            AND k.checked_at>=p.effective_at AND k.checked_at<p.valid_until
            AND k.checked_at<=i.research_as_of
            AND src.payload#>>'{monitor_context,parent_run_id}'=p.source_run_id::text
            AND src.payload#>>'{monitor_context,parent_output_id}'=p.source_output_id::text
            AND src.payload#>>'{monitor_context,authority,calendar_authority_id}'=cal::text
            AND r.actor_id='monitor:' || i.account_id::text
            AND r.idempotency_key=CASE WHEN i.batch_id IS NULL THEN 'monitor:' || i.trigger_id::text
                                  ELSE 'monitor-batch:' || i.batch_id::text END)
        $$
    """)
    # Budget admission precedes final route installation inside the same transaction.
    # A provisional match only commits when the normal native owner and reservation exist.
    op.execute("""
        CREATE FUNCTION single_sol_credit_valid() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
        DECLARE g single_sol_budget_scopes; c uuid; members uuid[];
        BEGIN
          SELECT * INTO STRICT g FROM single_sol_budget_scopes WHERE budget_day=NEW.budget_day;
          SELECT candidate_instrument_ids INTO STRICT members FROM agent_paper_candidate_batches
            WHERE id=g.batch_id AND account_id=g.account_id;
          IF NEW.task='increment' AND single_sol_increment_valid(
              g.budget_day,g.account_id,g.batch_id,g.model_id,g.calendar_id,g.maximum,NEW.run_id)
          THEN RETURN NEW; END IF;
          SELECT cy.id INTO c FROM agent_paper_cycles cy JOIN idempotency_records claim
            ON claim.actor_id='local-agent-paper-schwab:' || g.account_id::text
            AND claim.route='agent-paper-schwab-trigger-v2'
            AND claim.schema_version='agent_paper_schwab_trigger_v2'
            AND claim.idempotency_key=to_char(g.slot AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS') || '+00:00'
            AND claim.formal_budget_binding_id IS NULL
            AND (claim.response_payload='{}'::jsonb OR (claim.response_payload->>'outcome'='completed'
                 AND claim.response_payload->'result'->>'cycle_id'=cy.id::text))
            WHERE cy.account_id=g.account_id AND cy.candidate_batch_id=g.batch_id
            AND cy.decision_slot_at=g.slot AND cy.decision_session=g.budget_day AND cy.execution_locked AND cy.formal_budget_binding_id IS NULL
            AND cy.decision_time>=g.slot AND cy.decision_time<g.slot+interval '5 minutes'
            AND NOT EXISTS(SELECT 1 FROM manual_paper_request_events e WHERE e.cycle_id=cy.id);
          IF NEW.task='synthesis' AND EXISTS (
            SELECT FROM agent_portfolio_synthesis_runs s
            JOIN agent_portfolio_synthesis_budget_reservations b ON b.run_id=s.id
            JOIN agent_model_configs m ON m.id=g.model_id
            WHERE s.id=NEW.run_id AND s.cycle_id=c AND s.candidate_batch_id=g.batch_id
            AND s.frozen_input->>'calendar_authority_id'=g.calendar_id::text
            AND s.model_config_id=m.id AND s.model_hash=m.config_hash
            AND s.idempotency_key='portfolio-synthesis:' || c::text AND s.pricing_version=m.version
            AND b.business_date=g.budget_day AND b.pricing_version=m.version AND b.reserved_usd=g.maximum)
          THEN RETURN NEW; END IF;
          IF EXISTS (
            SELECT FROM agent_research_runs r JOIN agent_daily_budget_reservations b ON b.run_id=r.id
            JOIN research_stage_route route ON route.run_id=r.id
            JOIN data_snapshots src ON src.id=r.data_snapshot_id
            JOIN agent_model_configs m ON m.id=g.model_id
            WHERE r.id=NEW.run_id AND r.instrument_id=ANY(members)
            AND r.model_config_id=m.id AND r.model_hash=m.config_hash AND r.pricing_version=m.version
            AND b.business_date=g.budget_day AND b.pricing_version=m.version AND b.reserved_usd=g.maximum
            AND route.platform_binding IS NOT NULL AND route.kind='formal' AND route.scope_id=c
            AND src.payload#>>'{agent_paper_context,cycle_id}'=c::text
            AND src.payload#>>'{agent_paper_context,instrument_id}'=r.instrument_id::text
            AND NEW.task='research:' || r.instrument_id::text AND r.actor_id='local-agent-paper-cycle'
            AND r.idempotency_key='agent-paper:' || c::text || ':' || r.instrument_id::text)
          THEN RETURN NEW; END IF;
          RAISE EXCEPTION 'SINGLE_SOL_BUDGET_CREDIT_UNPROVEN';
        END $$
    """)
    op.execute("""
        CREATE CONSTRAINT TRIGGER single_sol_credit_complete AFTER INSERT ON single_sol_budget_credits
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION single_sol_credit_valid()
    """)


def downgrade() -> None:
    op.execute("""DO $$ BEGIN IF EXISTS(SELECT FROM single_sol_budget_scopes) THEN
        RAISE EXCEPTION 'SINGLE_SOL_BUDGET_DOWNGRADE_FORBIDDEN'; END IF; END $$""")
    op.execute("DROP TABLE single_sol_budget_credits, single_sol_budget_scopes")
    op.execute("DROP FUNCTION single_sol_credit_valid(), single_sol_budget_immutable()")
    op.execute("DROP FUNCTION single_sol_increment_valid(date,uuid,uuid,uuid,uuid,numeric,uuid)")
