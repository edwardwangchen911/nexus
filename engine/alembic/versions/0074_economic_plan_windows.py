"""Bind post-sale plan windows without changing root or prior residual facts."""

from alembic import op

revision = "0074_economic_plan_windows"
down_revision = "0073_portfolio_plan_continuation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP TRIGGER economic_window_guard ON economic_common_windows")
    op.execute(
        "CREATE TRIGGER economic_window_guard BEFORE INSERT ON economic_common_windows FOR EACH ROW WHEN (NEW.window_seq>1 AND NEW.qualification->>'plan_stage' IS NULL) EXECUTE FUNCTION nexus_economic_window_guard()"
    )
    op.execute(_PLAN_GUARD)
    op.execute(
        "CREATE TRIGGER economic_plan_window_guard BEFORE INSERT ON economic_common_windows FOR EACH ROW WHEN (NEW.window_seq>1 AND NEW.qualification->>'plan_stage' IS NOT NULL) EXECUTE FUNCTION nexus_economic_plan_window_guard()"
    )


def downgrade() -> None:
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT 1 FROM economic_common_windows WHERE qualification->>'plan_stage' IS NOT NULL) THEN RAISE EXCEPTION 'ECONOMIC_WINDOW_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    op.execute("DROP TRIGGER economic_plan_window_guard ON economic_common_windows")
    op.execute("DROP FUNCTION nexus_economic_plan_window_guard()")
    op.execute("DROP TRIGGER economic_window_guard ON economic_common_windows")
    op.execute(
        "CREATE TRIGGER economic_window_guard BEFORE INSERT ON economic_common_windows FOR EACH ROW WHEN (NEW.window_seq>1) EXECUTE FUNCTION nexus_economic_window_guard()"
    )


_PLAN_GUARD = """CREATE FUNCTION nexus_economic_plan_window_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE previous economic_common_windows; completion audit_events; root economic_common_windows; plan audit_events; plan_source economic_common_windows; expected_orders jsonb;
    BEGIN
      SELECT * INTO previous FROM economic_common_windows WHERE pair_slot_id=NEW.pair_slot_id AND window_seq=NEW.window_seq-1;
      SELECT * INTO root FROM economic_common_windows WHERE pair_slot_id=NEW.pair_slot_id AND window_seq=1;
      SELECT * INTO completion FROM audit_events WHERE id=(NEW.qualification->>'completion_id')::uuid;
      SELECT * INTO plan FROM audit_events WHERE id=(NEW.qualification->>'plan_event_id')::uuid;
      SELECT * INTO plan_source FROM economic_common_windows WHERE id=(plan.payload->>'source_window_id')::uuid;
      SELECT jsonb_agg(v.value ORDER BY v.value #>> '{}') INTO expected_orders
        FROM jsonb_array_elements(plan.payload->'result'->'lanes') lane,
             jsonb_array_elements(lane->'order_ids') v WHERE lane->>'status'='created';
      IF NEW.qualification->>'plan_stage' IS DISTINCT FROM 'post_sale_v1'
        OR plan.id IS NULL OR plan.action IS DISTINCT FROM 'economic.plan.continuation_finished'
        OR plan.actor_id IS DISTINCT FROM 'local-economic-cycle'
        OR plan.schema_version IS DISTINCT FROM 'economic_plan_continuation_v1'
        OR plan.payload->>'pair_slot_id' IS DISTINCT FROM NEW.pair_slot_id::text
        OR plan.payload->'result'->>'status' IS DISTINCT FROM 'plans_continued'
        OR plan.payload->'result'->>'execution_locked' IS DISTINCT FROM 'true'
        OR NEW.qualification->>'plan_event_hash' IS DISTINCT FROM plan.event_hash
        OR plan_source.pair_slot_id IS DISTINCT FROM NEW.pair_slot_id
        OR plan_source.content_hash IS DISTINCT FROM plan.payload->>'source_window_hash'
        OR plan_source.qualification->>'plan_stage' IS NOT NULL
        OR (previous.qualification->>'plan_stage' IS NULL AND plan_source.id IS DISTINCT FROM previous.id)
        OR (previous.qualification->>'plan_stage' IS NOT NULL AND previous.qualification->>'plan_event_id' IS DISTINCT FROM plan.id::text)
        OR NEW.eligible_at<greatest(plan.created_at,plan.occurred_at)
        OR jsonb_array_length(plan.payload->'result'->'lanes') IS DISTINCT FROM 2
        OR expected_orders IS NULL OR jsonb_array_length(NEW.qualification->'orders')=0
        OR EXISTS(SELECT 1 FROM jsonb_array_elements(plan.payload->'result'->'lanes') a
          LEFT JOIN economic_lane_cycles l ON l.pair_slot_id=NEW.pair_slot_id AND l.lane=a->>'lane'
          LEFT JOIN strategy_decisions d ON d.agent_paper_cycle_id=l.cycle_id AND d.account_id=l.account_id
          LEFT JOIN paper_order_batches p ON p.strategy_decision_id=d.id AND p.previous_batch_id IS NULL
          LEFT JOIN paper_order_batches b ON b.id=(a->>'batch_id')::uuid
          WHERE l.id IS NULL
            OR (a->>'status'='created' AND (b.id IS NULL OR b.previous_batch_id IS DISTINCT FROM p.id
                OR b.account_id IS DISTINCT FROM l.account_id OR b.strategy_decision_id IS DISTINCT FROM d.id
                OR b.target_version_id IS DISTINCT FROM p.target_version_id
                OR plan.occurred_at<greatest(b.created_at,b.evaluated_at)
                OR (SELECT jsonb_agg(o.id::text ORDER BY o.id::text) FROM paper_orders o WHERE o.batch_id=b.id)
                   IS DISTINCT FROM (SELECT jsonb_agg(x.value ORDER BY x.value #>> '{}') FROM jsonb_array_elements(a->'order_ids') x)))
            OR (a->>'status'='not_required' AND ((a->>'batch_id') IS NOT NULL
                OR EXISTS(SELECT 1 FROM paper_order_batches child WHERE child.previous_batch_id=p.id)))
            OR a->>'status' IS NULL OR a->>'status' NOT IN ('created','not_required')
            OR (a->>'status'='created' AND jsonb_typeof(a->'order_ids') IS DISTINCT FROM 'array'))
        OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(plan.payload->'result'->'lanes') a WHERE a->>'lane'='incumbent')
        OR NOT EXISTS(SELECT 1 FROM jsonb_array_elements(plan.payload->'result'->'lanes') a WHERE a->>'lane'='challenger')
        OR previous.id IS NULL OR previous.status<>'frozen' OR root.id IS NULL
        OR NEW.qualification->>'parent_id' IS DISTINCT FROM previous.id::text
        OR NEW.qualification->>'parent_hash' IS DISTINCT FROM previous.content_hash
        OR completion.action IS DISTINCT FROM 'economic.window.execution_finished'
        OR completion.payload->>'window_id' IS DISTINCT FROM previous.id::text
        OR completion.payload->>'window_hash' IS DISTINCT FROM previous.content_hash
        OR NEW.qualification->>'completion_hash' IS DISTINCT FROM completion.event_hash
        OR EXISTS(SELECT 1 FROM jsonb_array_elements(completion.payload->'orders') o WHERE o->>'action' NOT IN ('filled','partially_filled'))
        OR NEW.qualification->'lanes' IS DISTINCT FROM root.qualification->'lanes'
        OR NEW.eligible_at<greatest(previous.window_end,completion.occurred_at,completion.created_at)
        OR NEW.window_start<>date_trunc('minute',NEW.eligible_at)+interval '1 minute'
        OR NEW.frozen_at<NEW.eligible_at
        OR (NEW.status='missed' AND NEW.frozen_at<NEW.window_start)
        OR NEW.content_hash<>encode(sha256(convert_to(nexus_economic_json_text(NEW.qualification,false),'UTF8')),'hex')
        OR NEW.qualification->>'pair_slot_id' IS DISTINCT FROM NEW.pair_slot_id::text
        OR (NEW.qualification->>'window_seq')::int IS DISTINCT FROM NEW.window_seq
        OR NEW.qualification->>'status' IS DISTINCT FROM NEW.status
        OR (NEW.qualification->>'eligible_at')::timestamptz IS DISTINCT FROM NEW.eligible_at
        OR (NEW.qualification->>'window_start')::timestamptz IS DISTINCT FROM NEW.window_start
        OR (NEW.qualification->>'window_end')::timestamptz IS DISTINCT FROM NEW.window_end
        OR (NEW.qualification->>'frozen_at')::timestamptz IS DISTINCT FROM NEW.frozen_at
        OR NOT EXISTS(SELECT 1 FROM economic_pair_slots s JOIN economic_evaluations v ON v.id=s.evaluation_id JOIN audit_events b ON b.id=v.binding_event_id JOIN agent_model_configs m ON m.id=(b.payload->>'shared_model_id')::uuid WHERE s.id=NEW.pair_slot_id AND NEW.window_end<=s.market_close_at AND NEW.window_end<=m.pricing_expires_at AND NEW.eligible_at>=m.pricing_available_at)
        OR EXISTS(SELECT 1 FROM jsonb_array_elements(root.qualification->'lanes') a JOIN agent_portfolio_synthesis_runs r ON r.id=(a->>'run_id')::uuid JOIN agent_research_outputs o ON o.id=ANY(r.research_output_ids) WHERE o.intent IS NULL OR NEW.window_end>(o.intent->>'valid_until')::timestamptz)
        OR (SELECT jsonb_agg(r->>'order_id' ORDER BY r->>'order_id') FROM jsonb_array_elements(NEW.qualification->'order_states') r) IS DISTINCT FROM expected_orders
        OR jsonb_array_length(NEW.qualification->'order_states')<>jsonb_array_length(expected_orders)
        OR EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.qualification->'order_states') r
          LEFT JOIN paper_order_events e ON e.order_id=(r->>'order_id')::uuid AND e.event_seq=(r->>'event_seq')::int
          LEFT JOIN paper_orders o ON o.id=e.order_id
          LEFT JOIN paper_order_batches b ON b.id=o.batch_id
          LEFT JOIN strategy_decisions d ON d.id=b.strategy_decision_id
          LEFT JOIN economic_lane_cycles l ON l.cycle_id=d.agent_paper_cycle_id AND l.pair_slot_id=NEW.pair_slot_id
          LEFT JOIN paper_execution_window_authorities n ON n.id=(r->>'native_id')::uuid
          LEFT JOIN paper_fills f ON f.id=e.source_fill_id AND f.execution_window_authority_id=n.id
          WHERE e.id IS NULL OR l.id IS NULL OR r->>'lane' IS DISTINCT FROM l.lane
            OR e.payload_hash IS DISTINCT FROM r->>'event_hash' OR e.to_status IS DISTINCT FROM r->>'status'
            OR o.side<>'buy' OR o.account_id IS DISTINCT FROM l.account_id
            OR b.previous_batch_id IS NULL OR NOT(expected_orders @> jsonb_build_array(e.order_id::text))
            OR EXISTS(SELECT 1 FROM paper_order_events later WHERE later.order_id=e.order_id AND later.event_seq>e.event_seq)
            OR NEW.eligible_at<greatest(o.created_at,e.occurred_at,e.created_at,b.created_at,b.evaluated_at)
            OR CASE WHEN e.to_status IN ('submitted','risk_rejected') THEN
                 e.source_fill_id IS NOT NULL OR n.id IS NOT NULL OR r->>'native_id' IS NOT NULL OR r->>'native_hash' IS NOT NULL
                 OR EXISTS(SELECT 1 FROM paper_execution_window_authorities prior WHERE prior.order_id=e.order_id)
                 OR (e.to_status='submitted' AND o.risk_outcome='rejected')
                 OR (e.to_status='risk_rejected' AND (o.risk_outcome<>'rejected' OR e.reason_code<>'PAPER_RISK_REJECTED'))
               WHEN e.to_status IN ('filled','partially_filled') THEN
                 n.id IS NULL OR f.id IS NULL OR n.order_id IS DISTINCT FROM e.order_id
                 OR f.order_id IS DISTINCT FROM e.order_id OR f.account_id IS DISTINCT FROM o.account_id
                 OR n.content_hash IS DISTINCT FROM r->>'native_hash'
                 OR NEW.eligible_at<greatest(n.evaluated_at,n.created_at,n.data_available_at)
               ELSE true END)
        OR NEW.qualification->'orders' IS DISTINCT FROM (SELECT jsonb_agg(r->>'order_id' ORDER BY r->>'order_id') FROM jsonb_array_elements(NEW.qualification->'order_states') r WHERE r->>'status' IN ('submitted','partially_filled'))
      THEN RAISE EXCEPTION 'ECONOMIC_WINDOW_INVALID'; END IF;
      RETURN NEW;
    END $$"""
