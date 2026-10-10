"""Admit chained windows after confirmed native completion; preserve old facts."""

from alembic import op

revision = "0072_economic_later_windows"
down_revision = "0071_economic_pair_windows"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(_ECONOMIC_CHAIN)
    op.execute(
        "ALTER TABLE economic_common_windows DROP CONSTRAINT ck_economic_common_windows_first_window_only"
    )
    op.execute(
        "ALTER TABLE economic_common_windows ADD CONSTRAINT ck_economic_common_windows_sequence_positive CHECK(window_seq>=1)"
    )
    op.execute(
        "ALTER FUNCTION nexus_economic_window_guard() RENAME TO nexus_economic_first_window_guard"
    )
    op.execute("DROP TRIGGER economic_window_guard ON economic_common_windows")
    op.execute(
        "CREATE TRIGGER economic_first_window_guard BEFORE INSERT ON economic_common_windows FOR EACH ROW WHEN (NEW.window_seq=1) EXECUTE FUNCTION nexus_economic_first_window_guard()"
    )
    op.execute("""CREATE FUNCTION nexus_economic_window_guard() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE previous economic_common_windows; completion audit_events; root economic_common_windows;
    BEGIN
      SELECT * INTO previous FROM economic_common_windows WHERE pair_slot_id=NEW.pair_slot_id AND window_seq=NEW.window_seq-1;
      SELECT * INTO root FROM economic_common_windows WHERE pair_slot_id=NEW.pair_slot_id AND window_seq=1;
      SELECT * INTO completion FROM audit_events WHERE id=(NEW.qualification->>'completion_id')::uuid;
      IF previous.id IS NULL OR previous.status<>'frozen' OR root.id IS NULL
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
        OR (SELECT jsonb_agg(r->>'order_id' ORDER BY r->>'order_id') FROM jsonb_array_elements(NEW.qualification->'order_states') r) IS DISTINCT FROM root.qualification->'orders'
        OR jsonb_array_length(NEW.qualification->'order_states')<>jsonb_array_length(root.qualification->'orders')
        OR EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.qualification->'order_states') r
          LEFT JOIN paper_order_events e ON e.order_id=(r->>'order_id')::uuid AND e.event_seq=(r->>'event_seq')::int
          LEFT JOIN paper_execution_window_authorities n ON n.id=(r->>'native_id')::uuid
          LEFT JOIN paper_fills f ON f.id=e.source_fill_id AND f.execution_window_authority_id=n.id
          WHERE e.id IS NULL OR n.id IS NULL OR f.id IS NULL OR n.order_id<>e.order_id OR e.payload_hash IS DISTINCT FROM r->>'event_hash'
            OR n.content_hash IS DISTINCT FROM r->>'native_hash' OR e.to_status IS DISTINCT FROM r->>'status'
            OR e.to_status NOT IN ('submitted','partially_filled','filled')
            OR NOT (root.qualification->'orders' @> jsonb_build_array(e.order_id::text))
            OR EXISTS(SELECT 1 FROM paper_order_events later WHERE later.order_id=e.order_id AND later.event_seq>e.event_seq)
            OR NEW.eligible_at<greatest(e.occurred_at,e.created_at,n.evaluated_at,n.created_at,n.data_available_at))
        OR NEW.qualification->'orders' IS DISTINCT FROM (SELECT jsonb_agg(r->>'order_id' ORDER BY r->>'order_id') FROM jsonb_array_elements(NEW.qualification->'order_states') r WHERE r->>'status' IN ('submitted','partially_filled'))
      THEN RAISE EXCEPTION 'ECONOMIC_WINDOW_INVALID'; END IF;
      RETURN NEW;
    END $$""")
    op.execute(
        "CREATE TRIGGER economic_window_guard BEFORE INSERT ON economic_common_windows FOR EACH ROW WHEN (NEW.window_seq>1) EXECUTE FUNCTION nexus_economic_window_guard()"
    )


def downgrade() -> None:
    op.execute(
        """DO $$ BEGIN IF EXISTS(SELECT 1 FROM economic_common_windows WHERE window_seq>1) THEN RAISE EXCEPTION 'ECONOMIC_WINDOW_DOWNGRADE_FORBIDDEN'; END IF; END $$"""
    )
    op.execute(_LEGACY_CHAIN)
    op.execute("DROP TRIGGER economic_window_guard ON economic_common_windows")
    op.execute("DROP TRIGGER economic_first_window_guard ON economic_common_windows")
    op.execute("DROP FUNCTION nexus_economic_window_guard()")
    op.execute(
        "ALTER FUNCTION nexus_economic_first_window_guard() RENAME TO nexus_economic_window_guard"
    )
    op.execute(
        "CREATE TRIGGER economic_window_guard BEFORE INSERT ON economic_common_windows FOR EACH ROW EXECUTE FUNCTION nexus_economic_window_guard()"
    )
    op.execute(
        "ALTER TABLE economic_common_windows DROP CONSTRAINT ck_economic_common_windows_sequence_positive"
    )
    op.execute(
        "ALTER TABLE economic_common_windows ADD CONSTRAINT ck_economic_common_windows_first_window_only CHECK(window_seq=1)"
    )


_LEGACY_CHAIN = """CREATE OR REPLACE FUNCTION nexus_paper_execution_window_chain_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM (
              SELECT
                window_seq,
                row_number() OVER (ORDER BY window_seq) AS expected_seq,
                window_start_at,
                evaluated_at,
                source,
                feed,
                delivery_mode,
                lag(window_end_at) OVER (ORDER BY window_seq) AS prior_end_at,
                lag(evaluated_at) OVER (ORDER BY window_seq) AS prior_evaluated_at,
                lag(source) OVER (ORDER BY window_seq) AS prior_source,
                lag(feed) OVER (ORDER BY window_seq) AS prior_feed,
                lag(delivery_mode) OVER (ORDER BY window_seq) AS prior_delivery_mode
              FROM paper_execution_window_authorities
              WHERE order_id = NEW.order_id
            ) ordered
            WHERE window_seq <> expected_seq
              OR (
                window_seq > 1
                AND (
                  window_start_at IS DISTINCT FROM prior_end_at
                  OR evaluated_at < prior_evaluated_at
                  OR source IS DISTINCT FROM prior_source
                  OR feed IS DISTINCT FROM prior_feed
                  OR delivery_mode IS DISTINCT FROM prior_delivery_mode
                )
              )
          ) THEN
            RAISE EXCEPTION 'paper execution window chain is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
"""
_ECONOMIC_CHAIN = """CREATE OR REPLACE FUNCTION nexus_paper_execution_window_chain_valid()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM (
              SELECT
                id, window_end_at, source_authority_id,
                lag(id) OVER (ORDER BY window_seq) AS prior_id,
                window_seq,
                row_number() OVER (ORDER BY window_seq) AS expected_seq,
                window_start_at,
                evaluated_at,
                source,
                feed,
                delivery_mode,
                lag(window_end_at) OVER (ORDER BY window_seq) AS prior_end_at,
                lag(evaluated_at) OVER (ORDER BY window_seq) AS prior_evaluated_at,
                lag(source) OVER (ORDER BY window_seq) AS prior_source,
                lag(feed) OVER (ORDER BY window_seq) AS prior_feed,
                lag(delivery_mode) OVER (ORDER BY window_seq) AS prior_delivery_mode
              FROM paper_execution_window_authorities
              WHERE order_id = NEW.order_id
            ) ordered
            WHERE window_seq <> expected_seq
              OR (
                window_seq > 1
                AND (
                  (window_start_at IS DISTINCT FROM prior_end_at AND NOT EXISTS (
                    SELECT 1 FROM economic_common_windows w
                    JOIN economic_window_orders eo ON eo.window_id=w.id AND eo.order_id=NEW.order_id
                    JOIN agent_paper_market_windows b ON b.order_id=eo.order_id
                      AND b.window_start_at=w.window_start AND b.window_end_at=w.window_end
                    WHERE w.status='frozen' AND w.window_seq>1
                      AND w.window_start=ordered.window_start_at
                      AND w.window_end=ordered.window_end_at
                      AND b.source_authority_id=ordered.source_authority_id
                      AND ordered.window_start_at>=ordered.prior_end_at
                      AND w.qualification->'order_states' @> jsonb_build_array(
                        jsonb_build_object('order_id',NEW.order_id::text,'native_id',ordered.prior_id::text))
                  ))
                  OR evaluated_at < prior_evaluated_at
                  OR source IS DISTINCT FROM prior_source
                  OR feed IS DISTINCT FROM prior_feed
                  OR delivery_mode IS DISTINCT FROM prior_delivery_mode
                )
              )
          ) THEN
            RAISE EXCEPTION 'paper execution window chain is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END;
        $$
"""
