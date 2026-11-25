"""Bind restricted SEC discoveries to collection receipts and deny downstream use."""

from alembic import op
from sqlalchemy import text

revision = "0140_sec_observation_only"
down_revision = "0139_sec_audit_lookup"
branch_labels = None
depends_on = None

_AUDIT_BINDING = " SELECT count(*) INTO n FROM public.audit_events e WHERE e.action="
_COMPATIBLE_BINDING = (
    """  IF t='sec_discovery_events' AND v->>'observation_receipt_id' IS NULL THEN
   v:=v-'observation_receipt_id';
  END IF;
"""
    + _AUDIT_BINDING
)

_PRODUCER = """
CREATE FUNCTION public.sec_observation_origin_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE
 scope_rule public.sec_monitor_rules;
 state public.idempotency_records;
 receipt public.audit_events;
 mark text;
BEGIN
 SELECT * INTO scope_rule FROM public.sec_monitor_rules WHERE id=NEW.rule_id;
 IF scope_rule.id IS NULL THEN
  RAISE EXCEPTION 'SEC_OBSERVATION_ORIGIN_INVALID' USING ERRCODE='23514';
 END IF;
 -- The established fact trigger already holds the security/rule locks. A
 -- direct writer must not wait in reverse order for the portfolio lock.
 IF NOT pg_try_advisory_xact_lock(hashtextextended('internal-paper:'||scope_rule.account_id::text,1818))
 THEN RAISE EXCEPTION 'SEC_OBSERVATION_SCOPE_BUSY' USING ERRCODE='55P03'; END IF;
 SELECT * INTO state FROM public.idempotency_records
 WHERE actor_id='host-read-probe:'||scope_rule.account_id::text
   AND route='agent-paper-host-read-probe-v1'
 ORDER BY created_at DESC,id DESC LIMIT 1;
 IF TG_TABLE_NAME='sec_discovery_events' THEN
  mark:=NEW.observation_receipt_id::text;
 ELSE
  mark:=NEW.observation->>'observation_receipt_id';
  IF NEW.observation ? 'observation_receipt_id' AND mark IS NULL THEN
   RAISE EXCEPTION 'SEC_OBSERVATION_ORIGIN_INVALID' USING ERRCODE='23514';
  END IF;
 END IF;
 IF mark IS NULL AND state.schema_version IS DISTINCT FROM 'host_monitor_only_v1' THEN
  RETURN NEW;
 END IF;
 IF mark IS NULL OR state.schema_version IS DISTINCT FROM 'host_monitor_only_v1'
  OR state.response_payload->'admitting' IS DISTINCT FROM 'false'::jsonb
  OR state.response_payload->>'worker' IS DISTINCT FROM state.idempotency_key
 THEN RAISE EXCEPTION 'SEC_OBSERVATION_ORIGIN_INVALID' USING ERRCODE='23514'; END IF;
 SELECT * INTO receipt FROM public.audit_events WHERE id=mark::uuid
  AND actor_id='research-monitor' AND action='research.monitor.disclosure_refresh_result'
  AND schema_version='monitor_disclosure_refresh_v1';
 IF receipt.id IS NULL
  OR receipt.payload->>'account_id' IS DISTINCT FROM scope_rule.account_id::text
  OR receipt.payload->>'instrument_id' IS DISTINCT FROM scope_rule.instrument_id::text
  OR receipt.payload->>'rule_id' IS DISTINCT FROM scope_rule.id::text
  OR receipt.payload->>'issuer_cik' IS DISTINCT FROM scope_rule.issuer_cik
  OR receipt.payload->>'restricted_worker' IS DISTINCT FROM state.idempotency_key
  OR receipt.payload->'durable_sec' IS DISTINCT FROM 'true'::jsonb
  OR receipt.payload->>'feed' IS DISTINCT FROM 'sec_disclosure_index'
  OR receipt.payload->>'operation_id' IS NULL
  OR NOT EXISTS(
   SELECT FROM public.exchange_calendar_authorities authority
   JOIN public.exchange_calendar_sessions market ON market.calendar_authority_id=authority.id
   WHERE authority.id=(receipt.payload->>'calendar_authority_id')::uuid
     AND authority.exchange='XNYS' AND authority.completeness_status='complete'
     AND market.session=((receipt.payload->>'scheduled_at')::timestamptz AT TIME ZONE 'America/New_York')::date
     AND market.session BETWEEN authority.range_start AND authority.range_end
     AND market.market_open_at<=(receipt.payload->>'scheduled_at')::timestamptz
     AND receipt.occurred_at<market.market_close_at AND clock_timestamp()<market.market_close_at
     AND GREATEST(authority.created_at,authority.published_at,market.created_at)
         <=(receipt.payload->>'scheduled_at')::timestamptz
  )
  OR NOT EXISTS(
   SELECT FROM public.audit_events claim
   WHERE claim.actor_id=receipt.actor_id AND claim.schema_version=receipt.schema_version
    AND claim.action='research.monitor.disclosure_refresh_claimed'
    AND claim.payload->>'operation_id'=receipt.payload->>'operation_id'
    AND claim.event_seq<receipt.event_seq AND claim.occurred_at<=receipt.occurred_at
    AND claim.occurred_at>=(receipt.payload->>'scheduled_at')::timestamptz
    AND claim.payload=receipt.payload-ARRAY['status','snapshot_id','snapshot_hash','reason','provider_requests']
  )
 THEN RAISE EXCEPTION 'SEC_OBSERVATION_ORIGIN_INVALID' USING ERRCODE='23514'; END IF;
 IF TG_TABLE_NAME='sec_discovery_checks' THEN
  IF NEW.scheduled_at IS DISTINCT FROM (receipt.payload->>'scheduled_at')::timestamptz
    OR NEW.checked_at < receipt.occurred_at
    OR NOT (NEW.checked_at < NEW.scheduled_at+interval '5 minutes'
      AND clock_timestamp() < NEW.scheduled_at+interval '5 minutes')
    OR (NEW.capture_id IS NULL AND NEW.result IS DISTINCT FROM 'unknown')
    OR (receipt.payload->>'status' IS DISTINCT FROM 'refreshed'
        AND (receipt.payload->>'status' IS DISTINCT FROM 'failed'
          OR receipt.payload->>'reason' IS DISTINCT FROM 'SEC_INDEX_REFRESH_FAILED'
          OR NEW.capture_id IS NOT NULL))
    OR (NEW.capture_id IS NOT NULL AND NOT EXISTS(
      SELECT FROM public.data_snapshots snap WHERE snap.id=NEW.capture_id
      AND snap.id::text=receipt.payload->>'snapshot_id'
      AND snap.source='sec_durable_disclosure_index'
      AND snap.revision=receipt.payload->>'snapshot_hash'
      AND snap.payload#>>'{index,identity_snapshot_id}'=receipt.payload->>'identity_snapshot_id'
    ))
  THEN RAISE EXCEPTION 'SEC_OBSERVATION_ORIGIN_INVALID' USING ERRCODE='23514'; END IF;
 ELSE
  IF receipt.payload->>'status' IS DISTINCT FROM 'refreshed'
   OR NEW.capture_id::text IS DISTINCT FROM receipt.payload->>'snapshot_id'
   OR NEW.first_seen_at < receipt.occurred_at
   OR NOT (NEW.first_seen_at < (receipt.payload->>'scheduled_at')::timestamptz+interval '5 minutes')
  THEN RAISE EXCEPTION 'SEC_OBSERVATION_ORIGIN_INVALID' USING ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END
$$
"""

_PAIR = """
CREATE FUNCTION public.sec_observation_require_check() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
 IF TG_TABLE_NAME='sec_discovery_checks' THEN
  IF (NEW.result='true') IS DISTINCT FROM EXISTS(
   SELECT FROM jsonb_array_elements(coalesce(NEW.observation->'events','[]'::jsonb)) fact
   WHERE fact->>'classification'='new'
  ) OR EXISTS(
   SELECT FROM jsonb_array_elements(coalesce(NEW.observation->'events','[]'::jsonb)) fact
   WHERE fact->>'classification'='new' AND NOT EXISTS(
    SELECT FROM public.sec_discovery_events event
    WHERE (event.rule_id,event.capture_id,event.first_seen_at)=
          (NEW.rule_id,NEW.capture_id,NEW.checked_at)
      AND event.observation_receipt_id=(NEW.observation->>'observation_receipt_id')::uuid
      AND event.event_key=fact->>'event_key' AND event.entry=fact->'entry'
   )
  ) THEN RAISE EXCEPTION 'SEC_OBSERVATION_EVENT_REQUIRED' USING ERRCODE='23514'; END IF;
  RETURN NEW;
 END IF;
 IF NOT EXISTS (
   SELECT FROM public.sec_discovery_checks c
   WHERE c.rule_id=NEW.rule_id AND c.capture_id=NEW.capture_id
    AND c.scheduled_at=date_bin(interval '5 minutes',NEW.first_seen_at,timestamptz '2000-01-01Z')
    AND c.checked_at=NEW.first_seen_at AND c.result='true'
    AND c.observation->>'observation_receipt_id'=NEW.observation_receipt_id::text
    AND c.observation->'events' @> jsonb_build_array(jsonb_build_object(
         'event_key',NEW.event_key,'entry',NEW.entry,'classification','new'))
 )
 THEN RAISE EXCEPTION 'SEC_OBSERVATION_CHECK_REQUIRED' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END
$$
"""

_DOWNSTREAM = """
CREATE FUNCTION public.sec_observation_downstream_guard() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE held boolean; malformed boolean;
BEGIN
 IF TG_TABLE_NAME='sec_notification_claims' THEN
  SELECT EXISTS(SELECT FROM public.sec_discovery_events event
    WHERE (event.account_id,event.instrument_id,event.event_key)
      =(NEW.account_id,NEW.instrument_id,NEW.event_key)
    AND event.observation_receipt_id IS NOT NULL) INTO held;
 ELSIF TG_TABLE_NAME='outbox_events' THEN
  SELECT EXISTS(SELECT FROM public.sec_discovery_events event
    WHERE event.id=NEW.aggregate_id AND event.observation_receipt_id IS NOT NULL) INTO held;
 ELSIF TG_TABLE_NAME='data_snapshots' THEN
  SELECT coalesce(bool_or(event.observation_receipt_id IS NOT NULL),false),
         coalesce(bool_or(NOT ref.valid),false) INTO held, malformed
  FROM (
   SELECT item->>'event_id' AS event_id,
          coalesce(pg_input_is_valid(item->>'event_id','uuid'),false) AS valid
   FROM jsonb_path_query(NEW.payload,'$.agent_paper_context.pending_durable_sec.events[*]') item
   UNION ALL
   SELECT item->>'event_id', coalesce(pg_input_is_valid(item->>'event_id','uuid'),false)
   FROM jsonb_path_query(NEW.payload,'$.agent_paper_context.pending_durable_sec.issues[*]') item
  ) ref
  LEFT JOIN public.sec_discovery_events event ON event.id=CASE WHEN ref.valid THEN ref.event_id::uuid END;
  IF malformed THEN RAISE EXCEPTION 'SEC_OBSERVATION_REFERENCE_INVALID' USING ERRCODE='23514'; END IF;
 ELSE
  SELECT observation_receipt_id IS NOT NULL INTO held
  FROM public.sec_discovery_events WHERE id=NEW.event_id;
 END IF;
 IF held THEN RAISE EXCEPTION 'SEC_OBSERVATION_DOWNSTREAM_DENIED' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END
$$
"""

_ORIGIN_TABLES = ("sec_discovery_checks", "sec_discovery_events")
_DOWNSTREAM_TABLES = {
    "sec_notification_claims": "",
    "sec_event_research_bindings": "",
    "sec_event_consumption_receipts": "",
    "sec_durable_daily_event_receipts": "",
    "data_snapshots": "WHEN (NEW.payload#>'{agent_paper_context,pending_durable_sec}' IS NOT NULL)",
    "outbox_events": "WHEN (NEW.event_type='research.sec.discovered')",
}


def _rewrite_audit(*, forward: bool) -> None:
    connection = op.get_bind()
    definition = connection.scalar(
        text("SELECT pg_get_functiondef('public.sec_durable_complete()'::regprocedure)")
    )
    old, new = (
        (_AUDIT_BINDING, _COMPATIBLE_BINDING) if forward else (_COMPATIBLE_BINDING, _AUDIT_BINDING)
    )
    if not isinstance(definition, str) or definition.count(old) != 1:
        raise RuntimeError("SEC_OBSERVATION_AUDIT_PREDECESSOR_INVALID")
    connection.exec_driver_sql(definition.replace(old, new))


def upgrade() -> None:
    op.execute("""
        ALTER TABLE public.sec_discovery_events ADD COLUMN observation_receipt_id uuid
        CONSTRAINT fk_sec_discovery_events_observation_receipt_id_audit_events
        REFERENCES public.audit_events(id)
    """)
    op.execute("""
        CREATE UNIQUE INDEX uq_sec_refresh_operation_receipt ON public.audit_events
        ((payload->>'operation_id'),action) WHERE schema_version='monitor_disclosure_refresh_v1'
        AND actor_id='research-monitor'
        AND action IN ('research.monitor.disclosure_refresh_claimed',
                       'research.monitor.disclosure_refresh_result')
    """)
    _rewrite_audit(forward=True)
    op.execute(_PRODUCER)
    for table in _ORIGIN_TABLES:
        op.execute(
            f"CREATE TRIGGER sec_observation_origin_guard BEFORE INSERT ON public.{table} "
            "FOR EACH ROW EXECUTE FUNCTION public.sec_observation_origin_guard()"
        )
    op.execute(_PAIR)
    op.execute("""
        CREATE CONSTRAINT TRIGGER sec_observation_require_check AFTER INSERT
        ON public.sec_discovery_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        WHEN (NEW.observation_receipt_id IS NOT NULL)
        EXECUTE FUNCTION public.sec_observation_require_check()
    """)
    op.execute("""
        CREATE CONSTRAINT TRIGGER sec_observation_require_event AFTER INSERT
        ON public.sec_discovery_checks DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
        WHEN (NEW.observation ? 'observation_receipt_id')
        EXECUTE FUNCTION public.sec_observation_require_check()
    """)
    op.execute(_DOWNSTREAM)
    for table, condition in _DOWNSTREAM_TABLES.items():
        op.execute(
            f"CREATE TRIGGER a_sec_observation_downstream_guard BEFORE INSERT ON public.{table} "
            f"FOR EACH ROW {condition} EXECUTE FUNCTION public.sec_observation_downstream_guard()"
        )


def downgrade() -> None:
    # Removing the fence after observation would retroactively grant eligibility.
    op.execute("""
        DO $$ BEGIN
         IF EXISTS(SELECT FROM public.sec_discovery_checks
           WHERE observation ? 'observation_receipt_id')
         THEN RAISE EXCEPTION 'SEC_OBSERVATION_ROLLBACK_REQUIRES_FENCED_VERSION'; END IF;
        END $$
    """)
    for table in _DOWNSTREAM_TABLES:
        op.execute(f"DROP TRIGGER a_sec_observation_downstream_guard ON public.{table}")
    op.execute("DROP FUNCTION public.sec_observation_downstream_guard()")
    op.execute("DROP TRIGGER sec_observation_require_check ON public.sec_discovery_events")
    op.execute("DROP TRIGGER sec_observation_require_event ON public.sec_discovery_checks")
    op.execute("DROP FUNCTION public.sec_observation_require_check()")
    for table in _ORIGIN_TABLES:
        op.execute(f"DROP TRIGGER sec_observation_origin_guard ON public.{table}")
    op.execute("DROP FUNCTION public.sec_observation_origin_guard()")
    _rewrite_audit(forward=False)
    op.execute("DROP INDEX public.uq_sec_refresh_operation_receipt")
    op.execute("ALTER TABLE public.sec_discovery_events DROP COLUMN observation_receipt_id")
