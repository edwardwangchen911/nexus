"""Append-only refreezing of still-unsubmitted SEC research batches."""

from alembic import op

revision = "0131_sec_research_refreeze"
down_revision = "0130_auto_increment_scope"
branch_labels = None
depends_on = None

UPGRADE_SQL = (
    r"""
CREATE TABLE sec_research_batch_supersessions (
	id UUID NOT NULL,
	old_batch_id UUID NOT NULL,
	new_batch_id UUID NOT NULL,
	reason VARCHAR(40) DEFAULT 'current_evidence_refreeze' NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
	CONSTRAINT pk_sec_research_batch_supersessions PRIMARY KEY (id),
	CONSTRAINT uq_sec_research_batch_supersessions_old_batch_id UNIQUE (old_batch_id),
	CONSTRAINT ck_sec_research_batch_supersessions_distinct_batches CHECK (old_batch_id<>new_batch_id),
	CONSTRAINT ck_sec_research_batch_supersessions_reason CHECK (reason='current_evidence_refreeze'),
	CONSTRAINT fk_sec_research_batch_supersessions_old_batch_id_resear_f2be FOREIGN KEY(old_batch_id) REFERENCES research_monitor_batches (id),
	CONSTRAINT fk_sec_research_batch_supersessions_new_batch_id_resear_22bc FOREIGN KEY(new_batch_id) REFERENCES research_monitor_batches (id)
)

""",
    r"""ALTER TABLE public.sec_event_research_bindings DROP CONSTRAINT uq_sec_event_research_bindings_event_id""",
    r"""ALTER TABLE public.sec_event_research_bindings ADD CONSTRAINT uq_sec_event_research_bindings_event_id UNIQUE(event_id,batch_id)""",
    r"""CREATE TABLE public.sec_refreeze_function_backup(definition text NOT NULL)""",
    r"""INSERT INTO public.sec_refreeze_function_backup SELECT pg_get_functiondef('public.sec_research_require_current_batch(uuid)'::regprocedure)""",
    r"""DO $outer$ DECLARE d text; seam text:='PERFORM public.sec_research_validate_batch(batch_uuid);'; BEGIN
 SELECT pg_get_functiondef('public.sec_research_require_current_batch(uuid)'::regprocedure) INTO d;
 IF strpos(d,seam)=0 THEN RAISE EXCEPTION 'SEC_REFREEZE_PREDECESSOR_INVALID'; END IF;
 EXECUTE replace(d,seam,'IF EXISTS(SELECT FROM public.sec_research_batch_supersessions replacement WHERE replacement.old_batch_id=batch_uuid) THEN RAISE EXCEPTION ''SEC_RESEARCH_BATCH_SUPERSEDED'' USING ERRCODE=''23514''; END IF; '||seam);
END $outer$""",
    r"""CREATE FUNCTION public.sec_refreeze_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE source_batch public.research_monitor_batches; replacement_batch public.research_monitor_batches; frozen_capture uuid;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'SEC_REFREEZE_IMMUTABLE' USING ERRCODE='23514'; END IF;
 IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'SEC_REFREEZE_INVALID' USING ERRCODE='23514'; END IF;
 SELECT * INTO source_batch FROM public.research_monitor_batches WHERE id=NEW.old_batch_id;
 SELECT * INTO replacement_batch FROM public.research_monitor_batches WHERE id=NEW.new_batch_id;
 IF source_batch.id IS NULL OR replacement_batch.id IS NULL OR source_batch.id=replacement_batch.id
 OR source_batch.routing_version<>'durable_research_v2' OR replacement_batch.routing_version<>source_batch.routing_version
 OR (source_batch.account_id,source_batch.instrument_id) IS DISTINCT FROM (replacement_batch.account_id,replacement_batch.instrument_id)
 OR NEW.reason<>'current_evidence_refreeze' OR NEW.created_at<transaction_timestamp() OR NEW.created_at>clock_timestamp()
 OR NOT(source_batch.sealed_at<replacement_batch.sealed_at AND replacement_batch.sealed_at<=NEW.created_at)
 THEN RAISE EXCEPTION 'SEC_REFREEZE_INVALID' USING ERRCODE='23514'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('research-monitor:'||source_batch.account_id::text||':'||source_batch.instrument_id::text,0));
 IF EXISTS(SELECT FROM public.research_monitor_increments m WHERE m.batch_id IN (source_batch.id,replacement_batch.id))
 OR EXISTS(SELECT FROM public.research_monitor_triggers trigger_row WHERE trigger_row.batch_id IN(source_batch.id,replacement_batch.id) AND (trigger_row.research_run_id IS NOT NULL OR trigger_row.status NOT IN ('pending','blocked')))
 OR EXISTS(SELECT FROM public.sec_event_research_bindings prior_binding JOIN public.sec_event_research_bindings any_binding ON any_binding.event_id=prior_binding.event_id JOIN public.research_monitor_increments paid ON paid.batch_id=any_binding.batch_id WHERE prior_binding.batch_id=source_batch.id)
 OR EXISTS(SELECT FROM public.sec_research_batch_supersessions successor WHERE successor.old_batch_id IN(source_batch.id,replacement_batch.id))
 THEN RAISE EXCEPTION 'SEC_REFREEZE_ALREADY_SUBMITTED_OR_UNKNOWN' USING ERRCODE='23514'; END IF;
 PERFORM public.sec_research_validate_batch(source_batch.id);
 PERFORM public.sec_research_require_current_batch(replacement_batch.id);
 IF (SELECT count(DISTINCT capture_id) FROM public.sec_event_research_bindings WHERE batch_id=replacement_batch.id)<>1 THEN RAISE EXCEPTION 'SEC_REFREEZE_INVALID' USING ERRCODE='23514'; END IF;
 SELECT capture_id INTO frozen_capture FROM public.sec_event_research_bindings WHERE batch_id=replacement_batch.id LIMIT 1;
 -- A sound still-actionable old key cannot disappear merely because a new key arrived.
 IF EXISTS(SELECT FROM public.sec_event_research_bindings old_binding
 JOIN public.sec_discovery_events event_row ON event_row.id=old_binding.event_id
 JOIN public.data_snapshots capture_row ON capture_row.id=frozen_capture
 WHERE old_binding.batch_id=source_batch.id
 AND NOT EXISTS(SELECT FROM public.sec_discovery_event_revisions revision_row WHERE revision_row.event_id=event_row.id)
 AND NOT EXISTS(SELECT FROM public.sec_event_consumption_receipts receipt WHERE receipt.event_id=event_row.id)
 AND EXISTS(SELECT FROM jsonb_array_elements(capture_row.payload#>'{index,entries}') raw_entry WHERE raw_entry=event_row.entry)
 AND NOT EXISTS(SELECT FROM public.sec_event_research_bindings kept WHERE kept.batch_id=replacement_batch.id AND kept.event_id=event_row.id))
 THEN RAISE EXCEPTION 'SEC_REFREEZE_VALID_EVENT_DROPPED' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$""",
    r"""CREATE TRIGGER sec_refreeze_guard BEFORE INSERT OR UPDATE OR DELETE ON public.sec_research_batch_supersessions FOR EACH ROW EXECUTE FUNCTION public.sec_refreeze_guard()""",
    r"""CREATE FUNCTION public.sec_refreeze_duplicate_binding_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
 IF EXISTS(SELECT FROM public.sec_event_research_bindings prior_binding WHERE prior_binding.event_id=NEW.event_id AND prior_binding.id<>NEW.id AND prior_binding.batch_id<>NEW.batch_id
  AND NOT EXISTS(SELECT FROM public.sec_research_batch_supersessions replacement WHERE replacement.old_batch_id=prior_binding.batch_id))
 THEN RAISE EXCEPTION 'SEC_REFREEZE_SUPERSESSION_REQUIRED' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$""",
    r"""CREATE CONSTRAINT TRIGGER sec_refreeze_duplicate_binding AFTER INSERT ON public.sec_event_research_bindings DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_refreeze_duplicate_binding_guard()""",
    r"""CREATE UNIQUE INDEX uq_sec_refreeze_audit ON public.audit_events((payload->>'row_id')) WHERE action='research.monitor.sec_research_batch_superseded'""",
    r"""CREATE FUNCTION public.sec_refreeze_audit_complete() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE fact public.sec_research_batch_supersessions; n integer;
BEGIN
 IF TG_TABLE_NAME='audit_events' THEN
  IF NEW.action<>'research.monitor.sec_research_batch_superseded' THEN RETURN NEW; END IF;
  SELECT * INTO fact FROM public.sec_research_batch_supersessions WHERE id::text=NEW.payload->>'row_id';
 ELSE fact:=NEW; END IF;
 SELECT count(*) INTO n FROM public.audit_events audit_row WHERE audit_row.action='research.monitor.sec_research_batch_superseded'
 AND audit_row.actor_id='research-monitor' AND audit_row.schema_version='sec_research_refreeze_v1' AND audit_row.occurred_at=fact.created_at
 AND audit_row.payload=jsonb_build_object('row_id',fact.id::text,'binding',to_jsonb(fact)-'created_at');
 IF fact.id IS NULL OR n<>1 THEN RAISE EXCEPTION 'SEC_REFREEZE_AUDIT_INVALID' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$""",
    r"""CREATE CONSTRAINT TRIGGER sec_refreeze_audit_complete AFTER INSERT ON public.sec_research_batch_supersessions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_refreeze_audit_complete()""",
    r"""CREATE CONSTRAINT TRIGGER sec_refreeze_audit_complete AFTER INSERT ON public.audit_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.sec_refreeze_audit_complete()""",
)

DOWNGRADE_SQL = (
    r"""LOCK TABLE public.sec_research_batch_supersessions,public.sec_event_research_bindings,public.research_monitor_increments,public.audit_events IN SHARE ROW EXCLUSIVE MODE NOWAIT""",
    r"""DO $$ BEGIN IF EXISTS(SELECT FROM public.sec_research_batch_supersessions) OR EXISTS(SELECT FROM public.audit_events WHERE action='research.monitor.sec_research_batch_superseded') OR EXISTS(SELECT FROM public.sec_event_research_bindings GROUP BY event_id,plan_id HAVING count(*)>1) THEN RAISE EXCEPTION 'SEC_REFREEZE_DOWNGRADE_FORBIDDEN' USING ERRCODE='23514'; END IF; END $$""",
    r"""DROP TRIGGER sec_refreeze_audit_complete ON public.audit_events""",
    r"""DROP TRIGGER sec_refreeze_duplicate_binding ON public.sec_event_research_bindings""",
    r"""DO $$ DECLARE d text; BEGIN SELECT definition INTO STRICT d FROM public.sec_refreeze_function_backup; EXECUTE d; END $$""",
    r"""ALTER TABLE public.sec_event_research_bindings DROP CONSTRAINT uq_sec_event_research_bindings_event_id""",
    r"""ALTER TABLE public.sec_event_research_bindings ADD CONSTRAINT uq_sec_event_research_bindings_event_id UNIQUE(event_id,plan_id)""",
    r"""DROP TABLE public.sec_research_batch_supersessions""",
    r"""DROP FUNCTION public.sec_refreeze_guard()""",
    r"""DROP FUNCTION public.sec_refreeze_duplicate_binding_guard()""",
    r"""DROP FUNCTION public.sec_refreeze_audit_complete()""",
    r"""DROP INDEX public.uq_sec_refreeze_audit""",
    r"""DROP TABLE public.sec_refreeze_function_backup""",
)


def upgrade() -> None:
    for statement in UPGRADE_SQL:
        op.execute(statement)


def downgrade() -> None:
    for statement in DOWNGRADE_SQL:
        op.execute(statement)
