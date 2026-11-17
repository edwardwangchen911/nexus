"""Separate stock-session limiting scope from SEC discovery enablement."""

from alembic import op

revision = "0130_auto_increment_scope"
down_revision = "0129_sec_key_conflicts"
branch_labels = None
depends_on = None

UPGRADE_SQL = (
    "CREATE UNIQUE INDEX uq_auto_increment_scope_account ON public.data_snapshots(provider_object_id) WHERE source='nexus_auto_increment_policy'",
    "CREATE FUNCTION public.auto_increment_scope_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$\nDECLARE candidate_row public.agent_paper_candidate_batches;\nBEGIN\n IF TG_OP='DELETE' THEN IF OLD.source='nexus_auto_increment_policy' THEN RAISE EXCEPTION 'AUTO_INCREMENT_SCOPE_IMMUTABLE' USING ERRCODE='23514'; END IF; RETURN OLD; END IF;\n IF TG_OP='UPDATE' THEN IF OLD.source='nexus_auto_increment_policy' OR NEW.source='nexus_auto_increment_policy' THEN RAISE EXCEPTION 'AUTO_INCREMENT_SCOPE_IMMUTABLE' USING ERRCODE='23514'; END IF; RETURN NEW; END IF;\n IF NEW.source<>'nexus_auto_increment_policy' THEN RETURN NEW; END IF;\n IF current_setting('transaction_isolation')<>'read committed' THEN RAISE EXCEPTION 'AUTO_INCREMENT_SCOPE_INVALID' USING ERRCODE='23514'; END IF;\n PERFORM pg_advisory_xact_lock(hashtextextended('auto-increment-policy:'||NEW.provider_object_id,0));\n SELECT * INTO candidate_row FROM public.agent_paper_candidate_batches WHERE id=(NEW.payload->>'candidate_batch_id')::uuid;\n IF candidate_row.id IS NULL OR NEW.schema_version<>'auto_increment_scope_v1' OR NEW.payload IS DISTINCT FROM jsonb_build_object('schema_version','auto_increment_scope_v1','account_id',candidate_row.account_id::text,'candidate_batch_id',candidate_row.id::text,'candidate_hash',candidate_row.batch_hash,'instrument_ids',to_jsonb(candidate_row.candidate_instrument_ids),'effective_at',NEW.payload->>'effective_at','max_per_session',1,'session_timezone','America/New_York')\n OR NEW.provider_object_id IS DISTINCT FROM candidate_row.account_id::text\n OR NEW.created_at<transaction_timestamp() OR NEW.created_at>clock_timestamp() OR candidate_row.created_at>NEW.created_at\n OR greatest(NEW.observed_at,NEW.available_at,NEW.received_at) IS DISTINCT FROM NEW.created_at\n OR least(NEW.observed_at,NEW.available_at,NEW.received_at) IS DISTINCT FROM NEW.created_at\n OR (NEW.payload->>'effective_at')::timestamptz IS DISTINCT FROM NEW.created_at\n OR NEW.revision IS DISTINCT FROM encode(sha256(convert_to(public.sec_positive_chain_canonical(NEW.payload),'UTF8')),'hex')\n THEN RAISE EXCEPTION 'AUTO_INCREMENT_SCOPE_INVALID' USING ERRCODE='23514'; END IF;\n RETURN NEW;\nEND $$",
    "CREATE TRIGGER auto_increment_scope_guard BEFORE INSERT OR UPDATE OR DELETE ON public.data_snapshots FOR EACH ROW EXECUTE FUNCTION public.auto_increment_scope_guard()",
    "CREATE FUNCTION public.auto_increment_scope_complete() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$\nDECLARE row_record public.data_snapshots;\nBEGIN\n IF TG_TABLE_NAME='audit_events' THEN\n  IF NEW.action<>'research.monitor.auto_increment_scope' THEN RETURN NEW; END IF;\n  SELECT * INTO row_record FROM public.data_snapshots WHERE id=(NEW.payload->>'snapshot_id')::uuid AND source='nexus_auto_increment_policy';\n ELSE\n  IF NEW.source<>'nexus_auto_increment_policy' THEN RETURN NEW; END IF;\n  row_record:=NEW;\n END IF;\n IF row_record.id IS NULL THEN RAISE EXCEPTION 'AUTO_INCREMENT_SCOPE_AUDIT_INVALID' USING ERRCODE='23514'; END IF;\n PERFORM public.sec_auto_increment_scoped((row_record.payload->>'account_id')::uuid,(row_record.payload->'instrument_ids'->>0)::uuid);\n RETURN NEW;\nEND $$",
    "CREATE CONSTRAINT TRIGGER auto_increment_scope_complete AFTER INSERT ON public.data_snapshots DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.auto_increment_scope_complete()",
    "CREATE CONSTRAINT TRIGGER auto_increment_scope_complete AFTER INSERT ON public.audit_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.auto_increment_scope_complete()",
    "CREATE UNIQUE INDEX uq_auto_increment_scope_audit ON public.audit_events((payload->>'snapshot_id')) WHERE action='research.monitor.auto_increment_scope'",
)

DOWNGRADE_SQL = (
    "LOCK TABLE public.data_snapshots,public.audit_events IN SHARE ROW EXCLUSIVE MODE NOWAIT",
    "DO $$ BEGIN IF EXISTS(SELECT FROM public.data_snapshots WHERE source='nexus_auto_increment_policy') OR EXISTS(SELECT FROM public.audit_events WHERE action='research.monitor.auto_increment_scope') THEN RAISE EXCEPTION 'AUTO_INCREMENT_SCOPE_DOWNGRADE_FORBIDDEN'; END IF; END $$",
    "DROP TRIGGER auto_increment_scope_complete ON public.audit_events",
    "DROP TRIGGER auto_increment_scope_complete ON public.data_snapshots",
    "DROP TRIGGER auto_increment_scope_guard ON public.data_snapshots",
    "DROP FUNCTION public.auto_increment_scope_complete()",
    "DROP FUNCTION public.auto_increment_scope_guard()",
    "DROP INDEX public.uq_auto_increment_scope_audit",
    "DROP INDEX public.uq_auto_increment_scope_account",
)


def upgrade() -> None:
    for statement in UPGRADE_SQL:
        op.execute(statement)


def downgrade() -> None:
    for statement in DOWNGRADE_SQL:
        op.execute(statement)
