"""Bind one adopted graphic to complete filing custody and every new freeze."""

from alembic import op
from sqlalchemy import text

revision = "0136_sec_adopted_content"
down_revision = "0135_routine_budget"
branch_labels = None
depends_on = None

_ADOPTION = r"""
CREATE FUNCTION public.sec_graphic_adoption_valid(aid uuid, cutoff timestamptz)
RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,public AS $$
SELECT EXISTS(SELECT FROM public.data_snapshots a JOIN public.data_snapshots o
 ON o.id::text=a.payload#>>'{material,original_snapshot_id}'
 WHERE a.id=aid AND a.source='nexus_sec_graphic_adoption' AND a.schema_version='sec_graphic_adoption_v1'
 AND a.provider_object_id='0001045810:0001045810-26-000088:nvdalogoa19.jpg'
 AND a.payload->>'purpose'='nvda_000088_header_graphic_only'
 AND a.payload->'research_qualified'='false'::jsonb
 AND a.payload->>'graphic_filename'='nvdalogoa19.jpg'
 AND a.payload->>'graphic_sha256'='578e5181e5d55b715e64c2e3e53fed5b6cbd2c512f6422105769f0f06056ed32'
 AND a.payload#>>'{material,issuer_cik}'='0001045810'
 AND a.payload#>>'{material,entry,accession_number}'='0001045810-26-000088'
 AND encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(a.payload->'material'),'UTF8')),'hex')='1ee097fa875484f5a51b8df1740c4c3aa83a521b46fd3bfcd84be9bfc91d32c3'
 AND encode(sha256(convert_to(a.payload->>'report_text','UTF8')),'hex')='37992ce996ac13cdbd2dec11fe852fdd11f04b6b670282d8d7396b8b414840c6'
 AND encode(sha256(convert_to(a.payload->>'owner_message_text','UTF8')),'hex')='0dd184b199415fe361af5ab2823baaccdd35359fba34b5aacf8667a3a1af48a3'
 AND length(a.payload->>'report_source')>=8 AND length(a.payload->>'owner_message_source')>=8
 AND a.revision=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(a.payload),'UTF8')),'hex')
 AND a.observed_at=a.received_at AND a.received_at=a.available_at AND a.available_at=a.created_at
 AND a.created_at<=cutoff AND (a.payload#>>'{material,prepared_at}')::timestamptz<=a.created_at
 AND o.source='nexus_sec_uninterpreted_filing' AND o.schema_version='sec_uninterpreted_filing_v1'
 AND o.revision=a.payload#>>'{material,original_hash}'
 AND o.revision=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(o.payload),'UTF8')),'hex')
 AND greatest(o.created_at,o.observed_at,o.available_at,o.received_at)<=(a.payload#>>'{material,prepared_at}')::timestamptz
 AND o.payload->'declarations'=a.payload#>'{material,original_declarations}'
 AND o.payload->'entry'=a.payload#>'{material,entry}' AND o.payload->>'issuer_cik'=a.payload#>>'{material,issuer_cik}'
 AND (SELECT count(*) FROM public.audit_events ev WHERE ev.action='research.sec.graphic_adopted'
  AND ev.payload->>'snapshot_id'=a.id::text)=1
 AND EXISTS(SELECT FROM public.audit_events ev WHERE ev.action='research.sec.graphic_adopted'
  AND ev.actor_id='local-owner-operator' AND ev.schema_version='sec_graphic_adoption_v1'
  AND ev.occurred_at=a.created_at AND ev.created_at<=cutoff AND ev.payload=jsonb_build_object('snapshot_id',a.id::text,'canonical_hash',a.revision,'purpose',a.payload->>'purpose')))
$$;
"""

_CONTENT = r"""
CREATE FUNCTION public.sec_adopted_content_valid(cid uuid, cutoff timestamptz)
RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,public AS $$
SELECT EXISTS(SELECT FROM public.data_snapshots c JOIN public.data_snapshots a
 ON a.id::text=c.payload->>'graphic_adoption_id'
 JOIN public.data_snapshots o ON o.id::text=a.payload#>>'{material,original_snapshot_id}'
 WHERE c.id=cid AND c.source='nexus_sec_monitor_filing' AND c.schema_version='sec_monitor_filing_v3'
 AND c.payload->>'schema_version'=c.schema_version
 AND public.sec_graphic_adoption_valid(a.id,c.available_at)
 AND c.revision=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(c.payload),'UTF8')),'hex')
 AND c.provider_object_id=(c.payload->>'issuer_cik')||':'||(c.payload#>>'{entry,accession_number}')
 AND c.created_at=c.received_at AND c.received_at=c.available_at AND c.available_at<=cutoff
 AND c.observed_at=(c.payload#>>'{entry,accepted_at}')::timestamptz
 AND NOT EXISTS(SELECT FROM unnest(ARRAY['instrument_id','issuer_cik','index_snapshot_id','index_hash','entry','originals']) k
  WHERE c.payload->k IS DISTINCT FROM o.payload->k)
 AND encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(c.payload->'documents'),'UTF8')),'hex')='d29624c14794e4070bceb31054f43b9ec4ed50e2000ee48cc8a1f9ffff7d83cf'
 AND encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(c.payload->'dependency_proof'),'UTF8')),'hex')='955a0ad99071b1d0f382f08dba85bc446f0abbfbe3da50f2ed8f8f1f1e718e21'
 AND NOT EXISTS(SELECT FROM jsonb_each(c.payload->'originals') pair, jsonb_array_elements(pair.value) b WHERE NOT EXISTS(SELECT FROM public.data_artifacts ar
  WHERE ar.id::text=b->>'artifact_id' AND ar.uri=b->>'uri' AND ar.sha256=b->>'sha256' AND ar.byte_size=(b->>'byte_size')::bigint AND ar.source='sec'
  AND greatest(ar.created_at,ar.observed_at,ar.available_at,ar.received_at)<=(a.payload#>>'{material,prepared_at}')::timestamptz))
 AND EXISTS(SELECT FROM public.data_snapshots idx WHERE idx.id::text=c.payload->>'index_snapshot_id'
  AND ((idx.source='sec_disclosure_index' AND idx.schema_version='sec_disclosure_index_v1')
    OR (idx.source='sec_disclosure_listed_index' AND idx.schema_version='sec_disclosure_index_v2')
    OR (idx.source='sec_durable_disclosure_index' AND idx.schema_version='sec_durable_index_v1'))
  AND idx.payload->>'schema_version'=idx.schema_version
  AND idx.revision=c.payload->>'index_hash'
  AND idx.revision=encode(sha256(convert_to(public.nexus_mixed_receipt_canonical(idx.payload),'UTF8')),'hex')
  AND idx.payload#>>'{index,instrument_id}'=c.payload->>'instrument_id'
  AND idx.payload#>>'{index,issuer_cik}'=c.payload->>'issuer_cik'
  AND idx.payload#>'{index,entries}' @> jsonb_build_array(c.payload->'entry')
  AND greatest(idx.created_at,idx.observed_at,idx.available_at,idx.received_at)<=(a.payload#>>'{material,prepared_at}')::timestamptz)
 AND c.payload->>'accounting_revision_status'='unverified'
 AND c.payload->>'amendment_relation_status'='not_assessed'
 AND c.payload#>>'{dependency_proof,profile}'='exact_owner_designated_graphic_v1'
 AND c.payload#>'{dependency_proof,graphic_references}'='1'::jsonb
 AND c.payload#>'{dependency_proof,all_aggregate_members_reviewed}'='false'::jsonb
 AND jsonb_typeof(c.payload->'documents')='array'
 AND jsonb_array_length(c.payload->'documents')=(SELECT count(*) FROM jsonb_array_elements(o.payload->'declarations') d WHERE d->>'document_type'<>'GRAPHIC')
 AND (SELECT count(DISTINCT d->>'filename') FROM jsonb_array_elements(c.payload->'documents') d)=jsonb_array_length(c.payload->'documents')
 AND NOT EXISTS(SELECT FROM jsonb_array_elements(c.payload->'documents') d WHERE d->'truncated' IS DISTINCT FROM 'false'::jsonb
  OR NOT EXISTS(SELECT FROM jsonb_array_elements(o.payload->'declarations') old WHERE old->>'document_type'<>'GRAPHIC'
   AND old->>'filename'=d->>'filename' AND old->>'document_type'=d->>'document_type' AND old->>'raw_hash'=d->>'raw_hash'))
 AND EXISTS(SELECT FROM public.research_evidence_refs er WHERE er.source=c.source AND er.provider_object_id=c.provider_object_id
  AND er.revision=c.revision AND er.content_hash=c.revision AND er.available_at=c.available_at AND er.created_at=c.created_at)
 AND (SELECT count(*) FROM public.audit_events ev WHERE ev.action='research.monitor.filing_content_received'
  AND ev.payload->>'snapshot_id'=c.id::text)=1
 AND EXISTS(SELECT FROM public.audit_events ev WHERE ev.action='research.monitor.filing_content_received'
  AND ev.actor_id='research-monitor' AND ev.schema_version=c.schema_version AND ev.occurred_at=c.created_at AND ev.created_at<=cutoff
  AND ev.payload=jsonb_build_object('snapshot_id',c.id::text,'canonical_hash',c.revision,'index_snapshot_id',c.payload->>'index_snapshot_id')))
$$;
"""

_REFS = r"""
CREATE FUNCTION public.sec_adopted_refs_complete(v jsonb, cutoff timestamptz, depth integer DEFAULT 0)
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
DECLARE x jsonb; row public.data_snapshots;
BEGIN
 IF depth>40 THEN RAISE EXCEPTION 'SEC_ADOPTED_CONTENT_INVALID' USING ERRCODE='23514'; END IF;
 IF jsonb_typeof(v)='object' THEN
  row:=NULL;
  IF v ? 'snapshot_id' THEN
   SELECT * INTO row FROM public.data_snapshots WHERE id::text=v->>'snapshot_id';
  END IF;
  IF v#>>'{archive,schema_version}'='sec_monitor_filing_v3' OR row.schema_version='sec_monitor_filing_v3' THEN
   IF row.id IS NULL OR NOT public.sec_adopted_content_valid(row.id,cutoff)
    OR row.payload IS DISTINCT FROM v->'archive' OR row.revision IS DISTINCT FROM v->>'canonical_hash'
    OR NOT EXISTS(SELECT FROM public.research_evidence_refs er WHERE er.id::text=v->>'evidence_ref_id'
     AND er.source=row.source AND er.provider_object_id=row.provider_object_id AND er.revision=row.revision
     AND er.content_hash=row.revision AND er.available_at=row.available_at)
   THEN RAISE EXCEPTION 'SEC_ADOPTED_CONTENT_INVALID' USING ERRCODE='23514'; END IF;
  END IF;
  FOR x IN SELECT value FROM jsonb_each(v) LOOP PERFORM public.sec_adopted_refs_complete(x,cutoff,depth+1); END LOOP;
 ELSIF jsonb_typeof(v)='array' THEN
  FOR x IN SELECT value FROM jsonb_array_elements(v) LOOP PERFORM public.sec_adopted_refs_complete(x,cutoff,depth+1); END LOOP;
 END IF;
END $$;
CREATE FUNCTION public.sec_adopted_content_complete() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
 IF NEW.source='nexus_sec_graphic_adoption' AND NOT public.sec_graphic_adoption_valid(NEW.id,clock_timestamp())
 THEN RAISE EXCEPTION 'SEC_GRAPHIC_ADOPTION_INVALID' USING ERRCODE='23514'; END IF;
 IF NEW.schema_version='sec_monitor_filing_v3' AND NOT public.sec_adopted_content_valid(NEW.id,clock_timestamp())
 THEN RAISE EXCEPTION 'SEC_ADOPTED_CONTENT_INVALID' USING ERRCODE='23514'; END IF;
 IF NEW.source IN ('nexus_monitor_batch_inputs','nexus_monitor_increment_v2','nexus_research') THEN
  PERFORM public.sec_adopted_refs_complete(NEW.payload,NEW.observed_at);
 END IF;
 RETURN NEW;
END $$;
CREATE FUNCTION public.sec_adopted_content_immutable() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
BEGIN
 IF (TG_OP='UPDATE' AND (NEW.source='nexus_sec_graphic_adoption' OR NEW.schema_version='sec_monitor_filing_v3'))
 OR OLD.source='nexus_sec_graphic_adoption' OR OLD.schema_version='sec_monitor_filing_v3'
 OR (OLD.source='nexus_sec_uninterpreted_filing' AND OLD.revision='1e2b85a50497507f3cad8170e03de9422c35016768ddcf99b556d4a2ba7a6b65')
 OR (OLD.source IN ('sec_disclosure_index','sec_disclosure_listed_index','sec_durable_disclosure_index') AND OLD.revision='ed8d29fabc52df29b4e8237c4011999b5ee6eaa0393c25ca2494ffe9194a38cb')
 OR EXISTS(SELECT FROM public.data_snapshots c JOIN public.data_snapshots a ON a.id::text=c.payload->>'graphic_adoption_id'
  WHERE c.source='nexus_sec_monitor_filing' AND c.schema_version='sec_monitor_filing_v3'
  AND (a.payload#>>'{material,original_snapshot_id}'=OLD.id::text OR c.payload->>'index_snapshot_id'=OLD.id::text))
 THEN RAISE EXCEPTION 'SEC_ADOPTED_CONTENT_IMMUTABLE' USING ERRCODE='23514'; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
"""


def upgrade() -> None:
    op.execute(_ADOPTION)
    op.execute(_CONTENT)
    for statement in _REFS.split("\nCREATE FUNCTION "):
        if statement.strip():
            op.execute("CREATE FUNCTION " + statement)
    op.execute(
        "CREATE UNIQUE INDEX uq_sec_graphic_adoption_object ON data_snapshots(provider_object_id) WHERE source='nexus_sec_graphic_adoption'"
    )
    op.execute(
        "CREATE UNIQUE INDEX uq_sec_graphic_adoption_audit ON audit_events((payload->>'snapshot_id')) WHERE action='research.sec.graphic_adopted'"
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER sec_adopted_content_complete AFTER INSERT OR UPDATE ON data_snapshots DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION sec_adopted_content_complete()"
    )
    op.execute(
        "CREATE TRIGGER sec_adopted_content_immutable BEFORE UPDATE OR DELETE ON data_snapshots FOR EACH ROW EXECUTE FUNCTION sec_adopted_content_immutable()"
    )
    old = op.get_bind().scalar(
        text(
            "SELECT pg_get_functiondef('public.nexus_validate_daily_monitor_v2(uuid)'::regprocedure)"
        )
    )
    needle = "cs.schema_version='sec_monitor_filing_v1'"
    if not isinstance(old, str) or old.count(needle) != 1:
        raise RuntimeError("SEC_ADOPTED_PREDECESSOR_INVALID")
    op.execute("CREATE TABLE sec_content_guard_backup (definition text NOT NULL)")
    op.get_bind().execute(
        text("INSERT INTO sec_content_guard_backup VALUES (:definition)"), {"definition": old}
    )
    op.execute(
        old.replace(
            needle,
            "(cs.schema_version='sec_monitor_filing_v1' OR (cs.schema_version='sec_monitor_filing_v3' AND public.sec_adopted_content_valid(cs.id,s.observed_at)))",
        )
    )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE public.data_snapshots,public.audit_events IN SHARE ROW EXCLUSIVE MODE NOWAIT"
    )
    op.execute("""DO $$ BEGIN IF EXISTS(SELECT FROM public.data_snapshots WHERE schema_version='sec_monitor_filing_v3') THEN
      RAISE EXCEPTION 'SEC_ADOPTED_CONTENT_COMPATIBLE_ROLLBACK_REQUIRED' USING ERRCODE='23514'; END IF; END $$""")
    old = op.get_bind().scalar(text("SELECT definition FROM sec_content_guard_backup"))
    if not isinstance(old, str):
        raise RuntimeError("SEC_ADOPTED_PREDECESSOR_INVALID")
    op.execute(old)
    op.execute("DROP TABLE sec_content_guard_backup")
    op.execute("DROP TRIGGER sec_adopted_content_complete ON data_snapshots")
    op.execute("DROP TRIGGER sec_adopted_content_immutable ON data_snapshots")
    op.execute("DROP INDEX uq_sec_graphic_adoption_audit")
    op.execute("DROP INDEX uq_sec_graphic_adoption_object")
    for name in (
        "sec_adopted_content_immutable()",
        "sec_adopted_content_complete()",
        "sec_adopted_refs_complete(jsonb,timestamptz,integer)",
        "sec_adopted_content_valid(uuid,timestamptz)",
        "sec_graphic_adoption_valid(uuid,timestamptz)",
    ):
        op.execute("DROP FUNCTION public." + name)
