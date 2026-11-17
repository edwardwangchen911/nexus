"""Admit exact durable SEC baselines without changing legacy baseline semantics.

Only the two source predicates change. This grants no rule, research or once
permission. Original function definitions/hashes are retained for safe rollback.
"""

from alembic import op

revision = "0128_sec_durable_baseline"
down_revision = "0127_sec_research_bridge"
branch_labels = None
depends_on = None

_PLAN_OLD = "AND b.source='sec_disclosure_index' AND b.schema_version='sec_disclosure_index_v1'"
_PLAN_NEW = """AND ((b.source='sec_disclosure_index' AND b.schema_version='sec_disclosure_index_v1') OR
              (b.source='sec_durable_disclosure_index' AND b.schema_version='sec_durable_index_v1'
               AND b.payload->>'schema_version'='sec_durable_index_v1'
               AND b.revision=encode(sha256(convert_to(public.sec_positive_chain_canonical(b.payload),'UTF8')),'hex')
               AND b.payload#>>'{index,instrument_id}'=NEW.instrument_id::text
               AND EXISTS(SELECT FROM public.agent_research_outputs o
                 WHERE o.id=NEW.source_output_id AND o.run_id=NEW.source_run_id
                   AND o.status='completed' AND o.role IS NULL AND o.content_hash=NEW.source_hash
                   AND o.intent#>>'{disclosure_proposal,issuer_cik}'=b.payload#>>'{index,issuer_cik}'
                   AND o.intent#>>'{disclosure_proposal,baseline_snapshot_id}'=b.id::text
                   AND o.intent#>>'{disclosure_proposal,baseline_hash}'=b.revision
                   AND (o.intent#>>'{disclosure_proposal,baseline_as_of}')::timestamptz=NEW.baseline_as_of)))"""
_MIXED_OLD = "AND bs.source='sec_disclosure_index' AND bs.schema_version='sec_disclosure_index_v1'"
_MIXED_NEW = """AND ((bs.source='sec_disclosure_index' AND bs.schema_version='sec_disclosure_index_v1') OR
          (bs.source='sec_durable_disclosure_index' AND bs.schema_version='sec_durable_index_v1'
           AND bs.payload->>'schema_version'='sec_durable_index_v1'
           AND bs.revision=encode(sha256(convert_to(public.sec_positive_chain_canonical(bs.payload),'UTF8')),'hex')
           AND bs.payload#>>'{index,instrument_id}'=b.instrument_id::text
           AND bs.payload#>>'{index,issuer_cik}'=c#>>'{disclosure_monitoring,baseline,issuer_cik}'))"""
_PATCHES = (
    (
        "public.nexus_monitor_disclosure_baseline_guard()",
        "76da2deaf9e2da22ecae893813b3c3f9d1d674f42a28dd8734732e9de4267e59",
        _PLAN_OLD,
        _PLAN_NEW,
    ),
    (
        "public.nexus_monitor_validate_mixed_members(uuid)",
        "20b8dfbffd6df031050298ade1c78af547155cc1957e32cd6d75ce21bdc6d60e",
        _MIXED_OLD,
        _MIXED_NEW,
    ),
)


def _patch_sql(signature: str, expected_hash: str, before: str, after: str) -> str:
    return f"""DO $migration$ DECLARE d text; patched text; BEGIN
      SELECT pg_get_functiondef(to_regprocedure('{signature}')) INTO d;
      IF d IS NULL OR encode(sha256(convert_to(d,'UTF8')),'hex')<>'{expected_hash}'
       OR (length(d)-length(replace(d,$before${before}$before$,'')))/length($before${before}$before$)<>1
      THEN RAISE EXCEPTION 'SEC_DURABLE_BASELINE_PREDECESSOR_INVALID' USING ERRCODE='23514'; END IF;
      patched:=replace(d,$before${before}$before$,$after${after}$after$);
      EXECUTE patched;
      INSERT INTO public.sec_durable_baseline_function_backups(signature,definition,original_hash,upgraded_hash)
      SELECT '{signature}',d,'{expected_hash}',encode(sha256(convert_to(pg_get_functiondef(to_regprocedure('{signature}')),'UTF8')),'hex');
    END $migration$"""


UPGRADE_SQL = (
    "LOCK TABLE public.research_monitor_plans,public.data_snapshots IN SHARE ROW EXCLUSIVE MODE NOWAIT",
    """CREATE TABLE public.sec_durable_baseline_function_backups(
       signature text PRIMARY KEY,definition text NOT NULL,
       original_hash varchar(64) NOT NULL CHECK(original_hash ~ '^[0-9a-f]{64}$'),
       upgraded_hash varchar(64) NOT NULL CHECK(upgraded_hash ~ '^[0-9a-f]{64}$'))""",
    *tuple(_patch_sql(*x) for x in _PATCHES),
    """CREATE TRIGGER sec_durable_baseline_backup_immutable BEFORE UPDATE OR DELETE
       ON public.sec_durable_baseline_function_backups FOR EACH ROW
       EXECUTE FUNCTION public.nexus_monitor_immutable()""",
)

DOWNGRADE_SQL = (
    """LOCK TABLE public.research_monitor_plans,public.data_snapshots,
       public.research_monitor_increments,public.sec_event_research_bindings,
       public.sec_durable_baseline_function_backups IN SHARE ROW EXCLUSIVE MODE NOWAIT""",
    """DO $$ BEGIN
      IF EXISTS(SELECT FROM public.research_monitor_plans p JOIN public.data_snapshots s
        ON s.id=p.baseline_snapshot_id WHERE s.source='sec_durable_disclosure_index')
      OR EXISTS(SELECT FROM public.sec_event_research_bindings b JOIN public.data_snapshots s
        ON s.id=b.capture_id WHERE s.source='sec_durable_disclosure_index')
      OR EXISTS(SELECT FROM public.data_snapshots frozen JOIN public.data_snapshots cap
        ON cap.source='sec_durable_disclosure_index'
        WHERE frozen.source IN ('nexus_monitor_batch_inputs','nexus_monitor_increment_v2',
          'nexus_price_monitor_increment','nexus_independent_monitor_research','nexus_agent_paper_context')
         AND jsonb_path_exists(frozen.payload,'$.** ? (@ == $capture)',jsonb_build_object('capture',cap.id::text)))
      THEN RAISE EXCEPTION 'SEC_DURABLE_BASELINE_DOWNGRADE_FORBIDDEN' USING ERRCODE='23514'; END IF;
    END $$""",
    """DO $migration$ DECLARE r record; d text; BEGIN
      IF (SELECT count(*) FROM public.sec_durable_baseline_function_backups)<>2
      THEN RAISE EXCEPTION 'SEC_DURABLE_BASELINE_BACKUP_INVALID' USING ERRCODE='23514'; END IF;
      FOR r IN SELECT * FROM public.sec_durable_baseline_function_backups ORDER BY signature LOOP
        SELECT pg_get_functiondef(to_regprocedure(r.signature)) INTO d;
        IF d IS NULL OR encode(sha256(convert_to(d,'UTF8')),'hex')<>r.upgraded_hash
          OR encode(sha256(convert_to(r.definition,'UTF8')),'hex')<>r.original_hash
        THEN RAISE EXCEPTION 'SEC_DURABLE_BASELINE_BACKUP_INVALID' USING ERRCODE='23514'; END IF;
        EXECUTE r.definition;
      END LOOP;
    END $migration$""",
    "DROP TABLE public.sec_durable_baseline_function_backups",
)


def upgrade() -> None:
    for statement in UPGRADE_SQL:
        op.execute(statement)


def downgrade() -> None:
    for statement in DOWNGRADE_SQL:
        op.execute(statement)
