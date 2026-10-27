"""Separate independent-monitor request identity; preserve all prior frozen rows."""

from alembic import op

revision = "0098_independent_monitor"
down_revision = "0097_monitor_daily_events"
branch_labels = None
depends_on = None


def manifest_guard(*, independent: bool) -> str:
    contract = "(r.contract_version<>2 OR r.mode<>'research_only')"
    purpose = "'research_only_v2'"
    extra = ""
    if independent:
        contract = "NOT ((r.contract_version=2 AND r.mode='research_only') OR (r.contract_version=3 AND r.mode='research_and_monitor'))"
        purpose = "CASE WHEN r.contract_version=3 THEN 'independent_monitor_research_v1' ELSE 'research_only_v2' END"
        extra = """ OR (r.contract_version=3 AND (
          s.source<>'nexus_independent_monitor_research' OR s.schema_version<>'independent_monitor_research_v1'
          OR s.payload->'independent_monitor_context'->>'request_id' IS DISTINCT FROM r.id::text
          OR s.payload->'independent_monitor_context'->>'request_hash' IS DISTINCT FROM r.request_hash
          OR s.payload->'independent_monitor_context'->>'account_id' IS DISTINCT FROM r.account_id::text
          OR s.payload->'independent_monitor_context'->>'batch_hash' IS DISTINCT FROM r.batch_hash
          OR a.actor_id<>'local-independent-monitor-research-v1'
          OR a.idempotency_key<>('independent-monitor:' || r.id::text || ':' || x.instrument_id::text)))"""
    return f"""CREATE OR REPLACE FUNCTION nexus_research_manifest_complete() RETURNS trigger LANGUAGE plpgsql AS $$
    DECLARE m research_request_manifests; r manual_paper_requests; n integer;
    BEGIN
      IF TG_TABLE_NAME='research_request_manifests' THEN
        SELECT * INTO m FROM research_request_manifests WHERE id=NEW.id;
      ELSE
        SELECT * INTO m FROM research_request_manifests WHERE id=NEW.manifest_id;
      END IF;
      SELECT * INTO r FROM manual_paper_requests WHERE id=m.request_id;
      SELECT count(*) INTO n FROM research_request_members WHERE manifest_id=m.id;
      IF {contract} OR m.research_as_of<r.requested_at
        OR m.research_as_of>clock_timestamp() OR n<>m.member_count
        OR (SELECT array_agg(instrument_id ORDER BY ordinal) FROM research_request_members WHERE manifest_id=m.id)
          IS DISTINCT FROM (SELECT candidate_instrument_ids FROM agent_paper_candidate_batches WHERE id=r.candidate_batch_id)
        OR EXISTS(
          SELECT 1 FROM research_request_members x
          JOIN agent_research_runs a ON a.id=x.run_id JOIN jobs j ON j.id=x.job_id
          JOIN data_snapshots s ON s.id=x.data_snapshot_id
          WHERE x.manifest_id=m.id AND (a.instrument_id<>x.instrument_id OR a.data_snapshot_id<>x.data_snapshot_id
            OR a.decision_time<>m.research_as_of OR a.model_config_id<>r.model_config_id OR a.model_hash<>r.model_hash
            OR s.payload->>'purpose' IS DISTINCT FROM ({purpose})
            OR j.payload<>jsonb_build_object('run_id',a.id::text) OR j.job_type<>'agent.paper.research'{extra}))
      THEN RAISE EXCEPTION 'RESEARCH_MANIFEST_INVALID'; END IF;
      RETURN NEW;
    END $$"""


def upgrade() -> None:
    op.drop_constraint("mode_valid", "manual_paper_requests", type_="check")
    op.drop_constraint("contract_version_valid", "manual_paper_requests", type_="check")
    op.create_check_constraint(
        "mode_valid",
        "manual_paper_requests",
        "mode IN ('research_only','research_and_paper','research_and_monitor')",
    )
    op.create_check_constraint(
        "contract_version_valid",
        "manual_paper_requests",
        "(contract_version=1 AND mode IN ('research_only','research_and_paper')) OR (contract_version=2 AND mode='research_only') OR (contract_version=3 AND mode='research_and_monitor')",
    )
    op.execute(manifest_guard(independent=True))
    replace_evidence(_OLD, _NEW)


def downgrade() -> None:
    op.execute(
        "DO $$ BEGIN IF EXISTS(SELECT 1 FROM manual_paper_requests WHERE contract_version=3) THEN RAISE EXCEPTION 'INDEPENDENT_MONITOR_DOWNGRADE_FORBIDDEN'; END IF; END $$"
    )
    replace_evidence(_NEW, _OLD)
    op.execute(manifest_guard(independent=False))
    op.drop_constraint("contract_version_valid", "manual_paper_requests", type_="check")
    op.drop_constraint("mode_valid", "manual_paper_requests", type_="check")
    op.create_check_constraint(
        "mode_valid", "manual_paper_requests", "mode IN ('research_only','research_and_paper')"
    )
    op.create_check_constraint(
        "contract_version_valid",
        "manual_paper_requests",
        "contract_version=1 OR (contract_version=2 AND mode='research_only')",
    )


_OLD = """              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""
_NEW = """              ) OR EXISTS (
                SELECT 1 FROM public.data_snapshots binding
                JOIN public.manual_paper_requests request
                  ON request.id::text=binding.payload->'independent_monitor_context'->>'request_id'
                JOIN public.data_snapshots original
                  ON original.id::text=binding.payload->'independent_monitor_context'->>'source_snapshot_id'
                WHERE NEW.source='nexus_independent_monitor_research'
                  AND binding.id=NEW.data_snapshot_id
                  AND binding.source=NEW.source
                  AND binding.schema_version='independent_monitor_research_v1'
                  AND binding.payload->>'purpose'=binding.schema_version
                  AND binding.provider_object_id=NEW.instrument_id::text
                  AND binding.payload->'independent_monitor_context'->>'instrument_id'=NEW.instrument_id::text
                  AND request.contract_version=3 AND request.mode='research_and_monitor'
                  AND binding.payload->'independent_monitor_context'->>'request_hash'=request.request_hash
                  AND binding.payload->'independent_monitor_context'->>'account_id'=request.account_id::text
                  AND binding.payload->'independent_monitor_context'->>'candidate_batch_id'=request.candidate_batch_id::text
                  AND binding.payload->'independent_monitor_context'->>'batch_hash'=request.batch_hash
                  AND binding.payload->'independent_monitor_context'->'execution_locked'='true'::jsonb
                  AND binding.payload->'independent_monitor_context'->'formal_consumption_allowed'='false'::jsonb
                  AND original.revision=binding.payload->'independent_monitor_context'->>'source_snapshot_hash'
                  AND original.provider_object_id=NEW.instrument_id::text
                  AND original.observed_at=binding.observed_at
                  AND original.created_at<=snapshot_time AND binding.created_at<=snapshot_time
                  AND binding.observed_at=NEW.available_at
                  AND binding.available_at=NEW.available_at AND binding.received_at=NEW.available_at
                  AND NEW.provider_object_id=request.id::text || ':' || NEW.instrument_id::text
                  AND NEW.revision=NEW.content_hash
                  AND NEW.location='memory://sha256/' || NEW.content_hash
              ) OR EXISTS (
                SELECT 1 FROM macro_observations"""


def replace_evidence(old: str, new: str) -> None:
    from sqlalchemy import text

    if op.get_context().as_sql:
        op.execute(f"""DO $migration$ DECLARE definition text;
        BEGIN SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure) INTO definition;
        IF (length(definition)-length(replace(definition,$old${old}$old$,'')))/length($old${old}$old$) <> 1
        THEN RAISE EXCEPTION 'INDEPENDENT_MONITOR_GUARD_UNEXPECTED'; END IF;
        EXECUTE replace(definition,$old${old}$old$,$new${new}$new$); END $migration$;""")
        return
    definition = op.get_bind().scalar(
        text(
            "SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
        )
    )
    if not isinstance(definition, str) or definition.count(old) != 1:
        raise RuntimeError("INDEPENDENT_MONITOR_GUARD_UNEXPECTED")
    op.get_bind().exec_driver_sql(definition.replace(old, new))
