"""Permanently claim one v6-root increment across every frozen mixed parent."""

from alembic import op

revision = "0123_monitor_increment_once"
down_revision = "0122_single_monitor_once"
branch_labels = None
depends_on = None

# Missing legacy materials have no secondary parents. Malformed identifiers fail
# closed at the UUID cast; ordinary snapshot validation remains authoritative.
_PARENTS = """SELECT primary_plan AS id
          UNION
          SELECT (member->>'plan_id')::uuid
          FROM public.data_snapshots snapshot,
            LATERAL jsonb_array_elements(CASE WHEN
              jsonb_typeof(snapshot.payload#>'{monitor_context,materials,parents}')='array'
              THEN snapshot.payload#>'{monitor_context,materials,parents}' ELSE '[]'::jsonb END
            ) member
          WHERE snapshot.id=frozen_snapshot"""


def upgrade() -> None:
    op.execute("""CREATE TABLE public.single_monitor_increment_claims (
      root_request_id uuid PRIMARY KEY REFERENCES public.manual_paper_requests(id),
      run_id uuid NOT NULL UNIQUE REFERENCES public.agent_research_runs(id)
    )""")
    op.execute("""CREATE TRIGGER single_monitor_increment_claims_immutable
      BEFORE UPDATE OR DELETE ON public.single_monitor_increment_claims
      FOR EACH ROW EXECUTE FUNCTION public.nexus_monitor_immutable()""")
    op.execute("""CREATE FUNCTION public.single_monitor_root_request(source_run uuid)
      RETURNS uuid LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$
      DECLARE roots uuid[];
      BEGIN
        WITH RECURSIVE ancestry(run_id,account_id,instrument_id) AS (
          SELECT r.id,NULL::uuid,r.instrument_id
          FROM public.agent_research_runs r WHERE r.id=source_run
          UNION
          SELECT p.source_run_id,p.account_id,p.instrument_id
          FROM ancestry a
          JOIN public.research_monitor_increments i ON i.run_id=a.run_id
          JOIN public.agent_research_runs child ON child.id=i.run_id
            AND child.instrument_id=i.instrument_id AND child.data_snapshot_id=i.snapshot_id
          JOIN public.data_snapshots snapshot ON snapshot.id=i.snapshot_id
          CROSS JOIN LATERAL (
            SELECT i.plan_id AS id
            UNION
            SELECT (member->>'plan_id')::uuid
            FROM jsonb_array_elements(CASE WHEN
              jsonb_typeof(snapshot.payload#>'{monitor_context,materials,parents}')='array'
              THEN snapshot.payload#>'{monitor_context,materials,parents}' ELSE '[]'::jsonb END
            ) member
          ) links
          JOIN public.research_monitor_plans p ON p.id=links.id
            AND p.account_id=i.account_id AND p.instrument_id=i.instrument_id
          JOIN public.agent_research_runs parent ON parent.id=p.source_run_id
            AND parent.instrument_id=p.instrument_id
          WHERE a.instrument_id=i.instrument_id
            AND (a.account_id IS NULL OR a.account_id=i.account_id)
        )
        SELECT array_agg(DISTINCT q.id) INTO roots
        FROM ancestry a
        JOIN public.research_request_members m ON m.run_id=a.run_id
          AND m.instrument_id=a.instrument_id
        JOIN public.research_request_manifests f ON f.id=m.manifest_id
        JOIN public.manual_paper_requests q ON q.id=f.request_id
        JOIN public.agent_research_runs r ON r.id=m.run_id
          AND r.instrument_id=m.instrument_id AND r.data_snapshot_id=m.data_snapshot_id
        WHERE q.contract_version=6 AND q.mode='research_and_monitor'
          AND (a.account_id IS NULL OR a.account_id=q.account_id)
          AND m.instrument_id::text=q.research_profile#>>'{single_sol,instrument_id}'
          AND r.actor_id='local-single-monitor-sol-v1'
          AND r.idempotency_key='single-monitor-sol:'||q.id::text||':'||m.instrument_id::text;
        IF cardinality(roots)>1 THEN
          RAISE EXCEPTION 'SINGLE_MONITOR_INCREMENT_ROOT_INVALID' USING ERRCODE='23514';
        END IF;
        RETURN roots[1];
      END $$""")
    op.execute(f"""CREATE FUNCTION public.single_monitor_increment_roots(
        primary_plan uuid,frozen_snapshot uuid) RETURNS uuid[]
      LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$
      DECLARE roots uuid[];
      BEGIN
        SELECT array_agg(DISTINCT root.id) FILTER(WHERE root.id IS NOT NULL) INTO roots
        FROM ({_PARENTS}) links
        JOIN public.research_monitor_plans primary_owner ON primary_owner.id=primary_plan
        JOIN public.research_monitor_plans p ON p.id=links.id
          AND p.account_id=primary_owner.account_id
          AND p.instrument_id=primary_owner.instrument_id
        CROSS JOIN LATERAL (
          SELECT public.single_monitor_root_request(p.source_run_id) AS id
        ) root;
        roots:=COALESCE(roots,ARRAY[]::uuid[]);
        IF cardinality(roots)>1 THEN
          RAISE EXCEPTION 'SINGLE_MONITOR_INCREMENT_ROOT_INVALID' USING ERRCODE='23514';
        END IF;
        RETURN roots;
      END $$""")
    op.execute("""CREATE FUNCTION public.single_monitor_increment_once_guard()
      RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
      DECLARE parent public.research_monitor_plans; roots uuid[]; root uuid;
      BEGIN
        SELECT * INTO parent FROM public.research_monitor_plans WHERE id=NEW.plan_id;
        roots:=public.single_monitor_increment_roots(NEW.plan_id,NEW.snapshot_id);
        IF cardinality(roots)=0 THEN RETURN NEW; END IF;
        root:=roots[1];
        IF NEW.account_id IS DISTINCT FROM parent.account_id
          OR NEW.instrument_id IS DISTINCT FROM parent.instrument_id
          OR NOT EXISTS (SELECT FROM public.manual_paper_requests q WHERE q.id=root
            AND q.account_id=NEW.account_id
            AND q.research_profile#>>'{single_sol,instrument_id}'=NEW.instrument_id::text)
        THEN RAISE EXCEPTION 'SINGLE_MONITOR_INCREMENT_ROOT_INVALID' USING ERRCODE='23514';
        END IF;
        BEGIN
          INSERT INTO public.single_monitor_increment_claims(root_request_id,run_id)
          VALUES(root,NEW.run_id);
        EXCEPTION WHEN unique_violation THEN
          RAISE EXCEPTION 'SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED' USING ERRCODE='23514';
        END;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE TRIGGER single_monitor_increment_once
      BEFORE INSERT ON public.research_monitor_increments FOR EACH ROW
      EXECUTE FUNCTION public.single_monitor_increment_once_guard()""")
    op.execute("""CREATE FUNCTION public.single_monitor_increment_claim_valid()
      RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,public AS $$
      BEGIN
        IF NOT EXISTS (
          SELECT FROM public.research_monitor_increments i
          JOIN public.research_monitor_plans p ON p.id=i.plan_id
            AND p.account_id=i.account_id AND p.instrument_id=i.instrument_id
          JOIN public.manual_paper_requests q ON q.id=NEW.root_request_id
            AND q.account_id=i.account_id
            AND q.research_profile#>>'{single_sol,instrument_id}'=i.instrument_id::text
          WHERE i.run_id=NEW.run_id AND NEW.root_request_id=ANY(
            public.single_monitor_increment_roots(i.plan_id,i.snapshot_id))
        ) THEN RAISE EXCEPTION 'SINGLE_MONITOR_INCREMENT_CLAIM_INVALID' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE CONSTRAINT TRIGGER single_monitor_increment_claim_complete
      AFTER INSERT ON public.single_monitor_increment_claims DEFERRABLE INITIALLY DEFERRED
      FOR EACH ROW EXECUTE FUNCTION public.single_monitor_increment_claim_valid()""")
    # Backfill every frozen parent, with no grandfathering of multiple increments.
    op.execute("""INSERT INTO public.single_monitor_increment_claims(root_request_id,run_id)
      SELECT roots.id,i.run_id FROM public.research_monitor_increments i
      CROSS JOIN LATERAL unnest(
        public.single_monitor_increment_roots(i.plan_id,i.snapshot_id)) roots(id)""")


def downgrade() -> None:
    op.execute("""LOCK TABLE public.manual_paper_requests,public.research_monitor_increments,
      public.single_monitor_increment_claims IN SHARE ROW EXCLUSIVE MODE NOWAIT""")
    op.execute("""DO $$ BEGIN
      IF EXISTS(SELECT FROM public.single_monitor_increment_claims)
        OR EXISTS(SELECT FROM public.manual_paper_requests WHERE contract_version=6)
      THEN RAISE EXCEPTION 'SINGLE_MONITOR_INCREMENT_DOWNGRADE_FORBIDDEN'; END IF;
    END $$""")
    op.execute("DROP TRIGGER single_monitor_increment_once ON public.research_monitor_increments")
    op.execute("DROP TABLE public.single_monitor_increment_claims")
    op.execute("DROP FUNCTION public.single_monitor_increment_claim_valid()")
    op.execute("DROP FUNCTION public.single_monitor_increment_once_guard()")
    op.execute("DROP FUNCTION public.single_monitor_increment_roots(uuid,uuid)")
    op.execute("DROP FUNCTION public.single_monitor_root_request(uuid)")
