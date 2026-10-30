"""Immutable finite comparison registry; dispatch remains disabled until stage integration.

Same database and native budgets. No business identity is fabricated. Populated
storage requires a compatible reader rollback; downgrade never deletes evidence.
"""

from alembic import op
from sqlalchemy import text

revision = "0100_research_comparison"
down_revision = "0099_research_stages"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE research_comparison_manifest (
        id uuid PRIMARY KEY,
        purpose text NOT NULL UNIQUE CHECK(purpose='direct-responses-terra-v11-20261030'),
        manifest_hash text NOT NULL CHECK(manifest_hash ~ '^[0-9a-f]{64}$'),
        payload jsonb NOT NULL CHECK(jsonb_typeof(payload)='object'),
        created_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )""")
    op.execute("""CREATE TABLE research_comparison_chain (
        experiment_id uuid NOT NULL REFERENCES research_comparison_manifest(id),
        ordinal integer NOT NULL CHECK(ordinal BETWEEN 1 AND 12),
        run_id uuid NOT NULL UNIQUE REFERENCES agent_research_runs(id),
        job_id uuid NOT NULL UNIQUE REFERENCES jobs(id),
        reserved_id uuid NOT NULL UNIQUE REFERENCES agent_daily_budget_reservations(id),
        input_hash text NOT NULL CHECK(input_hash ~ '^[0-9a-f]{64}$'),
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY(experiment_id,ordinal)
    )""")
    for table in ("research_comparison_manifest", "research_comparison_chain"):
        op.execute(f"""CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table}
          FOR EACH ROW EXECUTE FUNCTION research_stage_immutable()""")
    # No Worker can turn this preparatory registry into model execution. The
    # reviewed native scope/authorization extension must replace this gate first.
    op.execute("""CREATE FUNCTION research_comparison_claim_guard() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.job_type='agent.research.comparison' AND
          (NEW.status='running' OR NEW.attempt_count>0) THEN
          RAISE EXCEPTION 'COMPARISON_DISPATCH_NOT_ENABLED';
        END IF;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE TRIGGER research_comparison_claim_guard BEFORE INSERT OR UPDATE ON jobs
      FOR EACH ROW EXECUTE FUNCTION research_comparison_claim_guard()""")


def downgrade() -> None:
    if op.get_bind().scalar(text("SELECT EXISTS(SELECT 1 FROM research_comparison_manifest)")):
        raise RuntimeError("COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    op.execute("DROP TRIGGER research_comparison_claim_guard ON jobs")
    op.execute("DROP FUNCTION research_comparison_claim_guard()")
    op.execute("DROP TABLE research_comparison_chain")
    op.execute("DROP TABLE research_comparison_manifest")
