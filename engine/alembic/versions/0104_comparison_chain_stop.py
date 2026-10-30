"""Persist per-chain experiment stops without clearing global safety fences."""

from alembic import op
from sqlalchemy import text

revision = "0104_comparison_chain_stop"
down_revision = "0103_comparison_claim"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""CREATE TABLE research_comparison_chain_stop (
      run_id uuid PRIMARY KEY REFERENCES research_comparison_chain(run_id),
      scope_id uuid NOT NULL REFERENCES research_stage_scope(id),
      stopped_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )""")
    op.execute("""CREATE FUNCTION research_comparison_chain_stop_guard() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        PERFORM 1 FROM research_stage_scope WHERE id=NEW.scope_id FOR UPDATE;
        IF NOT EXISTS(SELECT 1 FROM research_stage_member m
          JOIN research_stage_route r USING(run_id,job_id,scope_id)
          JOIN research_comparison_chain c USING(run_id,job_id)
          JOIN research_stage_scope s ON s.id=m.scope_id
          WHERE m.run_id=NEW.run_id AND m.scope_id=NEW.scope_id AND r.kind='comparison'
            AND s.comparison_id=c.experiment_id AND s.id=c.experiment_id)
        THEN RAISE EXCEPTION 'COMPARISON_CHAIN_STOP_BINDING_INVALID'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE TRIGGER research_comparison_chain_stop_binding BEFORE INSERT
      ON research_comparison_chain_stop FOR EACH ROW
      EXECUTE FUNCTION research_comparison_chain_stop_guard()""")
    op.execute("""CREATE TRIGGER research_comparison_chain_stop_immutable BEFORE UPDATE OR DELETE
      ON research_comparison_chain_stop FOR EACH ROW EXECUTE FUNCTION research_stage_immutable()""")
    op.execute("""CREATE FUNCTION research_comparison_stage_fence() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        PERFORM 1 FROM research_stage_scope s JOIN research_stage_member m ON m.scope_id=s.id
          WHERE m.run_id=NEW.run_id FOR UPDATE OF s;
        IF EXISTS(SELECT 1 FROM research_comparison_chain_stop WHERE run_id=NEW.run_id)
        THEN RAISE EXCEPTION 'COMPARISON_CHAIN_STOPPED'; END IF;
        RETURN NEW;
      END $$""")
    op.execute("""CREATE TRIGGER research_comparison_stage_fence BEFORE INSERT
      ON research_stage_authorization FOR EACH ROW EXECUTE FUNCTION research_comparison_stage_fence()""")


def downgrade() -> None:
    op.execute("LOCK TABLE research_comparison_chain_stop,jobs IN ACCESS EXCLUSIVE MODE NOWAIT")
    if op.get_bind().scalar(
        text("""SELECT EXISTS(SELECT 1 FROM research_comparison_chain_stop)
      OR EXISTS(SELECT 1 FROM jobs WHERE job_type='agent.research.comparison' AND attempt_count>0)""")
    ):
        raise RuntimeError("COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED")
    op.execute("DROP TRIGGER research_comparison_stage_fence ON research_stage_authorization")
    op.execute("DROP FUNCTION research_comparison_stage_fence()")
    op.execute("DROP TABLE research_comparison_chain_stop")
    op.execute("DROP FUNCTION research_comparison_chain_stop_guard()")
