"""Recognize exact v2-to-compact presentation compatibility for natural obligations.

Old guards, source owners and independent request contexts remain immutable.
Only a natural obligation's own reservation may cover its original protected task.
"""

from alembic import op
from sqlalchemy import text

revision = "0119_compact_budget_obligations"
down_revision = "0118_compact_sol_profile"
branch_labels = None
depends_on = None


def _replace(signature: str, before: str, after: str, count: int = 1) -> None:
    definition = op.get_bind().scalar(
        text("SELECT pg_get_functiondef(to_regprocedure(:signature))"),
        {"signature": "public." + signature},
    )
    if not isinstance(definition, str) or definition.count(before) != count:
        raise RuntimeError("COMPACT_BUDGET_PREDECESSOR_INVALID")
    op.execute(definition.replace(before, after))


def _routes(*, forward: bool) -> None:
    replacements = [
        (
            "single_sol_increment_valid(date,uuid,uuid,uuid,uuid,numeric,uuid)",
            "r.model_config_id=model",
            "single_sol_budget_model_compatible(model,r.model_config_id,maximum)",
            1,
        ),
        (
            "single_sol_credit_valid()",
            "JOIN agent_model_configs m ON m.id=g.model_id\n            WHERE s.id=NEW.run_id",
            "JOIN agent_model_configs m ON m.id=s.model_config_id\n"
            "            WHERE single_sol_budget_model_compatible(g.model_id,m.id,g.maximum)\n"
            "            AND s.id=NEW.run_id",
            1,
        ),
        (
            "single_sol_credit_valid()",
            "JOIN agent_model_configs m ON m.id=g.model_id\n            WHERE r.id=NEW.run_id",
            "JOIN agent_model_configs m ON m.id=r.model_config_id\n"
            "            WHERE single_sol_budget_model_compatible(g.model_id,m.id,g.maximum)\n"
            "            AND r.id=NEW.run_id",
            1,
        ),
    ]
    for signature, before, after, count in replacements:
        _replace(signature, before if forward else after, after if forward else before, count)


def upgrade() -> None:
    op.execute("""
      CREATE FUNCTION single_sol_budget_model_compatible(expected uuid, candidate uuid, maximum numeric)
      RETURNS boolean LANGUAGE plpgsql STABLE SET search_path=pg_catalog,public AS $$
      DECLARE old_model agent_model_configs; new_model agent_model_configs;
      BEGIN
        -- Preserve the existing exact-identity rule, without widening other owners.
        IF expected=candidate THEN RETURN true; END IF;
        SELECT * INTO old_model FROM agent_model_configs WHERE id=expected;
        SELECT * INTO new_model FROM agent_model_configs WHERE id=candidate;
        RETURN COALESCE(
          nexus_sol_model_profile_valid(old_model) AND nexus_sol_model_profile_valid(new_model)
          AND NOT (old_model.platform_profile ? 'output_units_version')
          AND new_model.platform_profile->>'output_units_version'=
              'platform-compact-json-http-utf8-plus8192-v3'
          AND old_model.platform_profile=new_model.platform_profile-'output_units_version'
          AND (old_model.max_input_tokens*old_model.input_usd_per_million+
               old_model.max_output_tokens*old_model.output_usd_per_million)/1000000=maximum
          AND (new_model.max_input_tokens*new_model.input_usd_per_million+
               new_model.max_output_tokens*new_model.output_usd_per_million)/1000000=maximum,
          false);
      END $$
    """)
    _routes(forward=True)


def downgrade() -> None:
    # Prevent a concurrent provisional credit from committing after the read.
    op.execute("LOCK TABLE single_sol_budget_credits IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    op.execute("""DO $$ BEGIN
      IF EXISTS (
        SELECT FROM single_sol_budget_credits c
        JOIN single_sol_budget_scopes g ON g.budget_day=c.budget_day
        LEFT JOIN agent_research_runs r ON r.id=c.run_id AND c.task<>'synthesis'
        LEFT JOIN agent_portfolio_synthesis_runs s ON s.id=c.run_id AND c.task='synthesis'
        WHERE COALESCE(r.model_config_id,s.model_config_id) IS DISTINCT FROM g.model_id
      ) THEN RAISE EXCEPTION 'COMPACT_BUDGET_DOWNGRADE_FORBIDDEN'; END IF;
    END $$""")
    _routes(forward=False)
    op.execute("DROP FUNCTION single_sol_budget_model_compatible(uuid,uuid,numeric)")
