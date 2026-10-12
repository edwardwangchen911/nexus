"""Bind isolated v9 outputs without enabling a business version or changing history."""

from alembic import op
from sqlalchemy import text

revision = "0076_isolated_v9_output_binding"
down_revision = "0075_manual_paper_requests"
branch_labels = None
depends_on = None

_V7 = "codex-sdk-chatgpt-v7-stage-capacity-candidate"
_V9 = "codex-sdk-chatgpt-v9-full-input-capacity"
_V7_HASH = "175ef8cf95fbafb050f89b4bb7f16aae278c8c9b99ee25c60cf35f044688141f"
_V9_HASH = "a456380dd56c32aa4b90348e85430d253a5e87241c3f9c276723824d35588a4c"
# Frozen fragments of the existing isolated branch. The database-name, cycle,
# event, Run/model, time, lock, budget and output guards stay in place.
_CHANGES = (
    (f"run_version_ref = '{_V7}'", f"run_version_ref IN ('{_V7}', '{_V9}')"),
    (f"model.version = '{_V7}'", "model.version = run_version_ref"),
    (
        f"model.config_hash =\n                       '{_V7_HASH}'",
        f"((model.version = '{_V7}' AND model.config_hash = '{_V7_HASH}')"
        f" OR (model.version = '{_V9}' AND model.config_hash = '{_V9_HASH}'))",
    ),
)


def _replace(old: str, new: str) -> None:
    statement = f"""
    DO $migration$
    DECLARE definition text;
      old_fragment text := $old${old}$old$;
      new_fragment text := $new${new}$new$;
    BEGIN
      SELECT pg_get_functiondef('public.nexus_agent_output_binding_valid()'::regprocedure)
        INTO definition;
      IF definition IS NULL OR
        (length(definition) - length(replace(definition, old_fragment, '')))
          / length(old_fragment) <> 1 THEN
        RAISE EXCEPTION 'AGENT_OUTPUT_VERSION_GUARD_UNEXPECTED';
      END IF;
      EXECUTE replace(definition, old_fragment, new_fragment);
    END;
    $migration$;
    """
    op.execute(text(statement))


def upgrade() -> None:
    for old, new in _CHANGES:
        _replace(old, new)


def downgrade() -> None:
    op.execute(
        text(f"""
    DO $migration$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM public.strategy_decision_agent_outputs binding
        JOIN public.agent_research_outputs output ON output.id = binding.agent_research_output_id
        JOIN public.agent_research_runs run ON run.id = output.run_id
        WHERE run.pricing_version = '{_V9}'
      ) THEN RAISE EXCEPTION 'V9_OUTPUT_BINDINGS_EXIST'; END IF;
    END;
    $migration$;
    """)
    )
    for old, new in reversed(_CHANGES):
        _replace(new, old)
