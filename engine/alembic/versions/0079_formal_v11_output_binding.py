"""Add exact formal v11 binding; retain frozen earlier identities."""

from alembic import op
from sqlalchemy import text

revision = "0079_formal_v11_output_binding"
down_revision = "0078_formal_v10_output_binding"
branch_labels = None
depends_on = None

_CHANGES = (
    (
        "run_version_ref IN ('codex-sdk-chatgpt-v8-stage-capacity', 'codex-sdk-chatgpt-v9-full-input-capacity', 'codex-sdk-chatgpt-v10-flexible-stage-budget')",
        "run_version_ref IN ('codex-sdk-chatgpt-v8-stage-capacity', 'codex-sdk-chatgpt-v9-full-input-capacity', 'codex-sdk-chatgpt-v10-flexible-stage-budget', 'codex-sdk-chatgpt-v11-bounded-stage-budget')",
    ),
    (
        "((model.version = 'codex-sdk-chatgpt-v8-stage-capacity' AND model.config_hash = 'e6a74f9855f93e2badec5640857b0f83fe22faba207911584caf2e71504ae0ac') OR (model.version = 'codex-sdk-chatgpt-v9-full-input-capacity' AND model.config_hash = 'a456380dd56c32aa4b90348e85430d253a5e87241c3f9c276723824d35588a4c') OR (model.version = 'codex-sdk-chatgpt-v10-flexible-stage-budget' AND model.config_hash = '7debe4c68c425cfa787b58c585365a90812ab1c8a453cb27f77dd959308bc7a4'))",
        "((model.version = 'codex-sdk-chatgpt-v8-stage-capacity' AND model.config_hash = 'e6a74f9855f93e2badec5640857b0f83fe22faba207911584caf2e71504ae0ac') OR (model.version = 'codex-sdk-chatgpt-v9-full-input-capacity' AND model.config_hash = 'a456380dd56c32aa4b90348e85430d253a5e87241c3f9c276723824d35588a4c') OR (model.version = 'codex-sdk-chatgpt-v10-flexible-stage-budget' AND model.config_hash = '7debe4c68c425cfa787b58c585365a90812ab1c8a453cb27f77dd959308bc7a4') OR (model.version = 'codex-sdk-chatgpt-v11-bounded-stage-budget' AND model.config_hash = 'cdf5e7d3b0cfd2a3e6fda267f74bc9b3d135f28e33f965431f17175b9f2a8720'))",
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
        text("""
    DO $migration$
    BEGIN
      IF EXISTS (SELECT 1 FROM public.agent_research_runs
                 WHERE pricing_version = 'codex-sdk-chatgpt-v11-bounded-stage-budget')
      THEN RAISE EXCEPTION 'V11_RUN_BINDINGS_EXIST'; END IF;
    END;
    $migration$;
    """)
    )
    for old, new in reversed(_CHANGES):
        _replace(new, old)
