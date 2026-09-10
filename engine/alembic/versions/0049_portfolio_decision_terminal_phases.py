"""Allow portfolio decision abstention at its actual failure phase.

Revision ID: 0049_pd_terminal_phases
Revises: 0048_portfolio_decision_runs
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

revision: str = "0049_pd_terminal_phases"
down_revision: str | None = "0048_portfolio_decision_runs"
branch_labels: str | None = None
depends_on: str | None = None


_FUNCTION_PREFIX = """
CREATE OR REPLACE FUNCTION nexus_validate_portfolio_decision_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW) - ARRAY['status','provider_phase','tool_result_hash',
      'tool_projection','settled_cost_usd','input_tokens','output_tokens'])
     IS DISTINCT FROM
     (to_jsonb(OLD) - ARRAY['status','provider_phase','tool_result_hash',
      'tool_projection','settled_cost_usd','input_tokens','output_tokens']) THEN
    RAISE EXCEPTION 'portfolio decision authority is immutable' USING ERRCODE = '23514';
  END IF;
  IF OLD.status IN ('completed','abstained','failed') THEN
    RAISE EXCEPTION 'terminal portfolio decision is immutable' USING ERRCODE = '23514';
  END IF;
  IF NEW.provider_phase <> OLD.provider_phase AND NOT (
    (OLD.provider_phase = 'none' AND NEW.provider_phase = 'first_requested') OR
    (OLD.provider_phase = 'first_requested' AND NEW.provider_phase = 'tool_recorded') OR
    (OLD.provider_phase = 'tool_recorded' AND NEW.provider_phase = 'second_requested')
  ) THEN
    RAISE EXCEPTION 'invalid portfolio decision provider phase' USING ERRCODE = '23514';
  END IF;
  IF NEW.status <> OLD.status AND NOT (
    (OLD.status = 'prepared' AND NEW.status IN ('running','failed')) OR
    (OLD.status = 'running' AND NEW.status IN ('completed','abstained','failed'))
  ) THEN
    RAISE EXCEPTION 'invalid portfolio decision transition' USING ERRCODE = '23514';
  END IF;
  IF OLD.status = 'prepared' AND NEW.status = 'failed'
     AND NEW.provider_phase <> 'none' THEN
    RAISE EXCEPTION 'prepared failure cannot follow provider request' USING ERRCODE = '23514';
  END IF;
"""


def upgrade() -> None:
    op.execute(
        _FUNCTION_PREFIX
        + """
  IF NEW.status = 'completed' AND NEW.provider_phase <> 'second_requested' THEN
    RAISE EXCEPTION 'completed portfolio decision requires second turn'
      USING ERRCODE = '23514';
  END IF;
  IF NEW.status = 'abstained'
     AND NEW.provider_phase NOT IN ('first_requested','tool_recorded','second_requested') THEN
    RAISE EXCEPTION 'abstained portfolio decision requires a provider phase'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$
"""
    )


def downgrade() -> None:
    op.execute("LOCK TABLE portfolio_decision_runs IN ACCESS EXCLUSIVE MODE")
    connection = op.get_bind()
    if connection.scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM portfolio_decision_runs "
            "WHERE status = 'abstained' AND provider_phase <> 'second_requested')"
        )
    ):
        raise RuntimeError("early portfolio decision abstentions prevent downgrade")
    op.execute(
        _FUNCTION_PREFIX
        + """
  IF NEW.status IN ('completed','abstained')
     AND NEW.provider_phase <> 'second_requested' THEN
    RAISE EXCEPTION 'portfolio decision terminal output requires second turn'
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$
"""
    )
