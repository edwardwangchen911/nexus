"""Preserve unknown Strategy R&D usage without fabricating zero counters.

Revision ID: 0036_strategy_rnd_usage_unknown
Revises: 0035_issuer_research_evidence
"""

from alembic import op
from sqlalchemy import text

revision = "0036_strategy_rnd_usage_unknown"
down_revision = "0035_issuer_research_evidence"
branch_labels = None
depends_on = None

_TABLE = "strategy_rnd_runs"
_CONSTRAINT = "ck_strategy_rnd_runs_usage"
_KNOWN_USAGE = (
    "input_tokens IS NOT NULL AND output_tokens IS NOT NULL "
    "AND tool_call_count IS NOT NULL AND input_tokens >= 0 "
    "AND output_tokens >= 0 AND tool_call_count >= 0"
)
_UNKNOWN_USAGE = (
    "input_tokens IS NULL AND output_tokens IS NULL "
    "AND tool_call_count IS NULL AND status = 'failed'"
)
_LEGACY_USAGE = "tool_call_count BETWEEN 0 AND 4 AND input_tokens >= 0 AND output_tokens >= 0"


def upgrade() -> None:
    op.execute(f"LOCK TABLE public.{_TABLE} IN ACCESS EXCLUSIVE MODE")
    op.drop_constraint(op.f(_CONSTRAINT), _TABLE, type_="check")
    for column in ("input_tokens", "output_tokens", "tool_call_count"):
        op.alter_column(_TABLE, column, nullable=True)
    op.create_check_constraint(
        op.f(_CONSTRAINT),
        _TABLE,
        f"({_KNOWN_USAGE}) OR ({_UNKNOWN_USAGE})",
    )


def downgrade() -> None:
    connection = op.get_bind()
    op.execute(f"LOCK TABLE public.{_TABLE} IN ACCESS EXCLUSIVE MODE")
    has_unknown = connection.scalar(
        text(
            f"SELECT EXISTS (SELECT 1 FROM public.{_TABLE} "
            "WHERE input_tokens IS NULL OR output_tokens IS NULL OR tool_call_count IS NULL)"
        )
    )
    if has_unknown:
        raise RuntimeError("unknown Strategy R&D usage prevents downgrade")
    legacy_incompatible = connection.scalar(
        text(f"SELECT EXISTS (SELECT 1 FROM public.{_TABLE} WHERE NOT ({_LEGACY_USAGE}))")
    )
    if legacy_incompatible:
        raise RuntimeError("legacy-incompatible Strategy R&D usage prevents downgrade")
    op.drop_constraint(op.f(_CONSTRAINT), _TABLE, type_="check")
    for column in ("input_tokens", "output_tokens", "tool_call_count"):
        op.alter_column(_TABLE, column, nullable=False)
    op.create_check_constraint(op.f(_CONSTRAINT), _TABLE, _LEGACY_USAGE)
