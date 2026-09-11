"""Publish purpose-specific scenario spread cost evidence policy.

Revision ID: 0050_scenario_spread_cost_policy
Revises: 0049_pd_terminal_phases
"""

from __future__ import annotations

import json

from alembic import op
from sqlalchemy import text

revision: str = "0050_scenario_spread_cost_policy"
down_revision: str | None = "0049_pd_terminal_phases"
branch_labels: str | None = None
depends_on: str | None = None

POLICY_ID = "7733d99e-1594-5f1e-87ea-e5cc23575159"
POLICY_PAYLOAD = {
    "purpose": "portfolio_scenario_cost_analysis",
    "selection": "latest_qualified_before_valuation_cutoff",
    "fallback": "disabled",
    "session_scope": "xnys_regular",
    "max_quote_age_seconds": 45,
    "calculation": "sell_half_spread_from_snapshot_mark_v1",
    "calendar_authority": {
        "library": "exchange_calendars",
        "library_version": "4.13.2",
        "calendar": "XNYS",
        "session_window_contract": "xnys_regular_open_closed_v1",
    },
    "require_verified_evidence": True,
    "require_empty_quality_flags": True,
    "alpaca": {
        "feed": "iex",
        "delivery_mode": "real_time",
        "transport": "websocket",
        "artifact_schema": "alpaca_stock_stream_v1",
        "market_scope": "US",
        "event_kind": "quote",
    },
}


def _canonical_payload() -> str:
    return json.dumps(POLICY_PAYLOAD, sort_keys=True, separators=(",", ":"))


def upgrade() -> None:
    op.execute(
        text(
            "INSERT INTO source_policies (id,key,version,schema_version,payload) "
            "VALUES (CAST(:id AS uuid),'portfolio_scenario_cost_analysis',"
            "'scenario_spread_cost_policy_v1','1',CAST(:payload AS jsonb))"
        ).bindparams(id=POLICY_ID, payload=_canonical_payload())
    )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE public.idempotency_records, public.source_policies "
        "IN ACCESS EXCLUSIVE MODE"
    )
    referenced = op.get_bind().scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM idempotency_records "
            "WHERE route='/internal/v1/portfolio-scenarios/evaluate:"
            "portfolio_scenario_evaluation_v3' "
            "AND schema_version='portfolio_scenario_v3' "
            "AND response_payload->>'source_policy_id'=:policy_id)"
        ),
        {"policy_id": POLICY_ID},
    )
    if referenced:
        raise RuntimeError("scenario spread cost policy is referenced")

    op.execute("ALTER TABLE source_policies DISABLE TRIGGER trg_source_policies_immutable")
    deleted = op.get_bind().execute(
        text(
            "DELETE FROM source_policies WHERE id=CAST(:id AS uuid) "
            "AND key='portfolio_scenario_cost_analysis' "
            "AND version='scenario_spread_cost_policy_v1' "
            "AND schema_version='1' AND payload=CAST(:payload AS jsonb)"
        ),
        {"id": POLICY_ID, "payload": _canonical_payload()},
    )
    if deleted.rowcount != 1:
        raise RuntimeError("scenario spread cost policy downgrade mismatch")
    op.execute("ALTER TABLE source_policies ENABLE TRIGGER trg_source_policies_immutable")
