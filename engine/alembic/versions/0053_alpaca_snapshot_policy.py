"""Admit scheduled Alpaca IEX REST snapshots for display-only reads.

Revision ID: 0053_alpaca_snapshot_policy
Revises: 0052_fosun_classification
"""

import json

from alembic import op
from sqlalchemy import text

revision = "0053_alpaca_snapshot_policy"
down_revision = "0052_fosun_classification"
branch_labels = None
depends_on = None

_POLICY_ID = "a9058fcb-38bb-5c2f-9455-ff40cb029e90"
_POLICY_PAYLOAD = {
    "selection": "latest_available",
    "fosun": {
        "feed": "l1",
        "delivery_modes": ["unknown", "real_time", "delayed"],
        "artifact_schema": "fosun_gateway_market_snapshot_v1",
        "market_scope": "US",
    },
    "alpaca": {
        "feed": "iex",
        "delivery_mode": "real_time",
        "artifact_schemas": [
            "alpaca_stock_stream_v1",
            "alpaca_stock_snapshots_v1",
        ],
        "market_scope": "US",
    },
    "purpose": "display_only_intraday_monitor",
}


def upgrade() -> None:
    op.execute(
        text(
            "INSERT INTO source_policies (id,key,version,schema_version,payload) "
            "VALUES (CAST(:id AS uuid),'intraday_monitor','intraday_monitor_policy_v4','4',"
            "CAST(:payload AS jsonb))"
        ).bindparams(
            id=_POLICY_ID,
            payload=json.dumps(_POLICY_PAYLOAD, sort_keys=True, separators=(",", ":")),
        )
    )


def downgrade() -> None:
    op.execute(
        "LOCK TABLE public.data_artifacts, public.latest_quotes, public.source_policies "
        "IN ACCESS EXCLUSIVE MODE"
    )
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS (SELECT 1 FROM latest_quotes AS quote "
            "JOIN data_artifacts AS artifact ON artifact.id=quote.data_artifact_id "
            "WHERE artifact.source='alpaca' "
            "AND artifact.schema_version='alpaca_stock_snapshots_v1')"
        )
    ):
        raise RuntimeError("Alpaca REST snapshot facts prevent policy downgrade")
    op.execute("ALTER TABLE source_policies DISABLE TRIGGER trg_source_policies_immutable")
    deleted = op.get_bind().execute(
        text(
            "DELETE FROM source_policies WHERE id=CAST(:id AS uuid) "
            "AND key='intraday_monitor' AND version='intraday_monitor_policy_v4' "
            "AND schema_version='4' AND payload=CAST(:payload AS jsonb)"
        ),
        {
            "id": _POLICY_ID,
            "payload": json.dumps(_POLICY_PAYLOAD, sort_keys=True, separators=(",", ":")),
        },
    )
    if deleted.rowcount != 1:
        raise RuntimeError("Alpaca REST snapshot policy downgrade mismatch")
    op.execute("ALTER TABLE source_policies ENABLE TRIGGER trg_source_policies_immutable")
