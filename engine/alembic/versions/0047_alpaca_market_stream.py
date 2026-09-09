"""Admit bounded Alpaca IEX stream observations for display-only reads.

Revision ID: 0047_alpaca_market_stream
Revises: 0046_fosun_push_quote_authority
"""

import json

from alembic import op
from sqlalchemy import text

revision = "0047_alpaca_market_stream"
down_revision = "0046_fosun_push_quote_authority"
branch_labels = None
depends_on = None

_POLICY_ID = "778e9ef4-2af4-5166-9f17-06087448de7f"
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
        "transport": "websocket",
        "artifact_schema": "alpaca_stock_stream_v1",
        "market_scope": "US",
    },
    "purpose": "display_only_intraday_monitor",
}


def upgrade() -> None:
    op.execute(
        text(
            "INSERT INTO source_policies (id,key,version,schema_version,payload) "
            "VALUES (CAST(:id AS uuid),'intraday_monitor','intraday_monitor_policy_v3','3',"
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
            "AND artifact.schema_version='alpaca_stock_stream_v1')"
        )
    ):
        raise RuntimeError("Alpaca market stream facts prevent downgrade")
    op.execute("ALTER TABLE source_policies DISABLE TRIGGER trg_source_policies_immutable")
    deleted = op.get_bind().execute(
        text(
            "DELETE FROM source_policies WHERE id=CAST(:id AS uuid) "
            "AND key='intraday_monitor' AND version='intraday_monitor_policy_v3' "
            "AND schema_version='3' AND payload=CAST(:payload AS jsonb)"
        ),
        {
            "id": _POLICY_ID,
            "payload": json.dumps(_POLICY_PAYLOAD, sort_keys=True, separators=(",", ":")),
        },
    )
    if deleted.rowcount != 1:
        raise RuntimeError("Alpaca market stream policy downgrade mismatch")
    op.execute("ALTER TABLE source_policies ENABLE TRIGGER trg_source_policies_immutable")
