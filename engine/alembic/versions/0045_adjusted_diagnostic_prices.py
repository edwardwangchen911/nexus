"""Publish adjusted daily-price policy for deterministic diagnostics.

Revision ID: 0045_adjusted_diagnostic_prices
Revises: 0044_news_research
"""

import json

from alembic import op
from sqlalchemy import text

revision = "0045_adjusted_diagnostic_prices"
down_revision = "0044_news_research"
branch_labels = None
depends_on = None

_POLICY_ID = "a434c593-b636-5f4a-ac54-09e93a395c23"
_PAYLOAD = {
    "selected_source": "alpaca",
    "fallback": "disabled",
    "alpaca": {
        "feed": "sip",
        "delivery_mode": "delayed",
        "adjustment": "all",
        "interval": "1Day",
        "market_scope": "US",
    },
    "fosun": {"purpose": "display_only_raw_prices"},
    "purpose": "portfolio_diagnostics_adjusted_daily_bars",
}


def upgrade() -> None:
    op.execute(
        text(
            "INSERT INTO source_policies (id,key,version,schema_version,payload) "
            "VALUES (CAST(:id AS uuid),'market_price','source_policy_v3','3',"
            "CAST(:payload AS jsonb))"
        ).bindparams(
            id=_POLICY_ID,
            payload=json.dumps(_PAYLOAD, sort_keys=True, separators=(",", ":")),
        )
    )


def downgrade() -> None:
    if op.get_bind().scalar(
        text(
            "SELECT EXISTS ("
            "SELECT 1 FROM price_series_manifests AS manifest "
            "JOIN price_bar_set_authorities AS authority "
            "ON authority.price_series_manifest_id=manifest.id "
            "WHERE manifest.source='alpaca' AND manifest.feed='sip' "
            "AND manifest.delivery_mode='delayed' AND manifest.adjustment='all' "
            "AND manifest.interval='1Day')"
        )
    ):
        raise RuntimeError("adjusted diagnostic price facts prevent downgrade")
    op.execute("ALTER TABLE source_policies DISABLE TRIGGER trg_source_policies_immutable")
    deleted = op.get_bind().execute(
        text(
            "DELETE FROM source_policies WHERE id=CAST(:id AS uuid) "
            "AND key='market_price' AND version='source_policy_v3' "
            "AND schema_version='3' AND payload=CAST(:payload AS jsonb)"
        ),
        {
            "id": _POLICY_ID,
            "payload": json.dumps(_PAYLOAD, sort_keys=True, separators=(",", ":")),
        },
    )
    if deleted.rowcount != 1:
        raise RuntimeError("cannot downgrade a missing or altered v3 source policy")
    op.execute("ALTER TABLE source_policies ENABLE TRIGGER trg_source_policies_immutable")
