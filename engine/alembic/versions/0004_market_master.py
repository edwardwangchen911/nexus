"""Add immutable market master and current quote/watchlist tables.

Revision ID: 0004_market_master
Revises: 0003_integration_admission_state
Create Date: 2026-08-23
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0004_market_master"
down_revision: str | None = "0003_integration_admission_state"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UUID = postgresql.UUID(as_uuid=True)
JSONB = postgresql.JSONB(astext_type=sa.Text())
UTC = sa.DateTime(timezone=True)
DECIMAL = sa.Numeric(38, 12)
CREATED_AT = sa.Column(
    "created_at", UTC, nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")
)
TABLES = (
    "instruments",
    "instrument_aliases",
    "universe_snapshots",
    "universe_members",
    "sector_exposures",
    "latest_quotes",
    "watchlists",
    "watchlist_members",
)


def _id() -> sa.Column[object]:
    return sa.Column("id", UUID, nullable=False)


def _primary_key(table_name: str) -> sa.PrimaryKeyConstraint:
    return sa.PrimaryKeyConstraint("id", name=f"pk_{table_name}")


def upgrade() -> None:
    op.create_table(
        "instruments",
        _id(),
        sa.Column("asset_type", sa.String(50), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("tick_size", DECIMAL),
        sa.Column("risk_bucket", sa.String(100), nullable=False),
        CREATED_AT,
        _primary_key("instruments"),
    )
    op.create_table(
        "instrument_aliases",
        _id(),
        sa.Column("instrument_id", UUID, nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("alias_type", sa.String(32), nullable=False),
        sa.Column("valid_from", sa.Date(), nullable=False),
        sa.Column("valid_to", sa.Date()),
        CREATED_AT,
        _primary_key("instrument_aliases"),
        sa.CheckConstraint(
            "valid_to IS NULL OR valid_to >= valid_from",
            name=op.f("ck_instrument_aliases_instrument_alias_valid_range"),
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_instrument_aliases_instrument_id_instruments",
        ),
        sa.UniqueConstraint(
            "symbol", "valid_from", name="uq_instrument_aliases_symbol_valid_from"
        ),
    )
    op.create_table(
        "universe_snapshots",
        _id(),
        sa.Column("key", sa.String(100), nullable=False),
        sa.Column("version", sa.String(100), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("source", sa.String(255), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        CREATED_AT,
        _primary_key("universe_snapshots"),
        sa.UniqueConstraint("key", "version", name="uq_universe_snapshots_key_version"),
        sa.UniqueConstraint("content_hash", name="uq_universe_snapshots_content_hash"),
    )
    op.create_table(
        "universe_members",
        _id(),
        sa.Column("universe_snapshot_id", UUID, nullable=False),
        sa.Column("instrument_id", UUID, nullable=False),
        sa.Column("inclusion_reason", sa.String(255), nullable=False),
        CREATED_AT,
        _primary_key("universe_members"),
        sa.ForeignKeyConstraint(
            ["universe_snapshot_id"],
            ["universe_snapshots.id"],
            name="fk_universe_members_universe_snapshot_id_universe_snapshots",
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_universe_members_instrument_id_instruments",
        ),
        sa.UniqueConstraint(
            "universe_snapshot_id",
            "instrument_id",
            name="uq_universe_members_snapshot_instrument",
        ),
    )
    op.create_table(
        "sector_exposures",
        _id(),
        sa.Column("instrument_id", UUID, nullable=False),
        sa.Column("sector_code", sa.String(100), nullable=False),
        sa.Column("weight", DECIMAL, nullable=False),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("synthetic", sa.Boolean(), nullable=False),
        sa.Column("reviewed", sa.Boolean(), nullable=False),
        CREATED_AT,
        _primary_key("sector_exposures"),
        sa.CheckConstraint(
            "weight >= 0 AND weight <= 1",
            name=op.f("ck_sector_exposures_sector_exposure_weight"),
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_sector_exposures_instrument_id_instruments",
        ),
        sa.UniqueConstraint(
            "instrument_id",
            "sector_code",
            "as_of",
            "content_hash",
            name="uq_sector_exposures_instrument_sector_as_of_hash",
        ),
    )
    op.create_table(
        "latest_quotes",
        _id(),
        sa.Column("instrument_id", UUID, nullable=False),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("feed", sa.String(100), nullable=False),
        sa.Column("bid", DECIMAL),
        sa.Column("ask", DECIMAL),
        sa.Column("last", DECIMAL),
        sa.Column("observed_at", UTC, nullable=False),
        sa.Column("available_at", UTC, nullable=False),
        sa.Column("received_at", UTC, nullable=False),
        sa.Column("revision", sa.String(255), nullable=False),
        sa.Column("quality_flags", JSONB, nullable=False),
        CREATED_AT,
        _primary_key("latest_quotes"),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_latest_quotes_instrument_id_instruments",
        ),
        sa.UniqueConstraint(
            "instrument_id", "source", "feed", name="uq_latest_quotes_instrument_source_feed"
        ),
    )
    op.create_table(
        "watchlists",
        _id(),
        sa.Column("name", sa.String(100), nullable=False),
        CREATED_AT,
        _primary_key("watchlists"),
        sa.UniqueConstraint("name", name="uq_watchlists_name"),
    )
    op.create_table(
        "watchlist_members",
        _id(),
        sa.Column("watchlist_id", UUID, nullable=False),
        sa.Column("instrument_id", UUID, nullable=False),
        CREATED_AT,
        _primary_key("watchlist_members"),
        sa.ForeignKeyConstraint(
            ["watchlist_id"],
            ["watchlists.id"],
            name="fk_watchlist_members_watchlist_id_watchlists",
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"],
            ["instruments.id"],
            name="fk_watchlist_members_instrument_id_instruments",
        ),
        sa.UniqueConstraint(
            "watchlist_id",
            "instrument_id",
            name="uq_watchlist_members_watchlist_instrument",
        ),
    )
    for table_name in TABLES:
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_created_at_immutable "
            f"BEFORE UPDATE ON {table_name} FOR EACH ROW "
            "EXECUTE FUNCTION nexus_prevent_created_at_change()"
        )
    op.execute(
        """
        CREATE FUNCTION nexus_prevent_instrument_id_change() RETURNS trigger AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id THEN
                RAISE EXCEPTION 'instrument id is immutable' USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        "CREATE TRIGGER trg_instruments_id_immutable BEFORE UPDATE ON instruments "
        "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_instrument_id_change()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_prevent_universe_snapshot_change() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'universe snapshot is immutable' USING ERRCODE = '23514';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        "CREATE TRIGGER trg_universe_snapshots_immutable BEFORE UPDATE OR DELETE "
        "ON universe_snapshots FOR EACH ROW EXECUTE FUNCTION nexus_prevent_universe_snapshot_change()"
    )


def downgrade() -> None:
    op.drop_table("watchlist_members")
    op.drop_table("watchlists")
    op.drop_table("latest_quotes")
    op.drop_table("sector_exposures")
    op.drop_table("universe_members")
    op.drop_table("universe_snapshots")
    op.execute("DROP FUNCTION nexus_prevent_universe_snapshot_change()")
    op.drop_table("instrument_aliases")
    op.drop_table("instruments")
    op.execute("DROP FUNCTION nexus_prevent_instrument_id_change()")
