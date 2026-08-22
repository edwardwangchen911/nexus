"""Seal published universes and publish the immutable market source policy.

Revision ID: 0005_market_integrity
Revises: 0004_market_master
Create Date: 2026-08-23
"""

from collections.abc import Sequence
from uuid import UUID

from alembic import op
import sqlalchemy as sa

revision: str = "0005_market_integrity"
down_revision: str | None = "0004_market_master"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SOURCE_POLICY_ID = UUID("3c60888d-e92d-5e04-913c-7d427004cfef")
SOURCE_POLICY_PAYLOAD = """{"priority":["fosun","alpaca","fixture"],"fosun":{"mode":"read_only","operation":"fosun_get_bars","market_scope":"US"},"alpaca":{"feed":"sip","modes":["delayed","fixture"]},"fixture":{"mode":"fixture"}}"""


def upgrade() -> None:
    op.add_column(
        "universe_snapshots",
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute("UPDATE universe_snapshots SET published_at = created_at")
    op.execute("DROP TRIGGER trg_universe_snapshots_immutable ON universe_snapshots")
    op.execute("DROP FUNCTION nexus_prevent_universe_snapshot_change()")
    op.execute(
        """
        CREATE FUNCTION nexus_guard_universe_snapshot_publication() RETURNS trigger AS $$
        BEGIN
            IF TG_OP = 'UPDATE'
               AND OLD.published_at IS NULL AND NEW.published_at IS NOT NULL
               AND NEW.id IS NOT DISTINCT FROM OLD.id
               AND NEW.key IS NOT DISTINCT FROM OLD.key
               AND NEW.version IS NOT DISTINCT FROM OLD.version
               AND NEW.effective_from IS NOT DISTINCT FROM OLD.effective_from
               AND NEW.source IS NOT DISTINCT FROM OLD.source
               AND NEW.content_hash IS NOT DISTINCT FROM OLD.content_hash
               AND NEW.created_at IS NOT DISTINCT FROM OLD.created_at THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'universe snapshot is immutable' USING ERRCODE = '23514';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        "CREATE TRIGGER trg_universe_snapshots_immutable BEFORE UPDATE OR DELETE "
        "ON universe_snapshots FOR EACH ROW EXECUTE FUNCTION nexus_guard_universe_snapshot_publication()"
    )
    op.execute(
        """
        CREATE FUNCTION nexus_guard_universe_membership() RETURNS trigger AS $$
        DECLARE snapshot_id uuid;
        BEGIN
            snapshot_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.universe_snapshot_id ELSE NEW.universe_snapshot_id END;
            IF TG_OP = 'UPDATE' THEN
                PERFORM 1 FROM universe_snapshots
                WHERE id IN (OLD.universe_snapshot_id, NEW.universe_snapshot_id)
                ORDER BY id FOR SHARE;
            ELSE
                PERFORM 1 FROM universe_snapshots WHERE id = snapshot_id FOR SHARE;
            END IF;
            IF EXISTS (SELECT 1 FROM universe_snapshots WHERE id = snapshot_id AND published_at IS NOT NULL)
               OR (TG_OP = 'UPDATE' AND EXISTS (
                   SELECT 1 FROM universe_snapshots
                   WHERE id = OLD.universe_snapshot_id AND published_at IS NOT NULL
               )) THEN
                RAISE EXCEPTION 'published universe membership is immutable' USING ERRCODE = '23514';
            END IF;
            RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        "CREATE TRIGGER trg_universe_members_publication_guard BEFORE INSERT OR UPDATE OR DELETE "
        "ON universe_members FOR EACH ROW EXECUTE FUNCTION nexus_guard_universe_membership()"
    )
    op.execute(
        sa.text(
            "INSERT INTO source_policies (id, key, version, schema_version, payload) "
            "VALUES (:id, 'market_price', 'source_policy_v1', '1', CAST(:payload AS jsonb))"
        ).bindparams(id=SOURCE_POLICY_ID, payload=SOURCE_POLICY_PAYLOAD)
    )
    op.execute(
        """
        CREATE FUNCTION nexus_prevent_source_policy_change() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'published source policy is immutable' USING ERRCODE = '23514';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        "CREATE TRIGGER trg_source_policies_immutable BEFORE UPDATE OR DELETE ON source_policies "
        "FOR EACH ROW EXECUTE FUNCTION nexus_prevent_source_policy_change()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER trg_source_policies_immutable ON source_policies")
    op.execute("DROP FUNCTION nexus_prevent_source_policy_change()")
    op.execute(
        sa.text("DELETE FROM source_policies WHERE id = :id").bindparams(id=SOURCE_POLICY_ID)
    )
    op.execute("DROP TRIGGER trg_universe_members_publication_guard ON universe_members")
    op.execute("DROP FUNCTION nexus_guard_universe_membership()")
    op.execute("DROP TRIGGER trg_universe_snapshots_immutable ON universe_snapshots")
    op.execute("DROP FUNCTION nexus_guard_universe_snapshot_publication()")
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
    op.drop_column("universe_snapshots", "published_at")
