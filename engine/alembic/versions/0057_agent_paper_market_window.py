"""Bind raw-first IEX windows to Agent paper cycles.

Revision ID: 0057_agent_paper_market_window
Revises: 0056_agent_paper_cycle
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0057_agent_paper_market_window"
down_revision: str | Sequence[str] | None = "0056_agent_paper_cycle"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LOCK_KEY = "nexus:agent-paper-market-window:v1"


def _install_price_authority_validator(*, allow_agent_paper: bool) -> None:
    agent_branch = (
        """
            IF NEW.source_policy_id='e99532cc-e24c-52a8-8165-c8076325fbd2'::uuid THEN
              SELECT * INTO prepared FROM public.nexus_prepare_agent_paper_price_bar_set(
                NEW.price_series_manifest_id
              );
              expected_policy_id := 'e99532cc-e24c-52a8-8165-c8076325fbd2'::uuid;
            ELSIF NEW.source_policy_id='2573d63a-fb6d-53d3-8d44-207766a78322'::uuid THEN
              SELECT * INTO prepared FROM public.nexus_prepare_agent_paper_decision_price_bar_set(
                NEW.price_series_manifest_id
              );
              expected_policy_id := '2573d63a-fb6d-53d3-8d44-207766a78322'::uuid;
            ELSE
              SELECT * INTO prepared FROM public.nexus_prepare_price_bar_set(
                NEW.price_series_manifest_id
              );
              expected_policy_id := '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid;
            END IF;
        """
        if allow_agent_paper
        else """
            SELECT * INTO prepared FROM public.nexus_prepare_price_bar_set(
              NEW.price_series_manifest_id
            );
            expected_policy_id := '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid;
        """
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_validate_price_bar_set_authority()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          prepared record;
          manifest public.price_series_manifests%ROWTYPE;
          evidence public.research_evidence_refs%ROWTYPE;
          expected_policy_id uuid;
        BEGIN
          IF NEW.hash_version='price_bar_set_sha256_v1'
             AND NEW.policy_version='agent_price_authority_v1' THEN
            {agent_branch}
          ELSIF NEW.hash_version='price_bar_set_sha256_v2'
                AND NEW.policy_version='agent_price_authority_v2' THEN
            SELECT * INTO prepared FROM public.nexus_prepare_price_bar_set_v2(
              NEW.price_series_manifest_id
            );
            expected_policy_id := 'f780fa7f-9135-5004-ab21-d665ae7be24b'::uuid;
          ELSE
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE='23514';
          END IF;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE='23514';
          END IF;
          SELECT * INTO manifest FROM public.price_series_manifests
          WHERE id=NEW.price_series_manifest_id;
          SELECT * INTO evidence FROM public.research_evidence_refs
          WHERE id=NEW.evidence_ref_id;
          IF NOT FOUND OR (
            NEW.source_policy_id=expected_policy_id
            AND NEW.source=manifest.source
            AND NEW.provider_object_id='price-bar-set:' || manifest.id::text
            AND NEW.revision=prepared.content_hash
            AND NEW.content_hash=prepared.content_hash
            AND NEW.byte_length=prepared.byte_length
            AND NEW.bar_count=prepared.bar_count
            AND NEW.first_session=prepared.first_session
            AND NEW.last_session=prepared.last_session
            AND NEW.available_at=prepared.available_at
            AND evidence.source=NEW.source
            AND evidence.provider_object_id=NEW.provider_object_id
            AND evidence.revision=NEW.revision
            AND evidence.available_at=NEW.available_at
            AND evidence.content_hash=prepared.content_hash
            AND evidence.byte_length=prepared.byte_length
            AND evidence.location='db://price-bar-set/' || manifest.id::text
              || '@sha256:' || prepared.content_hash
          ) IS NOT TRUE THEN
            RAISE EXCEPTION 'price bar set authority is invalid' USING ERRCODE='23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )


def upgrade() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_prepare_agent_paper_decision_price_bar_set(p_manifest_id uuid)
        RETURNS TABLE (
          content_hash text, byte_length bigint, bar_count integer,
          first_session date, last_session date, available_at timestamptz
        )
        LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE manifest public.price_series_manifests%ROWTYPE;
        BEGIN
          SELECT * INTO manifest FROM public.price_series_manifests WHERE id=p_manifest_id;
          IF NOT FOUND OR NOT (
            manifest.source='alpaca' AND manifest.feed='sip'
            AND manifest.delivery_mode='delayed' AND manifest.interval='1Day'
            AND manifest.adjustment='all'
            AND EXISTS (
              SELECT 1 FROM public.source_policies AS policy
              WHERE policy.id='2573d63a-fb6d-53d3-8d44-207766a78322'::uuid
                AND policy.key='paper_decision_price'
                AND policy.version='paper_decision_price_policy_v1'
                AND policy.schema_version='1'
            )
          ) THEN RETURN; END IF;
          RETURN QUERY SELECT * FROM public.nexus_prepare_price_bar_set(p_manifest_id);
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION nexus_prepare_agent_paper_price_bar_set(p_manifest_id uuid)
        RETURNS TABLE (
          content_hash text,
          byte_length bigint,
          bar_count integer,
          first_session date,
          last_session date,
          available_at timestamptz
        )
        LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp
        AS $$
        DECLARE
          manifest public.price_series_manifests%ROWTYPE;
          actual_count bigint;
          distinct_observation_count bigint;
          prepared_first_session date;
          prepared_last_session date;
          max_received timestamptz;
          canonical_text text;
        BEGIN
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('price-series-manifest:' || p_manifest_id::text, 0)
          );
          SELECT * INTO manifest FROM public.price_series_manifests WHERE id=p_manifest_id;
          IF NOT FOUND THEN RETURN; END IF;
          SELECT count(*),count(DISTINCT bar.observed_at),min(bar.session),max(bar.session),
                 max(bar.received_at)
            INTO actual_count,distinct_observation_count,prepared_first_session,
                 prepared_last_session,max_received
          FROM public.price_bar_records AS bar
          WHERE bar.price_series_manifest_id=manifest.id;
          IF NOT (
            manifest.source='alpaca' AND manifest.feed='iex'
            AND manifest.delivery_mode='real_time' AND manifest.interval='1Min'
            AND manifest.adjustment='raw' AND manifest.row_count=5
            AND manifest.end_at=manifest.start_at + interval '4 minutes'
            AND actual_count=5 AND distinct_observation_count=5
            AND NOT EXISTS (
              SELECT 1 FROM (
                SELECT bar.observed_at,
                       row_number() OVER (ORDER BY bar.observed_at) AS member_seq
                FROM public.price_bar_records AS bar
                WHERE bar.price_series_manifest_id=manifest.id
              ) AS ordered
              WHERE ordered.observed_at <> manifest.start_at
                    + (ordered.member_seq - 1) * interval '1 minute'
            )
            AND NOT EXISTS (
              SELECT 1 FROM public.price_bar_records AS bar
              WHERE bar.price_series_manifest_id=manifest.id
                AND (bar.instrument_id::text <> manifest.instrument
                     OR bar.observed_at > bar.available_at
                     OR bar.available_at > bar.received_at
                     OR jsonb_typeof(bar.quality_flags) <> 'array'
                     OR bar.quality_flags <> '[]'::jsonb)
            )
            AND EXISTS (
              SELECT 1 FROM public.source_policies AS policy
              WHERE policy.id='e99532cc-e24c-52a8-8165-c8076325fbd2'::uuid
                AND policy.key='paper_execution_price'
                AND policy.version='paper_execution_price_policy_v1'
                AND policy.schema_version='1'
            )
          ) THEN RETURN; END IF;
          canonical_text := public.nexus_price_bar_set_canonical_v1(p_manifest_id);
          IF canonical_text IS NULL THEN RETURN; END IF;
          RETURN QUERY SELECT
            pg_catalog.encode(public.digest(pg_catalog.convert_to(canonical_text,'UTF8'),'sha256'),
                              'hex'),
            pg_catalog.octet_length(pg_catalog.convert_to(canonical_text,'UTF8'))::bigint,
            actual_count::integer,prepared_first_session,prepared_last_session,max_received;
        END;
        $$
        """
    )
    _install_price_authority_validator(allow_agent_paper=True)
    op.create_table(
        "agent_paper_market_windows",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("cycle_id", sa.UUID(), nullable=False),
        sa.Column("instrument_id", sa.UUID(), nullable=False),
        sa.Column("order_id", sa.UUID(), nullable=True),
        sa.Column("purpose", sa.String(30), nullable=False),
        sa.Column("source_authority_id", sa.UUID(), nullable=False),
        sa.Column("raw_artifact_id", sa.UUID(), nullable=False),
        sa.Column("price_series_manifest_id", sa.UUID(), nullable=False),
        sa.Column("window_start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("data_available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(100), nullable=False),
        sa.Column("feed", sa.String(100), nullable=False),
        sa.Column("delivery_mode", sa.String(20), nullable=False),
        sa.Column("source_content_hash", sa.String(64), nullable=False),
        sa.Column("raw_content_hash", sa.String(64), nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("id", name="pk_agent_paper_market_windows"),
        sa.UniqueConstraint(
            "cycle_id",
            "instrument_id",
            "window_start_at",
            name="uq_agent_paper_market_windows_scope",
        ),
        sa.ForeignKeyConstraint(
            ["cycle_id"], ["agent_paper_cycles.id"], name="fk_agent_paper_market_windows_cycle"
        ),
        sa.ForeignKeyConstraint(
            ["instrument_id"], ["instruments.id"], name="fk_agent_paper_market_windows_instrument"
        ),
        sa.ForeignKeyConstraint(
            ["order_id"], ["paper_orders.id"], name="fk_agent_paper_market_windows_order"
        ),
        sa.ForeignKeyConstraint(
            ["raw_artifact_id"],
            ["data_artifacts.id"],
            name="fk_agent_paper_market_windows_raw_artifact",
        ),
        sa.ForeignKeyConstraint(
            ["price_series_manifest_id"],
            ["price_series_manifests.id"],
            name="fk_agent_paper_market_windows_manifest",
        ),
        sa.ForeignKeyConstraint(
            ["source_authority_id", "source", "source_content_hash"],
            [
                "price_bar_set_authorities.id",
                "price_bar_set_authorities.source",
                "price_bar_set_authorities.content_hash",
            ],
            name="fk_agent_paper_market_windows_authority",
        ),
        sa.CheckConstraint(
            "purpose IN ('paper_fill','acceptance_probe') AND "
            "((purpose='paper_fill' AND order_id IS NOT NULL) OR "
            "(purpose='acceptance_probe' AND order_id IS NULL))",
            name="ck_agent_paper_market_windows_purpose_order_shape",
        ),
        sa.CheckConstraint(
            "window_end_at = window_start_at + interval '5 minutes' "
            "AND data_available_at <= evaluated_at",
            name="ck_agent_paper_market_windows_window_bounds",
        ),
        sa.CheckConstraint(
            "source='alpaca' AND feed='iex' AND delivery_mode='real_time'",
            name="ck_agent_paper_market_windows_source_fixed",
        ),
        sa.CheckConstraint(
            "source_content_hash ~ '^[0-9a-f]{64}$' AND "
            "raw_content_hash ~ '^[0-9a-f]{64}$' AND content_hash ~ '^[0-9a-f]{64}$'",
            name="ck_agent_paper_market_windows_hashes_canonical",
        ),
    )
    op.create_index(
        "uq_agent_paper_market_windows_probe",
        "agent_paper_market_windows",
        ["cycle_id"],
        unique=True,
        postgresql_where=sa.text("purpose='acceptance_probe'"),
    )
    op.execute(
        """
        CREATE FUNCTION nexus_validate_agent_paper_market_window()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          cycle public.agent_paper_cycles%ROWTYPE;
          authority public.price_bar_set_authorities%ROWTYPE;
          manifest public.price_series_manifests%ROWTYPE;
          artifact public.data_artifacts%ROWTYPE;
          order_row public.paper_orders%ROWTYPE;
          decision_row public.strategy_decisions%ROWTYPE;
        BEGIN
          SELECT * INTO cycle FROM public.agent_paper_cycles WHERE id=NEW.cycle_id;
          SELECT * INTO authority FROM public.price_bar_set_authorities
            WHERE id=NEW.source_authority_id;
          SELECT * INTO manifest FROM public.price_series_manifests
            WHERE id=NEW.price_series_manifest_id;
          SELECT * INTO artifact FROM public.data_artifacts WHERE id=NEW.raw_artifact_id;
          IF cycle.id IS NULL OR authority.id IS NULL OR manifest.id IS NULL
             OR artifact.id IS NULL OR NOT (
              cycle.execution_locked
              AND cycle.execution_price_policy_id=
                'e99532cc-e24c-52a8-8165-c8076325fbd2'::uuid
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(cycle.candidate_manifest) AS candidate
                WHERE candidate->>'instrument_id'=NEW.instrument_id::text
              )
              AND authority.source_policy_id=
                'e99532cc-e24c-52a8-8165-c8076325fbd2'::uuid
              AND authority.price_series_manifest_id=manifest.id
              AND authority.source=NEW.source
              AND authority.content_hash=NEW.source_content_hash
              AND authority.available_at=NEW.data_available_at
              AND manifest.instrument=NEW.instrument_id::text
              AND manifest.source='alpaca' AND manifest.feed='iex'
              AND manifest.delivery_mode='real_time' AND manifest.interval='1Min'
              AND manifest.adjustment='raw' AND manifest.row_count=5
              AND manifest.start_at=NEW.window_start_at
              AND manifest.end_at=NEW.window_end_at - interval '1 minute'
              AND artifact.source='alpaca'
              AND artifact.schema_version='alpaca_iex_bars_raw_v1'
              AND artifact.sha256=NEW.raw_content_hash
          ) THEN
            RAISE EXCEPTION 'agent paper market window is invalid' USING ERRCODE='23514';
          END IF;
          IF NEW.purpose='acceptance_probe' THEN
            IF cycle.candidate_manifest->0->>'instrument_id' <> NEW.instrument_id::text THEN
              RAISE EXCEPTION 'agent paper market probe is invalid' USING ERRCODE='23514';
            END IF;
          ELSE
            SELECT * INTO order_row FROM public.paper_orders WHERE id=NEW.order_id;
            SELECT * INTO decision_row FROM public.strategy_decisions
              WHERE id=order_row.strategy_decision_id;
            IF order_row.id IS NULL OR decision_row.id IS NULL OR NOT (
              order_row.account_id=cycle.account_id
              AND order_row.instrument_id=NEW.instrument_id
              AND decision_row.account_id=cycle.account_id
              AND decision_row.decision_session=cycle.decision_session
              AND decision_row.decision_origin='agent_research'
            ) THEN
              RAISE EXCEPTION 'agent paper fill window is invalid' USING ERRCODE='23514';
            END IF;
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE CONSTRAINT TRIGGER trg_agent_paper_market_windows_validate "
        "AFTER INSERT ON agent_paper_market_windows DEFERRABLE INITIALLY DEFERRED "
        "FOR EACH ROW EXECUTE FUNCTION nexus_validate_agent_paper_market_window()"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_paper_market_windows_created_at_immutable BEFORE UPDATE ON "
        "agent_paper_market_windows FOR EACH ROW EXECUTE FUNCTION "
        "nexus_prevent_created_at_change()"
    )
    op.execute(
        "CREATE TRIGGER trg_agent_paper_market_windows_immutable BEFORE UPDATE OR DELETE ON "
        "agent_paper_market_windows FOR EACH ROW EXECUTE FUNCTION nexus_reject_paper_fact_mutation()"
    )


def downgrade() -> None:
    op.execute("SELECT pg_advisory_xact_lock(hashtextextended('" + _LOCK_KEY + "', 0))")
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM public.agent_paper_market_windows)
             OR EXISTS (
               SELECT 1 FROM public.price_bar_set_authorities
               WHERE source_policy_id IN (
                 '2573d63a-fb6d-53d3-8d44-207766a78322'::uuid,
                 'e99532cc-e24c-52a8-8165-c8076325fbd2'::uuid
               )
             ) THEN
            RAISE EXCEPTION 'agent paper market facts prevent downgrade' USING ERRCODE='23514';
          END IF;
        END $$
        """
    )
    op.execute("DROP TRIGGER trg_agent_paper_market_windows_validate ON agent_paper_market_windows")
    op.execute("DROP FUNCTION nexus_validate_agent_paper_market_window()")
    op.execute(
        "DROP TRIGGER trg_agent_paper_market_windows_immutable ON agent_paper_market_windows"
    )
    op.execute(
        "DROP TRIGGER trg_agent_paper_market_windows_created_at_immutable ON "
        "agent_paper_market_windows"
    )
    op.drop_table("agent_paper_market_windows")
    _install_price_authority_validator(allow_agent_paper=False)
    op.execute("DROP FUNCTION nexus_prepare_agent_paper_price_bar_set(uuid)")
    op.execute("DROP FUNCTION nexus_prepare_agent_paper_decision_price_bar_set(uuid)")
