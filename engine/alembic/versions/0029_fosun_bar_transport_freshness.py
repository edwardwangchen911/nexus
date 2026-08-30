"""Bind Fosun bars to raw transport evidence and decision-time freshness.

Revision ID: 0029_fosun_bar_lineage
Revises: 0028_fosun_authority_hardening
Create Date: 2026-08-30
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0029_fosun_bar_lineage"
down_revision: str | Sequence[str] | None = "0028_fosun_authority_hardening"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add fail-closed transport and calendar lineage for new Fosun authorities."""
    op.execute(
        "ALTER TABLE price_series_manifests "
        "ADD COLUMN gateway_transport_artifact_id uuid, "
        "ADD COLUMN daily_calendar_authority_id uuid, "
        "ADD COLUMN gateway_request_start_at timestamptz, "
        "ADD COLUMN gateway_request_end_at timestamptz"
    )
    op.execute(
        "ALTER TABLE price_series_manifests ADD CONSTRAINT "
        "fk_price_series_manifests_gateway_transport_artifact "
        "FOREIGN KEY (gateway_transport_artifact_id) REFERENCES data_artifacts(id) NOT VALID"
    )
    op.execute(
        "ALTER TABLE price_series_manifests ADD CONSTRAINT "
        "fk_price_series_manifests_daily_calendar_authority "
        "FOREIGN KEY (daily_calendar_authority_id) "
        "REFERENCES exchange_calendar_authorities(id) NOT VALID"
    )
    op.execute(
        "ALTER TABLE price_series_manifests ADD CONSTRAINT "
        "ck_price_series_manifests_fosun_transport_lineage_complete CHECK ("
        "(source = 'fosun' AND gateway_transport_artifact_id IS NOT NULL AND "
        "((interval = '1Day' AND daily_calendar_authority_id IS NOT NULL AND "
        "gateway_request_start_at IS NOT NULL AND gateway_request_end_at IS NOT NULL) OR "
        "(interval = '1Min' AND daily_calendar_authority_id IS NULL AND "
        "gateway_request_start_at IS NULL AND gateway_request_end_at IS NULL))) OR "
        "(source <> 'fosun' AND gateway_transport_artifact_id IS NULL AND "
        "daily_calendar_authority_id IS NULL AND gateway_request_start_at IS NULL AND "
        "gateway_request_end_at IS NULL)) NOT VALID"
    )
    op.execute(
        "CREATE INDEX ix_price_series_manifests_gateway_transport "
        "ON price_series_manifests (gateway_transport_artifact_id)"
    )
    op.execute(
        "CREATE INDEX ix_price_series_manifests_daily_calendar "
        "ON price_series_manifests (daily_calendar_authority_id)"
    )
    _install_calendar_publication_guard()
    _create_manifest_lineage_guard()
    _create_bar_lineage_guard()
    _create_price_freshness_functions()
    _create_intraday_transport_function()
    _create_authority_guards()
    _create_agent_freshness_guard()
    _create_lineage_immutability()


def downgrade() -> None:
    """Remove lineage only before any new binding has been persisted."""
    # Match the cross-migration lock order used by the authority downgrade and
    # take final DDL-strength locks before inspecting facts. Waiting writers
    # therefore commit before the guard is evaluated; no protected lineage can
    # appear between the guard and trigger/column removal.
    op.execute(
        "LOCK TABLE "
        "public.agent_data_snapshot_evidence, "
        "public.data_artifacts, "
        "public.exchange_calendar_authorities, "
        "public.intraday_bar_set_authorities, "
        "public.latest_quotes, "
        "public.price_bar_records, "
        "public.price_bar_set_authorities, "
        "public.price_series_manifests "
        "IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM price_series_manifests
            WHERE provider_capability_id IS NOT NULL
               OR gateway_transport_artifact_id IS NOT NULL
               OR daily_calendar_authority_id IS NOT NULL
          ) OR EXISTS (
            SELECT 1 FROM latest_quotes
            WHERE provider_capability_id IS NOT NULL
          ) OR EXISTS (
            SELECT 1 FROM price_bar_set_authorities
            WHERE hash_version = 'price_bar_set_sha256_v2'
          ) OR EXISTS (
            SELECT 1 FROM intraday_bar_set_authorities
            WHERE hash_version = 'intraday_bar_set_sha256_v2'
          ) OR EXISTS (
            SELECT 1 FROM agent_data_snapshot_evidence AS evidence
            WHERE evidence.source = 'fosun'
               OR EXISTS (
                 SELECT 1 FROM price_bar_set_authorities AS authority
                 WHERE authority.evidence_ref_id = evidence.evidence_ref_id
                   AND authority.hash_version = 'price_bar_set_sha256_v2'
               )
          ) OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles AS role
            WHERE role.rolname <> current_user
              AND NOT role.rolsuper
              AND role.rolcanlogin
              AND (
                has_table_privilege(role.oid, 'public.price_series_manifests', 'INSERT')
                OR has_table_privilege(role.oid, 'public.price_series_manifests', 'UPDATE')
                OR has_table_privilege(role.oid, 'public.price_series_manifests', 'DELETE')
                OR has_table_privilege(role.oid, 'public.price_bar_records', 'INSERT')
                OR has_table_privilege(role.oid, 'public.price_bar_set_authorities', 'INSERT')
                OR has_table_privilege(
                  role.oid, 'public.intraday_bar_set_authorities', 'INSERT'
                )
                OR has_table_privilege(
                  role.oid, 'public.agent_data_snapshot_evidence', 'INSERT'
                )
                OR has_table_privilege(role.oid, 'public.data_artifacts', 'UPDATE')
                OR has_table_privilege(role.oid, 'public.data_artifacts', 'DELETE')
              )
          ) THEN
            RAISE EXCEPTION 'Fosun transport lineage prevents downgrade'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    _restore_sealed_backtest_append_guard()
    op.execute(
        "DROP TRIGGER trg_data_artifacts_fosun_transport_immutable ON data_artifacts"
    )
    op.execute(
        "DROP TRIGGER trg_price_series_fosun_lineage_immutable "
        "ON price_series_manifests"
    )
    op.execute("DROP FUNCTION nexus_prevent_fosun_lineage_change()")
    op.execute(
        "DROP TRIGGER trg_agent_price_fosun_freshness "
        "ON agent_data_snapshot_evidence"
    )
    op.execute("DROP FUNCTION nexus_validate_agent_price_freshness()")
    op.execute(
        "DROP TRIGGER trg_intraday_authority_fosun_transport "
        "ON intraday_bar_set_authorities"
    )
    op.execute(
        "DROP TRIGGER trg_price_authority_fosun_transport "
        "ON price_bar_set_authorities"
    )
    op.execute("DROP FUNCTION nexus_validate_fosun_authority_transport()")
    op.execute("DROP FUNCTION nexus_prepare_intraday_bar_set_v2_bound(uuid)")
    op.execute("DROP FUNCTION nexus_prepare_price_bar_set_v2_at(uuid,timestamptz)")
    op.execute("DROP FUNCTION nexus_fosun_price_bar_set_valid_at(uuid,timestamptz)")
    op.execute("DROP TRIGGER trg_price_bar_records_fosun_transport ON price_bar_records")
    op.execute("DROP FUNCTION nexus_validate_fosun_bar_transport()")
    op.execute(
        "DROP TRIGGER trg_price_series_fosun_transport "
        "ON price_series_manifests"
    )
    op.execute("DROP FUNCTION nexus_validate_fosun_manifest_transport()")
    op.execute(
        "DROP INDEX ix_price_series_manifests_daily_calendar"
    )
    op.execute(
        "DROP INDEX ix_price_series_manifests_gateway_transport"
    )
    op.execute(
        "ALTER TABLE price_series_manifests DROP CONSTRAINT "
        "ck_price_series_manifests_fosun_transport_lineage_complete"
    )
    op.execute(
        "ALTER TABLE price_series_manifests DROP CONSTRAINT "
        "fk_price_series_manifests_daily_calendar_authority"
    )
    op.execute(
        "ALTER TABLE price_series_manifests DROP CONSTRAINT "
        "fk_price_series_manifests_gateway_transport_artifact"
    )
    op.execute(
        "ALTER TABLE price_series_manifests "
        "DROP COLUMN gateway_request_end_at, "
        "DROP COLUMN gateway_request_start_at, "
        "DROP COLUMN daily_calendar_authority_id, "
        "DROP COLUMN gateway_transport_artifact_id"
    )


def _install_calendar_publication_guard() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION nexus_prevent_sealed_backtest_append()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          sealed boolean := false;
          fosun_published boolean := false;
        BEGIN
          IF TG_TABLE_NAME = 'exchange_calendar_sessions' THEN
            PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
              'exchange-calendar-authority:' || NEW.calendar_authority_id::text, 0
            ));
            PERFORM id FROM exchange_calendar_authorities
            WHERE id = NEW.calendar_authority_id FOR UPDATE;
            SELECT EXISTS (
              SELECT 1 FROM backtest_runs AS run
              JOIN backtest_authority_bundles AS bundle
                ON bundle.id = run.authority_bundle_id
              WHERE bundle.calendar_authority_id = NEW.calendar_authority_id
            ) INTO sealed;
            SELECT EXISTS (
              SELECT 1 FROM price_series_manifests AS manifest
              WHERE manifest.source = 'fosun'
                AND manifest.interval = '1Day'
                AND manifest.daily_calendar_authority_id = NEW.calendar_authority_id
            ) OR EXISTS (
              SELECT 1 FROM price_bar_set_authorities AS authority
              JOIN price_series_manifests AS manifest
                ON manifest.id = authority.price_series_manifest_id
              WHERE manifest.source = 'fosun'
                AND manifest.interval = '1Day'
                AND manifest.daily_calendar_authority_id = NEW.calendar_authority_id
            ) INTO fosun_published;
          ELSIF TG_TABLE_NAME IN (
            'backtest_universe_authority_members',
            'backtest_sector_authority_members',
            'backtest_corporate_action_authority_members'
          ) THEN
            PERFORM id FROM backtest_authority_collections
            WHERE id = NEW.collection_id FOR UPDATE;
            SELECT EXISTS (
              SELECT 1 FROM backtest_runs AS run
              JOIN backtest_authority_bundles AS bundle
                ON bundle.id = run.authority_bundle_id
              WHERE NEW.collection_id IN (
                bundle.universe_collection_id,
                bundle.sector_collection_id,
                bundle.corporate_action_collection_id
              )
            ) INTO sealed;
          ELSIF TG_TABLE_NAME IN (
            'backtest_price_authority_members',
            'backtest_instrument_authority_members'
          ) THEN
            PERFORM id FROM backtest_authority_bundles
            WHERE id = NEW.bundle_id FOR UPDATE;
            SELECT EXISTS (
              SELECT 1 FROM backtest_runs AS run
              WHERE run.authority_bundle_id = NEW.bundle_id
            ) INTO sealed;
          ELSIF TG_TABLE_NAME IN (
            'backtest_windows','backtest_nav_points','backtest_fills',
            'backtest_order_outcomes','backtest_metrics'
          ) THEN
            PERFORM pg_catalog.pg_advisory_xact_lock(
              pg_catalog.hashtextextended(NEW.backtest_run_id::text, 1602)
            );
            SELECT COALESCE((
              SELECT run.graph_sealed FROM backtest_runs AS run
              WHERE run.id = NEW.backtest_run_id
            ), false) INTO sealed;
          END IF;
          IF sealed THEN
            RAISE EXCEPTION 'backtest authority set is sealed'
              USING ERRCODE = '23514';
          END IF;
          IF fosun_published THEN
            RAISE EXCEPTION 'calendar authority set is sealed by Fosun publication'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )


def _restore_sealed_backtest_append_guard() -> None:
    op.execute(
        "CREATE OR REPLACE FUNCTION nexus_prevent_sealed_backtest_append() RETURNS trigger "
        "LANGUAGE plpgsql AS $$ DECLARE sealed boolean := false; BEGIN "
        "IF TG_TABLE_NAME = 'exchange_calendar_sessions' THEN "
        "PERFORM id FROM exchange_calendar_authorities WHERE id = NEW.calendar_authority_id FOR UPDATE; "
        "SELECT EXISTS (SELECT 1 FROM backtest_runs r JOIN backtest_authority_bundles b ON b.id = r.authority_bundle_id WHERE b.calendar_authority_id = NEW.calendar_authority_id) INTO sealed; "
        "ELSIF TG_TABLE_NAME IN ('backtest_universe_authority_members','backtest_sector_authority_members','backtest_corporate_action_authority_members') THEN "
        "PERFORM id FROM backtest_authority_collections WHERE id = NEW.collection_id FOR UPDATE; "
        "SELECT EXISTS (SELECT 1 FROM backtest_runs r JOIN backtest_authority_bundles b ON b.id = r.authority_bundle_id WHERE NEW.collection_id IN (b.universe_collection_id,b.sector_collection_id,b.corporate_action_collection_id)) INTO sealed; "
        "ELSIF TG_TABLE_NAME IN ('backtest_price_authority_members','backtest_instrument_authority_members') THEN "
        "PERFORM id FROM backtest_authority_bundles WHERE id = NEW.bundle_id FOR UPDATE; "
        "SELECT EXISTS (SELECT 1 FROM backtest_runs r WHERE r.authority_bundle_id = NEW.bundle_id) INTO sealed; "
        "ELSIF TG_TABLE_NAME IN ('backtest_windows','backtest_nav_points','backtest_fills','backtest_order_outcomes','backtest_metrics') THEN "
        "PERFORM pg_advisory_xact_lock(hashtextextended(NEW.backtest_run_id::text, 1602)); "
        "SELECT COALESCE((SELECT r.graph_sealed FROM backtest_runs r WHERE r.id = NEW.backtest_run_id), false) INTO sealed; "
        "END IF; IF sealed THEN RAISE EXCEPTION 'backtest authority set is sealed' USING ERRCODE = '23514'; END IF; RETURN NEW; END; $$"
    )
    op.execute("ALTER FUNCTION nexus_prevent_sealed_backtest_append() RESET search_path")


def _create_manifest_lineage_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_validate_fosun_manifest_transport()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          artifact data_artifacts%ROWTYPE;
          calendar exchange_calendar_authorities%ROWTYPE;
          expected_schema text;
        BEGIN
          IF NEW.source <> 'fosun' THEN
            IF NEW.gateway_transport_artifact_id IS NOT NULL
               OR NEW.daily_calendar_authority_id IS NOT NULL
               OR NEW.gateway_request_start_at IS NOT NULL
               OR NEW.gateway_request_end_at IS NOT NULL THEN
              RAISE EXCEPTION 'Fosun transport lineage is invalid'
                USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
          END IF;
          expected_schema := CASE NEW.interval
            WHEN '1Day' THEN 'fosun_gateway_market_bars_v1'
            WHEN '1Min' THEN 'fosun_gateway_market_snapshot_v1'
            ELSE NULL
          END;
          PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            'fosun-data-artifact:' || NEW.gateway_transport_artifact_id::text, 0
          ));
          SELECT * INTO artifact FROM data_artifacts
          WHERE id = NEW.gateway_transport_artifact_id;
          IF expected_schema IS NULL OR NOT FOUND OR (
            artifact.source = 'fosun'
            AND artifact.schema_version = expected_schema
            AND artifact.mime_type = 'application/json'
            AND artifact.sha256 ~ '^[0-9a-f]{64}$'
            AND artifact.byte_size > 0
            AND artifact.observed_at <= artifact.available_at
            AND artifact.available_at <= artifact.received_at
            AND artifact.received_at <= NEW.provider_authorized_at
          ) IS NOT TRUE THEN
            RAISE EXCEPTION 'Fosun transport lineage is invalid'
              USING ERRCODE = '23514';
          END IF;
          IF NEW.interval = '1Day' THEN
            IF NEW.gateway_request_start_at IS NULL
               OR NEW.gateway_request_end_at IS NULL
               OR NEW.gateway_request_end_at - NEW.gateway_request_start_at
                  <> interval '400 days'
               OR NEW.gateway_request_end_at > NEW.provider_authorized_at THEN
              RAISE EXCEPTION 'Fosun daily request lineage is invalid'
                USING ERRCODE = '23514';
            END IF;
            PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
              'exchange-calendar-authority:'
                || NEW.daily_calendar_authority_id::text, 0
            ));
            SELECT * INTO calendar FROM exchange_calendar_authorities
            WHERE id = NEW.daily_calendar_authority_id;
            IF NOT FOUND OR (
              calendar.exchange = 'XNYS'
              AND calendar.completeness_status = 'complete'
              AND calendar.published_at <= NEW.provider_authorized_at
              AND calendar.range_start <=
                  (NEW.gateway_request_start_at AT TIME ZONE 'UTC')::date
              AND calendar.range_end >=
                  (NEW.gateway_request_end_at AT TIME ZONE 'UTC')::date
            ) IS NOT TRUE THEN
              RAISE EXCEPTION 'Fosun daily calendar lineage is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF NEW.daily_calendar_authority_id IS NOT NULL
                OR NEW.gateway_request_start_at IS NOT NULL
                OR NEW.gateway_request_end_at IS NOT NULL THEN
            RAISE EXCEPTION 'Fosun intraday calendar lineage is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_price_series_fosun_transport BEFORE INSERT "
        "ON price_series_manifests FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_fosun_manifest_transport()"
    )


def _create_bar_lineage_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_validate_fosun_bar_transport()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE expected_revision text;
        BEGIN
          SELECT artifact.sha256 INTO expected_revision
          FROM price_series_manifests AS manifest
          JOIN data_artifacts AS artifact
            ON artifact.id = manifest.gateway_transport_artifact_id
          WHERE manifest.id = NEW.price_series_manifest_id
            AND manifest.source = 'fosun';
          IF FOUND AND NEW.revision <> expected_revision THEN
            RAISE EXCEPTION 'Fosun bar transport revision is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_price_bar_records_fosun_transport BEFORE INSERT "
        "ON price_bar_records FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_fosun_bar_transport()"
    )


def _create_price_freshness_functions() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_fosun_price_bar_set_valid_at(
          p_manifest_id uuid, p_as_of timestamptz
        ) RETURNS boolean LANGUAGE plpgsql STABLE
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          manifest price_series_manifests%ROWTYPE;
          artifact data_artifacts%ROWTYPE;
          calendar exchange_calendar_authorities%ROWTYPE;
          capability provider_capabilities%ROWTYPE;
          prepared record;
          authorized_session date;
          selected_session date;
          expected_session_count bigint;
        BEGIN
          IF p_as_of IS NULL THEN RETURN false; END IF;
          SELECT * INTO manifest FROM price_series_manifests
          WHERE id = p_manifest_id;
          IF NOT FOUND OR manifest.source <> 'fosun' OR manifest.interval <> '1Day'
             OR manifest.provider_authorized_at > p_as_of THEN RETURN false; END IF;
          PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(
            'exchange-calendar-authority:'
              || manifest.daily_calendar_authority_id::text, 0
          ));
          SELECT * INTO prepared FROM nexus_prepare_price_bar_set_v2(p_manifest_id);
          IF NOT FOUND THEN RETURN false; END IF;
          SELECT * INTO artifact FROM data_artifacts
          WHERE id = manifest.gateway_transport_artifact_id;
          IF NOT FOUND THEN RETURN false; END IF;
          SELECT * INTO calendar FROM exchange_calendar_authorities
          WHERE id = manifest.daily_calendar_authority_id;
          IF NOT FOUND THEN RETURN false; END IF;
          SELECT * INTO capability FROM provider_capabilities
          WHERE id = manifest.provider_capability_id;
          IF NOT FOUND THEN RETURN false; END IF;
          SELECT max(session) INTO authorized_session
          FROM exchange_calendar_sessions
          WHERE calendar_authority_id = calendar.id
            AND market_close_at <= manifest.provider_authorized_at - interval '30 minutes';
          SELECT max(session) INTO selected_session
          FROM exchange_calendar_sessions
          WHERE calendar_authority_id = calendar.id
            AND market_close_at <= p_as_of - interval '30 minutes';
          SELECT count(*) INTO expected_session_count
          FROM exchange_calendar_sessions
          WHERE calendar_authority_id = calendar.id
            AND session BETWEEN
                (manifest.gateway_request_start_at AT TIME ZONE 'UTC')::date
                AND (manifest.gateway_request_end_at AT TIME ZONE 'UTC')::date;
          RETURN (
            manifest.gateway_transport_artifact_id = artifact.id
            AND artifact.source = 'fosun'
            AND artifact.schema_version = 'fosun_gateway_market_bars_v1'
            AND artifact.mime_type = 'application/json'
            AND artifact.sha256 ~ '^[0-9a-f]{64}$'
            AND artifact.byte_size > 0
            AND artifact.observed_at <= artifact.available_at
            AND artifact.available_at <= artifact.received_at
            AND artifact.received_at <= manifest.provider_authorized_at
            AND NOT EXISTS (
              SELECT 1 FROM price_bar_records AS bar
              WHERE bar.price_series_manifest_id = manifest.id
                AND bar.revision <> artifact.sha256
            )
            AND calendar.exchange = 'XNYS'
            AND calendar.completeness_status = 'complete'
            AND calendar.published_at <= manifest.provider_authorized_at
            AND manifest.gateway_request_end_at - manifest.gateway_request_start_at
                = interval '400 days'
            AND manifest.gateway_request_end_at <= manifest.provider_authorized_at
            AND calendar.range_start <=
                (manifest.gateway_request_start_at AT TIME ZONE 'UTC')::date
            AND calendar.range_end >= (p_as_of AT TIME ZONE 'UTC')::date
            AND prepared.last_session = authorized_session
            AND prepared.last_session = selected_session
            AND prepared.bar_count = expected_session_count
            AND NOT EXISTS (
              SELECT 1 FROM price_bar_records AS bar
              LEFT JOIN exchange_calendar_sessions AS expected
                ON expected.calendar_authority_id = calendar.id
               AND expected.session = bar.session
               AND expected.session BETWEEN
                   (manifest.gateway_request_start_at AT TIME ZONE 'UTC')::date
                   AND (manifest.gateway_request_end_at AT TIME ZONE 'UTC')::date
              WHERE bar.price_series_manifest_id = manifest.id
                AND expected.session IS NULL
            )
            AND capability.provider = 'fosun'
            AND capability.capability = 'READ_ONLY'
            AND capability.schema_version = '1'
            AND capability.contract_hash = manifest.provider_contract_hash
            AND capability.payload ->> 'operation' = 'fosun_get_bars'
            AND capability.payload ->> 'market_scope' = 'US'
            AND capability.payload ->> 'delivery_mode' = manifest.delivery_mode
            AND capability.created_at <= manifest.provider_authorized_at
            AND manifest.provider_authorized_at < capability.expires_at
            AND prepared.available_at <= p_as_of
          ) IS TRUE;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE FUNCTION nexus_prepare_price_bar_set_v2_at(
          p_manifest_id uuid, p_as_of timestamptz
        ) RETURNS TABLE (
          content_hash text, byte_length bigint, bar_count integer,
          first_session date, last_session date, available_at timestamptz
        ) LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        BEGIN
          IF NOT nexus_fosun_price_bar_set_valid_at(p_manifest_id, p_as_of) THEN
            RETURN;
          END IF;
          RETURN QUERY SELECT * FROM nexus_prepare_price_bar_set_v2(p_manifest_id);
        END;
        $$
        """
    )


def _create_intraday_transport_function() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_prepare_intraday_bar_set_v2_bound(p_manifest_id uuid)
        RETURNS TABLE (
          content_hash text, byte_length bigint, bar_count integer,
          first_observed_at timestamptz, last_observed_at timestamptz,
          available_at timestamptz
        ) LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          manifest price_series_manifests%ROWTYPE;
          artifact data_artifacts%ROWTYPE;
        BEGIN
          SELECT * INTO manifest FROM price_series_manifests
          WHERE id = p_manifest_id;
          IF NOT FOUND OR manifest.source <> 'fosun' OR manifest.interval <> '1Min'
             THEN RETURN; END IF;
          SELECT * INTO artifact FROM data_artifacts
          WHERE id = manifest.gateway_transport_artifact_id;
          IF NOT FOUND OR (
            artifact.source = 'fosun'
            AND artifact.schema_version = 'fosun_gateway_market_snapshot_v1'
            AND artifact.mime_type = 'application/json'
            AND artifact.sha256 ~ '^[0-9a-f]{64}$'
            AND artifact.byte_size > 0
            AND artifact.observed_at <= artifact.available_at
            AND artifact.available_at <= artifact.received_at
            AND artifact.received_at <= manifest.provider_authorized_at
            AND NOT EXISTS (
              SELECT 1 FROM price_bar_records AS bar
              WHERE bar.price_series_manifest_id = manifest.id
                AND bar.revision <> artifact.sha256
            )
          ) IS NOT TRUE THEN RETURN; END IF;
          RETURN QUERY SELECT * FROM nexus_prepare_intraday_bar_set_v2(p_manifest_id);
        END;
        $$
        """
    )


def _create_authority_guards() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_validate_fosun_authority_transport()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE authorized_at timestamptz;
        BEGIN
          IF TG_TABLE_NAME = 'price_bar_set_authorities'
             AND NEW.hash_version = 'price_bar_set_sha256_v2' THEN
            SELECT provider_authorized_at INTO authorized_at
            FROM price_series_manifests
            WHERE id = NEW.price_series_manifest_id;
            IF NOT nexus_fosun_price_bar_set_valid_at(
              NEW.price_series_manifest_id, authorized_at
            ) THEN
              RAISE EXCEPTION 'Fosun price authority transport is invalid'
                USING ERRCODE = '23514';
            END IF;
          ELSIF TG_TABLE_NAME = 'intraday_bar_set_authorities'
                AND NEW.hash_version = 'intraday_bar_set_sha256_v2'
                AND NOT EXISTS (
                  SELECT 1 FROM nexus_prepare_intraday_bar_set_v2_bound(
                    NEW.price_series_manifest_id
                  )
                ) THEN
            RAISE EXCEPTION 'Fosun intraday authority transport is invalid'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_price_authority_fosun_transport BEFORE INSERT "
        "ON price_bar_set_authorities FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_fosun_authority_transport()"
    )
    op.execute(
        "CREATE TRIGGER trg_intraday_authority_fosun_transport BEFORE INSERT "
        "ON intraday_bar_set_authorities FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_fosun_authority_transport()"
    )


def _create_agent_freshness_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_validate_agent_price_freshness()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE manifest_id uuid;
        BEGIN
          IF NEW.authority_kind <> 'price' THEN RETURN NEW; END IF;
          SELECT authority.price_series_manifest_id INTO manifest_id
          FROM price_bar_set_authorities AS authority
          WHERE authority.evidence_ref_id = NEW.evidence_ref_id
            AND authority.hash_version = 'price_bar_set_sha256_v2';
          IF FOUND AND (
            NEW.selected_at IS NULL
            OR NOT nexus_fosun_price_bar_set_valid_at(manifest_id, NEW.selected_at)
          ) THEN
            RAISE EXCEPTION 'Agent price authority is stale or unbound'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_agent_price_fosun_freshness BEFORE INSERT "
        "ON agent_data_snapshot_evidence FOR EACH ROW "
        "EXECUTE FUNCTION nexus_validate_agent_price_freshness()"
    )


def _create_lineage_immutability() -> None:
    op.execute(
        """
        CREATE FUNCTION nexus_prevent_fosun_lineage_change()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        BEGIN
          IF TG_TABLE_NAME = 'data_artifacts' THEN
            IF EXISTS (
              SELECT 1 FROM price_series_manifests
              WHERE gateway_transport_artifact_id = OLD.id
            ) THEN
              RAISE EXCEPTION 'Fosun transport lineage is immutable'
                USING ERRCODE = '23514';
            END IF;
          ELSIF OLD.gateway_transport_artifact_id IS NOT NULL THEN
            IF TG_OP = 'DELETE' OR ROW(
              NEW.gateway_transport_artifact_id, NEW.daily_calendar_authority_id,
              NEW.gateway_request_start_at, NEW.gateway_request_end_at
            ) IS DISTINCT FROM ROW(
              OLD.gateway_transport_artifact_id, OLD.daily_calendar_authority_id,
              OLD.gateway_request_start_at, OLD.gateway_request_end_at
            ) THEN
              RAISE EXCEPTION 'Fosun transport lineage is immutable'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER trg_price_series_fosun_lineage_immutable "
        "BEFORE UPDATE OR DELETE ON price_series_manifests FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_fosun_lineage_change()"
    )
    op.execute(
        "CREATE TRIGGER trg_data_artifacts_fosun_transport_immutable "
        "BEFORE UPDATE OR DELETE ON data_artifacts FOR EACH ROW "
        "EXECUTE FUNCTION nexus_prevent_fosun_lineage_change()"
    )
