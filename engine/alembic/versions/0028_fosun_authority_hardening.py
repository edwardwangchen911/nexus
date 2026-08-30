"""Pin authority functions to trusted schemas.

Revision ID: 0028_fosun_authority_hardening
Revises: 0027_fosun_market_authority
Create Date: 2026-08-30
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0028_fosun_authority_hardening"
down_revision: str | Sequence[str] | None = "0027_fosun_market_authority"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_AUTHORITY_FUNCTIONS: tuple[tuple[str, str], ...] = (
    ("nexus_price_bar_set_canonical_v1", "uuid"),
    ("nexus_prepare_price_bar_set", "uuid"),
    ("nexus_intraday_bar_set_canonical_v1", "uuid"),
    ("nexus_prepare_intraday_bar_set", "uuid"),
    ("nexus_price_bar_set_canonical_v2", "uuid"),
    ("nexus_prepare_price_bar_set_v2", "uuid"),
    ("nexus_intraday_bar_set_canonical_v2", "uuid"),
    ("nexus_prepare_intraday_bar_set_v2", "uuid"),
    ("nexus_validate_price_bar_set_authority", ""),
    ("nexus_validate_intraday_bar_set_authority", ""),
    ("nexus_guard_price_bar_set_mutation", ""),
    ("nexus_validate_latest_quote_raw_artifact", ""),
    ("nexus_validate_intraday_review_authority", ""),
    ("nexus_prevent_frozen_backtest_authority_change", ""),
    ("nexus_validate_agent_snapshot_evidence", ""),
    ("nexus_prevent_provider_capability_change", ""),
    ("nexus_validate_fosun_market_binding", ""),
    ("nexus_prevent_fosun_market_binding_change", ""),
)


def upgrade() -> None:
    """Keep pg_temp behind trusted schemas for every market-authority path."""
    for name, arguments in _AUTHORITY_FUNCTIONS:
        op.execute(
            f"ALTER FUNCTION public.{name}({arguments}) "
            "SET search_path TO pg_catalog, public, pg_temp"
        )
    _install_market_input_mutation_lock()
    _install_provider_publication_function()
    _install_provider_price_bar_guard()


def downgrade() -> None:
    """Restore the function configuration owned by the preceding revisions."""
    op.execute(
        """
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM public.price_series_manifests
            WHERE provider_capability_id IS NOT NULL
          ) OR EXISTS (
            SELECT 1 FROM public.latest_quotes
            WHERE provider_capability_id IS NOT NULL
          ) OR EXISTS (
            SELECT 1 FROM public.price_bar_set_authorities
            WHERE hash_version = 'price_bar_set_sha256_v2'
          ) OR EXISTS (
            SELECT 1 FROM public.intraday_bar_set_authorities
            WHERE hash_version = 'intraday_bar_set_sha256_v2'
          ) OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles AS role
            WHERE role.rolname <> current_user
              AND NOT role.rolsuper
              AND role.rolcanlogin
              AND (
                has_table_privilege(role.oid, 'public.price_series_manifests', 'INSERT')
                OR has_table_privilege(role.oid, 'public.latest_quotes', 'INSERT')
                OR has_table_privilege(role.oid, 'public.price_bar_set_authorities', 'INSERT')
                OR has_table_privilege(
                  role.oid, 'public.intraday_bar_set_authorities', 'INSERT'
                )
              )
          ) THEN
            RAISE EXCEPTION 'Fosun authority hardening prevents downgrade'
              USING ERRCODE = 'P3D01';
          END IF;
        END $$
        """
    )
    _restore_owner_price_bar_guard()
    _restore_owner_publication_triggers()
    _drop_market_input_mutation_lock()
    for name, arguments in reversed(_AUTHORITY_FUNCTIONS):
        op.execute(f"ALTER FUNCTION public.{name}({arguments}) RESET search_path")


def _install_market_input_mutation_lock() -> None:
    op.execute(
        """
        CREATE FUNCTION public.nexus_lock_fosun_market_input_change()
        RETURNS trigger LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
        DECLARE
          row_id text;
          old_source text;
          new_source text;
        BEGIN
          IF TG_OP NOT IN ('UPDATE','DELETE')
             OR TG_LEVEL <> 'ROW'
             OR TG_WHEN <> 'BEFORE'
             OR TG_TABLE_SCHEMA <> 'public' THEN
            RAISE EXCEPTION 'unsupported Fosun market input lock context'
              USING ERRCODE = '23514';
          END IF;
          row_id := to_jsonb(OLD) ->> 'id';
          old_source := CASE TG_TABLE_NAME
            WHEN 'integration_status' THEN to_jsonb(OLD) ->> 'provider'
            WHEN 'data_artifacts' THEN to_jsonb(OLD) ->> 'source'
            WHEN 'price_series_manifests' THEN to_jsonb(OLD) ->> 'source'
            ELSE NULL
          END;
          IF TG_OP = 'UPDATE' THEN
            new_source := CASE TG_TABLE_NAME
              WHEN 'integration_status' THEN to_jsonb(NEW) ->> 'provider'
              WHEN 'data_artifacts' THEN to_jsonb(NEW) ->> 'source'
              WHEN 'price_series_manifests' THEN to_jsonb(NEW) ->> 'source'
              ELSE NULL
            END;
          END IF;
          IF TG_TABLE_NAME NOT IN (
            'integration_status','data_artifacts','price_series_manifests'
          ) OR row_id IS NULL THEN
            RAISE EXCEPTION 'unsupported Fosun market input lock relation'
              USING ERRCODE = '23514';
          END IF;
          IF old_source = 'fosun' OR new_source = 'fosun' THEN
            IF TG_TABLE_NAME = 'integration_status' THEN
              PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended(
                  'fosun-integration-status:' || row_id, 0
                )
              );
            ELSIF TG_TABLE_NAME = 'data_artifacts' THEN
              PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended('fosun-data-artifact:' || row_id, 0)
              );
            ELSE
              PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended('price-series-manifest:' || row_id, 0)
              );
              PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended(
                  'intraday-price-series-manifest:' || row_id, 0
                )
              );
            END IF;
          END IF;
          IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    for table_name in (
        "integration_status",
        "data_artifacts",
        "price_series_manifests",
    ):
        op.execute(
            f"CREATE TRIGGER trg_00_{table_name}_fosun_authority_lock "
            f"BEFORE UPDATE OR DELETE ON public.{table_name} FOR EACH ROW "
            "EXECUTE FUNCTION public.nexus_lock_fosun_market_input_change()"
        )


def _drop_market_input_mutation_lock() -> None:
    for table_name in (
        "integration_status",
        "data_artifacts",
        "price_series_manifests",
    ):
        op.execute(
            f"DROP TRIGGER trg_00_{table_name}_fosun_authority_lock "
            f"ON public.{table_name}"
        )
    op.execute("DROP FUNCTION public.nexus_lock_fosun_market_input_change()")


def _install_provider_publication_function() -> None:
    op.execute(
        """
        CREATE FUNCTION public.nexus_mark_provider_backtest_authority_publication()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path TO pg_catalog, public, pg_temp
        AS $$
        DECLARE
          publication_domain text;
        BEGIN
          IF TG_OP <> 'INSERT'
             OR TG_LEVEL <> 'ROW'
             OR TG_WHEN <> 'AFTER'
             OR TG_TABLE_SCHEMA <> 'public'
          THEN
            RAISE EXCEPTION 'unsupported provider publication trigger context'
              USING ERRCODE = '23514';
          END IF;
          publication_domain := CASE TG_RELID
            WHEN 'public.corporate_actions'::pg_catalog.regclass
              THEN 'corporate_action'
            WHEN 'public.price_bar_set_authorities'::pg_catalog.regclass
              THEN 'price'
          END;
          IF publication_domain IS NULL THEN
            RAISE EXCEPTION 'unsupported provider publication trigger relation'
              USING ERRCODE = '23514';
          END IF;
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('backtest-authority-global', 1602)
          );
          INSERT INTO public.backtest_authority_publication_marks
            (domain, record_id, created_at)
          VALUES (publication_domain, NEW.id, pg_catalog.clock_timestamp())
          ON CONFLICT (domain, record_id) DO NOTHING;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "ALTER FUNCTION public.nexus_mark_provider_backtest_authority_publication() "
        "OWNER TO CURRENT_USER"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.nexus_mark_provider_backtest_authority_publication() FROM PUBLIC"
    )
    for table_name in ("corporate_actions", "price_bar_set_authorities"):
        op.execute(
            f"DROP TRIGGER trg_{table_name}_backtest_publication "
            f"ON public.{table_name}"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_backtest_publication "
            f"AFTER INSERT ON public.{table_name} FOR EACH ROW "
            "EXECUTE FUNCTION "
            "public.nexus_mark_provider_backtest_authority_publication()"
        )


def _restore_owner_publication_triggers() -> None:
    for table_name in ("corporate_actions", "price_bar_set_authorities"):
        op.execute(
            f"DROP TRIGGER trg_{table_name}_backtest_publication "
            f"ON public.{table_name}"
        )
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_backtest_publication "
            f"AFTER INSERT ON public.{table_name} FOR EACH ROW "
            "EXECUTE FUNCTION public.nexus_mark_backtest_authority_publication()"
        )
    op.execute(
        "DROP FUNCTION public.nexus_mark_provider_backtest_authority_publication()"
    )


def _install_provider_price_bar_guard() -> None:
    op.execute(
        """
        CREATE FUNCTION public.nexus_guard_provider_price_bar_frozen_insert()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path TO pg_catalog, public, pg_temp
        AS $$
        DECLARE
          referenced boolean := false;
        BEGIN
          IF TG_OP <> 'INSERT'
             OR TG_LEVEL <> 'ROW'
             OR TG_WHEN <> 'BEFORE'
             OR TG_TABLE_SCHEMA <> 'public'
             OR TG_RELID <> 'public.price_bar_records'::pg_catalog.regclass
          THEN
            RAISE EXCEPTION 'unsupported provider price bar guard context'
              USING ERRCODE = '23514';
          END IF;
          PERFORM pg_catalog.pg_advisory_xact_lock(
            pg_catalog.hashtextextended('backtest-authority-global', 1602)
          );
          SELECT EXISTS (
            SELECT 1
            FROM public.backtest_price_authority_members AS member
            JOIN public.price_bar_set_authorities AS authority
              ON authority.id = member.price_bar_set_authority_id
            WHERE authority.price_series_manifest_id = NEW.price_series_manifest_id
          ) INTO referenced;
          IF referenced THEN
            RAISE EXCEPTION 'backtest upstream authority is frozen'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "ALTER FUNCTION public.nexus_guard_provider_price_bar_frozen_insert() "
        "OWNER TO CURRENT_USER"
    )
    op.execute(
        "REVOKE ALL ON FUNCTION "
        "public.nexus_guard_provider_price_bar_frozen_insert() FROM PUBLIC"
    )
    op.execute(
        "DROP TRIGGER trg_00_backtest_price_bar_frozen_insert "
        "ON public.price_bar_records"
    )
    op.execute(
        "CREATE TRIGGER trg_00_backtest_price_bar_frozen_insert "
        "BEFORE INSERT ON public.price_bar_records FOR EACH ROW "
        "EXECUTE FUNCTION public.nexus_guard_provider_price_bar_frozen_insert()"
    )


def _restore_owner_price_bar_guard() -> None:
    op.execute(
        "DROP TRIGGER trg_00_backtest_price_bar_frozen_insert "
        "ON public.price_bar_records"
    )
    op.execute(
        "CREATE TRIGGER trg_00_backtest_price_bar_frozen_insert "
        "BEFORE INSERT ON public.price_bar_records FOR EACH ROW "
        "EXECUTE FUNCTION public.nexus_prevent_frozen_backtest_authority_change()"
    )
    op.execute(
        "DROP FUNCTION public.nexus_guard_provider_price_bar_frozen_insert()"
    )
