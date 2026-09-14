"""Admit bounded Schwab facts for quasi-realtime Agent paper cycles.

Revision ID: 0058_agent_paper_schwab_realtime
Revises: 0057_agent_paper_market_window
"""

import hashlib
import json
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0058_agent_paper_schwab_realtime"
down_revision: str | Sequence[str] | None = "0057_agent_paper_market_window"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHWAB_DECISION_POLICY_ID = "4d24df4a-b510-541b-8a6a-fe7db5afaac4"
SCHWAB_EXECUTION_POLICY_ID = "cf525f07-4e55-5f84-a839-60d845819ec5"
LEGACY_DECISION_POLICY_ID = "2573d63a-fb6d-53d3-8d44-207766a78322"
LEGACY_EXECUTION_POLICY_ID = "e99532cc-e24c-52a8-8165-c8076325fbd2"
_LOCK_KEY = "nexus:agent-paper-schwab-realtime:v1"

_DECISION_DOCUMENT: dict[str, object] = {
    "price_source": {
        "source": "alpaca",
        "feed": "sip",
        "delivery_mode": "delayed",
        "interval": "1Day",
        "adjustment": "all",
        "session": "previous_complete_xnys_session",
    },
    "market_context_source": "schwab",
    "fallback": "disabled",
    "schwab": {
        "feed": "l1",
        "delivery_mode": "real_time",
        "mode": "read_only",
        "delayed": False,
        "market_scope": "US",
    },
    "purpose": "agent_paper_quasi_realtime_decision_context",
    "quote": {
        "two_sided": True,
        "max_age_seconds": 60,
        "raw_artifact_required": True,
    },
}
_EXECUTION_DOCUMENT: dict[str, object] = {
    "selected_source": "schwab",
    "fallback": "disabled",
    "schwab": {
        "feed": "price_history",
        "delivery_mode": "unknown",
        "interval": "1Min",
        "adjustment": "raw",
        "mode": "read_only",
        "market_scope": "US",
    },
    "purpose": "agent_paper_fill_only",
    "window": {
        "minutes": 5,
        "row_count": 5,
        "max_receive_lag_seconds": 120,
        "single_instrument": True,
        "single_manifest_authority": True,
    },
    "raw_artifact_required": True,
}


def _payload(document: dict[str, object]) -> dict[str, object]:
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
    return {**document, "content_hash": hashlib.sha256(canonical.encode()).hexdigest()}


def _seed_policy(policy_id: str, key: str, version: str, document: dict[str, object]) -> None:
    payload = json.dumps(_payload(document), sort_keys=True, separators=(",", ":"))
    op.execute(
        sa.text(
            "INSERT INTO source_policies (id,key,version,schema_version,payload) VALUES "
            f"('{policy_id}'::uuid,'{key}','{version}','1',CAST(:payload AS jsonb))"
        ).bindparams(payload=payload)
    )


def _install_price_authority_validator(*, include_schwab: bool) -> None:
    schwab_branch = (
        f"""
            ELSIF NEW.source_policy_id='{SCHWAB_DECISION_POLICY_ID}'::uuid THEN
              SELECT * INTO prepared
              FROM public.nexus_prepare_agent_paper_decision_price_bar_set(
                NEW.price_series_manifest_id
              );
              expected_policy_id := '{SCHWAB_DECISION_POLICY_ID}'::uuid;
            ELSIF NEW.source_policy_id='{SCHWAB_EXECUTION_POLICY_ID}'::uuid THEN
              SELECT * INTO prepared
              FROM public.nexus_prepare_agent_paper_schwab_price_bar_set(
                NEW.price_series_manifest_id
              );
              expected_policy_id := '{SCHWAB_EXECUTION_POLICY_ID}'::uuid;
        """
        if include_schwab
        else ""
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
            IF NEW.source_policy_id='{LEGACY_EXECUTION_POLICY_ID}'::uuid THEN
              SELECT * INTO prepared FROM public.nexus_prepare_agent_paper_price_bar_set(
                NEW.price_series_manifest_id
              );
              expected_policy_id := '{LEGACY_EXECUTION_POLICY_ID}'::uuid;
            ELSIF NEW.source_policy_id='{LEGACY_DECISION_POLICY_ID}'::uuid THEN
              SELECT * INTO prepared
              FROM public.nexus_prepare_agent_paper_decision_price_bar_set(
                NEW.price_series_manifest_id
              );
              expected_policy_id := '{LEGACY_DECISION_POLICY_ID}'::uuid;
            {schwab_branch}
            ELSE
              SELECT * INTO prepared FROM public.nexus_prepare_price_bar_set(
                NEW.price_series_manifest_id
              );
              expected_policy_id := '3c60888d-e92d-5e04-913c-7d427004cfef'::uuid;
            END IF;
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


def _install_market_window_validator(*, include_schwab: bool) -> None:
    schwab_contract = (
        f"""
              OR (
                cycle.decision_price_policy_id='{SCHWAB_DECISION_POLICY_ID}'::uuid
                AND cycle.execution_price_policy_id='{SCHWAB_EXECUTION_POLICY_ID}'::uuid
                AND authority.source_policy_id='{SCHWAB_EXECUTION_POLICY_ID}'::uuid
                AND NEW.source='schwab' AND NEW.feed='price_history'
                AND NEW.delivery_mode='unknown'
                AND manifest.source='schwab' AND manifest.feed='price_history'
                AND manifest.delivery_mode='unknown'
                AND artifact.source='schwab'
                AND artifact.schema_version='schwab_price_history_raw_v1'
                AND NEW.data_available_at >= NEW.window_end_at
                AND NEW.data_available_at <= NEW.window_end_at + interval '120 seconds'
              )
        """
        if include_schwab
        else ""
    )
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION nexus_validate_agent_paper_market_window()
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
              AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(cycle.candidate_manifest) AS candidate
                WHERE candidate->>'instrument_id'=NEW.instrument_id::text
              )
              AND authority.price_series_manifest_id=manifest.id
              AND authority.source=NEW.source
              AND authority.content_hash=NEW.source_content_hash
              AND authority.available_at=NEW.data_available_at
              AND manifest.instrument=NEW.instrument_id::text
              AND manifest.interval='1Min' AND manifest.adjustment='raw'
              AND manifest.row_count=5
              AND manifest.start_at=NEW.window_start_at
              AND manifest.end_at=NEW.window_end_at - interval '1 minute'
              AND artifact.sha256=NEW.raw_content_hash
              AND (
                (
                  cycle.decision_price_policy_id='{LEGACY_DECISION_POLICY_ID}'::uuid
                  AND cycle.execution_price_policy_id='{LEGACY_EXECUTION_POLICY_ID}'::uuid
                  AND authority.source_policy_id='{LEGACY_EXECUTION_POLICY_ID}'::uuid
                  AND NEW.source='alpaca' AND NEW.feed='iex'
                  AND NEW.delivery_mode='real_time'
                  AND manifest.source='alpaca' AND manifest.feed='iex'
                  AND manifest.delivery_mode='real_time'
                  AND artifact.source='alpaca'
                  AND artifact.schema_version='alpaca_iex_bars_raw_v1'
                )
                {schwab_contract}
              )
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


def _replace_cycle_constraints(*, include_schwab: bool) -> None:
    op.drop_constraint("ck_agent_paper_cycles_sessions_ordered", "agent_paper_cycles")
    op.drop_constraint("ck_agent_paper_cycles_policies_fixed", "agent_paper_cycles")
    if include_schwab:
        op.create_check_constraint(
            "sessions_ordered",
            "agent_paper_cycles",
            "execution_session >= decision_session",
        )
        op.create_check_constraint(
            "policies_fixed",
            "agent_paper_cycles",
            f"((decision_price_policy_id = '{LEGACY_DECISION_POLICY_ID}'::uuid "
            f"AND execution_price_policy_id = '{LEGACY_EXECUTION_POLICY_ID}'::uuid "
            "AND execution_session > decision_session) OR "
            f"(decision_price_policy_id = '{SCHWAB_DECISION_POLICY_ID}'::uuid "
            f"AND execution_price_policy_id = '{SCHWAB_EXECUTION_POLICY_ID}'::uuid "
            "AND execution_session = decision_session))",
        )
    else:
        op.create_check_constraint(
            "sessions_ordered",
            "agent_paper_cycles",
            "execution_session > decision_session",
        )
        op.create_check_constraint(
            "policies_fixed",
            "agent_paper_cycles",
            f"decision_price_policy_id = '{LEGACY_DECISION_POLICY_ID}'::uuid AND "
            f"execution_price_policy_id = '{LEGACY_EXECUTION_POLICY_ID}'::uuid",
        )


def _replace_strategy_decision_constraints(*, include_schwab: bool) -> None:
    op.drop_constraint("ck_strategy_decisions_origin_authority", "strategy_decisions")
    op.drop_constraint("ck_strategy_decisions_sessions_ordered", "strategy_decisions")
    registered = (
        "(decision_origin = 'registered_strategy' "
        "AND num_nonnulls(strategy_definition_id, registry_event_id, champion_generation) = 3 "
        "AND champion_generation >= 1 AND execution_source_policy_id IS NULL "
        "AND quality_report_id IS NOT NULL AND reason = 'WEEKLY_REBALANCE' "
        "AND execution_session > decision_session)"
    )
    agent_base = (
        "decision_origin = 'agent_research' AND strategy_id = 'agent_research' "
        "AND strategy_version = 'agent_paper_decision_v1' "
        "AND num_nonnulls(strategy_definition_id, registry_event_id, champion_generation) = 0 "
        "AND quality_report_id IS NULL AND reason = 'AGENT_RESEARCH'"
    )
    legacy_pair = (
        f"source_policy_id = '{LEGACY_DECISION_POLICY_ID}'::uuid "
        f"AND execution_source_policy_id = '{LEGACY_EXECUTION_POLICY_ID}'::uuid "
        "AND execution_session > decision_session"
    )
    if include_schwab:
        schwab_pair = (
            f"source_policy_id = '{SCHWAB_DECISION_POLICY_ID}'::uuid "
            f"AND execution_source_policy_id = '{SCHWAB_EXECUTION_POLICY_ID}'::uuid "
            "AND execution_session = decision_session"
        )
        agent = f"({agent_base} AND (({legacy_pair}) OR ({schwab_pair})))"
        sessions = "execution_session >= decision_session"
    else:
        agent = f"({agent_base} AND {legacy_pair})"
        sessions = "execution_session > decision_session"
    op.create_check_constraint(
        "origin_authority",
        "strategy_decisions",
        f"{registered} OR {agent}",
    )
    op.create_check_constraint("sessions_ordered", "strategy_decisions", sessions)


def _replace_market_source_constraint(*, include_schwab: bool) -> None:
    op.execute(
        """
        DO $$
        DECLARE source_constraint name;
        BEGIN
          SELECT con.conname INTO STRICT source_constraint
          FROM pg_catalog.pg_constraint AS con
          JOIN pg_catalog.pg_class AS rel ON rel.oid=con.conrelid
          JOIN pg_catalog.pg_namespace AS ns ON ns.oid=rel.relnamespace
          WHERE ns.nspname='public'
            AND rel.relname='agent_paper_market_windows'
            AND con.contype='c'
            AND pg_catalog.pg_get_constraintdef(con.oid) LIKE '%alpaca%'
            AND pg_catalog.pg_get_constraintdef(con.oid) LIKE '%iex%'
            AND pg_catalog.pg_get_constraintdef(con.oid) LIKE '%delivery_mode%';
          EXECUTE pg_catalog.format(
            'ALTER TABLE public.agent_paper_market_windows DROP CONSTRAINT %I',
            source_constraint
          );
        END $$
        """
    )
    condition = "source='alpaca' AND feed='iex' AND delivery_mode='real_time'"
    if include_schwab:
        condition = (
            f"({condition}) OR "
            "(source='schwab' AND feed='price_history' AND delivery_mode='unknown')"
        )
    op.create_check_constraint("source_fixed", "agent_paper_market_windows", condition)


def upgrade() -> None:
    op.execute(f"SELECT pg_advisory_xact_lock(hashtextextended('{_LOCK_KEY}', 0))")
    _seed_policy(
        SCHWAB_DECISION_POLICY_ID,
        "paper_decision_price",
        "paper_schwab_l1_decision_v1",
        _DECISION_DOCUMENT,
    )
    _seed_policy(
        SCHWAB_EXECUTION_POLICY_ID,
        "paper_execution_price",
        "paper_schwab_price_history_execution_v1",
        _EXECUTION_DOCUMENT,
    )
    _replace_cycle_constraints(include_schwab=True)
    _replace_strategy_decision_constraints(include_schwab=True)
    op.execute(
        f"""
        CREATE FUNCTION nexus_prepare_agent_paper_schwab_price_bar_set(p_manifest_id uuid)
        RETURNS TABLE (
          content_hash text, byte_length bigint, bar_count integer,
          first_session date, last_session date, available_at timestamptz
        )
        LANGUAGE plpgsql
        SET search_path TO pg_catalog, public, pg_temp AS $$
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
            manifest.source='schwab' AND manifest.feed='price_history'
            AND manifest.delivery_mode='unknown' AND manifest.interval='1Min'
            AND manifest.adjustment='raw' AND manifest.row_count=5
            AND manifest.end_at=manifest.start_at + interval '4 minutes'
            AND actual_count=5 AND distinct_observation_count=5
            AND max_received >= manifest.end_at + interval '1 minute'
            AND max_received <= manifest.end_at + interval '3 minutes'
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
              WHERE policy.id='{SCHWAB_EXECUTION_POLICY_ID}'::uuid
                AND policy.key='paper_execution_price'
                AND policy.version='paper_schwab_price_history_execution_v1'
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
    _install_price_authority_validator(include_schwab=True)
    _replace_market_source_constraint(include_schwab=True)
    _install_market_window_validator(include_schwab=True)


def downgrade() -> None:
    op.execute(f"SELECT pg_advisory_xact_lock(hashtextextended('{_LOCK_KEY}', 0))")
    op.execute(
        f"""
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM public.agent_paper_cycles
            WHERE decision_price_policy_id='{SCHWAB_DECISION_POLICY_ID}'::uuid
               OR execution_price_policy_id='{SCHWAB_EXECUTION_POLICY_ID}'::uuid
          ) OR EXISTS (
            SELECT 1 FROM public.strategy_decisions
            WHERE source_policy_id='{SCHWAB_DECISION_POLICY_ID}'::uuid
               OR execution_source_policy_id='{SCHWAB_EXECUTION_POLICY_ID}'::uuid
          ) OR EXISTS (
            SELECT 1 FROM public.price_bar_set_authorities
            WHERE source_policy_id='{SCHWAB_EXECUTION_POLICY_ID}'::uuid
          ) OR EXISTS (
            SELECT 1 FROM public.agent_paper_market_windows
            WHERE source='schwab' OR feed='price_history' OR delivery_mode='unknown'
          ) THEN
            RAISE EXCEPTION 'SCHWAB_AGENT_PAPER_FACTS_EXIST' USING ERRCODE='23514';
          END IF;
        END $$
        """
    )
    _install_market_window_validator(include_schwab=False)
    _replace_market_source_constraint(include_schwab=False)
    _install_price_authority_validator(include_schwab=False)
    op.execute("DROP FUNCTION nexus_prepare_agent_paper_schwab_price_bar_set(uuid)")
    _replace_strategy_decision_constraints(include_schwab=False)
    _replace_cycle_constraints(include_schwab=False)
    op.execute("ALTER TABLE source_policies DISABLE TRIGGER trg_source_policies_immutable")
    op.execute(
        "DELETE FROM source_policies WHERE id IN "
        f"('{SCHWAB_DECISION_POLICY_ID}'::uuid,'{SCHWAB_EXECUTION_POLICY_ID}'::uuid)"
    )
    op.execute("ALTER TABLE source_policies ENABLE TRIGGER trg_source_policies_immutable")
