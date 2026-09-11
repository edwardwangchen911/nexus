"""Provider-only composition root with no Agent or broker capability."""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, cast

import httpx
from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.domain.ids import new_uuid7
from nexus.jobs.daily_profiles import PRIMARY_DAILY_PROFILE
from nexus.jobs.intraday_collection import (
    IntradaySnapshotProvider,
    build_intraday_collection_registry,
)
from nexus.jobs.intraday_schedule import (
    IntradaySnapshotJobPayloadV2,
    schedule_due_intraday_snapshot,
)
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.provider_collection import RawEvidenceStore, build_provider_collection_registry
from nexus.jobs.registry import JobHandlerContext, JobHandlerResult, JobHandlerSpec, JobRegistry
from nexus.jobs.worker import run_job_worker_once
from nexus.market.ingestion import MarketObjectStore
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.runtime import Job
from nexus.providers.composition import (
    FosunGatewayFactory,
    build_provider_bundle,
)
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.providers.market.fosun_gateway import FosunMarketGatewayClient
from nexus.providers.settings import (
    IntradayMonitorMode,
    ProviderRuntimeMode,
    ProviderSettings,
    ProviderWorkerSettings,
)

_PROCESS_VERSION = "nexus-provider-phase3-intraday"
_CURRENT_INTRADAY_JOB_SCHEMA = "intraday_snapshot_job_v2"
_PROVIDER_INTRADAY_BUCKET_INDEX_DEFINITION = (
    "CREATE UNIQUE INDEX uq_jobs_intraday_v2_bucket ON public.jobs USING btree "
    "(effective_at) WHERE (((job_type)::text = "
    "'collect.intraday_snapshot'::text) AND ((schema_version)::text = "
    "'intraday_snapshot_job_v2'::text))"
)
_PROVIDER_TICKER_ALIAS_CONSTRAINTS = {
    "ex_instrument_aliases_ticker_instrument_period": (
        "EXCLUDE USING gist (instrument_id WITH =, "
        "daterange(valid_from, valid_to, '[]'::text) WITH &&) "
        "WHERE (alias_type::text = 'ticker'::text)"
    ),
    "ex_instrument_aliases_ticker_symbol_period": (
        "EXCLUDE USING gist (upper(symbol::text) WITH =, "
        "daterange(valid_from, valid_to, '[]'::text) WITH &&) "
        "WHERE (alias_type::text = 'ticker'::text)"
    ),
}
_ACTIONABLE_INTRADAY_JOB_STATUSES = ("queued", "running", "failed")
_INTRADAY_SCHEMA_ADMISSION_LOCK = SCHEMA_ADMISSION_LOCK_KEY
_PROVIDER_PRIVILEGES: dict[str, frozenset[str]] = {
    "alembic_version": frozenset({"SELECT"}),
    "jobs": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "job_leases": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "job_attempts": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "job_dependencies": frozenset({"SELECT"}),
    "worker_heartbeats": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "integration_status": frozenset({"SELECT"}),
    "provider_capabilities": frozenset({"SELECT"}),
    "source_policies": frozenset({"SELECT"}),
    "instruments": frozenset({"SELECT"}),
    "instrument_aliases": frozenset({"SELECT"}),
    "universe_snapshots": frozenset({"SELECT"}),
    "universe_members": frozenset({"SELECT"}),
    "watchlists": frozenset({"SELECT"}),
    "watchlist_members": frozenset({"SELECT"}),
    "exchange_calendar_authorities": frozenset({"SELECT"}),
    "exchange_calendar_sessions": frozenset({"SELECT"}),
    "data_artifacts": frozenset({"SELECT", "INSERT"}),
    "fosun_daily_bar_staging": frozenset({"SELECT", "INSERT"}),
    "research_evidence_refs": frozenset({"SELECT", "INSERT"}),
    "price_series_manifests": frozenset({"SELECT", "INSERT"}),
    "price_bar_records": frozenset({"SELECT", "INSERT"}),
    "price_bar_set_authorities": frozenset({"SELECT", "INSERT"}),
    "intraday_bar_set_authorities": frozenset({"SELECT", "INSERT"}),
    "latest_quotes": frozenset({"SELECT", "INSERT"}),
    "filings": frozenset({"SELECT", "INSERT"}),
    "financial_facts": frozenset({"SELECT", "INSERT"}),
    "corporate_actions": frozenset({"SELECT", "INSERT"}),
    "macro_observations": frozenset({"SELECT", "INSERT"}),
}
_ROW_SCOPED_PROVIDER_TABLES = frozenset(
    {
        "jobs",
        "job_leases",
        "job_attempts",
        "job_dependencies",
        "worker_heartbeats",
        "fosun_daily_bar_staging",
    }
)


@dataclass(frozen=True)
class _PolicyFunctionSpec:
    identity_arguments: str
    source: str
    volatility: str


@dataclass(frozen=True)
class _PublicationFunctionSpec:
    name: str
    identity_arguments: str
    source: str


@dataclass(frozen=True)
class _ProviderTriggerFunctionSpec:
    source_sha256: str
    search_path_exact: bool
    security_definer: bool = False


_PROVIDER_POLICY_FUNCTIONS = {
    "nexus_provider_job_visible": _PolicyFunctionSpec(
        identity_arguments="p_queue text, p_job_type text",
        source=(
            "SELECT (p_queue = 'provider' AND p_job_type IN "
            "('collect.market','collect.sec_macro','collect.intraday_snapshot',"
            "'collect.fosun_daily_instrument','publish.fosun_daily')) "
            "OR (p_queue = 'general' AND p_job_type = 'daily.pipeline')"
        ),
        volatility="i",
    ),
    "nexus_provider_job_writable": _PolicyFunctionSpec(
        identity_arguments="p_queue text, p_job_type text",
        source=(
            "SELECT p_queue = 'provider' AND p_job_type IN "
            "('collect.market','collect.sec_macro','collect.intraday_snapshot',"
            "'collect.fosun_daily_instrument','publish.fosun_daily')"
        ),
        volatility="i",
    ),
    "nexus_provider_job_owned": _PolicyFunctionSpec(
        identity_arguments="p_job_id uuid",
        source=(
            "SELECT EXISTS (SELECT 1 FROM public.jobs AS job WHERE job.id = p_job_id "
            "AND public.nexus_provider_job_writable(job.queue, job.job_type))"
        ),
        volatility="s",
    ),
    "nexus_provider_queue": _PolicyFunctionSpec(
        identity_arguments="p_queue text",
        source="SELECT p_queue = 'provider'",
        volatility="i",
    ),
}
_PROVIDER_PUBLICATION_FUNCTION = _PublicationFunctionSpec(
    name="nexus_mark_provider_backtest_authority_publication",
    identity_arguments="",
    source="""
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
    """,
)
_PROVIDER_PUBLICATION_TRIGGERS = {
    "corporate_actions": "trg_corporate_actions_backtest_publication",
    "price_bar_set_authorities": ("trg_price_bar_set_authorities_backtest_publication"),
}
_PROVIDER_PRICE_BAR_GUARD_FUNCTION = _PublicationFunctionSpec(
    name="nexus_guard_provider_price_bar_frozen_insert",
    identity_arguments="",
    source="""
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
    """,
)
_PROVIDER_PRICE_BAR_GUARD_TRIGGERS = {
    "price_bar_records": "trg_00_backtest_price_bar_frozen_insert",
}
_PROVIDER_SECURITY_DEFINER_FUNCTIONS = {
    spec.name: spec
    for spec in (
        _PROVIDER_PUBLICATION_FUNCTION,
        _PROVIDER_PRICE_BAR_GUARD_FUNCTION,
    )
}
_PROVIDER_SECURITY_DEFINER_TRIGGERS = {
    **{
        (table_name, trigger_name): (
            _PROVIDER_PUBLICATION_FUNCTION.name,
            _PROVIDER_PUBLICATION_FUNCTION.identity_arguments,
            5,
        )
        for table_name, trigger_name in _PROVIDER_PUBLICATION_TRIGGERS.items()
    },
    **{
        (table_name, trigger_name): (
            _PROVIDER_PRICE_BAR_GUARD_FUNCTION.name,
            _PROVIDER_PRICE_BAR_GUARD_FUNCTION.identity_arguments,
            7,
        )
        for table_name, trigger_name in _PROVIDER_PRICE_BAR_GUARD_TRIGGERS.items()
    },
}
_PROVIDER_TRIGGER_FUNCTIONS = {
    "nexus_guard_intraday_job_schema_admission": _ProviderTriggerFunctionSpec(
        "532d7cdb9bf48ddb50fe129dfda5845ea9ad62b63771d0611ecfc18e25fb916f",
        True,
    ),
    "nexus_guard_job_attempt": _ProviderTriggerFunctionSpec(
        "c9ed462f54862a1228bd0d727532ce4055d9618c278f4b708fd2ec9bbaef0d4a",
        False,
    ),
    "nexus_guard_job_lease": _ProviderTriggerFunctionSpec(
        "958d3a8d350e444b94281b696ec4622961aab33f0ceaed1fc7cee7cbae71e8ad",
        False,
    ),
    "nexus_guard_job_state": _ProviderTriggerFunctionSpec(
        "e348a08f1b3d60df05e0db62756d69948118f07578ea1f52b0791b275dfd7137",
        False,
    ),
    "nexus_guard_price_bar_set_mutation": _ProviderTriggerFunctionSpec(
        "979983f6d9d64a79aa804a7d4702bba526e1934cd75af263fdb1c966125c9bb7",
        True,
    ),
    "nexus_guard_provider_price_bar_frozen_insert": _ProviderTriggerFunctionSpec(
        "3bd73b20571a3f2aaa0388b921bf143294ac959d08402071a4ae55ab35aaaa98",
        True,
        True,
    ),
    "nexus_guard_worker_heartbeat": _ProviderTriggerFunctionSpec(
        "12d892a66db6e8f2bdaf80e472bf75b8d7bdbdf49c390d3954c64cf5bef17ee3",
        False,
    ),
    "nexus_lock_fosun_market_input_change": _ProviderTriggerFunctionSpec(
        "f23a03349b77378e580ce141ae9d0bfd3ed048e18f7190d741084f2d6f2b189b",
        True,
    ),
    "nexus_lock_provider_capability_scope": _ProviderTriggerFunctionSpec(
        "98c8ca71e953b325be6834040802195ba6070032429a01ca44dd81df0cfa357f",
        False,
    ),
    "nexus_mark_provider_backtest_authority_publication": (
        _ProviderTriggerFunctionSpec(
            "f02fe33d9d9f534151a85fd0c3f918b7e029f8dc738dc51f67b73a9844b8ba56",
            True,
            True,
        )
    ),
    "nexus_prevent_agent_immutable_change": _ProviderTriggerFunctionSpec(
        "2d3449b1d7954a07ce6edb62a85a6474f5dad8654c8373934be05f3739bd9143",
        False,
    ),
    "nexus_prevent_created_at_change": _ProviderTriggerFunctionSpec(
        "4e79513bb12e031d4792b8f1efd73c62b8e350d053c74bf0dd7c525599da6f28",
        False,
    ),
    "nexus_prevent_diagnostic_authority_change": _ProviderTriggerFunctionSpec(
        "81b0ad65b15dd80ef090b3b38546ed32b67f472eb0e99132ca14e34de07aab3b",
        False,
    ),
    "nexus_prevent_fosun_lineage_change": _ProviderTriggerFunctionSpec(
        "a70b0c91727199bbcbe65907bec159f28f67440e5a6cee0abd52b25ed012972e",
        True,
    ),
    "nexus_prevent_fosun_daily_staging_change": _ProviderTriggerFunctionSpec(
        "e9fd33467be00bf6388e71644221af4d4fa41cb26a9a0f39f360ee4369f48d9a",
        True,
    ),
    "nexus_prevent_fosun_market_binding_change": _ProviderTriggerFunctionSpec(
        "34874d7f04eee793675ddd42aef1c00b9d1921cec2162334aef4e265aa938c38",
        True,
    ),
    "nexus_prevent_frozen_backtest_authority_change": (
        _ProviderTriggerFunctionSpec(
            "5e30851fa3bf12f651d27618a5d56d9f7c3a324a2d48c5f87332d3ca83e3e5ec",
            True,
        )
    ),
    "nexus_prevent_frozen_price_authority_change": _ProviderTriggerFunctionSpec(
        "06080cee9b32240133e4239e0804969b83fc66c297e2c80210fd7c040e66311f",
        False,
    ),
    "nexus_prevent_intraday_quote_change": _ProviderTriggerFunctionSpec(
        "762ccf9de63d82f829fee2ac4470f40202e25117eff0ce4a40f1a69e11de300e",
        False,
    ),
    "nexus_prevent_legacy_macro_insert": _ProviderTriggerFunctionSpec(
        "7a65f072f2191d41c05059a3cdeaca03dd5a644e00ff6f2a8c2d5bb114700c40",
        False,
    ),
    "nexus_prevent_provider_capability_change": _ProviderTriggerFunctionSpec(
        "78c50e067b69dc282c2fa4a4af49d8e1c87d6a579aab14af2e44301da34a726b",
        True,
    ),
    "nexus_prevent_research_change": _ProviderTriggerFunctionSpec(
        "b2de73cf8099adef23aaa38d1dc9feefcffe88147b60ee3485e2b485a687341b",
        False,
    ),
    "nexus_validate_fosun_authority_transport": _ProviderTriggerFunctionSpec(
        "9a00f611857a61674d8f863a3623093962f4c913ef54df63865057778bb62627",
        True,
    ),
    "nexus_validate_fosun_bar_transport": _ProviderTriggerFunctionSpec(
        "bc9d9d7c295c341d8186e5d6701a16ee73aade7d15346278283b4e7213dc2311",
        True,
    ),
    "nexus_validate_fosun_manifest_transport": _ProviderTriggerFunctionSpec(
        "7e36f0ad0bac521ddd57c1e60239e0ff22db0bba4f030fa096da8e69603352fa",
        True,
    ),
    "nexus_validate_fosun_market_binding": _ProviderTriggerFunctionSpec(
        "da60c12b5dc22cba5bdfa6e7332cb2bc0ca403eb1124525f1e2d25e0dea6ce3b",
        True,
    ),
    "nexus_validate_intraday_bar_set_authority": _ProviderTriggerFunctionSpec(
        "927714be11a20095bcf49a179e3bf6ce5b787cde22e0a359de588259233d0fcc",
        True,
    ),
    "nexus_validate_intraday_review_authority": _ProviderTriggerFunctionSpec(
        "487041ed1700dd9bd6a318c64951430c88d02a1dc074276a50306860cdf7ed68",
        True,
    ),
    "nexus_validate_latest_quote_raw_artifact": _ProviderTriggerFunctionSpec(
        "761a713cf2f49dcc325bbfbe0b0572aed7225431d8831fc150539973ce03b609",
        True,
    ),
    "nexus_validate_price_bar_set_authority": _ProviderTriggerFunctionSpec(
        "c0c4208f3b3b2318613e9848ad8a4407b0041f47fdee9201a6abc0df45b1acc5",
        True,
    ),
}
_PROVIDER_TRIGGER_TOPOLOGY = {
    (
        "corporate_actions",
        "trg_corporate_actions_backtest_frozen",
    ): ("nexus_prevent_frozen_backtest_authority_change", 27),
    (
        "corporate_actions",
        "trg_corporate_actions_backtest_publication",
    ): ("nexus_mark_provider_backtest_authority_publication", 5),
    (
        "corporate_actions",
        "trg_corporate_actions_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "corporate_actions",
        "trg_corporate_actions_immutable",
    ): ("nexus_prevent_research_change", 27),
    (
        "data_artifacts",
        "trg_00_data_artifacts_fosun_authority_lock",
    ): ("nexus_lock_fosun_market_input_change", 27),
    (
        "data_artifacts",
        "trg_data_artifacts_backtest_frozen",
    ): ("nexus_prevent_frozen_backtest_authority_change", 27),
    (
        "data_artifacts",
        "trg_data_artifacts_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "data_artifacts",
        "trg_data_artifacts_fosun_binding",
    ): ("nexus_prevent_fosun_market_binding_change", 27),
    (
        "data_artifacts",
        "trg_data_artifacts_fosun_transport_immutable",
    ): ("nexus_prevent_fosun_lineage_change", 27),
    (
        "data_artifacts",
        "trg_data_artifacts_frozen_authority",
    ): ("nexus_prevent_frozen_price_authority_change", 27),
    (
        "data_artifacts",
        "trg_data_artifacts_z_diagnostic_authority_immutable",
    ): ("nexus_prevent_diagnostic_authority_change", 27),
    (
        "filings",
        "trg_filings_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    ("filings", "trg_filings_immutable"): ("nexus_prevent_research_change", 27),
    (
        "financial_facts",
        "trg_financial_facts_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "financial_facts",
        "trg_financial_facts_immutable",
    ): ("nexus_prevent_research_change", 27),
    (
        "fosun_daily_bar_staging",
        "trg_fosun_daily_bar_staging_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "fosun_daily_bar_staging",
        "trg_fosun_daily_bar_staging_immutable",
    ): ("nexus_prevent_fosun_daily_staging_change", 27),
    (
        "intraday_bar_set_authorities",
        "trg_intraday_authority_fosun_transport",
    ): ("nexus_validate_fosun_authority_transport", 7),
    (
        "intraday_bar_set_authorities",
        "trg_intraday_bar_set_authorities_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "intraday_bar_set_authorities",
        "trg_intraday_bar_set_authorities_immutable",
    ): ("nexus_prevent_intraday_quote_change", 27),
    (
        "intraday_bar_set_authorities",
        "trg_intraday_bar_set_authorities_review",
    ): ("nexus_validate_intraday_review_authority", 7),
    (
        "intraday_bar_set_authorities",
        "trg_intraday_bar_set_authorities_validate",
    ): ("nexus_validate_intraday_bar_set_authority", 7),
    (
        "integration_status",
        "trg_00_integration_status_fosun_authority_lock",
    ): ("nexus_lock_fosun_market_input_change", 27),
    (
        "integration_status",
        "trg_integration_status_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "job_attempts",
        "trg_job_attempts_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    ("job_attempts", "trg_job_attempts_state"): ("nexus_guard_job_attempt", 27),
    (
        "job_leases",
        "trg_job_leases_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    ("job_leases", "trg_job_leases_state"): ("nexus_guard_job_lease", 27),
    (
        "jobs",
        "trg_jobs_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "jobs",
        "trg_jobs_intraday_schema_admission",
    ): ("nexus_guard_intraday_job_schema_admission", 23),
    ("jobs", "trg_jobs_state"): ("nexus_guard_job_state", 27),
    (
        "latest_quotes",
        "trg_latest_quotes_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "latest_quotes",
        "trg_latest_quotes_fosun_market_binding",
    ): ("nexus_validate_fosun_market_binding", 7),
    (
        "latest_quotes",
        "trg_latest_quotes_observation_immutable",
    ): ("nexus_prevent_intraday_quote_change", 27),
    (
        "latest_quotes",
        "trg_latest_quotes_provider_binding_immutable",
    ): ("nexus_prevent_fosun_market_binding_change", 27),
    (
        "latest_quotes",
        "trg_latest_quotes_raw_artifact",
    ): ("nexus_validate_latest_quote_raw_artifact", 7),
    (
        "macro_observations",
        "trg_macro_observations_authority_insert",
    ): ("nexus_prevent_legacy_macro_insert", 7),
    (
        "macro_observations",
        "trg_macro_observations_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "macro_observations",
        "trg_macro_observations_immutable",
    ): ("nexus_prevent_research_change", 27),
    (
        "price_bar_records",
        "trg_00_backtest_price_bar_frozen_insert",
    ): ("nexus_guard_provider_price_bar_frozen_insert", 7),
    (
        "price_bar_records",
        "trg_00_price_bar_records_authority_guard",
    ): ("nexus_guard_price_bar_set_mutation", 31),
    (
        "price_bar_records",
        "trg_price_bar_records_backtest_frozen",
    ): ("nexus_prevent_frozen_backtest_authority_change", 27),
    (
        "price_bar_records",
        "trg_price_bar_records_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "price_bar_records",
        "trg_price_bar_records_fosun_transport",
    ): ("nexus_validate_fosun_bar_transport", 7),
    (
        "price_bar_records",
        "trg_price_bar_records_immutable",
    ): ("nexus_prevent_agent_immutable_change", 27),
    (
        "price_bar_set_authorities",
        "trg_price_authority_fosun_transport",
    ): ("nexus_validate_fosun_authority_transport", 7),
    (
        "price_bar_set_authorities",
        "trg_price_bar_set_authorities_backtest_frozen",
    ): ("nexus_prevent_frozen_backtest_authority_change", 27),
    (
        "price_bar_set_authorities",
        "trg_price_bar_set_authorities_backtest_publication",
    ): ("nexus_mark_provider_backtest_authority_publication", 5),
    (
        "price_bar_set_authorities",
        "trg_price_bar_set_authorities_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "price_bar_set_authorities",
        "trg_price_bar_set_authorities_immutable",
    ): ("nexus_prevent_agent_immutable_change", 27),
    (
        "price_bar_set_authorities",
        "trg_price_bar_set_authorities_validate",
    ): ("nexus_validate_price_bar_set_authority", 7),
    (
        "provider_capabilities",
        "trg_provider_capabilities_append_only",
    ): ("nexus_prevent_provider_capability_change", 27),
    (
        "provider_capabilities",
        "trg_provider_capabilities_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "provider_capabilities",
        "trg_provider_capabilities_scope_lock",
    ): ("nexus_lock_provider_capability_scope", 7),
    (
        "price_series_manifests",
        "trg_00_price_series_manifests_fosun_authority_lock",
    ): ("nexus_lock_fosun_market_input_change", 27),
    (
        "price_series_manifests",
        "trg_price_series_fosun_lineage_immutable",
    ): ("nexus_prevent_fosun_lineage_change", 27),
    (
        "price_series_manifests",
        "trg_price_series_manifests_z_fosun_transport",
    ): ("nexus_validate_fosun_manifest_transport", 7),
    (
        "price_series_manifests",
        "trg_price_series_manifests_backtest_frozen",
    ): ("nexus_prevent_frozen_backtest_authority_change", 27),
    (
        "price_series_manifests",
        "trg_price_series_manifests_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "price_series_manifests",
        "trg_price_series_manifests_fosun_market_binding",
    ): ("nexus_validate_fosun_market_binding", 7),
    (
        "price_series_manifests",
        "trg_price_series_manifests_frozen_authority",
    ): ("nexus_prevent_frozen_price_authority_change", 27),
    (
        "price_series_manifests",
        "trg_price_series_manifests_provider_binding_immutable",
    ): ("nexus_prevent_fosun_market_binding_change", 27),
    (
        "price_series_manifests",
        "trg_price_series_manifests_z_diagnostic_authority_immutable",
    ): ("nexus_prevent_diagnostic_authority_change", 27),
    (
        "research_evidence_refs",
        "trg_research_evidence_refs_backtest_frozen",
    ): ("nexus_prevent_frozen_backtest_authority_change", 27),
    (
        "research_evidence_refs",
        "trg_research_evidence_refs_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "research_evidence_refs",
        "trg_research_evidence_refs_immutable",
    ): ("nexus_prevent_research_change", 27),
    (
        "worker_heartbeats",
        "trg_worker_heartbeats_created_at_immutable",
    ): ("nexus_prevent_created_at_change", 19),
    (
        "worker_heartbeats",
        "trg_worker_heartbeats_state",
    ): ("nexus_guard_worker_heartbeat", 27),
}
_PROVIDER_POLICY_EXPRESSIONS = {
    "jobs": (
        "nexus_provider_job_visible(queue, job_type)",
        "nexus_provider_job_writable(queue, job_type)",
    ),
    "job_leases": (
        "nexus_provider_job_owned(job_id)",
        "nexus_provider_job_owned(job_id)",
    ),
    "job_attempts": (
        "nexus_provider_job_owned(job_id)",
        "nexus_provider_job_owned(job_id)",
    ),
    "job_dependencies": (
        "nexus_provider_job_owned(job_id)",
        "nexus_provider_job_owned(job_id)",
    ),
    "worker_heartbeats": (
        "nexus_provider_queue(queue)",
        "nexus_provider_queue(queue)",
    ),
    "fosun_daily_bar_staging": (
        "nexus_provider_job_owned(fetch_job_id)",
        "nexus_provider_job_owned(fetch_job_id)",
    ),
}


class ProviderStore(MarketObjectStore, RawEvidenceStore, Protocol):
    """Combined create-only provider evidence boundary."""


async def _assert_intraday_job_queue_drained(session: AsyncSession) -> None:
    """Block v2 activation while any retryable incompatible intraday Job remains."""
    payload_source = Job.payload["source"].as_string()
    payload_feed = Job.payload["feed"].as_string()
    admitted_source_feed = or_(
        and_(payload_source == "fosun", payload_feed == "l1"),
        and_(
            payload_source == "alpaca",
            payload_feed.in_(("iex", "sip", "delayed_sip")),
        ),
    )
    incompatible = await session.scalar(
        select(func.count())
        .select_from(Job)
        .where(
            Job.job_type == JobType.COLLECT_INTRADAY_SNAPSHOT.value,
            or_(
                Job.schema_version != _CURRENT_INTRADAY_JOB_SCHEMA,
                Job.payload["schema"].as_string().is_distinct_from(_CURRENT_INTRADAY_JOB_SCHEMA),
                admitted_source_feed.is_not(True),
                Job.retry_policy.is_distinct_from(RetryPolicy.NEVER.value),
                Job.max_attempts.is_distinct_from(1),
                text(
                    "CASE WHEN pg_catalog.pg_input_is_valid("
                    "COALESCE(jobs.payload ->> 'bucket_start', ''), "
                    "'timestamp with time zone') "
                    "AND pg_catalog.pg_input_is_valid("
                    "COALESCE(jobs.payload ->> 'bucket_end', ''), "
                    "'timestamp with time zone') THEN ("
                    "jobs.effective_at IS DISTINCT FROM "
                    "(jobs.payload ->> 'bucket_start')::pg_catalog.timestamptz "
                    "OR (jobs.payload ->> 'bucket_end')::pg_catalog.timestamptz "
                    "IS DISTINCT FROM jobs.effective_at + INTERVAL '15 seconds' "
                    "OR pg_catalog.mod(EXTRACT(EPOCH FROM jobs.effective_at), 15) <> 0"
                    ") ELSE TRUE END"
                ),
            ),
            Job.status.in_(_ACTIONABLE_INTRADAY_JOB_STATUSES),
        )
    )
    if incompatible:
        raise RuntimeError("INTRADAY_V1_QUEUE_DRAIN_REQUIRED")


@asynccontextmanager
async def _provider_registry(
    *,
    provider_settings: ProviderSettings,
    provider_http_client: httpx.AsyncClient,
    store: ProviderStore,
    raw_archive_session_factory: async_sessionmaker[AsyncSession] | None = None,
    fosun_gateway_factory: FosunGatewayFactory = FosunMarketGatewayClient,
) -> AsyncIterator[JobRegistry]:
    """Compose only provider jobs; construction performs no external request."""

    def http_factory(
        *,
        provider_name: str,
        origin: AllowedOrigin,
        policy: HttpRequestPolicy,
    ) -> FixedOriginHttpClient:
        return FixedOriginHttpClient(
            provider_name=provider_name,
            origin=origin,
            policy=policy,
            client=provider_http_client,
        )

    bundle = build_provider_bundle(
        provider_settings,
        http_client_factory=http_factory,
        fosun_gateway_factory=fosun_gateway_factory,
    )
    try:
        provider = build_provider_collection_registry(
            bundle,
            store,
            store,
            lambda: datetime.now(UTC),
            expected_profile_id=PRIMARY_DAILY_PROFILE,
            raw_archive_session_factory=raw_archive_session_factory,
        )
        intraday_specs: tuple[JobHandlerSpec, ...] = (_disabled_intraday_spec(),)
        if provider_settings.intraday_monitor_mode is IntradayMonitorMode.PRODUCTION:

            def snapshot_provider(session: object) -> IntradaySnapshotProvider:
                return cast(
                    IntradaySnapshotProvider,
                    bundle.snapshot_provider(session),
                )

            intraday_specs = build_intraday_collection_registry(
                snapshot_provider,
                store,
                lambda: datetime.now(UTC),
                settings=provider_settings,
                raw_archive_session_factory=raw_archive_session_factory,
            ).specs
        yield JobRegistry((*provider.specs, *intraday_specs))
    finally:
        await bundle.aclose()


def _disabled_intraday_spec() -> JobHandlerSpec:
    """Drain already-approved v2 work without any network when monitoring is off."""

    async def skip(_context: JobHandlerContext) -> JobHandlerResult:
        return JobHandlerResult(
            outcome="skipped",
            result_code="INTRADAY_MONITOR_DISABLED",
        )

    return JobHandlerSpec(
        job_type=JobType.COLLECT_INTRADAY_SNAPSHOT,
        schema_version=_CURRENT_INTRADAY_JOB_SCHEMA,
        payload_model=IntradaySnapshotJobPayloadV2,
        queue=QueueName.PROVIDER,
        timeout_seconds=1,
        retry_policy=RetryPolicy.NEVER,
        max_attempts=1,
        idempotency_namespace="intraday.snapshot",
        handler=skip,
    )


async def run_provider_worker(
    runtime_settings: ProviderWorkerSettings | None = None,
    provider_settings: ProviderSettings | None = None,
    *,
    run_forever: bool = False,
) -> None:
    """Poll only the provider queue and schedule provider-owned data jobs."""
    runtime = runtime_settings or ProviderWorkerSettings.model_validate({})
    configured = provider_settings or ProviderSettings.model_validate({})
    if configured.runtime_mode is not ProviderRuntimeMode.PRODUCTION:
        raise RuntimeError("PROVIDER_RUNTIME_PRODUCTION_REQUIRED")
    engine = create_async_engine(
        runtime.database_url,
        pool_pre_ping=True,
        isolation_level="READ COMMITTED",
        connect_args={"server_settings": {"search_path": "pg_catalog,public,pg_temp"}},
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await _assert_provider_migration_head(session)
            await _assert_intraday_job_queue_drained(session)
        if not run_forever:
            await asyncio.sleep(0)
            return
        store = S3ProviderEvidenceStore(runtime)
        async with _provider_http_client() as client:
            async with _provider_registry(
                provider_settings=configured,
                provider_http_client=client,
                store=store,
                raw_archive_session_factory=factory,
            ) as registry:
                worker_id = f"provider-{new_uuid7()}"
                started_at = datetime.now(UTC)
                while True:
                    now = datetime.now(UTC)
                    async with factory() as session, session.begin():
                        await _assert_provider_preclaim_ready(session)
                        if configured.intraday_monitor_mode is IntradayMonitorMode.PRODUCTION:
                            await schedule_due_intraday_snapshot(
                                session,
                                now=now,
                                settings=configured,
                            )
                    worked = await run_job_worker_once(
                        factory,
                        registry=registry,
                        queue=QueueName.PROVIDER,
                        worker_id=worker_id,
                        worker_started_at=started_at,
                        process_version=_PROCESS_VERSION,
                        migration_head=CURRENT_MIGRATION_HEAD,
                        clock=lambda: datetime.now(UTC),
                        claim_registered_only=True,
                        preclaim_guard=_assert_provider_preclaim_ready,
                    )
                    if not worked:
                        await asyncio.sleep(0.5)
    finally:
        await engine.dispose()


def _provider_http_client() -> httpx.AsyncClient:
    """Create the provider egress client without proxy or netrc inheritance."""
    return httpx.AsyncClient(
        follow_redirects=False,
        timeout=None,
        trust_env=False,
    )


def main() -> None:
    asyncio.run(run_provider_worker(run_forever=True))


async def _assert_provider_preclaim_ready(session: AsyncSession) -> None:
    """Serialize schema rollback through the exact head check and claim transaction."""
    await _assert_provider_transaction_isolation(session)
    await session.execute(
        text(
            "SELECT pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtextextended(:key, 0))"
        ),
        {"key": _INTRADAY_SCHEMA_ADMISSION_LOCK},
    )
    await _assert_provider_migration_head(session)
    await _assert_intraday_job_queue_drained(session)


async def _assert_provider_transaction_isolation(session: AsyncSession) -> None:
    """Keep lock-then-refresh checks on a fresh READ COMMITTED statement snapshot."""
    isolation = await session.scalar(text("SELECT current_setting('transaction_isolation')"))
    if isolation != "read committed":
        raise RuntimeError("PROVIDER_DATABASE_TRANSACTION_ISOLATION_INVALID")


async def _assert_provider_migration_head(session: AsyncSession) -> None:
    """Require one exact Alembic head before scheduling or claiming Provider work."""
    head = await session.scalar(
        text("SELECT CASE WHEN COUNT(*) = 1 THEN MIN(version_num) END FROM public.alembic_version")
    )
    if head != CURRENT_MIGRATION_HEAD:
        raise RuntimeError("PROVIDER_DATABASE_MIGRATION_HEAD_MISMATCH")


async def _assert_provider_database_role(session: AsyncSession) -> None:
    role = (
        (
            await session.execute(
                text(
                    "SELECT current_user AS role_name, "
                    "session_user = current_user AS direct_login, "
                    "role.rolcanlogin AS role_can_login, role.rolsuper AS superuser, "
                    "role.rolbypassrls AS bypass_rls, role.rolcreatedb AS create_db, "
                    "role.rolcreaterole AS create_role, role.rolreplication AS replication, "
                    "has_database_privilege(current_user, current_database(), 'CREATE') "
                    "AS database_create, "
                    "has_parameter_privilege(current_user, 'session_replication_role', "
                    "'SET') AS can_set_session_replication_role, "
                    "has_parameter_privilege(current_user, 'session_replication_role', "
                    "'ALTER SYSTEM') AS can_alter_system_session_replication_role, "
                    "current_setting('session_replication_role') "
                    "AS session_replication_role, "
                    "current_setting('transaction_isolation') AS transaction_isolation, "
                    "has_schema_privilege(current_user, 'public', 'USAGE') AS schema_usage, "
                    "has_schema_privilege(current_user, 'public', 'CREATE') AS schema_create, "
                    "EXISTS (SELECT 1 FROM pg_namespace AS namespace "
                    "WHERE namespace.nspname <> 'public' "
                    "AND namespace.nspname <> 'information_schema' "
                    "AND left(namespace.nspname, 3) <> 'pg_' "
                    "AND (has_schema_privilege(current_user, namespace.oid, 'USAGE') "
                    "OR has_schema_privilege(current_user, namespace.oid, 'CREATE'))) "
                    "AS has_untrusted_schema_privileges, "
                    "EXISTS (SELECT 1 FROM pg_roles AS granted_role "
                    "WHERE granted_role.oid <> current_user::regrole "
                    "AND pg_has_role(current_user::regrole, granted_role.oid, 'MEMBER')) "
                    "OR EXISTS (SELECT 1 FROM pg_auth_members AS membership "
                    "WHERE membership.member = current_user::regrole "
                    "OR membership.roleid = current_user::regrole) "
                    "AS has_role_memberships "
                    "FROM pg_roles AS role WHERE role.rolname = current_user"
                )
            )
        )
        .mappings()
        .one()
    )
    rows = (
        (
            await session.execute(
                text(
                    "SELECT namespace.nspname AS schema_name, "
                    "relation.relname AS table_name, relation.relkind::text AS relation_kind, "
                    "relation.relrowsecurity AS row_security, "
                    "relation.relforcerowsecurity AS force_row_security, "
                    "has_table_privilege(current_user, relation.oid, 'SELECT') AS can_select, "
                    "has_table_privilege(current_user, relation.oid, 'INSERT') AS can_insert, "
                    "has_table_privilege(current_user, relation.oid, 'UPDATE') AS can_update, "
                    "has_table_privilege(current_user, relation.oid, 'DELETE') AS can_delete, "
                    "has_table_privilege(current_user, relation.oid, 'TRUNCATE') AS can_truncate, "
                    "has_table_privilege(current_user, relation.oid, 'REFERENCES') "
                    "AS can_references, "
                    "has_table_privilege(current_user, relation.oid, 'TRIGGER') AS can_trigger, "
                    "has_table_privilege(current_user, relation.oid, 'MAINTAIN') AS can_maintain, "
                    "ARRAY(SELECT acl.privilege_type::text "
                    "FROM aclexplode(COALESCE(relation.relacl, "
                    "acldefault('r', relation.relowner))) AS acl "
                    "WHERE acl.grantee = current_user::regrole "
                    "ORDER BY acl.privilege_type::text) AS direct_table_privileges, "
                    "NOT EXISTS (SELECT 1 FROM aclexplode(COALESCE(relation.relacl, "
                    "acldefault('r', relation.relowner))) AS acl "
                    "WHERE acl.grantee = current_user::regrole "
                    "AND acl.grantor <> relation.relowner) AS direct_table_grantors_trusted, "
                    "EXISTS (SELECT 1 FROM aclexplode(COALESCE(relation.relacl, "
                    "acldefault('r', relation.relowner))) AS acl WHERE acl.grantee = 0) "
                    "AS has_public_table_privilege, "
                    "has_any_column_privilege(current_user, relation.oid, 'SELECT') "
                    "AS can_column_select, "
                    "has_any_column_privilege(current_user, relation.oid, 'INSERT') "
                    "AS can_column_insert, "
                    "has_any_column_privilege(current_user, relation.oid, 'UPDATE') "
                    "AS can_column_update, "
                    "has_any_column_privilege(current_user, relation.oid, 'REFERENCES') "
                    "AS can_column_references, "
                    "EXISTS (SELECT 1 FROM aclexplode(COALESCE(relation.relacl, "
                    "acldefault('r', relation.relowner))) AS acl "
                    "WHERE acl.grantee IN (0, current_user::regrole) "
                    "AND acl.is_grantable) AS has_table_grant_option, "
                    "EXISTS (SELECT 1 FROM pg_attribute AS attribute "
                    "CROSS JOIN LATERAL aclexplode(attribute.attacl) AS acl "
                    "WHERE attribute.attrelid = relation.oid "
                    "AND attribute.attnum > 0 AND NOT attribute.attisdropped "
                    "AND acl.grantee IN (0, current_user::regrole) "
                    "AND acl.is_grantable) AS has_column_grant_option "
                    ", EXISTS (SELECT 1 FROM pg_attribute AS attribute "
                    "CROSS JOIN LATERAL aclexplode(attribute.attacl) AS acl "
                    "WHERE attribute.attrelid = relation.oid "
                    "AND attribute.attnum > 0 AND NOT attribute.attisdropped "
                    "AND acl.grantee = current_user::regrole) AS has_provider_column_privilege "
                    ", EXISTS (SELECT 1 FROM pg_attribute AS attribute "
                    "CROSS JOIN LATERAL aclexplode(attribute.attacl) AS acl "
                    "WHERE attribute.attrelid = relation.oid "
                    "AND attribute.attnum > 0 AND NOT attribute.attisdropped "
                    "AND acl.grantee = 0) AS has_public_column_privilege "
                    "FROM pg_class AS relation "
                    "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
                    "WHERE namespace.nspname <> 'information_schema' "
                    "AND left(namespace.nspname, 3) <> 'pg_' "
                    "AND relation.relkind IN ('r','p','v','m','f')"
                )
            )
        )
        .mappings()
        .all()
    )
    sequence_rows = (
        (
            await session.execute(
                text(
                    "SELECT namespace.nspname AS schema_name, "
                    "relation.relname AS sequence_name, "
                    "has_sequence_privilege(current_user, relation.oid, 'USAGE') AS can_usage, "
                    "has_sequence_privilege(current_user, relation.oid, 'SELECT') AS can_select, "
                    "has_sequence_privilege(current_user, relation.oid, 'UPDATE') AS can_update, "
                    "EXISTS (SELECT 1 FROM aclexplode(COALESCE(relation.relacl, "
                    "acldefault('s', relation.relowner))) AS acl WHERE acl.grantee = 0) "
                    "AS has_public_privilege, "
                    "EXISTS (SELECT 1 FROM aclexplode(COALESCE(relation.relacl, "
                    "acldefault('s', relation.relowner))) AS acl "
                    "WHERE acl.grantee IN (0, current_user::regrole) "
                    "AND acl.is_grantable) AS has_grant_option "
                    "FROM pg_sequence AS sequence "
                    "JOIN pg_class AS relation ON relation.oid = sequence.seqrelid "
                    "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
                    "WHERE namespace.nspname <> 'information_schema' "
                    "AND left(namespace.nspname, 3) <> 'pg_'"
                )
            )
        )
        .mappings()
        .all()
    )
    policy_rows = (
        (
            await session.execute(
                text(
                    "SELECT policy.tablename AS table_name, policy.policyname AS policy_name, "
                    "policy.permissive, policy.roles, policy.cmd AS command, "
                    "policy.qual, policy.with_check, "
                    "CASE WHEN policy.policyname LIKE 'nexus_non_provider_%_bypass' THEN "
                    "strpos(policy_record.polqual::text, chr(58) || 'funcid ' || "
                    "'pg_catalog.pg_has_role(name,name,text)'::regprocedure::oid::text || ' ') > 0 "
                    "AND strpos(policy_record.polwithcheck::text, chr(58) || 'funcid ' || "
                    "'pg_catalog.pg_has_role(name,name,text)'::regprocedure::oid::text || ' ') > 0 "
                    "ELSE strpos(policy_record.polqual::text, chr(58) || 'funcid ' || "
                    "'pg_catalog.pg_has_role(name,name,text)'::regprocedure::oid::text || ' ') = 0 "
                    "AND strpos(policy_record.polwithcheck::text, chr(58) || 'funcid ' || "
                    "'pg_catalog.pg_has_role(name,name,text)'::regprocedure::oid::text || ' ') = 0 "
                    "END AS builtin_binding_valid, "
                    "ARRAY(SELECT DISTINCT procedure_namespace.nspname || '.' || procedure.proname "
                    "FROM pg_depend AS dependency "
                    "JOIN pg_proc AS procedure ON procedure.oid = dependency.refobjid "
                    "JOIN pg_namespace AS procedure_namespace "
                    "ON procedure_namespace.oid = procedure.pronamespace "
                    "WHERE dependency.classid = 'pg_policy'::regclass "
                    "AND dependency.objid = policy_record.oid "
                    "AND dependency.refclassid = 'pg_proc'::regclass "
                    "ORDER BY procedure_namespace.nspname || '.' || procedure.proname) "
                    "AS function_dependencies "
                    "FROM pg_policies AS policy "
                    "JOIN pg_namespace AS relation_namespace "
                    "ON relation_namespace.nspname = policy.schemaname "
                    "JOIN pg_class AS relation ON relation.relnamespace = relation_namespace.oid "
                    "AND relation.relname = policy.tablename "
                    "JOIN pg_policy AS policy_record ON policy_record.polrelid = relation.oid "
                    "AND policy_record.polname = policy.policyname "
                    "WHERE policy.schemaname = 'public' "
                    "AND policy.tablename IN ('jobs','job_leases','job_attempts',"
                    "'job_dependencies','worker_heartbeats','fosun_daily_bar_staging')"
                )
            )
        )
        .mappings()
        .all()
    )
    function_rows = (
        (
            await session.execute(
                text(
                    "SELECT procedure.proname AS function_name, "
                    "pg_get_function_identity_arguments(procedure.oid) AS identity_arguments, "
                    "procedure.prosrc AS source, procedure.provolatile::text AS volatility, "
                    "procedure.proparallel::text AS parallel_safety, "
                    "procedure.prosecdef AS security_definer, "
                    "procedure.proconfig = ARRAY["
                    "'search_path=pg_catalog, public, pg_temp']::text[] "
                    "AS search_path_fixed, language.lanname AS language, "
                    "procedure.prorettype = 'boolean'::regtype AS returns_boolean, "
                    "procedure.proowner = jobs_table.relowner AS trusted_owner, "
                    "pg_has_role(current_user::regrole, procedure.proowner, 'MEMBER') "
                    "AS provider_is_owner_member, "
                    "has_function_privilege(current_user, procedure.oid, 'EXECUTE') "
                    "AS provider_execute, "
                    "EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                    "acldefault('f', procedure.proowner))) AS acl "
                    "WHERE acl.privilege_type = 'EXECUTE' AND acl.grantee = 0) "
                    "AS public_execute, "
                    "EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                    "acldefault('f', procedure.proowner))) AS acl "
                    "WHERE acl.privilege_type = 'EXECUTE' "
                    "AND acl.grantee = current_user::regrole AND acl.is_grantable) "
                    "AS provider_execute_grantable, "
                    "NOT EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                    "acldefault('f', procedure.proowner))) AS acl "
                    "WHERE acl.privilege_type = 'EXECUTE' AND acl.grantee NOT IN "
                    "(procedure.proowner, current_user::regrole)) "
                    "AS acl_only_owner_and_provider, "
                    "NOT EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                    "acldefault('f', procedure.proowner))) AS acl "
                    "WHERE acl.privilege_type = 'EXECUTE' "
                    "AND acl.grantor <> procedure.proowner) AS acl_grantor_only_owner "
                    "FROM pg_proc AS procedure "
                    "JOIN pg_language AS language ON language.oid = procedure.prolang "
                    "JOIN pg_class AS jobs_table ON jobs_table.oid = 'public.jobs'::regclass "
                    "WHERE procedure.pronamespace = 'public'::regnamespace "
                    "AND procedure.proname IN ("
                    "'nexus_provider_job_visible','nexus_provider_job_writable',"
                    "'nexus_provider_job_owned','nexus_provider_queue')"
                )
            )
        )
        .mappings()
        .all()
    )
    publication_function_rows = (
        (
            await session.execute(
                text(
                    "SELECT procedure_namespace.nspname AS schema_name, "
                    "procedure.proname AS function_name, "
                    "pg_get_function_identity_arguments(procedure.oid) AS identity_arguments, "
                    "procedure.prosrc AS source, procedure.provolatile::text AS volatility, "
                    "procedure.proparallel::text AS parallel_safety, "
                    "procedure.prosecdef AS security_definer, "
                    "procedure.proleakproof AS leakproof, "
                    "procedure.proconfig = ARRAY["
                    "'search_path=pg_catalog, public, pg_temp']::text[] "
                    "AS search_path_exact, language.lanname AS language, "
                    "procedure.prorettype = 'trigger'::regtype AS returns_trigger, "
                    "procedure.proowner = marks.relowner "
                    "AND marks.relowner = actions.relowner "
                    "AND marks.relowner = authorities.relowner "
                    "AND marks.relowner = bars.relowner "
                    "AND marks.relowner = members.relowner AS trusted_owner, "
                    "pg_has_role(current_user::regrole, procedure.proowner, 'MEMBER') "
                    "AS provider_is_owner_member, "
                    "has_function_privilege(current_user, procedure.oid, 'EXECUTE') "
                    "AS provider_execute, "
                    "EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                    "acldefault('f', procedure.proowner))) AS acl "
                    "WHERE acl.privilege_type = 'EXECUTE' AND acl.grantee = 0) "
                    "AS public_execute, "
                    "NOT EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                    "acldefault('f', procedure.proowner))) AS acl "
                    "WHERE acl.privilege_type = 'EXECUTE' "
                    "AND acl.grantee <> procedure.proowner) AS acl_only_owner, "
                    "NOT EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                    "acldefault('f', procedure.proowner))) AS acl "
                    "WHERE acl.privilege_type = 'EXECUTE' "
                    "AND acl.grantor <> procedure.proowner) AS acl_grantor_only_owner "
                    "FROM pg_proc AS procedure "
                    "JOIN pg_namespace AS procedure_namespace "
                    "ON procedure_namespace.oid = procedure.pronamespace "
                    "JOIN pg_language AS language ON language.oid = procedure.prolang "
                    "JOIN pg_class AS marks ON marks.oid = "
                    "'public.backtest_authority_publication_marks'::regclass "
                    "JOIN pg_class AS actions ON actions.oid = "
                    "'public.corporate_actions'::regclass "
                    "JOIN pg_class AS authorities ON authorities.oid = "
                    "'public.price_bar_set_authorities'::regclass "
                    "JOIN pg_class AS bars ON bars.oid = "
                    "'public.price_bar_records'::regclass "
                    "JOIN pg_class AS members ON members.oid = "
                    "'public.backtest_price_authority_members'::regclass "
                    "WHERE procedure.prosecdef "
                    "AND procedure_namespace.nspname <> 'information_schema' "
                    "AND left(procedure_namespace.nspname, 3) <> 'pg_'"
                )
            )
        )
        .mappings()
        .all()
    )
    publication_trigger_rows = (
        (
            await session.execute(
                text(
                    "SELECT trigger.tgname AS trigger_name, "
                    "relation_namespace.nspname AS schema_name, "
                    "relation.relname AS table_name, procedure.proname AS function_name, "
                    "pg_get_function_identity_arguments(procedure.oid) AS identity_arguments, "
                    "trigger.tgtype AS trigger_type, "
                    "trigger.tgenabled::text AS enabled, "
                    "trigger.tgisinternal AS internal, "
                    "trigger.tgqual IS NULL AS unconditional, "
                    "trigger.tgconstraint = 0 AS non_constraint, "
                    "NOT trigger.tgdeferrable AS non_deferrable, "
                    "NOT trigger.tginitdeferred AS initially_immediate, "
                    "trigger.tgnargs = 0 AS no_arguments, "
                    "trigger.tgparentid = 0 AS not_partition_clone, "
                    "trigger.tgattr::text = '' AS no_update_columns, "
                    "relation.relkind::text AS relation_kind "
                    "FROM pg_trigger AS trigger "
                    "JOIN pg_proc AS procedure ON procedure.oid = trigger.tgfoid "
                    "JOIN pg_class AS relation ON relation.oid = trigger.tgrelid "
                    "JOIN pg_namespace AS relation_namespace "
                    "ON relation_namespace.oid = relation.relnamespace "
                    "WHERE procedure.pronamespace = 'public'::regnamespace "
                    "AND procedure.proname IN ("
                    "'nexus_mark_provider_backtest_authority_publication',"
                    "'nexus_guard_provider_price_bar_frozen_insert') "
                    "ORDER BY relation.relname, trigger.tgname"
                )
            )
        )
        .mappings()
        .all()
    )
    provider_trigger_rows = await _load_provider_trigger_rows(session)
    violation = _provider_privilege_violation(
        direct_login=bool(role["direct_login"]),
        role_can_login=bool(role["role_can_login"]),
        superuser=bool(role["superuser"]),
        bypass_rls=bool(role["bypass_rls"]),
        create_db=bool(role["create_db"]),
        create_role=bool(role["create_role"]),
        replication=bool(role["replication"]),
        database_create=bool(role["database_create"]),
        can_set_session_replication_role=bool(role["can_set_session_replication_role"]),
        can_alter_system_session_replication_role=bool(
            role["can_alter_system_session_replication_role"]
        ),
        session_replication_role=str(role["session_replication_role"]),
        transaction_isolation=str(role["transaction_isolation"]),
        role_memberships=bool(role["has_role_memberships"]),
        schema_usage=bool(role["schema_usage"]),
        schema_create=bool(role["schema_create"]),
        untrusted_schema_privileges=bool(role["has_untrusted_schema_privileges"]),
        rows=[dict(row) for row in rows],
        sequence_rows=[dict(row) for row in sequence_rows],
    )
    if violation is not None:
        raise RuntimeError(violation)
    policy_violation = _provider_policy_violation(
        [dict(row) for row in policy_rows],
        provider_role=str(role["role_name"]),
    )
    if policy_violation is not None:
        raise RuntimeError(policy_violation)
    function_violation = _provider_policy_function_violation([dict(row) for row in function_rows])
    if function_violation is not None:
        raise RuntimeError(function_violation)
    publication_violation = _provider_publication_function_violation(
        [dict(row) for row in publication_function_rows],
        [dict(row) for row in publication_trigger_rows],
    )
    if publication_violation is not None:
        raise RuntimeError(publication_violation)
    trigger_violation = _provider_trigger_topology_violation(
        [dict(row) for row in provider_trigger_rows]
    )
    if trigger_violation is not None:
        raise RuntimeError(trigger_violation)
    await _assert_provider_ticker_alias_constraints(session)
    await _assert_provider_intraday_bucket_index(session)


async def _assert_provider_ticker_alias_constraints(session: AsyncSession) -> None:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT namespace.nspname AS schema_name, "
                    "relation.relname AS table_name, constraint_record.conname AS constraint_name, "
                    "constraint_record.contype::text AS constraint_type, "
                    "constraint_record.condeferrable AS deferrable, "
                    "constraint_record.condeferred AS initially_deferred, "
                    "constraint_record.convalidated AS validated, "
                    "index_record.indimmediate AS immediate, "
                    "index_record.indisvalid AS index_valid, "
                    "index_record.indisready AS index_ready, "
                    "pg_get_constraintdef(constraint_record.oid, true) AS definition "
                    "FROM pg_constraint AS constraint_record "
                    "JOIN pg_class AS relation ON relation.oid = constraint_record.conrelid "
                    "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
                    "JOIN pg_index AS index_record "
                    "ON index_record.indexrelid = constraint_record.conindid "
                    "WHERE namespace.nspname = 'public' "
                    "AND relation.relname = 'instrument_aliases' "
                    "AND constraint_record.contype = 'x' "
                    "ORDER BY constraint_record.conname"
                )
            )
        )
        .mappings()
        .all()
    )
    actual = {str(row["constraint_name"]): row for row in rows}
    if len(actual) != len(rows) or set(actual) != set(_PROVIDER_TICKER_ALIAS_CONSTRAINTS):
        raise RuntimeError("PROVIDER_DATABASE_TICKER_ALIAS_CONSTRAINT_INVALID")
    for name, definition in _PROVIDER_TICKER_ALIAS_CONSTRAINTS.items():
        row = actual[name]
        if (
            row["schema_name"] != "public"
            or row["table_name"] != "instrument_aliases"
            or row["constraint_type"] != "x"
            or bool(row["deferrable"])
            or bool(row["initially_deferred"])
            or not bool(row["validated"])
            or not bool(row["immediate"])
            or not bool(row["index_valid"])
            or not bool(row["index_ready"])
            or row["definition"] != definition
        ):
            raise RuntimeError("PROVIDER_DATABASE_TICKER_ALIAS_CONSTRAINT_INVALID")


async def _assert_provider_intraday_bucket_index(session: AsyncSession) -> None:
    row = (
        (
            await session.execute(
                text(
                    "SELECT table_namespace.nspname AS schema_name, "
                    "table_relation.relname AS table_name, "
                    "index_relation.relname AS index_name, "
                    "access_method.amname AS access_method, "
                    "index_relation.relkind::text AS relation_kind, "
                    "index_record.indisunique AS unique_index, "
                    "index_record.indisvalid AS index_valid, "
                    "index_record.indisready AS index_ready, "
                    "index_record.indislive AS index_live, "
                    "index_record.indimmediate AS immediate, "
                    "index_record.indnkeyatts = 1 AS one_key, "
                    "index_record.indnatts = 1 AS one_attribute, "
                    "index_relation.relowner = table_relation.relowner AS trusted_owner, "
                    "pg_get_indexdef(index_relation.oid) AS definition "
                    "FROM pg_catalog.pg_class AS index_relation "
                    "JOIN pg_catalog.pg_index AS index_record "
                    "ON index_record.indexrelid = index_relation.oid "
                    "JOIN pg_catalog.pg_class AS table_relation "
                    "ON table_relation.oid = index_record.indrelid "
                    "JOIN pg_catalog.pg_namespace AS table_namespace "
                    "ON table_namespace.oid = table_relation.relnamespace "
                    "JOIN pg_catalog.pg_am AS access_method "
                    "ON access_method.oid = index_relation.relam "
                    "WHERE table_namespace.nspname = 'public' "
                    "AND table_relation.relname = 'jobs' "
                    "AND index_relation.relname = 'uq_jobs_intraday_v2_bucket'"
                )
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None or any(
        (
            row["schema_name"] != "public",
            row["table_name"] != "jobs",
            row["index_name"] != "uq_jobs_intraday_v2_bucket",
            row["access_method"] != "btree",
            row["relation_kind"] != "i",
            not bool(row["unique_index"]),
            not bool(row["index_valid"]),
            not bool(row["index_ready"]),
            not bool(row["index_live"]),
            not bool(row["immediate"]),
            not bool(row["one_key"]),
            not bool(row["one_attribute"]),
            not bool(row["trusted_owner"]),
            row["definition"] != _PROVIDER_INTRADAY_BUCKET_INDEX_DEFINITION,
        )
    ):
        raise RuntimeError("PROVIDER_DATABASE_INTRADAY_BUCKET_INDEX_INVALID")


async def _load_provider_trigger_rows(
    session: AsyncSession,
) -> list[dict[str, object]]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT relation_namespace.nspname AS schema_name, "
                    "relation.relname AS table_name, trigger.tgname AS trigger_name, "
                    "procedure_namespace.nspname AS function_schema, "
                    "procedure.proname AS function_name, "
                    "pg_get_function_identity_arguments(procedure.oid) AS identity_arguments, "
                    "encode(public.digest(procedure.prosrc, 'sha256'), 'hex') "
                    "AS source_sha256, trigger.tgtype AS trigger_type, "
                    "trigger.tgenabled::text AS enabled, trigger.tgisinternal AS internal, "
                    "trigger.tgqual IS NULL AS unconditional, "
                    "trigger.tgconstraint = 0 AS non_constraint, "
                    "NOT trigger.tgdeferrable AS non_deferrable, "
                    "NOT trigger.tginitdeferred AS initially_immediate, "
                    "trigger.tgnargs = 0 AS no_arguments, "
                    "trigger.tgparentid = 0 AS not_partition_clone, "
                    "trigger.tgattr::text = '' AS no_update_columns, "
                    "relation.relkind::text AS relation_kind, "
                    "procedure.provolatile::text AS volatility, "
                    "procedure.proparallel::text AS parallel_safety, "
                    "procedure.proisstrict AS strict, "
                    "procedure.prosecdef AS security_definer, "
                    "procedure.proleakproof AS leakproof, "
                    "procedure.proconfig = ARRAY["
                    "'search_path=pg_catalog, public, pg_temp']::text[] "
                    "AS search_path_exact, procedure.proconfig IS NULL "
                    "AS no_function_config, language.lanname AS language, "
                    "procedure.prorettype = 'trigger'::regtype AS returns_trigger, "
                    "procedure.proowner = relation.relowner AS trusted_owner, "
                    "pg_has_role(current_user::regrole, procedure.proowner, 'MEMBER') "
                    "AS provider_is_owner_member "
                    "FROM pg_trigger AS trigger "
                    "JOIN pg_proc AS procedure ON procedure.oid = trigger.tgfoid "
                    "JOIN pg_namespace AS procedure_namespace "
                    "ON procedure_namespace.oid = procedure.pronamespace "
                    "JOIN pg_language AS language ON language.oid = procedure.prolang "
                    "JOIN pg_class AS relation ON relation.oid = trigger.tgrelid "
                    "JOIN pg_namespace AS relation_namespace "
                    "ON relation_namespace.oid = relation.relnamespace "
                    "WHERE relation_namespace.nspname = 'public' "
                    "AND NOT trigger.tgisinternal "
                    "AND relation.relname IN ("
                    "'jobs','job_leases','job_attempts','worker_heartbeats',"
                    "'data_artifacts','research_evidence_refs','price_series_manifests',"
                    "'price_bar_records','price_bar_set_authorities',"
                    "'intraday_bar_set_authorities','latest_quotes','filings',"
                    "'financial_facts','corporate_actions','macro_observations',"
                    "'fosun_daily_bar_staging','integration_status',"
                    "'provider_capabilities') "
                    "ORDER BY relation.relname, trigger.tgname"
                )
            )
        )
        .mappings()
        .all()
    )
    return [dict(row) for row in rows]


def _provider_privilege_violation(
    *,
    direct_login: bool = True,
    role_can_login: bool = True,
    superuser: bool,
    bypass_rls: bool,
    create_db: bool,
    create_role: bool,
    replication: bool,
    database_create: bool = False,
    can_set_session_replication_role: bool = False,
    can_alter_system_session_replication_role: bool = False,
    session_replication_role: str = "origin",
    transaction_isolation: str = "read committed",
    role_memberships: bool = False,
    schema_usage: bool,
    schema_create: bool,
    untrusted_schema_privileges: bool = False,
    rows: Sequence[Mapping[str, object]],
    sequence_rows: Sequence[Mapping[str, object]] = (),
) -> str | None:
    if not direct_login or not role_can_login:
        return "PROVIDER_DATABASE_SESSION_ROLE_INVALID"
    if superuser:
        return "PROVIDER_DATABASE_ROLE_SUPERUSER"
    if bypass_rls:
        return "PROVIDER_DATABASE_ROLE_BYPASSRLS"
    if create_db or create_role or replication:
        return "PROVIDER_DATABASE_ROLE_PRIVILEGED"
    if database_create:
        return "PROVIDER_DATABASE_CREATE_PRIVILEGE_INVALID"
    if (
        can_set_session_replication_role
        or can_alter_system_session_replication_role
        or session_replication_role != "origin"
    ):
        return "PROVIDER_DATABASE_PARAMETER_PRIVILEGES_INVALID"
    if transaction_isolation != "read committed":
        return "PROVIDER_DATABASE_TRANSACTION_ISOLATION_INVALID"
    if role_memberships:
        return "PROVIDER_DATABASE_ROLE_MEMBERSHIP"
    if not schema_usage or schema_create or untrusted_schema_privileges:
        return "PROVIDER_DATABASE_SCHEMA_PRIVILEGES_INVALID"
    if any(
        bool(row.get(field))
        for row in sequence_rows
        for field in (
            "can_usage",
            "can_select",
            "can_update",
            "has_public_privilege",
            "has_grant_option",
        )
    ):
        return "PROVIDER_DATABASE_SEQUENCE_PRIVILEGES_INVALID"
    relation_rows = {(str(row["schema_name"]), str(row["table_name"])): row for row in rows}
    if len(relation_rows) != len(rows):
        return "PROVIDER_DATABASE_SCHEMA_INCOMPLETE"
    if any(
        bool(row.get("has_public_table_privilege")) or bool(row.get("has_public_column_privilege"))
        for row in rows
    ):
        return "PROVIDER_DATABASE_PUBLIC_PRIVILEGES_INVALID"
    if any(bool(row.get("has_provider_column_privilege")) for row in rows):
        return "PROVIDER_DATABASE_COLUMN_PRIVILEGES_INVALID"
    if any(
        bool(row.get("has_table_grant_option")) or bool(row.get("has_column_grant_option"))
        for row in rows
    ):
        return "PROVIDER_DATABASE_GRANT_OPTIONS_INVALID"
    for table_name, required in _PROVIDER_PRIVILEGES.items():
        row = relation_rows.get(("public", table_name))
        if row is None:
            return "PROVIDER_DATABASE_SCHEMA_INCOMPLETE"
        relation_kind = str(row["relation_kind"])
        if (
            required
            & {
                "INSERT",
                "UPDATE",
                "DELETE",
                "TRUNCATE",
                "REFERENCES",
                "TRIGGER",
                "MAINTAIN",
            }
            and relation_kind != "r"
        ) or ("SELECT" in required and relation_kind not in {"r", "p"}):
            return "PROVIDER_DATABASE_RELATION_KIND_INVALID"
        actual = {
            privilege
            for privilege, column in (
                ("SELECT", "can_select"),
                ("INSERT", "can_insert"),
                ("UPDATE", "can_update"),
                ("DELETE", "can_delete"),
                ("TRUNCATE", "can_truncate"),
                ("REFERENCES", "can_references"),
                ("TRIGGER", "can_trigger"),
                ("MAINTAIN", "can_maintain"),
            )
            if bool(row[column])
        }
        if actual != required:
            return "PROVIDER_DATABASE_PRIVILEGES_INVALID"
        direct_privileges = row.get("direct_table_privileges")
        if (
            not isinstance(direct_privileges, Sequence)
            or isinstance(direct_privileges, str)
            or {str(item) for item in direct_privileges} != required
            or not bool(row.get("direct_table_grantors_trusted"))
        ):
            return "PROVIDER_DATABASE_PRIVILEGES_INVALID"
        if any(
            bool(row[column]) and privilege not in required
            for privilege, column in (
                ("SELECT", "can_column_select"),
                ("INSERT", "can_column_insert"),
                ("UPDATE", "can_column_update"),
                ("REFERENCES", "can_column_references"),
            )
        ):
            return "PROVIDER_DATABASE_PRIVILEGES_INVALID"
        if table_name in _ROW_SCOPED_PROVIDER_TABLES and not (
            bool(row["row_security"]) and bool(row["force_row_security"])
        ):
            return "PROVIDER_DATABASE_ROW_SCOPE_REQUIRED"
    for (schema_name, table_name), row in relation_rows.items():
        if schema_name == "public" and table_name in _PROVIDER_PRIVILEGES:
            continue
        if any(
            bool(row[column])
            for column in (
                "can_select",
                "can_insert",
                "can_update",
                "can_delete",
                "can_truncate",
                "can_references",
                "can_trigger",
                "can_maintain",
                "can_column_select",
                "can_column_insert",
                "can_column_update",
                "can_column_references",
            )
        ):
            return "PROVIDER_DATABASE_PRIVILEGES_TOO_BROAD"
    return None


def _provider_policy_violation(
    rows: Sequence[Mapping[str, object]],
    *,
    provider_role: str,
) -> str | None:
    expected: dict[tuple[str, str], tuple[tuple[str, ...], str, str, tuple[str, ...]]] = {}
    escaped_role = provider_role.replace("'", "''")
    bypass = f"NOT pg_catalog.pg_has_role(CURRENT_USER, '{escaped_role}', 'MEMBER')"
    provider_dependencies = {
        "jobs": (
            "public.nexus_provider_job_visible",
            "public.nexus_provider_job_writable",
        ),
        "job_leases": ("public.nexus_provider_job_owned",),
        "job_attempts": ("public.nexus_provider_job_owned",),
        "job_dependencies": ("public.nexus_provider_job_owned",),
        "worker_heartbeats": ("public.nexus_provider_queue",),
        "fosun_daily_bar_staging": ("public.nexus_provider_job_owned",),
    }
    for table_name, (using, with_check) in _PROVIDER_POLICY_EXPRESSIONS.items():
        expected[(table_name, f"nexus_provider_{table_name}_scope")] = (
            (provider_role,),
            using,
            with_check,
            provider_dependencies[table_name],
        )
        expected[(table_name, f"nexus_non_provider_{table_name}_bypass")] = (
            ("public",),
            bypass,
            bypass,
            (),
        )
    actual = {(str(row["table_name"]), str(row["policy_name"])): row for row in rows}
    if len(actual) != len(rows) or set(actual) != set(expected):
        return "PROVIDER_DATABASE_POLICY_SET_INVALID"
    for key, (roles, using, with_check, function_dependencies) in expected.items():
        row = actual[key]
        actual_roles = row["roles"]
        actual_dependencies = row["function_dependencies"]
        if not isinstance(actual_roles, Sequence) or isinstance(actual_roles, str):
            return "PROVIDER_DATABASE_POLICY_SET_INVALID"
        if not isinstance(actual_dependencies, Sequence) or isinstance(actual_dependencies, str):
            return "PROVIDER_DATABASE_POLICY_SET_INVALID"
        if (
            tuple(str(role) for role in actual_roles) != roles
            or tuple(str(item) for item in actual_dependencies) != function_dependencies
            or not bool(row["builtin_binding_valid"])
            or row["permissive"] != "PERMISSIVE"
            or row["command"] != "ALL"
            or _normalize_policy_expression(row["qual"]) != _normalize_policy_expression(using)
            or _normalize_policy_expression(row["with_check"])
            != _normalize_policy_expression(with_check)
        ):
            return "PROVIDER_DATABASE_POLICY_SET_INVALID"
    return None


def _provider_policy_function_violation(
    rows: Sequence[Mapping[str, object]],
) -> str | None:
    actual = {str(row["function_name"]): row for row in rows}
    if len(actual) != len(rows) or set(actual) != set(_PROVIDER_POLICY_FUNCTIONS):
        return "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"
    for name, expected in _PROVIDER_POLICY_FUNCTIONS.items():
        row = actual[name]
        if (
            row["identity_arguments"] != expected.identity_arguments
            or _normalize_plpgsql_source(row["source"])
            != _normalize_plpgsql_source(expected.source)
            or row["volatility"] != expected.volatility
            or row["parallel_safety"] != "s"
            or bool(row["security_definer"])
            or not bool(row["search_path_fixed"])
            or row["language"] != "sql"
            or not bool(row["returns_boolean"])
            or not bool(row["trusted_owner"])
            or bool(row["provider_is_owner_member"])
            or not bool(row["provider_execute"])
            or bool(row["public_execute"])
            or bool(row["provider_execute_grantable"])
            or not bool(row["acl_only_owner_and_provider"])
            or not bool(row["acl_grantor_only_owner"])
        ):
            return "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"
    return None


def _provider_publication_function_violation(
    function_rows: Sequence[Mapping[str, object]],
    trigger_rows: Sequence[Mapping[str, object]],
) -> str | None:
    actual_functions = {
        (str(row["schema_name"]), str(row["function_name"])): row for row in function_rows
    }
    expected_function_keys = {("public", name) for name in _PROVIDER_SECURITY_DEFINER_FUNCTIONS}
    if (
        len(actual_functions) != len(function_rows)
        or set(actual_functions) != expected_function_keys
    ):
        return "PROVIDER_DATABASE_PUBLICATION_FUNCTION_INVALID"
    for name, expected in _PROVIDER_SECURITY_DEFINER_FUNCTIONS.items():
        row = actual_functions[("public", name)]
        if (
            row["identity_arguments"] != expected.identity_arguments
            or _normalize_plpgsql_source(row["source"])
            != _normalize_plpgsql_source(expected.source)
            or row["volatility"] != "v"
            or row["parallel_safety"] != "u"
            or not bool(row["security_definer"])
            or bool(row["leakproof"])
            or not bool(row["search_path_exact"])
            or row["language"] != "plpgsql"
            or not bool(row["returns_trigger"])
            or not bool(row["trusted_owner"])
            or bool(row["provider_is_owner_member"])
            or bool(row["provider_execute"])
            or bool(row["public_execute"])
            or not bool(row["acl_only_owner"])
            or not bool(row["acl_grantor_only_owner"])
        ):
            return "PROVIDER_DATABASE_PUBLICATION_FUNCTION_INVALID"

    expected_triggers = set(_PROVIDER_SECURITY_DEFINER_TRIGGERS)
    actual_triggers = {
        (str(trigger["table_name"]), str(trigger["trigger_name"])) for trigger in trigger_rows
    }
    if len(actual_triggers) != len(trigger_rows) or actual_triggers != expected_triggers:
        return "PROVIDER_DATABASE_PUBLICATION_FUNCTION_INVALID"
    for trigger in trigger_rows:
        expected_function, expected_arguments, expected_type = _PROVIDER_SECURITY_DEFINER_TRIGGERS[
            (str(trigger["table_name"]), str(trigger["trigger_name"]))
        ]
        if (
            trigger["schema_name"] != "public"
            or trigger["function_name"] != expected_function
            or trigger["identity_arguments"] != expected_arguments
            or trigger["trigger_type"] != expected_type
            or trigger["enabled"] != "O"
            or bool(trigger["internal"])
            or not bool(trigger["unconditional"])
            or not bool(trigger["non_constraint"])
            or not bool(trigger["non_deferrable"])
            or not bool(trigger["initially_immediate"])
            or not bool(trigger["no_arguments"])
            or not bool(trigger["not_partition_clone"])
            or not bool(trigger["no_update_columns"])
            or trigger["relation_kind"] != "r"
        ):
            return "PROVIDER_DATABASE_PUBLICATION_FUNCTION_INVALID"
    return None


def _provider_trigger_topology_violation(
    rows: Sequence[Mapping[str, object]],
) -> str | None:
    actual = {(str(row["table_name"]), str(row["trigger_name"])): row for row in rows}
    if len(actual) != len(rows) or set(actual) != set(_PROVIDER_TRIGGER_TOPOLOGY):
        return "PROVIDER_DATABASE_TRIGGER_TOPOLOGY_INVALID"
    for key, (function_name, trigger_type) in _PROVIDER_TRIGGER_TOPOLOGY.items():
        row = actual[key]
        function = _PROVIDER_TRIGGER_FUNCTIONS[function_name]
        if (
            row["schema_name"] != "public"
            or row["function_schema"] != "public"
            or row["function_name"] != function_name
            or row["identity_arguments"] != ""
            or row["source_sha256"] != function.source_sha256
            or row["trigger_type"] != trigger_type
            or row["enabled"] != "O"
            or bool(row["internal"])
            or not bool(row["unconditional"])
            or not bool(row["non_constraint"])
            or not bool(row["non_deferrable"])
            or not bool(row["initially_immediate"])
            or not bool(row["no_arguments"])
            or not bool(row["not_partition_clone"])
            or not bool(row["no_update_columns"])
            or row["relation_kind"] != "r"
            or row["volatility"] != "v"
            or row["parallel_safety"] != "u"
            or bool(row["strict"])
            or bool(row["security_definer"]) != function.security_definer
            or bool(row["leakproof"])
            or bool(row["search_path_exact"]) != function.search_path_exact
            or bool(row["no_function_config"]) == function.search_path_exact
            or row["language"] != "plpgsql"
            or not bool(row["returns_trigger"])
            or not bool(row["trusted_owner"])
            or bool(row["provider_is_owner_member"])
        ):
            return "PROVIDER_DATABASE_TRIGGER_TOPOLOGY_INVALID"
    return None


def _normalize_policy_expression(value: object) -> str:
    if not isinstance(value, str):
        return ""
    parts = re.split(r"('(?:''|[^'])*')", value)
    normalized: list[str] = []
    for index, part in enumerate(parts):
        if index % 2:
            normalized.append(part)
            continue
        unquoted = part.lower().replace("public.", "").replace("pg_catalog.", "")
        unquoted = re.sub(r"::(?:text|character varying|name)", "", unquoted)
        normalized.append(re.sub(r"[\s()]", "", unquoted))
    return "".join(normalized)


def _normalize_plpgsql_source(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", value.strip().rstrip(";"))
