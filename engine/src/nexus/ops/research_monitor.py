"""Protected local monitoring entry. Installation never calls a model or data source."""

import argparse
import asyncio
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url

from nexus.monitoring.installation import install_plan_from_output
from nexus.monitoring.scanning import scan_account
from nexus.ops.agent_paper_loop import _FileEngineSettings, load_agent_paper_service_environment
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.local_stack import _validate_local_env
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="nexus-engine-research-monitor")
    result.add_argument("--repo-root", type=Path, default=Path.cwd())
    result.add_argument("--env-file", type=Path, default=Path("infra/.env.local"))
    commands = result.add_subparsers(dest="action", required=True)
    commands.add_parser(
        "reconcile", help="Reconcile persisted research results into monitor coverage"
    )
    commands.add_parser(
        "enable-auto-increment-limit",
        help="Register stock-session limits without research permission",
    )
    durable = commands.add_parser("enable-sec-monitor-rule", help="Register durable SEC discovery")
    durable.add_argument("--plan-id", type=UUID, required=True)
    for action in (
        "pause-sec-monitor-rule",
        "resume-sec-monitor-rule",
        "revoke-sec-monitor-rule",
        "inspect-sec-monitor-rule",
    ):
        durable = commands.add_parser(action, help="Operate an existing durable SEC rule")
        durable.add_argument("--rule-id", type=UUID, required=True)
    durable = commands.add_parser("record-sec-discovery", help="Verify an archived SEC source")
    durable.add_argument("--rule-id", type=UUID, required=True)
    durable.add_argument("--capture-id", type=UUID, required=True)
    bridge = commands.add_parser(
        "prepare-sec-event-research",
        help="Bind durable events to qualified research without a notice or job",
    )
    bridge.add_argument("--rule-id", type=UUID, required=True)
    for action in ("import-durable-sec-index", "collect-durable-sec-index"):
        source = commands.add_parser(action, help="Archive verified durable SEC originals")
        source.add_argument("--rule-id", type=UUID, required=True)
        source.add_argument("--identity-snapshot-id", type=UUID, required=True)
        source.add_argument("--archive-dir", type=Path, required=True)
    pending = commands.add_parser(
        "prepare-pending", help="Prepare one sealed batch without model I/O"
    )
    pending.add_argument("--batch-id", type=UUID, required=True)
    positive = commands.add_parser(
        "enable-recent-positive-once", help="Opt this one authorized plan into seen additions only"
    )
    positive.add_argument("--plan-id", type=UUID, required=True)
    for action in (
        "enable-positive-chain-scope",
        "ensure-positive-chain-member",
        "revoke-positive-chain-scope",
    ):
        chain = commands.add_parser(
            action, help="Explicit seven-day SEC comparison scope operation"
        )
        chain.add_argument("--plan-id", type=UUID, required=True)

    install = commands.add_parser("install", help="Install one original audited research proposal")
    install.add_argument("--kind", choices=("price", "disclosure"), default="price")
    install.add_argument("--output-id", type=UUID, required=True)
    install.add_argument("--expected-predecessor-id", type=UUID)
    inspect = commands.add_parser(
        "inspect", help="Read one installed plan without model/source I/O"
    )
    inspect.add_argument("--plan-id", type=UUID, required=True)
    scan = commands.add_parser("scan", help="Check only the current market-session bucket")
    scan.add_argument("--calendar-authority-id", type=UUID, required=True)
    observe = commands.add_parser(
        "observe", help="Refresh this current observation bucket without research"
    )
    observe.add_argument("--calendar-authority-id", type=UUID, required=True)
    candidates = commands.add_parser(
        "observe-sec-candidates",
        help="Archive official hints without granting research or negative coverage",
    )
    candidates.add_argument("--calendar-authority-id", type=UUID, required=True)
    candidates = commands.add_parser(
        "inspect-sec-candidates", help="Verify one saved official hint observation"
    )
    candidates.add_argument("--snapshot-id", type=UUID, required=True)
    content = commands.add_parser(
        "import-content", help="Import exact SEC filing inventory and every declared text original"
    )
    content.add_argument("--index-snapshot-id", type=UUID, required=True)
    content.add_argument("--accession", required=True)
    content.add_argument("--archive-dir", type=Path, required=True)
    collect = commands.add_parser(
        "collect-content", help="Collect exact SEC filing originals without research admission"
    )
    collect.add_argument("--index-snapshot-id", type=UUID, required=True)
    collect.add_argument("--accession", required=True)
    collect.add_argument("--archive-dir", type=Path, required=True)
    preparation = commands.add_parser(
        "prepare-batch",
        help="Freeze parent theses and verified SEC content without admitting research",
    )
    preparation.add_argument("--batch-id", type=UUID, required=True)
    admit = commands.add_parser("admit", help="Admit one new single-security trigger review")
    admit.add_argument("--trigger-id", type=UUID, required=True)
    admit.add_argument("--calendar-authority-id", type=UUID, required=True)
    mixed = commands.add_parser(
        "admit-batch", help="Admit one sealed mixed single-stock research job"
    )
    mixed.add_argument("--batch-id", type=UUID, required=True)
    mixed.add_argument("--materials-snapshot-id", type=UUID, required=True)
    mixed.add_argument("--calendar-authority-id", type=UUID, required=True)
    finish = commands.add_parser("finish", help="Record the original incremental job outcome")
    finish.add_argument("--trigger-id", type=UUID, required=True)
    return result


async def scan_host_once(
    settings: EngineSettings,
    *,
    account_id: UUID,
    calendar_authority_id: UUID,
    repo_root: Path,
    env_file: Path | None = None,
    scheduled_at: datetime | None = None,
) -> dict[str, object]:
    current = await load_host_settings(
        settings,
        account_id=account_id,
        calendar_authority_id=calendar_authority_id,
        repo_root=repo_root,
        env_file=env_file,
    )
    if not current.agent_paper_cycle_enabled:
        return {"status": "disabled", "model_requests": 0, "provider_requests": 0}
    return await scan_configured_once(
        current, calendar_authority_id=calendar_authority_id, scheduled_at=scheduled_at
    )


async def refresh_host_observations(
    settings: EngineSettings,
    *,
    account_id: UUID,
    calendar_authority_id: UUID,
    scheduled_at: datetime,
    repo_root: Path,
    env_file: Path | None = None,
) -> dict[str, object]:
    # Legacy hosts never gain a provider loop merely by loading new code.
    if (
        not settings.agent_paper_cycle_enabled
        or settings.agent_paper_formal_model_config_id is None
    ):
        return {
            "status": "disabled",
            "model_requests": 0,
            "provider_requests": 0,
            "execution_locked": True,
        }
    environment: dict[str, str] = {}
    current = await load_host_settings(
        settings,
        account_id=account_id,
        calendar_authority_id=calendar_authority_id,
        repo_root=repo_root,
        env_file=env_file,
        private_context=environment,
    )
    from nexus.ops.agent_paper_formal import require_formal_host_configuration_match

    require_formal_host_configuration_match(settings, current)
    if settings.agent_paper_candidate_batch_id != current.agent_paper_candidate_batch_id:
        raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
    return await refresh_configured_observations(
        current,
        calendar_authority_id=calendar_authority_id,
        scheduled_at=scheduled_at,
        environment=environment,
        repo_root=repo_root,
    )


def _candidate_deadline(preparation: float, bucket: float, now: float) -> float | None:
    # Existing providers each own 90s; price closeout 5s. Preserve a further
    # 25s for the existing bridge/account turn and 8s for the separate scan.
    deadline = min(now + 3, preparation - 210, bucket - 218)
    return deadline if deadline > now else None


async def refresh_configured_observations(
    settings: EngineSettings,
    *,
    calendar_authority_id: UUID,
    environment: dict[str, str],
    repo_root: Path,
    scheduled_at: datetime | None = None,
) -> dict[str, object]:
    from nexus.domain.time import require_utc
    from nexus.monitoring.host import preparation_deadline
    from nexus.paper.agent_cycle_authority import load_agent_paper_service_calendar_authority

    loop = asyncio.get_running_loop()
    preparation = preparation_deadline.get() or (loop.time() + 240)
    fixed: dict[str, object] = {"model_requests": 0, "job_created": False, "execution_locked": True}
    if (
        not settings.agent_paper_cycle_enabled
        or settings.agent_paper_account_id is None
        or settings.agent_paper_formal_model_config_id is None
    ):
        return {**fixed, "status": "disabled", "provider_requests": 0}
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            if (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                != CURRENT_MIGRATION_HEAD
            ):
                raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
            sampled = loop.time()
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
            current_bucket = now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0)
            bucket = current_bucket if scheduled_at is None else require_utc(scheduled_at)
            if bucket != current_bucket:
                return {**fixed, "status": "missed_bucket", "provider_requests": 0}
            calendar = await load_agent_paper_service_calendar_authority(
                session, now=now, expected_calendar_authority_id=calendar_authority_id
            )
            if not calendar.market_open_at <= now < calendar.market_close_at:
                return {**fixed, "status": "market_closed", "provider_requests": 0}
    finally:
        await factory.kw["bind"].dispose()
    # Cold completed-study verification can span several securities. Commit its
    # normal guarded reconciliation within the existing preparation allowance,
    # before source capture; the subsequent scan keeps its eight-second bound
    # and still checks for results that completed after this transaction.
    reconciled = await run(settings, argparse.Namespace(action="reconcile"))
    from nexus.ops.sec_atom_candidates import refresh_candidates

    # Optional discovery hints never qualify an event or replace capture B.
    # An unseen hint can defeat the brief reuse window, never the bucket once gate.
    optional_deadline = _candidate_deadline(
        preparation,
        sampled
        + (min(bucket + timedelta(minutes=5), calendar.market_close_at) - now).total_seconds(),
        loop.time(),
    )
    try:
        if optional_deadline is None:
            candidates = {
                "status": "skipped",
                "reason": "MONITOR_PRIMARY_TIME_RESERVED",
                "provider_requests": 0,
                "items": [],
            }
        else:
            async with asyncio.timeout_at(optional_deadline):
                candidates = await refresh_candidates(
                    settings,
                    calendar_authority_id=calendar_authority_id,
                    scheduled_at=bucket,
                    environment=environment,
                    repo_root=repo_root,
                )
    except Exception:
        candidates = {
            "status": "unknown",
            "provider_requests": None,
            "model_requests": 0,
            "negative_conclusion_authorized": False,
        }
    from nexus.ops.monitor_disclosure_refresh import refresh_disclosure_observations
    from nexus.ops.monitor_price_refresh import refresh_price_observations

    candidate_hints = {}
    candidate_items = candidates.get("items", [])
    for item in candidate_items if isinstance(candidate_items, list) else []:
        if isinstance(item, dict) and item.get("instrument_id") and item.get("candidate_hints"):
            candidate_hints[UUID(str(item["instrument_id"]))] = tuple(item["candidate_hints"])
    results: dict[str, dict[str, object]] = {}
    # SEC first; the final L1 capture keeps quotes fresh for the independent scan.
    for source, refresh in (
        ("disclosure", refresh_disclosure_observations),
        ("price", refresh_price_observations),
    ):
        try:
            results[source] = await refresh(
                settings,
                account_id=settings.agent_paper_account_id,
                calendar_authority_id=calendar_authority_id,
                scheduled_at=bucket,
                environment=environment,
                repo_root=repo_root,
                **({"candidate_hints": candidate_hints} if source == "disclosure" else {}),
            )
            if source == "disclosure" and results[source].get("status") in {
                "refreshed",
                "unchanged",
            }:
                from nexus.monitoring.durable_sec_host import prepare_registered_research

                results[source]["research_bridge"] = await prepare_registered_research(
                    settings,
                    account_id=settings.agent_paper_account_id,
                    calendar_authority_id=calendar_authority_id,
                    scheduled_at=bucket,
                )
        except Exception:
            results[source] = {
                "status": "unknown",
                "error_code": "MONITOR_OBSERVATION_REFRESH_FAILED",
                "provider_requests": None,
                "model_requests": 0,
            }
    request_counts = [value.get("provider_requests") for value in results.values()]
    request_counts.append(candidates.get("provider_requests"))
    known_requests = all(type(value) is int and value >= 0 for value in request_counts)
    ready_statuses = {
        "refreshed",
        "unchanged",
        "prepared",
        "disabled",
        "skipped",
        "market_closed",
        "missed_bucket",
    }
    status = (
        "refreshed"
        if all(value.get("status") in ready_statuses for value in results.values())
        else "partial"
    )
    return {
        **fixed,
        "status": status,
        "scheduled_at": bucket.isoformat(),
        "reconciliation": reconciled,
        "official_candidates": candidates,
        "sources": results,
        "provider_requests": sum(value for value in request_counts if isinstance(value, int))
        if known_requests
        else None,
    }


async def load_host_settings(
    settings: EngineSettings,
    *,
    account_id: UUID,
    calendar_authority_id: UUID,
    repo_root: Path,
    env_file: Path | None = None,
    private_context: dict[str, str] | None = None,
) -> EngineSettings:
    # Reload the existing protected host configuration even while research is busy.
    # This does not prepare market data, admit research, or change host identity.
    from nexus.ops.agent_paper_loop import _load_bound_loop_environment

    environment, pinned_calendar = await _load_bound_loop_environment(
        settings, repo_root, {} if env_file is not None else os.environ, env_file=env_file
    )
    current = (
        _FileEngineSettings.model_validate(environment)
        if env_file is not None
        else EngineSettings.model_validate(environment)
    )
    if not current.agent_paper_cycle_enabled:
        return current
    if (
        current.agent_paper_account_id != account_id
        or current.database_url is None
        or settings.database_url is None
        or make_url(current.database_url) != make_url(settings.database_url)
        or (pinned_calendar is not None and UUID(pinned_calendar) != calendar_authority_id)
    ):
        raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
    if private_context is not None:
        private_context.update(environment)
    return current


async def scan_configured_once(
    settings: EngineSettings, *, calendar_authority_id: UUID, scheduled_at: datetime | None = None
) -> dict[str, object]:
    return await run(
        settings,
        argparse.Namespace(
            action="scan", calendar_authority_id=calendar_authority_id, scheduled_at=scheduled_at
        ),
    )


async def _collect_content_with_scope(
    settings: EngineSettings, args: argparse.Namespace
) -> dict[str, object]:
    from nexus.ops.monitor_content_local import collect_filing_content
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
    from nexus.persistence.models.data import DataSnapshot

    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key,0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            if (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                != CURRENT_MIGRATION_HEAD
            ):
                raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
            now = await session.scalar(select(func.clock_timestamp()))
            candidate = (
                None
                if settings.agent_paper_candidate_batch_id is None
                else await load_agent_paper_candidate_batch(
                    session, batch_id=settings.agent_paper_candidate_batch_id
                )
            )
            source = await session.get(DataSnapshot, args.index_snapshot_id)
            index = None if source is None else source.payload.get("index")
            if (
                not isinstance(now, datetime)
                or candidate is None
                or candidate.account_id != settings.agent_paper_account_id
                or candidate.created_at > now
                or not isinstance(index, dict)
                or index.get("instrument_id")
                not in {str(value) for value in candidate.instrument_ids}
            ):
                raise ValueError("MONITOR_SCOPE_INVALID")
    finally:
        await factory.kw["bind"].dispose()
    # Source transport runs after the short scope transaction has closed.
    return await collect_filing_content(
        settings,
        index_snapshot_id=args.index_snapshot_id,
        accession=args.accession,
        archive_dir=args.archive_dir,
        contact=getattr(args, "_sec_contact", None) or "",
    )


async def _durable_index_with_scope(
    settings: EngineSettings, args: argparse.Namespace
) -> dict[str, object]:
    from nexus.monitoring.durable_sec_storage import rule_state
    from nexus.ops.durable_sec_index_local import collect_durable_index, import_durable_index
    from nexus.persistence.models.sec_monitor import SecMonitorRule
    from nexus.research.sec_exhibit_storage import _read_identity, configured_exhibit_store

    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key,0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            if (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                != CURRENT_MIGRATION_HEAD
            ):
                raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
            rule = await session.get(SecMonitorRule, args.rule_id)
            if (
                rule is None
                or rule.account_id != settings.agent_paper_account_id
                or await rule_state(session, rule) != "active"
            ):
                raise ValueError("SEC_DURABLE_SCOPE_INVALID")
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            store = configured_exhibit_store(session)
            identity = await _read_identity(
                session, store, snapshot_id=args.identity_snapshot_id, as_of=now
            )
            if (identity.instrument_id, identity.identity.cik) != (
                rule.instrument_id,
                rule.issuer_cik,
            ):
                raise ValueError("SEC_DURABLE_SCOPE_INVALID")
        if args.action == "collect-durable-sec-index":
            return await collect_durable_index(
                settings,
                identity_snapshot_id=args.identity_snapshot_id,
                archive_dir=args.archive_dir,
                contact=getattr(args, "_sec_contact", None) or "",
            )
        return await import_durable_index(
            settings, identity_snapshot_id=args.identity_snapshot_id, archive_dir=args.archive_dir
        )
    finally:
        await factory.kw["bind"].dispose()


async def run(settings: EngineSettings, args: argparse.Namespace) -> dict[str, object]:
    from nexus.paper.account_transaction_turn import account_transaction_turn

    if args.action not in {"scan", "reconcile"} or settings.agent_paper_account_id is None:
        return await _run(settings, args)
    bucket = None
    if args.action == "scan":
        _validate_local_database(settings.database_url)
        factory = session_factory(settings)
        try:
            async with factory.begin() as session:
                await session.execute(text("SET TRANSACTION READ ONLY"))
                at = await session.scalar(select(func.clock_timestamp()))
                if not isinstance(at, datetime) or at.utcoffset() is None:
                    raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
                from nexus.domain.time import require_utc

                original = getattr(args, "scheduled_at", None)
                bucket = (
                    at.replace(minute=at.minute // 5 * 5, second=0, microsecond=0)
                    if original is None
                    else require_utc(original)
                )
        finally:
            await factory.kw["bind"].dispose()
    async with account_transaction_turn(settings):
        return await _run(settings, args, expected_scan_bucket=bucket)


async def _run(
    settings: EngineSettings,
    args: argparse.Namespace,
    *,
    expected_scan_bucket: datetime | None = None,
) -> dict[str, object]:
    if settings.agent_paper_account_id is None:
        raise ValueError("MONITOR_ACCOUNT_UNCONFIGURED")
    _validate_local_database(settings.database_url)
    if args.action == "observe-sec-candidates":
        from nexus.ops.sec_atom_candidates import refresh_candidates

        return await refresh_candidates(
            settings,
            calendar_authority_id=args.calendar_authority_id,
            environment=getattr(args, "_observation_environment", {}),
            repo_root=getattr(args, "repo_root", Path.cwd()),
        )
    if args.action == "inspect-sec-candidates":
        from nexus.ops.sec_atom_candidates import inspect_snapshot

        return await inspect_snapshot(settings, args.snapshot_id)
    if args.action in {"import-durable-sec-index", "collect-durable-sec-index"}:
        return await _durable_index_with_scope(settings, args)
    if args.action == "observe":
        return await refresh_configured_observations(
            settings,
            calendar_authority_id=args.calendar_authority_id,
            environment=getattr(args, "_observation_environment", {}),
            repo_root=getattr(args, "repo_root", Path.cwd()),
        )
    if args.action == "prepare-pending":
        from nexus.ops.monitor_batch_prepare import prepare_pending_batch

        return await prepare_pending_batch(
            settings,
            batch_id=args.batch_id,
            repo_root=getattr(args, "repo_root", Path.cwd()),
            sec_contact=getattr(args, "_sec_contact", None),
        )
    if args.action == "collect-content":
        return await _collect_content_with_scope(settings, args)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            await session.execute(text("SET LOCAL lock_timeout='2s'"))
            await session.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            if (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                != CURRENT_MIGRATION_HEAD
            ):
                raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
            if (
                expected_scan_bucket is not None
                and now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0)
                != expected_scan_bucket
            ):
                return {
                    "status": "missed_bucket",
                    "scheduled_at": expected_scan_bucket.isoformat(),
                    "observed_at": now.isoformat(),
                    "checks": 0,
                    "model_requests": 0,
                    "provider_requests": 0,
                    "execution_locked": True,
                    "reconciled_results": 0,
                }
            if args.action == "enable-auto-increment-limit":
                from nexus.monitoring.auto_increment_policy import (
                    enable_scope as enable_auto_increment_scope,
                )

                if settings.agent_paper_candidate_batch_id is None:
                    raise ValueError("AUTO_INCREMENT_SCOPE_INVALID")
                frequency_scope = await enable_auto_increment_scope(
                    session,
                    account_id=settings.agent_paper_account_id,
                    candidate_batch_id=settings.agent_paper_candidate_batch_id,
                )
                return {
                    "status": "registered",
                    "snapshot_id": str(frequency_scope.id),
                    "max_per_stock_session": 1,
                    "research_authority_granted": False,
                    "model_requests": 0,
                    "provider_requests": 0,
                    "execution_locked": True,
                }
            if args.action == "prepare-sec-event-research":
                from nexus.monitoring.durable_sec_research import prepare_durable_sec_research_batch
                from nexus.paper.host_probe_state import require_host_admitting

                await require_host_admitting(session, settings.agent_paper_account_id)
                bridge_batch = await prepare_durable_sec_research_batch(
                    session, account_id=settings.agent_paper_account_id, rule_id=args.rule_id
                )
                return {
                    "status": "empty" if bridge_batch is None else "sealed",
                    "batch_id": None if bridge_batch is None else str(bridge_batch.id),
                    "model_requests": 0,
                    "provider_requests": 0,
                    "job_created": False,
                    "execution_locked": True,
                }
            if args.action in {
                "enable-sec-monitor-rule",
                "pause-sec-monitor-rule",
                "resume-sec-monitor-rule",
                "revoke-sec-monitor-rule",
                "inspect-sec-monitor-rule",
                "record-sec-discovery",
            }:
                from nexus.monitoring.durable_sec_contracts import RuleState
                from nexus.monitoring.durable_sec_storage import (
                    change_state,
                    enable_rule,
                    record_discovery,
                    rule_state,
                )
                from nexus.persistence.models.sec_monitor import SecMonitorRule

                rule: SecMonitorRule | None
                if args.action == "enable-sec-monitor-rule":
                    rule = await enable_rule(
                        session, account_id=settings.agent_paper_account_id, plan_id=args.plan_id
                    )
                else:
                    rule = await session.get(SecMonitorRule, args.rule_id)
                    if rule is None or rule.account_id != settings.agent_paper_account_id:
                        raise ValueError("SEC_DURABLE_SCOPE_INVALID")
                    if args.action == "record-sec-discovery":
                        from nexus.paper.host_probe_state import require_host_admitting

                        await require_host_admitting(session, rule.account_id)
                        check = await record_discovery(
                            session,
                            account_id=rule.account_id,
                            rule_id=rule.id,
                            capture_id=args.capture_id,
                            scheduled_at=now.replace(
                                minute=now.minute // 5 * 5, second=0, microsecond=0
                            ),
                        )
                        return {
                            "status": "observed",
                            "check_id": str(check.id),
                            "result": check.result,
                            "coverage": check.coverage,
                            "model_requests": 0,
                            "provider_requests": 0,
                            "execution_locked": True,
                        }
                    if args.action != "inspect-sec-monitor-rule":
                        states: dict[str, RuleState] = {
                            "pause-sec-monitor-rule": "paused",
                            "resume-sec-monitor-rule": "active",
                            "revoke-sec-monitor-rule": "revoked",
                        }
                        state = states[args.action]
                        await change_state(
                            session, account_id=rule.account_id, rule_id=rule.id, state=state
                        )
                return {
                    "status": await rule_state(session, rule),
                    "rule_id": str(rule.id),
                    "effective_at": rule.effective_at.isoformat(),
                    "expires_at": None,
                    "research_authority_granted": False,
                    "model_requests": 0,
                    "provider_requests": 0,
                    "execution_locked": True,
                }
            if args.action in {
                "enable-positive-chain-scope",
                "ensure-positive-chain-member",
                "revoke-positive-chain-scope",
            }:
                from nexus.monitoring.recent_positive_chain import (
                    enable_scope,
                    ensure_member,
                    revoke_scope,
                )

                if args.action == "ensure-positive-chain-member":
                    plan = await session.get(MonitorPlan, args.plan_id)
                    if plan is None or plan.account_id != settings.agent_paper_account_id:
                        raise ValueError("SEC_POSITIVE_CHAIN_SCOPE_INVALID")
                    policy = await ensure_member(session, plan=plan, now=now)
                else:
                    operation = (
                        enable_scope
                        if args.action == "enable-positive-chain-scope"
                        else revoke_scope
                    )
                    policy = await operation(
                        session,
                        account_id=settings.agent_paper_account_id,
                        plan_id=args.plan_id,
                        now=now,
                    )
                return {
                    "status": "recorded" if policy is not None else "not_applicable",
                    "snapshot_id": str(policy.id) if policy is not None else None,
                    "policy_hash": policy.revision if policy is not None else None,
                    "model_requests": 0,
                    "provider_requests": 0,
                    "job_created": False,
                    "execution_locked": True,
                }
            if args.action == "enable-recent-positive-once":
                from nexus.monitoring.recent_positive_policy import enable_policy

                policy = await enable_policy(
                    session,
                    account_id=settings.agent_paper_account_id,
                    plan_id=args.plan_id,
                    now=now,
                )
                return {
                    "status": "enabled",
                    "snapshot_id": str(policy.id),
                    "policy_hash": policy.revision,
                    "model_requests": 0,
                    "provider_requests": 0,
                    "job_created": False,
                    "execution_locked": True,
                }
            if args.action == "prepare-batch":
                from nexus.monitoring.batch_inputs import prepare_batch_inputs

                return await prepare_batch_inputs(
                    session,
                    account_id=settings.agent_paper_account_id,
                    batch_id=args.batch_id,
                    now=now,
                )
            if args.action == "import-content":
                from nexus.monitoring.filing_archive import import_filing_content
                from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
                from nexus.persistence.models.data import DataSnapshot

                candidate = (
                    None
                    if settings.agent_paper_candidate_batch_id is None
                    else await load_agent_paper_candidate_batch(
                        session, batch_id=settings.agent_paper_candidate_batch_id
                    )
                )
                source = await session.get(DataSnapshot, args.index_snapshot_id)
                index_payload = None if source is None else source.payload.get("index")
                if (
                    not isinstance(index_payload, dict)
                    or candidate is None
                    or candidate.account_id != settings.agent_paper_account_id
                    or source is None
                    or index_payload.get("instrument_id")
                    not in {str(value) for value in candidate.instrument_ids}
                ):
                    raise ValueError("MONITOR_SCOPE_INVALID")
                material, created = await import_filing_content(
                    session,
                    index_snapshot_id=args.index_snapshot_id,
                    accession=args.accession,
                    archive_dir=args.archive_dir,
                    now=now,
                )
                return {
                    "status": "created" if created else "existing",
                    "snapshot_id": str(material.snapshot_id),
                    "evidence_ref_id": str(material.evidence_ref_id),
                    "canonical_hash": material.canonical_hash,
                    "provider_requests": 0,
                    "model_requests": 0,
                    "job_created": False,
                    "execution_locked": True,
                    "impact_review": "not_performed",
                }
            if args.action == "finish":
                from nexus.monitoring.admission import finish_increment

                finished_trigger = await finish_increment(
                    session,
                    account_id=settings.agent_paper_account_id,
                    trigger_id=args.trigger_id,
                    now=now,
                )
                from nexus.monitoring.mixed_readback import mixed_batch_readback

                mixed_metadata: dict[str, object] = {}
                if finished_trigger.batch_id is not None:
                    mixed_metadata.update(
                        await mixed_batch_readback(session, finished_trigger.batch_id)
                    )
                return {
                    "status": finished_trigger.status,
                    "reason": (
                        "MONITOR_EVENT_REVIEW_VERIFIED"
                        if mixed_metadata.get("impact_review") == "verified"
                        else finished_trigger.reason
                    ),
                    **(
                        {"native_reason": finished_trigger.reason}
                        if finished_trigger.batch_id is not None
                        else {}
                    ),
                    **mixed_metadata,
                    "trigger_id": str(finished_trigger.id),
                    "run_id": str(finished_trigger.research_run_id),
                    "execution_locked": True,
                    "model_requests": 0,
                    "provider_requests": 0,
                }
            if args.action == "admit-batch":
                from nexus.monitoring.mixed_admission import admit_batch

                if (
                    settings.agent_paper_candidate_batch_id is None
                    or settings.agent_paper_formal_model_config_id is None
                ):
                    raise ValueError("MONITOR_CONFIGURATION_UNAVAILABLE")
                from nexus.agents.budget import BudgetExhausted
                from nexus.audit.service import append_audit_event
                from nexus.monitoring.storage import lock_security
                from nexus.paper.target_service import lock_portfolio_account
                from nexus.persistence.models.monitoring import (
                    MonitorObservationBatch,
                    MonitorTrigger,
                )

                await lock_portfolio_account(session, settings.agent_paper_account_id)
                sealed_batch = await session.get(MonitorObservationBatch, args.batch_id)
                if (
                    sealed_batch is None
                    or sealed_batch.account_id != settings.agent_paper_account_id
                ):
                    raise ValueError("MONITOR_BATCH_UNAVAILABLE")
                await lock_security(
                    session,
                    account_id=sealed_batch.account_id,
                    instrument_id=sealed_batch.instrument_id,
                )
                try:
                    from nexus.agents.platform_native import credential_for_preparation

                    member = await admit_batch(
                        session,
                        account_id=settings.agent_paper_account_id,
                        batch_id=args.batch_id,
                        materials_snapshot_id=args.materials_snapshot_id,
                        candidate_batch_id=settings.agent_paper_candidate_batch_id,
                        calendar_authority_id=args.calendar_authority_id,
                        model_config_id=settings.agent_paper_formal_model_config_id,
                        daily_limit_usd=settings.agent_daily_budget_usd,
                        daily_sdk_call_limit=settings.agent_monitor_daily_sdk_call_limit,
                        platform_credential=(
                            credential_for_preparation(settings)
                            if settings.agent_runtime_mode is AgentRuntimeMode.RESPONSES
                            else None
                        ),
                        now=now,
                    )
                except (ValueError, BudgetExhausted) as error:
                    members = tuple(
                        await session.scalars(
                            select(MonitorTrigger)
                            .where(MonitorTrigger.batch_id == sealed_batch.id)
                            .order_by(MonitorTrigger.id)
                        )
                    )
                    if tuple(str(row.id) for row in members) != tuple(
                        sealed_batch.trigger_ids
                    ) or any(
                        row.research_run_id is not None or row.status not in {"pending", "blocked"}
                        for row in members
                    ):
                        raise
                    reason = (
                        "SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED"
                        if str(error) == "SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED"
                        else "MONITOR_DAILY_LIMIT_REACHED"
                        if str(error) == "MONITOR_DAILY_CALL_LIMIT"
                        else "MONITOR_BUDGET_UNAVAILABLE"
                        if isinstance(error, BudgetExhausted)
                        else "MONITOR_ACCOUNT_OCCUPIED"
                        if str(error)
                        in {
                            "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN",
                            "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
                            "MANUAL_PREPARATION_ACK_BLOCKED",
                        }
                        else "MONITOR_INPUTS_UNAVAILABLE"
                    )
                    safe_codes = {
                        "MONITOR_DAILY_CALL_LIMIT",
                        "SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED",
                        "MONITOR_CURRENT_INPUTS_UNAVAILABLE",
                        "MONITOR_MARKET_CLOSED",
                        "MONITOR_SOURCE_INVALID",
                        "MONITOR_CONTEXT_INVALID",
                        "MONITOR_PREPARED_INPUTS_CHANGED",
                        "MONITOR_PENDING_OVERFLOW",
                        "MONITOR_TRIGGER_SUPERSEDED_OR_EXPIRED",
                        "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN",
                        "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
                        "MANUAL_PREPARATION_ACK_BLOCKED",
                    }
                    code = (
                        str(error) if str(error) in safe_codes else "MONITOR_ADMISSION_UNCONFIRMED"
                    )
                    for row in members:
                        row.status, row.reason = "blocked", reason
                    await append_audit_event(
                        session,
                        actor_id="research-monitor",
                        action="research.monitor.batch_admission_blocked",
                        schema_version="monitor_batch_admission_v1",
                        occurred_at=now,
                        payload={
                            "batch_id": str(sealed_batch.id),
                            "trigger_ids": list(sealed_batch.trigger_ids),
                            "reason": reason,
                            "error_code": code,
                        },
                    )
                    await session.flush()
                    return {
                        "status": "blocked",
                        "batch_id": str(sealed_batch.id),
                        "reason": reason,
                        "error_code": code,
                        "execution_locked": True,
                        "model_requests": 0,
                        "provider_requests": 0,
                    }
                from nexus.persistence.models.monitoring import MonitorTrigger

                first = await session.get(MonitorTrigger, member.trigger_id)
                return {
                    "status": "admitted" if first is None else first.status,
                    "batch_id": str(args.batch_id),
                    "run_id": str(member.run_id),
                    "job_id": str(member.job_id),
                    "reserved_sdk_calls": 3,
                    "execution_locked": True,
                    "model_requests": 0,
                    "provider_requests": 0,
                }
            if args.action == "admit":
                from nexus.agents.budget import BudgetExhausted
                from nexus.monitoring.admission import admit_trigger
                from nexus.monitoring.storage import mark_trigger_blocked

                if (
                    settings.agent_paper_candidate_batch_id is None
                    or settings.agent_paper_formal_model_config_id is None
                ):
                    raise ValueError("MONITOR_CONFIGURATION_UNAVAILABLE")
                from nexus.persistence.models.monitoring import MonitorTrigger

                trigger = await session.get(MonitorTrigger, args.trigger_id)
                if (
                    trigger is None
                    or trigger.kind != "price"
                    or trigger.account_id != settings.agent_paper_account_id
                ):
                    raise ValueError("MONITOR_TRIGGER_UNAVAILABLE")
                from nexus.monitoring.daily_consumption import daily_receipt
                from nexus.paper.target_service import lock_portfolio_account

                await lock_portfolio_account(session, settings.agent_paper_account_id)
                daily_covered = await daily_receipt(session, trigger.id)
                if daily_covered is not None:
                    return {
                        "status": "covered",
                        "trigger_id": str(trigger.id),
                        "run_id": str(daily_covered.run_id),
                        "output_id": str(daily_covered.output_id),
                        "execution_locked": True,
                        "model_requests": 0,
                        "provider_requests": 0,
                    }
                try:
                    from nexus.agents.platform_native import credential_for_preparation

                    member = await admit_trigger(
                        session,
                        account_id=settings.agent_paper_account_id,
                        trigger_id=args.trigger_id,
                        candidate_batch_id=settings.agent_paper_candidate_batch_id,
                        calendar_authority_id=args.calendar_authority_id,
                        model_config_id=settings.agent_paper_formal_model_config_id,
                        daily_limit_usd=settings.agent_daily_budget_usd,
                        daily_sdk_call_limit=settings.agent_monitor_daily_sdk_call_limit,
                        platform_credential=(
                            credential_for_preparation(settings)
                            if settings.agent_runtime_mode is AgentRuntimeMode.RESPONSES
                            else None
                        ),
                        now=now,
                    )
                except (ValueError, BudgetExhausted) as error:
                    reason = (
                        "SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED"
                        if str(error) == "SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED"
                        else "MONITOR_DAILY_LIMIT_REACHED"
                        if str(error) == "MONITOR_DAILY_CALL_LIMIT"
                        else "MONITOR_BUDGET_UNAVAILABLE"
                        if isinstance(error, BudgetExhausted)
                        else "MONITOR_ACCOUNT_OCCUPIED"
                        if str(error)
                        in {
                            "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN",
                            "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
                            "MANUAL_PREPARATION_ACK_BLOCKED",
                        }
                        else "MONITOR_INPUTS_UNAVAILABLE"
                    )
                    safe_codes = {
                        "MONITOR_TYPED_CONTEXT_REQUIRED",
                        "MONITOR_PENDING_OVERFLOW",
                        "MONITOR_PENDING_CONDITION_UNRESOLVED",
                        "MONITOR_DAILY_CALL_LIMIT",
                        "SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED",
                        "MONITOR_CURRENT_INPUTS_UNAVAILABLE",
                        "MONITOR_SOURCE_INVALID",
                        "MONITOR_CONTEXT_INVALID",
                        "MONITOR_INCREMENT_BINDING_INVALID",
                        "MONITOR_VERSION_BINDING_INVALID",
                        "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN",
                        "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
                        "MONITOR_TRIGGER_SUPERSEDED_OR_EXPIRED",
                        "MONITOR_MARKET_CLOSED",
                        "MONITOR_SCOPE_INVALID",
                        "MONITOR_EVIDENCE_SCOPE_INVALID",
                    }
                    code = (
                        str(error) if str(error) in safe_codes else "MONITOR_ADMISSION_UNCONFIRMED"
                    )
                    # The savepoint removed only failed admission, never its trigger/outbox.
                    await mark_trigger_blocked(
                        session, trigger_id=args.trigger_id, reason=reason, now=now
                    )
                    return {
                        "status": "blocked",
                        "reason": reason,
                        "error_code": code,
                        "execution_locked": True,
                        "model_requests": 0,
                        "provider_requests": 0,
                    }
                await session.refresh(trigger)
                return {
                    "status": trigger.status,
                    "run_id": str(member.run_id),
                    "job_id": str(member.job_id),
                    "trigger_id": str(member.trigger_id),
                    "execution_locked": True,
                    "model_requests": 0,
                    "provider_requests": 0,
                }
            if args.action in {"scan", "reconcile"}:
                from nexus.monitoring.reconciliation import reconcile_account_results
                from nexus.paper.candidate_batch import load_agent_paper_candidate_batch

                if settings.agent_paper_candidate_batch_id is None:
                    raise ValueError("MONITOR_SCOPE_UNCONFIGURED")
                batch = await load_agent_paper_candidate_batch(
                    session, batch_id=settings.agent_paper_candidate_batch_id
                )
                if (
                    batch is None
                    or batch.account_id != settings.agent_paper_account_id
                    or batch.created_at > now
                ):
                    raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
                from nexus.paper.host_probe_state import require_host_admitting

                await require_host_admitting(session, settings.agent_paper_account_id)
                reconciled = await reconcile_account_results(
                    session,
                    account_id=settings.agent_paper_account_id,
                    instrument_ids=batch.instrument_ids,
                    now=now,
                )
                if args.action == "reconcile":
                    return reconciled
                # Reconciliation can publish a plan and its inherited SEC
                # origin after the initial clock. Scan only facts visible at
                # the new clock, without moving this request to another bucket.
                scan_at = await session.scalar(select(func.clock_timestamp()))
                if (
                    not isinstance(scan_at, datetime)
                    or scan_at.utcoffset() is None
                    or scan_at < now
                ):
                    raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
                scheduled_at = now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0)
                scan_bucket = scan_at.replace(
                    minute=scan_at.minute // 5 * 5, second=0, microsecond=0
                )
                if scan_bucket != scheduled_at:
                    return {
                        "status": "missed_bucket",
                        "scheduled_at": scheduled_at.isoformat(),
                        "observed_at": scan_at.isoformat(),
                        "checks": 0,
                        "model_requests": 0,
                        "provider_requests": 0,
                        "execution_locked": True,
                        "reconciled_results": reconciled["recorded"],
                    }
                scanned = await scan_account(
                    session,
                    account_id=settings.agent_paper_account_id,
                    calendar_authority_id=args.calendar_authority_id,
                    now=scan_at,
                    instrument_ids=batch.instrument_ids,
                )
                return {**scanned, "reconciled_results": reconciled["recorded"]}
            if args.action == "install":
                if getattr(args, "kind", "price") == "disclosure":
                    from nexus.monitoring.disclosure_installation import install_disclosure_plan

                    plan = await install_disclosure_plan(
                        session,
                        account_id=settings.agent_paper_account_id,
                        output_id=args.output_id,
                        expected_predecessor_id=args.expected_predecessor_id,
                        now=now,
                    )
                    return {
                        "status": "installed",
                        "kind": "disclosure",
                        "plan_id": str(plan.id),
                        "source_run_id": str(plan.source_run_id),
                        "generation": plan.generation,
                        "baseline_snapshot_id": str(plan.baseline_snapshot_id),
                        "execution_locked": True,
                        "model_requests": 0,
                        "provider_requests": 0,
                    }
                plan = await install_plan_from_output(
                    session,
                    account_id=settings.agent_paper_account_id,
                    output_id=args.output_id,
                    expected_predecessor_id=args.expected_predecessor_id,
                    now=now,
                )
            elif args.action == "inspect":
                selected = await session.scalar(
                    select(MonitorPlan).where(
                        MonitorPlan.kind == "price",
                        MonitorPlan.id == args.plan_id,
                        MonitorPlan.account_id == settings.agent_paper_account_id,
                    )
                )
                if selected is None:
                    raise ValueError("MONITOR_PLAN_UNAVAILABLE")
                plan = selected
            else:
                raise ValueError("MONITOR_ACTION_INVALID")
            current = await session.scalar(
                select(MonitorPlan.id)
                .where(
                    MonitorPlan.kind == "price",
                    MonitorPlan.account_id == plan.account_id,
                    MonitorPlan.instrument_id == plan.instrument_id,
                )
                .order_by(MonitorPlan.generation.desc())
                .limit(1)
            )
            from nexus.monitoring.reconciliation import latest_coverage, plan_covered
            from nexus.persistence.models.agents import (
                AgentResearchOutputRecord,
                AgentResearchRunRecord,
            )

            coverage_receipt = await latest_coverage(
                session,
                account_id=settings.agent_paper_account_id,
                instrument_id=plan.instrument_id,
                now=now,
            )
            source_run = await session.get(AgentResearchRunRecord, plan.source_run_id)
            source_output = await session.get(AgentResearchOutputRecord, plan.source_output_id)
            applicable_receipt = (
                coverage_receipt
                if coverage_receipt is not None
                and source_run is not None
                and coverage_receipt.research_as_of >= source_run.decision_time
                else None
            )
            covered = await plan_covered(session, plan=plan, now=now)
            quality = (
                applicable_receipt.quality_status
                if applicable_receipt is not None
                else None
                if source_output is None or source_output.intent is None
                else source_output.intent.get("quality_status")
            )
            return {
                "status": "installed" if args.action == "install" else "observed",
                "plan_id": str(plan.id),
                "source_output_id": str(plan.source_output_id),
                "generation": plan.generation,
                "current": current == plan.id,
                "coverage": "superseded"
                if current != plan.id
                else "not_effective"
                if now < plan.effective_at
                else "expired"
                if now >= plan.valid_until
                else "unavailable"
                if not covered
                else "review_due"
                if now >= plan.review_due_at
                else "active",
                "research_quality": quality,
                "latest_result_state": None
                if applicable_receipt is None
                else applicable_receipt.status,
                "coverage_reason": (
                    "MONITOR_RESULT_RECONCILIATION_PENDING"
                    if not covered
                    and (applicable_receipt is None or applicable_receipt.active_plan_id == plan.id)
                    else None
                    if applicable_receipt is None
                    else applicable_receipt.reason
                ),
                "execution_locked": True,
                "model_requests": 0,
                "provider_requests": 0,
            }
    finally:
        await factory.kw["bind"].dispose()


async def run_configured(
    environment: dict[str, str], args: argparse.Namespace
) -> dict[str, object]:
    settings = _FileEngineSettings.model_validate(environment)
    if args.action in {"observe", "observe-sec-candidates"}:
        args = argparse.Namespace(**vars(args), _observation_environment=dict(environment))
    if args.action in {"collect-content", "prepare-pending", "collect-durable-sec-index"}:
        args = argparse.Namespace(**vars(args), _sec_contact=environment.get("SEC_CONTACT"))

    if args.action in {"admit", "admit-batch"}:
        from nexus.ops.formal_budget_protection import bind_runtime_environment

        bound, calendar = await bind_runtime_environment(settings, environment)
        if calendar is not None and UUID(calendar) != args.calendar_authority_id:
            raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
        settings = _FileEngineSettings.model_validate(bound)
        if not settings.agent_paper_cycle_enabled:
            raise ValueError("MONITOR_HOST_DISABLED")
    return await run(settings, args)


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        root = args.repo_root.resolve()
        env_file = args.env_file if args.env_file.is_absolute() else root / args.env_file
        env_file = _validate_local_env(root, env_file)
        environment = load_agent_paper_service_environment(root, {}, env_file=env_file)
        result = asyncio.run(run_configured(environment, args))
    except Exception as error:
        safe = {
            "MONITOR_ACCOUNT_UNCONFIGURED",
            "MONITOR_SCHEMA_UNAVAILABLE",
            "MONITOR_CLOCK_UNAVAILABLE",
            "MONITOR_SOURCE_INVALID",
            "MONITOR_SOURCE_NOT_NEWER",
            "MONITOR_PLAN_CONFLICT",
            "MONITOR_PLAN_UNAVAILABLE",
            "MONITOR_PROPOSAL_NOT_APPROVED",
            "MONITOR_ACCOUNT_UNAVAILABLE",
            "MONITOR_SCOPE_EXCEEDED",
            "AGENT_PAPER_CALENDAR_UNAVAILABLE",
            "RESEARCH_OUTPUT_NOT_ACTIONABLE",
            "SEC_MONITOR_ARCHIVE_INVALID",
            "SEC_MONITOR_ARCHIVE_UNAVAILABLE",
            "SEC_MONITOR_ARTIFACT_INVALID",
            "SEC_MONITOR_ENTRY_UNAVAILABLE",
            "SEC_MONITOR_ATTACHMENT_UNSUPPORTED",
            "SEC_MONITOR_CONTENT_CAPACITY",
            "SEC_MONITOR_DOCUMENT_SET_INVALID",
            "SEC_MONITOR_DOCUMENT_SIZE_INVALID",
            "SEC_MONITOR_DOCUMENT_TIME_INVALID",
            "SEC_MONITOR_IDENTITY_INVALID",
            "SEC_MONITOR_INVENTORY_IDENTITY_INVALID",
            "SEC_MONITOR_INVENTORY_INVALID",
            "SEC_MONITOR_ORIGINAL_INVALID",
            "SEC_MONITOR_CLOCK_UNAVAILABLE",
            "SEC_MONITOR_CONTACT_REQUIRED",
            "SEC_MONITOR_ENTRY_INVALID",
            "SEC_MONITOR_COLLECTION_DIRECTORY_INVALID",
            "SEC_MONITOR_COLLECTION_FAILED",
            "MONITOR_CONTEXT_INVALID",
            "MONITOR_INCREMENT_BINDING_INVALID",
            "MONITOR_CURRENT_INPUTS_UNAVAILABLE",
            "MONITOR_PENDING_OVERFLOW",
            "MONITOR_PREPARED_INPUTS_CHANGED",
            "MONITOR_DAILY_CALL_LIMIT",
            "SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED",
            "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN",
            "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
            "MANUAL_PREPARATION_ACK_BLOCKED",
            "MONITOR_AWAITING_RECONCILIATION",
            "MONITOR_PREPARED_INPUTS_INVALID",
            "MONITOR_CONTENT_MISSING",
            "MONITOR_CONTENT_CONFLICT",
            "MONITOR_MARKET_CLOSED",
            "MONITOR_BATCH_UNAVAILABLE",
            "MONITOR_SCOPE_INVALID",
        }
        code = str(error) if str(error) in safe else "MONITOR_OPERATION_UNCONFIRMED"
        print(json.dumps({"status": "blocked", "error_code": code, "execution_locked": True}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
