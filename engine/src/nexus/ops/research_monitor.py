"""Protected local monitoring entry. Installation never calls a model or data source."""

import argparse
import asyncio
import json
import os
from datetime import datetime
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
from nexus.settings import EngineSettings


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="nexus-engine-research-monitor")
    result.add_argument("--repo-root", type=Path, default=Path.cwd())
    result.add_argument("--env-file", type=Path, default=Path("infra/.env.local"))
    commands = result.add_subparsers(dest="action", required=True)
    commands.add_parser(
        "reconcile", help="Reconcile persisted research results into monitor coverage"
    )
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
    admit = commands.add_parser("admit", help="Admit one new single-security trigger review")
    admit.add_argument("--trigger-id", type=UUID, required=True)
    admit.add_argument("--calendar-authority-id", type=UUID, required=True)
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
    return await scan_configured_once(current, calendar_authority_id=calendar_authority_id)


async def load_host_settings(
    settings: EngineSettings,
    *,
    account_id: UUID,
    calendar_authority_id: UUID,
    repo_root: Path,
    env_file: Path | None = None,
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
    return current


async def scan_configured_once(
    settings: EngineSettings, *, calendar_authority_id: UUID
) -> dict[str, object]:
    return await run(
        settings, argparse.Namespace(action="scan", calendar_authority_id=calendar_authority_id)
    )


async def run(settings: EngineSettings, args: argparse.Namespace) -> dict[str, object]:
    if settings.agent_paper_account_id is None:
        raise ValueError("MONITOR_ACCOUNT_UNCONFIGURED")
    _validate_local_database(settings.database_url)
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
            if args.action == "finish":
                from nexus.monitoring.admission import finish_increment

                finished_trigger = await finish_increment(
                    session,
                    account_id=settings.agent_paper_account_id,
                    trigger_id=args.trigger_id,
                    now=now,
                )
                return {
                    "status": finished_trigger.status,
                    "reason": finished_trigger.reason,
                    "trigger_id": str(finished_trigger.id),
                    "run_id": str(finished_trigger.research_run_id),
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
                    member = await admit_trigger(
                        session,
                        account_id=settings.agent_paper_account_id,
                        trigger_id=args.trigger_id,
                        candidate_batch_id=settings.agent_paper_candidate_batch_id,
                        calendar_authority_id=args.calendar_authority_id,
                        model_config_id=settings.agent_paper_formal_model_config_id,
                        daily_limit_usd=settings.agent_daily_budget_usd,
                        daily_sdk_call_limit=settings.agent_monitor_daily_sdk_call_limit,
                        now=now,
                    )
                except (ValueError, BudgetExhausted) as error:
                    reason = (
                        "MONITOR_DAILY_LIMIT_REACHED"
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
                reconciled = await reconcile_account_results(
                    session,
                    account_id=settings.agent_paper_account_id,
                    instrument_ids=batch.instrument_ids,
                    now=now,
                )
                if args.action == "reconcile":
                    return reconciled
                scanned = await scan_account(
                    session,
                    account_id=settings.agent_paper_account_id,
                    calendar_authority_id=args.calendar_authority_id,
                    now=now,
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
    if args.action == "admit":
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
        }
        code = str(error) if str(error) in safe else "MONITOR_OPERATION_UNCONFIRMED"
        print(json.dumps({"status": "blocked", "error_code": code, "execution_locked": True}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
