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
    install = commands.add_parser("install", help="Install one original audited research proposal")
    install.add_argument("--output-id", type=UUID, required=True)
    install.add_argument("--expected-predecessor-id", type=UUID)
    inspect = commands.add_parser(
        "inspect", help="Read one installed plan without model/source I/O"
    )
    inspect.add_argument("--plan-id", type=UUID, required=True)
    scan = commands.add_parser("scan", help="Check only the current market-session bucket")
    scan.add_argument("--calendar-authority-id", type=UUID, required=True)
    return result


async def scan_host_once(
    settings: EngineSettings,
    *,
    account_id: UUID,
    calendar_authority_id: UUID,
    repo_root: Path,
    env_file: Path | None = None,
) -> dict[str, object]:
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
        return {"status": "disabled", "model_requests": 0, "provider_requests": 0}
    if (
        current.agent_paper_account_id != account_id
        or current.database_url is None
        or settings.database_url is None
        or make_url(current.database_url) != make_url(settings.database_url)
        or (pinned_calendar is not None and UUID(pinned_calendar) != calendar_authority_id)
    ):
        raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
    return await scan_configured_once(current, calendar_authority_id=calendar_authority_id)


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
            if args.action == "scan":
                return await scan_account(
                    session,
                    account_id=settings.agent_paper_account_id,
                    calendar_authority_id=args.calendar_authority_id,
                    now=now,
                )
            if args.action == "install":
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
                    MonitorPlan.account_id == plan.account_id,
                    MonitorPlan.instrument_id == plan.instrument_id,
                )
                .order_by(MonitorPlan.generation.desc())
                .limit(1)
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
                else "review_due"
                if now >= plan.review_due_at
                else "active",
                "execution_locked": True,
                "model_requests": 0,
                "provider_requests": 0,
            }
    finally:
        await factory.kw["bind"].dispose()


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        root = args.repo_root.resolve()
        env_file = args.env_file if args.env_file.is_absolute() else root / args.env_file
        env_file = _validate_local_env(root, env_file)
        environment = load_agent_paper_service_environment(root, {}, env_file=env_file)
        result = asyncio.run(run(_FileEngineSettings.model_validate(environment), args))
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
