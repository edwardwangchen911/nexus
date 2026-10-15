"""Normal protected-config CLI for explicit admission and receipt-only inspection."""

import argparse
import asyncio
import importlib
import json
import subprocess
import sys
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select, text

from nexus.ops.agent_paper_loop import _FileEngineSettings, load_agent_paper_service_environment
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.local_stack import _checkout_revision, _validate_local_env
from nexus.paper.current_preparation_recovery import (
    PreparationRecoveryReceipt,
    recover_current_preparation,
)
from nexus.paper.history_seal import HistorySealReceipt, seal_failed_research
from nexus.paper.manual_cancel import (
    TerminalCycleCancelReceipt,
    close_terminal_cycle_unused_research,
)
from nexus.paper.manual_entry import (
    SAFE_ENTRY_CODES,
    manual_host_scope,
    read_configured_manual_request,
    require_manual_host,
    start_configured_manual_request,
)
from nexus.paper.manual_preparation_close import (
    PreparationCloseReceipt,
    preparation_service_control_lock,
)
from nexus.paper.manual_request import ManualRequestView
from nexus.paper.preparation_unknown_ack import UnknownPreparationReceipt
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


def _require_maintenance_code(root: Path, revision: str | None) -> None:
    """Bind this normal CLI's loaded Nexus sources to its checkout, not a caller label."""
    blocked = "MANUAL_CONFIGURATION_UNAVAILABLE"
    root = root.resolve()
    if revision is None or _checkout_revision(root) != revision:
        raise ValueError(blocked)
    # Partial proof imports these lazily; include the evidence compiler before
    # checking provenance, then recheck all loaded sources before commit.
    for name in ("nexus.api.research_stages", "nexus.ops.research_workload_inspection"):
        importlib.import_module(name)
    source = root / "engine/src"
    paths = {Path(__file__).resolve()}
    for name, module in tuple(sys.modules.items()):
        if name == "nexus" or name.startswith("nexus."):
            filename = getattr(module, "__file__", None)
            if filename is not None:
                paths.add(Path(filename).resolve())
    try:
        for path in sorted(paths):
            if not path.is_relative_to(source) or path.suffix != ".py":
                raise ValueError(blocked)
            relative = path.relative_to(root).as_posix()
            result = subprocess.run(
                ["git", "-C", str(root), "show", f"{revision}:{relative}"],
                capture_output=True,
                timeout=5,
                check=False,
            )
            if result.returncode != 0 or result.stdout != path.read_bytes():
                raise ValueError(blocked)
    except (OSError, subprocess.SubprocessError):
        raise ValueError(blocked) from None
    if _checkout_revision(root) != revision:
        raise ValueError(blocked)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="nexus-engine-manual-paper")
    result.add_argument("--repo-root", type=Path, default=Path.cwd())
    result.add_argument("--env-file", type=Path, default=Path("infra/.env.local"))
    commands = result.add_subparsers(dest="action", required=True)
    start = commands.add_parser("start")
    start.add_argument("--mode", choices=("research_only", "research_and_paper"), required=True)
    start.add_argument("--idempotency-key", required=True)
    read = commands.add_parser("read")
    read.add_argument("--request-id", type=UUID, required=True)
    recover = commands.add_parser("recover-preparation")
    recover.add_argument("--request-id", type=UUID, required=True)
    close = commands.add_parser("close-unstarted-preparation")
    close.add_argument("--request-id", type=UUID, required=True)
    ack = commands.add_parser("acknowledge-unknown-preparations")
    ack.add_argument("--request-id", type=UUID, required=True)
    ack.add_argument("--claim-id", dest="claim_ids", type=UUID, action="append", required=True)
    ack.add_argument("--accept-unknown-effects", action="store_true", required=True)
    cancel = commands.add_parser("cancel-unused-research")
    cancel.add_argument("--cycle-id", type=UUID, required=True)
    seal = commands.add_parser("seal-failed-research")
    seal.add_argument("--cycle-id", type=UUID, required=True)
    seal.add_argument("--run-id", dest="run_ids", type=UUID, action="append", required=True)
    seal.add_argument("--accept-unknown-usage", action="store_true", required=True)
    seal.add_argument(
        "--same-day",
        action="store_true",
        help="Close a terminal failed research today; preserve unknown usage and reservations",
    )
    return result


async def run(
    settings: EngineSettings, args: argparse.Namespace
) -> (
    ManualRequestView
    | TerminalCycleCancelReceipt
    | HistorySealReceipt
    | PreparationCloseReceipt
    | UnknownPreparationReceipt
    | PreparationRecoveryReceipt
):
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    lifecycle_guards = ExitStack()
    try:
        if args.action in {
            "close-unstarted-preparation",
            "acknowledge-unknown-preparations",
            "recover-preparation",
        }:
            _require_maintenance_code(args.repo_root, settings.release_revision)
            try:
                lifecycle_guards.enter_context(
                    preparation_service_control_lock(args.repo_root, settings.release_revision)
                )
            except RuntimeError:
                raise ValueError("MANUAL_PREPARATION_CLOSE_BLOCKED") from None
        async with factory.begin() as session:
            if args.action in {
                "close-unstarted-preparation",
                "acknowledge-unknown-preparations",
                "recover-preparation",
            }:
                from nexus.paper.manual_preparation_close import close_unstarted_preparation

                _require_maintenance_code(args.repo_root, settings.release_revision)
                if settings.agent_paper_account_id is None:
                    raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
                now = await session.scalar(select(func.clock_timestamp()))
                if not isinstance(now, datetime) or now.utcoffset() is None:
                    raise ValueError("MANUAL_RESULT_UNKNOWN")
                await session.execute(
                    text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
                    {"key": SCHEMA_ADMISSION_LOCK_KEY},
                )
                await session.execute(text("LOCK TABLE public.alembic_version IN SHARE MODE"))
                if (
                    await session.scalar(
                        text(
                            "SELECT CASE WHEN COUNT(*)=1 THEN MIN(version_num) END "
                            "FROM public.alembic_version"
                        )
                    )
                    != CURRENT_MIGRATION_HEAD
                ):
                    raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
                if args.action == "recover-preparation":
                    recovery_receipt = await recover_current_preparation(
                        session,
                        account_id=settings.agent_paper_account_id,
                        request_id=args.request_id,
                        repo_root=args.repo_root,
                        env_file=args.env_file,
                    )
                    _require_maintenance_code(args.repo_root, settings.release_revision)
                    return recovery_receipt
                if args.action == "acknowledge-unknown-preparations":
                    from nexus.paper.preparation_unknown_ack import acknowledge_unknown_preparations

                    ack_receipt = await acknowledge_unknown_preparations(
                        session,
                        settings=settings,
                        request_id=args.request_id,
                        claim_ids=tuple(args.claim_ids),
                        accept_unknown_effects=args.accept_unknown_effects,
                        now=now,
                        repo_root=args.repo_root,
                        env_file=args.env_file,
                    )
                    _require_maintenance_code(args.repo_root, settings.release_revision)
                    return ack_receipt
                close_receipt = await close_unstarted_preparation(
                    session,
                    account_id=settings.agent_paper_account_id,
                    request_id=args.request_id,
                    now=now,
                    repo_root=args.repo_root,
                    env_file=args.env_file,
                )
                _require_maintenance_code(args.repo_root, settings.release_revision)
                return close_receipt
            if args.action in {"cancel-unused-research", "seal-failed-research"}:
                if settings.agent_paper_account_id is None:
                    raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
                now = await session.scalar(select(func.clock_timestamp()))
                if now is None:
                    raise ValueError("MANUAL_RESULT_UNKNOWN")
                if args.action == "seal-failed-research":
                    _require_maintenance_code(args.repo_root, settings.release_revision)
                    await session.execute(
                        text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
                        {"key": SCHEMA_ADMISSION_LOCK_KEY},
                    )
                    # Not every historical migration takes the advisory lock.
                    # Hold the real version table until disposition commits.
                    await session.execute(text("LOCK TABLE public.alembic_version IN SHARE MODE"))
                    head = await session.scalar(
                        text(
                            "SELECT CASE WHEN COUNT(*) = 1 THEN MIN(version_num) END "
                            "FROM public.alembic_version"
                        )
                    )
                    if head != CURRENT_MIGRATION_HEAD:
                        raise ValueError("MANUAL_CONFIGURATION_UNAVAILABLE")
                    receipt = await seal_failed_research(
                        session,
                        account_id=settings.agent_paper_account_id,
                        cycle_id=args.cycle_id,
                        run_ids=tuple(args.run_ids),
                        now=now,
                        accept_unknown_usage=args.accept_unknown_usage,
                        same_day=getattr(args, "same_day", False),
                    )
                    _require_maintenance_code(args.repo_root, settings.release_revision)
                    return receipt
                rows = (
                    await session.scalars(
                        select(WorkerHeartbeat).where(
                            WorkerHeartbeat.queue == "agent_paper",
                            WorkerHeartbeat.seen_at >= now - timedelta(seconds=30),
                        )
                    )
                ).all()
                require_manual_host(
                    rows,
                    now=now,
                    release_revision=settings.release_revision,
                    scope_hash=manual_host_scope(settings),
                )
                return await close_terminal_cycle_unused_research(
                    session,
                    account_id=settings.agent_paper_account_id,
                    cycle_id=args.cycle_id,
                    now=now,
                )
            if args.action == "start":
                return await start_configured_manual_request(
                    session, settings, mode=args.mode, idempotency_key=args.idempotency_key
                )
            return await read_configured_manual_request(
                session, settings, request_id=args.request_id
            )
    finally:
        try:
            await factory.kw["bind"].dispose()
        finally:
            lifecycle_guards.close()


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        root = args.repo_root.resolve()
        env_file = args.env_file if args.env_file.is_absolute() else root / args.env_file
        env_file = _validate_local_env(root, env_file)
        environment = load_agent_paper_service_environment(root, {}, env_file=env_file)
        environment.pop("NEXUS_RELEASE_REVISION", None)
        revision = _checkout_revision(root)
        if revision is not None:
            environment["NEXUS_RELEASE_REVISION"] = revision
        settings = _FileEngineSettings.model_validate(environment)
        view = asyncio.run(run(settings, args))
    except Exception as error:
        code = str(error) if str(error) in SAFE_ENTRY_CODES else "MANUAL_RESULT_UNKNOWN"
        print(json.dumps({"status": "blocked", "error_code": code, "execution_locked": True}))
        return 1
    print(view.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
