"""Normal protected-config CLI for explicit admission and receipt-only inspection."""

import argparse
import asyncio
import importlib
import json
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import ExitStack, asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.ops.agent_paper_loop import _FileEngineSettings, load_agent_paper_service_environment
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.local_stack import _checkout_revision, _validate_local_env
from nexus.ops.preparation_diagnostics import ERROR_CODES, exception_code
from nexus.ops.synthesis_recovery_target import load_recovery_producer
from nexus.paper.current_preparation_acceptance import (
    PreparationAcceptanceCheck,
    PreparationAcceptanceReceipt,
    accept_current_unknown,
    check_current_unknown,
)
from nexus.paper.current_preparation_recovery import (
    PreparationRecoveryReceipt,
    recover_current_preparation,
)
from nexus.paper.history_seal import (
    HistorySealReceipt,
    ResearchProcessDiagnosticError,
    seal_failed_research,
)
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
    PreparationCloseDiagnosticError,
    PreparationCloseReceipt,
    preparation_service_control_lock,
    require_preparation_inspector_source,
)
from nexus.paper.manual_request import ManualRequestView
from nexus.paper.preparation_unknown_ack import UnknownPreparationReceipt
from nexus.paper.scheduled_unknown_acceptance import (
    ScheduledAcceptanceCheck,
    ScheduledAcceptanceReceipt,
    inspect_or_accept_scheduled_unknown,
)
from nexus.paper.synthesis_usage_acceptance import (
    SynthesisUsageDiagnosticError,
    SynthesisUsageReceipt,
    accept_failed_synthesis_usage,
    check_failed_synthesis_usage,
)
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


@dataclass
class _OperationDiagnostic:
    stage: str = "maintenance_guard"
    outcome: str = "not_started"
    primary: BaseException | None = None
    primary_stage: str = "maintenance_guard"
    secondary: list[dict[str, str]] = field(default_factory=list)

    def record(self, error: BaseException) -> None:
        if self.primary is None:
            self.primary = error
            self.primary_stage = self.stage
            error._nexus_manual_operation = self  # type: ignore[attr-defined]
        elif error is not self.primary:
            self.secondary.append({"stage": self.stage, "code": exception_code(error)})

    @asynccontextmanager
    async def transaction(
        self, factory: async_sessionmaker[AsyncSession]
    ) -> AsyncIterator[AsyncSession]:
        self.stage, self.outcome = "transaction_open", "unconfirmed"
        try:
            async with factory() as session:
                try:
                    await session.begin()
                    self.stage = "transaction_body"
                    try:
                        yield session
                        self.stage = "transaction_commit"
                        await session.commit()
                    except BaseException as error:
                        self.record(error)
                        self.stage = "transaction_rollback"
                        try:
                            await session.rollback()
                        except BaseException as rollback_error:
                            self.record(rollback_error)
                        raise
                    self.outcome = "committed"
                except BaseException as error:
                    self.record(error)
                    raise
                finally:
                    self.stage = "session_close"
        except BaseException as error:
            self.record(error)
            # Closing or rollback must not erase the first failure.
            if self.primary is not error:
                assert self.primary is not None
                raise self.primary from None
            raise


def _operation_fields(error: BaseException) -> dict[str, object]:
    state = getattr(error, "_nexus_manual_operation", None)
    stages = {
        "maintenance_guard",
        "transaction_open",
        "session_close",
        "transaction_body",
        "transaction_commit",
        "transaction_rollback",
        "engine_dispose",
        "lifecycle_unlock",
    }
    if (
        not isinstance(state, _OperationDiagnostic)
        or state.primary_stage not in stages
        or state.outcome not in {"not_started", "unconfirmed", "committed"}
    ):
        return {}
    result: dict[str, object] = {
        "operation_stage": state.primary_stage,
        "operation_code": exception_code(error),
        "transaction_outcome": state.outcome,
    }
    secondary = [
        item
        for item in state.secondary
        if set(item) == {"stage", "code"}
        and item["stage"] in stages
        and item["code"] in ERROR_CODES
    ]
    if secondary:
        result["secondary_failures"] = secondary
    return result


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
    bounded = commands.add_parser(
        "run-research", help="Run one complete pure-research request without scheduling"
    )
    bounded.add_argument("--idempotency-key", required=True)
    finish = commands.add_parser(
        "finish-research", help="Close persisted pure-research work without a model call"
    )
    finish.add_argument("--request-id", type=UUID, required=True)
    read = commands.add_parser("read")
    read.add_argument("--request-id", type=UUID, required=True)
    recover = commands.add_parser("recover-preparation")
    recover.add_argument("--request-id", type=UUID, required=True)
    recover.add_argument("--accept-unknown-effects", action="store_true")
    check = commands.add_parser("check-preparation-acceptance")
    check.add_argument("--request-id", type=UUID, required=True)
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
    seal.add_argument(
        "--expired-attempt",
        action="store_true",
        help="Use native lost-attempt proof with --same-day; never infer SDK usage",
    )
    for action in ("check-synthesis-acceptance", "accept-failed-synthesis-usage"):
        synthesis = commands.add_parser(action)
        synthesis.add_argument("--cycle-id", type=UUID, required=True)
        synthesis.add_argument("--run-id", type=UUID, required=True)
        synthesis.add_argument("--recovery-producer-repo-root", type=Path)
        synthesis.add_argument("--recovery-producer-env-file", type=Path)
        if action == "accept-failed-synthesis-usage":
            synthesis.add_argument("--accept-unknown-usage", action="store_true", required=True)
    for action in ("check-scheduled-acceptance", "accept-scheduled-unknown"):
        scheduled = commands.add_parser(action)
        scheduled.add_argument("--claim-id", type=UUID, required=True)
        if action == "accept-scheduled-unknown":
            scheduled.add_argument("--accept-unknown-effects", action="store_true", required=True)
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
    | PreparationAcceptanceReceipt
    | PreparationAcceptanceCheck
    | SynthesisUsageReceipt
    | ScheduledAcceptanceCheck
    | ScheduledAcceptanceReceipt
):
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    lifecycle_guards = ExitStack()
    diagnostic = _OperationDiagnostic()
    try:
        if args.action == "run-research":
            from nexus.ops.manual_paper_host import run_pure_research_once

            return await run_pure_research_once(settings, idempotency_key=args.idempotency_key)
        if args.action == "finish-research":
            from nexus.ops.agent_paper_loop import acquire_agent_paper_lock
            from nexus.ops.manual_paper_host import finish_configured_manual_request

            with acquire_agent_paper_lock():
                async with factory.begin() as session:
                    return await finish_configured_manual_request(
                        session, settings, request_id=args.request_id, cancel_unused=True
                    )
        if (
            args.action == "seal-failed-research" and getattr(args, "expired_attempt", False)
        ) or args.action in {
            "check-synthesis-acceptance",
            "accept-failed-synthesis-usage",
            "check-preparation-acceptance",
            "check-scheduled-acceptance",
            "accept-scheduled-unknown",
            "close-unstarted-preparation",
            "acknowledge-unknown-preparations",
            "recover-preparation",
        }:
            _require_maintenance_code(args.repo_root, settings.release_revision)
            try:
                lifecycle_guards.enter_context(
                    preparation_service_control_lock(args.repo_root, settings.release_revision)
                )
            except RuntimeError as error:
                reason = {
                    "AGENT_PAPER_SERVICE_CONTROL_BUSY": "LOCK_BUSY",
                    "AGENT_PAPER_SERVICE_CONTROL_LOCK_UNSAFE": "LOCK_UNSAFE",
                }.get(str(error), "LOCK_UNAVAILABLE")
                raise PreparationCloseDiagnosticError("service_control_lock", reason) from None
        async with diagnostic.transaction(factory) as session:
            if args.action in {
                "check-synthesis-acceptance",
                "accept-failed-synthesis-usage",
                "check-preparation-acceptance",
                "check-scheduled-acceptance",
                "accept-scheduled-unknown",
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
                if args.action in {"check-scheduled-acceptance", "accept-scheduled-unknown"}:
                    if not await session.scalar(
                        text("SELECT pg_try_advisory_xact_lock_shared(hashtextextended(:key,0))"),
                        {"key": SCHEMA_ADMISSION_LOCK_KEY},
                    ):
                        raise ValueError("SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED")
                    try:
                        await session.execute(
                            text("LOCK TABLE public.alembic_version IN SHARE MODE NOWAIT")
                        )
                    except DBAPIError as error:
                        if getattr(error.orig, "sqlstate", None) == "55P03":
                            raise ValueError("SCHEDULED_UNKNOWN_ACCEPTANCE_BLOCKED") from None
                        raise
                else:
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
                if args.action in {"check-scheduled-acceptance", "accept-scheduled-unknown"}:
                    scheduled_receipt = await inspect_or_accept_scheduled_unknown(
                        session,
                        settings=settings,
                        claim_id=args.claim_id,
                        repo_root=args.repo_root,
                        env_file=args.env_file,
                        accept=args.action == "accept-scheduled-unknown",
                        accept_unknown_effects=getattr(args, "accept_unknown_effects", False),
                    )
                    _require_maintenance_code(args.repo_root, settings.release_revision)
                    return scheduled_receipt
                if args.action in {"check-synthesis-acceptance", "accept-failed-synthesis-usage"}:
                    recovery = load_recovery_producer(settings, args)
                    if recovery is not None:
                        await recovery.verify(session)
                    options = dict(
                        account_id=settings.agent_paper_account_id,
                        cycle_id=args.cycle_id,
                        run_id=args.run_id,
                        now=now,
                        repo_root=recovery.repo_root if recovery else args.repo_root,
                        env_file=recovery.env_file if recovery else args.env_file,
                        inspector_repo_root=args.repo_root if recovery else None,
                        inspector_revision=settings.release_revision if recovery else None,
                    )
                    if args.action == "check-synthesis-acceptance":
                        synthesis_receipt = await check_failed_synthesis_usage(session, **options)
                    else:
                        synthesis_receipt = await accept_failed_synthesis_usage(
                            session, **options, accept_unknown_usage=args.accept_unknown_usage
                        )
                    _require_maintenance_code(args.repo_root, settings.release_revision)
                    if recovery is not None:
                        require_preparation_inspector_source(
                            args.repo_root, settings.release_revision
                        )
                        await recovery.verify(session)
                    return synthesis_receipt
                if args.action == "check-preparation-acceptance":
                    checked = await check_current_unknown(
                        session,
                        account_id=settings.agent_paper_account_id,
                        request_id=args.request_id,
                        repo_root=args.repo_root,
                        env_file=args.env_file,
                    )
                    _require_maintenance_code(args.repo_root, settings.release_revision)
                    return checked
                if args.action == "recover-preparation":
                    if getattr(args, "accept_unknown_effects", False):
                        accepted = await accept_current_unknown(
                            session,
                            account_id=settings.agent_paper_account_id,
                            request_id=args.request_id,
                            repo_root=args.repo_root,
                            env_file=args.env_file,
                        )
                        _require_maintenance_code(args.repo_root, settings.release_revision)
                        return accepted
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
                        expired_attempt=getattr(args, "expired_attempt", False),
                    )
                    _require_maintenance_code(args.repo_root, settings.release_revision)
                    return receipt
                from nexus.paper.manual_host_exit import valid_host_exit

                rows = (
                    await session.scalars(
                        select(WorkerHeartbeat).where(
                            WorkerHeartbeat.queue == "agent_paper",
                            WorkerHeartbeat.seen_at >= now - timedelta(seconds=30),
                            ~valid_host_exit(),
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
    except BaseException as error:
        diagnostic.record(error)
        raise
    finally:
        had_primary = diagnostic.primary is not None
        try:
            diagnostic.stage = "engine_dispose"
            await factory.kw["bind"].dispose()
        except BaseException as error:
            diagnostic.record(error)
        try:
            diagnostic.stage = "lifecycle_unlock"
            lifecycle_guards.close()
        except BaseException as error:
            diagnostic.record(error)
        if not had_primary and diagnostic.primary is not None:
            raise diagnostic.primary


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
        from nexus.agents.research_dispatch import dispatch_trace_output

        with dispatch_trace_output():
            view = asyncio.run(run(settings, args))
    except Exception as error:
        code = str(error) if str(error) in SAFE_ENTRY_CODES else "MANUAL_RESULT_UNKNOWN"
        report: dict[str, object] = {
            "status": "blocked",
            "error_code": code,
            "execution_locked": True,
        }
        if isinstance(
            error,
            PreparationCloseDiagnosticError
            | ResearchProcessDiagnosticError
            | SynthesisUsageDiagnosticError,
        ):
            report.update(error.safe_diagnostics())
        report.update(_operation_fields(error))
        print(json.dumps(report))
        return 1
    print(view.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
