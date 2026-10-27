"""Explicit fixed test notification through the existing protected local configuration."""

import argparse
import asyncio
import json
from pathlib import Path
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import ArgumentError

from nexus.notifications.diagnostic import (
    DiagnosticNotificationRejected,
    enqueue_diagnostic_notification,
    read_diagnostic_notification,
)
from nexus.notifications.worker import (
    BarkWorkerConfigurationError,
    load_bark_credentials,
    load_bark_worker_settings,
)
from nexus.ops.agent_paper_loop import load_agent_paper_service_environment
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.local_stack import _validate_local_env
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--repo-root", type=Path, default=Path.cwd())
    result.add_argument("--env-file", type=Path, default=Path("infra/.env.local"))
    result.add_argument("action", choices=("enqueue", "read"))
    result.add_argument("--test-id", type=UUID, required=True)
    return result


async def run(environment: dict[str, str], *, action: str, test_id: UUID) -> dict[str, object]:
    database = environment.get("DATABASE_URL")
    try:
        _validate_local_database(database)
    except (ValueError, ArgumentError):
        raise DiagnosticNotificationRejected("DIAGNOSTIC_DATABASE_UNAVAILABLE") from None
    settings = EngineSettings(database_url=database)
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            await session.execute(text("SET LOCAL lock_timeout='2s'"))
            await session.execute(text("SET LOCAL statement_timeout='10s'"))
            if action == "read":
                await session.execute(text("SET TRANSACTION READ ONLY"))
                return await read_diagnostic_notification(session, test_id=test_id)
            if action != "enqueue":
                raise ValueError("DIAGNOSTIC_NOTIFICATION_ACTION_INVALID")
            # Existing identities are read even after notification configuration changes.
            existing = await read_diagnostic_notification(session, test_id=test_id)
            if existing["status"] != "not_found":
                return existing
            try:
                bark = load_bark_worker_settings(environment)
                if not bark.enabled or bark.activated_at is None:
                    raise DiagnosticNotificationRejected("DIAGNOSTIC_NOTIFICATION_DISABLED")
                load_bark_credentials(bark)
            except BarkWorkerConfigurationError:
                raise DiagnosticNotificationRejected("DIAGNOSTIC_CONFIGURATION_INVALID") from None
            await session.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key,0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            await session.execute(text("LOCK TABLE public.alembic_version IN SHARE MODE"))
            head = await session.scalar(
                text(
                    "SELECT CASE WHEN COUNT(*)=1 THEN MIN(version_num) END "
                    "FROM public.alembic_version"
                )
            )
            if head != CURRENT_MIGRATION_HEAD:
                raise DiagnosticNotificationRejected("DIAGNOSTIC_NOTIFICATION_SCHEMA_UNAVAILABLE")
            return await enqueue_diagnostic_notification(
                session, test_id=test_id, activated_at=bark.activated_at
            )
    finally:
        await factory.kw["bind"].dispose()


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        root = args.repo_root.resolve()
        config = args.env_file if args.env_file.is_absolute() else root / args.env_file
        config = _validate_local_env(root, config)
        environment = load_agent_paper_service_environment(root, {}, env_file=config)
        receipt = asyncio.run(run(environment, action=args.action, test_id=args.test_id))
    except DiagnosticNotificationRejected as error:
        report = {
            "test_id": str(args.test_id),
            "operation": args.action,
            "operation_status": "rejected",
            "status": "unknown",
            "error_code": str(error),
            "next_action": "read_same_test_id",
        }
        if args.action == "enqueue":
            report["submission"] = "not_started"
        print(json.dumps(report))
        return 1
    except Exception:
        # A failed response/cleanup does not prove that the enqueue transaction rolled back.
        print(
            json.dumps(
                {
                    "test_id": str(args.test_id),
                    "status": "unknown",
                    "error_code": "DIAGNOSTIC_NOTIFICATION_UNCONFIRMED",
                    "next_action": "read_same_test_id",
                }
            )
        )
        return 1
    print(json.dumps(receipt))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
