"""Local operator entry for the one approved historical NVDA disposition."""

import argparse
import asyncio
import json
import sys
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from uuid import UUID

from sqlalchemy import text

from nexus.monitoring.original_occupancy import BLOCKED, dispose
from nexus.ops.agent_paper_loop import (
    _FileEngineSettings,
    load_agent_paper_service_environment,
)
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.local_stack import _checkout_revision, _validate_local_env
from nexus.ops.manual_paper import _require_maintenance_code
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


class DispositionResultUnknown(RuntimeError):
    """Only exact receipt readback can resolve an unacknowledged commit."""


async def _run(
    settings: EngineSettings, args: argparse.Namespace, revalidate: Callable[[], None]
) -> dict[str, object]:
    account_id = settings.agent_paper_account_id
    _validate_local_database(settings.database_url)
    if account_id is None or not settings.agent_paper_cycle_enabled:
        raise ValueError(BLOCKED)

    factory = session_factory(settings)
    commit_started = False
    try:
        async with factory.begin() as session:
            if args.action == "check":
                await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            if not await session.scalar(
                text("SELECT pg_try_advisory_xact_lock_shared(hashtextextended(:key,0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            ) or await session.scalar(text("SELECT version_num FROM alembic_version")) != (
                CURRENT_MIGRATION_HEAD
            ):
                raise ValueError(BLOCKED)
            if args.action == "record" and await session.scalar(
                text("""
                    SELECT 1 FROM jobs
                    WHERE status IN ('queued','running')
                      AND job_type IN ('agent.paper.research','agent.research',
                                       'agent.portfolio_synthesis','agent.portfolio_decision')
                    LIMIT 1
                """),
            ):
                raise ValueError(BLOCKED)
            now = await session.scalar(text("SELECT clock_timestamp()"))
            if not isinstance(now, datetime):
                raise ValueError(BLOCKED)
            receipt = await dispose(
                session,
                action=args.action,
                account_id=account_id,
                run_id=args.run_id,
                expected_hash=args.expect_proof,
                now=now,
            )
            revalidate()
            commit_started = args.action != "check"
    except Exception as error:
        if commit_started:
            raise DispositionResultUnknown(BLOCKED) from error
        raise
    finally:
        await factory.kw["bind"].dispose()
    return receipt


def record_for_managed_exit(
    settings: EngineSettings, *, expected_hash: str, revalidate: Callable[[], None]
) -> dict[str, object]:
    """Called from the ordinary reconciliation action with both lifecycle locks held."""
    from nexus.monitoring.original_occupancy import RUN_ID

    args = argparse.Namespace(action="record", run_id=RUN_ID, expect_proof=expected_hash)
    return asyncio.run(_run(settings, args, revalidate))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check", "revoke"))
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--env-file", type=Path, default=Path("infra/.env.local"))
    parser.add_argument("--run-id", type=UUID, required=True)
    parser.add_argument("--expect-proof")
    args = parser.parse_args(argv)
    stage = "configuration"
    try:
        root = args.repo_root.resolve()
        env_file = _validate_local_env(root, root / args.env_file)
        revision = _checkout_revision(root)
        environment = load_agent_paper_service_environment(root, {}, env_file=env_file)
        if revision is None:
            raise ValueError(BLOCKED)
        environment["NEXUS_RELEASE_REVISION"] = revision
        settings = _FileEngineSettings.model_validate(environment)
        _require_maintenance_code(root, revision)
        stage = "disposition"
        receipt = asyncio.run(
            _run(settings, args, lambda: _require_maintenance_code(root, revision))
        )
    except Exception as error:
        # After an ambiguous database outcome, only "check" can resolve it.
        print(
            json.dumps(
                {
                    "status": "blocked" if isinstance(error, ValueError) else "unknown",
                    "error_code": BLOCKED,
                    "stage": stage,
                    "execution_locked": True,
                    "external_requests": 0,
                }
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps({**receipt, "execution_locked": True, "external_requests": 0}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
