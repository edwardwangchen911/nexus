"""Protected operator registration of the single approved SEC graphic judgment."""

import argparse
import asyncio
import json
from pathlib import Path
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from nexus.monitoring.filing_adoption import OBJECT_ID, SOURCE, read_adoption, register_adoption
from nexus.ops.accounting_review_local import read_json
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.persistence.migrations import SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--repo-root", type=Path, default=Path.cwd())
    commands = result.add_subparsers(dest="action", required=True)
    register = commands.add_parser("register")
    register.add_argument("--designation", type=Path, required=True)
    register.add_argument("--operator-confirmed", action="store_true", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--snapshot-id", type=UUID)
    return result


async def run(settings: EngineSettings, args: argparse.Namespace) -> dict[str, object]:
    if settings.agent_paper_account_id is None:
        raise ValueError("MONITOR_ACCOUNT_UNCONFIGURED")
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as db, db.begin():
            if args.action == "inspect":
                await db.execute(text("SET TRANSACTION READ ONLY"))
            await db.execute(text("SET LOCAL statement_timeout='10s'"))
            await db.execute(text("SET LOCAL lock_timeout='2s'"))
            await db.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key,0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            schema = await db.scalar(text("SELECT version_num FROM alembic_version"))
            if schema not in {
                "0134_sec_foundation",
                "0135_routine_budget",
                "0136_sec_adopted_content",
                "0137_linear_json_canonical",
                "0138_sec_accepted_cutoff",
                "0139_sec_audit_lookup",
            }:
                raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
            created = False
            if args.action == "register":
                if not args.operator_confirmed:
                    raise ValueError("SEC_GRAPHIC_OPERATOR_CONFIRMATION_REQUIRED")
                row, created = await register_adoption(
                    db, read_json(args.designation), account_id=settings.agent_paper_account_id
                )
                snapshot_id = row.id
            else:
                snapshot_id = args.snapshot_id
                if snapshot_id is None:
                    matches = list(
                        (
                            await db.scalars(
                                select(DataSnapshot.id).where(
                                    DataSnapshot.source == SOURCE,
                                    DataSnapshot.provider_object_id == OBJECT_ID,
                                )
                            )
                        ).all()
                    )
                    if len(matches) != 1:
                        raise ValueError("SEC_GRAPHIC_ADOPTION_INVALID")
                    snapshot_id = matches[0]
            now = await db.scalar(text("SELECT clock_timestamp()"))
            result = await read_adoption(
                db, snapshot_id=snapshot_id, account_id=settings.agent_paper_account_id, as_of=now
            )
            return {
                "status": "graphic_judgment_adopted",
                "snapshot_id": str(snapshot_id),
                "created": created,
                "purpose": result.purpose,
                "research_qualified": False,
                "provider_requests": 0,
                "model_requests": 0,
                "job_created": False,
                "execution_locked": True,
            }
    finally:
        await factory.kw["bind"].dispose()


def main() -> int:
    from nexus.ops.local_stack import _validate_local_env
    from nexus.ops.openai_local import local_settings

    args = parser().parse_args()
    root = args.repo_root.resolve()
    _validate_local_env(root, root / "infra/.env.local")
    try:
        result = asyncio.run(run(local_settings(root, load_model_key=False), args))
    except SQLAlchemyError:
        # A connection error may arrive after commit. Preserve the original
        # logical identity and inspect it; never claim rollback or retry here.
        print(
            json.dumps(
                {
                    "status": "unknown",
                    "reason": "SEC_GRAPHIC_COMMIT_UNCONFIRMED",
                    "next_action": "inspect_original_identity_without_resubmission",
                }
            )
        )
        return 2
    except (ValueError, KeyError, TypeError, OSError, BotoCoreError, ClientError):
        print(json.dumps({"status": "rejected", "reason": "SEC_GRAPHIC_ADOPTION_INVALID"}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
