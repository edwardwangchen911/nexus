"""Normal comparison preparation/admission/readback entries; never dispatch."""

import argparse
import asyncio
import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from nexus.agents.budget import BudgetExhausted
from nexus.agents.comparison_preparation import prepare_archive


async def native_command(args: argparse.Namespace) -> dict[str, Any]:
    from nexus.agents.comparison_manifest import ComparisonManifest
    from nexus.agents.comparison_producer import admit_comparison_position
    from nexus.agents.research_stages.routes import selected_route
    from nexus.ops.openai_local import local_settings
    from nexus.persistence.session import session_factory

    settings = local_settings(args.repo_root, load_model_key=False)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            if args.command == "inspect-position":
                await session.execute(text("SET TRANSACTION READ ONLY"))
                row = (
                    (
                        await session.execute(
                            text("""SELECT c.run_id,c.job_id,c.reserved_id,
                    r.status AS run_status,j.status AS job_status,
                    j.attempt_count,b.state AS budget_state
                    FROM research_comparison_chain c JOIN agent_research_runs r ON r.id=c.run_id
                    JOIN jobs j ON j.id=c.job_id
                    JOIN agent_daily_budget_reservations b ON b.id=c.reserved_id
                    WHERE c.experiment_id=:id AND c.ordinal=:ordinal"""),
                            {"id": args.experiment_id, "ordinal": args.position},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                route = None if row is None else await selected_route(session, row["job_id"])
                stopped = (
                    None
                    if route is None
                    else await session.scalar(
                        text("SELECT stopped FROM research_stage_scope WHERE id=:id"),
                        {"id": route["scope_id"]},
                    )
                )
                return {
                    "status": "position_not_admitted" if row is None else "position_registered",
                    "inspection_only": True,
                    "stage_route": None if route is None else route["kind"],
                    "stage_version": None if route is None else route["version"],
                    "stage_scope_stopped": stopped,
                    "dispatch_authority": False,
                    **(
                        {}
                        if row is None
                        else {k: str(v) if isinstance(v, UUID) else v for k, v in row.items()}
                    ),
                }
            manifest = ComparisonManifest.model_validate_json(args.manifest.read_bytes())
            with args.source.open("rb") as stream:
                source = stream.read(8 * 1024 * 1024 + 1)
            with args.prepared.open("rb") as stream:
                prepared = json.loads(stream.read(8 * 1024 * 1024 + 1))
            result = await admit_comparison_position(
                session,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=args.position,
                model_config_id=args.model_config_id,
                daily_limit_usd=settings.agent_daily_budget_usd,
            )
        return {
            "status": "position_prepared",
            "run_id": str(result.run_id),
            "job_id": str(result.job_id),
            "reservation_id": str(result.reservation_id),
            "dispatch_authority": False,
        }
    finally:
        await factory.kw["bind"].dispose()


def prepare_file(
    *, source: Path, expected_source_hash: str, experiment_id: UUID, output: Path
) -> dict[str, Any]:
    with source.open("rb") as stream:
        raw = stream.read(8 * 1024 * 1024 + 1)
    artifact = prepare_archive(
        raw, expected_source_hash=expected_source_hash, experiment_id=experiment_id
    )
    artifact["rebuilt_at"] = datetime.now(UTC).isoformat()
    wire = json.dumps(artifact, sort_keys=True, allow_nan=False).encode()
    # Never overwrite an earlier preparation or follow an existing output symlink.
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(wire)
        stream.flush()
        os.fsync(stream.fileno())
    return {
        "status": "prepared_only",
        "artifact_hash": hashlib.sha256(wire).hexdigest(),
        "members": len(artifact["members"]),
        "dispatch_enabled": False,
        "model_calls": 0,
        "database_writes": 0,
        "source_unchanged": hashlib.sha256(source.read_bytes()).hexdigest() == expected_source_hash,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    prepare = subcommands.add_parser("prepare")
    prepare.add_argument("--source", type=Path, required=True)
    prepare.add_argument("--source-sha256", required=True)
    prepare.add_argument("--experiment-id", type=UUID, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    for name in ("inspect-grant", "refresh-grant"):
        grant = subcommands.add_parser(name)
        grant.add_argument(
            "--credentials-dir",
            type=Path,
            default=Path.home() / ".config" / "nexus-chatgpt-latency-probe",
        )
    admit = subcommands.add_parser("admit-position")
    inspect = subcommands.add_parser("inspect-position")
    for subcommand in (admit, inspect):
        subcommand.add_argument("--repo-root", type=Path, required=True)
        subcommand.add_argument("--position", type=int, choices=range(1, 13), required=True)
    admit.add_argument("--source", type=Path, required=True)
    admit.add_argument("--prepared", type=Path, required=True)
    admit.add_argument("--manifest", type=Path, required=True)
    admit.add_argument("--model-config-id", type=UUID, required=True)
    inspect.add_argument("--experiment-id", type=UUID, required=True)
    args = parser.parse_args()
    try:
        if args.command in {"inspect-grant", "refresh-grant"}:
            from nexus.agents.chatgpt_grant import GrantStore

            store = GrantStore(args.credentials_dir)
            if args.command == "inspect-grant":
                result = asyncio.run(store.inspect(clock=lambda: datetime.now(UTC)))
            else:
                asyncio.run(store.refresh_once(clock=lambda: datetime.now(UTC)))
                result = {
                    "status": "access_ready",
                    "credential_writes": 1,
                    "model_calls": 0,
                    "dispatch_authority": False,
                }
            print(json.dumps(result))
            return 0
        if args.command != "prepare":
            result = asyncio.run(native_command(args))
            print(json.dumps(result))
            return 0
        result = prepare_file(
            source=args.source,
            expected_source_hash=args.source_sha256,
            experiment_id=args.experiment_id,
            output=args.output,
        )
    except SQLAlchemyError:
        print(
            json.dumps(
                {
                    "status": "unknown",
                    "reason": "COMPARISON_ADMISSION_RESULT_UNKNOWN"
                    if args.command == "admit-position"
                    else "COMPARISON_READ_FAILED",
                }
            )
        )
        return 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        code = "COMPARISON_BUDGET_EXHAUSTED" if isinstance(error, BudgetExhausted) else str(error)
        if not re.fullmatch(
            r"(?:COMPARISON|FORMAL_BUDGET|CODEX|CHATGPT)_[A-Z_]+", code
        ) and code not in {
            "RESEARCH_ONLY_SNAPSHOT_INVALID",
            "INDEPENDENT_MONITOR_CONTEXT_INVALID",
        }:
            code = "COMPARISON_PREPARATION_FAILED"
        details = {}
        if args.command in {"inspect-grant", "refresh-grant"}:
            from nexus.agents.chatgpt_grant import GrantError

            if isinstance(error, GrantError):
                details = {"phase": error.phase, "http_status": error.http_status}
        print(json.dumps({"status": "rejected", "reason": code, **details}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
