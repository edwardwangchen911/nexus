"""Isolated comparison preparation, exact once-only execution and readback."""

import argparse
import asyncio
import hashlib
import json
import os
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted, ResearchAccountingBusy
from nexus.agents.comparison_manifest import (
    PLATFORM_PURPOSE,
    PLATFORM_PURPOSES,
    PURPOSE,
    PURPOSES,
    parse_comparison_manifest,
)
from nexus.agents.comparison_preparation import prepare_archive
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION, PLATFORM_PRICE_VERSION


class PreclaimCommitUnknown(RuntimeError):
    def __init__(self) -> None:
        super().__init__("COMPARISON_PRECLOSE_RESULT_UNKNOWN")


@asynccontextmanager
async def _preclaim_transaction(session: AsyncSession) -> AsyncIterator[None]:
    transaction = await session.begin()
    try:
        yield
    except BaseException:
        await transaction.rollback()
        raise
    else:
        try:
            await transaction.commit()
        except Exception as error:
            raise PreclaimCommitUnknown() from error


async def native_command(args: argparse.Namespace) -> dict[str, Any]:
    from nexus.agents.comparison_producer import admit_comparison_position
    from nexus.agents.platform_funding import load_bound_platform_funding
    from nexus.agents.platform_manifest import PlatformManifest
    from nexus.ops.openai_local import local_settings
    from nexus.persistence.session import session_factory

    settings = local_settings(args.repo_root, load_model_key=False)
    factory = session_factory(settings)
    try:
        if args.command == "close-expired-position":
            from nexus.agents.comparison_closeout import close_expired_comparison_position

            return await close_expired_comparison_position(
                factory, args.experiment_id, args.position
            )
        if args.command == "run-position":
            from nexus.agents.chatgpt_grant import GrantStore
            from nexus.agents.comparison_execution import execute_comparison_position

            manifest = parse_comparison_manifest(json.loads(args.manifest.read_bytes()))
            with args.source.open("rb") as stream:
                source = stream.read(8 * 1024 * 1024 + 1)
            with args.prepared.open("rb") as stream:
                prepared = json.loads(stream.read(8 * 1024 * 1024 + 1))
            grant = None
            platform_funding_loader = None
            if isinstance(manifest, PlatformManifest):
                if getattr(args, "funding_proof", None) is None:
                    raise ValueError("PLATFORM_FUNDING_REQUIRED")
                # Reload the same frozen proof and actual dedicated Key before
                # claim and every send. Never disguise Platform as a Grant.
                platform_funding_loader = partial(
                    load_bound_platform_funding, args.repo_root, args.funding_proof, manifest
                )
            else:
                grant = await GrantStore(args.credentials_dir).load_verified(
                    clock=lambda: datetime.now(UTC)
                )
            # The executor owns short transactions and the once-only claim.
            # Never hold this command's admission transaction around inference.
            await execute_comparison_position(
                factory=factory,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=args.position,
                worker_id=args.worker_id,
                grant=grant,
                platform_funding_loader=platform_funding_loader,
            )
            return {
                "status": "position_executor_returned",
                "experiment_id": str(manifest.experiment_id),
                "position": args.position,
                "readback_required": True,
                "replay_allowed": False,
            }
        async with (
            factory() as session,
            _preclaim_transaction(session) if args.command == "close-preclaim" else session.begin(),
        ):
            if args.command in {
                "inspect-profile",
                "register-profile",
                "inspect-preclaim",
                "close-preclaim",
            }:
                await session.execute(text("SET LOCAL statement_timeout='5s'"))
                await session.execute(text("SET LOCAL lock_timeout='1s'"))
            if args.command in {"inspect-preclaim", "close-preclaim"}:
                from nexus.agents.comparison_preclaim import capacity_refusal, preclaim_close

                if args.command == "inspect-preclaim":
                    await session.execute(
                        text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    )
                manifest = parse_comparison_manifest(json.loads(args.manifest.read_bytes()))
                with args.source.open("rb") as stream:
                    source = stream.read(8 * 1024 * 1024 + 1)
                with args.prepared.open("rb") as stream:
                    prepared = json.loads(stream.read(8 * 1024 * 1024 + 1))
                return await preclaim_close(
                    session,
                    manifest=manifest,
                    refusal=capacity_refusal(source, prepared, manifest),
                    close=args.command == "close-preclaim",
                )
            if args.command == "inspect-profile":
                from nexus.agents.comparison_profile import read_api_profile

                await session.execute(text("SET TRANSACTION READ ONLY"))
                if getattr(args, "purpose", PURPOSE) in PLATFORM_PURPOSES:
                    from nexus.agents.platform_profile import read_platform_profile

                    profile = await read_platform_profile(
                        session,
                        version=(
                            CURRENT_PLATFORM_PRICE_VERSION
                            if args.purpose != PLATFORM_PURPOSE
                            else PLATFORM_PRICE_VERSION
                        ),
                    )
                else:
                    profile = await read_api_profile(session)
                return {
                    "status": "profile_not_registered" if profile is None else "profile_registered",
                    "inspection_only": True,
                    "dispatch_authority": False,
                    **(
                        {}
                        if profile is None
                        else {
                            "model_config_id": str(profile.id),
                            "config_hash": profile.config_hash,
                            "version": profile.version,
                        }
                    ),
                }
            if args.command == "register-profile":
                from nexus.agents.comparison_profile import register_api_profile

                if getattr(args, "purpose", PURPOSE) in PLATFORM_PURPOSES:
                    from nexus.agents.platform_profile import register_platform_profile

                    profile = await register_platform_profile(
                        session,
                        version=(
                            CURRENT_PLATFORM_PRICE_VERSION
                            if args.purpose != PLATFORM_PURPOSE
                            else PLATFORM_PRICE_VERSION
                        ),
                    )
                else:
                    profile = await register_api_profile(
                        session, sdk_model_config_id=args.sdk_model_config_id
                    )
                return {
                    "status": "profile_registered",
                    "model_config_id": str(profile.id),
                    "config_hash": profile.config_hash,
                    "version": profile.version,
                    "dispatch_authority": False,
                    "model_calls": 0,
                }
            if args.command == "inspect-position":
                from nexus.agents.comparison_closeout import inspect_comparison_terminal

                await session.execute(text("SET TRANSACTION READ ONLY"))
                return await inspect_comparison_terminal(session, args.experiment_id, args.position)
            manifest = parse_comparison_manifest(json.loads(args.manifest.read_bytes()))
            with args.source.open("rb") as stream:
                source = stream.read(8 * 1024 * 1024 + 1)
            with args.prepared.open("rb") as stream:
                prepared = json.loads(stream.read(8 * 1024 * 1024 + 1))
            funding = None
            if isinstance(manifest, PlatformManifest):
                if getattr(args, "funding_proof", None) is None:
                    raise ValueError("PLATFORM_FUNDING_REQUIRED")
                funding = load_bound_platform_funding(args.repo_root, args.funding_proof, manifest)
            result = await admit_comparison_position(
                session,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=args.position,
                model_config_id=args.model_config_id,
                daily_limit_usd=settings.agent_daily_budget_usd,
                platform_funding=funding,
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
    *,
    source: Path,
    expected_source_hash: str,
    experiment_id: UUID,
    output: Path,
    purpose: str = PURPOSE,
) -> dict[str, Any]:
    with source.open("rb") as stream:
        raw = stream.read(8 * 1024 * 1024 + 1)
    artifact = prepare_archive(
        raw,
        expected_source_hash=expected_source_hash,
        experiment_id=experiment_id,
        purpose=purpose,
        platform_price_version=(
            CURRENT_PLATFORM_PRICE_VERSION
            if purpose in PLATFORM_PURPOSES and purpose != PLATFORM_PURPOSE
            else PLATFORM_PRICE_VERSION
        ),
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
    prepare.add_argument("--purpose", choices=PURPOSES, default=PURPOSE)
    for name in ("inspect-grant", "refresh-grant"):
        grant = subcommands.add_parser(name)
        grant.add_argument(
            "--credentials-dir",
            type=Path,
            default=Path.home() / ".config" / "nexus-chatgpt-latency-probe",
        )
    admit = subcommands.add_parser("admit-position")
    inspect = subcommands.add_parser("inspect-position")
    run = subcommands.add_parser("run-position")
    close_expired = subcommands.add_parser("close-expired-position")
    profile = subcommands.add_parser("register-profile")
    profile.add_argument("--repo-root", type=Path, required=True)
    profile.add_argument("--sdk-model-config-id", type=UUID)
    profile.add_argument("--purpose", choices=PURPOSES, default=PURPOSE)
    profile_inspect = subcommands.add_parser("inspect-profile")
    profile_inspect.add_argument("--repo-root", type=Path, required=True)
    profile_inspect.add_argument("--purpose", choices=PURPOSES, default=PURPOSE)
    for name in ("inspect-preclaim", "close-preclaim"):
        command = subcommands.add_parser(name)
        for flag in ("repo-root", "source", "prepared", "manifest"):
            command.add_argument("--" + flag, type=Path, required=True)
    for subcommand in (admit, inspect, run, close_expired):
        subcommand.add_argument("--repo-root", type=Path, required=True)
        subcommand.add_argument("--position", type=int, choices=range(1, 13), required=True)
    for subcommand in (admit, run):
        subcommand.add_argument("--source", type=Path, required=True)
        subcommand.add_argument("--prepared", type=Path, required=True)
        subcommand.add_argument("--manifest", type=Path, required=True)
        subcommand.add_argument("--funding-proof", type=Path)
    run.add_argument("--worker-id", required=True)
    run.add_argument(
        "--credentials-dir",
        type=Path,
        default=Path.home() / ".config" / "nexus-chatgpt-latency-probe",
    )
    admit.add_argument("--model-config-id", type=UUID, required=True)
    inspect.add_argument("--experiment-id", type=UUID, required=True)
    close_expired.add_argument("--experiment-id", type=UUID, required=True)
    args = parser.parse_args()
    if (
        args.command == "register-profile"
        and args.purpose == PURPOSE
        and args.sdk_model_config_id is None
    ):
        parser.error("--sdk-model-config-id is required for the original comparison purpose")
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
            purpose=args.purpose,
        )
    except ResearchAccountingBusy:
        # native_command has exited its owning transaction. An executing
        # position may already have called the provider; never label it unclaimed.
        print(
            json.dumps(
                {
                    "status": "unknown"
                    if args.command in {"run-position", "close-expired-position"}
                    else "rejected",
                    "reason": "RESEARCH_ACCOUNTING_BUSY",
                    "replay_allowed": False,
                }
            )
        )
        return 1
    except SQLAlchemyError:
        print(
            json.dumps(
                {
                    "status": "unknown",
                    "reason": "COMPARISON_ADMISSION_RESULT_UNKNOWN"
                    if args.command == "admit-position"
                    else "COMPARISON_PROFILE_REGISTRATION_UNKNOWN"
                    if args.command == "register-profile"
                    else "COMPARISON_EXECUTION_RESULT_UNKNOWN"
                    if args.command == "run-position"
                    else "COMPARISON_PRECLOSE_RESULT_UNKNOWN"
                    if args.command == "close-preclaim"
                    else "COMPARISON_CLOSEOUT_RESULT_UNKNOWN"
                    if args.command == "close-expired-position"
                    else "COMPARISON_READ_FAILED",
                    **(
                        {"replay_allowed": False}
                        if args.command
                        in {"run-position", "close-preclaim", "close-expired-position"}
                        else {}
                    ),
                }
            )
        )
        return 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        code = "COMPARISON_BUDGET_EXHAUSTED" if isinstance(error, BudgetExhausted) else str(error)
        from nexus.ops.platform_credentials import PlatformCredentialError

        platform_safe = isinstance(error, PlatformCredentialError) or code in {
            "PLATFORM_FUNDING_REQUIRED",
            "PLATFORM_FUNDING_BINDING_CHANGED",
            "PLATFORM_PRIOR_USAGE_UNCONFIRMED",
            "PLATFORM_ALLOCATION_CHANGED",
            "PLATFORM_EXPERIMENT_BUDGET_EXCEEDED",
            "PLATFORM_PRICE_WINDOW_INVALID",
            "PLATFORM_PROFILE_EXISTING_INVALID",
        }
        if (
            not platform_safe
            and not re.fullmatch(r"(?:COMPARISON|FORMAL_BUDGET|CODEX|CHATGPT)_[A-Z_]+", code)
            and code
            not in {
                "RESEARCH_ONLY_SNAPSHOT_INVALID",
                "INDEPENDENT_MONITOR_CONTEXT_INVALID",
            }
        ):
            code = "COMPARISON_PREPARATION_FAILED"
        details: dict[str, Any] = (
            {"replay_allowed": False}
            if args.command in {"run-position", "close-preclaim", "close-expired-position"}
            else {}
        )
        if args.command in {"inspect-grant", "refresh-grant"}:
            from nexus.agents.chatgpt_grant import GrantError

            if isinstance(error, GrantError):
                details = {"phase": error.phase, "http_status": error.http_status}
        print(json.dumps({"status": "rejected", "reason": code, **details}))
        return 1
    except Exception:
        if args.command not in {"run-position", "close-preclaim", "close-expired-position"}:
            raise
        print(
            json.dumps(
                {
                    "status": "unknown",
                    "reason": "COMPARISON_PRECLOSE_RESULT_UNKNOWN"
                    if args.command == "close-preclaim"
                    else "COMPARISON_CLOSEOUT_RESULT_UNKNOWN"
                    if args.command == "close-expired-position"
                    else "COMPARISON_EXECUTION_RESULT_UNKNOWN",
                    "replay_allowed": False,
                }
            )
        )
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
