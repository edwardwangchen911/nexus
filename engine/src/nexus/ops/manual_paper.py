"""Normal protected-config CLI for explicit admission and receipt-only inspection."""

import argparse
import asyncio
import json
from pathlib import Path
from uuid import UUID

from nexus.ops.agent_paper_loop import _FileEngineSettings, load_agent_paper_service_environment
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.local_stack import _validate_local_env
from nexus.paper.manual_entry import (
    SAFE_ENTRY_CODES,
    read_configured_manual_request,
    start_configured_manual_request,
)
from nexus.paper.manual_request import ManualRequestView
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


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
    return result


async def run(settings: EngineSettings, args: argparse.Namespace) -> ManualRequestView:
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory.begin() as session:
            if args.action == "start":
                return await start_configured_manual_request(
                    session, settings, mode=args.mode, idempotency_key=args.idempotency_key
                )
            return await read_configured_manual_request(
                session, settings, request_id=args.request_id
            )
    finally:
        await factory.kw["bind"].dispose()


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        root = args.repo_root.resolve()
        env_file = args.env_file if args.env_file.is_absolute() else root / args.env_file
        env_file = _validate_local_env(root, env_file)
        environment = load_agent_paper_service_environment(root, {}, env_file=env_file)
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
