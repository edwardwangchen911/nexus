"""Read current Agent Paper candidate prerequisites without business writes."""

import argparse
import asyncio
import json
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select, text

from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_stack import LocalStackConfigurationError, _validate_local_env
from nexus.paper.candidate_batch import inspect_agent_paper_candidate_static_readiness
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


async def _inspect(
    settings: EngineSettings, *, account_id: UUID, batch_id: UUID
) -> dict[str, object]:
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            now = await session.scalar(select(func.clock_timestamp()))
            if now is None:
                raise ValueError("AGENT_PAPER_READINESS_UNAVAILABLE")
            return await inspect_agent_paper_candidate_static_readiness(
                session, account_id=account_id, batch_id=batch_id, as_of=now
            )
    finally:
        await factory.kw["bind"].dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nexus-engine-agent-paper-readiness")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    try:
        root = args.repo_root.resolve()
        if not (root / "infra/compose.yaml").is_file():
            raise LocalStackConfigurationError("REPOSITORY_ROOT_INVALID")
        _validate_local_env(root, root / "infra/.env.local")
        environment = load_local_environment(root, {})
        settings = EngineSettings.model_validate(environment)
        account_id = UUID(environment.get("NEXUS_AGENT_PAPER_ACCOUNT_ID", ""))
        batch_id = UUID(environment.get("NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID", ""))
        result = asyncio.run(_inspect(settings, account_id=account_id, batch_id=batch_id))
    except (KeyError, TypeError, ValueError, LocalStackConfigurationError):
        print(json.dumps({"status": "blocked", "error_code": "AGENT_PAPER_READINESS_INVALID"}))
        return 1
    except Exception:
        print(json.dumps({"status": "blocked", "error_code": "AGENT_PAPER_READINESS_UNAVAILABLE"}))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "ready_static" else 1


if __name__ == "__main__":
    raise SystemExit(main())
