"""Inspect current economic version authorities with no provider calls or business writes."""

import argparse
import asyncio
import json
from pathlib import Path
from uuid import UUID

from sqlalchemy import text

from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_stack import _validate_local_env
from nexus.ops.openai_local import local_settings
from nexus.paper.economic_authorities import EconomicAuthorityInspection, inspect_authorities
from nexus.persistence.session import session_factory


async def inspect(root: Path, proposal_hash: str) -> EconomicAuthorityInspection:
    root = root.resolve()
    if not (root / "infra/compose.yaml").is_file():
        raise ValueError("ECONOMIC_AUTHORITY_INPUT_INVALID")
    _validate_local_env(root, root / "infra/.env.local")
    environment = load_local_environment(root, {})
    settings = local_settings(root, backend="codex_sdk")
    _validate_local_database(settings.database_url)
    batch_id = UUID(environment.get("NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID", ""))
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            return await inspect_authorities(
                session,
                batch_id=batch_id,
                proposal_hash=proposal_hash,
                daily_limit_usd=settings.agent_daily_budget_usd,
            )
    finally:
        await factory.kw["bind"].dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--proposal-hash", required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(inspect(args.repo_root, args.proposal_hash))
    except (ValueError, OSError):
        print(json.dumps({"status": "unavailable", "code": "ECONOMIC_AUTHORITY_INPUT_INVALID"}))
        return 1
    except Exception:
        print(
            json.dumps({"status": "unavailable", "code": "ECONOMIC_AUTHORITY_DATABASE_UNAVAILABLE"})
        )
        return 1
    print(result.model_dump_json())
    return 0 if result.pair.reason_code == "VERSIONS_MATCH_AT_OBSERVATION" else 1


if __name__ == "__main__":
    raise SystemExit(main())
