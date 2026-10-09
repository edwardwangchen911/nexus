"""Register an exact version binding only in the selected isolated diagnostic database."""

import argparse
import asyncio
import json
from pathlib import Path

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.ops.research_budget_diagnostic import read_database_url
from nexus.paper.economic_version_binding import bind_versions, decode_binding


async def execute(
    *,
    database_url_file: Path,
    expected_database: str,
    protocol_hash: str,
    proposal_hash: str,
) -> dict[str, object]:
    url = read_database_url(database_url_file)
    if make_url(url).database != expected_database:
        raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            event = await bind_versions(
                session,
                expected_database=expected_database,
                protocol_hash=protocol_hash,
                proposal_hash=proposal_hash,
            )
            binding = decode_binding(event)
            return {
                "status": binding.status,
                "protocol_hash": binding.protocol_hash,
                "binding_hash": binding.content_hash,
                "registered_at": event.occurred_at.isoformat(),
                "model_version": binding.shared_model_version,
                "activation_permitted": False,
                "economic_status": "not_evaluated",
                "execution_locked": True,
            }
    finally:
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url-file", type=Path, required=True)
    parser.add_argument("--expected-database", required=True)
    parser.add_argument("--protocol-hash", required=True)
    parser.add_argument("--proposal-hash", required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(
            execute(
                database_url_file=args.database_url_file,
                expected_database=args.expected_database,
                protocol_hash=args.protocol_hash,
                proposal_hash=args.proposal_hash,
            )
        )
    except Exception as error:
        code = str(error)
        safe = {
            "ECONOMIC_PROTOCOL_DATABASE_MISMATCH",
            "ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN",
            "ECONOMIC_VERSION_BINDING_INPUT_INVALID",
            "ECONOMIC_VERSION_BINDING_INVALID",
            "ECONOMIC_VERSION_BINDING_CONFLICT",
            "ECONOMIC_VERSION_PROTOCOL_UNAVAILABLE",
            "ECONOMIC_VERSION_COST_CLAIM_MISMATCH",
            "MODEL_REVISION_MISMATCH",
            "CURRENT_MODEL_UNAVAILABLE",
            "MODEL_SPEC_UNSUPPORTED",
            "MODEL_PRICING_UNAVAILABLE",
            "PUBLICATION_UNAVAILABLE",
            "PROMPT_PAIR_INVALID",
            "PUBLICATION_MODEL_UNAVAILABLE",
        }
        print(
            json.dumps(
                {
                    "status": "unavailable",
                    "code": code
                    if code in safe
                    else "ECONOMIC_VERSION_BINDING_INPUT_OR_DATABASE_INVALID",
                }
            )
        )
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
