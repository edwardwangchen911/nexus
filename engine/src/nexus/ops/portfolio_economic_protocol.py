"""Register an unactivated protocol only in an explicitly selected isolated database."""

import argparse
import asyncio
import json
from pathlib import Path

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.ops.research_budget_diagnostic import read_database_url
from nexus.paper.portfolio_economic_preflight import EconomicPlanRequest
from nexus.paper.portfolio_economic_protocol import decode_protocol, register_protocol


async def execute(
    *, input_path: Path, database_url_file: Path, expected_database: str
) -> dict[str, object]:
    url = read_database_url(database_url_file)
    if make_url(url).database != expected_database:
        raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
    request = EconomicPlanRequest.model_validate_json(input_path.read_bytes())
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            event = await register_protocol(session, request, expected_database=expected_database)
            plan = decode_protocol(event)
            return {
                "status": "registered_not_activated",
                "protocol_hash": plan.content_hash,
                "planned_slots": len(plan.slots),
                "economic_status": "not_evaluated",
                "execution_locked": True,
            }
    finally:
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--database-url-file", type=Path, required=True)
    parser.add_argument("--expected-database", required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(
            execute(
                input_path=args.input,
                database_url_file=args.database_url_file,
                expected_database=args.expected_database,
            )
        )
    except Exception as error:
        code = str(error)
        safe = {
            "ECONOMIC_PROTOCOL_DATABASE_MISMATCH",
            "ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN",
            "ECONOMIC_PROTOCOL_EVIDENCE_INVALID",
            "ECONOMIC_PROTOCOL_IDEMPOTENCY_CONFLICT",
            "ECONOMIC_PLAN_SESSION_INVALID",
            "ECONOMIC_PLAN_CALENDAR_INCOMPLETE",
        }
        print(
            json.dumps(
                {
                    "status": "unavailable",
                    "code": code if code in safe else "ECONOMIC_PROTOCOL_INPUT_OR_DATABASE_INVALID",
                }
            )
        )
        return 1
    print(json.dumps(result))
    return 0
