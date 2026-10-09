"""Hold both lanes' diagnostic budget without creating or authorizing model jobs."""

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.ops.openai_local import local_settings
from nexus.ops.research_budget_diagnostic import read_database_url
from nexus.paper.economic_budget_hold import HoldRequest, decode_hold, hold_pair_budget
from nexus.paper.economic_version_binding import read_binding
from nexus.paper.portfolio_economic_protocol import read_protocol


async def execute(
    *,
    repo_root: Path,
    input_path: Path,
    database_url_file: Path,
    expected_database: str,
) -> dict[str, object]:
    request = HoldRequest.model_validate_json(input_path.read_bytes())
    configured_limit = local_settings(
        repo_root.resolve(), backend="codex_sdk"
    ).agent_daily_budget_usd
    url = read_database_url(database_url_file)
    if make_url(url).database != expected_database:
        raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("ECONOMIC_HOLD_AUTHORITY_MISMATCH")
            _, _, protocol = await read_protocol(session, as_of=now)
            _, binding = await read_binding(session, as_of=now)
            if (
                protocol is None
                or binding is None
                or binding.protocol_hash != request.protocol_hash
                or protocol.content_hash != request.protocol_hash
                or request.evaluation_key != protocol.request.evaluation_key
                or request.global_limit_usd != configured_limit
                or request.global_limit_usd != protocol.request.global_daily_limit_usd
                or request.research_cost_usd != binding.configured_cost_usd
                or request.synthesis_cost_usd != binding.configured_cost_usd
                or not binding.pricing_available_at <= now < binding.pricing_expires_at
            ):
                raise ValueError("ECONOMIC_HOLD_AUTHORITY_MISMATCH")
            event = await hold_pair_budget(session, request, expected_database=expected_database)
            hold = decode_hold(event)
            return {
                "status": hold.status,
                "budget_day_utc": event.occurred_at.date().isoformat(),
                "receipt_hash": event.event_hash,
                "pair_held_usd": str(hold.request.pair_ceiling),
                "formal_protection_kind": "diagnostic_claim",
                "allowed_model_jobs": 0,
                "activation_permitted": False,
                "execution_locked": True,
            }
    finally:
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--database-url-file", type=Path, required=True)
    parser.add_argument("--expected-database", required=True)
    args = parser.parse_args()
    try:
        result = asyncio.run(
            execute(
                repo_root=args.repo_root,
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
            "ECONOMIC_HOLD_AUTHORITY_MISMATCH",
            "ECONOMIC_HOLD_DAY_MISMATCH",
            "ECONOMIC_HOLD_CONFLICT",
            "ECONOMIC_HOLD_INVALID",
            "ECONOMIC_PAIR_BUDGET_INSUFFICIENT",
        }
        print(
            json.dumps(
                {
                    "status": "unavailable",
                    "code": code if code in safe else "ECONOMIC_HOLD_INPUT_OR_DATABASE_INVALID",
                }
            )
        )
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
