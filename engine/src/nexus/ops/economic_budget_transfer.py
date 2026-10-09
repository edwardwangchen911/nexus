"""Transfer a diagnostic hold to an existing prepared research/synthesis run; no provider or Job."""

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.agents.persistence import (
    _lock_budget_day,
    _shared_daily_budget_used,
    reserve_daily_budget,
)
from nexus.agents.portfolio_synthesis_persistence import reserve_portfolio_synthesis_budget
from nexus.ops.openai_local import local_settings
from nexus.ops.research_budget_diagnostic import read_database_url
from nexus.paper.economic_budget_hold import (
    AllocationRequest,
    StrictModel,
    decode_hold,
    require_protocol_budget_day,
)
from nexus.paper.economic_receiver import require_economic_receiver
from nexus.paper.economic_version_binding import read_binding
from nexus.paper.portfolio_economic_protocol import read_protocol
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import AuditEvent


class TransferInput(StrictModel):
    allocation: AllocationRequest
    run_id: UUID


async def execute(
    *,
    repo_root: Path,
    input_path: Path,
    database_url_file: Path,
    expected_database: str,
) -> dict[str, object]:
    request = TransferInput.model_validate_json(input_path.read_bytes())
    limit = local_settings(repo_root.resolve(), backend="codex_sdk").agent_daily_budget_usd
    url = read_database_url(database_url_file)
    if make_url(url).database != expected_database:
        raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            actual = await session.scalar(text("SELECT current_database()"))
            if actual != expected_database:
                raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("ECONOMIC_TRANSFER_RUN_INVALID")
            source = await session.get(AuditEvent, request.allocation.hold_event_id)
            if source is None:
                raise ValueError("ECONOMIC_TRANSFER_RUN_INVALID")
            hold = decode_hold(source)
            _, _, protocol = await read_protocol(session, as_of=now)
            if protocol is None:
                raise ValueError("ECONOMIC_TRANSFER_SLOT_AUTHORITY_INVALID")
            require_protocol_budget_day(protocol, hold.request, observed_at=now)
            _, binding = await read_binding(session, as_of=now)
            synthesis = request.allocation.action == "synthesis"
            run: AgentResearchRunRecord | AgentPortfolioSynthesisRunRecord | None = (
                await session.get(AgentPortfolioSynthesisRunRecord, request.run_id)
                if synthesis
                else await session.get(AgentResearchRunRecord, request.run_id)
            )
            expected_cost = (
                hold.request.synthesis_cost_usd if synthesis else hold.request.research_cost_usd
            )
            model = (
                None
                if run is None
                else await session.get(AgentModelConfigRecord, run.model_config_id)
            )
            if (
                binding is None
                or run is None
                or model is None
                or binding.protocol_hash != hold.request.protocol_hash
                or binding.shared_model_id != model.id
                or binding.shared_model_hash != model.config_hash
                or model.worst_case_cost_usd != expected_cost
                or not model.pricing_available_at <= now < model.pricing_expires_at
            ):
                raise ValueError("ECONOMIC_TRANSFER_RUN_INVALID")
            require_economic_receiver(protocol, binding, request.allocation, run)
            await _lock_budget_day(session, now.date().isoformat())
            before = await _shared_daily_budget_used(session, business_date=now.date())
            reserve = reserve_portfolio_synthesis_budget if synthesis else reserve_daily_budget
            await reserve(
                session,
                run_id=run.id,
                now=now,
                daily_limit_usd=limit,
                worst_case_usd=model.worst_case_cost_usd,
                pricing_version=model.version,
                pricing_fresh=True,
                economic_allocation=request.allocation,
            )
            after = await _shared_daily_budget_used(session, business_date=now.date())
            return {
                "status": (
                    "diagnostic_synthesis_budget_transferred"
                    if synthesis
                    else "diagnostic_research_budget_transferred"
                ),
                "budget_day_utc": now.date().isoformat(),
                "shared_before_usd": str(before),
                "shared_after_usd": str(after),
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
            "ECONOMIC_TRANSFER_DAY_MISMATCH",
            "ECONOMIC_TRANSFER_SLOT_DAY_MISMATCH",
            "ECONOMIC_TRANSFER_SLOT_AUTHORITY_INVALID",
            "ECONOMIC_TRANSFER_AMOUNT_MISMATCH",
            "ECONOMIC_TRANSFER_RUN_INVALID",
            "ECONOMIC_TRANSFER_RECEIVER_INVALID",
            "ECONOMIC_TRANSFER_CONFLICT",
            "ECONOMIC_TRANSFER_INVALID",
            "ECONOMIC_PAIR_BUDGET_INSUFFICIENT",
            "ECONOMIC_HOLD_INVALID",
        }
        print(
            json.dumps(
                {
                    "status": "unavailable",
                    "code": code if code in safe else "ECONOMIC_TRANSFER_INPUT_OR_DATABASE_INVALID",
                }
            )
        )
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
