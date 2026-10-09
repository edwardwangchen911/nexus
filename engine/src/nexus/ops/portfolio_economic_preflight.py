"""Normal zero-call CLI: a private planning receipt, never business registration."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from nexus.paper.portfolio_economic_preflight import EconomicPlanRequest, compile_plan, save_plan


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        request = EconomicPlanRequest.model_validate_json(args.input.read_bytes())
        plan = compile_plan(request, now=datetime.now(UTC))
        receipt = save_plan(args.output, plan)
    except (OSError, ValidationError, ValueError) as error:
        code = str(error)
        print(
            json.dumps(
                {
                    "status": "unavailable",
                    "code": code
                    if code.startswith("ECONOMIC_PLAN_") and "\n" not in code
                    else "ECONOMIC_PLAN_INPUT_OR_RECEIPT_INVALID",
                }
            )
        )
        return 1
    print(
        json.dumps(
            {
                "status": plan.status,
                "receipt": receipt,
                "content_hash": plan.content_hash,
                "planned_slots": len(plan.slots),
                "daily_model_jobs": plan.daily_model_jobs,
                "daily_sdk_calls": plan.daily_sdk_calls,
                "activation_blockers": plan.activation_blockers,
                "execution_locked": True,
            }
        )
    )
    return 0
