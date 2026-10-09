"""Zero-call budget feasibility from explicit claims, never live activation or reservation."""

import argparse
import json
from pathlib import Path

from nexus.paper.economic_budget import BudgetRequest, calculate_budget


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = calculate_budget(BudgetRequest.model_validate_json(args.input.read_bytes()))
    except (OSError, ValueError):
        print(json.dumps({"status": "unavailable", "code": "ECONOMIC_BUDGET_CLAIMS_INVALID"}))
        return 1
    print(result.model_dump_json())
    return 0 if result.status == "both_fit_arithmetic" else 1


if __name__ == "__main__":
    raise SystemExit(main())
