"""Explicit bounded formal profile; frozen earlier profiles remain unchanged."""

import hashlib
import json

from nexus.agents.codex_capacity import capacity_config
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION


def bounded_stage_config() -> dict[str, str | int | bool]:
    config = capacity_config()
    budget = json.loads(str(config["stage_time_budget"]))
    budget.update(
        job_absolute_seconds=300,
        handler_absolute_seconds=315,
        lease_seconds=330,
        preclaim_absolute_seconds=195,
        handler_clock_origin="claim_after_preflight",
        stage_caps_seconds={"research": None, "critique": None, "synthesis": None},
        stage_allocation="parent_remaining_after_close_and_later_reserves",
    )
    config.update(
        version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
        stage_time_budget=json.dumps(budget, sort_keys=True),
    )
    return config


def bounded_stage_config_hash() -> str:
    return hashlib.sha256(json.dumps(bounded_stage_config(), sort_keys=True).encode()).hexdigest()
