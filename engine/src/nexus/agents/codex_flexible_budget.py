"""Explicit local qualification identity; never changes a frozen v9 profile."""

import hashlib
import json

from nexus.agents.codex_capacity import capacity_config
from nexus.agents.codex_versions import FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION


def flexible_stage_config() -> dict[str, str | int | bool]:
    config = capacity_config()
    budget = json.loads(str(config["stage_time_budget"]))
    budget["stage_caps_seconds"]["critique"] = None
    budget["critique_allocation"] = "parent_remaining_after_close_and_later_reserves"
    config.update(
        version=FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
        stage_time_budget=json.dumps(budget, sort_keys=True),
    )
    return config


def flexible_stage_config_hash() -> str:
    return hashlib.sha256(json.dumps(flexible_stage_config(), sort_keys=True).encode()).hexdigest()
