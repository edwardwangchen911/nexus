"""Pending formal stage-capacity identity, separate from frozen candidate and v6."""

import hashlib
import json
from datetime import datetime

from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.persistence.models.agents import AgentModelConfigRecord


def formal_stage_config() -> dict[str, str | int]:
    from nexus.agents.codex_sdk import MODEL_ID

    return {
        "model_id": MODEL_ID,
        "version": FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
        "max_input_tokens": 96000,
        "max_output_tokens": 8192,
        "max_tool_calls": 0,
        "input_usd_per_million": "0",
        "output_usd_per_million": "0",
        "pricing_available_at": "2026-09-02T00:00:00+00:00",
        "pricing_expires_at": "2027-09-02T00:00:00+00:00",
        "billing_mode": "chatgpt_subscription",
        "max_sdk_turns": 3,
        "max_wire_bytes": 64000,
        "inline_citations": True,
        "stage_time_budget": json.dumps(
            {
                "job_absolute_seconds": 180,
                "handler_absolute_seconds": 195,
                "lease_seconds": 210,
                "terminal_reserve_seconds": 5,
                "sdk_close_reserve_seconds": 7,
                "stage_caps_seconds": {"research": 60, "critique": 45, "synthesis": 45},
                "later_phase_reserve_seconds": {"research": 104, "critique": 52, "synthesis": 0},
            },
            sort_keys=True,
        ),
    }


def formal_stage_config_hash() -> str:
    return hashlib.sha256(json.dumps(formal_stage_config(), sort_keys=True).encode()).hexdigest()


def formal_stage_model_matches(model: AgentModelConfigRecord) -> bool:
    config = formal_stage_config()
    return (
        model.version == config["version"]
        and model.config_hash == formal_stage_config_hash()
        and model.model_id == config["model_id"]
        and model.max_input_tokens == config["max_input_tokens"]
        and model.max_output_tokens == config["max_output_tokens"]
        and model.max_tool_calls == 0
        and model.input_usd_per_million == 0
        and model.output_usd_per_million == 0
        and model.pricing_available_at
        == datetime.fromisoformat(str(config["pricing_available_at"]))
        and model.pricing_expires_at == datetime.fromisoformat(str(config["pricing_expires_at"]))
    )
