"""One opt-in accounting projection of immutable native Platform stage facts.

Raw provider data never grants permission. Verification, frozen membership and
native ownership are established by the stage protocol. Missing classifications
retain the full reservation; the scalar reservation price is never an actual fee.
"""

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.platform_usage import PLATFORM_PRICE_VERSION, price_platform_usage
from nexus.agents.platform_wire import PlatformStageWire
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord


@dataclass(frozen=True)
class PlatformStageAccounting:
    input_tokens: int
    output_tokens: int
    known_cost_usd: Decimal
    exact_cost_usd: Decimal | None
    limit_exceeded: bool


def platform_stage_accounting(rows: Sequence[Mapping[str, Any]]) -> PlatformStageAccounting:
    cost, inp, out = Decimal(0), 0, 0
    complete, exceeded = bool(rows), False
    for index, row in enumerate(rows, 1):
        binding = row.get("platform_binding")
        trusted = row.get("verified") is True and row.get("eligible") is True
        counts = (row.get("input_tokens"), row.get("output_tokens"))
        if trusted and all(type(n) is int and n >= 0 for n in counts):
            assert isinstance(counts[0], int) and isinstance(counts[1], int)
            inp += counts[0]
            out += counts[1]
            if isinstance(binding, dict) and type(binding.get("input_upper_bound")) is int:
                exceeded |= counts[0] > binding["input_upper_bound"]
        else:
            complete = False
            continue
        raw = row.get("platform_response")
        if (
            row.get("stage") != index
            or not isinstance(binding, dict)
            or binding.get("version") != 1
            or binding.get("price_version") != PLATFORM_PRICE_VERSION
            or binding.get("body_sha256") != row.get("wire_hash")
            or type(binding.get("input_upper_bound")) is not int
            or not 8192 < binding["input_upper_bound"] <= 272000
            or not isinstance(raw, str)
            or hashlib.sha256(raw.encode()).hexdigest() != row.get("response_hash")
        ):
            complete = False
            continue
        try:
            response = json.loads(raw)
        except (ValueError, TypeError):
            complete = False
            continue
        if not isinstance(response, dict) or response.get("status") not in {
            "completed",
            "failed",
            "incomplete",
        }:
            complete = False
            continue
        usage = price_platform_usage(
            response.get("usage"),
            model=response.get("model"),
            service_tier=response.get("service_tier"),
            cache_mode=binding.get("cache_mode"),
            cache_ttl=binding.get("cache_ttl"),
        )
        if (
            not usage.totals_verified
            or (usage.input_tokens, usage.output_tokens) != counts
            or usage.exact_cost_usd is None
        ):
            complete = False
            continue
        cost += usage.exact_cost_usd
    exceeded |= out > 8192
    return PlatformStageAccounting(inp, out, cost, cost if complete else None, exceeded)


async def platform_authorization_binding(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    stage: int,
    wire: bytes,
    compiled: PlatformStageWire | None,
) -> dict[str, Any]:
    """Bind the compiler output to this exact registered allocation, not a caller profile."""
    from sqlalchemy import text

    from nexus.agents.codex_sdk import BASE_INSTRUCTIONS, DEVELOPER_INSTRUCTIONS
    from nexus.agents.comparison_manifest import PLATFORM_PURPOSE, parse_comparison_manifest
    from nexus.agents.platform_manifest import PlatformManifest
    from nexus.agents.platform_profile import platform_profile_hash
    from nexus.agents.platform_wire import PlatformStageWire, _hash

    if not isinstance(compiled, PlatformStageWire) or run.model_hash != platform_profile_hash():
        raise ValueError("PLATFORM_AUTHORIZATION_BINDING_INVALID")
    compiled.verify_wire(wire)
    record = (
        (
            await session.execute(
                text(
                    "SELECT m.payload,c.ordinal FROM research_comparison_chain c "
                    "JOIN research_comparison_manifest m "
                    "ON m.id=c.experiment_id WHERE c.run_id=:run AND m.purpose=:purpose"
                ),
                {"run": run.id, "purpose": PLATFORM_PURPOSE},
            )
        )
        .mappings()
        .one_or_none()
    )
    if record is None:
        raise ValueError("PLATFORM_AUTHORIZATION_BINDING_INVALID")
    manifest = parse_comparison_manifest(record["payload"])
    if not isinstance(manifest, PlatformManifest) or not 1 <= stage <= 3:
        raise ValueError("PLATFORM_AUTHORIZATION_BINDING_INVALID")
    allocation = manifest.allocations[record["ordinal"] - 1]
    # Pydantic retains the typed frozen dataclass allocation.
    if (
        allocation.input_hash != run.input_hash
        or compiled.allocation_hash != allocation.allocation_hash
        or compiled.stage != allocation.stage_order[stage - 1]
        or compiled.input_upper_bound != allocation.stage_input_upper_bounds[stage - 1]
        or compiled.input_units != len(wire) + allocation.framing_units
        or compiled.endpoint != allocation.profile.endpoint
        or compiled.credential_kind != allocation.profile.credential_kind
        or compiled.binding_hash
        != _hash(
            dict(
                allocation_hash=allocation.allocation_hash,
                stage=compiled.stage,
                body_sha256=compiled.body_sha256,
            )
        )
    ):
        raise ValueError("PLATFORM_AUTHORIZATION_BINDING_INVALID")
    from nexus.agents.research_stages.protocol import stage_evidence

    prior = await stage_evidence(session, run.id)
    accounting = platform_stage_accounting(prior)
    if len(prior) != stage - 1 or (
        stage > 1
        and (
            accounting.exact_cost_usd is None
            or accounting.limit_exceeded
            or any(
                not r["valid"]
                or r["confirmed"] is not True
                or json.loads(r["platform_response"]).get("status") != "completed"
                for r in prior
            )
        )
    ):
        raise ValueError("PLATFORM_PRIOR_USAGE_INVALID")
    body = json.loads(wire)
    if (
        body.get("model") != allocation.profile.model
        or body.get("service_tier") != "default"
        or body.get("prompt_cache_options") != {"mode": "implicit", "ttl": "30m"}
        or body.get("reasoning") != {"effort": "low"}
        or body.get("tools") != []
        or body.get("store") is not False
        or body.get("stream") is not True
        or body.get("instructions") != BASE_INSTRUCTIONS
        or body.get("input", [None])[0] != {"role": "developer", "content": DEVELOPER_INSTRUCTIONS}
        or type(body.get("max_output_tokens")) is not int
        or not 16 <= body["max_output_tokens"] <= 8192
        or body["max_output_tokens"] != 8192 - accounting.output_tokens
    ):
        raise ValueError("PLATFORM_AUTHORIZATION_BINDING_INVALID")
    return dict(
        version=1,
        price_version=PLATFORM_PRICE_VERSION,
        input_hash=run.input_hash,
        allocation_hash=compiled.allocation_hash,
        binding_hash=compiled.binding_hash,
        body_sha256=compiled.body_sha256,
        input_upper_bound=compiled.input_upper_bound,
        cache_mode="implicit",
        cache_ttl="30m",
    )


def require_platform_stage_cost(
    model: AgentModelConfigRecord,
    rows: Sequence[Mapping[str, Any]],
    input_tokens: int,
    output_tokens: int,
) -> Decimal:
    from nexus.agents.platform_profile import platform_model_matches

    result = platform_stage_accounting(rows)
    if (
        not platform_model_matches(model)
        or result.exact_cost_usd is None
        or result.limit_exceeded
        or input_tokens > model.max_input_tokens
        or output_tokens > model.max_output_tokens
        or (input_tokens, output_tokens) != (result.input_tokens, result.output_tokens)
    ):
        raise ValueError("PLATFORM_USAGE_ACCOUNTING_UNCONFIRMED")
    return result.exact_cost_usd
