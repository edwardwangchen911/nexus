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

from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_usage import PLATFORM_PRICE_VERSIONS, price_platform_usage
from nexus.agents.platform_wire import PlatformStageWire
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord


@dataclass(frozen=True)
class PlatformStageAccounting:
    input_tokens: int
    output_tokens: int
    known_cost_usd: Decimal
    exact_cost_usd: Decimal | None
    limit_exceeded: bool


def platform_stage_accounting(
    rows: Sequence[Mapping[str, Any]],
    *,
    price_version: str | None = None,
    price_record: object = None,
) -> PlatformStageAccounting:
    picodollars, inp, out = 0, 0, 0
    priced_stages = 0
    complete, exceeded = bool(rows), False
    allowed_versions = set(PLATFORM_PRICE_VERSIONS)
    binding_version, price_hash = 1, None
    if price_record is not None:
        binding_version = 2
        try:
            pricing = sol_price_record(price_record)
            allowed_versions = {pricing.version}
            price_hash = pricing.content_hash
        except ValueError:
            allowed_versions = set()
    versions = {
        r["platform_binding"].get("price_version")
        for r in rows
        if isinstance(r.get("platform_binding"), dict)
        and isinstance(r["platform_binding"].get("price_version"), str)
    }
    complete &= len(versions) == 1 and versions.issubset(allowed_versions)
    if price_version is not None:
        complete &= versions == {price_version}
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
            or binding.get("version") != binding_version
            or binding.get("price_version") not in allowed_versions
            or binding_version == 2
            and binding.get("price_record_hash") != price_hash
            or price_version is not None
            and binding.get("price_version") != price_version
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
            price_record=price_record,
        )
        if (
            not usage.totals_verified
            or (usage.input_tokens, usage.output_tokens) != counts
            or usage.exact_cost_usd is None
        ):
            complete = False
            continue
        numerator, denominator = usage.exact_cost_usd.as_integer_ratio()
        picodollars += numerator * 1_000_000_000_000 // denominator
        priced_stages += 1
    exceeded |= out > 8192
    # Preserve the legacy textual scale used by frozen receipts, including the
    # distinction between no priced evidence (0) and a measured zero (0E-7).
    cost = Decimal(0)
    if priced_stages:
        cost = (
            Decimal(f"{picodollars // 100000}e-7")
            if price_record is None
            else Decimal(f"{picodollars}e-12")
        )
    return PlatformStageAccounting(inp, out, cost, cost if complete else None, exceeded)


def is_platform_stage_model(model: AgentModelConfigRecord) -> bool:
    return (
        model.version in PLATFORM_PRICE_VERSIONS
        or getattr(model, "platform_profile", None) is not None
    )


def platform_accounting_for_model(
    model: AgentModelConfigRecord, rows: Sequence[Mapping[str, Any]]
) -> PlatformStageAccounting:
    """Use only this immutable model's original price, including historical reads."""
    from nexus.agents.platform_profile import platform_model_matches, sol_business_model_matches

    if sol_business_model_matches(model):
        assert model.platform_profile is not None
        record = model.platform_profile["price_record"]
        return platform_stage_accounting(
            rows, price_version=sol_price_record(record).version, price_record=record
        )
    if not platform_model_matches(model):
        raise ValueError("PLATFORM_USAGE_ACCOUNTING_UNCONFIRMED")
    return platform_stage_accounting(rows, price_version=model.version)


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
    from nexus.agents.comparison_manifest import parse_comparison_manifest
    from nexus.agents.platform_manifest import PlatformManifest
    from nexus.agents.platform_profile import platform_profile_hash, sol_business_model_matches
    from nexus.agents.platform_wire import PlatformStageWire, _hash

    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    native = model is not None and sol_business_model_matches(model)
    price_record = None
    native_binding = None
    if not isinstance(compiled, PlatformStageWire) or not 1 <= stage <= 3:
        raise ValueError("PLATFORM_AUTHORIZATION_BINDING_INVALID")
    compiled.verify_wire(wire)
    if native:
        from nexus.agents.platform_native import read_native_allocation

        native_binding, allocation = await read_native_allocation(session, run=run)
        assert model is not None and model.platform_profile is not None
        price_record = model.platform_profile["price_record"]
        version = sol_price_record(price_record).version
        input_hash = allocation.input_hash
    else:
        if (
            run.pricing_version not in PLATFORM_PRICE_VERSIONS
            or run.model_hash != platform_profile_hash(run.pricing_version)
        ):
            raise ValueError("PLATFORM_AUTHORIZATION_BINDING_INVALID")
        record = (
            (
                await session.execute(
                    text(
                        "SELECT m.payload,c.ordinal FROM research_comparison_chain c "
                        "JOIN research_comparison_manifest m ON m.id=c.experiment_id "
                        "WHERE c.run_id=:run"
                    ),
                    {"run": run.id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if record is None:
            raise ValueError("PLATFORM_AUTHORIZATION_BINDING_INVALID")
        manifest = parse_comparison_manifest(record["payload"])
        if (
            not isinstance(manifest, PlatformManifest)
            or manifest.price_version != run.pricing_version
        ):
            raise ValueError("PLATFORM_AUTHORIZATION_BINDING_INVALID")
        allocation = manifest.allocations[record["ordinal"] - 1]
        input_hash, version = run.input_hash, run.pricing_version
    # Pydantic retains the typed frozen dataclass allocation.
    if (
        allocation.input_hash != input_hash
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
    accounting = platform_stage_accounting(prior, price_version=version, price_record=price_record)
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
    # Allocation stores the maximal schema envelope. The actual stage narrows
    # prior claim enums; its exact schema is already frozen in body_sha256.
    if (
        body.get("model") != allocation.profile.model
        or body.get("service_tier") != "default"
        or body.get("prompt_cache_options") != {"mode": "implicit", "ttl": "30m"}
        or body.get("reasoning") != {"effort": allocation.profile.effort}
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
    binding = dict(
        version=2 if native else 1,
        price_version=version,
        input_hash=input_hash,
        allocation_hash=compiled.allocation_hash,
        binding_hash=compiled.binding_hash,
        body_sha256=compiled.body_sha256,
        input_upper_bound=compiled.input_upper_bound,
        cache_mode="implicit",
        cache_ttl="30m",
    )

    if native_binding is not None:
        binding.update(
            price_record_hash=native_binding["price_record_hash"],
            model_config_id=native_binding["model_config_id"],
            model_hash=native_binding["model_hash"],
            credential_binding_hash=native_binding["credential_binding_hash"],
        )
    return binding


def require_platform_stage_cost(
    model: AgentModelConfigRecord,
    rows: Sequence[Mapping[str, Any]],
    input_tokens: int,
    output_tokens: int,
) -> Decimal:
    from nexus.agents.platform_profile import platform_model_matches, sol_business_model_matches

    normal_sol = sol_business_model_matches(model)
    record = None
    if normal_sol:
        assert model.platform_profile is not None
        record = model.platform_profile["price_record"]
    version = sol_price_record(record).version if normal_sol else model.version
    result = platform_stage_accounting(rows, price_version=version, price_record=record)
    if (
        not (normal_sol or platform_model_matches(model))
        or result.exact_cost_usd is None
        or result.limit_exceeded
        or input_tokens > model.max_input_tokens
        or output_tokens > model.max_output_tokens
        or (input_tokens, output_tokens) != (result.input_tokens, result.output_tokens)
    ):
        raise ValueError("PLATFORM_USAGE_ACCOUNTING_UNCONFIRMED")
    return result.exact_cost_usd
