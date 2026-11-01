"""Deterministic shared preparation for the CLI and native comparison producer."""

import hashlib
import json
from typing import Any
from uuid import UUID

from nexus.agents.codex_sdk import codex_output_schema, codex_request_wire
from nexus.agents.comparison_compiler import compile_comparison_stage, validate_comparison_capacity
from nexus.agents.comparison_inputs import (
    TOOLS,
    PreparedComparisonInput,
    assert_outbound_safe,
    prepare_comparison_input,
)
from nexus.agents.comparison_manifest import PLATFORM_PURPOSES, PURPOSE, PURPOSES
from nexus.agents.models import ResearchRequest
from nexus.agents.platform_usage import PLATFORM_PRICE_VERSION
from nexus.jobs.models import job_input_hash

SYMBOLS = ("AAPL", "AVGO", "NVDA")


def prepare_archive(
    source: bytes,
    *,
    expected_source_hash: str,
    experiment_id: UUID,
    purpose: str = PURPOSE,
    platform_price_version: str = PLATFORM_PRICE_VERSION,
) -> dict[str, Any]:
    artifact = reconstruct_archive(
        source,
        expected_source_hash=expected_source_hash,
        experiment_id=experiment_id,
        purpose=purpose,
    )
    if purpose in PLATFORM_PURPOSES:
        from dataclasses import asdict

        from nexus.agents.platform_wire import PlatformProfile, freeze_platform_allocation

        artifact["allocations"] = [
            json.loads(
                json.dumps(
                    asdict(
                        freeze_platform_allocation(
                            PreparedComparisonInput(
                                m["payload"], m["proof"], tuple(m["forbidden"])
                            ),
                            profile=PlatformProfile(price_version=platform_price_version),
                        )
                    )
                )
            )
            for m in artifact["members"]
        ]
        return artifact
    for member in artifact["members"]:
        validate_comparison_capacity(
            PreparedComparisonInput(member["payload"], member["proof"], tuple(member["forbidden"]))
        )
    return artifact


def reconstruct_archive(
    source: bytes, *, expected_source_hash: str, experiment_id: UUID, purpose: str = PURPOSE
) -> dict[str, Any]:
    """Read/reconstruct frozen identity, including rejected archives; never admit or dispatch."""
    if purpose not in PURPOSES:
        raise ValueError("COMPARISON_PURPOSE_INVALID")
    if len(source) > 8 * 1024 * 1024 or hashlib.sha256(source).hexdigest() != expected_source_hash:
        raise ValueError("COMPARISON_SOURCE_HASH_INVALID")
    original = json.loads(source)
    members = original["members"]
    if len(members) != 3:
        raise ValueError("COMPARISON_POOL_INVALID")
    ordered: dict[str, Any] = {}
    candidates = {row["request"]["instrument_id"] for row in members}
    bindings = set()
    for member in members:
        request = ResearchRequest.model_validate(member["request"])
        context = member["inputs"]["security_snapshot"]["independent_monitor_context"]
        symbols = {
            item["symbol"]
            for tool in ("price_close", "price_features")
            for item in member["inputs"][tool]["items"]
        }
        identity_symbols = {
            item["research_observation_identity_binding"]["symbol"]
            for item in member["inputs"]["security_snapshot"]["items"]
            if "research_observation_identity_binding" in item
        }
        identity_instruments = {
            item["research_observation_identity_binding"]["instrument_id"]
            for item in member["inputs"]["security_snapshot"]["items"]
            if "research_observation_identity_binding" in item
        }
        if (
            len(candidates) != 3
            or set(context["candidate_ids"]) != candidates
            or request.decision_time.date().isoformat() != "2026-10-27"
            or set(member["inputs"]) != TOOLS
            or len(symbols) != 1
            or symbols != identity_symbols
            or identity_instruments != {str(request.instrument_id)}
            or not symbols.issubset(SYMBOLS)
        ):
            raise ValueError("COMPARISON_POOL_INVALID")
        symbol = next(iter(symbols))
        if symbol in ordered:
            raise ValueError("COMPARISON_POOL_INVALID")
        bindings.add(
            tuple(
                context[k]
                for k in (
                    "account_id",
                    "request_id",
                    "request_hash",
                    "candidate_batch_id",
                    "batch_hash",
                )
            )
            + (tuple(context["candidate_ids"]),)
        )
        prepared = prepare_comparison_input(member, experiment_id=experiment_id, purpose=purpose)
        again = prepare_comparison_input(member, experiment_id=experiment_id, purpose=purpose)
        if prepared != again:
            raise ValueError("COMPARISON_RECONSTRUCTION_UNSTABLE")
        # Both routes receive this exact compiler result. No provider or SDK is called.
        payload, schema = compile_comparison_stage(
            prepared, expected_input_hash=prepared.input_hash, stage="research", previous=[]
        )
        wire = codex_request_wire(payload)
        assert_outbound_safe(wire, prepared.forbidden)
        if purpose == PURPOSE and len(wire.encode()) > 100479:
            raise ValueError("COMPARISON_INPUT_CAPACITY_EXCEEDED")
        ordered[symbol] = {
            "symbol": symbol,
            "input_hash": prepared.input_hash,
            "payload": prepared.payload,
            "proof": prepared.proof,
            "forbidden": prepared.forbidden,
            "first_stage_wire_hash": hashlib.sha256(wire.encode()).hexdigest(),
            "first_stage_schema_hash": job_input_hash(codex_output_schema(schema)),
            "first_stage_wire_bytes": len(wire.encode()),
        }
    if set(ordered) != set(SYMBOLS) or len(bindings) != 1:
        raise ValueError("COMPARISON_POOL_INVALID")
    rows = [ordered[symbol] for symbol in SYMBOLS]
    return {
        "schema_version": "research-comparison-inputs-v1",
        "purpose": purpose,
        "experiment_id": str(experiment_id),
        "source_manifest_hash": expected_source_hash,
        "members": rows,
        "input_hashes": [r["input_hash"] for r in rows],
        "instrument_ids": [r["payload"]["request"]["instrument_id"] for r in rows],
        "identity_equivalence_hash": job_input_hash({"proofs": [r["proof"] for r in rows]}),
        "outbound_safety_hash": job_input_hash(
            {
                "wires": [r["first_stage_wire_hash"] for r in rows],
                "schemas": [r["first_stage_schema_hash"] for r in rows],
            }
        ),
        "dispatch_enabled": False,
        "model_calls": 0,
        "database_writes": 0,
    }
