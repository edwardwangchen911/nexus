"""Normal offline preparation entry; no registration, reservation or dispatch yet."""

import argparse
import hashlib
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from nexus.agents.codex_sdk import codex_output_schema, codex_request_wire
from nexus.agents.comparison_compiler import compile_comparison_stage
from nexus.agents.comparison_inputs import TOOLS, assert_outbound_safe, prepare_comparison_input
from nexus.agents.comparison_manifest import PURPOSE
from nexus.agents.models import ResearchRequest
from nexus.jobs.models import job_input_hash

SYMBOLS = ("AAPL", "AVGO", "NVDA")


def prepare_archive(
    source: bytes, *, expected_source_hash: str, experiment_id: UUID
) -> dict[str, Any]:
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
        prepared = prepare_comparison_input(member, experiment_id=experiment_id)
        again = prepare_comparison_input(member, experiment_id=experiment_id)
        if prepared != again:
            raise ValueError("COMPARISON_RECONSTRUCTION_UNSTABLE")
        # Both routes receive this exact compiler result. No provider or SDK is called.
        payload, schema = compile_comparison_stage(
            prepared, expected_input_hash=prepared.input_hash, stage="research", previous=[]
        )
        wire = codex_request_wire(payload)
        assert_outbound_safe(wire, prepared.forbidden)
        if len(wire.encode()) > 100479:
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
        "purpose": PURPOSE,
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


def prepare_file(
    *, source: Path, expected_source_hash: str, experiment_id: UUID, output: Path
) -> dict[str, Any]:
    with source.open("rb") as stream:
        raw = stream.read(8 * 1024 * 1024 + 1)
    artifact = prepare_archive(
        raw, expected_source_hash=expected_source_hash, experiment_id=experiment_id
    )
    artifact["rebuilt_at"] = datetime.now(UTC).isoformat()
    wire = json.dumps(artifact, sort_keys=True, allow_nan=False).encode()
    # Never overwrite an earlier preparation or follow an existing output symlink.
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(wire)
        stream.flush()
        os.fsync(stream.fileno())
    return {
        "status": "prepared_only",
        "artifact_hash": hashlib.sha256(wire).hexdigest(),
        "members": len(artifact["members"]),
        "dispatch_enabled": False,
        "model_calls": 0,
        "database_writes": 0,
        "source_unchanged": hashlib.sha256(source.read_bytes()).hexdigest() == expected_source_hash,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    prepare = subcommands.add_parser("prepare")
    prepare.add_argument("--source", type=Path, required=True)
    prepare.add_argument("--source-sha256", required=True)
    prepare.add_argument("--experiment-id", type=UUID, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = prepare_file(
            source=args.source,
            expected_source_hash=args.source_sha256,
            experiment_id=args.experiment_id,
            output=args.output,
        )
    except (OSError, ValueError, KeyError, TypeError) as error:
        code = str(error)
        if not re.fullmatch(r"COMPARISON_[A-Z_]+", code):
            code = "COMPARISON_PREPARATION_FAILED"
        print(json.dumps({"status": "rejected", "reason": code}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
