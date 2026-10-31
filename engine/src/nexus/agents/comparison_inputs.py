"""Typed, deterministic preparation of the approved comparison's frozen inputs.

Only private run/request/account/batch bindings are replaced. Company instruments,
source snapshots, SEC baselines, source availability and research time stay intact.
This prepares inputs; it grants neither a business identity nor dispatch authority.
"""

import copy
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from typing import Any
from uuid import UUID, uuid5

from nexus.agents.committee import INDEPENDENT_MONITOR_PROMPT_VERSIONS, ROLE_ORDER, PromptAsset
from nexus.agents.comparison_manifest import PURPOSE, PURPOSES
from nexus.agents.models import ResearchRequest
from nexus.domain.evidence import EvidenceRef
from nexus.monitoring.independent_context import SOURCE, IndependentMonitorContext, content

PROFILE = "codex-sdk-chatgpt-v11-bounded-stage-budget"
INVALID = "COMPARISON_SOURCE_INVALID"
TOOLS = frozenset(
    {
        "security_snapshot",
        "filing_disclosures",
        "financial_facts",
        "trailing_valuation",
        "price_close",
        "price_features",
        "macro",
    }
)


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def assert_outbound_safe(value: object, forbidden: tuple[str, ...]) -> None:
    """Inspect the entire body, including dictionary keys and JSON inside strings.

    This is deliberately a rejection, not redaction. An unexpected private path
    requires an explicit typed binding change and a new equivalence check.
    """
    if isinstance(value, dict):
        for key, child in value.items():
            assert_outbound_safe(key, forbidden)
            assert_outbound_safe(child, forbidden)
    elif isinstance(value, list | tuple):
        for child in value:
            assert_outbound_safe(child, forbidden)
    elif isinstance(value, str):
        if any(old.lower() in value.lower() for old in forbidden):
            raise ValueError("COMPARISON_PRIVATE_BINDING_REMAINED")
        if value.lstrip().startswith(("{", "[", '"')):
            try:
                decoded = json.loads(value)
            except (ValueError, RecursionError):
                return
            if decoded != value:
                assert_outbound_safe(decoded, forbidden)
    elif isinstance(value, float):
        if not isfinite(value):
            raise ValueError("COMPARISON_OUTBOUND_VALUE_INVALID")
    elif value is not None and not isinstance(value, bool | int):
        raise ValueError("COMPARISON_OUTBOUND_VALUE_INVALID")


def _manifest(payload: dict[str, Any]) -> list[dict[str, Any]]:
    refs = payload["evidence"]
    by_key = {(r["source"], r["object_id"], r["content_hash"]): r for r in refs}
    items = payload["inputs"]["security_snapshot"]["items"]
    keys = [(i["source"], i["provider_object_id"], i["content_hash"]) for i in items]
    if (
        len(by_key) != len(refs)
        or len(set(keys)) != len(keys)
        or set(keys) != set(by_key)
        or len({i["evidence_ref_id"] for i in items}) != len(items)
    ):
        raise ValueError(INVALID)
    result = []
    for item, key in zip(items, keys, strict=True):
        if item["available_at"] != by_key[key]["available_at"]:
            raise ValueError(INVALID)
        UUID(item["evidence_ref_id"])
        result.append(
            {
                "id": item["evidence_ref_id"],
                **{
                    name: item[name]
                    for name in (
                        "source",
                        "provider_object_id",
                        "revision",
                        "available_at",
                        "content_hash",
                    )
                },
                "location": by_key[key]["location"],
            }
        )
    return result


def _validate(payload: dict[str, Any]) -> IndependentMonitorContext:
    try:
        request = ResearchRequest.model_validate(payload["request"])
        if payload["profile"] != PROFILE or set(payload["prompts"]) != set(ROLE_ORDER):
            raise ValueError(INVALID)
        for role, value in payload["prompts"].items():
            prompt = PromptAsset.model_validate(value)
            if (
                prompt.role != role
                or hashlib.sha256(prompt.content.encode()).hexdigest() != prompt.content_hash
            ):
                raise ValueError(INVALID)
        if (
            payload["prompts"]["portfolio"]["version"] not in INDEPENDENT_MONITOR_PROMPT_VERSIONS
            or "## Claim and audit scope" not in payload["prompts"]["portfolio"]["content"]
        ):
            raise ValueError(INVALID)
        security = payload["inputs"]["security_snapshot"]
        context = IndependentMonitorContext.model_validate_json(
            json.dumps(security["independent_monitor_context"])
        )
        if (
            context.instrument_id != request.instrument_id
            or context.decision_time != request.decision_time
            or any(p.plan_id is not None for p in context.predecessors)
            or security["purpose"] != context.purpose
            or security["portfolio_context_status"] != "not_applicable"
            or not set(payload["inputs"]).issubset(TOOLS)
        ):
            raise ValueError(INVALID)
        refs = [EvidenceRef.model_validate(e) for e in payload["evidence"]]
        own = [r for r in refs if r.source == SOURCE]
        digest = hashlib.sha256(content(context)).hexdigest()
        if (
            len(own) != 1
            or own[0].object_id != f"{context.request_id}:{context.instrument_id}"
            or own[0].content_hash != digest
            or own[0].location != f"memory://sha256/{digest}"
            or own[0].available_at != request.decision_time
            or any(r.available_at > request.decision_time for r in refs)
        ):
            raise ValueError(INVALID)
        manifest = _manifest(payload)
        refs_by_id = {row["id"]: row for row in manifest}
        if next(m for m in manifest if m["source"] == SOURCE)["revision"] != digest:
            raise ValueError(INVALID)
        for tool, envelope in payload["inputs"].items():
            if tool == "trailing_valuation" and "data_snapshot_id" not in envelope:
                continue
            if (
                envelope["data_snapshot_id"] != str(request.data_snapshot_id)
                or envelope["instrument_id"] != str(request.instrument_id)
                or envelope["evidence_hash"] != _hash(manifest)
                or datetime.fromisoformat(envelope["as_of"]) != request.decision_time
            ):
                raise ValueError(INVALID)
            for item in envelope["items"]:
                if "instrument_id" in item and item["instrument_id"] != str(request.instrument_id):
                    raise ValueError(INVALID)
                if "evidence_ref_id" in item:
                    ref = refs_by_id[item["evidence_ref_id"]]
                    if "content_hash" in item and item["content_hash"] != ref["content_hash"]:
                        raise ValueError(INVALID)
        return context
    except (KeyError, TypeError, StopIteration, ValueError) as error:
        raise ValueError(INVALID) from error


@dataclass(frozen=True)
class PreparedComparisonInput:
    payload: dict[str, Any]
    proof: dict[str, Any]  # Private provenance; never part of a model request.
    forbidden: tuple[str, ...]

    @property
    def input_hash(self) -> str:
        return _hash(self.payload)


def prepare_comparison_input(
    member: dict[str, Any], *, experiment_id: UUID, purpose: str = PURPOSE
) -> PreparedComparisonInput:
    """Rebuild only explicit private paths; verify both graphs and semantic equality."""
    if purpose not in PURPOSES:
        raise ValueError("COMPARISON_PURPOSE_INVALID")
    source = copy.deepcopy(
        {key: member[key] for key in ("profile", "request", "inputs", "evidence", "prompts")}
    )
    context = _validate(source)
    run_id = str(UUID(member["run_id"]))
    target = copy.deepcopy(source)
    changes: list[dict[str, Any]] = []

    def alias(label: str) -> str:
        return str(uuid5(experiment_id, f"{purpose}:{label}"))

    def replace(path: tuple[str | int, ...], new: object) -> None:
        parent: Any = target
        for part in path[:-1]:
            parent = parent[part]
        old = parent[path[-1]]
        changes.append({"path": list(path), "old": old, "new": new})
        parent[path[-1]] = new

    # Company source_snapshot_id/hash and SEC baseline are source evidence, not
    # the private research wrapper. They retain their original resolvable IDs.
    prefix = ("inputs", "security_snapshot", "independent_monitor_context")
    bindings = {
        "request_id": alias("request"),
        "account_id": alias("account"),
        "candidate_batch_id": alias("batch"),
    }
    bindings["request_hash"] = _hash(
        {
            "purpose": purpose,
            "experiment_id": str(experiment_id),
            "request_id": bindings["request_id"],
        }
    )
    bindings["batch_hash"] = _hash(
        {
            "purpose": purpose,
            "account_id": bindings["account_id"],
            "candidate_batch_id": bindings["candidate_batch_id"],
            "candidate_ids": [str(i) for i in context.candidate_ids],
        }
    )
    for name, value in bindings.items():
        replace((*prefix, name), value)
    rebuilt = IndependentMonitorContext.model_validate_json(
        json.dumps(target["inputs"]["security_snapshot"]["independent_monitor_context"])
    )
    new_hash = hashlib.sha256(content(rebuilt)).hexdigest()
    ref_index = next(i for i, r in enumerate(source["evidence"]) if r["source"] == SOURCE)
    item_index = next(
        i
        for i, r in enumerate(source["inputs"]["security_snapshot"]["items"])
        if r["source"] == SOURCE
    )
    object_id = f"{rebuilt.request_id}:{rebuilt.instrument_id}"
    for name, value in {
        "object_id": object_id,
        "location": f"memory://sha256/{new_hash}",
        "content_hash": new_hash,
    }.items():
        replace(("evidence", ref_index, name), value)
    for name, value in {
        "evidence_ref_id": alias(f"context-ref:{context.instrument_id}"),
        "provider_object_id": object_id,
        "revision": new_hash,
        "content_hash": new_hash,
    }.items():
        replace(("inputs", "security_snapshot", "items", item_index, name), value)
    snapshot_id = alias(f"research-snapshot:{context.instrument_id}")
    replace(("request", "data_snapshot_id"), snapshot_id)
    manifest_hash = _hash(_manifest(target))
    for tool, envelope in source["inputs"].items():
        if "data_snapshot_id" in envelope:
            replace(("inputs", tool, "data_snapshot_id"), snapshot_id)
            replace(("inputs", tool, "evidence_hash"), manifest_hash)
    _validate(target)
    normalized_source, normalized_target = copy.deepcopy(source), copy.deepcopy(target)
    for change in changes:
        for tree, side in ((normalized_source, "old"), (normalized_target, "new")):
            parent = tree
            for part in change["path"][:-1]:
                parent = parent[part]
            key = change["path"][-1]
            if parent[key] != change[side]:
                raise ValueError("COMPARISON_EQUIVALENCE_FAILED")
            parent[key] = {"comparison_binding_path": change["path"]}
    if normalized_source != normalized_target:
        raise ValueError("COMPARISON_EQUIVALENCE_FAILED")
    forbidden = tuple(sorted({run_id, *(str(c["old"]) for c in changes)}))
    assert_outbound_safe(target, forbidden)
    return PreparedComparisonInput(
        target,
        {
            "purpose": purpose,
            "experiment_id": str(experiment_id),
            "source_manifest_hash": _hash(source),
            "input_hash": _hash(target),
            "source_evidence_manifest_hash": _hash(_manifest(source)),
            "rebuilt_evidence_manifest_hash": manifest_hash,
            "private_binding_map": changes,
            "normalized_semantic_hash": _hash(normalized_source),
            "semantic_difference": [],
            "company_evidence_unchanged": True,
        },
        forbidden,
    )
