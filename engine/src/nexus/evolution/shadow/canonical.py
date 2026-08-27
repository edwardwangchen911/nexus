"""Canonical SHA-256 identities for Strategy Shadow facts."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel

from nexus.audit.canonical import canonical_json


def content_hash(value: BaseModel | Mapping[str, object]) -> str:
    payload = value.model_dump(mode="json") if isinstance(value, BaseModel) else dict(value)
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def model_content_hash(value: BaseModel, *, exclude: set[str]) -> str:
    return hashlib.sha256(
        canonical_json(value.model_dump(mode="json", exclude=exclude)).encode()
    ).hexdigest()


def initial_state_content_hash(
    *,
    baseline_snapshot_id: UUID,
    baseline_snapshot_hash: str,
    position_projection_hashes: Sequence[str],
    normalized_state: BaseModel,
) -> str:
    return content_hash(
        {
            "schema_version": "strategy_shadow_initial_state_v1",
            "baseline_snapshot_id": str(baseline_snapshot_id),
            "baseline_snapshot_hash": baseline_snapshot_hash,
            "position_projection_hashes": sorted(position_projection_hashes),
            "normalized_state": normalized_state.model_dump(mode="json"),
        }
    )


def group_authority_content_hash(
    *,
    experiment_id: UUID,
    family_id: UUID,
    historical_decision_id: UUID,
    champion_definition_id: UUID,
    challenger_definition_id: UUID,
    baseline_snapshot_id: UUID,
    calendar_authority_id: UUID,
    source_policy_id: UUID,
    cost_policy_id: UUID,
    champion_generation: int,
    initial_nav: Decimal,
    initial_state_hash: str,
) -> str:
    return content_hash(
        {
            "schema_version": "strategy_shadow_group_authority_v1",
            "experiment_id": str(experiment_id),
            "family_id": str(family_id),
            "historical_decision_id": str(historical_decision_id),
            "champion_definition_id": str(champion_definition_id),
            "challenger_definition_id": str(challenger_definition_id),
            "baseline_snapshot_id": str(baseline_snapshot_id),
            "calendar_authority_id": str(calendar_authority_id),
            "source_policy_id": str(source_policy_id),
            "cost_policy_id": str(cost_policy_id),
            "champion_generation": champion_generation,
            "evaluation_sessions": 63,
            "grace_sessions": 5,
            "initial_nav": format(initial_nav, "f"),
            "initial_state_hash": initial_state_hash,
        }
    )


def session_evidence_content_hash(value: BaseModel) -> str:
    return model_content_hash(value, exclude={"content_hash"})
