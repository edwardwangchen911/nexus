"""Canonical SHA-256 payloads for immutable Proposal Shadow facts."""

import hashlib
import json
from dataclasses import fields, is_dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast
from uuid import UUID

from nexus.shadow.models import (
    SHADOW_VARIANTS,
    ProposalVariantHashInput,
    ShadowSessionHashInput,
    VariantMetrics,
)
from nexus.simulation.canonical import RESULT_QUANTUM, canonical_decimal, validated_decimal
from nexus.simulation.models import SimulationState, TargetWeight


def _canonical_value(value: object) -> object:
    if isinstance(value, float):
        raise TypeError("shadow hashes must not contain binary floats")
    if isinstance(value, Decimal):
        return format(canonical_decimal(value), "f")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("shadow hash timestamps must be UTC")
        return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if isinstance(value, date):
        return value.isoformat()
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _canonical_value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("shadow hash mappings require string keys")
        return {key: _canonical_value(value[key]) for key in sorted(value)}
    if isinstance(value, tuple | list):
        return [_canonical_value(item) for item in value]
    if isinstance(value, set | frozenset):
        normalized = [_canonical_value(item) for item in value]
        return sorted(normalized, key=_canonical_json)
    if value is None or isinstance(value, str | int | bool):
        return value
    raise TypeError(f"unsupported shadow hash value: {type(value).__name__}")


def _canonical_json(value: object) -> str:
    return json.dumps(
        _canonical_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _content_hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def shadow_session_content_hash(value: ShadowSessionHashInput) -> str:
    corporate_actions = tuple(event for event in value.events if event.event_kind != "delisting")
    delistings = tuple(event for event in value.events if event.event_kind == "delisting")
    return _content_hash(
        {
            "schema": "shadow_session_sha256_v1",
            "shadow_group_id": value.shadow_group_id,
            "session": value.session,
            "ordinal": value.ordinal,
            "calendar": value.calendar,
            "policies": {
                "source_policy_id": value.source_policy_id,
                "cost_policy_id": value.cost_policy_id,
            },
            "bars": value.bars,
            "missing_execution_inputs": value.missing_execution_inputs,
            "corporate_actions": corporate_actions,
            "delistings": delistings,
        }
    )


def shadow_source_query_content_hash(
    *,
    shadow_group_id: UUID,
    session: date,
    kind: str,
    source: str,
    delivery_mode: str,
    instrument_ids: tuple[UUID, ...],
    complete: bool,
    member_ids: tuple[UUID, ...],
    negative_instrument_ids: tuple[UUID, ...],
) -> str:
    """Hash one sanitized, exact provider/source query proof."""
    if kind not in {"price_bars", "corporate_actions"}:
        raise ValueError("shadow query kind is unsupported")
    if not source or not delivery_mode:
        raise ValueError("shadow query source and delivery mode are required")
    ordered_instruments = tuple(sorted(instrument_ids, key=lambda item: item.int))
    ordered_members = tuple(sorted(member_ids, key=lambda item: item.int))
    ordered_negative = tuple(
        sorted(negative_instrument_ids, key=lambda item: item.int)
    )
    if (
        len(ordered_instruments) != len(set(ordered_instruments))
        or len(ordered_members) != len(set(ordered_members))
        or len(ordered_negative) != len(set(ordered_negative))
        or not set(ordered_negative) <= set(ordered_instruments)
        or (not complete and (ordered_members or ordered_negative))
    ):
        raise ValueError("shadow query proof members are invalid")
    return _content_hash(
        {
            "schema": "shadow_source_query_sha256_v1",
            "shadow_group_id": shadow_group_id,
            "session": session,
            "kind": kind,
            "source": source,
            "delivery_mode": delivery_mode,
            "instrument_ids": ordered_instruments,
            "complete": complete,
            "member_ids": ordered_members,
            "negative_instrument_ids": ordered_negative,
        }
    )


def shadow_query_bundle_content_hash(values: tuple[object, ...]) -> str:
    """Hash already-canonical source summaries without provider payloads."""
    return _content_hash(
        {
            "schema": "shadow_query_bundle_sha256_v1",
            "source_queries": values,
        }
    )


def target_weights_content_hash(
    targets: tuple[TargetWeight, ...],
    cash_weight: Decimal,
) -> str:
    instrument_ids = [item.instrument_id for item in targets]
    if len(instrument_ids) != len(set(instrument_ids)):
        raise ValueError("target weight instruments must be unique")
    cash = validated_decimal(cash_weight, field="cash weight")
    if cash < 0 or cash > 1:
        raise ValueError("cash weight must be between zero and one")
    total = sum((item.weight for item in targets), cash)
    if abs(total - Decimal(1)) > RESULT_QUANTUM:
        raise ValueError("target and cash weights must sum to one")
    ordered = tuple(sorted(targets, key=lambda item: item.instrument_id.int))
    return _content_hash(
        {
            "schema": "shadow_target_weights_sha256_v1",
            "targets": ordered,
            "cash_weight": cash,
        }
    )


def proposal_bundle_content_hash(values: tuple[ProposalVariantHashInput, ...]) -> str:
    if len(values) != 3 or {item.variant for item in values} != set(SHADOW_VARIANTS):
        raise ValueError("proposal bundle requires exactly three fixed variants")
    if len({item.proposal_id for item in values}) != 3:
        raise ValueError("proposal identifiers must be unique")
    order = {variant: index for index, variant in enumerate(SHADOW_VARIANTS)}
    ordered = tuple(sorted(values, key=lambda item: order[item.variant]))
    return _content_hash(
        {
            "schema": "shadow_proposal_bundle_sha256_v1",
            "proposals": tuple(
                {
                    "proposal_id": item.proposal_id,
                    "variant": item.variant,
                    "target_weights_hash": target_weights_content_hash(
                        item.targets,
                        item.cash_weight,
                    ),
                }
                for item in ordered
            ),
        }
    )


def simulation_state_content_hash(state: SimulationState) -> str:
    return _content_hash(
        {
            "schema": "shadow_state_sha256_v1",
            "cash": state.cash,
            "instruments": state.instruments,
            "receivables": state.receivables,
        }
    )


def checkpoint_metrics_content_hash(values: tuple[VariantMetrics, ...]) -> str:
    if tuple(item.variant for item in values) != SHADOW_VARIANTS:
        raise ValueError("checkpoint metrics must use fixed variant order")
    return _content_hash(
        {
            "schema": "shadow_checkpoint_metrics_sha256_v1",
            "metrics": values,
        }
    )


def final_authority_content_hash(
    session_hashes: tuple[tuple[date, str], ...],
    checkpoint_hashes: tuple[tuple[int, str], ...],
) -> str:
    ordered_sessions = tuple(sorted(session_hashes, key=lambda item: item[0]))
    ordered_checkpoints = tuple(sorted(checkpoint_hashes, key=lambda item: item[0]))
    for _, value in (*ordered_sessions, *ordered_checkpoints):
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("final authority members require canonical SHA-256 values")
    return _content_hash(
        {
            "schema": "shadow_final_authority_sha256_v1",
            "sessions": cast(object, ordered_sessions),
            "checkpoints": cast(object, ordered_checkpoints),
        }
    )
