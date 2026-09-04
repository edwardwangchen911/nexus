"""Lossless, temporary JSON view for staged research; never changes frozen facts."""

import json
import math
from typing import cast

from nexus.agents.codex_sdk import codex_request_wire

_MARKERS = {"$nexus_view", "$nexus_columns"}
_SCOPE_FIELDS = (
    "schema_version",
    "instrument_id",
    "data_snapshot_id",
    "as_of",
    "evidence_hash",
    "portfolio_scope",
)
_FORMAT_NOTE = (
    " Frozen-input view v1: $nexus_view wraps tools plus common_scope shared by every tool. "
    "Recursively decode {$nexus_columns:[keys],rows:[[values]]} as objects in column/row order. "
    "Preserve values and nulls."
)


def _safe_json(value: object) -> bool:
    if isinstance(value, dict):
        return all(
            isinstance(key, str) and key not in _MARKERS and _safe_json(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return all(_safe_json(item) for item in value)
    return (
        value is None
        or type(value) in {str, int, bool}
        or (type(value) is float and math.isfinite(value))
    )


def _json(value: object, *, sort_keys: bool = False) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=sort_keys)


def _columns(value: object) -> object:
    if isinstance(value, dict):
        return {key: _columns(item) for key, item in value.items()}
    if not isinstance(value, list):
        return value
    items = [_columns(item) for item in value]
    if not value or not all(isinstance(item, dict) for item in value):
        return items
    keys = list(value[0])
    if any(list(item) != keys for item in value):
        return items
    candidate = {
        "$nexus_columns": keys,
        "rows": [[row[key] for key in keys] for row in cast(list[dict[str, object]], items)],
    }
    return candidate if len(_json(candidate).encode()) < len(_json(items).encode()) else items


def project_staged_payload(payload: dict[str, object]) -> dict[str, object]:
    """Return a smaller model-facing payload, including its fixed decoding note.

    Reserved-key collisions, non-JSON inputs and no net savings preserve the
    complete original payload. Object order is not semantic; array order is.
    """
    inputs = payload.get("frozen_inputs")
    instructions = payload.get("instructions")
    if (
        not isinstance(inputs, dict)
        or not inputs
        or not isinstance(instructions, str)
        or not _safe_json(inputs)
        or not all(isinstance(tool, dict) for tool in inputs.values())
    ):
        return payload
    wrappers = tuple(inputs.values())
    common = {
        field: wrappers[0][field]
        for field in _SCOPE_FIELDS
        if len(wrappers) > 1
        and all(field in tool for tool in wrappers)
        # Python equality conflates true/1 and 1/1.0. JSON type/value equality does not.
        and all(
            _json(tool[field], sort_keys=True) == _json(wrappers[0][field], sort_keys=True)
            for tool in wrappers[1:]
        )
    }
    view = {
        "$nexus_view": 1,
        "common_scope": _columns(common),
        "tools": {
            name: _columns({key: item for key, item in tool.items() if key not in common})
            for name, tool in inputs.items()
        },
    }
    candidate = {**payload, "frozen_inputs": view, "instructions": instructions + _FORMAT_NOTE}
    return (
        candidate
        if len(codex_request_wire(candidate).encode()) < len(codex_request_wire(payload).encode())
        else payload
    )
