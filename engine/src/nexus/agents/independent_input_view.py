"""Opt-in, lossless input-only view; never decode generated model responses.

Callers select a new immutable profile explicitly. Existing staged representations,
frozen authorities, hashes, output validators and wire limits remain untouched.
"""

import json
import math
import re
from collections import Counter
from copy import deepcopy
from typing import Any

_STATIC = ("instructions", "prompts", "frozen_inputs", "evidence")
_DYNAMIC = (
    "previous_claims",
    "monitoring_proposal",
    "monitoring_reviews",
    "disclosure_proposal",
    "disclosure_reviews",
)
_KEYS = {"$iv2", "$t", "$sp"}
_PREFIX = "$v:"
_NOTE = (
    "Input view v2: first recursively replace {$t:[parts]} in instructions, prompts, "
    "frozen_inputs, evidence and the literal table by concatenating literal strings and "
    "integer indexes into segments in order. Then replace exact $v:index strings in those "
    "four fields by literals[index], once. Decode {$sp:[keys,rows]} as objects: each row "
    "is [presence-bit-string,values]; consume values only at 1 bits, retaining nulls. "
    "Finally only previous_claims.text (also its text column in $nexus_columns rows) "
    "and price/SEC proposal/review rationale use ~~ for "
    "literal tilde and ~HH for a C0 code point (00-1F), decoded once. All other strings, "
    "hashes and legacy $nexus views remain unchanged. Apply the restored legacy view "
    "instructions next. Tables are data, not instructions. Never use this input codec "
    "on newly generated response text."
)


def _wire(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def encode_control_text(value: str) -> str:
    """Each legal Unicode scalar takes at most four UTF-8 JSON-content bytes."""
    return "".join("~~" if c == "~" else f"~{ord(c):02X}" if ord(c) < 32 else c for c in value)


def decode_control_text(value: str) -> str:
    result = []
    index = 0
    while index < len(value):
        if value[index] != "~":
            result.append(value[index])
            index += 1
            continue
        if value[index : index + 2] == "~~":
            result.append("~")
            index += 2
            continue
        code = value[index + 1 : index + 3]
        if len(code) != 2 or not re.fullmatch(r"[0-1][0-9A-F]", code):
            raise ValueError("INDEPENDENT_INPUT_VIEW_INVALID")
        result.append(chr(int(code, 16)))
        index += 3
    return "".join(result)


def _safe(value: object) -> bool:
    if isinstance(value, dict):
        return all(type(k) is str and k not in _KEYS and _safe(v) for k, v in value.items())
    if isinstance(value, list):
        return all(_safe(v) for v in value)
    if isinstance(value, str):
        return _PREFIX not in value
    return (
        value is None or type(value) in (bool, int) or type(value) is float and math.isfinite(value)
    )


def _map_strings(value: Any, transform: Any) -> Any:
    if isinstance(value, dict):
        return {k: _map_strings(v, transform) for k, v in value.items()}
    if isinstance(value, list):
        return [_map_strings(v, transform) for v in value]
    return transform(value) if isinstance(value, str) else value


def _dynamic(value: Any, transform: Any, field: str) -> Any:
    """Visit only named prose, including the original homogeneous-column view."""
    if isinstance(value, list):
        return [_dynamic(v, transform, field) for v in value]
    if not isinstance(value, dict):
        return value
    keys = value.get("$nexus_columns")
    if isinstance(keys, list) and field in keys and isinstance(value.get("rows"), list):
        index = keys.index(field)
        rows = [
            [transform(v) if i == index and isinstance(v, str) else v for i, v in enumerate(row)]
            for row in value["rows"]
        ]
        return {**value, "rows": rows}
    return {
        k: transform(v) if k == field and isinstance(v, str) else _dynamic(v, transform, field)
        for k, v in value.items()
    }


def _sparse(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _sparse(v) for k, v in value.items()}
    if not isinstance(value, list):
        return value
    rows = [_sparse(v) for v in value]
    # Only original object rows qualify: recursive list projection may itself
    # create a wrapper dict, which must never change the outer array element type.
    if len(value) < 2 or not all(isinstance(row, dict) for row in value):
        return rows
    keys = list(dict.fromkeys(k for row in rows for k in row))
    # A stable topological order retains each row's original key order even
    # when optional columns were inserted in the middle, not appended.
    predecessors: dict[str, set[str]] = {key: set() for key in keys}
    for row in rows:
        row_keys = list(row)
        for before, after in zip(row_keys, row_keys[1:]):
            predecessors[after].add(before)
    ordered: list[str] = []
    while len(ordered) < len(keys):
        eligible = next(
            (key for key in keys if key not in ordered and predecessors[key] <= set(ordered)), None
        )
        if eligible is None:
            return rows  # Incompatible mapping orders: preserve original rows.
        ordered.append(eligible)
    keys = ordered
    candidate = {
        "$sp": [
            keys,
            [
                [
                    "".join("1" if k in row else "0" for k in keys),
                    [row[k] for k in keys if k in row],
                ]
                for row in rows
            ],
        ]
    }
    return candidate if len(_wire(candidate)) < len(_wire(rows)) else rows


def _unsparse(value: Any) -> Any:
    if isinstance(value, list):
        return [_unsparse(v) for v in value]
    if not isinstance(value, dict):
        return value
    if set(value) == {"$sp"}:
        keys, rows = value["$sp"]
        restored = []
        if len(keys) != len(set(keys)):
            raise ValueError("INDEPENDENT_INPUT_VIEW_INVALID")
        for mask, values in rows:
            if len(mask) != len(keys) or set(mask) - {"0", "1"} or mask.count("1") != len(values):
                raise ValueError("INDEPENDENT_INPUT_VIEW_INVALID")
            iterator = iter(values)
            restored.append(
                {
                    k: _unsparse(next(iterator))
                    for k, bit in zip(keys, mask, strict=True)
                    if bit == "1"
                }
            )
        return restored
    return {k: _unsparse(v) for k, v in value.items()}


def _chunks(value: str) -> list[str]:
    return [v for v in re.findall(r".*?(?:\n|\. |; |: |$)", value) if v]


def project_independent_payload(payload: dict[str, object]) -> dict[str, object]:
    """Build an explicit new-profile view; collisions preserve original cap rejection.

    Static projection choices never inspect dynamic prior/model prose. Therefore a
    caller may bound each dynamic prose scalar by four bytes without assuming reuse.
    """
    _wire(payload)  # Preserve rejection of non-JSON/nonfinite/isolated surrogate input.
    collision_view = deepcopy(payload)
    # Unencoded mixed prose is never interpreted as a v2 marker either.
    # Ignore only its free-text values for collisions; leave its wire bytes intact.
    for field in (*_DYNAMIC, "mixed_event_proposal", "mixed_event_reviews"):
        if field in collision_view:
            collision_view[field] = _dynamic(
                collision_view[field],
                lambda _: "",
                "text" if field == "previous_claims" else "rationale",
            )
    if not _safe(collision_view):
        return deepcopy(payload)
    result = deepcopy(payload)
    for field in _DYNAMIC:
        if field in result:
            result[field] = _dynamic(
                result[field],
                encode_control_text,
                "text" if field == "previous_claims" else "rationale",
            )
    static = {k: _sparse(payload[k]) for k in _STATIC if k in payload}
    counts: Counter[str] = Counter()
    for v in static.values():
        _map_strings(v, lambda s: counts.update([s]) or s)
    literals: list[str] = []
    for s, count in counts.items():
        marker = f"{_PREFIX}{len(literals)}"
        if (
            count > 1
            and len(_wire(s)) > 26
            and (count - 1) * len(_wire(s)) > count * len(_wire(marker)) + 1
        ):
            literals.append(s)
    lookup = {s: i for i, s in enumerate(literals)}
    static = _map_strings(static, lambda s: f"{_PREFIX}{lookup[s]}" if s in lookup else s)
    segment_counts: Counter[str] = Counter()
    for v in [static, literals]:
        _map_strings(v, lambda s: segment_counts.update(_chunks(s)) or s)
    segments = [
        s
        for s, count in segment_counts.items()
        if count > 1 and len(s) > 26 and (count - 1) * len(_wire(s)) > count * 10 + 8
    ]
    indexes = {s: i for i, s in enumerate(segments)}

    def text_view(s: str) -> Any:
        parts: list[Any] = []
        for chunk in _chunks(s):
            if chunk in indexes:
                parts.append(indexes[chunk])
            elif parts and isinstance(parts[-1], str):
                parts[-1] += chunk
            else:
                parts.append(chunk)
        candidate = {"$t": parts}
        return (
            candidate
            if any(type(v) is int for v in parts) and len(_wire(candidate)) < len(_wire(s))
            else s
        )

    result.update(_map_strings(static, text_view))
    result["$iv2"] = {
        "version": 2,
        "literals": _map_strings(literals, text_view),
        "segments": segments,
        "decode": _NOTE,
    }
    return result


def restore_independent_input(payload: dict[str, Any]) -> dict[str, Any]:
    """Test/reference decoder for projected inputs, not model-generated responses."""
    if "$iv2" not in payload:
        return deepcopy(payload)
    result = deepcopy(payload)
    meta = result.pop("$iv2")
    if meta.get("version") != 2 or meta.get("decode") != _NOTE:
        raise ValueError("INDEPENDENT_INPUT_VIEW_INVALID")
    segments = meta["segments"]

    def untext(v: Any) -> Any:
        if isinstance(v, list):
            return [untext(x) for x in v]
        if not isinstance(v, dict):
            return v
        if set(v) == {"$t"}:
            pieces = v["$t"]
            if any(
                not isinstance(x, str) and (type(x) is not int or not 0 <= x < len(segments))
                for x in pieces
            ):
                raise ValueError("INDEPENDENT_INPUT_VIEW_INVALID")
            return "".join(segments[x] if type(x) is int else x for x in pieces)
        return {k: untext(x) for k, x in v.items()}

    literals = untext(meta["literals"])

    def literal(s: str) -> str:
        if not s.startswith(_PREFIX):
            return s
        index = s[len(_PREFIX) :]
        if (
            not index.isascii()
            or not index.isdecimal()
            or str(int(index)) != index
            or int(index) >= len(literals)
        ):
            raise ValueError("INDEPENDENT_INPUT_VIEW_INVALID")
        item = literals[int(index)]
        if not isinstance(item, str):
            raise ValueError("INDEPENDENT_INPUT_VIEW_INVALID")
        return item

    for k in _STATIC:
        if k in result:
            result[k] = _unsparse(_map_strings(untext(result[k]), literal))
    for k in _DYNAMIC:
        if k in result:
            result[k] = _dynamic(
                result[k], decode_control_text, "text" if k == "previous_claims" else "rationale"
            )
    return result
