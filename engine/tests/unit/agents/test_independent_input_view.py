"""New input-only view must preserve every frozen value and historical wire."""

import json
from copy import deepcopy

import pytest
from nexus.agents.independent_input_view import (
    decode_control_text,
    encode_control_text,
    project_independent_payload,
    restore_independent_input,
)


def test_control_encoding_exact_for_every_c0_tilde_and_unicode():
    text = "".join(chr(i) for i in range(32)) + '~00~~中文𠀀"\\'
    assert decode_control_text(encode_control_text(text)) == text
    for char in text:
        assert len(json.dumps(encode_control_text(char), ensure_ascii=False).encode()) - 2 <= 4
    with pytest.raises(UnicodeEncodeError):
        project_independent_payload({"instructions": "\ud800"})


@pytest.mark.parametrize("invalid", ["~", "~GG", "~20", "~0"])
def test_control_decoder_rejects_invalid_forms(invalid):
    with pytest.raises(ValueError):
        decode_control_text(invalid)


def sample():
    prose = "A complete immutable paragraph shared exactly across model inputs. "
    return {
        "instructions": prose * 8,
        "prompts": [{"role": "portfolio", "content": prose * 8}],
        "frozen_inputs": {
            "$nexus_strings": ["original legacy literal"],
            "tools": {
                "rows": [
                    {
                        "very_long_column_name_a": i,
                        "very_long_column_name_b": None,
                        "description": prose,
                    }
                    if i % 2
                    else {"very_long_column_name_a": i, "description": prose}
                    for i in range(20)
                ],
                "literal": "$nexus:0",
                "nested": {"normal": "\0~00"},
            },
        },
        "evidence": [{"index": i, "source": prose, "hash": "f" * 64} for i in range(4)],
        "previous_claims": {
            "$nexus_columns": ["claim_id", "text", "citations"],
            "rows": [
                ["bear:0", "\0~00中文𠀀", [0]],
                ["audit:0", "\1" * 400, [1]],
            ],
        },
        "monitoring_proposal": {"rationale": "\0" * 500, "content_hash": "~unchanged"},
        "monitoring_reviews": {"bear": {"rationale": "literal ~00", "proposal_hash": "f" * 64}},
        "disclosure_proposal": None,
        "disclosure_reviews": {"evidence_audit": {"rationale": "\x1f" * 160}},
    }


def test_projection_exact_roundtrip_and_no_input_mutation_or_hash_rewrite():
    original = sample()
    before = deepcopy(original)
    projected = project_independent_payload(original)
    assert projected != original
    assert original == before
    assert restore_independent_input(projected) == original
    assert projected["monitoring_proposal"]["content_hash"] == "~unchanged"
    assert projected["previous_claims"]["rows"][0][1] == "~00~~00中文𠀀"
    assert len(json.dumps(projected, ensure_ascii=False).encode()) < len(
        json.dumps(original, ensure_ascii=False).encode()
    )


@pytest.mark.parametrize("collision", [{"$iv2": 1}, {"nested": {"$t": []}}, {"text": "$v:0"}])
def test_reserved_collision_preserves_complete_original(collision):
    payload = sample()
    payload["collision"] = collision
    assert project_independent_payload(payload) == payload


def test_static_projection_does_not_depend_on_dynamic_prose():
    first = sample()
    second = deepcopy(first)
    second["previous_claims"]["rows"][0][1] = "different unique prose" * 10
    second["monitoring_proposal"]["rationale"] = "another explanation"
    a = project_independent_payload(first)
    b = project_independent_payload(second)
    for field in ("instructions", "prompts", "frozen_inputs", "evidence", "$iv2"):
        assert a[field] == b[field]
    assert restore_independent_input(b) == second


def test_missing_null_array_order_and_dictionary_key_order_survive():
    payload = sample()
    payload["frozen_inputs"]["order"] = [{"b": 1, "a": 2}, {"a": 3, "b": 4}]
    restored = restore_independent_input(project_independent_payload(payload))
    assert json.dumps(restored, ensure_ascii=False) == json.dumps(payload, ensure_ascii=False)


def test_sparse_preserves_middle_optional_key_order_without_losing_savings():
    from nexus.agents.independent_input_view import _sparse, _unsparse

    rows = [
        {"long_column_alpha": 1, "long_column_gamma": 3},
        {"long_column_alpha": 2, "long_column_beta": None, "long_column_gamma": 4},
    ] * 10
    projected = _sparse(rows)
    assert isinstance(projected, dict) and "$sp" in projected
    assert json.dumps(_unsparse(projected)) == json.dumps(rows)


def test_dynamic_literal_marker_does_not_change_static_view_or_fall_back():
    first = sample()
    second = deepcopy(first)
    second["previous_claims"]["rows"][0][1] = "$v:0 $iv2 ~00 \0"
    a, b = project_independent_payload(first), project_independent_payload(second)
    assert b["$iv2"] == a["$iv2"]
    assert restore_independent_input(b) == second


def test_four_byte_bound_without_duplicate_prose_or_unicode_assumptions():
    # UTF-8 scalar boundaries, JSON escapes, C0 and the new escape character.
    points = [*range(128), 0x7FF, 0x800, 0xD7FF, 0xE000, 0xFFFF, 0x10000, 0x10FFFF]
    unique = "".join(chr(point) for point in points)
    encoded = encode_control_text(unique)
    assert decode_control_text(encoded) == unique
    assert len(
        json.dumps(encoded, ensure_ascii=False, separators=(",", ":")).encode()
    ) - 2 <= 4 * len(unique)
    for point in points:
        char = chr(point)
        assert len(json.dumps(encode_control_text(char), ensure_ascii=False).encode()) - 2 <= 4


def test_plain_previous_claims_and_empty_sparse_rows_roundtrip():
    payload = sample()
    payload["previous_claims"] = [{"claim_id": "bear:0", "text": "~00\0", "citations": [0]}]
    payload["frozen_inputs"]["empty_rows"] = [{}, {"optional_long_key": None}] * 10
    before = json.dumps(payload, ensure_ascii=False)
    assert (
        json.dumps(
            restore_independent_input(project_independent_payload(payload)), ensure_ascii=False
        )
        == before
    )


@pytest.mark.parametrize("depth", [1, 2, 5])
def test_nested_mixed_arrays_keep_original_types_values_and_key_order(depth):
    row = {
        "long_column_a_abcdefghijklmnopqrstuvwxyz": 1,
        "long_column_b_abcdefghijklmnopqrstuvwxyz": "x",
    }
    items = [row, row]
    for _ in range(depth):
        items = [items, row, row]
    original = {"frozen_inputs": {"items": items}}
    restored = restore_independent_input(project_independent_payload(original))
    assert restored == original
    assert json.dumps(restored, ensure_ascii=False) == json.dumps(original, ensure_ascii=False)
