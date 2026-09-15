"""Two SEC objects must bind by identity even when tool and catalog orders differ."""

import hashlib
from copy import deepcopy

import pytest
from nexus.agents.codex_input_view import project_staged_payload
from unit.agents.test_codex_sdk import _restore_view


def payload():
    refs = [
        {
            "index": i,
            "source": "sec",
            "object_id": f"filing:{kind}",
            "content_hash": str(i) * 64,
            "available_at": "2026-09-01T00:00:00Z",
            "location": f"memory://{kind}",
        }
        for i, kind in enumerate(["older", "latest"])
    ]
    security = [
        {
            "evidence_ref_id": f"ref-{i}",
            "source": r["source"],
            "provider_object_id": r["object_id"],
            "content_hash": r["content_hash"],
            "available_at": r["available_at"],
        }
        for i, r in enumerate(refs)
    ]
    return {
        "instructions": "Use frozen sources.",
        "evidence": refs,
        "frozen_inputs": {
            "security_snapshot": {"items": security},
            "filing_disclosures": {
                "items": [
                    {
                        "evidence_ref_id": "ref-1",
                        "disclosure": {"quarter_metrics": {"current_period_end": "2026-08-01"}},
                    },
                    {"evidence_ref_id": "ref-0", "disclosure": None},
                ]
            },
            "financial_facts": {
                "items": [
                    {
                        "evidence_ref_id": "ref-0",
                        "fact": "revenue",
                        "value": "12.345",
                        "period_end": "2026-05-01",
                    }
                ]
            },
        },
    }


def restore(value):
    restored = _restore_view(value)
    return restored


def test_tool_adjacent_indexes_follow_exact_identity_not_position_and_preserve_raw():
    raw = payload()
    before = deepcopy(raw)
    projected = project_staged_payload(raw, inline_citations=True)
    items = restore(projected["frozen_inputs"])
    assert [r["citation_index"] for r in items["filing_disclosures"]["items"]] == [1, 0]
    assert items["financial_facts"]["items"][0]["citation_index"] == 0
    assert raw == before
    for name, tool in items.items():
        for row in tool["items"]:
            row.pop("citation_index")
    assert items == before["frozen_inputs"]


def test_derived_frozen_input_is_preserved_while_tool_rows_receive_citations():
    raw = payload()
    raw["frozen_inputs"]["trailing_valuation"] = {
        "schema_version": "agent_trailing_valuation_v1",
        "status": "available",
        "price": {"midpoint": "329.575000000000", "evidence_ref_id": "ref-1"},
        "metrics": {"trailing_pe": "37.795298165138"},
    }
    before = deepcopy(raw)

    projected = project_staged_payload(raw, inline_citations=True)
    inputs = restore(projected["frozen_inputs"])

    assert inputs["trailing_valuation"] == before["frozen_inputs"]["trailing_valuation"]
    assert inputs["financial_facts"]["items"][0]["citation_index"] == 0
    assert raw == before


@pytest.mark.parametrize(
    "problem", ["unknown_id", "duplicate_id", "wrong_hash", "wrong_time", "duplicate_identity"]
)
def test_unbound_or_ambiguous_source_never_falls_back_to_zero(problem):
    raw = payload()
    security = raw["frozen_inputs"]["security_snapshot"]["items"]
    if problem == "unknown_id":
        raw["frozen_inputs"]["filing_disclosures"]["items"][0]["evidence_ref_id"] = "not-bound"
    elif problem == "duplicate_id":
        security[1]["evidence_ref_id"] = security[0]["evidence_ref_id"]
    elif problem == "wrong_hash":
        security[1]["content_hash"] = "f" * 64
    elif problem == "wrong_time":
        security[1]["available_at"] = "2026-08-01T00:00:00Z"
    else:
        raw["evidence"].append({**raw["evidence"][0], "index": 2})
    with pytest.raises(ValueError, match="CODEX_TOOL_CITATION_BINDING_INVALID"):
        project_staged_payload(raw, inline_citations=True)


def test_default_preserves_legacy_wire_and_does_not_annotate():
    from nexus.agents.codex_sdk import codex_request_wire

    raw = payload()
    implicit = project_staged_payload(raw)
    explicit = project_staged_payload(raw, inline_citations=False)
    assert codex_request_wire(implicit) == codex_request_wire(explicit)
    # Captured from the unchanged parent implementation, before inline citation support.
    assert hashlib.sha256(codex_request_wire(implicit).encode()).hexdigest() == (
        "18ada285bf47f4948275f8b956d6aaba3d51d6bc606edec01b855580868c588b"
    )
    assert restore(implicit["frozen_inputs"]) == raw["frozen_inputs"]


def test_equal_hashes_on_distinct_objects_are_not_conflated():
    raw = payload()
    raw["evidence"][1]["content_hash"] = raw["evidence"][0]["content_hash"]
    raw["frozen_inputs"]["security_snapshot"]["items"][1]["content_hash"] = raw["evidence"][0][
        "content_hash"
    ]
    result = restore(project_staged_payload(raw, inline_citations=True)["frozen_inputs"])
    assert [r["citation_index"] for r in result["filing_disclosures"]["items"]] == [1, 0]
