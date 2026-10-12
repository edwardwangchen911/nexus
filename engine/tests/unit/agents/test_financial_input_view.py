from copy import deepcopy

import pytest
from nexus.agents.financial_input_view import financial_table_payload, restore_financial_table


def payload():
    base = {
        "fact": "revenue",
        "raw_taxonomy": "us-gaap",
        "raw_tag": "Revenue",
        "unit": "USD",
        "evidence_ref_id": "source-1",
        "citation_index": 3,
        "period_start": "2026-01-01",
        "period_end": "2026-03-31",
        "period_kind": "duration",
        "accepted_at": "2026-05-01T00:00:00Z",
        "revision": "v1",
        "value": "0.000000000001",
    }
    rows = [
        base,
        {**base, "value": None},
        {**base, "value": "0"},
        {
            **base,
            "unit": "USD/shares",
            "period_start": "2025-01-01",
            "value": "-12.0000",
            "accounting_limitations": ["unresolved_revision"],
        },
        {**base, "value": "0.000000000001"},
    ]
    return {
        "instructions": "PRIVATE_INSTRUCTION",
        "frozen_inputs": {
            "financial_facts": {"status": "partial", "items": rows},
            "filing_disclosures": {"items": [{"text": "PRIVATE_DISCLOSURE"}]},
        },
    }


def test_roundtrip_keeps_all_fields_periods_units_nulls_order_and_duplicate_conflicts():
    raw = payload()
    before = deepcopy(raw)
    view = financial_table_payload(raw)
    assert raw == before
    assert (
        restore_financial_table(view["frozen_inputs"]["financial_facts"])
        == before["frozen_inputs"]["financial_facts"]["items"]
    )
    assert (
        view["frozen_inputs"]["filing_disclosures"] == before["frozen_inputs"]["filing_disclosures"]
    )
    assert view["frozen_inputs"]["financial_facts"]["status"] == "partial"
    view["frozen_inputs"]["financial_facts"]["source_columns"][-1]["accounting_limitations"].append(
        "changed"
    )
    assert raw == before


@pytest.mark.parametrize(
    "problem", ["duplicate", "missing", "negative", "bool_column", "column_collision"]
)
def test_invalid_table_does_not_silently_repair_or_fill_missing_facts(problem):
    tool = financial_table_payload(payload())["frozen_inputs"]["financial_facts"]
    cells = tool["items"][0]["cells"]
    if problem == "duplicate":
        cells.append(deepcopy(cells[0]))
    elif problem == "missing":
        cells.pop()
    elif problem == "negative":
        cells[0][1] = -1
    elif problem == "bool_column":
        cells[0][0] = True
    else:
        tool["source_columns"][0]["value"] = "fake"
    with pytest.raises(ValueError, match="MODEL_FINANCIAL_TABLE_INVALID"):
        restore_financial_table(tool)


def test_missing_citation_or_metric_never_creates_default_binding():
    for field in ("citation_index", "fact"):
        raw = payload()
        raw["frozen_inputs"]["financial_facts"]["items"][0].pop(field)
        with pytest.raises(ValueError, match="MODEL_FINANCIAL_TABLE_INVALID"):
            financial_table_payload(raw)


def test_empty_and_already_projected_inputs_are_not_reinterpreted():
    raw = payload()
    raw["frozen_inputs"]["financial_facts"]["items"] = []
    assert financial_table_payload(raw) is raw
    with pytest.raises(ValueError, match="MODEL_FINANCIAL_TABLE_INVALID"):
        financial_table_payload(financial_table_payload(payload()))


def bound_payload():
    raw = payload()
    raw["evidence"] = [
        {
            "index": 0,
            "source": "fixture",
            "object_id": "facts",
            "content_hash": "a" * 64,
            "available_at": "2026-05-01T00:00:00Z",
            "location": "memory://facts",
        }
    ]
    raw["frozen_inputs"]["security_snapshot"] = {
        "items": [
            {
                "evidence_ref_id": "source-1",
                "source": "fixture",
                "provider_object_id": "facts",
                "content_hash": "a" * 64,
                "available_at": "2026-05-01T00:00:00Z",
            }
        ]
    }
    base = raw["frozen_inputs"]["financial_facts"]["items"][0]
    base.pop("citation_index")
    raw["frozen_inputs"]["financial_facts"]["items"] = [
        {
            **base,
            "fact": f"metric-{index % 7}",
            "raw_tag": f"tag-{index % 7}",
            "period_end": f"2026-0{index % 3 + 1}-28",
            "value": str(index),
        }
        for index in range(50)
    ]
    return raw


def test_opt_in_compiler_uses_bound_indexes_and_default_wire_stays_unchanged():
    from nexus.agents.codex_input_view import project_staged_payload
    from nexus.agents.codex_sdk import codex_request_wire
    from unit.agents.test_codex_sdk import _restore_view

    raw = bound_payload()
    before = deepcopy(raw)
    legacy = project_staged_payload(raw, inline_citations=True)
    assert codex_request_wire(legacy) == codex_request_wire(
        project_staged_payload(raw, inline_citations=True, financial_table_candidate=False)
    )
    candidate = project_staged_payload(raw, inline_citations=True, financial_table_candidate=True)
    restored = _restore_view(candidate["frozen_inputs"])
    records = restore_financial_table(restored["financial_facts"])
    assert records == [
        {**row, "citation_index": 0} for row in before["frozen_inputs"]["financial_facts"]["items"]
    ]
    assert len(codex_request_wire(candidate).encode()) < len(codex_request_wire(legacy).encode())
    assert raw == before


def test_candidate_never_bypasses_source_binding_or_infers_zero():
    from nexus.agents.codex_input_view import project_staged_payload

    raw = bound_payload()
    with pytest.raises(ValueError, match="MODEL_FINANCIAL_TABLE_BINDING_REQUIRED"):
        project_staged_payload(raw, financial_table_candidate=True)
    raw["frozen_inputs"]["financial_facts"]["items"][0]["evidence_ref_id"] = "not-bound"
    with pytest.raises(ValueError, match="CODEX_TOOL_CITATION_BINDING_INVALID"):
        project_staged_payload(raw, inline_citations=True, financial_table_candidate=True)
