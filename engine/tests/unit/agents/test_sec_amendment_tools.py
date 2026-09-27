"""Only newly bound amendment evidence expands the read-only filing selector."""

import pytest
from nexus.agents.tools import ReadOnlyTools, ToolInputError, response_function_tools


def test_amendment_tool_schema_is_opt_in_and_legacy_defaults_unchanged():
    legacy = ReadOnlyTools()
    amended = ReadOnlyTools(allow_sec_amendments=True)
    payload = {
        "instrument_id": "00000000-0000-0000-0000-000000000001",
        "data_snapshot_id": "00000000-0000-0000-0000-000000000002",
        "as_of": "2026-09-02T00:00:00Z",
        "forms": ["8-K/A"],
        "filed_from": "2026-08-01",
        "filed_to": "2026-09-01",
    }
    with pytest.raises(ToolInputError):
        legacy.validate("search_filing_evidence", payload)
    assert amended.validate("search_filing_evidence", payload)["forms"] == ("8-K/A",)
    assert legacy.filing_forms == ("10-K", "10-Q", "8-K", "6-K", "497K")
    assert "8-K/A" in amended.filing_forms
    before = response_function_tools()
    after = response_function_tools(allow_sec_amendments=True)
    assert response_function_tools(allow_sec_amendments=False) == before
    for name in before:
        if name != "search_filing_evidence":
            assert before[name] == after[name]
    assert "8-K/A" not in str(before["search_filing_evidence"])
    assert "8-K/A" in str(after["search_filing_evidence"])
