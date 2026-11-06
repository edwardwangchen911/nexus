"""The old gap survives bootstrap; later discovery never filters by filing date."""

from copy import deepcopy

import pytest
from nexus.monitoring.disclosure_forward import bootstrap_entries, compare_forward
from nexus.research.sec_disclosure_index import build_disclosure_index
from unit.research.test_sec_listed_history import (
    CIK,
    IDENTITY,
    INSTRUMENT,
    NAME,
    build,
    case,
    packed,
)


def indexes():
    parent, history = case()
    baseline = build_disclosure_index(
        *packed(parent),
        instrument_id=INSTRUMENT,
        identity_snapshot_id=IDENTITY,
        issuer_cik=CIK,
    )
    first = build(parent, ((NAME, *packed(history, NAME)),))
    return parent, history, baseline, first


def test_bootstrap_is_unknown_and_does_not_dispatch_first_history():
    _, _, old, first = indexes()
    assert not old.history_complete
    assert bootstrap_entries(old, first) == ()
    assert compare_forward(old, first, first, is_origin=True) == (
        "unknown",
        "SEC_INDEX_FORWARD_BASELINE_ESTABLISHED",
        (),
        "partial",
    )
    assert compare_forward(old, first, first, is_origin=False) == ("false", None, (), "complete")


def test_new_recent_at_bootstrap_is_not_swallowed():
    parent, history, old, _ = indexes()
    recent = parent["filings"]["recent"]
    for key in recent:
        recent[key].append(recent[key][0])
    recent["accessionNumber"][-1] = "0000000042-26-000003"
    first = build(parent, ((NAME, *packed(history, NAME)),))
    result = compare_forward(old, first, first, is_origin=True)
    assert result[0] == "true" and result[3] == "partial"
    assert [x.accession_number for x in result[2]] == ["0000000042-26-000003"]
    assert compare_forward(old, first, first, is_origin=False)[2] == result[2]


@pytest.mark.parametrize("mode", ["late", "move", "conflict", "missing", "partial"])
def test_complete_origin_comparison_preserves_late_metadata_and_unknown(mode):
    parent, history, old, first = indexes()
    next_parent, next_history = deepcopy(parent), deepcopy(history)
    if mode == "late":
        for key in next_history:
            next_history[key].append(next_history[key][0])
        next_history["accessionNumber"][-1] = "0000000042-20-000009"
        next_parent["filings"]["files"][0]["filingCount"] = 2
    elif mode == "move":
        for key in next_history:
            next_parent["filings"]["recent"][key].extend(next_history[key])
        next_parent["filings"]["files"] = []
    elif mode == "conflict":
        next_history["form"][0] = "8-K"
    elif mode == "missing":
        next_parent["filings"]["files"] = []
    history_input = (
        () if mode in {"move", "missing", "partial"} else ((NAME, *packed(next_history, NAME)),)
    )
    current = build(next_parent, history_input)
    result = compare_forward(old, first, current, is_origin=False)
    if mode == "late":
        assert result[0] == "true" and len(result[2]) == 1
        assert result[2][0].primary_document == "old.txt"
    elif mode == "move":
        assert result == ("false", None, (), "complete")
    else:
        assert result[0] == "unknown" and not result[2]
