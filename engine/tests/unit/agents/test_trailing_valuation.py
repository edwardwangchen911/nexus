"""Deterministic trailing valuation from frozen Agent paper inputs."""

from copy import deepcopy

import pytest
from nexus.agents import codex_sdk


def _security_snapshot() -> dict[str, object]:
    return {
        "status": "ok",
        "items": [
            {
                "evidence_ref_id": "00000000-0000-4000-8000-000000000008",
                "source": "nexus_agent_paper_context",
                "agent_paper_context": {
                    "decision_time": "2026-09-15T17:05:34.420570Z",
                    "decision_quote": {
                        "source": "schwab",
                        "feed": "l1",
                        "delivery_mode": "real_time",
                        "delayed": False,
                        "bid": "329.560000000000",
                        "ask": "329.590000000000",
                        "observed_at": "2026-09-15T17:05:33.351000Z",
                        "available_at": "2026-09-15T17:05:34.300722Z",
                    },
                },
            }
        ],
    }


def _eps_fact(
    value: str,
    start: str,
    end: str,
    *,
    evidence_ref_id: str,
) -> dict[str, object]:
    return {
        "evidence_ref_id": evidence_ref_id,
        "fact": "diluted_eps",
        "raw_taxonomy": "us-gaap",
        "raw_tag": "EarningsPerShareDiluted",
        "value": value,
        "unit": "USD/shares",
        "period_start": start,
        "period_end": end,
        "period_kind": "duration",
        "accepted_at": "2026-07-31T10:01:02Z",
        "revision": "0000320193-26-000020@2026-07-31T10:01:02+00:00",
    }


def _financial_facts() -> dict[str, object]:
    annual_ref = "00000000-0000-4000-8000-000000000001"
    quarter_ref = "00000000-0000-4000-8000-000000000002"
    return {
        "status": "ok",
        "items": [
            _eps_fact("7.46", "2024-09-29", "2025-09-27", evidence_ref_id=annual_ref),
            _eps_fact("5.62", "2024-09-29", "2025-06-28", evidence_ref_id=quarter_ref),
            _eps_fact("6.88", "2025-09-28", "2026-06-27", evidence_ref_id=quarter_ref),
            _eps_fact("2.02", "2026-03-29", "2026-06-27", evidence_ref_id=quarter_ref),
        ],
    }


def test_trailing_valuation_uses_frozen_schwab_midpoint_and_aligned_eps() -> None:
    result = codex_sdk.derive_trailing_valuation(
        _security_snapshot(), _financial_facts()
    )

    assert result == {
        "schema_version": "agent_trailing_valuation_v1",
        "status": "available",
        "reason": None,
        "price": {
            "basis": "schwab_l1_bid_ask_midpoint",
            "bid": "329.560000000000",
            "ask": "329.590000000000",
            "midpoint": "329.575000000000",
            "observed_at": "2026-09-15T17:05:33.351000Z",
            "evidence_ref_id": "00000000-0000-4000-8000-000000000008",
        },
        "ttm_eps": {
            "formula": "annual_fy_minus_prior_ytd_plus_current_ytd",
            "value": "8.720000000000",
            "unit": "USD/shares",
            "components": [
                {
                    "kind": "annual_fy",
                    "value": "7.46",
                    "period_start": "2024-09-29",
                    "period_end": "2025-09-27",
                    "evidence_ref_id": "00000000-0000-4000-8000-000000000001",
                },
                {
                    "kind": "prior_ytd",
                    "value": "5.62",
                    "period_start": "2024-09-29",
                    "period_end": "2025-06-28",
                    "evidence_ref_id": "00000000-0000-4000-8000-000000000002",
                },
                {
                    "kind": "current_ytd",
                    "value": "6.88",
                    "period_start": "2025-09-28",
                    "period_end": "2026-06-27",
                    "evidence_ref_id": "00000000-0000-4000-8000-000000000002",
                },
            ],
        },
        "metrics": {"trailing_pe": "37.795298165138"},
    }


def test_trailing_valuation_allows_negative_period_component_when_ttm_is_positive() -> None:
    facts = _financial_facts()
    facts["items"][2]["value"] = "-1.00"  # type: ignore[index]

    result = codex_sdk.derive_trailing_valuation(_security_snapshot(), facts)

    assert result["status"] == "available"
    assert result["ttm_eps"]["value"] == "0.840000000000"  # type: ignore[index]


@pytest.mark.parametrize(
    ("prior_end", "current_end"),
    [("2025-04-27", "2026-04-26"), ("2025-07-27", "2026-07-26")],
)
def test_trailing_valuation_accepts_aligned_first_quarter_and_half_year(
    prior_end: str, current_end: str
) -> None:
    facts = {
        "status": "ok",
        "items": [
            _eps_fact("7.46", "2025-01-27", "2026-01-25", evidence_ref_id="annual"),
            _eps_fact("5.62", "2025-01-27", prior_end, evidence_ref_id="interim"),
            _eps_fact("6.88", "2026-01-26", current_end, evidence_ref_id="interim"),
            # A standalone quarter must not substitute for fiscal-year-to-date EPS.
            _eps_fact("2.02", "2025-04-28", "2025-07-27", evidence_ref_id="interim"),
        ],
    }

    result = codex_sdk.derive_trailing_valuation(_security_snapshot(), facts)

    assert result["status"] == "available"
    assert result["ttm_eps"]["value"] == "8.720000000000"  # type: ignore[index]


def test_trailing_valuation_rejects_unequal_ytd_lengths_despite_annual_end_spacing() -> None:
    facts = _financial_facts()
    # Both fit the old nine-month range and end dates are one year apart,
    # but one begins 14 days late and is not a comparable cumulative period.
    facts["items"][2]["period_start"] = "2025-10-12"  # type: ignore[index]
    facts["items"][0]["period_end"] = "2025-10-11"  # type: ignore[index]

    result = codex_sdk.derive_trailing_valuation(_security_snapshot(), facts)

    assert result["status"] == "unavailable"
    assert result["reason"] == "aligned_diluted_eps_unavailable"


@pytest.mark.parametrize("mutation", ["annual_missing", "quarter_only", "duplicate", "unit"])
def test_half_year_valuation_preserves_component_authority(mutation: str) -> None:
    items = [
        _eps_fact("7.46", "2025-01-27", "2026-01-25", evidence_ref_id="annual"),
        _eps_fact("5.62", "2025-01-27", "2025-07-27", evidence_ref_id="interim"),
        _eps_fact("6.88", "2026-01-26", "2026-07-26", evidence_ref_id="interim"),
    ]
    if mutation == "annual_missing":
        items.pop(0)
    elif mutation == "quarter_only":
        items[1]["period_start"] = "2025-04-28"
        items[2]["period_start"] = "2026-04-27"
    elif mutation == "duplicate":
        items.append(deepcopy(items[2]))
    else:
        items[2]["unit"] = "shares"

    result = codex_sdk.derive_trailing_valuation(
        _security_snapshot(), {"status": "ok", "items": items}
    )

    assert result["status"] == "unavailable"
    assert result["reason"] == "aligned_diluted_eps_unavailable"


def _multiple_interim_facts() -> dict[str, object]:
    return {
        "status": "ok",
        "items": [
            _eps_fact("7.46", "2024-11-04", "2025-11-02", evidence_ref_id="annual"),
            _eps_fact("3.00", "2024-11-04", "2025-05-04", evidence_ref_id="half"),
            _eps_fact("4.00", "2025-11-03", "2026-05-03", evidence_ref_id="half"),
            _eps_fact("5.62", "2024-11-04", "2025-08-03", evidence_ref_id="nine"),
            _eps_fact("6.88", "2025-11-03", "2026-08-02", evidence_ref_id="nine"),
        ],
    }


def test_trailing_valuation_selects_latest_period_independent_of_item_order() -> None:
    facts = _multiple_interim_facts()
    result = codex_sdk.derive_trailing_valuation(_security_snapshot(), facts)
    facts["items"].reverse()  # type: ignore[attr-defined]
    assert codex_sdk.derive_trailing_valuation(_security_snapshot(), facts) == result
    assert result["status"] == "available"
    assert result["ttm_eps"]["value"] == "8.720000000000"  # type: ignore[index]
    assert result["ttm_eps"]["components"][2]["period_end"] == "2026-08-02"  # type: ignore[index]


@pytest.mark.parametrize("mutation", ["prior_missing", "latest_conflict"])
def test_trailing_valuation_does_not_fall_back_from_incomplete_or_conflicting_latest(
    mutation: str,
) -> None:
    facts = _multiple_interim_facts()
    if mutation == "prior_missing":
        facts["items"].pop(3)  # type: ignore[attr-defined]
    else:
        duplicate = deepcopy(facts["items"][4])  # type: ignore[index]
        duplicate["value"] = "7.00"
        facts["items"].append(duplicate)  # type: ignore[attr-defined]
    result = codex_sdk.derive_trailing_valuation(_security_snapshot(), facts)
    assert result["status"] == "unavailable"
    assert result["reason"] == "aligned_diluted_eps_unavailable"


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("delayed_quote", "decision_quote_unavailable"),
        ("cross_source_ytd", "aligned_diluted_eps_unavailable"),
        ("unit_mismatch", "aligned_diluted_eps_unavailable"),
    ],
)
def test_trailing_valuation_fails_closed_for_unqualified_components(
    mutation: str, reason: str
) -> None:
    security = _security_snapshot()
    facts = _financial_facts()
    if mutation == "delayed_quote":
        security["items"][0]["agent_paper_context"]["decision_quote"]["delayed"] = True  # type: ignore[index]
    elif mutation == "cross_source_ytd":
        facts["items"][2]["evidence_ref_id"] = (  # type: ignore[index]
            "00000000-0000-4000-8000-000000000003"
        )
    else:
        facts["items"][2]["unit"] = "shares"  # type: ignore[index]

    original_security = deepcopy(security)
    original_facts = deepcopy(facts)
    result = codex_sdk.derive_trailing_valuation(security, facts)

    assert result == {
        "schema_version": "agent_trailing_valuation_v1",
        "status": "unavailable",
        "reason": reason,
    }
    assert security == original_security
    assert facts == original_facts


def test_trailing_valuation_does_not_qualify_eps_pending_amendment_review():
    facts = _financial_facts()
    facts["items"][0]["amendment_review_status"] = "unverified"
    result = codex_sdk.derive_trailing_valuation(_security_snapshot(), facts)
    assert result["status"] == "unavailable"
    assert result["reason"] == "sec_amendment_accounting_review_unverified"
