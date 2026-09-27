"""Fail-closed trailing valuation derived from frozen Agent research inputs."""

from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from typing import NamedTuple

_SCHEMA_VERSION = "agent_trailing_valuation_v1"
_TWELVE_PLACES = Decimal("0.000000000001")


class _EpsFact(NamedTuple):
    value: Decimal
    value_text: str
    unit: str
    period_start: date
    period_end: date
    evidence_ref_id: str
    revision: str
    raw_taxonomy: str
    raw_tag: str


def _unavailable(reason: str) -> dict[str, object]:
    return {
        "schema_version": _SCHEMA_VERSION,
        "status": "unavailable",
        "reason": reason,
    }


def _finite_decimal(value: object) -> Decimal | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        return None
    return parsed if parsed.is_finite() else None


def _positive_decimal(value: object) -> Decimal | None:
    parsed = _finite_decimal(value)
    return parsed if parsed is not None and parsed > 0 else None


def _decision_quote(
    security_snapshot: Mapping[str, object],
) -> tuple[Decimal, Decimal, str, str] | None:
    if security_snapshot.get("status") != "ok":
        return None
    items = security_snapshot.get("items")
    if not isinstance(items, Sequence) or isinstance(items, str | bytes):
        return None
    candidates: list[tuple[Decimal, Decimal, str, str]] = []
    for item in items:
        if not isinstance(item, Mapping) or item.get("source") != "nexus_agent_paper_context":
            continue
        evidence_ref_id = item.get("evidence_ref_id")
        context = item.get("agent_paper_context")
        if not isinstance(evidence_ref_id, str) or not isinstance(context, Mapping):
            continue
        quote = context.get("decision_quote")
        if not isinstance(quote, Mapping):
            continue
        if (
            quote.get("source") != "schwab"
            or quote.get("feed") != "l1"
            or quote.get("delivery_mode") != "real_time"
            or quote.get("delayed") is not False
        ):
            continue
        bid = _positive_decimal(quote.get("bid"))
        ask = _positive_decimal(quote.get("ask"))
        observed_at = quote.get("observed_at")
        if bid is None or ask is None or ask < bid or not isinstance(observed_at, str):
            continue
        candidates.append((bid, ask, observed_at, evidence_ref_id))
    return candidates[0] if len(candidates) == 1 else None


def _parse_eps_fact(item: object) -> _EpsFact | None:
    if not isinstance(item, Mapping):
        return None
    required_strings = (
        "value",
        "unit",
        "period_start",
        "period_end",
        "evidence_ref_id",
        "revision",
        "raw_taxonomy",
        "raw_tag",
    )
    if (
        item.get("fact") != "diluted_eps"
        or item.get("period_kind") != "duration"
        or any(not isinstance(item.get(key), str) or not item.get(key) for key in required_strings)
    ):
        return None
    value_text = str(item["value"])
    value = _finite_decimal(value_text)
    if value is None:
        return None
    try:
        period_start = date.fromisoformat(str(item["period_start"]))
        period_end = date.fromisoformat(str(item["period_end"]))
    except ValueError:
        return None
    if period_end < period_start:
        return None
    return _EpsFact(
        value=value,
        value_text=value_text,
        unit=str(item["unit"]),
        period_start=period_start,
        period_end=period_end,
        evidence_ref_id=str(item["evidence_ref_id"]),
        revision=str(item["revision"]),
        raw_taxonomy=str(item["raw_taxonomy"]),
        raw_tag=str(item["raw_tag"]),
    )


def _aligned_eps_components(
    financial_facts: Mapping[str, object],
) -> tuple[_EpsFact, _EpsFact, _EpsFact] | None:
    if financial_facts.get("status") != "ok":
        return None
    items = financial_facts.get("items")
    if not isinstance(items, Sequence) or isinstance(items, str | bytes):
        return None
    facts = tuple(fact for item in items if (fact := _parse_eps_fact(item)) is not None)
    if not facts:
        return None
    # Older interim reports remain useful evidence, but cannot make current
    # valuation ambiguous or stand in for an incomplete latest reporting period.
    latest_period_end = max(fact.period_end for fact in facts)
    annual = tuple(
        fact for fact in facts if 330 <= (fact.period_end - fact.period_start).days <= 380
    )
    # Interim reports carry fiscal-year-to-date EPS for one, two, or three
    # quarters. Fiscal boundaries below distinguish YTD from standalone quarters.
    ytd = tuple(fact for fact in facts if 75 <= (fact.period_end - fact.period_start).days <= 300)
    matches: list[tuple[_EpsFact, _EpsFact, _EpsFact]] = []
    for current in ytd:
        if current.period_end != latest_period_end:
            continue
        for prior in ytd:
            if not 350 <= (current.period_end - prior.period_end).days <= 380:
                continue
            # A 52/53-week fiscal calendar can differ by one week, but unlike
            # end-date spacing alone this rejects unequal cumulative periods.
            if abs(
                (current.period_end - current.period_start).days
                - (prior.period_end - prior.period_start).days
            ) > 7:
                continue
            if (
                current.evidence_ref_id != prior.evidence_ref_id
                or current.revision != prior.revision
                or current.unit != prior.unit
                or current.raw_taxonomy != prior.raw_taxonomy
                or current.raw_tag != prior.raw_tag
            ):
                continue
            for full_year in annual:
                if (
                    full_year.period_start == prior.period_start
                    and current.period_start == full_year.period_end + timedelta(days=1)
                    and full_year.unit == current.unit
                    and full_year.raw_taxonomy == current.raw_taxonomy
                    and full_year.raw_tag == current.raw_tag
                ):
                    matches.append((full_year, prior, current))
    return matches[0] if len(matches) == 1 else None


def _component(kind: str, fact: _EpsFact) -> dict[str, str]:
    return {
        "kind": kind,
        "value": fact.value_text,
        "period_start": fact.period_start.isoformat(),
        "period_end": fact.period_end.isoformat(),
        "evidence_ref_id": fact.evidence_ref_id,
    }


def derive_trailing_valuation(
    security_snapshot: Mapping[str, object],
    financial_facts: Mapping[str, object],
) -> dict[str, object]:
    """Derive trailing P/E only when frozen quote and EPS periods align exactly."""

    quote = _decision_quote(security_snapshot)
    if quote is None:
        return _unavailable("decision_quote_unavailable")
    components = _aligned_eps_components(financial_facts)
    if components is None:
        return _unavailable("aligned_diluted_eps_unavailable")
    annual, prior_ytd, current_ytd = components
    ttm_eps = annual.value - prior_ytd.value + current_ytd.value
    if not ttm_eps.is_finite() or ttm_eps <= 0:
        return _unavailable("positive_ttm_eps_unavailable")
    bid, ask, observed_at, quote_evidence_ref_id = quote
    midpoint = (bid + ask) / 2
    trailing_pe = midpoint / ttm_eps
    return {
        "schema_version": _SCHEMA_VERSION,
        "status": "available",
        "reason": None,
        "price": {
            "basis": "schwab_l1_bid_ask_midpoint",
            "bid": format(bid.quantize(_TWELVE_PLACES), "f"),
            "ask": format(ask.quantize(_TWELVE_PLACES), "f"),
            "midpoint": format(midpoint.quantize(_TWELVE_PLACES), "f"),
            "observed_at": observed_at,
            "evidence_ref_id": quote_evidence_ref_id,
        },
        "ttm_eps": {
            "formula": "annual_fy_minus_prior_ytd_plus_current_ytd",
            "value": format(ttm_eps.quantize(_TWELVE_PLACES), "f"),
            "unit": annual.unit,
            "components": [
                _component("annual_fy", annual),
                _component("prior_ytd", prior_ytd),
                _component("current_ytd", current_ytd),
            ],
        },
        "metrics": {
            "trailing_pe": format(trailing_pe.quantize(_TWELVE_PLACES), "f")
        },
    }
