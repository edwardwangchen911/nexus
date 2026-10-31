"""Opt-in, fixed Platform pricing; no dispatch, ledger or payment-source authority.

Price basis (2026-10-31): Terra/default, implicit caching, 30-minute writes,
at most 272,000 input tokens per request. USD per million: ordinary 2, read
0.20, write 2.50, output 12. Sources:
https://developers.openai.com/api/docs/models/gpt-5.6-terra
https://developers.openai.com/api/docs/guides/prompt-caching

Existing profiles and consumers deliberately do not import this module.
"""

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from decimal import Decimal

PLATFORM_PRICE_VERSION = "platform-terra-default-implicit30m-20261031-v1"
_MAX_REQUEST_INPUT = 272_000


@dataclass(frozen=True)
class PlatformUsage:
    """Counts are independently validated integers; verify totals before aggregating.

    ``totals_verified`` checks input + full output == total, independently of
    cache classifications and pricing eligibility. Exact cost is unavailable
    whenever any required pricing evidence is missing or contradictory. This
    result never releases a reservation or proves a successful/terminal stage.
    ``raw_usage`` is an isolated evidence copy, omitted from repr; callers retain
    their original immutable observation and must not log arbitrary extras.
    """

    input_tokens: int | None
    output_tokens: int | None
    total_tokens: int | None
    totals_verified: bool
    cached_tokens: int | None
    cache_write_tokens: int | None
    cache_write_30m_tokens: int | None
    cache_write_12h_tokens: int | None
    ordinary_input_tokens: int | None
    reasoning_tokens: int | None
    exact_cost_usd: Decimal | None
    issues: tuple[str, ...]
    raw_usage: object = field(repr=False)
    price_version: str = PLATFORM_PRICE_VERSION


def _mapping(value: object) -> Mapping[object, object]:
    return value if isinstance(value, Mapping) else {}


def _count(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _usd(tenth_microdollars: int) -> Decimal:
    # Integer arithmetic and decimal construction avoid context-dependent rounding.
    return Decimal(f"{tenth_microdollars}e-7")


def price_platform_usage(
    raw_usage: object,
    *,
    model: object,
    service_tier: object,
    cache_mode: object,
    cache_ttl: object,
) -> PlatformUsage:
    """Evaluate one raw usage mapping without coercion or missing-value defaults.

    The caller supplies actual response model/tier and the bound request's cache
    policy; absence is unknown, not a request-default inference. Read the original
    JSON usage before lossy normalization. A strict native ResponseUsage dump
    preserves model_extra fields; this function cannot undo earlier coercion or
    field loss. No current business route is opted in by this helper alone.

    Cost is model-token cost under this price version, not a provider invoice,
    a funding-source proof, or an authority to settle/release shared budget.
    """
    raw = _mapping(raw_usage)
    inp = _count(raw.get("input_tokens"))
    out = _count(raw.get("output_tokens"))
    total = _count(raw.get("total_tokens"))
    details = _mapping(raw.get("input_tokens_details"))
    cached = _count(details.get("cached_tokens"))
    written = _count(details.get("cache_write_tokens"))
    writes = _mapping(details.get("cache_write_tokens_details"))
    write_30m = _count(writes.get("ttl_30m_tokens"))
    write_12h = _count(writes.get("ttl_12h_tokens"))
    reasoning = _count(_mapping(raw.get("output_tokens_details")).get("reasoning_tokens"))
    issues: list[str] = []
    totals_verified = False
    if inp is None or out is None or total is None:
        issues.append("PLATFORM_USAGE_TOTALS_UNKNOWN")
    elif inp + out != total:
        issues.append("PLATFORM_USAGE_TOTALS_CONFLICT")
    else:
        totals_verified = True

    if any(value is None for value in (cached, written, write_30m, write_12h, reasoning)):
        issues.append("PLATFORM_USAGE_DETAILS_UNKNOWN")
    ordinary = None
    if inp is not None and cached is not None and written is not None:
        if cached + written > inp:
            issues.append("PLATFORM_USAGE_DETAILS_CONFLICT")
        else:
            ordinary = inp - cached - written
    if written is not None and write_30m is not None and write_12h is not None:
        if write_30m + write_12h != written:
            issues.append("PLATFORM_USAGE_DETAILS_CONFLICT")
    if out is not None and reasoning is not None and reasoning > out:
        issues.append("PLATFORM_USAGE_DETAILS_CONFLICT")
    if write_12h is not None and write_12h > 0:
        issues.append("PLATFORM_CACHE_TTL_UNSUPPORTED")
    if (
        model != "gpt-5.6-terra"
        or service_tier != "default"
        or cache_mode != "implicit"
        or cache_ttl != "30m"
    ):
        issues.append("PLATFORM_PROFILE_MISMATCH")
    if inp is not None and inp > _MAX_REQUEST_INPUT:
        issues.append("PLATFORM_PRICE_INPUT_TIER_UNSUPPORTED")

    cost = None
    if not issues:
        assert ordinary is not None and cached is not None and write_30m is not None
        assert out is not None
        # All output is billed, including reasoning; writes replace ordinary input.
        cost = _usd(ordinary * 20 + cached * 2 + write_30m * 25 + out * 120)
    return PlatformUsage(
        input_tokens=inp,
        output_tokens=out,
        total_tokens=total,
        totals_verified=totals_verified,
        cached_tokens=cached,
        cache_write_tokens=written,
        cache_write_30m_tokens=write_30m,
        cache_write_12h_tokens=write_12h,
        ordinary_input_tokens=ordinary,
        reasoning_tokens=reasoning,
        exact_cost_usd=cost,
        issues=tuple(dict.fromkeys(issues)),
        raw_usage=deepcopy(raw_usage),
    )


def platform_reservation_usd(
    stage_input_upper_bounds: Sequence[int], output_upper_bound: int
) -> Decimal:
    """Conservative bound for this fixed profile, never an actual usage settlement.

    Each input bound must cover that stage's final wire and framing; the caller
    binds the fixed profile to those requests. Output is the shared chain bound,
    including reasoning, not a fresh allowance for each stage. The 272k price
    threshold applies per request, so a chain's sum may exceed it. This pure
    calculation does not reserve funds or approve any request or budget limit.
    """
    if (
        not stage_input_upper_bounds
        or any(
            type(value) is not int or not 0 <= value <= _MAX_REQUEST_INPUT
            for value in stage_input_upper_bounds
        )
        or type(output_upper_bound) is not int
        or output_upper_bound < 0
    ):
        raise ValueError("PLATFORM_RESERVATION_INVALID")
    return _usd(sum(stage_input_upper_bounds) * 25 + output_upper_bound * 120)
