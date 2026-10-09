"""Exact SDK runtime versions; a budget revision does not upgrade research semantics."""

from decimal import Decimal
from math import isfinite

CONFIG_VERSION = "codex-sdk-chatgpt-v1"
STAGED_CONFIG_VERSION = "codex-sdk-chatgpt-v2"
STRUCTURED_CONFIG_VERSION = "codex-sdk-chatgpt-v3"
STRUCTURED_96K_CONFIG_VERSION = "codex-sdk-chatgpt-v3-input96k"
STRUCTURED_96K_WIRE64K_CONFIG_VERSION = "codex-sdk-chatgpt-v3-input96k-wire64k"
INLINE_CITATION_CONFIG_VERSION = "codex-sdk-chatgpt-v3-input96k-wire64k-cite1"
LEGACY_ACTIONABLE_CONFIG_VERSION = "codex-sdk-chatgpt-v4-actionable"
ACTIONABLE_CONFIG_VERSION = "codex-sdk-chatgpt-v5-paper-scope"
RESEARCH_SDK_HANDLER_TIMEOUT_SECONDS = 135
RESEARCH_STAGE_JOB_TIMEOUT_SECONDS = 120
DEADLINE_BUDGET_CONFIG_VERSION = "codex-sdk-chatgpt-v6-stage-time-budget"
# Qualification candidate only. Keep it out of admission/runtime version sets
# until its frozen identity, exact claim, and full-cycle routing are implemented.
# Pending formal identity: registration alone never opens execution or selection.
FORMAL_STAGE_CAPACITY_CONFIG_VERSION = "codex-sdk-chatgpt-v8-stage-capacity"
CANDIDATE_BUDGET_CONFIG_VERSION = "codex-sdk-chatgpt-v7-stage-capacity-candidate"
FORMAL_RESEARCH_JOB_TIMEOUT_SECONDS = 180
FORMAL_RESEARCH_HANDLER_TIMEOUT_SECONDS = 195
CANDIDATE_RESEARCH_JOB_TIMEOUT_SECONDS = 180
CANDIDATE_RESEARCH_HANDLER_TIMEOUT_SECONDS = 195
PAPER_SCOPE_CONFIG_VERSIONS = frozenset({ACTIONABLE_CONFIG_VERSION, DEADLINE_BUDGET_CONFIG_VERSION})
ACTIONABLE_CONFIG_VERSIONS = frozenset(
    {LEGACY_ACTIONABLE_CONFIG_VERSION, *PAPER_SCOPE_CONFIG_VERSIONS}
)
PORTFOLIO_DECISION_CONFIG_VERSION = "codex-sdk-portfolio-decision-v1"
PORTFOLIO_DECISION_MAX_INPUT_TOKENS = 128_000
PORTFOLIO_DECISION_MAX_INPUT_TOKENS_PER_TURN = 64_000
PORTFOLIO_DECISION_MAX_OUTPUT_TOKENS = 16_384
PORTFOLIO_DECISION_MAX_OUTPUT_TOKENS_PER_TURN = 8_192
PORTFOLIO_DECISION_MAX_TOOL_CALLS = 1
PORTFOLIO_DECISION_MAX_WIRE_BYTES = 64_000
PORTFOLIO_DECISION_SDK_TIMEOUT_SECONDS = 45
# Two provider turns plus bounded SDK cleanup remain below the dedicated lease.
# This does not add retries or increase the two-call limit for either transport.
PORTFOLIO_DECISION_ABSOLUTE_TIMEOUT_SECONDS = Decimal("105")
PORTFOLIO_DECISION_LEASE_SECONDS = 120
STRUCTURED_CONFIG_VERSIONS = frozenset(
    {
        STRUCTURED_CONFIG_VERSION,
        STRUCTURED_96K_CONFIG_VERSION,
        STRUCTURED_96K_WIRE64K_CONFIG_VERSION,
        INLINE_CITATION_CONFIG_VERSION,
        DEADLINE_BUDGET_CONFIG_VERSION,
        ACTIONABLE_CONFIG_VERSION,
        LEGACY_ACTIONABLE_CONFIG_VERSION,
    }
)
STAGED_CONFIG_VERSIONS = STRUCTURED_CONFIG_VERSIONS | {STAGED_CONFIG_VERSION}
SDK_CONFIG_VERSIONS = STAGED_CONFIG_VERSIONS | {CONFIG_VERSION}


def aggregate_input_limit(version: str) -> int:
    if version not in SDK_CONFIG_VERSIONS:
        raise ValueError("CODEX_CONFIG_VERSION_INVALID")
    return (
        96000
        if version
        in {
            STRUCTURED_96K_CONFIG_VERSION,
            STRUCTURED_96K_WIRE64K_CONFIG_VERSION,
            INLINE_CITATION_CONFIG_VERSION,
            DEADLINE_BUDGET_CONFIG_VERSION,
            ACTIONABLE_CONFIG_VERSION,
            LEGACY_ACTIONABLE_CONFIG_VERSION,
        }
        else 64000
    )


def wire_byte_limit(version: str) -> int:
    if version not in SDK_CONFIG_VERSIONS:
        raise ValueError("CODEX_CONFIG_VERSION_INVALID")
    return (
        64000
        if version
        in {
            STRUCTURED_96K_WIRE64K_CONFIG_VERSION,
            INLINE_CITATION_CONFIG_VERSION,
            DEADLINE_BUDGET_CONFIG_VERSION,
            ACTIONABLE_CONFIG_VERSION,
            LEGACY_ACTIONABLE_CONFIG_VERSION,
        }
        else 55808
    )


def stage_timeout_seconds(
    stage: str,
    *,
    deadline: float,
    now: float,
    version: str = DEADLINE_BUDGET_CONFIG_VERSION,
) -> float:
    """Allocate inside an already fixed committee deadline; never restart time."""
    if version in {CANDIDATE_BUDGET_CONFIG_VERSION, FORMAL_STAGE_CAPACITY_CONFIG_VERSION}:
        if (
            type(deadline) not in {int, float}
            or type(now) not in {int, float}
            or not isfinite(deadline)
            or not isfinite(now)
        ):
            raise ValueError("CODEX_STAGE_BUDGET_CLOCK_INVALID")
        cap = {"research": 60, "critique": 45, "synthesis": 45}[stage]
        # Each future stage reserves both its provider cap and seven-second
        # close bound. Terminal writes are already outside this deadline.
        later_reserve = {"research": 104, "critique": 52, "synthesis": 0}[stage]
        return max(0.0, min(cap, deadline - now - 7 - later_reserve))
    if version != DEADLINE_BUDGET_CONFIG_VERSION:
        raise ValueError("CODEX_STAGE_BUDGET_VERSION_INVALID")
    # Frozen v6: preserve its exact allocation for old Run identities.
    # Provisional allocation for isolated representative-load validation. Changes
    # to these reserves require another runtime version, never a silent v6 edit.
    later_reserve = {"research": 55, "critique": 30, "synthesis": 0}[stage]
    # close() 5s + force-stop observation 1s + stream task observation 1s.
    return max(0.0, min(115.0, deadline - now) - later_reserve - 7)
