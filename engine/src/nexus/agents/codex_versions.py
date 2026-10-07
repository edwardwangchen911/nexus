"""Exact SDK runtime versions; a budget revision does not upgrade research semantics."""

from decimal import Decimal

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


def stage_timeout_seconds(stage: str, *, deadline: float, now: float) -> float:
    """v6 only: protect later phases and SDK shutdown within the existing total cap."""
    # Provisional allocation for isolated representative-load validation. Changes
    # to these reserves require another runtime version, never a silent v6 edit.
    later_reserve = {"research": 55, "critique": 30, "synthesis": 0}[stage]
    # close() 5s + force-stop observation 1s + stream task observation 1s.
    return max(0.0, min(115.0, deadline - now) - later_reserve - 7)
