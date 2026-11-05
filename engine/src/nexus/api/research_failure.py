"""Refine a failed read view from verified frozen facts, without changing accounting."""

import json
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.research_stages.proof import TerminalProof
from nexus.agents.research_stages.protocol import evidence_digest


async def read_output_limit_reason(
    session: AsyncSession, proof: TerminalProof
) -> Literal["model_output_limit_reached"] | None:
    # This is only called after the normal terminal reader verifies the exact
    # owner, membership and settlement. Unknown usage keeps its existing view.
    if not (proof.known and proof.budget_settled and proof.cleanup_confirmed):
        return None
    evidence = await session.scalar(
        text("SELECT evidence FROM research_stage_terminal WHERE run_id=:run"),
        {"run": proof.run_id},
    )
    if (
        not isinstance(evidence, list)
        or not evidence
        or evidence_digest(evidence) != proof.evidence_hash
    ):
        return None
    last = evidence[-1]
    if (
        not isinstance(last, dict)
        or last.get("verified") is not True
        or last.get("valid") is not False
        or last.get("eligible") is not True
        or last.get("platform_binding") is None
        or not isinstance(last.get("platform_response"), str)
    ):
        return None
    try:
        response = json.loads(last["platform_response"])
    except ValueError:
        return None
    if not isinstance(response, dict) or response.get("status") != "incomplete":
        return None
    details = response.get("incomplete_details")
    if isinstance(details, dict) and details.get("reason") == "max_output_tokens":
        return "model_output_limit_reached"
    return None
