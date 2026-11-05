import json
from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.agents.research_stages.proof import TerminalProof
from nexus.agents.research_stages.protocol import evidence_digest
from nexus.api.research_failure import read_output_limit_reason


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "exact",
        "unknown_usage",
        "unsettled",
        "unclean",
        "changed_membership",
        "missing",
        "late_unverified",
        "valid",
        "not_eligible",
        "legacy",
        "malformed",
        "completed",
        "private_reason",
        "missing_reason",
    ],
)
async def test_output_limit_requires_exact_terminal_membership_and_known_settlement(case):
    response = {"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}}
    if case == "completed":
        response["status"] = "completed"
    if case == "private_reason":
        response["incomplete_details"] = {"reason": "max_output_tokens PRIVATE"}
    if case == "missing_reason":
        response.pop("incomplete_details")
    rows = [
        {
            "verified": case != "late_unverified",
            "valid": case == "valid",
            "eligible": case != "not_eligible",
            "platform_binding": None if case == "legacy" else {"frozen": True},
            "platform_response": "PRIVATE" if case == "malformed" else json.dumps(response),
        }
    ]
    if case == "missing":
        rows = []
    proof = TerminalProof(UUID(int=1), True, True, True, False, evidence_digest(rows), "a" * 64)
    if case in {"unknown_usage", "unsettled", "unclean"}:
        field = {
            "unknown_usage": "known",
            "unsettled": "budget_settled",
            "unclean": "cleanup_confirmed",
        }[case]
        proof = replace(proof, **{field: False})
    if case == "changed_membership":
        proof = replace(proof, evidence_hash="b" * 64)
    session = AsyncMock()
    session.scalar.return_value = rows
    reason = await read_output_limit_reason(session, proof)
    assert reason == ("model_output_limit_reached" if case == "exact" else None)
    if case in {"unknown_usage", "unsettled", "unclean"}:
        session.scalar.assert_not_awaited()
