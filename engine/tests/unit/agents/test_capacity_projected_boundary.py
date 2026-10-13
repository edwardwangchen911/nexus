"""Admission bounds the lossless wire, including projection fallback branches."""

from unittest.mock import AsyncMock

import pytest
from nexus.agents.codex_capacity import (
    CapacityPoolMember,
    candidate_capacity_envelope,
    preflight_capacity_pool,
)
from nexus.agents.codex_sdk import codex_request_wire
from nexus.agents.committee import ROLE_ORDER
from unit.agents.test_codex_sdk import _authority
from unit.agents.test_full_input_capacity_v9 import committee


@pytest.mark.asyncio
@pytest.mark.parametrize("padding", [61088, 61089, 61441])
async def test_pool_accepts_wire_within_limit_even_when_decoded_rows_exceed_it(padding):
    request, evidence, _ = _authority()
    model = committee()
    inputs = {
        "security_snapshot": {"items": [], "padding": "x" * padding},
        "financial_facts": {"items": []},
    }
    model._inputs = AsyncMock(return_value=inputs)
    receipt = await preflight_capacity_pool(
        [CapacityPoolMember(model, request, evidence)],
        expected_instrument_ids=(request.instrument_id,),
    )
    assert receipt["model_calls"] == 0
    assert model.usage.provider_calls == 0
    assert model._capacity_pool_manifest is not None


@pytest.mark.asyncio
async def test_pool_still_rejects_projected_wire_one_byte_over_limit():
    request, evidence, _ = _authority()
    model = committee()
    inputs = {
        "security_snapshot": {"items": [], "padding": "x" * 61442},
        "financial_facts": {"items": []},
    }
    model._inputs = AsyncMock(return_value=inputs)
    previous = [
        dict(
            claim_id=f"{role}:{i}",
            role=role,
            vote="abstain",
            text="\x00" * 400,
            citations=[0] * 3,
        )
        for role in ROLE_ORDER[:7]
        for i in range(2)
    ]
    payload, _ = model.build_stage_request(
        stage="synthesis",
        request=request,
        evidence_refs=evidence,
        frozen_inputs=inputs,
        previous=previous,
        financial_table_candidate=True,
    )
    assert len(codex_request_wire(payload).encode()) == 100480
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_WIRE_EXCEEDED$"):
        await preflight_capacity_pool(
            [CapacityPoolMember(model, request, evidence)],
            expected_instrument_ids=(request.instrument_id,),
        )
    assert model._capacity_pool_manifest is None
    assert model.usage.provider_calls == 0


@pytest.mark.parametrize("evidence_count", [1, 11])
def test_projected_envelope_covers_prior_row_counts_unicode_and_citation_width(evidence_count):
    request, evidence, _ = _authority()
    refs = tuple(
        evidence[0].model_copy(update={"object_id": f"source-{i}"}) for i in range(evidence_count)
    )
    model = committee()
    inputs = {"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
    envelope = candidate_capacity_envelope(
        model, request=request, evidence_refs=refs, inputs=inputs
    )
    for stage, roles in (
        ("research", ()),
        ("critique", ROLE_ORDER[:5]),
        ("synthesis", ROLE_ORDER[:7]),
    ):
        upper = next(row["wire_upper_bytes"] for row in envelope["stages"] if row["stage"] == stage)
        for count in range(2 * len(roles) + 1):
            for chars in ("", "证" * 400, "𠀀" * 400, "\x00" * 400, '"\\' * 200):
                for citation_count in (0, 1, 3):
                    previous = [
                        {
                            "claim_id": f"{role}:{i}",
                            "role": role,
                            "vote": "support",
                            "text": chars,
                            "citations": [evidence_count - 1] * citation_count,
                        }
                        for role in roles
                        for i in range(2)
                    ][:count]
                    payload, _ = model.build_stage_request(
                        stage=stage,
                        request=request,
                        evidence_refs=refs,
                        frozen_inputs=inputs,
                        previous=previous,
                        financial_table_candidate=True,
                    )
                    assert len(codex_request_wire(payload).encode()) <= upper
