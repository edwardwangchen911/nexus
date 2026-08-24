"""Typed proposal-read and anchored what-if API contracts."""

import hashlib
import json

import httpx
import pytest
from helpers.optimization_api import seed_readable_group
from helpers.portfolio_api import (
    INSTRUMENT_ID,
    ORDER_INSTRUMENT_ID,
    app,
    headers,
    seed_portfolio_list,
    settings,
)
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_group_get_is_closed_three_way_and_what_if_requires_its_allowed_universe(
    migrated_database_url: str,
) -> None:
    """A crafted browser request must not read or evaluate a foreign proposal universe."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        group_id, proposal_id = await seed_readable_group(session)

    group_path = f"/internal/v1/optimization-proposals/{group_id}"
    what_if_path = "/internal/v1/what-if/evaluate"
    request = {
        "proposal_group_id": str(group_id),
        "proposal_id": str(proposal_id),
        "policy_version": "optimizer_v1",
        "positions": [{"instrument_id": str(INSTRUMENT_ID), "weight": "0.20"}],
    }
    body = json.dumps(request, separators=(",", ":")).encode()
    changed_body = json.dumps(
        {**request, "positions": [{"instrument_id": str(INSTRUMENT_ID), "weight": "0.21"}]},
        separators=(",", ":"),
    ).encode()
    foreign_body = json.dumps(
        {**request, "positions": [{"instrument_id": str(ORDER_INSTRUMENT_ID), "weight": "0.20"}]},
        separators=(",", ":"),
    ).encode()
    invalid_bodies = tuple(
        json.dumps({**request, "positions": positions}, separators=(",", ":")).encode()
        for positions in (
            [],
            [{"instrument_id": str(INSTRUMENT_ID), "weight": "1e-1"}],
            [{"instrument_id": str(INSTRUMENT_ID), "weight": "NaN"}],
            [{"instrument_id": str(INSTRUMENT_ID), "weight": "Infinity"}],
        )
    )
    engine_app = app(migrated_database_url)
    group_operation = engine_app.openapi()["paths"][
        "/internal/v1/optimization-proposals/{group_id}"
    ]["get"]
    what_if_operation = engine_app.openapi()["paths"][what_if_path]["post"]
    admission_operation = engine_app.openapi()["paths"]["/internal/v1/optimization-proposals"][
        "post"
    ]
    lifecycle_operation = engine_app.openapi()[
        "paths"
    ]["/internal/v1/optimization-proposals/{group_id}/shadow"]["post"]
    assert group_operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/OptimizationProposalGroupEnvelope"
    }
    assert what_if_operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/WhatIfEnvelope"
    }
    assert what_if_operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/WhatIfRequest"
    }
    assert admission_operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/ProposalAdmissionRequest"
    }
    assert lifecycle_operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/OptimizationLifecycleRequest"
    }
    assert engine_app.openapi()["components"]["schemas"]["WhatIfRequest"]["properties"][
        "positions"
    ]["minItems"] == 1
    assert "maxItems" not in engine_app.openapi()["components"]["schemas"][
        "OptimizationProposalResponse"
    ]["properties"]["positions"]
    for operation in (what_if_operation, admission_operation):
        header = next(
            parameter
            for parameter in operation["parameters"]
            if parameter["in"] == "header" and parameter["name"] == "Idempotency-Key"
        )
        assert header["required"] is True
    lifecycle_headers = {
        parameter["name"]: parameter
        for parameter in lifecycle_operation["parameters"]
        if parameter["in"] == "header"
    }
    assert lifecycle_headers["Idempotency-Key"]["required"] is True
    assert lifecycle_headers["If-Match"]["required"] is True
    group_parameter = next(
        parameter
        for parameter in lifecycle_operation["parameters"]
        if parameter["in"] == "path" and parameter["name"] == "group_id"
    )
    assert group_parameter["schema"]["format"] == "uuid"
    assert set(admission_operation["responses"]) == {"202", "401", "404", "409", "422"}
    assert admission_operation["responses"]["202"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/OptimizationJobEnvelope"
    }
    assert admission_operation["responses"]["202"]["headers"]["Location"]["schema"] == {
        "type": "string"
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        group_response = await client.get(group_path, headers=headers("GET", group_path))
        request_headers = headers("POST", what_if_path, body)
        request_headers["Idempotency-Key"] = "what-if-read-api"
        result = await client.post(what_if_path, content=body, headers=request_headers)
        replay_headers = headers("POST", what_if_path, body)
        replay_headers["Idempotency-Key"] = "what-if-read-api"
        replay = await client.post(what_if_path, content=body, headers=replay_headers)
        changed_headers = headers("POST", what_if_path, changed_body)
        changed_headers["Idempotency-Key"] = "what-if-read-api"
        changed = await client.post(what_if_path, content=changed_body, headers=changed_headers)
        foreign = await client.post(
            what_if_path,
            content=foreign_body,
            headers=headers("POST", what_if_path, foreign_body),
        )
        invalid = []
        for invalid_body in invalid_bodies:
            invalid.append(
                await client.post(
                    what_if_path,
                    content=invalid_body,
                    headers=headers("POST", what_if_path, invalid_body),
                )
            )

    assert group_response.status_code == 200
    proposals = group_response.json()["data"]["proposals"]
    assert [proposal["variant"] for proposal in proposals] == [
        "hold_current",
        "minimum_change",
        "strategy_target",
    ]
    assert all("comparison" in proposal for proposal in proposals)
    assert group_response.json()["data"]["shadow_eligibility"] == {
        "eligible": False,
        "reasons": ["SELECTION_AUTHORITY_MISSING"],
    }
    assert "account_id" not in group_response.text
    assert result.status_code == 200
    assert result.json()["data"]["proposal_group_id"] == str(group_id)
    assert result.json()["data"]["proposal_id"] == str(proposal_id)
    assert result.json()["data"]["instrument_count"] == 1
    assert str(INSTRUMENT_ID) not in result.text
    assert "0.20" not in result.text
    assert replay.status_code == 200
    assert replay.json() == result.json()
    assert changed.status_code == 409
    assert changed.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert foreign.status_code == 422
    assert foreign.json()["error"]["code"] == "VALIDATION_ERROR"
    assert all(response.status_code == 422 for response in invalid)
    assert all(response.json()["error"]["code"] == "VALIDATION_ERROR" for response in invalid)
    async with factory() as session:
        audits = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "what_if_evaluated")
            )
        )
        audit_count = await session.scalar(select(func.count()).select_from(AuditEvent))
    assert audit_count == 1
    assert audits[0].actor_id == "nexus-test-user"
    assert audits[0].payload == {
        "policy_version": "optimizer_v1",
        "instrument_count": 1,
        "request_hash": hashlib.sha256(body).hexdigest(),
    }


@pytest.mark.asyncio
async def test_group_get_keeps_a_persisted_31_position_solution_readable(
    migrated_database_url: str,
) -> None:
    """The what-if input cap must not truncate or reject a persisted proposal read."""
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        group_id, _ = await seed_readable_group(session, position_count=31)
    path = f"/internal/v1/optimization-proposals/{group_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    assert [len(proposal["positions"]) for proposal in response.json()["data"]["proposals"]] == [
        31,
        31,
        0,
    ]
