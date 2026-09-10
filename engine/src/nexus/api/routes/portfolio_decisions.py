"""Authenticated create/read routes for bounded portfolio decision runs."""

import hashlib
from datetime import UTC, datetime
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse
from nexus.agents.budget import BudgetExhausted
from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioProjectionV1,
    PortfolioDecisionInputV1,
    PortfolioResearchIntentV1,
    canonical_portfolio_decision_hash,
    validate_portfolio_intent,
)
from nexus.agents.portfolio_decision_persistence import admit_portfolio_decision_run
from nexus.agents.portfolio_decision_tools import PortfolioCitationAuthority
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.portfolio_decisions import (
    PortfolioDecisionCitationResponse,
    PortfolioDecisionCreateRequest,
    PortfolioDecisionErrorCode,
    PortfolioDecisionOutputResponse,
    PortfolioDecisionRunEnvelope,
    PortfolioDecisionRunResponse,
    PortfolioDecisionStatus,
)
from nexus.domain.decimal import decimal_to_string
from nexus.domain.time import require_utc
from nexus.persistence.models.agents import (
    PortfolioDecisionOutputRecord,
    PortfolioDecisionRunRecord,
)
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import AgentRuntimeMode, EngineSettings
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_MAX_BODY_BYTES = 64 * 1024
_IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 404, 409, 422)
}
_CREATE_RESPONSES = {
    202: {
        "description": "Portfolio decision run accepted or replayed.",
        "model": PortfolioDecisionRunEnvelope,
        "headers": {"Location": {"schema": {"type": "string"}}},
    },
    **_ERROR_RESPONSES,
}
_CREATE_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/PortfolioDecisionCreateRequest"}
            }
        },
    }
}


async def _bounded_create_request(request: Request) -> PortfolioDecisionCreateRequest:
    content_type = request.headers.get("Content-Type")
    if content_type is None or content_type.split(";", 1)[0].strip().lower() != "application/json":
        raise NexusError(ErrorCode.VALIDATION_ERROR, "Request is invalid.", status_code=422)
    content_length = request.headers.get("Content-Length")
    if content_length is not None and (
        not content_length.isascii()
        or not content_length.isdecimal()
        or content_length != "0"
        and content_length.startswith("0")
        or int(content_length) > _MAX_BODY_BYTES
    ):
        raise NexusError(ErrorCode.VALIDATION_ERROR, "Request is invalid.", status_code=422)
    body = bytearray()
    async for chunk in request.stream():
        if len(chunk) > _MAX_BODY_BYTES - len(body):
            raise NexusError(ErrorCode.VALIDATION_ERROR, "Request is invalid.", status_code=422)
        body.extend(chunk)
    raw_body = bytes(body)
    setattr(request, "_body", raw_body)
    try:
        return PortfolioDecisionCreateRequest.model_validate_json(raw_body)
    except (TypeError, ValueError, ValidationError) as error:
        raise NexusError(
            ErrorCode.PORTFOLIO_DECISION_INPUT_UNSAFE,
            "Portfolio decision request is unsafe.",
            status_code=422,
        ) from error


@router.post(
    "/internal/v1/portfolio-decision-runs",
    response_model=PortfolioDecisionRunEnvelope,
    status_code=202,
    operation_id="createPortfolioDecisionRun",
    responses=_CREATE_RESPONSES,
    openapi_extra=_CREATE_REQUEST_BODY,
)
async def create_portfolio_decision_run(
    request: Request,
    idempotency_key: _IdempotencyKey,
    payload: PortfolioDecisionCreateRequest = Depends(_bounded_create_request),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    settings: EngineSettings = Depends(get_settings),
    session: AsyncSession = Depends(transactional_session),
) -> JSONResponse:
    """Admit one immutable run without executing model work in the API process."""
    raw_body = await request.body()
    now = _utc_now(request)
    required_model_id, required_model_version = _runtime_model_authority(settings)
    try:
        admission = await admit_portfolio_decision_run(
            session,
            actor_id=actor.actor_id,
            route="/internal/v1/portfolio-decision-runs",
            idempotency_key=idempotency_key,
            request_hash=hashlib.sha256(raw_body).hexdigest(),
            request=payload.scenario_request,
            expected_scenario_result_hash=payload.expected_scenario_result_hash,
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=settings.agent_daily_budget_usd,
            now=now,
            required_model_id=required_model_id,
            required_model_version=required_model_version,
        )
    except BudgetExhausted as error:
        raise NexusError(
            ErrorCode.PORTFOLIO_DECISION_BUDGET_EXHAUSTED,
            "Portfolio decision budget is exhausted.",
            status_code=409,
        ) from error
    except IdempotencyConflict as error:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "Idempotency key has a different request body.",
            status_code=409,
        ) from error
    except ValueError as error:
        raise NexusError(
            ErrorCode.PORTFOLIO_DECISION_AUTHORITY_MISMATCH,
            "Portfolio decision authority does not match the request.",
            status_code=409,
        ) from error
    row = (
        await session.execute(
            select(PortfolioDecisionRunRecord, PortfolioDecisionOutputRecord)
            .outerjoin(
                PortfolioDecisionOutputRecord,
                PortfolioDecisionOutputRecord.run_id == PortfolioDecisionRunRecord.id,
            )
            .where(
                PortfolioDecisionRunRecord.id == admission.run.id,
                PortfolioDecisionRunRecord.actor_id == actor.actor_id,
            )
            .execution_options(populate_existing=True)
        )
    ).one()
    run, output = row
    envelope = PortfolioDecisionRunEnvelope(
        data=_response(run, output),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=run.decision_time),
    )
    return JSONResponse(
        status_code=202,
        content=envelope.model_dump(mode="json"),
        headers={"Location": f"/api/v1/portfolio-decision-runs/{run.id}"},
    )


def _runtime_model_authority(settings: EngineSettings) -> tuple[str | None, str | None]:
    if settings.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK:
        from nexus.agents.codex_sdk import MODEL_ID
        from nexus.agents.codex_versions import PORTFOLIO_DECISION_CONFIG_VERSION

        return MODEL_ID, PORTFOLIO_DECISION_CONFIG_VERSION
    if settings.agent_runtime_mode is AgentRuntimeMode.RESPONSES:
        from nexus.ops.openai_local import _CONFIG, MODEL_ID

        return MODEL_ID, str(_CONFIG["version"])
    return None, None


@router.get(
    "/internal/v1/portfolio-decision-runs/{run_id}",
    response_model=PortfolioDecisionRunEnvelope,
    operation_id="getPortfolioDecisionRun",
    responses=_ERROR_RESPONSES,
)
async def get_portfolio_decision_run(
    run_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> PortfolioDecisionRunEnvelope:
    """Return only a run owned by the authenticated local actor."""
    row = (
        await session.execute(
            select(PortfolioDecisionRunRecord, PortfolioDecisionOutputRecord)
            .outerjoin(
                PortfolioDecisionOutputRecord,
                PortfolioDecisionOutputRecord.run_id == PortfolioDecisionRunRecord.id,
            )
            .where(
                PortfolioDecisionRunRecord.id == run_id,
                PortfolioDecisionRunRecord.actor_id == actor.actor_id,
            )
        )
    ).one_or_none()
    if row is None:
        raise NexusError(
            ErrorCode.NOT_FOUND, "Portfolio decision run was not found.", status_code=404
        )
    run, output = row
    try:
        data = _response(run, output)
    except (TypeError, ValueError, ValidationError) as error:
        raise NexusError(
            ErrorCode.INTERNAL_ERROR,
            "The request could not be completed.",
            status_code=500,
        ) from error
    return PortfolioDecisionRunEnvelope(
        data=data,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=run.decision_time),
    )


def _response(
    run: PortfolioDecisionRunRecord,
    output: PortfolioDecisionOutputRecord | None,
) -> PortfolioDecisionRunResponse:
    frozen = PortfolioDecisionInputV1.model_validate(run.frozen_input)
    if (
        canonical_portfolio_decision_hash(frozen) != run.input_hash
        or frozen.portfolio_id != run.portfolio_id
        or frozen.portfolio_snapshot_id != run.portfolio_snapshot_id
        or frozen.diagnostic_report_id != run.diagnostic_report_id
        or frozen.decision_time != run.decision_time
        or frozen.evidence_hash != run.evidence_hash
    ):
        raise ValueError("portfolio decision run authority is invalid")
    citation_authority = PortfolioCitationAuthority.from_manifest(
        run_id=run.id,
        manifest=run.evidence_manifest,
        evidence_hash=run.evidence_hash,
        decision_time=run.decision_time,
    )
    citations: tuple[PortfolioDecisionCitationResponse, ...] = ()
    projected_output = None
    projection = None
    if (run.tool_result_hash is None) != (run.tool_projection is None):
        raise ValueError("portfolio decision tool authority is incomplete")
    if run.tool_projection is not None:
        projection = AgentScenarioProjectionV1.model_validate(run.tool_projection)
        if projection.result_hash != run.tool_result_hash:
            raise ValueError("portfolio decision tool authority is invalid")
    terminal = run.status in {"completed", "abstained", "failed"}
    if terminal != (output is not None):
        raise ValueError("portfolio decision terminal state is incomplete")
    if output is not None:
        intent = (
            None
            if output.intent is None
            else PortfolioResearchIntentV1.model_validate(output.intent)
        )
        raw_error_code = output.details.get("error_code")
        error_code = (
            None if raw_error_code is None else cast(PortfolioDecisionErrorCode, raw_error_code)
        )
        expected_content = {
            "status": output.status,
            "scenario_result_hash": output.scenario_result_hash,
            "intent": output.intent,
            "details": output.details,
        }
        if (
            output.status != run.status
            or output.content_hash != canonical_portfolio_decision_hash(expected_content)
            or (output.status == "completed") != (intent is not None)
            or (
                output.status == "completed"
                and output.scenario_result_hash != frozen.expected_scenario_result_hash
            )
            or (
                intent is not None
                and intent.scenario_result_hash != frozen.expected_scenario_result_hash
            )
        ):
            raise ValueError("portfolio decision output authority is invalid")
        if intent is not None:
            if intent.run_id != run.id or projection is None:
                raise ValueError("portfolio decision intent authority is invalid")
            validate_portfolio_intent(
                intent,
                frozen=frozen,
                projection=projection,
                allowed_citation_ids=frozenset(citation_authority.citation_ids),
            )
            citations = tuple(
                PortfolioDecisionCitationResponse(
                    citation_id=citation_id,
                    evidence=citation_authority.resolve(citation_id),
                )
                for citation_id in intent.citation_ids
            )
        projected_output = PortfolioDecisionOutputResponse(
            status=cast(Any, output.status),
            scenario_result_hash=output.scenario_result_hash,
            intent=intent,
            error_code=error_code,
        )
    return PortfolioDecisionRunResponse(
        run_id=run.id,
        portfolio_id=run.portfolio_id,
        portfolio_snapshot_id=run.portfolio_snapshot_id,
        diagnostic_report_id=run.diagnostic_report_id,
        decision_time=run.decision_time,
        scenario_request_hash=frozen.scenario_request_hash,
        expected_scenario_result_hash=frozen.expected_scenario_result_hash,
        objective=frozen.objective,
        status=cast(PortfolioDecisionStatus, run.status),
        reserved_cost_usd=decimal_to_string(run.reserved_cost_usd),
        settled_cost_usd=(
            None if run.settled_cost_usd is None else decimal_to_string(run.settled_cost_usd)
        ),
        input_tokens=run.input_tokens,
        output_tokens=run.output_tokens,
        output=projected_output,
        citations=citations,
    )


def _utc_now(request: Request) -> datetime:
    clock = getattr(request.app.state, "utc_clock", None)
    return datetime.now(UTC) if clock is None else require_utc(clock())
