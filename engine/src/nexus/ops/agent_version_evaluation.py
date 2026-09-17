"""Local-only CLI for the bounded paired Agent-version evaluation."""

import argparse
import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.fosun_local import load_local_environment
from nexus.paper.agent_version_evaluation import (
    finalize_agent_version_evaluation,
    register_agent_version_evaluation,
)
from nexus.paper.agent_version_models import AgentVersionEvaluationRequest
from nexus.paper.agent_version_runner import (
    advance_agent_version_slot,
    resume_agent_version_slot,
)
from nexus.persistence.models.agent_versions import (
    AgentVersionCyclePairRecord,
    AgentVersionEvaluationDecisionRecord,
    AgentVersionEvaluationRecord,
)
from nexus.settings import EngineSettings


class _Request(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class RegisterRequest(_Request):
    action: Literal["register"]
    instrument_id: UUID
    incumbent_version_source_run_id: UUID
    challenger_prompt_version_id: UUID
    challenger_model_config_id: UUID
    initial_capital: Decimal
    train_slot: tuple[AwareDatetime, ...] = Field(min_length=4, max_length=4)
    oos_slot: tuple[AwareDatetime, ...] = Field(min_length=20, max_length=20)
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")


class SlotRequest(_Request):
    action: Literal["advance", "resume"]
    evaluation_id: UUID
    decision_slot: AwareDatetime


class EvaluationRequest(_Request):
    action: Literal["finalize", "status"]
    evaluation_id: UUID


class StatusResult(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    evaluation_id: UUID
    status: Literal["observing", "PASS", "FAIL", "UNKNOWN"]
    train_completed: int = Field(ge=0, le=4)
    oos_completed: int = Field(ge=0, le=20)
    reason_code: str = Field(min_length=1, max_length=100)
    execution_locked: Literal[True] = True
    broker_write: Literal[False] = False
    live_write: Literal[False] = False


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="nexus-engine-agent-version-evaluation")
    commands = root.add_subparsers(dest="action", required=True)
    register = commands.add_parser("register")
    register.add_argument("--instrument-id", required=True)
    register.add_argument("--incumbent-version-source-run-id", required=True)
    register.add_argument("--challenger-prompt-version-id", required=True)
    register.add_argument("--challenger-model-config-id", required=True)
    register.add_argument("--initial-capital", required=True)
    register.add_argument("--train-slot", action="append", required=True)
    register.add_argument("--oos-slot", action="append", required=True)
    register.add_argument("--idempotency-key", required=True)
    for action in ("advance", "resume"):
        slot = commands.add_parser(action)
        slot.add_argument("--evaluation-id", required=True)
        slot.add_argument("--decision-slot", required=True)
    for action in ("finalize", "status"):
        evaluation = commands.add_parser(action)
        evaluation.add_argument("--evaluation-id", required=True)
    return root


def _request(values: dict[str, object]) -> RegisterRequest | SlotRequest | EvaluationRequest:
    action = values.get("action")
    if action == "register":
        return RegisterRequest.model_validate(values)
    if action in {"advance", "resume"}:
        return SlotRequest.model_validate(values)
    return EvaluationRequest.model_validate(values)


async def execute(
    settings: EngineSettings,
    request: RegisterRequest | SlotRequest | EvaluationRequest,
) -> BaseModel:
    _validate_local_database(settings.database_url)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    now = datetime.now(UTC)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        if request.action == "status":
            async with factory() as session:
                await session.execute(text("SET TRANSACTION READ ONLY"))
                return await _status(session, evaluation_id=request.evaluation_id)
        async with factory.begin() as session:
            if isinstance(request, RegisterRequest):
                return await register_agent_version_evaluation(
                    session,
                    request=AgentVersionEvaluationRequest(
                        instrument_id=request.instrument_id,
                        incumbent_version_source_run_id=(request.incumbent_version_source_run_id),
                        challenger_prompt_version_id=request.challenger_prompt_version_id,
                        challenger_model_config_id=request.challenger_model_config_id,
                        initial_capital=request.initial_capital,
                        train_slots=request.train_slot,
                        oos_slots=request.oos_slot,
                    ),
                    actor_id="local-agent-version-evaluation",
                    idempotency_key=request.idempotency_key,
                    now=now,
                )
            if isinstance(request, SlotRequest):
                if request.action == "advance":
                    return await advance_agent_version_slot(
                        session,
                        evaluation_id=request.evaluation_id,
                        decision_slot_at=request.decision_slot,
                        daily_limit_usd=settings.agent_daily_budget_usd,
                        now=now,
                    )
                return await resume_agent_version_slot(
                    session,
                    evaluation_id=request.evaluation_id,
                    decision_slot_at=request.decision_slot,
                    now=now,
                )
            return await finalize_agent_version_evaluation(
                session,
                evaluation_id=request.evaluation_id,
                now=now,
            )
    finally:
        await engine.dispose()


async def _status(session: AsyncSession, *, evaluation_id: UUID) -> StatusResult:
    evaluation = await session.get(AgentVersionEvaluationRecord, evaluation_id)
    if evaluation is None:
        raise ValueError("AGENT_VERSION_EVALUATION_UNKNOWN")
    decision = await session.scalar(
        select(AgentVersionEvaluationDecisionRecord).where(
            AgentVersionEvaluationDecisionRecord.evaluation_id == evaluation_id
        )
    )
    rows = (
        await session.execute(
            select(
                AgentVersionCyclePairRecord.phase,
                func.count(AgentVersionCyclePairRecord.id),
            )
            .where(AgentVersionCyclePairRecord.evaluation_id == evaluation_id)
            .group_by(AgentVersionCyclePairRecord.phase)
        )
    ).all()
    counts: dict[str, int] = {phase: int(count) for phase, count in rows}
    return StatusResult(
        evaluation_id=evaluation_id,
        status=(
            "observing"
            if decision is None
            else cast(Literal["PASS", "FAIL", "UNKNOWN"], decision.status)
        ),
        train_completed=int(counts.get("train", 0)),
        oos_completed=int(counts.get("oos", 0)),
        reason_code="EVALUATION_OBSERVING" if decision is None else decision.reason_code,
    )


def _emit_error(code: str) -> int:
    print(json.dumps({"error_code": code}, separators=(",", ":"), sort_keys=True))
    return 1


def main(argv: list[str] | None = None) -> int:
    try:
        request = _request(vars(parser().parse_args(argv)))
        environment = load_local_environment(Path.cwd(), {})
        settings = EngineSettings.model_validate(environment)
        result = asyncio.run(execute(settings, request))
    except Exception as error:
        code = str(error)
        if not code.startswith("AGENT_VERSION_") and code not in {
            "AGENT_PAPER_DATABASE_NOT_LOCAL",
            "AGENT_PAPER_DATABASE_UNCONFIGURED",
        }:
            code = "AGENT_VERSION_RESULT_UNKNOWN"
        return _emit_error(code)
    print(json.dumps(result.model_dump(mode="json"), separators=(",", ":"), sort_keys=True))
    return 0
