"""Explicit one-shot internal-paper admission or advancement from persisted facts."""

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime
from typing import Literal, Never, get_args
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.paper.errors import (
    PaperBatchError,
    PaperBatchErrorCode,
    PaperExecutionError,
    PaperExecutionErrorCode,
)
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.paper.projection import load_paper_projection
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import PaperAccountRecord, PaperOrderRecord
from nexus.risk.authority import prepare_risk_market_snapshot
from nexus.settings import EngineSettings
from nexus.strategy.rank_checkpoint import (
    RankCheckpointAuthority,
    evaluate_and_persist_rank_checkpoint,
)
from nexus.strategy.snapshot_reader import (
    FrozenStrategyReferences,
    StrategySnapshotUnavailable,
    load_unranked_strategy_features,
    require_forward_adjusted_authority,
)

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_SAFE_CODES = {
    "PAPER_ONCE_REQUEST_INVALID",
    "PAPER_ONCE_DATABASE_UNCONFIGURED",
    "PAPER_ONCE_DATABASE_NOT_LOCAL",
    "PAPER_ONCE_TIME_IN_FUTURE",
    "PAPER_ONCE_ACCOUNT_MISMATCH",
    "PAPER_ONCE_RESULT_UNKNOWN",
    "PAPER_FORWARD_INPUT_UNAVAILABLE",
    "PAPER_FORWARD_STRATEGY_MISMATCH",
    "PAPER_FORWARD_PRICE_BINDING_MISMATCH",
    "STRATEGY_SNAPSHOT_ADJUSTED_AUTHORITY_UNAVAILABLE",
    "STRATEGY_SNAPSHOT_FACT_NOT_VISIBLE",
    "STRATEGY_SNAPSHOT_FEATURES_UNAVAILABLE",
    "STRATEGY_SNAPSHOT_INSTRUMENT_MISSING",
    "STRATEGY_SNAPSHOT_INSTRUMENT_UNSUPPORTED",
    "STRATEGY_SNAPSHOT_PRICE_COVERAGE_MISMATCH",
    "STRATEGY_SNAPSHOT_PRICE_HASH_MISMATCH",
    "STRATEGY_SNAPSHOT_PRICE_PROTOCOL_MISMATCH",
    "STRATEGY_SNAPSHOT_PRICE_REFERENCE_MISSING",
    "STRATEGY_SNAPSHOT_PRICE_UNITS_UNAVAILABLE",
    "STRATEGY_SNAPSHOT_ROOT_REFERENCE_MISSING",
    "STRATEGY_SNAPSHOT_ROOT_UNAVAILABLE",
    "STRATEGY_SNAPSHOT_SECTOR_REFERENCE_MISMATCH",
    "STRATEGY_SNAPSHOT_SOURCE_POLICY_UNAVAILABLE",
    *get_args(PaperBatchErrorCode),
    *get_args(PaperExecutionErrorCode),
}


class PaperOnceError(ValueError):
    """Only fixed codes may leave the local command."""


class _Request(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    account_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")

    @field_validator("request_time", "evaluated_at", check_fields=False)
    @classmethod
    def utc_clock(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("explicit UTC time is required")
        return value


class PaperAdmitRequest(_Request):
    action: Literal["admit"]
    strategy_decision_id: UUID
    request_time: datetime


class PaperAdvanceRequest(_Request):
    action: Literal["advance"]
    order_id: UUID
    evaluated_at: datetime


class PaperForwardRequest(_Request):
    action: Literal["forward"]
    request_time: datetime
    references: FrozenStrategyReferences
    strategy_definition_id: UUID
    champion_generation: int = Field(ge=1)

    @model_validator(mode="after")
    def _ordered(self) -> "PaperForwardRequest":
        if self.references.market.decision_time > self.request_time:
            raise ValueError("forward request precedes its frozen cutoff")
        if self.references.market.quality_report_id is None:
            raise ValueError("forward request requires a quality report")
        return self


async def _forward(session: AsyncSession, request: PaperForwardRequest) -> dict[str, object]:
    # Fail on the actual frozen source contract; never elevate an all label or fixture.
    require_forward_adjusted_authority(request.references)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1818))"),
        {"scope": f"internal-paper:{request.account_id}"},
    )
    account = await session.scalar(
        select(PaperAccountRecord)
        .where(PaperAccountRecord.id == request.account_id)
        .with_for_update()
    )
    if account is None:
        raise PaperOnceError("PAPER_ONCE_ACCOUNT_MISMATCH")
    identity = request.references.market
    quality_report_id = identity.quality_report_id
    if quality_report_id is None:
        raise PaperOnceError("PAPER_FORWARD_INPUT_UNAVAILABLE")
    projection = await load_paper_projection(
        session, account_id=request.account_id, as_of=identity.decision_time
    )
    loaded = await load_unranked_strategy_features(
        session,
        references=request.references,
        current_positions=tuple(
            item.instrument_id for item in projection.positions if item.quantity > 0
        ),
    )
    market = await prepare_risk_market_snapshot(
        session,
        authority=identity,
        required_instrument_ids=tuple(item.instrument_id for item in loaded.snapshot.instruments),
    )
    if {item.instrument_id: item.authority_id for item in market.prices} != dict(
        loaded.raw_authorities
    ):
        raise PaperOnceError("PAPER_FORWARD_PRICE_BINDING_MISMATCH")
    checkpoint = await evaluate_and_persist_rank_checkpoint(
        session,
        snapshot=loaded.snapshot,
        authority=RankCheckpointAuthority(
            account_id=request.account_id,
            portfolio_id=account.portfolio_id,
            decision_time=identity.decision_time,
            execution_session=identity.execution_session,
            source_policy_id=identity.source_policy_id,
            calendar_authority_id=identity.calendar_authority_id,
            universe_snapshot_id=identity.universe_snapshot_id,
            quality_report_id=quality_report_id,
            portfolio_projection_hash=projection.projection_hash,
            authority_hash=market.authority_hash,
            input_references=request.references,
        ),
    )
    if checkpoint is None:
        return {"status": "not_week_end"}
    decision = checkpoint.forward.decision
    if (
        decision.strategy_definition_id != request.strategy_definition_id
        or decision.champion_generation != request.champion_generation
    ):
        raise PaperOnceError("PAPER_FORWARD_STRATEGY_MISMATCH")
    batch = await create_paper_order_batch(
        session,
        account_id=request.account_id,
        strategy_decision_id=checkpoint.forward.decision_id,
        idempotency_key=request.idempotency_key,
        request_time=request.request_time,
    )
    return {
        "status": "batch_evaluated",
        "strategy_decision_id": str(checkpoint.forward.decision_id),
        "checkpoint_hash": checkpoint.checkpoint_hash,
        "batch_id": str(batch.batch_id),
        "order_ids": [str(identifier) for identifier in batch.order_ids],
    }


async def run_once(
    settings: EngineSettings, request: PaperAdmitRequest | PaperAdvanceRequest | PaperForwardRequest
) -> dict[str, object]:
    """Commit one service operation; never collect, schedule, seed, or retry."""
    if not settings.database_url:
        raise PaperOnceError("PAPER_ONCE_DATABASE_UNCONFIGURED")
    try:
        url = make_url(settings.database_url)
    except Exception as error:
        raise PaperOnceError("PAPER_ONCE_DATABASE_NOT_LOCAL") from error
    if (
        url.drivername != "postgresql+asyncpg"
        or url.host not in _LOCAL_HOSTS
        or bool(set(url.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise PaperOnceError("PAPER_ONCE_DATABASE_NOT_LOCAL")
    clock = (
        request.request_time
        if isinstance(request, PaperAdmitRequest | PaperForwardRequest)
        else request.evaluated_at
    )
    if clock > datetime.now(UTC):
        raise PaperOnceError("PAPER_ONCE_TIME_IN_FUTURE")
    try:
        engine = create_async_engine(settings.database_url)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session, session.begin():
                if isinstance(request, PaperForwardRequest):
                    output = await _forward(session, request)
                elif isinstance(request, PaperAdmitRequest):
                    batch = await create_paper_order_batch(
                        session,
                        account_id=request.account_id,
                        strategy_decision_id=request.strategy_decision_id,
                        idempotency_key=request.idempotency_key,
                        request_time=request.request_time,
                    )
                    output = {
                        "status": "batch_evaluated",
                        "batch_id": str(batch.batch_id),
                        "order_ids": [str(identifier) for identifier in batch.order_ids],
                    }
                else:
                    order = await session.get(PaperOrderRecord, request.order_id)
                    if order is None or order.account_id != request.account_id:
                        raise PaperOnceError("PAPER_ONCE_ACCOUNT_MISMATCH")
                    result = await advance_internal_paper_order(
                        session,
                        order_id=request.order_id,
                        evaluated_at=request.evaluated_at,
                        idempotency_key=request.idempotency_key,
                    )
                    output = {
                        "status": result.status,
                        "action": result.action,
                        "order_id": str(result.order_id),
                    }
            return output
        finally:
            await engine.dispose()
    except PaperOnceError:
        raise
    except (PaperBatchError, PaperExecutionError) as error:
        raise PaperOnceError(error.code) from error
    except StrategySnapshotUnavailable as error:
        code = str(error)
        raise PaperOnceError(
            code if code in _SAFE_CODES else "PAPER_FORWARD_INPUT_UNAVAILABLE"
        ) from error
    except ValueError as error:
        code = (
            "PAPER_FORWARD_INPUT_UNAVAILABLE"
            if isinstance(request, PaperForwardRequest)
            else "PAPER_ONCE_RESULT_UNKNOWN"
        )
        raise PaperOnceError(code) from error
    except Exception as error:
        # A transport/commit interruption does not prove the operation failed.
        # Existing service idempotency permits resolving the exact original request.
        raise PaperOnceError("PAPER_ONCE_RESULT_UNKNOWN") from error


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise PaperOnceError("PAPER_ONCE_REQUEST_INVALID")


def main() -> None:
    parser = _Parser(description="从已存事实执行一次内部模拟操作，不采集、不调度、不发送券商订单。")
    commands = parser.add_subparsers(dest="action", required=True)
    admit = commands.add_parser("admit", help="用已存策略决策创建一次风控订单批次")
    forward = commands.add_parser("forward", help="从显式冻结资料准备一次决策并调用内部风控准入")
    forward.add_argument("--request-file", required=True)
    advance = commands.add_parser("advance", help="在已到达的成交窗口推进一笔已有订单")
    for command in (admit, advance):
        command.add_argument("--account-id", required=True)
        command.add_argument("--idempotency-key", required=True)
    admit.add_argument("--strategy-decision-id", required=True)
    admit.add_argument("--request-time", required=True)
    advance.add_argument("--order-id", required=True)
    advance.add_argument("--evaluated-at", required=True)
    try:
        args = vars(parser.parse_args())
        if args["action"] == "forward":
            with open(args["request_file"], encoding="utf-8") as stream:
                payload = stream.read(65537)
            if len(payload) > 65536:
                raise PaperOnceError("PAPER_ONCE_REQUEST_INVALID")
            request: PaperAdmitRequest | PaperAdvanceRequest | PaperForwardRequest = (
                PaperForwardRequest.model_validate_json(payload)
            )
        else:
            request = (
                PaperAdmitRequest.model_validate(args)
                if args["action"] == "admit"
                else PaperAdvanceRequest.model_validate(args)
            )
    except (PaperOnceError, ValidationError, OSError, UnicodeError):
        sys.stderr.write(
            json.dumps({"status": "failed", "code": "PAPER_ONCE_REQUEST_INVALID"}) + "\n"
        )
        raise SystemExit(1) from None
    try:
        output = asyncio.run(run_once(EngineSettings(), request))
    except Exception as error:
        code = str(error) if isinstance(error, PaperOnceError) else "PAPER_ONCE_RESULT_UNKNOWN"
        if code not in _SAFE_CODES:
            code = "PAPER_ONCE_RESULT_UNKNOWN"
        status = "unknown" if code == "PAPER_ONCE_RESULT_UNKNOWN" else "failed"
        sys.stderr.write(json.dumps({"status": status, "code": code}) + "\n")
        raise SystemExit(1) from None
    sys.stdout.write(json.dumps(output, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
