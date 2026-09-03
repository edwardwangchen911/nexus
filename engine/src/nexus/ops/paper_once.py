"""Explicit one-shot internal-paper admission or advancement from persisted facts."""

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime
from typing import Literal, Never, get_args
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.paper.errors import (
    PaperBatchError,
    PaperBatchErrorCode,
    PaperExecutionError,
    PaperExecutionErrorCode,
)
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import PaperOrderRecord
from nexus.settings import EngineSettings

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_SAFE_CODES = {
    "PAPER_ONCE_REQUEST_INVALID",
    "PAPER_ONCE_DATABASE_UNCONFIGURED",
    "PAPER_ONCE_DATABASE_NOT_LOCAL",
    "PAPER_ONCE_TIME_IN_FUTURE",
    "PAPER_ONCE_ACCOUNT_MISMATCH",
    "PAPER_ONCE_RESULT_UNKNOWN",
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


async def run_once(
    settings: EngineSettings, request: PaperAdmitRequest | PaperAdvanceRequest
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
    clock = request.request_time if isinstance(request, PaperAdmitRequest) else request.evaluated_at
    if clock > datetime.now(UTC):
        raise PaperOnceError("PAPER_ONCE_TIME_IN_FUTURE")
    try:
        engine = create_async_engine(settings.database_url)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with factory() as session, session.begin():
                if isinstance(request, PaperAdmitRequest):
                    batch = await create_paper_order_batch(
                        session,
                        account_id=request.account_id,
                        strategy_decision_id=request.strategy_decision_id,
                        idempotency_key=request.idempotency_key,
                        request_time=request.request_time,
                    )
                    output: dict[str, object] = {
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
        request = (
            PaperAdmitRequest.model_validate(args)
            if args["action"] == "admit"
            else PaperAdvanceRequest.model_validate(args)
        )
    except (PaperOnceError, ValidationError):
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
