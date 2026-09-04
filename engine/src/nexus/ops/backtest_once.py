"""Explicit local baseline execution from existing frozen authorities, without collection."""

import argparse
import asyncio
import json
import os
import stat
import sys
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal, Never
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.backtest.authority_loader import load_frozen_point_in_time_dataset
from nexus.backtest.bundle import BundleRequest, prepare_bundle
from nexus.backtest.cost_policy import load_backtest_cost_policy_v1
from nexus.backtest.costs import CostModel
from nexus.backtest.engine import BacktestProtocol, run_backtest
from nexus.backtest.persistence import (
    BacktestPersistenceAuthority,
    _advisory_lock,
    _lock_authority_roots,
    _validate_strategy_definition,
    persist_backtest_result,
)
from nexus.domain.time import require_utc
from nexus.persistence.models.backtest import (
    BacktestAuthorityBundleRecord,
    BacktestRunRecord,
    StrategyDefinitionRecord,
)
from nexus.settings import EngineSettings
from nexus.strategy.dsl import baseline_feature_registry, baseline_v1_document, compile_strategy


class BacktestOnceError(ValueError):
    """Only fixed non-sensitive codes may leave the command."""


class BacktestOnceRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    strategy_version: Literal["baseline_v1"]
    authority_bundle_id: UUID
    data_snapshot_id: UUID
    source_policy_id: UUID
    cost_policy_id: UUID
    start: date
    end: date
    initial_nav: Decimal
    as_of: datetime

    @field_validator("initial_nav", mode="before")
    @classmethod
    def reject_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise ValueError("initial_nav requires a decimal string")
        return value

    @model_validator(mode="after")
    def validate_request(self) -> "BacktestOnceRequest":
        require_utc(self.as_of)
        if self.start > self.end or not self.initial_nav.is_finite() or self.initial_nav <= 0:
            raise ValueError("ordered dates and positive finite initial_nav are required")
        return self


async def run_once(settings: EngineSettings, request: BacktestOnceRequest) -> dict[str, object]:
    """Commit one whole run or roll back; never create or repair market authorities."""
    if settings.database_url is None:
        raise BacktestOnceError("BACKTEST_DATABASE_UNCONFIGURED")
    if request.as_of > datetime.now(UTC):
        raise BacktestOnceError("BACKTEST_CUTOFF_IN_FUTURE")
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session, session.begin():
            bundle = await session.get(BacktestAuthorityBundleRecord, request.authority_bundle_id)
            if bundle is None:
                raise BacktestOnceError("BACKTEST_FROZEN_AUTHORITY_MISSING")
            if (bundle.data_snapshot_id, bundle.source_policy_id, bundle.cost_policy_id) != (
                request.data_snapshot_id,
                request.source_policy_id,
                request.cost_policy_id,
            ):
                raise BacktestOnceError("BACKTEST_AUTHORITY_BINDING_MISMATCH")
            cost_policy = await load_backtest_cost_policy_v1(session)
            if cost_policy.id != request.cost_policy_id:
                raise BacktestOnceError("BACKTEST_COST_POLICY_MISMATCH")
            protocol = BacktestProtocol(
                start=request.start,
                end=request.end,
                initial_nav=request.initial_nav,
                cost_model=CostModel(
                    **{
                        name: Decimal(str(cost_policy.parameters[name]))
                        for name in ("commission_bps", "spread_bps", "slippage_bps")
                    }
                ),
            )
            data = await load_frozen_point_in_time_dataset(
                session, bundle.id, protocol, request.as_of
            )
            strategy = compile_strategy(baseline_v1_document(), baseline_feature_registry())
            definition = await session.scalar(
                select(StrategyDefinitionRecord).where(
                    StrategyDefinitionRecord.strategy_version == request.strategy_version
                )
            )
            if definition is None or definition.created_at > request.as_of:
                raise BacktestOnceError("BACKTEST_STRATEGY_UNAVAILABLE")
            _validate_strategy_definition(definition, strategy, request.as_of)
            result = run_backtest(strategy, data, protocol)
            authority = BacktestPersistenceAuthority(
                authority_bundle_id=bundle.id,
                data_snapshot_id=bundle.data_snapshot_id,
                source_policy_id=bundle.source_policy_id,
                cost_policy_id=bundle.cost_policy_id,
                completed_at=datetime.now(UTC),
                data_cutoff_at=bundle.completed_at,
            )
            # Use the writer's lock order before resolving replay completion time.
            # The writer repeats these transaction-local locks and all integrity checks.
            await _lock_authority_roots(session, data, authority)
            await _advisory_lock(session, f"strategy:{strategy.strategy_version}")
            await _advisory_lock(session, f"backtest:{result.result_hash}")
            existing = await session.scalar(
                select(BacktestRunRecord).where(BacktestRunRecord.result_hash == result.result_hash)
            )
            if existing is not None:
                authority = replace(authority, completed_at=existing.completed_at)
            record = await persist_backtest_result(
                session, strategy, data, protocol, result, authority
            )
            output: dict[str, object] = {
                "status": "BACKTEST_REPLAYED" if existing is not None else "BACKTEST_COMPLETED",
                "run_id": str(record.id),
                "strategy_version": strategy.strategy_version,
                "result_hash": record.result_hash,
                "authority_hash": record.authority_hash,
                "data_hash": record.data_hash,
                "data_snapshot_id": str(record.data_snapshot_id),
                "benchmarks": [item.comparison_key for item in result.comparisons],
                "cost_policy_version": cost_policy.version,
                "cost_basis": "historical_assumption_not_broker_fee",
                "history_requirement_met": record.history_requirement_met,
                "promotable": record.promotable,
                "execution_locked": True,
                "read_path": "/internal/v1/strategies",
            }
        return output
    except BacktestOnceError:
        raise
    except (LookupError, TypeError, ValueError) as error:
        raise BacktestOnceError("BACKTEST_AUTHORITY_OR_DATA_INCOMPLETE") from error
    finally:
        await engine.dispose()


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise BacktestOnceError("BACKTEST_REQUEST_INVALID")


def _prepare_request(path: str) -> BundleRequest:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            raise BacktestOnceError("BACKTEST_REQUEST_INVALID")
        raw = handle.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise BacktestOnceError("BACKTEST_REQUEST_INVALID")
    return BundleRequest.model_validate_json(raw)


async def prepare_and_run(settings: EngineSettings, request: BundleRequest) -> dict[str, object]:
    """Commit the legitimate input package, then invoke the unchanged execution path."""
    if settings.database_url is None:
        raise BacktestOnceError("BACKTEST_DATABASE_UNCONFIGURED")
    engine = create_async_engine(settings.database_url, pool_pre_ping=True)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory.begin() as session:
            prepared = await prepare_bundle(session, request)
    finally:
        await engine.dispose()
    # New package materialization is not backdated to a caller-supplied as_of.
    return await run_once(
        settings,
        BacktestOnceRequest(
            strategy_version=request.strategy_version,
            authority_bundle_id=prepared.authority_bundle_id,
            data_snapshot_id=prepared.data_snapshot_id,
            source_policy_id=request.source_policy_id,
            cost_policy_id=request.cost_policy_id,
            start=request.start,
            end=request.end,
            initial_nav=request.initial_nav,
            as_of=datetime.now(UTC),
        ),
    )


def main() -> None:
    parser = _Parser(
        description="运行一次 baseline 历史回测；只读已有冻结数据，不采集、不运行模型、不交易。"
    )
    parser.add_argument("--prepare-request-file")
    parser.add_argument("--strategy-version", choices=["baseline_v1"])
    for name in ("authority-bundle-id", "data-snapshot-id", "source-policy-id", "cost-policy-id"):
        parser.add_argument(f"--{name}")
    for name in ("start", "end", "initial-nav", "as-of"):
        parser.add_argument(f"--{name}")
    parser.epilog = "基准固定为 SPY、同一冻结股票池等权及 baseline；成本使用冻结历史假设。"
    try:
        args = parser.parse_args()
        values = vars(args)
        path = values.pop("prepare_request_file")
        if path is not None:
            if any(value is not None for value in values.values()):
                raise BacktestOnceError("BACKTEST_REQUEST_INVALID")
            prepared_request = _prepare_request(path)
            output = asyncio.run(prepare_and_run(EngineSettings(), prepared_request))
        else:
            if any(value is None for value in values.values()):
                raise BacktestOnceError("BACKTEST_REQUEST_INVALID")
            request = BacktestOnceRequest.model_validate(values)
            output = asyncio.run(run_once(EngineSettings(), request))
    except Exception as error:
        code = str(error) if isinstance(error, BacktestOnceError) else "BACKTEST_RUN_FAILED"
        sys.stderr.write(json.dumps({"status": "failed", "code": code}) + "\n")
        raise SystemExit(1) from None
    sys.stdout.write(json.dumps(output, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
