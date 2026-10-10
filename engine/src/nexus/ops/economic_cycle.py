"""Normal isolated economic registration, start and persisted-state resume."""

import argparse
import asyncio
import json
import os
import stat
from pathlib import Path
from typing import Protocol
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.paper.economic_pair import register_economic_identity


class _PairOperation(Protocol):
    async def __call__(
        self, *, database_url_file: Path, expected_database: str, pair_slot_id: UUID
    ) -> dict[str, object]: ...


def read_economic_database_url(path: Path, *, expected_database: str) -> str:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("ECONOMIC_PAIR_CONFIG_UNSAFE")
        url = stream.read(4097).strip()
    parsed = make_url(url)
    if (
        len(url) > 4096
        or bool(parsed.query)
        or parsed.drivername != "postgresql+asyncpg"
        or parsed.host not in {"localhost", "127.0.0.1", "::1"}
        or parsed.database != expected_database
    ):
        raise ValueError("ECONOMIC_PROTOCOL_DATABASE_MISMATCH")
    return url


async def execute_registration(
    *,
    database_url_file: Path,
    expected_database: str,
    protocol_hash: str,
    version_binding_hash: str,
) -> dict[str, object]:
    url = read_economic_database_url(database_url_file, expected_database=expected_database)
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            identity = await register_economic_identity(
                session,
                protocol_hash=protocol_hash,
                version_binding_hash=version_binding_hash,
                expected_database=expected_database,
            )
            return {
                "status": "registered_not_started",
                "evaluation_id": str(identity.id),
                "protocol_hash": identity.protocol_hash,
                "binding_hash": identity.binding_hash,
                "planned_slots": 24,
                "lane_count": 2,
                "economic_status": "not_evaluated",
                "execution_locked": True,
            }
    finally:
        await engine.dispose()


async def execute_start(
    *,
    database_url_file: Path,
    expected_database: str,
    pair_slot_id: UUID,
) -> dict[str, object]:
    return await _execute_pair(
        database_url_file=database_url_file,
        expected_database=expected_database,
        pair_slot_id=pair_slot_id,
        resume=False,
    )


async def execute_resume(
    *, database_url_file: Path, expected_database: str, pair_slot_id: UUID
) -> dict[str, object]:
    return await _execute_pair(
        database_url_file=database_url_file,
        expected_database=expected_database,
        pair_slot_id=pair_slot_id,
        resume=True,
    )


async def _execute_pair(
    *, database_url_file: Path, expected_database: str, pair_slot_id: UUID, resume: bool
) -> dict[str, object]:
    from nexus.paper.economic_cycle import resume_economic_pair, start_economic_pair

    url = read_economic_database_url(database_url_file, expected_database=expected_database)
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            operation = resume_economic_pair if resume else start_economic_pair
            result = await operation(
                session, pair_slot_id=pair_slot_id, expected_database=expected_database
            )
            return {
                "status": result.status,
                "pair_slot_id": str(result.pair_slot_id),
                "cycle_ids": [str(item) for item in result.cycle_ids],
                "replayed": result.replayed,
                "lanes": [
                    {"lane": lane, "status": status, "code": code}
                    for lane, status, code in result.lane_states
                ],
                "execution_locked": True,
                "economic_status": "not_evaluated",
            }
    finally:
        await engine.dispose()


async def execute_freeze(
    *,
    database_url_file: Path,
    expected_database: str,
    pair_slot_id: UUID,
    next_window: bool = False,
) -> dict[str, object]:
    from nexus.paper.economic_window import freeze_common_window
    from nexus.paper.portfolio_economic_protocol import require_economic_database_target

    url = read_economic_database_url(database_url_file, expected_database=expected_database)
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            await require_economic_database_target(session, expected=expected_database)
            result = await freeze_common_window(
                session, pair_slot_id=pair_slot_id, window_seq=None if next_window else 1
            )
            return {
                "status": result.status,
                "pair_slot_id": str(pair_slot_id),
                "window_id": str(result.window_id) if result.window_id is not None else None,
                "window_start": result.window_start.isoformat() if result.window_start else None,
                "window_end": result.window_end.isoformat() if result.window_end else None,
                "replayed": result.replayed,
                "execution_locked": True,
                "consumption_status": "unavailable",
                "economic_status": "not_evaluated",
            }
    finally:
        await engine.dispose()


async def _latest_sequence(session: AsyncSession, pair_slot_id: UUID) -> int:
    from nexus.persistence.models.economic import EconomicCommonWindowRecord

    seq = await session.scalar(
        select(func.max(EconomicCommonWindowRecord.window_seq)).where(
            EconomicCommonWindowRecord.pair_slot_id == pair_slot_id
        )
    )
    if seq is None:
        raise ValueError("ECONOMIC_WINDOW_REQUIRED")
    return int(seq)


async def execute_freeze_next(
    *, database_url_file: Path, expected_database: str, pair_slot_id: UUID
) -> dict[str, object]:
    return await execute_freeze(
        database_url_file=database_url_file,
        expected_database=expected_database,
        pair_slot_id=pair_slot_id,
        next_window=True,
    )


async def execute_advance(
    *, database_url_file: Path, expected_database: str, pair_slot_id: UUID
) -> dict[str, object]:
    from nexus.paper.economic_execution import advance_economic_pair

    url = read_economic_database_url(database_url_file, expected_database=expected_database)
    engine = create_async_engine(url)
    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions() as session:
            sequence = await _latest_sequence(session, pair_slot_id)
        return await advance_economic_pair(
            async_sessionmaker(engine, expire_on_commit=False),
            pair_slot_id=pair_slot_id,
            window_seq=sequence,
            expected_database=expected_database,
        )
    finally:
        await engine.dispose()


async def execute_continue_plans(
    *, database_url_file: Path, expected_database: str, pair_slot_id: UUID
) -> dict[str, object]:
    from nexus.paper.economic_plan import continue_economic_plans

    url = read_economic_database_url(database_url_file, expected_database=expected_database)
    engine = create_async_engine(url)
    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions.begin() as session:
            return await continue_economic_plans(
                session, pair_slot_id=pair_slot_id, expected_database=expected_database
            )
    finally:
        await engine.dispose()


async def execute_status(
    *, database_url_file: Path, expected_database: str, pair_slot_id: UUID
) -> dict[str, object]:
    from nexus.paper.economic_execution import read_economic_pair_execution

    url = read_economic_database_url(database_url_file, expected_database=expected_database)
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            return await read_economic_pair_execution(
                session,
                pair_slot_id=pair_slot_id,
                window_seq=None,
                expected_database=expected_database,
            )
    finally:
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    register = commands.add_parser(
        "register", help="Register isolated synthetic lanes; creates no Job"
    )
    register.add_argument("--database-url-file", type=Path, required=True)
    register.add_argument("--expected-database", required=True)
    register.add_argument("--protocol-hash", required=True)
    register.add_argument("--version-binding-hash", required=True)
    start = commands.add_parser("start", help="Start the due registered isolated pair")
    start.add_argument("--database-url-file", type=Path, required=True)
    start.add_argument("--expected-database", required=True)
    start.add_argument("--pair-slot-id", type=UUID, required=True)
    resume = commands.add_parser(
        "resume", help="Resume the registered pair from persisted native Job/Run/Output"
    )
    resume.add_argument("--database-url-file", type=Path, required=True)
    resume.add_argument("--expected-database", required=True)
    resume.add_argument("--pair-slot-id", type=UUID, required=True)
    freeze = commands.add_parser("freeze-window", help="Freeze the first DB-owned common window")
    freeze.add_argument("--database-url-file", type=Path, required=True)
    freeze.add_argument("--expected-database", required=True)
    freeze.add_argument("--pair-slot-id", type=UUID, required=True)
    next_window = commands.add_parser(
        "freeze-next-window", help="Freeze the DB-owned next window or read its existing receipt"
    )
    next_window.add_argument("--database-url-file", type=Path, required=True)
    next_window.add_argument("--expected-database", required=True)
    next_window.add_argument("--pair-slot-id", type=UUID, required=True)
    advance = commands.add_parser(
        "advance-window", help="Consume the latest registered common window once"
    )
    advance.add_argument("--database-url-file", type=Path, required=True)
    advance.add_argument("--expected-database", required=True)
    advance.add_argument("--pair-slot-id", type=UUID, required=True)
    continuation = commands.add_parser(
        "continue-plans", help="Create post-sale plans from the pair's completed native fills"
    )
    continuation.add_argument("--database-url-file", type=Path, required=True)
    continuation.add_argument("--expected-database", required=True)
    continuation.add_argument("--pair-slot-id", type=UUID, required=True)
    status = commands.add_parser(
        "execution-status", help="Read persisted execution or qualified no-trade facts"
    )
    status.add_argument("--database-url-file", type=Path, required=True)
    status.add_argument("--expected-database", required=True)
    status.add_argument("--pair-slot-id", type=UUID, required=True)
    args = parser.parse_args()
    try:
        if args.action in {
            "start",
            "resume",
            "freeze-window",
            "freeze-next-window",
            "advance-window",
            "execution-status",
            "continue-plans",
        }:
            operations: dict[str, _PairOperation] = {
                "start": execute_start,
                "resume": execute_resume,
                "freeze-window": execute_freeze,
                "freeze-next-window": execute_freeze_next,
                "advance-window": execute_advance,
                "execution-status": execute_status,
                "continue-plans": execute_continue_plans,
            }
            operation = operations[args.action]
            result = asyncio.run(
                operation(
                    database_url_file=args.database_url_file,
                    expected_database=args.expected_database,
                    pair_slot_id=args.pair_slot_id,
                )
            )
        else:
            result = asyncio.run(
                execute_registration(
                    database_url_file=args.database_url_file,
                    expected_database=args.expected_database,
                    protocol_hash=args.protocol_hash,
                    version_binding_hash=args.version_binding_hash,
                )
            )
    except Exception as error:
        code = str(error)
        safe = {
            "ECONOMIC_WINDOW_ARCHIVES_REQUIRED",
            "ECONOMIC_WINDOW_ARCHIVE_CONFLICT",
            "ECONOMIC_WINDOW_CONSUMPTION_UNAVAILABLE",
            "ECONOMIC_WINDOW_MISSED",
            "ECONOMIC_WINDOW_REQUIRED",
            "ECONOMIC_WINDOW_INVALID",
            "ECONOMIC_WINDOW_NOT_READY",
            "ECONOMIC_WINDOW_ORDER_NOT_READY",
            "ECONOMIC_WINDOW_SEQUENCE_UNSUPPORTED",
            "ECONOMIC_WINDOW_SOURCE_EXPIRED",
            "ECONOMIC_PAIR_SLOT_NOT_DUE",
            "ECONOMIC_PAIR_SLOT_MISSED",
            "ECONOMIC_PAIR_RESEARCH_UNAVAILABLE",
            "ECONOMIC_PAIR_LANE_INVALID",
            "ECONOMIC_PAIR_RESULT_UNKNOWN",
            "ECONOMIC_PAIR_CALENDAR_INVALID",
            "ECONOMIC_PAIR_CONFIG_UNSAFE",
            "ECONOMIC_PROTOCOL_DATABASE_MISMATCH",
            "ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN",
            "ECONOMIC_PAIR_INPUT_INVALID",
            "ECONOMIC_PAIR_IDEMPOTENCY_CONFLICT",
            "ECONOMIC_PAIR_AUTHORITY_UNAVAILABLE",
            "ECONOMIC_PAIR_EXISTING_INVALID",
            "ECONOMIC_PAIR_FIRST_SLOT_MISSED",
            "ECONOMIC_PROTOCOL_EVIDENCE_INVALID",
            "ECONOMIC_VERSION_BINDING_INVALID",
        }
        print(
            json.dumps(
                {
                    "status": "unavailable",
                    "code": code if code in safe else "ECONOMIC_PAIR_INPUT_OR_DATABASE_INVALID",
                }
            )
        )
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
