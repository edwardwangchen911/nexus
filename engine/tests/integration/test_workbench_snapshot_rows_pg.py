"""Repeated reads reuse rows only within one verified read-only PostgreSQL snapshot."""

import asyncio
from contextlib import asynccontextmanager

import pytest
from nexus.workbench.snapshot_rows import snapshot_rows
from sqlalchemy import JSON, Integer, event, func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Row(Base):
    __tablename__ = "scope_rows"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    payload: Mapped[dict] = mapped_column(JSON)


@asynccontextmanager
async def database(url):
    engine = create_async_engine(url)
    counts = []

    def before(conn, cursor, statement, params, context, many):
        if "scope_rows" in statement and statement.lstrip().startswith("SELECT"):
            counts.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", before)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(
                Row.__table__.insert(),
                [{"id": 1, "payload": {"v": "original"}}, {"id": 2, "payload": {"v": "second"}}],
            )
        yield async_sessionmaker(engine, expire_on_commit=False), counts
    finally:
        await engine.dispose()


async def readonly(session):
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))


@pytest.mark.asyncio
async def test_exact_rows_reused_but_parameters_and_requests_stay_distinct(temporary_database_url):
    async with database(temporary_database_url) as (factory, reads):
        async with factory.begin() as session:
            await readonly(session)
            async with snapshot_rows(session):
                for _ in range(2):
                    assert (await session.scalar(select(Row).where(Row.id == 1))).payload == {
                        "v": "original"
                    }
                assert (await session.scalar(select(Row).where(Row.id == 2))).payload == {
                    "v": "second"
                }
                assert await session.scalar(select(Row).where(Row.id == 3)) is None
                assert await session.scalar(select(Row).where(Row.id == 3)) is None
                assert len(reads) == 3
        async with factory.begin() as writer:
            await writer.execute(text("UPDATE scope_rows SET payload='{}' WHERE id=1"))
        async with factory.begin() as session:
            await readonly(session)
            async with snapshot_rows(session):
                assert (await session.scalar(select(Row).where(Row.id == 1))).payload == {}
        assert len(reads) == 4


@pytest.mark.asyncio
async def test_read_committed_and_writable_transactions_never_reuse(temporary_database_url):
    async with database(temporary_database_url) as (factory, reads):
        for setting in (
            "SET TRANSACTION READ ONLY",
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        ):
            async with factory.begin() as session:
                await session.execute(text(setting))
                async with snapshot_rows(session):
                    for _ in range(2):
                        await session.scalar(select(Row).where(Row.id == 1))
        assert len(reads) == 4


@pytest.mark.asyncio
async def test_limits_clock_expiry_and_cleanup(temporary_database_url):
    async with database(temporary_database_url) as (factory, reads):
        async with factory.begin() as session:
            await readonly(session)
            async with snapshot_rows(session, max_entries=1):
                await session.scalar(select(Row).where(Row.id == 1))
                for _ in range(2):
                    await session.scalar(select(Row).where(Row.id == 2))
                assert len(reads) == 3
                first = await session.scalar(select(func.clock_timestamp()))
                second = await session.scalar(select(func.clock_timestamp()))
                assert second > first
                row = await session.scalar(select(Row).where(Row.id == 1))
                session.expire(row)
                await session.scalar(select(Row).where(Row.id == 1))
                assert len(reads) == 4
            await session.scalar(select(Row).where(Row.id == 1))
            assert len(reads) == 5
        async with factory.begin() as session:
            await readonly(session)
            with pytest.raises(asyncio.CancelledError):
                async with snapshot_rows(session):
                    await session.scalar(select(Row).where(Row.id == 1))
                    raise asyncio.CancelledError
            await session.scalar(select(Row).where(Row.id == 1))
            assert len(reads) == 7


@pytest.mark.asyncio
async def test_new_transaction_does_not_reuse_previous_rows(temporary_database_url):
    async with database(temporary_database_url) as (factory, reads):
        async with factory() as session:
            await readonly(session)
            async with snapshot_rows(session):
                await session.scalar(select(Row).where(Row.id == 1))
                await session.commit()
                await readonly(session)
                await session.scalar(select(Row).where(Row.id == 1))
                assert len(reads) == 2
            await session.rollback()


@pytest.mark.asyncio
async def test_dirty_state_byte_row_limits_and_clock_predicates_bypass(temporary_database_url):
    async with database(temporary_database_url) as (factory, reads):
        async with factory.begin() as session:
            await readonly(session)
            async with snapshot_rows(session):
                row = await session.scalar(select(Row).where(Row.id == 1))
                row.payload = {"v": "in-memory change"}
                with session.no_autoflush:
                    for _ in range(2):
                        assert (await session.scalar(select(Row).where(Row.id == 1))).payload == {
                            "v": "in-memory change"
                        }
                assert len(reads) == 3
                session.expunge(row)
        for limits in ({"max_bytes": 1}, {"max_rows": 1}):
            async with factory.begin() as session:
                await readonly(session)
                start = len(reads)
                async with snapshot_rows(session, **limits):
                    for _ in range(2):
                        assert len((await session.scalars(select(Row))).all()) == 2
                assert len(reads) == start + 2
        async with factory.begin() as session:
            await readonly(session)
            start = len(reads)
            async with snapshot_rows(session):
                for _ in range(2):
                    await session.scalars(
                        select(Row).where(
                            Row.id > func.extract("second", func.clock_timestamp()) - 60
                        )
                    )
            assert len(reads) == start + 2


@pytest.mark.asyncio
async def test_callable_parameters_are_evaluated_once_per_query_and_never_reused(
    temporary_database_url,
):
    from sqlalchemy import bindparam

    async with database(temporary_database_url) as (factory, reads):
        values = iter((1, 2))
        stmt = select(Row).where(Row.id == bindparam("selected", callable_=lambda: next(values)))
        async with factory.begin() as session:
            await readonly(session)
            async with snapshot_rows(session):
                assert (await session.scalar(stmt)).id == 1
                assert (await session.scalar(stmt)).id == 2
        assert len(reads) == 2
