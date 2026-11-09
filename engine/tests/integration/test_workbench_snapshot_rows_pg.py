"""Repeated reads reuse rows only within one verified read-only PostgreSQL snapshot."""

import asyncio
from contextlib import asynccontextmanager

import pytest
from nexus.workbench.snapshot_rows import snapshot_rows
from sqlalchemy import JSON, Integer, String, event, func, inspect, select, text
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


class OtherRow(Base):
    __tablename__ = "other_scope_rows"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    payload: Mapped[dict] = mapped_column(JSON)


class ThirdRow(Base):
    __tablename__ = "third_scope_rows"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    payload: Mapped[dict] = mapped_column(JSON)


@pytest.mark.asyncio
async def test_refresh_keeps_unrelated_rows_but_invalidates_both_join_dependencies(
    temporary_database_url,
):
    async with database(temporary_database_url) as (factory, reads):
        async with factory.begin() as writer:
            writer.add(OtherRow(id=1, payload={"v": "other"}))
            writer.add(ThirdRow(id=1, payload={"v": "third"}))
        pair = select(Row, OtherRow).join(OtherRow, OtherRow.id == Row.id)
        third = select(ThirdRow).where(ThirdRow.id == 1)
        async with factory.begin() as session:
            await readonly(session)
            async with snapshot_rows(session):
                result = (await session.execute(pair)).all()
                assert [(a.id, b.id) for a, b in result] == [(1, 1)]
                assert [a.id for a in (await session.scalars(pair)).all()] == [1]
                assert len(reads) == 1
                await session.scalar(third)
                await session.execute(
                    select(Row).where(Row.id == 1).execution_options(populate_existing=True)
                )
                assert (await session.scalar(third)).payload == {"v": "third"}
                assert len(reads) == 3
                await session.execute(pair)
                assert len(reads) == 4
                await session.execute(select(OtherRow).execution_options(populate_existing=True))
                await session.execute(pair)
                assert len(reads) == 6
                await session.scalar(third)
                assert len(reads) == 6


@pytest.mark.asyncio
async def test_outer_join_none_and_entity_budget_are_preserved(temporary_database_url):
    async with database(temporary_database_url) as (factory, reads):
        async with factory.begin() as writer:
            writer.add(OtherRow(id=1, payload={"v": "other"}))
        pair = select(Row, OtherRow).outerjoin(OtherRow, OtherRow.id == Row.id).order_by(Row.id)
        async with factory.begin() as session:
            await readonly(session)
            async with snapshot_rows(session):
                for _ in range(2):
                    result = (await session.execute(pair)).all()
                    assert [(a.id, b.id if b else None) for a, b in result] == [(1, 1), (2, None)]
                assert len(reads) == 1
                session.expunge(result[0][1])
                await session.execute(pair)
                assert len(reads) == 2
        async with factory.begin() as session:
            await readonly(session)
            start = len(reads)
            async with snapshot_rows(session, max_rows=1):
                for _ in range(2):
                    await session.execute(
                        select(Row, OtherRow).join(OtherRow, OtherRow.id == Row.id)
                    )
            assert len(reads) == start + 2


@pytest.mark.asyncio
async def test_unidentified_refresh_still_clears_all_rows(temporary_database_url):
    async with database(temporary_database_url) as (factory, reads):
        async with factory.begin() as session:
            await readonly(session)
            async with snapshot_rows(session):
                await session.scalar(select(Row).where(Row.id == 1))
                await session.execute(text("SELECT 1").execution_options(populate_existing=True))
                await session.scalar(select(Row).where(Row.id == 1))
                assert len(reads) == 2


class ParentRow(Base):
    __tablename__ = "inherit_scope_rows"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    kind: Mapped[str] = mapped_column(String(20))
    payload: Mapped[dict] = mapped_column(JSON)
    __mapper_args__ = {"polymorphic_on": kind, "polymorphic_identity": "parent"}


class ChildRow(ParentRow):
    __mapper_args__ = {"polymorphic_identity": "child"}


@pytest.mark.asyncio
async def test_parent_refresh_invalidates_child_and_loader_options_fall_back(
    temporary_database_url,
):
    from sqlalchemy.orm import load_only

    async with database(temporary_database_url) as (factory, reads):
        async with factory.begin() as writer:
            writer.add(ChildRow(id=1, payload={"v": "child"}))
            writer.add(ThirdRow(id=1, payload={"v": "unrelated"}))
        async with factory.begin() as session:
            await readonly(session)
            async with snapshot_rows(session):
                await session.scalar(select(ChildRow))
                await session.scalar(select(ThirdRow))
                await session.execute(select(ParentRow).execution_options(populate_existing=True))
                await session.scalar(select(ChildRow))
                await session.scalar(select(ThirdRow))
                assert len(reads) == 4
                await session.execute(select(Row).options(load_only(Row.id)))
                await session.execute(select(Row).options(load_only(Row.id)))
                assert len(reads) == 6


@pytest.mark.asyncio
@pytest.mark.parametrize("dual", [False, True])
async def test_partially_expired_member_is_loaded_by_entity_select(temporary_database_url, dual):
    async with database(temporary_database_url) as (factory, reads):
        async with factory.begin() as writer:
            writer.add(OtherRow(id=1, payload={"v": "other"}))
        statement = (
            select(Row, OtherRow).join(OtherRow, OtherRow.id == Row.id)
            if dual
            else select(Row).where(Row.id == 1)
        )
        async with factory.begin() as session:
            await readonly(session)
            async with snapshot_rows(session):
                initial = (await session.execute(statement)).one()
                member = initial[1] if dual else initial[0]
                session.expire(member, ["payload"])
                assert not inspect(member).expired
                fresh = (await session.execute(statement)).one()
                assert (fresh[1] if dual else fresh[0]).payload == {
                    "v": "other" if dual else "original"
                }
                assert len(reads) == 2
