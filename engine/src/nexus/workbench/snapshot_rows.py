"""Bounded row reuse inside one verified read-only repeatable-read transaction.

Only ordinary SELECTs of one or two complete ORM entities participate. Callers still run every
binding, time, hash, policy and qualification check. Nothing survives the scope.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from sqlalchemy import event, inspect, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapper, ORMExecuteState
from sqlalchemy.sql import Select, visitors
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.functions import FunctionElement


@dataclass(frozen=True)
class _Rows:
    result: Any
    models: frozenset[Mapper[Any]]
    width: int
    count: int
    size: int


def _objects(value: Any, width: int) -> tuple[Any, ...]:
    return tuple(item for item in ((value,) if width == 1 else value) if item is not None)


def _simple(mapper: Mapper[Any]) -> bool:
    return all(
        child.local_table is mapper.base_mapper.local_table
        and not child.relationships
        and not any(prop.deferred for prop in child.column_attrs)
        for child in mapper.base_mapper.self_and_descendants
    )


@asynccontextmanager
async def snapshot_rows(
    session: AsyncSession,
    *,
    max_entries: int = 2048,
    max_rows: int = 4096,
    max_bytes: int = 32 * 1024 * 1024,
) -> AsyncIterator[None]:
    if any(type(n) is not int or n < 1 for n in (max_entries, max_rows, max_bytes)):
        raise ValueError("WORKBENCH_SNAPSHOT_LIMIT_INVALID")
    isolation = await session.scalar(text("SHOW transaction_isolation"))
    readonly = await session.scalar(text("SHOW transaction_read_only"))
    if isolation != "repeatable read" or readonly != "on":
        yield
        return
    owner = session.sync_session
    transaction = owner.get_transaction()
    nested = owner.get_nested_transaction()
    cache: dict[Any, _Rows] = {}
    rows = size = 0
    active = True

    def discard(keys: list[Any]) -> None:
        nonlocal rows, size
        for key in keys:
            entry = cache.pop(key)
            rows -= entry.count
            size -= entry.size

    def execute(state: ORMExecuteState) -> Any:
        nonlocal rows, size, active
        if (
            not active
            or owner.get_transaction() is not transaction
            or owner.get_nested_transaction() is not nested
            or not session.is_active
        ):
            active = False
            cache.clear()
            return None
        if session.new or session.dirty or session.deleted:
            cache.clear()
            rows = size = 0
            return None
        statement = state.statement
        if (
            not isinstance(statement, Select)
            or not state.is_select
            or not state.is_orm_statement
            or state.is_column_load
            or state.is_relationship_load
            or getattr(statement, "_for_update_arg", None) is not None
            or any(
                state.execution_options.get(k)
                for k in ("populate_existing", "yield_per", "stream_results")
            )
        ):
            # Explicit refresh must invalidate previously retained objects.
            if state.execution_options.get("populate_existing") or state.is_column_load:
                refreshed = frozenset(mapper.base_mapper for mapper in state.all_mappers)
                if (
                    not refreshed
                    or not isinstance(statement, Select)
                    or statement._with_options
                    or any(not _simple(mapper) for mapper in refreshed)
                ):
                    cache.clear()
                    rows = size = 0
                else:
                    discard([key for key, entry in cache.items() if entry.models & refreshed])
            return None
        descriptions = statement.column_descriptions
        if (
            not 1 <= len(descriptions) <= 2
            or statement._with_options
            or any(not isinstance(item["expr"], type) for item in descriptions)
        ):
            return None
        models = frozenset(inspect(item["expr"]).base_mapper for item in descriptions)
        if any(not _simple(mapper) for mapper in models):
            return None
        width = len(descriptions)
        # Database clocks and other evaluated expressions are never memoized,
        # even when they occur inside a predicate of an entity SELECT.
        if any(
            isinstance(node, FunctionElement | TextClause) for node in visitors.iterate(statement)
        ):
            return None
        compiled = statement._generate_cache_key()
        if compiled is None or any(bind.callable is not None for bind in compiled.bindparams):
            return None
        values = repr(([bind.value for bind in compiled.bindparams], state.parameters))
        if len(values) > 16 * 1024:
            return None
        key = (compiled.key, values)
        cached = cache.get(key)
        if cached is not None:
            if all(
                inspect(item).persistent
                and not inspect(item).expired
                and not inspect(item).expired_attributes
                and not inspect(item).unloaded
                for row in cached.result.data
                for item in _objects(row, cached.width)
            ):
                return cached.result()
            discard([key])
        # Freeze database rows, not the result of any domain validator.
        result = state.invoke_statement().freeze()
        retained = (
            sum(
                len(
                    repr(
                        tuple(getattr(row, col.key) for col in inspect(row).mapper.column_attrs)
                    ).encode()
                )
                for value in result.data
                for row in _objects(value, width)
            )
            + len(values)
            + 32 * len(result.data)
        )
        count = sum(max(1, len(_objects(value, width))) for value in result.data)
        if len(cache) < max_entries and rows + count <= max_rows and size + retained <= max_bytes:
            cache[key] = _Rows(result, models, width, count, retained)
            rows += count
            size += retained
        return result()

    event.listen(owner, "do_orm_execute", execute, retval=True)
    try:
        yield
    finally:
        active = False
        event.remove(owner, "do_orm_execute", execute)
        cache.clear()
