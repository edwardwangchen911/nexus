"""Real PostgreSQL guards for the separate formal-optimization evidence clock.

These synthetic storage fixtures test migration constraints, not producer admission.
The producer's positive entry path is exercised separately by optimizer tests.
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from helpers.optimization_api import SNAPSHOT_ID, seed_readable_group
from helpers.portfolio_api import seed_portfolio_list, settings
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.session import session_factory
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

_VALUATION = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
_CUTOFF = _VALUATION + timedelta(hours=1)
_MATERIALIZED = _CUTOFF + timedelta(seconds=2)
_TABLES = ("optimization_admission_authorities", "optimization_proposal_groups")


async def _clone(
    session: AsyncSession, table: str, original: UUID, patch: dict[str, object]
) -> UUID:
    assert table in (*_TABLES, "optimization_proposals", "proposal_comparisons")
    identifier = uuid4()
    await session.execute(
        text(
            f"INSERT INTO {table} SELECT (jsonb_populate_record(NULL::{table}, "
            f"to_jsonb(original) || CAST(:patch AS jsonb))).* "
            f"FROM {table} original WHERE id = :original"
        ),
        {"original": original, "patch": json.dumps(patch | {"id": str(identifier)})},
    )
    return identifier


async def _seed_legacy(database_url: str) -> dict[str, UUID]:
    await seed_portfolio_list(database_url)
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        group_id, _ = await seed_readable_group(session)
        await session.flush()
        admission_id = uuid4()
        await session.execute(
            text(
                "INSERT INTO optimization_admission_authorities SELECT "
                "(jsonb_populate_record(NULL::optimization_admission_authorities, "
                "to_jsonb(g) || jsonb_build_object('id', CAST(:id AS text), "
                "'portfolio_snapshot_id', g.baseline_portfolio_snapshot_id::text, "
                "'optimizer_policy_id', g.policy_id::text))).* "
                "FROM optimization_proposal_groups g WHERE g.id = :group_id"
            ),
            {"id": str(admission_id), "group_id": group_id},
        )
    return dict(zip(_TABLES, (admission_id, group_id), strict=True))


async def _manifest(session: AsyncSession, **overrides: object) -> UUID:
    payload = {
        "schema_version": "optimization_formal_inputs_v2",
        "portfolio_snapshot_id": str(SNAPSHOT_ID),
        "valuation_time": _VALUATION.isoformat(),
        "decision_cutoff_at": _CUTOFF.isoformat(),
        "materialized_at": _MATERIALIZED.isoformat(),
    }
    payload.update(overrides.pop("payload_patch", {}))  # type: ignore[arg-type]
    record = DataSnapshot(
        **(
            {
                "id": uuid4(),
                "source": "nexus_optimization_formal",
                "provider_object_id": str(SNAPSHOT_ID),
                "revision": uuid4().hex,
                "schema_version": "optimization_formal_inputs_v2",
                "observed_at": _VALUATION,
                "received_at": _MATERIALIZED,
                "available_at": _MATERIALIZED,
                "payload": payload,
            }
            | overrides
        )
    )
    session.add(record)
    await session.flush()
    return record.id


async def _children(session: AsyncSession, original: UUID, new_group: UUID) -> None:
    rows = (
        await session.execute(
            text(
                "SELECT p.id, c.id FROM optimization_proposals p "
                "JOIN proposal_comparisons c ON c.proposal_id = p.id "
                "WHERE p.proposal_group_id = :id"
            ),
            {"id": original},
        )
    ).all()
    assert len(rows) == 3
    for proposal_id, comparison_id in rows:
        new_proposal = await _clone(
            session, "optimization_proposals", proposal_id, {"proposal_group_id": str(new_group)}
        )
        await _clone(
            session, "proposal_comparisons", comparison_id, {"proposal_id": str(new_proposal)}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("table", _TABLES)
async def test_formal_clock_rejects_missing_mismatched_and_backdated_bindings(
    migrated_database_url: str,
    table: str,
) -> None:
    legacy = await _seed_legacy(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    invalid_manifests = [
        {"source": "fixture"},
        {"schema_version": "1"},
        {"provider_object_id": str(uuid4())},
        {"payload_patch": {"schema_version": "1"}},
        {"payload_patch": {"portfolio_snapshot_id": str(uuid4())}},
        {"payload_patch": {"portfolio_snapshot_id": None}},
        {"payload_patch": {"valuation_time": _CUTOFF.isoformat()}},
        {"payload_patch": {"decision_cutoff_at": None}},
        {"payload_patch": {"decision_cutoff_at": "not-a-time"}},
        {"payload_patch": {"decision_cutoff_at": "2026-99-24T04:59:00Z"}},
        {"payload_patch": {"decision_cutoff_at": "2026-08-24T04:59:00"}},
        {"payload_patch": {"materialized_at": None}},
        {"payload_patch": {"materialized_at": _VALUATION.isoformat()}},
        {"observed_at": _CUTOFF},
        {"available_at": _CUTOFF},
        {"received_at": _CUTOFF},
    ]
    async with factory() as session, session.begin():
        for invalid in invalid_manifests:
            data_id = await _manifest(session, **invalid)
            with pytest.raises(IntegrityError, match="formal optimization clock"):
                async with session.begin_nested():
                    await _clone(
                        session,
                        table,
                        legacy[table],
                        {
                            "data_snapshot_id": str(data_id),
                            "decision_cutoff_at": _CUTOFF.isoformat(),
                        },
                    )
        data_id = await _manifest(session)
        for cutoff in (
            None,
            (_VALUATION - timedelta(seconds=1)).isoformat(),
            _VALUATION.isoformat(),
            _MATERIALIZED.isoformat(),
        ):
            with pytest.raises(IntegrityError, match="formal optimization clock"):
                async with session.begin_nested():
                    await _clone(
                        session,
                        table,
                        legacy[table],
                        {
                            "data_snapshot_id": str(data_id),
                            "decision_cutoff_at": cutoff,
                        },
                    )
        # A legacy snapshot cannot acquire a new clock and bypass formal binding.
        with pytest.raises(IntegrityError, match="formal optimization clock"):
            async with session.begin_nested():
                await _clone(
                    session,
                    table,
                    legacy[table],
                    {
                        "decision_cutoff_at": _CUTOFF.isoformat(),
                    },
                )


@pytest.mark.asyncio
async def test_valid_clock_persists_but_cannot_be_rewritten_or_enter_shadow(
    migrated_database_url: str,
) -> None:
    legacy = await _seed_legacy(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        data_id = await _manifest(session)
        patch = {"data_snapshot_id": str(data_id), "decision_cutoff_at": _CUTOFF.isoformat()}
        await _clone(session, _TABLES[0], legacy[_TABLES[0]], patch)
        group_id = await _clone(session, _TABLES[1], legacy[_TABLES[1]], patch)
        await _children(session, legacy[_TABLES[1]], group_id)
    async with factory() as session, session.begin():
        clocks = (
            await session.execute(
                text(
                    "SELECT valuation_time, decision_cutoff_at FROM optimization_proposal_groups "
                    "WHERE id = :id"
                ),
                {"id": group_id},
            )
        ).one()
        assert tuple(clocks) == (_VALUATION, _CUTOFF)
        for cutoff in (None, _MATERIALIZED):
            with pytest.raises(IntegrityError, match="formal optimization clock is immutable"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            "UPDATE optimization_proposal_groups SET decision_cutoff_at = :cutoff, "
                            "status = 'rejected', version = version + 1 WHERE id = :id"
                        ),
                        {"cutoff": cutoff, "id": group_id},
                    )
        for status in ("shadowing", "validated"):
            with pytest.raises(IntegrityError, match="PROPOSAL_NOT_EXECUTABLE"):
                async with session.begin_nested():
                    await _clone(session, _TABLES[1], group_id, {"status": status})
        with pytest.raises(IntegrityError, match="PROPOSAL_NOT_EXECUTABLE"):
            async with session.begin_nested():
                await session.execute(
                    text(
                        "UPDATE optimization_proposal_groups SET status = 'shadowing', "
                        "version = version + 1 WHERE id = :id"
                    ),
                    {"id": group_id},
                )
        await session.execute(
            text(
                "UPDATE optimization_proposal_groups SET status = 'rejected', "
                "version = version + 1 WHERE id = :id"
            ),
            {"id": group_id},
        )


@pytest.mark.parametrize("table", _TABLES)
def test_legacy_rows_round_trip_unchanged_and_formal_rows_prevent_downgrade(
    monkeypatch: pytest.MonkeyPatch,
    migrated_database_url: str,
    table: str,
) -> None:
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    legacy = asyncio.run(_seed_legacy(migrated_database_url))

    async def read_rows() -> list[object]:
        factory = session_factory(settings(migrated_database_url))
        async with factory() as session:
            return [
                await session.scalar(
                    text(
                        f"SELECT to_jsonb(t) - 'decision_cutoff_at' FROM {table} t WHERE id = :id"
                    ),
                    {"id": legacy[table]},
                )
                for table in _TABLES
            ]

    before = asyncio.run(read_rows())
    command.downgrade(config, "0039_issuer_nav_history")
    assert asyncio.run(read_rows()) == before
    command.upgrade(config, "head")
    assert asyncio.run(read_rows()) == before

    async def add_formal() -> None:
        factory = session_factory(settings(migrated_database_url))
        async with factory() as session, session.begin():
            data_id = await _manifest(session)
            new_id = await _clone(
                session,
                table,
                legacy[table],
                {
                    "data_snapshot_id": str(data_id),
                    "decision_cutoff_at": _CUTOFF.isoformat(),
                },
            )
            if table == _TABLES[1]:
                await _children(session, legacy[table], new_id)

    asyncio.run(add_formal())
    with pytest.raises(DBAPIError, match="formal optimization evidence prevents downgrade"):
        command.downgrade(config, "0039_issuer_nav_history")
    assert asyncio.run(read_rows()) == before

    async def assert_rollback() -> None:
        factory = session_factory(settings(migrated_database_url))
        async with factory() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == (
                "0040_formal_decision_cutoff"
            )
            assert (
                await session.scalar(
                    text(f"SELECT count(*) FROM {table} WHERE decision_cutoff_at IS NOT NULL")
                )
                == 1
            )

    asyncio.run(assert_rollback())


@pytest.mark.asyncio
async def test_downgrade_waits_for_inflight_formal_write_before_checking_evidence(
    monkeypatch: pytest.MonkeyPatch,
    migrated_database_url: str,
) -> None:
    """A just-committed authority must not lose its cutoff to a stale emptiness check."""
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    legacy = await _seed_legacy(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as writer:
        async with writer.begin():
            data_id = await _manifest(writer)
            await _clone(
                writer,
                _TABLES[0],
                legacy[_TABLES[0]],
                {
                    "data_snapshot_id": str(data_id),
                    "decision_cutoff_at": _CUTOFF.isoformat(),
                },
            )
            downgrade = asyncio.create_task(
                asyncio.to_thread(
                    command.downgrade,
                    config,
                    "0039_issuer_nav_history",
                )
            )

            async def wait_for_ddl_lock() -> None:
                async with factory() as observer:
                    while not downgrade.done():
                        if await observer.scalar(
                            text(
                                "SELECT EXISTS (SELECT 1 FROM pg_locks l "
                                "JOIN pg_class c ON c.oid = l.relation "
                                "WHERE c.relname = 'optimization_admission_authorities' "
                                "AND NOT l.granted)"
                            )
                        ):
                            return
                        await asyncio.sleep(0.01)
                    pytest.fail("downgrade did not wait for the in-flight writer")

            try:
                await asyncio.wait_for(wait_for_ddl_lock(), timeout=10)
            except BaseException:
                await writer.rollback()
                await asyncio.gather(downgrade, return_exceptions=True)
                raise
        with pytest.raises(DBAPIError, match="formal optimization evidence prevents downgrade"):
            await asyncio.wait_for(downgrade, timeout=10)
    async with factory() as session:
        assert (
            await session.scalar(
                text(
                    "SELECT count(*) FROM optimization_admission_authorities "
                    "WHERE decision_cutoff_at IS NOT NULL"
                )
            )
            == 1
        )
