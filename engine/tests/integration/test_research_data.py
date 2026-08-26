"""PostgreSQL assertions for immutable, point-in-time research authority."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from helpers.migration_helpers import _table_names
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

NOW = datetime(2026, 8, 24, tzinfo=UTC)


def _location(digest: str) -> str:
    return f"memory://sha256/{digest}"


async def _insert_evidence(
    connection: AsyncConnection,
    *,
    evidence_id: UUID,
    source: str,
    object_id: str,
    revision: str,
    digest: str,
) -> None:
    await connection.execute(
        text(
            "INSERT INTO research_evidence_refs "
            "(id, source, provider_object_id, revision, available_at, location, "
            "content_hash, byte_length) VALUES "
            "(:id, :source, :object_id, :revision, :available_at, :location, :digest, 7)"
        ),
        {
            "id": evidence_id,
            "source": source,
            "object_id": object_id,
            "revision": revision,
            "available_at": NOW,
            "location": _location(digest),
            "digest": digest,
        },
    )


async def _insert_us_instrument(connection: AsyncConnection) -> UUID:
    instrument_id = uuid4()
    await connection.execute(
        text(
            "INSERT INTO instruments (id, asset_type, exchange, currency, risk_bucket) "
            "VALUES (:id, 'equity', 'NASDAQ', 'USD', 'large_cap')"
        ),
        {"id": instrument_id},
    )
    return instrument_id


def test_0010_research_data_round_trips_from_0009_with_the_current_single_head(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))
    research_tables = {
        "research_evidence_refs",
        "filings",
        "financial_facts",
        "corporate_actions",
    }

    assert ScriptDirectory.from_config(config).get_heads() == [
        "0018_paper_performance_chain"
    ]
    command.upgrade(config, "0009_optimization")
    assert research_tables.isdisjoint(_table_names(temporary_database_url))
    command.upgrade(config, "0010_research_data")
    assert research_tables.issubset(_table_names(temporary_database_url))
    command.downgrade(config, "0009_optimization")
    assert research_tables.isdisjoint(_table_names(temporary_database_url))
    command.upgrade(config, "0010_research_data")
    assert research_tables.issubset(_table_names(temporary_database_url))


def test_0010_quarantines_existing_case_insensitive_macro_rows_and_rejects_new_legacy_rows(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))

    command.upgrade(config, "0009_optimization")

    async def seed_legacy_rows() -> None:
        engine = create_async_engine(temporary_database_url)
        try:
            async with engine.begin() as connection:
                for source in ("FRED", "alfred"):
                    await connection.execute(
                        text(
                            "INSERT INTO macro_observations "
                            "(id, source, series, observed_at, available_at, revision, value, "
                            "unit) "
                            "VALUES (:id, :source, 'DFF', :now, :now, 'task3', 1, 'percent')"
                        ),
                        {"id": uuid4(), "source": source, "now": NOW},
                    )
        finally:
            await engine.dispose()

    async def assert_upgraded_authority() -> None:
        engine = create_async_engine(temporary_database_url)
        try:
            async with engine.connect() as connection:
                rows = (
                    await connection.execute(
                        text(
                            "SELECT source, authority_version FROM macro_observations "
                            "ORDER BY source"
                        )
                    )
                ).all()
                assert rows == [("alfred", "legacy_task3"), ("FRED", "legacy_task3")]

            calendar_evidence_id = uuid4()
            fixture_evidence_id = uuid4()
            case_variant_evidence_id = uuid4()
            async with engine.begin() as connection:
                await _insert_evidence(
                    connection,
                    evidence_id=calendar_evidence_id,
                    source="exchange_calendars",
                    object_id="calendar:XNYS:fixture",
                    revision="1",
                    digest="c" * 64,
                )
                await _insert_evidence(
                    connection,
                    evidence_id=fixture_evidence_id,
                    source="fixture",
                    object_id="DFF:fixture-v1",
                    revision="fixture-v1",
                    digest="d" * 64,
                )
                await _insert_evidence(
                    connection,
                    evidence_id=case_variant_evidence_id,
                    source="FRED",
                    object_id="DFF:case-v1",
                    revision="case-v1",
                    digest="e" * 64,
                )

            async with engine.begin() as connection:
                with pytest.raises(DBAPIError, match="legacy macro authority is migration-only"):
                    await connection.execute(
                        text(
                            "INSERT INTO macro_observations "
                            "(id, source, series, observed_at, available_at, revision, value, "
                            "unit, "
                            "authority_version) VALUES "
                            "(:id, 'fred', 'DFF', :now, :now, 'new', 1, 'percent', 'legacy_task3')"
                        ),
                        {"id": uuid4(), "now": NOW},
                    )

            async with engine.begin() as connection:
                with pytest.raises(DBAPIError, match="legacy macro authority is migration-only"):
                    await connection.execute(
                        text(
                            "INSERT INTO macro_observations "
                            "(id, source, series, observed_at, available_at, revision, value, "
                            "unit, authority_version) VALUES "
                            "(:id, 'fixture', 'DFF', :now, :now, 'new', 1, 'percent', "
                            "'legacy_task3')"
                        ),
                        {"id": uuid4(), "now": NOW},
                    )

            async with engine.begin() as connection:
                with pytest.raises(DBAPIError, match="macro_research_v1_authority_required"):
                    await connection.execute(
                        text(
                            "INSERT INTO macro_observations "
                            "(id, source, series, observed_at, available_at, revision, value, "
                            "unit, provider_object_id, real_time_start, real_time_end, "
                            "evidence_ref_id, calendar_evidence_ref_id, calendar_source, "
                            "calendar_provider_object_id, calendar_revision, display_sanity_only, "
                            "authority_version) VALUES "
                            "(:id, 'fixture', 'DFF', :now, :now, 'fixture-v1', 1, 'percent', "
                            "'DFF:fixture-v1', '2026-08-07', '2026-08-20', :evidence_id, "
                            ":calendar_id, 'exchange_calendars', 'calendar:XNYS:fixture', '1', "
                            "false, 'research_macro_v1')"
                        ),
                        {
                            "id": uuid4(),
                            "now": NOW,
                            "evidence_id": fixture_evidence_id,
                            "calendar_id": calendar_evidence_id,
                        },
                    )

            async with engine.begin() as connection:
                with pytest.raises(DBAPIError, match="macro_research_v1_authority_required"):
                    await connection.execute(
                        text(
                            "INSERT INTO macro_observations "
                            "(id, source, series, observed_at, available_at, revision, value, "
                            "unit, provider_object_id, real_time_start, real_time_end, "
                            "evidence_ref_id, calendar_evidence_ref_id, calendar_source, "
                            "calendar_provider_object_id, calendar_revision, display_sanity_only, "
                            "authority_version) VALUES "
                            "(:id, 'FRED', 'DFF', :now, :now, 'case-v1', 1, 'percent', "
                            "'DFF:case-v1', '2026-08-07', '2026-08-20', :evidence_id, "
                            ":calendar_id, 'exchange_calendars', 'calendar:XNYS:fixture', '1', "
                            "false, 'research_macro_v1')"
                        ),
                        {
                            "id": uuid4(),
                            "now": NOW,
                            "evidence_id": case_variant_evidence_id,
                            "calendar_id": calendar_evidence_id,
                        },
                    )

            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO macro_observations "
                        "(id, source, series, observed_at, available_at, revision, value, unit, "
                        "display_sanity_only) VALUES "
                        "(:id, 'fixture', 'DFF', :now, :now, 'plain-fixture', 1, 'percent', false)"
                    ),
                    {"id": uuid4(), "now": NOW},
                )
        finally:
            await engine.dispose()

    import asyncio

    asyncio.run(seed_legacy_rows())
    command.upgrade(config, "0010_research_data")
    asyncio.run(assert_upgraded_authority())
    command.downgrade(config, "0009_optimization")


@pytest.mark.asyncio
async def test_research_evidence_location_and_owner_composites_fail_closed(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    evidence_id = uuid4()
    try:
        async with engine.begin() as connection:
            with pytest.raises(DBAPIError, match="location_content_addressed"):
                await connection.execute(
                    text(
                        "INSERT INTO research_evidence_refs "
                        "(id, source, provider_object_id, revision, available_at, location, "
                        "content_hash, byte_length) VALUES "
                        "(:id, 'sec', 'bad', '1', :available_at, 'memory://mutable', "
                        ":digest, 7)"
                    ),
                    {"id": uuid4(), "available_at": NOW, "digest": "f" * 64},
                )
        async with engine.begin() as connection:
            with pytest.raises(DBAPIError, match="byte_length_nonnegative"):
                await connection.execute(
                    text(
                        "INSERT INTO research_evidence_refs "
                        "(id, source, provider_object_id, revision, available_at, location, "
                        "content_hash, byte_length) VALUES "
                        "(:id, 'sec', 'bad-size', '1', :available_at, :location, :digest, -1)"
                    ),
                    {
                        "id": uuid4(),
                        "available_at": NOW,
                        "location": _location("f" * 64),
                        "digest": "f" * 64,
                    },
                )
        async with engine.begin() as connection:
            with pytest.raises(DBAPIError, match="location_content_addressed"):
                await connection.execute(
                    text(
                        "INSERT INTO research_evidence_refs "
                        "(id, source, provider_object_id, revision, available_at, location, "
                        "content_hash, byte_length) VALUES "
                        "(:id, 'sec', 'empty-version', '1', :available_at, "
                        "'s3://evidence/object?versionId=', :digest, 7)"
                    ),
                    {"id": uuid4(), "available_at": NOW, "digest": "f" * 64},
                )
        async with engine.begin() as connection:
            instrument_id = await _insert_us_instrument(connection)
            await _insert_evidence(
                connection,
                evidence_id=evidence_id,
                source="sec",
                object_id="0000320193-26-000001",
                revision="1",
                digest="a" * 64,
            )
            with pytest.raises(DBAPIError, match="fk_filings_evidence_owner"):
                await connection.execute(
                    text(
                        "INSERT INTO filings "
                        "(id, instrument_id, evidence_ref_id, source, provider_object_id, "
                        "accession_number, form, filed_at, accepted_at, revision) VALUES "
                        "(:id, :instrument_id, :evidence_id, 'sec', 'different-accession', "
                        "'0000320193-26-000001', '10-Q', :now, :now, '1')"
                    ),
                    {
                        "id": uuid4(),
                        "instrument_id": instrument_id,
                        "evidence_id": evidence_id,
                        "now": NOW,
                    },
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_instant_fact_uniqueness_and_filing_evidence_identity_are_database_enforced(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    evidence_id = uuid4()
    filing_id = uuid4()
    try:
        async with engine.begin() as connection:
            instrument_id = await _insert_us_instrument(connection)
            await _insert_evidence(
                connection,
                evidence_id=evidence_id,
                source="sec",
                object_id="0000320193-26-000001",
                revision="1",
                digest="a" * 64,
            )
            await connection.execute(
                text(
                    "INSERT INTO filings "
                    "(id, instrument_id, evidence_ref_id, source, provider_object_id, "
                    "accession_number, form, filed_at, accepted_at, revision) VALUES "
                    "(:id, :instrument_id, :evidence_id, 'sec', '0000320193-26-000001', "
                    "'0000320193-26-000001', '10-Q', :now, :now, '1')"
                ),
                {
                    "id": filing_id,
                    "instrument_id": instrument_id,
                    "evidence_id": evidence_id,
                    "now": NOW,
                },
            )
            fact_values = {
                "filing_id": filing_id,
                "evidence_id": evidence_id,
                "instrument_id": instrument_id,
                "now": NOW,
            }
            fact_insert = text(
                "INSERT INTO financial_facts "
                "(id, filing_id, evidence_ref_id, instrument_id, canonical_fact, raw_taxonomy, "
                "raw_tag, unit, value, period_kind, period_start, period_end, fiscal_year, "
                "fiscal_period, form, filed_at, accepted_at, accession_number, frame, revision) "
                "VALUES (:id, :filing_id, :evidence_id, :instrument_id, 'total_assets', "
                "'us-gaap', 'Assets', 'USD', 100, 'instant', NULL, '2026-06-30', 2026, 'Q2', "
                "'10-Q', :now, :now, '0000320193-26-000001', 'CY2026Q2I', '1')"
            )
            await connection.execute(fact_insert, {**fact_values, "id": uuid4()})
            with pytest.raises(DBAPIError, match="uq_financial_facts_point_in_time"):
                await connection.execute(fact_insert, {**fact_values, "id": uuid4()})
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_financial_facts_and_actions_cannot_cross_evidence_owners(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    filing_evidence_id, other_evidence_id = uuid4(), uuid4()
    action_evidence_id, other_action_evidence_id = uuid4(), uuid4()
    filing_id = uuid4()
    try:
        async with engine.begin() as connection:
            instrument_id = await _insert_us_instrument(connection)
            await _insert_evidence(
                connection,
                evidence_id=filing_evidence_id,
                source="sec",
                object_id="0000320193-26-000001",
                revision="1",
                digest="a" * 64,
            )
            await _insert_evidence(
                connection,
                evidence_id=other_evidence_id,
                source="sec",
                object_id="0000320193-26-000002",
                revision="1",
                digest="e" * 64,
            )
            await _insert_evidence(
                connection,
                evidence_id=action_evidence_id,
                source="alpaca",
                object_id="ca-001",
                revision="1",
                digest="b" * 64,
            )
            await _insert_evidence(
                connection,
                evidence_id=other_action_evidence_id,
                source="alpaca",
                object_id="ca-002",
                revision="1",
                digest="c" * 64,
            )
            await connection.execute(
                text(
                    "INSERT INTO filings "
                    "(id, instrument_id, evidence_ref_id, source, provider_object_id, "
                    "accession_number, form, filed_at, accepted_at, revision) VALUES "
                    "(:id, :instrument_id, :evidence_id, 'sec', '0000320193-26-000001', "
                    "'0000320193-26-000001', '10-Q', :now, :now, '1')"
                ),
                {
                    "id": filing_id,
                    "instrument_id": instrument_id,
                    "evidence_id": filing_evidence_id,
                    "now": NOW,
                },
            )
        async with engine.begin() as connection:
            with pytest.raises(DBAPIError, match="fk_financial_facts_filing_identity"):
                await connection.execute(
                    text(
                        "INSERT INTO financial_facts "
                        "(id, filing_id, evidence_ref_id, instrument_id, canonical_fact, "
                        "raw_taxonomy, raw_tag, unit, value, period_kind, period_end, form, "
                        "filed_at, accepted_at, accession_number, revision) VALUES "
                        "(:id, :filing_id, :evidence_id, :instrument_id, 'total_assets', "
                        "'us-gaap', 'Assets', 'USD', 100, 'instant', '2026-06-30', '10-Q', "
                        ":now, :now, '0000320193-26-000001', '1')"
                    ),
                    {
                        "id": uuid4(),
                        "filing_id": filing_id,
                        "evidence_id": other_evidence_id,
                        "instrument_id": instrument_id,
                        "now": NOW,
                    },
                )
        async with engine.begin() as connection:
            with pytest.raises(DBAPIError, match="fk_corporate_actions_evidence_owner"):
                await connection.execute(
                    text(
                        "INSERT INTO corporate_actions "
                        "(id, evidence_ref_id, source, provider_event_id, action_type, status, "
                        "process_date, revision) VALUES "
                        "(:id, :evidence_id, 'alpaca', 'ca-001', 'cash_dividend', 'complete', "
                        "'2026-08-24', '1')"
                    ),
                    {"id": uuid4(), "evidence_id": other_action_evidence_id},
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_task14_macro_authority_and_all_research_rows_are_append_only(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    evidence_id, calendar_id, action_evidence_id = uuid4(), uuid4(), uuid4()
    filing_evidence_id, filing_id, fact_id, action_id, macro_id = (
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
        uuid4(),
    )
    try:
        async with engine.begin() as connection:
            instrument_id = await _insert_us_instrument(connection)
            await _insert_evidence(
                connection,
                evidence_id=filing_evidence_id,
                source="sec",
                object_id="0000320193-26-000001",
                revision="1",
                digest="a" * 64,
            )
            await _insert_evidence(
                connection,
                evidence_id=evidence_id,
                source="fred",
                object_id="DFF:2026-08-07",
                revision="2026-08-07",
                digest="d" * 64,
            )
            await _insert_evidence(
                connection,
                evidence_id=calendar_id,
                source="exchange_calendars",
                object_id="calendar:XNYS:fixture",
                revision="1",
                digest="c" * 64,
            )
            await _insert_evidence(
                connection,
                evidence_id=action_evidence_id,
                source="alpaca",
                object_id="ca-001",
                revision="1",
                digest="b" * 64,
            )
            await connection.execute(
                text(
                    "INSERT INTO filings "
                    "(id, instrument_id, evidence_ref_id, source, provider_object_id, "
                    "accession_number, form, filed_at, accepted_at, revision) VALUES "
                    "(:id, :instrument_id, :evidence_id, 'sec', '0000320193-26-000001', "
                    "'0000320193-26-000001', '10-Q', :now, :now, '1')"
                ),
                {
                    "id": filing_id,
                    "instrument_id": instrument_id,
                    "evidence_id": filing_evidence_id,
                    "now": NOW,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO financial_facts "
                    "(id, filing_id, evidence_ref_id, instrument_id, canonical_fact, raw_taxonomy, "
                    "raw_tag, unit, value, period_kind, period_end, form, filed_at, accepted_at, "
                    "accession_number, revision) VALUES "
                    "(:id, :filing_id, :evidence_id, :instrument_id, 'total_assets', 'us-gaap', "
                    "'Assets', 'USD', 100, 'instant', '2026-06-30', '10-Q', :now, :now, "
                    "'0000320193-26-000001', '1')"
                ),
                {
                    "id": fact_id,
                    "filing_id": filing_id,
                    "evidence_id": filing_evidence_id,
                    "instrument_id": instrument_id,
                    "now": NOW,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO corporate_actions "
                    "(id, evidence_ref_id, source, provider_event_id, action_type, status, "
                    "process_date, revision) VALUES "
                    "(:id, :evidence_id, 'alpaca', 'ca-001', 'cash_dividend', 'complete', "
                    "'2026-08-24', '1')"
                ),
                {"id": action_id, "evidence_id": action_evidence_id},
            )
        async with engine.begin() as connection:
            with pytest.raises(DBAPIError, match="macro_research_v1_authority_required"):
                await connection.execute(
                    text(
                        "INSERT INTO macro_observations "
                        "(id, source, series, observed_at, available_at, revision, value, unit, "
                        "display_sanity_only) VALUES (:id, 'fred', 'DFF', :now, :now, '1', 1, "
                        "'percent', false)"
                    ),
                    {"id": uuid4(), "now": NOW},
                )
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO macro_observations "
                    "(id, source, series, observed_at, available_at, revision, value, unit, "
                    "provider_object_id, real_time_start, real_time_end, evidence_ref_id, "
                    "calendar_evidence_ref_id, calendar_source, calendar_provider_object_id, "
                    "calendar_revision, display_sanity_only, authority_version) VALUES "
                    "(:id, 'fred', 'DFF', :now, :now, '2026-08-07', 1, 'percent', "
                    "'DFF:2026-08-07', '2026-08-07', '2026-08-20', :evidence_id, :calendar_id, "
                    "'exchange_calendars', 'calendar:XNYS:fixture', '1', false, "
                    "'research_macro_v1')"
                ),
                {
                    "id": macro_id,
                    "now": NOW,
                    "evidence_id": evidence_id,
                    "calendar_id": calendar_id,
                },
            )
        table_rows = {
            "research_evidence_refs": evidence_id,
            "filings": filing_id,
            "financial_facts": fact_id,
            "corporate_actions": action_id,
            "macro_observations": macro_id,
        }
        for table_name, row_id in table_rows.items():
            for statement in (
                f"UPDATE {table_name} SET revision = revision WHERE id = :id",
                f"DELETE FROM {table_name} WHERE id = :id",
            ):
                async with engine.begin() as connection:
                    with pytest.raises(DBAPIError, match="research evidence is append-only"):
                        await connection.execute(text(statement), {"id": row_id})
    finally:
        await engine.dispose()
