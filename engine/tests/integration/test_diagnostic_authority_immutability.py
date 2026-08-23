from uuid import UUID

import pytest
from helpers.portfolio_api import settings
from nexus.persistence.session import session_factory
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000071")
ALIAS_ID = UUID("0198d500-1000-7000-8000-000000000072")
ARTIFACT_ID = UUID("0198d500-1000-7000-8000-000000000073")
MANIFEST_ID = UUID("0198d500-1000-7000-8000-000000000074")


@pytest.mark.asyncio
async def test_diagnostic_authority_rows_reject_full_update_and_delete(
    migrated_database_url: str,
) -> None:
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO instruments "
                "(id, asset_type, exchange, currency, tick_size, risk_bucket) "
                "VALUES (:id, 'equity', 'XNYS', 'USD', 0.01, 'us_equity')"
            ),
            {"id": INSTRUMENT_ID},
        )
        await session.execute(
            text(
                "INSERT INTO instrument_aliases "
                "(id, instrument_id, symbol, alias_type, valid_from) "
                "VALUES (:id, :instrument_id, 'AUTH', 'ticker', DATE '2026-01-01')"
            ),
            {"id": ALIAS_ID, "instrument_id": INSTRUMENT_ID},
        )
        await session.execute(
            text(
                "INSERT INTO data_artifacts "
                "(id, uri, sha256, byte_size, mime_type, schema_version, source, "
                "observed_at, available_at, received_at) VALUES "
                "(:id, 'memory://authority', :sha, 1, 'application/vnd.apache.parquet', "
                "'price_bars_v1', 'fixture', now(), now(), now())"
            ),
            {"id": ARTIFACT_ID, "sha": "a" * 64},
        )
        await session.execute(
            text(
                "INSERT INTO price_series_manifests "
                "(id, data_artifact_id, instrument, source, feed, delivery_mode, "
                "adjustment, start_at, end_at, row_count) VALUES "
                "(:id, :artifact_id, :instrument, 'fixture', 'fixture', 'fixture', "
                "'all', now(), now(), 1)"
            ),
            {
                "id": MANIFEST_ID,
                "artifact_id": ARTIFACT_ID,
                "instrument": str(INSTRUMENT_ID),
            },
        )

    statements = (
        "UPDATE data_artifacts SET byte_size = 2 WHERE id = :id",
        "DELETE FROM data_artifacts WHERE id = :id",
        "UPDATE price_series_manifests SET row_count = 2 WHERE id = :id",
        "DELETE FROM price_series_manifests WHERE id = :id",
        "UPDATE instruments SET exchange = 'XNAS' WHERE id = :id",
        "DELETE FROM instruments WHERE id = :id",
        "UPDATE instrument_aliases SET symbol = 'MUTATED' WHERE id = :id",
        "DELETE FROM instrument_aliases WHERE id = :id",
    )
    identifiers = (
        ARTIFACT_ID,
        ARTIFACT_ID,
        MANIFEST_ID,
        MANIFEST_ID,
        INSTRUMENT_ID,
        INSTRUMENT_ID,
        ALIAS_ID,
        ALIAS_ID,
    )
    async with factory() as session, session.begin():
        for statement, identifier in zip(statements, identifiers, strict=True):
            savepoint = await session.begin_nested()
            with pytest.raises(DBAPIError, match="diagnostic authority record is immutable"):
                await session.execute(text(statement), {"id": identifier})
            await savepoint.rollback()
