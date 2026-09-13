"""Reviewed sector CSV workflow stays local, traceable, and scoped to real gaps."""

import json
import stat
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from helpers.portfolio_api import settings
from nexus.market.universe import SectorExposureRecord
from nexus.persistence.models.market import Instrument, InstrumentAlias, SectorExposure
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.session import session_factory
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

STOCK_ID = UUID(int=9101)
ETF_ID = UUID(int=9102)
COVERED_ID = UUID(int=9103)
ETN_ID = UUID(int=9104)
ZERO_ID = UUID(int=9105)
ACCOUNT_ID = UUID(int=9110)
PORTFOLIO_ID = UUID(int=9111)
RUN_ID = UUID(int=9112)
SNAPSHOT_ID = UUID(int=9113)


def _record(
    instrument_id: UUID,
    sector: str,
    weight: str,
    *,
    digest: str,
) -> SectorExposureRecord:
    return SectorExposureRecord(
        instrument_id=instrument_id,
        sector_code=sector,
        weight=Decimal(weight),
        as_of=date(2026, 1, 1),
        source_url=f"https://issuer.example/{instrument_id}.csv",
        content_hash=digest,
        synthetic=False,
        reviewed=True,
    )


async def _seed_instruments(session: AsyncSession) -> None:
    session.add_all(
        (
            Instrument(
                id=STOCK_ID,
                asset_type="common_stock",
                exchange="XNAS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="EQUITY",
            ),
            Instrument(
                id=ETF_ID,
                asset_type="equity_etf",
                exchange="XNYS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="EQUITY",
            ),
        )
    )


@pytest.mark.asyncio
async def test_reviewed_sector_batch_is_validated_persisted_and_idempotent(
    migrated_database_url: str,
) -> None:
    from nexus.market import sector_exposure_storage

    records = (
        _record(STOCK_ID, "industrials", "1", digest="a" * 64),
        _record(ETF_ID, "financials", "0.400", digest="b" * 64),
        _record(ETF_ID, "information_technology", "0.599", digest="b" * 64),
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_instruments(session)
        first = await sector_exposure_storage.persist_reviewed_sector_exposures(session, records)
        replay = await sector_exposure_storage.persist_reviewed_sector_exposures(session, records)

    assert replay == first
    assert len(first) == 3
    async with factory() as session:
        rows = tuple(
            await session.scalars(
                select(SectorExposure).order_by(
                    SectorExposure.instrument_id, SectorExposure.sector_code
                )
            )
        )
        assert len(rows) == 3
        assert all(row.reviewed and not row.synthetic for row in rows)
        assert await session.scalar(select(func.count()).select_from(SectorExposure)) == 3


@pytest.mark.asyncio
async def test_reviewed_sector_batch_rejects_incomplete_stock_without_writing(
    migrated_database_url: str,
) -> None:
    from nexus.market import sector_exposure_storage

    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_instruments(session)
        with pytest.raises(ValueError, match="SECTOR_EXPOSURE_BATCH_INVALID"):
            await sector_exposure_storage.persist_reviewed_sector_exposures(
                session,
                (_record(STOCK_ID, "industrials", "0.9", digest="c" * 64),),
            )

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(SectorExposure)) == 0


@pytest.mark.asyncio
async def test_local_entry_imports_reviewed_csv_with_aggregate_receipt(
    migrated_database_url: str,
    tmp_path: Path,
) -> None:
    from nexus.ops import sector_exposure_local

    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_instruments(session)
    csv_path = tmp_path / "reviewed-sector.csv"
    csv_path.write_text(
        "instrument_id,sector_code,weight,as_of,source_url,content_hash\n"
        f"{STOCK_ID},industrials,1,2026-01-01,https://issuer.example/stock.csv,{'d' * 64}\n"
    )

    result = await sector_exposure_local.run(
        settings(migrated_database_url),
        csv_path=csv_path,
    )

    assert result == {
        "status": "imported",
        "instrument_count": 1,
        "row_count": 1,
        "execution_locked": True,
        "model_called": False,
    }


@pytest.mark.asyncio
async def test_review_template_contains_only_latest_snapshot_sector_gaps(
    migrated_database_url: str,
    tmp_path: Path,
) -> None:
    """A wrong scope query would expose closed, non-equity, or already covered positions."""
    from nexus.ops import sector_exposure_local

    as_of = datetime(2027, 1, 2, tzinfo=UTC)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_instruments(session)
        session.add_all(
            (
                Instrument(
                    id=COVERED_ID,
                    asset_type="common_stock",
                    exchange="XNAS",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="EQUITY",
                ),
                Instrument(
                    id=ETN_ID,
                    asset_type="exchange_traded_note",
                    exchange="XNAS",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="OTHER",
                ),
                Instrument(
                    id=ZERO_ID,
                    asset_type="common_stock",
                    exchange="XNAS",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="EQUITY",
                ),
                Account(id=ACCOUNT_ID, provider="fosun", status="ACTIVE"),
                Portfolio(id=PORTFOLIO_ID, account_id=ACCOUNT_ID, name="Private"),
            )
        )
        await session.flush()
        session.add_all(
            (
                BrokerSyncRun(
                    id=RUN_ID,
                    account_id=ACCOUNT_ID,
                    portfolio_id=PORTFOLIO_ID,
                    trigger_kind="MANUAL",
                    actor_id="test",
                    route="broker-sync",
                    idempotency_key="sector-review-template",
                    request_hash="a" * 64,
                    status="READY",
                    accepted_at=as_of,
                    completed_at=as_of,
                    attempt_count=1,
                    cash_flow_quality="PASS",
                ),
                PortfolioSnapshotRecord(
                    id=SNAPSHOT_ID,
                    portfolio_id=PORTFOLIO_ID,
                    sync_run_id=RUN_ID,
                    stale_from_snapshot_id=None,
                    status="READY",
                    as_of=as_of,
                    position_hash_version="position_snapshot_v1",
                    position_hash="b" * 64,
                    order_hash_version="order_snapshot_v1",
                    order_hash="c" * 64,
                    source_request_ids=[],
                    source_observations=[],
                ),
            )
        )
        await session.flush()
        for instrument_id, symbol in (
            (STOCK_ID, "MISSING"),
            (ETF_ID, "ETF-GAP"),
            (COVERED_ID, "COVERED"),
            (ETN_ID, "NOTE"),
            (ZERO_ID, "CLOSED"),
        ):
            session.add(
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alias_type="ticker",
                    valid_from=date(2026, 1, 1),
                    valid_to=None,
                )
            )
            session.add(
                PositionSnapshot(
                    portfolio_snapshot_id=SNAPSHOT_ID,
                    instrument_id=instrument_id,
                    currency="USD",
                    quantity=Decimal(0 if instrument_id == ZERO_ID else 1),
                    available_quantity=Decimal(0 if instrument_id == ZERO_ID else 1),
                    frozen_quantity=Decimal(0),
                    avg_cost=Decimal(10),
                    diluted_cost=Decimal(10),
                    current_price=Decimal(10),
                    provider_as_of=as_of,
                )
            )
        session.add(
            SectorExposure(
                instrument_id=COVERED_ID,
                sector_code="industrials",
                weight=Decimal(1),
                as_of=as_of.date(),
                source_url="https://issuer.example/covered",
                content_hash="d" * 64,
                synthetic=False,
                reviewed=True,
            )
        )

    tmp_path.chmod(0o700)
    output_path = tmp_path / "sector-review.json"
    before = output_path.exists()
    result = await sector_exposure_local.export_review_template(
        settings(migrated_database_url), output_path=output_path
    )
    payload = json.loads(output_path.read_bytes())

    assert before is False
    assert stat.S_IMODE(output_path.stat().st_mode) == 0o600
    assert result == {
        "status": "review_template_written",
        "required_instrument_count": 3,
        "missing_instrument_count": 2,
        "execution_locked": True,
        "model_called": False,
    }
    assert payload["format"] == "nexus_sector_exposure_review_v1"
    assert payload["portfolio_snapshot_id"] == str(SNAPSHOT_ID)
    assert payload["portfolio_snapshot_as_of"] == as_of.isoformat()
    assert payload["import_columns"] == [
        "instrument_id",
        "sector_code",
        "weight",
        "as_of",
        "source_url",
        "content_hash",
    ]
    assert [item["symbol"] for item in payload["missing_instruments"]] == [
        "ETF-GAP",
        "MISSING",
    ]
    assert [item["asset_type"] for item in payload["missing_instruments"]] == [
        "equity_etf",
        "common_stock",
    ]
    assert all(
        item["import_row"]
        == {
            "instrument_id": item["instrument_id"],
            "sector_code": "",
            "weight": "",
            "as_of": "",
            "source_url": "",
            "content_hash": "",
        }
        for item in payload["missing_instruments"]
    )
    with pytest.raises(ValueError, match="SECTOR_EXPOSURE_PRIVATE_OUTPUT_REQUIRED"):
        await sector_exposure_local.export_review_template(
            settings(migrated_database_url), output_path=output_path
        )


@pytest.mark.asyncio
async def test_review_template_can_scope_one_agent_paper_candidate(
    migrated_database_url: str,
    tmp_path: Path,
) -> None:
    """The Agent workflow must not depend on a real-portfolio snapshot."""
    from nexus.ops import sector_exposure_local

    decision_time = datetime(2026, 1, 2, tzinfo=UTC)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await _seed_instruments(session)
        session.add(
            InstrumentAlias(
                instrument_id=STOCK_ID,
                symbol="AGENT-CANDIDATE",
                alias_type="ticker",
                valid_from=date(2026, 1, 1),
                valid_to=None,
            )
        )

    tmp_path.chmod(0o700)
    output_path = tmp_path / "agent-sector-review.json"
    result = await sector_exposure_local.export_review_template(
        settings(migrated_database_url),
        output_path=output_path,
        instrument_id=STOCK_ID,
        decision_time=decision_time,
    )
    payload = json.loads(output_path.read_bytes())

    assert result == {
        "status": "review_template_written",
        "required_instrument_count": 1,
        "missing_instrument_count": 1,
        "execution_locked": True,
        "model_called": False,
    }
    assert payload["scope"] == "agent_paper_candidate"
    assert payload["decision_time"] == decision_time.isoformat()
    assert "portfolio_snapshot_id" not in payload
    assert payload["missing_instruments"] == [
        {
            "symbol": "AGENT-CANDIDATE",
            "instrument_id": str(STOCK_ID),
            "asset_type": "common_stock",
            "import_row": {
                "instrument_id": str(STOCK_ID),
                "sector_code": "",
                "weight": "",
                "as_of": "",
                "source_url": "",
                "content_hash": "",
            },
        }
    ]
