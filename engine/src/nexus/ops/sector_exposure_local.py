"""Import an already-reviewed sector CSV into the local read-only research database."""

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.repository import load_frozen_snapshot, load_instrument_identities
from nexus.market.sector_exposure_storage import persist_reviewed_sector_exposures
from nexus.market.universe import EQUITY_SECTOR_CODES, parse_reviewed_sector_csv
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.optimizer.scenario_risk import load_scenario_evidence
from nexus.persistence.models.market import Instrument, InstrumentAlias, SectorExposure
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


def _require_local_database(settings: EngineSettings) -> None:
    database = make_url(settings.database_url or "")
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise ValueError("SECTOR_EXPOSURE_LOCAL_DATABASE_REQUIRED")


async def run(
    settings: EngineSettings,
    *,
    csv_path: Path,
) -> dict[str, object]:
    _require_local_database(settings)
    records = parse_reviewed_sector_csv(csv_path.read_bytes())
    async with session_factory(settings).begin() as session:
        ids = await persist_reviewed_sector_exposures(session, records)
    return {
        "status": "imported",
        "instrument_count": len({record.instrument_id for record in records}),
        "row_count": len(ids),
        "execution_locked": True,
        "model_called": False,
    }


async def export_review_template(
    settings: EngineSettings,
    *,
    output_path: Path,
    instrument_id: UUID | None = None,
    decision_time: datetime | None = None,
) -> dict[str, object]:
    """Write private identifiers only to a new operator-owned local review file."""
    _require_local_database(settings)
    parent = output_path.parent
    if (
        parent.is_symlink()
        or not parent.is_dir()
        or parent.stat().st_mode & 0o077
        or output_path.exists()
    ):
        raise ValueError("SECTOR_EXPOSURE_PRIVATE_OUTPUT_REQUIRED")
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        if (instrument_id is None) != (decision_time is None):
            raise ValueError("SECTOR_EXPOSURE_AGENT_SCOPE_INVALID")
        if instrument_id is not None and decision_time is not None:
            if (
                decision_time.tzinfo is None
                or decision_time.utcoffset() != UTC.utcoffset(decision_time)
            ):
                raise ValueError("SECTOR_EXPOSURE_AGENT_SCOPE_INVALID")
            instrument = await session.get(Instrument, instrument_id)
            aliases = tuple(
                await session.scalars(
                    select(InstrumentAlias).where(
                        InstrumentAlias.instrument_id == instrument_id,
                        InstrumentAlias.alias_type == "ticker",
                        InstrumentAlias.valid_from <= decision_time.date(),
                        (InstrumentAlias.valid_to.is_(None))
                        | (InstrumentAlias.valid_to >= decision_time.date()),
                        InstrumentAlias.created_at <= decision_time,
                    )
                )
            )
            if (
                instrument is None
                or instrument.created_at > decision_time
                or instrument.currency != "USD"
                or instrument.risk_bucket.upper() not in {"EQUITY", "US_EQUITY"}
                or instrument.asset_type not in {"common_stock", "equity", "equity_etf"}
                or len(aliases) != 1
            ):
                raise ValueError("SECTOR_EXPOSURE_REVIEW_IDENTITY_MISSING")
            covered = await session.scalar(
                select(SectorExposure.id)
                .where(
                    SectorExposure.instrument_id == instrument_id,
                    SectorExposure.as_of <= decision_time.date(),
                    SectorExposure.reviewed.is_(True),
                    SectorExposure.synthetic.is_(False),
                    SectorExposure.created_at <= decision_time,
                )
                .limit(1)
            )
            items: list[dict[str, object]] = []
            if covered is None:
                items.append(
                    {
                        "symbol": aliases[0].symbol,
                        "instrument_id": str(instrument_id),
                        "asset_type": instrument.asset_type,
                        "import_row": {
                            "instrument_id": str(instrument_id),
                            "sector_code": "",
                            "weight": "",
                            "as_of": "",
                            "source_url": "",
                            "content_hash": "",
                        },
                    }
                )
            payload = _review_payload(items)
            payload.update(
                {
                    "scope": "agent_paper_candidate",
                    "decision_time": decision_time.isoformat(),
                }
            )
            required_count = 1
            missing_count = len(items)
        else:
            payload, required_count, missing_count = await _portfolio_review_payload(session)
    write_private(
        output_path,
        (
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode(),
    )
    return {
        "status": "review_template_written",
        "required_instrument_count": required_count,
        "missing_instrument_count": missing_count,
        "execution_locked": True,
        "model_called": False,
    }


def _review_payload(items: list[dict[str, object]]) -> dict[str, object]:
    return {
        "format": "nexus_sector_exposure_review_v1",
        "import_columns": [
            "instrument_id",
            "sector_code",
            "weight",
            "as_of",
            "source_url",
            "content_hash",
        ],
        "allowed_sector_codes": list(EQUITY_SECTOR_CODES),
        "rules": {
            "common_stock": "exactly one row with weight 1",
            "equity_etf": "one or more rows totaling 0.995 through 1.005",
            "source": "reviewed HTTPS source and SHA256 of the reviewed original",
        },
        "missing_instruments": items,
    }


async def _portfolio_review_payload(
    session: AsyncSession,
) -> tuple[dict[str, object], int, int]:
    record = await session.scalar(
        select(PortfolioSnapshotRecord)
        .where(
            PortfolioSnapshotRecord.status == "READY",
            PortfolioSnapshotRecord.as_of.is_not(None),
        )
        .order_by(
            PortfolioSnapshotRecord.created_at.desc(),
            PortfolioSnapshotRecord.id.desc(),
        )
        .limit(1)
    )
    if record is None or record.as_of is None:
        raise ValueError("SECTOR_EXPOSURE_READY_SNAPSHOT_REQUIRED")
    snapshot = await load_frozen_snapshot(
        session, portfolio_id=record.portfolio_id, snapshot_id=record.id
    )
    evidence = await load_scenario_evidence(session, snapshot)
    identities = await load_instrument_identities(session, snapshot=snapshot)
    missing = evidence.sector_required_ids - evidence.sectors.keys()
    items: list[dict[str, object]] = []
    for instrument_id in missing:
        identity = identities.get(instrument_id)
        if (
            identity is None
            or identity.canonical_ticker is None
            or identity.asset_type is None
            or identity.restriction == "UNRESOLVED"
        ):
            raise ValueError("SECTOR_EXPOSURE_REVIEW_IDENTITY_MISSING")
        items.append(
            {
                "symbol": identity.canonical_ticker,
                "instrument_id": str(instrument_id),
                "asset_type": identity.asset_type,
                "import_row": {
                    "instrument_id": str(instrument_id),
                    "sector_code": "",
                    "weight": "",
                    "as_of": "",
                    "source_url": "",
                    "content_hash": "",
                },
            }
        )
    items.sort(key=lambda item: str(item["symbol"]))
    payload = _review_payload(items)
    payload.update(
        {
            "portfolio_snapshot_id": str(record.id),
            "portfolio_snapshot_as_of": record.as_of.isoformat(),
        }
    )
    return payload, len(evidence.sector_required_ids), len(items)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--csv", type=Path)
    operation.add_argument("--review-template", type=Path)
    parser.add_argument("--instrument-id", type=UUID)
    parser.add_argument("--decision-time", type=datetime.fromisoformat)
    args = parser.parse_args()
    try:
        settings = EngineSettings.model_validate(
            load_local_environment(args.repo_root, os.environ)
        )
        result = asyncio.run(
            run(settings, csv_path=args.csv)
            if args.csv is not None
            else export_review_template(
                settings,
                output_path=args.review_template,
                instrument_id=args.instrument_id,
                decision_time=args.decision_time,
            )
        )
    except (Exception, KeyboardInterrupt):
        print(
            json.dumps(
                {
                    "status": "unknown",
                    "code": "SECTOR_EXPOSURE_IMPORT_CHECK_PERSISTED_STATE",
                }
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
