"""Import an already-reviewed sector CSV into the local read-only research database."""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from sqlalchemy.engine import make_url

from nexus.market.sector_exposure_storage import persist_reviewed_sector_exposures
from nexus.market.universe import parse_reviewed_sector_csv
from nexus.ops.fosun_local import load_local_environment
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


async def run(
    settings: EngineSettings,
    *,
    csv_path: Path,
) -> dict[str, object]:
    database = make_url(settings.database_url or "")
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise ValueError("SECTOR_EXPOSURE_LOCAL_DATABASE_REQUIRED")
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--csv", type=Path, required=True)
    args = parser.parse_args()
    try:
        settings = EngineSettings.model_validate(
            load_local_environment(args.repo_root, os.environ)
        )
        result = asyncio.run(run(settings, csv_path=args.csv))
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
