"""Check or publish a reviewed local multi-stock Agent Paper research Universe."""

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from nexus.ops.fosun_local import load_local_environment
from nexus.paper.universe_publisher import publish_reviewed_research_universe
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


def _require_local_database(settings: EngineSettings) -> None:
    database = make_url(settings.database_url or "")
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(database.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise ValueError("REVIEWED_UNIVERSE_LOCAL_DATABASE_REQUIRED")


async def run(
    settings: EngineSettings,
    *,
    instrument_ids: tuple[UUID, ...],
    effective_from: date,
    publish: bool,
) -> dict[str, object]:
    _require_local_database(settings)
    async with session_factory(settings)() as session:
        async with session.begin():
            now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(now, datetime)
            universe = await publish_reviewed_research_universe(
                session,
                instrument_ids=instrument_ids,
                effective_from=effective_from,
                as_of=now,
            )
            result: dict[str, object] = {
                "status": "published" if publish else "publishable",
                "universe_snapshot_id": str(universe.id),
                "version": universe.version,
                "member_count": len(instrument_ids),
                "effective_from": effective_from.isoformat(),
                "execution_locked": True,
                "external_requests": 0,
            }
            if not publish:
                await session.rollback()
            return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--instrument-id", action="append", type=UUID, required=True)
    parser.add_argument("--effective-from", type=date.fromisoformat, required=True)
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    try:
        settings = EngineSettings.model_validate(
            load_local_environment(args.repo_root, os.environ)
        )
        result = asyncio.run(
            run(
                settings,
                instrument_ids=tuple(args.instrument_id),
                effective_from=args.effective_from,
                publish=args.publish,
            )
        )
    except (Exception, KeyboardInterrupt):
        print(
            json.dumps(
                {"status": "unknown", "code": "REVIEWED_UNIVERSE_CHECK_PERSISTED_STATE"}
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
