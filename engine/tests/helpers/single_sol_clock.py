from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from nexus import worker
from sqlalchemy import text


def preopen_target(wall):
    target = wall.astimezone(ZoneInfo("America/New_York")).replace(
        hour=8, minute=0, second=0, microsecond=0
    )
    while target >= wall or target.weekday() >= 5:
        target -= timedelta(days=1)
    return target.astimezone(UTC)


async def preopen_fixture_clock(url, monkeypatch, *, target=None):
    """One synthetic morning for archives, DB inserts, claims and model-local clocks."""
    from helpers import current_identity
    from integration import test_research_identity_context
    from nexus.agents import snapshots
    from nexus.agents.codex_candidate import require_candidate_database
    from nexus.ops import instrument_identity_local
    from nexus.persistence.session import session_factory
    from nexus.research import current_identity_archive
    from nexus.settings import EngineSettings
    from sqlalchemy.engine import make_url

    factory = session_factory(EngineSettings(database_url=url))
    async with factory.begin() as db:
        await require_candidate_database(db, configured_database=make_url(url).database)
    async with factory.begin() as db:
        name = await db.scalar(text("SELECT current_database()"))
        assert name.isidentifier() and name != "postgres"
        wall = await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
        target = target or preopen_target(wall)
        delta = int((wall - target).total_seconds())
        await db.execute(text("CREATE SCHEMA fixture_clock"))
        await db.execute(
            text(f"""CREATE FUNCTION fixture_clock.clock_timestamp() RETURNS timestamptz
            LANGUAGE sql VOLATILE AS $$
            SELECT pg_catalog.clock_timestamp() - interval '{delta} seconds' $$""")
        )
        await db.execute(
            text(f"""CREATE FUNCTION fixture_clock.now() RETURNS timestamptz
            LANGUAGE sql STABLE AS $$
            SELECT pg_catalog.transaction_timestamp() - interval '{delta} seconds' $$""")
        )
        await db.execute(
            text(f'ALTER DATABASE "{name}" SET search_path TO fixture_clock,public,pg_catalog')
        )
        # The route trigger has its own search_path. Keep its original price
        # deadline check on the same synthetic clock as registration and claims.
        await db.execute(
            text(
                "ALTER FUNCTION public.nexus_sol_native_route_guard() "
                "SET search_path TO fixture_clock,public,pg_catalog"
            )
        )
        await db.execute(
            text("""DO $$ DECLARE obj record; BEGIN
            FOR obj IN SELECT table_name FROM information_schema.columns
            WHERE table_schema='public' AND column_name='created_at' AND column_default IS NOT NULL
            LOOP EXECUTE format('ALTER TABLE public.%I ALTER COLUMN created_at '
                'SET DEFAULT fixture_clock.now()', obj.table_name);
            END LOOP; END $$""")
        )
    await factory.kw["bind"].dispose()

    class FixtureTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return (datetime.now(UTC) - timedelta(seconds=delta)).astimezone(tz)

    monkeypatch.setattr(worker, "_ONCE_STARTED_AT", target)
    for module in (
        current_identity,
        test_research_identity_context,
        instrument_identity_local,
        current_identity_archive,
        snapshots,
        worker,
    ):
        monkeypatch.setattr(module, "datetime", FixtureTime)
