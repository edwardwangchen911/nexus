"""New-OID time functions owned only by one disposable PostgreSQL test database.

PostgreSQL builtin OIDs have native fast paths; replacing their SQL bodies is
insufficient. Install before business fixtures, on a cache-disabled test URL.
Original native functions are retained under explicit names inside this DB.
"""

import re
from datetime import datetime, timedelta
from time import monotonic
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import make_url

FUNCTIONS = ("now", "transaction_timestamp", "clock_timestamp", "statement_timestamp")


class DisposableClock:
    def __init__(self, url: str):
        self.database = make_url(url).database
        if self.database is None or re.fullmatch(r"nexus_test_[0-9a-f]{32}", self.database) is None:
            raise AssertionError("DISPOSABLE_CLOCK_DATABASE_REQUIRED")
        self.token = uuid4()
        self.logical: datetime | None = None
        self.wall: float | None = None
        self.running = True

    @staticmethod
    def uncached_url(url: str) -> str:
        return (
            make_url(url)
            .update_query_dict({"prepared_statement_cache_size": "0"})
            .render_as_string(hide_password=False)
        )

    def now(self) -> datetime:
        assert self.logical is not None and self.wall is not None
        return (
            self.logical + timedelta(seconds=monotonic() - self.wall)
            if self.running
            else self.logical
        )

    async def set(
        self, session, at: datetime, *, running: bool = True, refresh_defaults: bool = False
    ):
        database = (await session.execute(text("SELECT current_database()"))).scalar_one()
        assert database == self.database
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS fixture_clock"))
        await session.execute(
            text(
                "CREATE TABLE IF NOT EXISTS fixture_clock.durable_clock_owner("
                "singleton boolean PRIMARY KEY CHECK(singleton), token uuid NOT NULL, "
                "database_name text NOT NULL, logical_at timestamptz NOT NULL, "
                "real_at timestamptz NOT NULL, running boolean NOT NULL)"
            )
        )
        owner = (
            await session.execute(
                text("SELECT token,database_name FROM fixture_clock.durable_clock_owner")
            )
        ).one_or_none()
        if owner is None:
            # No function body or business condition is weakened. Only this new
            # disposable DB's clock names are remapped to new SQL function OIDs.
            for name in FUNCTIONS:
                await session.execute(
                    text(f"ALTER FUNCTION pg_catalog.{name}() RENAME TO nexus_original_{name}")
                )
            await session.execute(
                text(
                    "INSERT INTO fixture_clock.durable_clock_owner "
                    "VALUES(true,:token,:db,:at,pg_catalog.nexus_original_clock_timestamp(),:running)"
                ),
                dict(token=self.token, db=self.database, at=at, running=running),
            )
        else:
            assert owner.token == self.token and owner.database_name == self.database
            await session.execute(
                text(
                    "UPDATE fixture_clock.durable_clock_owner SET logical_at=:at,"
                    "real_at=pg_catalog.nexus_original_clock_timestamp(),"
                    "running=:running WHERE singleton"
                ),
                dict(at=at, running=running),
            )
        for name in FUNCTIONS:
            native = (
                "clock_timestamp"
                if name == "clock_timestamp"
                else (
                    "statement_timestamp"
                    if name == "statement_timestamp"
                    else "transaction_timestamp"
                )
            )
            volatility = "VOLATILE" if name == "clock_timestamp" else "STABLE"
            body = (
                f"SELECT logical_at+CASE WHEN running THEN "
                f"pg_catalog.nexus_original_{native}()-real_at "
                "ELSE interval '0 seconds' END "
                "FROM fixture_clock.durable_clock_owner WHERE singleton"
            )
            for schema in ("fixture_clock", "pg_catalog"):
                await session.execute(
                    text(
                        f"CREATE OR REPLACE FUNCTION {schema}.{name}() RETURNS timestamptz "
                        f"LANGUAGE sql {volatility} AS $$ {body} $$"
                    )
                )
        # Defaults affect future synthetic rows only; old immutable facts remain.
        if owner is None or refresh_defaults:
            await session.execute(
                text("""DO $$ DECLARE col record; BEGIN FOR col IN
              SELECT table_name,column_name FROM information_schema.columns
              WHERE table_schema='public' AND column_name='created_at'
              LOOP EXECUTE format('ALTER TABLE public.%I ALTER COLUMN %I
              SET DEFAULT fixture_clock.now()',col.table_name,col.column_name);
              END LOOP; END $$""")
            )
        server = (
            await session.execute(
                text("SELECT pg_catalog.clock_timestamp() /* disposable calibration */")
            )
        ).scalar_one()
        assert timedelta(0) <= server - at < timedelta(seconds=10)
        self.logical, self.wall, self.running = server, monotonic(), running
        return server


async def outside_builtin_fingerprints(connection):
    return tuple(
        (
            await connection.execute(
                text(
                    "SELECT proname,encode(sha256(convert_to(pg_get_functiondef(p.oid),"
                    "'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                    "WHERE n.nspname='pg_catalog' AND proname IN ('now','transaction_timestamp',"
                    "'clock_timestamp','statement_timestamp') ORDER BY proname"
                )
            )
        ).all()
    )
