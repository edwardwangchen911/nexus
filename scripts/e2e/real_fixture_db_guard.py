"""Hold an exclusive safety guard around the real fixture E2E database."""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import unquote, urlsplit

import asyncpg

ADVISORY_LOCK_KEY = "nexus-real-fixture-e2e-v1"
GUARD_READY = "NEXUS_REAL_E2E_GUARD_READY"

_ERROR_MESSAGES = {
    "INVALID_DATABASE_URL": "real fixture E2E database URL is invalid",
    "CONNECTION_FAILED": "real fixture E2E database guard could not connect",
    "LOCK_UNAVAILABLE": "real fixture E2E database is already in use",
    "DATABASE_NOT_EMPTY": "real fixture E2E database must be empty",
    "PRECHECK_FAILED": "real fixture E2E database guard preflight failed",
}

_USER_OBJECTS_QUERY = """
SELECT EXISTS (
    SELECT 1
    FROM pg_namespace AS namespace
    WHERE namespace.nspname <> 'public'
      AND namespace.nspname <> 'information_schema'
      AND namespace.nspname !~ '^pg_'
    UNION ALL
    SELECT 1
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = 'public'
    UNION ALL
    SELECT 1
    FROM pg_proc AS routine
    JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace
    WHERE namespace.nspname = 'public'
    UNION ALL
    SELECT 1
    FROM pg_type AS data_type
    JOIN pg_namespace AS namespace ON namespace.oid = data_type.typnamespace
    WHERE namespace.nspname = 'public'
      AND data_type.typtype IN ('c', 'd', 'e', 'm', 'r')
    UNION ALL
    SELECT 1
    FROM pg_extension AS extension
    WHERE extension.extname <> 'plpgsql'
)
"""


class DatabaseGuardError(RuntimeError):
    """A fixed, credential-free database guard failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(_ERROR_MESSAGES.get(code, _ERROR_MESSAGES["PRECHECK_FAILED"]))


def asyncpg_dsn(database_url: str) -> str:
    """Convert the already-validated SQLAlchemy asyncpg scheme for asyncpg itself."""
    prefix = "postgresql+asyncpg://"
    try:
        parsed = urlsplit(database_url)
        database_name = unquote(parsed.path.removeprefix("/"))
    except (AttributeError, TypeError, ValueError):
        raise DatabaseGuardError("INVALID_DATABASE_URL") from None
    if (
        not isinstance(database_url, str)
        or not database_url.startswith(prefix)
        or parsed.scheme != "postgresql+asyncpg"
        or parsed.hostname not in {"127.0.0.1", "::1"}
        or parsed.query
        or parsed.fragment
        or not database_name.startswith("nexus_e2e_")
        or "/" in database_name
    ):
        raise DatabaseGuardError("INVALID_DATABASE_URL")
    return "postgresql://" + database_url.removeprefix(prefix)


@asynccontextmanager
async def hold_database_guard(database_url: str) -> AsyncIterator[None]:
    """Require an empty database and hold a session advisory lock until exit."""
    try:
        connection = await asyncpg.connect(asyncpg_dsn(database_url), timeout=10)
    except DatabaseGuardError:
        raise
    except Exception:
        raise DatabaseGuardError("CONNECTION_FAILED") from None

    try:
        try:
            locked = await connection.fetchval(
                "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
                ADVISORY_LOCK_KEY,
            )
            if locked is not True:
                raise DatabaseGuardError("LOCK_UNAVAILABLE")
            if await connection.fetchval(_USER_OBJECTS_QUERY) is True:
                raise DatabaseGuardError("DATABASE_NOT_EMPTY")
        except DatabaseGuardError:
            raise
        except Exception:
            raise DatabaseGuardError("PRECHECK_FAILED") from None
        yield
    finally:
        await connection.close()


async def _serve() -> int:
    database_url = os.environ.get("DATABASE_URL", "")
    try:
        async with hold_database_guard(database_url):
            sys.stdout.write(f"{GUARD_READY}\n")
            sys.stdout.flush()
            await asyncio.to_thread(sys.stdin.buffer.read)
    except DatabaseGuardError as error:
        sys.stderr.write(f"{error}\n")
        return 1
    return 0


def main() -> None:
    try:
        exit_code = asyncio.run(_serve())
    except KeyboardInterrupt:
        exit_code = 1
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
