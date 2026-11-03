"""Clock query used by dated integration fixtures."""

from sqlalchemy import Select


def is_fixture_clock_read(statement: object) -> bool:
    # A standalone SELECT with a table implied by its columns cannot be the clock-only SQL.
    if isinstance(statement, Select) and statement.columns_clause_froms:
        return False
    return str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1"
