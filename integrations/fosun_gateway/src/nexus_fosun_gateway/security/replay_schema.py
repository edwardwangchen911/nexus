"""Exact SQLite schema contract for the gateway replay journal."""

import re
import sqlite3
from collections.abc import Sequence

REPLAY_APPLICATION_ID = 0x4E585253
REPLAY_SCHEMA_VERSION = 1

_EXPECTED_TABLE_SQL = {
    "gateway_replay_metadata": (
        "CREATE TABLE gateway_replay_metadata ("
        " key TEXT PRIMARY KEY, value INTEGER NOT NULL) WITHOUT ROWID"
    ),
    "gateway_replay_nonces": (
        "CREATE TABLE gateway_replay_nonces ("
        " key_id TEXT NOT NULL, nonce TEXT NOT NULL, expires_at REAL NOT NULL,"
        " PRIMARY KEY (key_id, nonce)) WITHOUT ROWID"
    ),
}
_EXPECTED_COLUMNS = {
    "gateway_replay_metadata": (
        (0, "key", "TEXT", 1, None, 1),
        (1, "value", "INTEGER", 1, None, 0),
    ),
    "gateway_replay_nonces": (
        (0, "key_id", "TEXT", 1, None, 1),
        (1, "nonce", "TEXT", 1, None, 2),
        (2, "expires_at", "REAL", 1, None, 0),
    ),
}


def replay_schema_is_valid(connection: sqlite3.Connection) -> bool:
    """Require the exact tables, PK order, application ID, and intact pages."""
    try:
        if int(connection.execute("PRAGMA application_id").fetchone()[0]) != (
            REPLAY_APPLICATION_ID
        ):
            return False
        if int(connection.execute("PRAGMA user_version").fetchone()[0]) != (
            REPLAY_SCHEMA_VERSION
        ):
            return False
        if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
            return False
        objects = connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        if len(objects) != len(_EXPECTED_TABLE_SQL):
            return False
        for object_type, name, table_name, sql in objects:
            if (
                object_type != "table"
                or name != table_name
                or name not in _EXPECTED_TABLE_SQL
                or _normalized_sql(sql) != _normalized_sql(_EXPECTED_TABLE_SQL[name])
            ):
                return False
            columns = tuple(connection.execute(f"PRAGMA table_info('{name}')").fetchall())
            if columns != _EXPECTED_COLUMNS[name]:
                return False
            indexes = connection.execute(f"PRAGMA index_list('{name}')").fetchall()
            if not _primary_key_index_is_exact(connection, name, indexes):
                return False
        return True
    except (sqlite3.Error, TypeError, ValueError):
        return False


def _primary_key_index_is_exact(
    connection: sqlite3.Connection,
    table_name: str,
    indexes: Sequence[Sequence[object]],
) -> bool:
    if len(indexes) != 1:
        return False
    _, index_name, unique, origin, partial = indexes[0]
    if unique != 1 or origin != "pk" or partial != 0:
        return False
    expected_columns: tuple[tuple[int, int, str], ...]
    if table_name == "gateway_replay_metadata":
        expected_columns = ((0, 0, "key"),)
    else:
        expected_columns = ((0, 0, "key_id"), (1, 1, "nonce"))
    actual_columns = tuple(
        connection.execute(f"PRAGMA index_info('{index_name}')").fetchall()
    )
    return actual_columns == expected_columns


def _normalized_sql(value: object) -> str:
    if not isinstance(value, str):
        return ""
    collapsed = " ".join(value.split())
    return re.sub(r"\s*([(),])\s*", r"\1", collapsed).casefold()
