"""The dated fixture clock must intercept only its exact standalone scalar read."""

from helpers.database_clock import is_fixture_clock_read
from sqlalchemy import column, func, select, table, text
from sqlalchemy.sql import Select


def test_fixture_clock_preserves_sql_shape_and_raw_text():
    clock = select(func.clock_timestamp())
    source = table("source", column("id"))
    for statement, expected in (
        (clock, True),
        (text("SELECT clock_timestamp() AS clock_timestamp_1"), True),
        (text("SELECT clock_timestamp()"), False),
        (select(func.clock_timestamp().label("custom")), False),
        (clock.where(source.c.id == 1), False),
        (clock.limit(1), False),
        (clock.select_from(source), False),
        (select(source.c.id, func.clock_timestamp()), False),
    ):
        assert is_fixture_clock_read(statement) is expected


def test_table_query_reaches_database_without_compiling_for_clock_detection(monkeypatch):
    source = table("source", column("id"), column("value"))

    def unexpected_compile(*_args, **_kwargs):
        raise AssertionError("ordinary table queries should compile only for database execution")

    monkeypatch.setattr(Select, "compile", unexpected_compile)
    assert not is_fixture_clock_read(select(source.c.id))
    assert not is_fixture_clock_read(select(source))
