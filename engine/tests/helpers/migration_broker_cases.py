"""Broker migration round-trip cases."""

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from helpers.migration_helpers import _assert_schema, _table_names


def test_0007_round_trips_the_final_broker_schema(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))

    command.upgrade(config, "0006_market_delivery_authority")
    assert "accounts" not in _table_names(temporary_database_url)
    command.upgrade(config, "0007_portfolio_snapshots")
    assert {
        "broker_sync_requests",
        "account_external_id_envelopes",
    }.issubset(_table_names(temporary_database_url))
    _assert_schema(temporary_database_url)
    command.downgrade(config, "0006_market_delivery_authority")
    assert "accounts" not in _table_names(temporary_database_url)
    command.upgrade(config, "head")
    _assert_schema(temporary_database_url)
