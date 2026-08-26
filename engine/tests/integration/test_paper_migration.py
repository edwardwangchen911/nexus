import asyncio
from pathlib import Path
from uuid import UUID

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

PAPER_ACCOUNT_ID = UUID("d31672c0-3922-5ebd-8fdb-cd3174266731")
PAPER_TABLES = {
    "risk_policies",
    "strategy_decisions",
    "strategy_decision_targets",
    "paper_accounts",
    "paper_drawdown_lock_events",
    "paper_order_batches",
    "paper_orders",
    "paper_order_events",
    "risk_evaluations",
    "risk_evaluation_rules",
    "risk_evaluation_price_inputs",
    "risk_evaluation_sector_inputs",
    "risk_query_authorities",
    "risk_evaluation_query_proofs",
    "paper_ledger_accounts",
    "paper_ledger_transactions",
    "paper_ledger_entries",
    "paper_reservations",
    "paper_reservation_events",
    "paper_fills",
    "paper_position_adjustment_events",
    "paper_position_snapshots",
    "paper_performance_snapshots",
}
TASK19_TABLES = {
    "paper_fills",
    "paper_position_adjustment_events",
    "paper_position_snapshots",
    "paper_performance_snapshots",
}
ALEMBIC_CONFIG = Path(__file__).parents[2] / "alembic.ini"


def _config() -> Config:
    return Config(str(ALEMBIC_CONFIG))


async def _paper_seed(database_url: str) -> dict[str, object]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            account = (
                await connection.execute(
                    text(
                        "SELECT pa.id, pa.account_id, pa.portfolio_id, pa.code, "
                        "pa.currency, pa.account_model, pa.position_model, "
                        "pa.initial_capital, a.provider, p.name "
                        "FROM paper_accounts pa "
                        "JOIN accounts a ON a.id = pa.account_id "
                        "JOIN portfolios p ON p.id = pa.portfolio_id"
                    )
                )
            ).mappings().one()
            policy = (
                await connection.execute(
                    text(
                        "SELECT version, active, content_hash "
                        "FROM risk_policies"
                    )
                )
            ).mappings().one()
            ledger_accounts = (
                await connection.execute(
                    text(
                        "SELECT code, normal_side, currency "
                        "FROM paper_ledger_accounts ORDER BY code"
                    )
                )
            ).mappings().all()
            funding = (
                await connection.execute(
                    text(
                        "SELECT count(*) AS entry_count, "
                        "sum(debit_amount) AS debits, "
                        "sum(credit_amount) AS credits "
                        "FROM paper_ledger_entries"
                    )
                )
            ).mappings().one()
            drawdown = (
                await connection.execute(
                    text(
                        "SELECT event_seq, from_state, to_state, drawdown_ratio, "
                        "reason_code FROM paper_drawdown_lock_events"
                    )
                )
            ).mappings().one()
            task19_counts = {
                table: int(
                    await connection.scalar(text(f"SELECT count(*) FROM {table}"))
                    or 0
                )
                for table in TASK19_TABLES
            }
            return {
                "account": dict(account),
                "policy": dict(policy),
                "ledger_accounts": [dict(row) for row in ledger_accounts],
                "funding": dict(funding),
                "drawdown": dict(drawdown),
                "task19_counts": task19_counts,
            }
    finally:
        await engine.dispose()


async def _table_names(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            rows = await connection.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public'"
                )
            )
            return {str(row.table_name) for row in rows}
    finally:
        await engine.dispose()


def test_paper_revision_precedes_the_execution_release_head() -> None:
    script = ScriptDirectory.from_config(_config())

    assert script.get_heads() == ["0015_paper_execution"]
    revision = script.get_revision("0014_paper")
    assert revision is not None
    assert revision.down_revision == "0013_shadow"


def test_paper_migration_seeds_balanced_internal_account_and_round_trips(
    monkeypatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = _config()

    command.upgrade(config, "head")
    assert PAPER_TABLES <= asyncio.run(_table_names(temporary_database_url))
    seed = asyncio.run(_paper_seed(temporary_database_url))
    assert seed["account"] == {
        "id": PAPER_ACCOUNT_ID,
        "account_id": UUID("877caec6-5092-5945-a268-544f5e78a40c"),
        "portfolio_id": UUID("ee2ff673-eaf6-5076-bbab-f4c9cedfc74e"),
        "code": "internal_usd_v1",
        "currency": "USD",
        "account_model": "cash",
        "position_model": "long_only",
        "initial_capital": 10000,
        "provider": "internal",
        "name": "NEXUS Internal USD Paper",
    }
    assert seed["policy"] == {
        "version": "risk_policy_v1",
        "active": True,
        "content_hash": "70f395ac13218d3e19b65e1717b3e2fdcbc31eaa5614ff60613f1c509dd7280b",
    }
    assert len(seed["ledger_accounts"]) == 7
    assert {row["currency"] for row in seed["ledger_accounts"]} == {"USD"}
    assert seed["funding"] == {
        "entry_count": 2,
        "debits": 10000,
        "credits": 10000,
    }
    assert seed["drawdown"] == {
        "event_seq": 1,
        "from_state": None,
        "to_state": "unlocked",
        "drawdown_ratio": 0,
        "reason_code": "PAPER_ACCOUNT_INITIALIZED",
    }
    assert seed["task19_counts"] == {table: 0 for table in TASK19_TABLES}

    command.downgrade(config, "0013_shadow")
    assert PAPER_TABLES.isdisjoint(asyncio.run(_table_names(temporary_database_url)))

    command.upgrade(config, "head")
    assert PAPER_TABLES <= asyncio.run(_table_names(temporary_database_url))
    replayed = asyncio.run(_paper_seed(temporary_database_url))
    assert replayed["account"] == seed["account"]
    assert replayed["funding"] == seed["funding"]
