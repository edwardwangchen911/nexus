"""Raw-SQL bypass tests for Task 18 paper-account database invariants."""

import asyncio
from collections.abc import Iterable

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

PAPER_ACCOUNT_ID = "d31672c0-3922-5ebd-8fdb-cd3174266731"
RISK_POLICY_ID = "2a3bf2d3-85c2-524b-abe3-d37112685caf"
AVAILABLE_CASH_ID = "065e4d75-50c0-5ed3-b24c-723855334a58"
RESERVED_CASH_ID = "96993de2-2715-5945-8e83-72128e4f69a8"
CONTRIBUTED_CAPITAL_ID = "2865b289-4871-5d60-a4f7-5a219122f18c"

SOURCE_POLICY_ID = "10000000-0000-0000-0000-000000000001"
INSTRUMENT_ID = "10000000-0000-0000-0000-000000000002"
DATA_SNAPSHOT_ID = "10000000-0000-0000-0000-000000000003"
QUALITY_REPORT_ID = "10000000-0000-0000-0000-000000000004"
CALENDAR_ID = "10000000-0000-0000-0000-000000000005"
UNIVERSE_ID = "10000000-0000-0000-0000-000000000006"
IDEMPOTENCY_ID = "10000000-0000-0000-0000-000000000007"
DECISION_ID = "10000000-0000-0000-0000-000000000008"
TARGET_ID = "10000000-0000-0000-0000-000000000009"
BATCH_ID = "10000000-0000-0000-0000-00000000000a"
ORDER_ID = "10000000-0000-0000-0000-00000000000b"
EVALUATION_ID = "10000000-0000-0000-0000-00000000000c"
RULE_ID = "10000000-0000-0000-0000-00000000000d"
RESERVE_TRANSACTION_ID = "10000000-0000-0000-0000-00000000000e"
RESERVE_DEBIT_ID = "10000000-0000-0000-0000-00000000000f"
RESERVE_CREDIT_ID = "10000000-0000-0000-0000-000000000010"
RESERVATION_ID = "10000000-0000-0000-0000-000000000011"
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64
HASH_E = "e" * 64
HASH_F = "f" * 64

PAPER_TABLES = {
    "risk_policies",
    "paper_accounts",
    "strategy_decisions",
    "strategy_decision_targets",
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


async def _execute(connection: AsyncConnection, statements: Iterable[str]) -> None:
    for statement in statements:
        await connection.execute(text(statement))


async def _seed_order_graph(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await _execute(
                connection,
                (
                    f"INSERT INTO source_policies "
                    f"(id, key, version, schema_version, payload) VALUES "
                    f"('{SOURCE_POLICY_ID}', 'paper_test', 'v1', 'v1', '{{}}'::jsonb)",
                    f"INSERT INTO instruments "
                    f"(id, asset_type, exchange, currency, tick_size, risk_bucket) VALUES "
                    f"('{INSTRUMENT_ID}', 'common_stock', 'NASDAQ', 'USD', 0.01, 'technology')",
                    f"INSERT INTO data_snapshots "
                    f"(id, source, provider_object_id, revision, observed_at, available_at, "
                    f"received_at, schema_version, payload) VALUES "
                    f"('{DATA_SNAPSHOT_ID}', 'fixture', 'paper-quality', 'v1', "
                    f"'2026-08-21T20:00:00Z', '2026-08-21T20:00:00Z', "
                    f"'2026-08-21T20:00:00Z', 'v1', '{{}}'::jsonb)",
                    f"INSERT INTO quality_reports "
                    f"(id, data_snapshot_id, status, schema_version, payload) VALUES "
                    f"('{QUALITY_REPORT_ID}', '{DATA_SNAPSHOT_ID}', 'PASS', 'v1', '{{}}'::jsonb)",
                    f"INSERT INTO exchange_calendar_authorities "
                    f"(id, exchange, version, source, range_start, range_end, query_parameters, "
                    f"completeness_status, content_hash, published_at) VALUES "
                    f"('{CALENDAR_ID}', 'XNYS', 'paper-v1', 'fixture', '2026-08-01', "
                    f"'2026-08-31', '{{}}'::jsonb, 'complete', '{HASH_A}', "
                    f"'2026-08-21T20:00:00Z')",
                    f"INSERT INTO universe_snapshots "
                    f"(id, key, version, effective_from, source, content_hash, "
                    f"published_at) VALUES "
                    f"('{UNIVERSE_ID}', 'paper-us', 'v1', '2026-08-01', 'fixture', "
                    f"'{HASH_B}', '2026-08-21T20:00:00Z')",
                    f"INSERT INTO idempotency_records "
                    f"(id, actor_id, route, idempotency_key, request_hash, schema_version, "
                    f"response_payload) VALUES ('{IDEMPOTENCY_ID}', 'paper-service', "
                    f"'/paper/orders', 'paper-test-key', '{HASH_C}', 'v1', '{{}}'::jsonb)",
                    f"INSERT INTO strategy_decisions "
                    f"(id, account_id, portfolio_id, strategy_id, strategy_version, "
                    f"decision_session, decision_time, execution_session, source_policy_id, "
                    f"calendar_authority_id, universe_snapshot_id, quality_report_id, "
                    f"portfolio_projection_hash, authority_hash, strategy_parameters_hash, "
                    f"decision_hash, cash_weight, reason) VALUES "
                    f"('{DECISION_ID}', '{PAPER_ACCOUNT_ID}', "
                    f"'ee2ff673-eaf6-5076-bbab-f4c9cedfc74e', 'baseline', 'baseline_v1', "
                    f"'2026-08-21', '2026-08-21T20:00:00Z', '2026-08-24', "
                    f"'{SOURCE_POLICY_ID}', '{CALENDAR_ID}', '{UNIVERSE_ID}', "
                    f"'{QUALITY_REPORT_ID}', '{HASH_A}', '{HASH_B}', '{HASH_C}', "
                    f"'{HASH_D}', 0.900000000000, 'WEEKLY_REBALANCE')",
                    f"INSERT INTO strategy_decision_targets "
                    f"(id, strategy_decision_id, account_id, instrument_id, target_weight, "
                    f"exit, strategy_priority, reason) VALUES "
                    f"('{TARGET_ID}', '{DECISION_ID}', '{PAPER_ACCOUNT_ID}', "
                    f"'{INSTRUMENT_ID}', 0.100000000000, false, 2, 'NEW_TARGET')",
                    f"INSERT INTO paper_order_batches "
                    f"(id, account_id, strategy_decision_id, risk_policy_id, "
                    f"idempotency_record_id, request_hash, projection_hash, risk_input_hash, "
                    f"result_hash, batch_hash, evaluated_at) VALUES "
                    f"('{BATCH_ID}', '{PAPER_ACCOUNT_ID}', '{DECISION_ID}', "
                    f"'{RISK_POLICY_ID}', '{IDEMPOTENCY_ID}', '{HASH_A}', '{HASH_B}', "
                    f"'{HASH_C}', '{HASH_D}', '{HASH_E}', '2026-08-21T20:01:00Z')",
                    f"INSERT INTO paper_orders "
                    f"(id, batch_id, account_id, strategy_decision_id, client_order_id, "
                    f"instrument_id, side, quantity, target_weight, reference_close, "
                    f"worst_price, risk_outcome) VALUES "
                    f"('{ORDER_ID}', '{BATCH_ID}', '{PAPER_ACCOUNT_ID}', '{DECISION_ID}', "
                    f"'paper-order-1', '{INSTRUMENT_ID}', 'buy', 1, 0.1, 100, 101, 'approved')",
                    f"INSERT INTO risk_evaluations "
                    f"(id, order_id, batch_id, account_id, risk_policy_id, outcome, "
                    f"desired_quantity, approved_quantity, target_weight, strategy_priority, "
                    f"reference_close, worst_price, reserve_amount, projected_available_cash, "
                    f"projected_cash_ratio, projected_name_ratio, input_hash, result_hash) VALUES "
                    f"('{EVALUATION_ID}', '{ORDER_ID}', '{BATCH_ID}', '{PAPER_ACCOUNT_ID}', "
                    f"'{RISK_POLICY_ID}', 'approved', 1, 1, 0.1, 2, 100, 101, 102, "
                    f"9898, 0.9898, 0.01, '{HASH_E}', '{HASH_F}')",
                    f"INSERT INTO risk_evaluation_rules "
                    f"(id, risk_evaluation_id, account_id, rule_seq, rule_code, passed, "
                    f"limit_value, observed_value, remaining_value, authority_ids) VALUES "
                    f"('{RULE_ID}', '{EVALUATION_ID}', '{PAPER_ACCOUNT_ID}', 1, "
                    f"'MINIMUM_CASH', true, 0.1, 0.9898, 0.8898, "
                    f"ARRAY['{CALENDAR_ID}'::uuid])",
                    f"INSERT INTO paper_ledger_transactions "
                    f"(id, account_id, order_id, transaction_type, reference_type, "
                    f"reference_hash, occurred_at, transaction_hash) VALUES "
                    f"('{RESERVE_TRANSACTION_ID}', '{PAPER_ACCOUNT_ID}', '{ORDER_ID}', "
                    f"'cash_reservation', 'cash_reservation', '{HASH_E}', "
                    f"'2026-08-21T20:01:00Z', '{HASH_F}')",
                    f"INSERT INTO paper_ledger_entries "
                    f"(id, transaction_id, account_id, entry_seq, ledger_account_id, "
                    f"account_code, currency, debit_amount, credit_amount) VALUES "
                    f"('{RESERVE_DEBIT_ID}', '{RESERVE_TRANSACTION_ID}', '{PAPER_ACCOUNT_ID}', "
                    f"1, '{RESERVED_CASH_ID}', 'asset.cash.reserved.usd', 'USD', 102, 0)",
                    f"INSERT INTO paper_ledger_entries "
                    f"(id, transaction_id, account_id, entry_seq, ledger_account_id, "
                    f"account_code, currency, debit_amount, credit_amount) VALUES "
                    f"('{RESERVE_CREDIT_ID}', '{RESERVE_TRANSACTION_ID}', '{PAPER_ACCOUNT_ID}', "
                    f"2, '{AVAILABLE_CASH_ID}', 'asset.cash.available.usd', 'USD', 0, 102)",
                    f"INSERT INTO paper_reservations "
                    f"(id, order_id, account_id, ledger_transaction_id, reservation_type, "
                    f"currency, instrument_id, amount) VALUES "
                    f"('{RESERVATION_ID}', '{ORDER_ID}', '{PAPER_ACCOUNT_ID}', "
                    f"'{RESERVE_TRANSACTION_ID}', 'cash', 'USD', NULL, 102)",
                ),
            )
    finally:
        await engine.dispose()


async def _expect_rejected(database_url: str, statements: Iterable[str]) -> None:
    engine = create_async_engine(database_url)
    try:
        with pytest.raises(DBAPIError):
            async with engine.begin() as connection:
                await _execute(connection, statements)
    finally:
        await engine.dispose()


async def _rows(database_url: str, statement: str) -> list[tuple[object, ...]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return list((await connection.execute(text(statement))).tuples())
    finally:
        await engine.dispose()


def test_every_paper_table_has_raw_mutation_guards_and_task19_starts_empty(
    migrated_database_url: str,
) -> None:
    trigger_rows = asyncio.run(
        _rows(
            migrated_database_url,
            "SELECT c.relname, t.tgname FROM pg_trigger t "
            "JOIN pg_class c ON c.oid = t.tgrelid "
            "WHERE NOT t.tgisinternal AND c.relname LIKE ANY "
            "(ARRAY['paper_%', 'risk_%', 'strategy_decision%'])",
        )
    )
    triggers = {(str(table), str(trigger)) for table, trigger in trigger_rows}
    for table in PAPER_TABLES:
        assert (table, f"trg_{table}_immutable") in triggers
        assert (table, f"trg_{table}_created_at_immutable") in triggers

    counts = asyncio.run(
        _rows(
            migrated_database_url,
            " UNION ALL ".join(
                f"SELECT '{table}', count(*) FROM {table}" for table in sorted(TASK19_TABLES)
            ),
        )
    )
    assert counts == [(table, 0) for table in sorted(TASK19_TABLES)]


@pytest.mark.parametrize(
    ("table", "row_id"),
    (
        ("paper_accounts", PAPER_ACCOUNT_ID),
        ("risk_policies", RISK_POLICY_ID),
        ("strategy_decisions", DECISION_ID),
        ("strategy_decision_targets", TARGET_ID),
        ("paper_order_batches", BATCH_ID),
        ("paper_orders", ORDER_ID),
        ("risk_evaluations", EVALUATION_ID),
        ("risk_evaluation_rules", RULE_ID),
        ("paper_ledger_transactions", RESERVE_TRANSACTION_ID),
        ("paper_ledger_entries", RESERVE_DEBIT_ID),
        ("paper_reservations", RESERVATION_ID),
    ),
)
def test_raw_update_and_delete_are_rejected(
    migrated_database_url: str,
    table: str,
    row_id: str,
) -> None:
    asyncio.run(_seed_order_graph(migrated_database_url))
    asyncio.run(
        _expect_rejected(
            migrated_database_url,
            (f"UPDATE {table} SET created_at = created_at WHERE id = '{row_id}'",),
        )
    )
    asyncio.run(
        _expect_rejected(
            migrated_database_url,
            (f"DELETE FROM {table} WHERE id = '{row_id}'",),
        )
    )


@pytest.mark.parametrize(
    "events",
    (
        ("(gen_random_uuid(), 2, 'proposed', 'risk_approved', 'task18', 'SEQ_GAP')",),
        ("(gen_random_uuid(), 1, 'proposed', 'proposed', 'task18', 'BAD_FROM')",),
        ("(gen_random_uuid(), 1, NULL, 'risk_approved', 'task18', 'SKIP_PROPOSED')",),
        (
            "(gen_random_uuid(), 1, NULL, 'proposed', 'task18', 'PROPOSED')",
            "(gen_random_uuid(), 2, 'proposed', 'risk_approved', 'task18', 'APPROVED')",
            "(gen_random_uuid(), 3, 'risk_approved', 'awaiting_approval', 'task18', 'NO')",
        ),
        (
            "(gen_random_uuid(), 1, NULL, 'proposed', 'task18', 'PROPOSED')",
            "(gen_random_uuid(), 2, 'proposed', 'risk_approved', 'task18', 'APPROVED')",
            "(gen_random_uuid(), 3, 'risk_approved', 'submitted', 'task19', 'WRONG_OWNER')",
        ),
        (
            "(gen_random_uuid(), 1, NULL, 'proposed', 'task18', 'PROPOSED')",
            "(gen_random_uuid(), 2, 'proposed', 'risk_rejected', 'task18', 'REJECTED')",
            "(gen_random_uuid(), 3, 'risk_rejected', 'submitted', 'user', 'TERMINAL')",
        ),
    ),
)
def test_invalid_order_event_chains_are_rejected(
    migrated_database_url: str,
    events: tuple[str, ...],
) -> None:
    asyncio.run(_seed_order_graph(migrated_database_url))
    prefix = (
        "INSERT INTO paper_order_events "
        "(id, order_id, account_id, event_seq, from_status, to_status, actor_kind, "
        "reason_code, occurred_at, payload_hash) SELECT event.id, "
        f"'{ORDER_ID}', '{PAPER_ACCOUNT_ID}', event.seq, event.from_status, "
        "event.to_status, event.actor, event.reason, '2026-08-21T20:02:00Z', "
        f"'{HASH_A}' FROM (VALUES "
    )
    values = ",".join(events)
    suffix = ") AS event(id, seq, from_status, to_status, actor, reason)"
    asyncio.run(_expect_rejected(migrated_database_url, (prefix + values + suffix,)))


@pytest.mark.parametrize(
    "statements",
    (
        (
            f"INSERT INTO paper_reservation_events VALUES "
            f"(gen_random_uuid(), '{RESERVATION_ID}', '{PAPER_ACCOUNT_ID}', 2, "
            f"'released', 1, '2026-08-21T20:03:00Z', '{HASH_A}', now())",
        ),
        (
            f"INSERT INTO paper_reservation_events VALUES "
            f"(gen_random_uuid(), '{RESERVATION_ID}', '{PAPER_ACCOUNT_ID}', 1, "
            f"'reserved', 102, '2026-08-21T20:03:00Z', '{HASH_A}', now())",
            f"INSERT INTO paper_reservation_events VALUES "
            f"(gen_random_uuid(), '{RESERVATION_ID}', '{PAPER_ACCOUNT_ID}', 2, "
            f"'consumed', 103, '2026-08-21T20:04:00Z', '{HASH_B}', now())",
        ),
    ),
)
def test_invalid_reservation_event_chains_are_rejected(
    migrated_database_url: str,
    statements: tuple[str, ...],
) -> None:
    asyncio.run(_seed_order_graph(migrated_database_url))
    asyncio.run(_expect_rejected(migrated_database_url, statements))


@pytest.mark.parametrize(
    "entry_values",
    (
        f"1, '{AVAILABLE_CASH_ID}', 'asset.cash.available.usd', 'USD', 1, 1",
        f"1, '{AVAILABLE_CASH_ID}', 'asset.cash.available.usd', 'USD', 0, 0",
        f"1, '{AVAILABLE_CASH_ID}', 'asset.cash.available.usd', 'EUR', 1, 0",
        "1, '99999999-9999-9999-9999-999999999999', 'asset.unknown.usd', 'USD', 1, 0",
    ),
)
def test_invalid_ledger_entries_are_rejected(
    migrated_database_url: str,
    entry_values: str,
) -> None:
    transaction_id = "20000000-0000-0000-0000-000000000001"
    statements = (
        f"INSERT INTO paper_ledger_transactions "
        f"(id, account_id, order_id, transaction_type, reference_type, reference_hash, "
        f"occurred_at, transaction_hash) VALUES ('{transaction_id}', "
        f"'{PAPER_ACCOUNT_ID}', NULL, 'funding', 'initial_funding', '{HASH_A}', now(), "
        f"'{HASH_B}')",
        f"INSERT INTO paper_ledger_entries "
        f"(id, transaction_id, account_id, entry_seq, ledger_account_id, account_code, "
        f"currency, debit_amount, credit_amount) VALUES (gen_random_uuid(), "
        f"'{transaction_id}', '{PAPER_ACCOUNT_ID}', {entry_values})",
    )
    asyncio.run(_expect_rejected(migrated_database_url, statements))


def test_unbalanced_and_empty_ledger_transactions_fail_at_commit(
    migrated_database_url: str,
) -> None:
    empty_id = "20000000-0000-0000-0000-000000000002"
    asyncio.run(
        _expect_rejected(
            migrated_database_url,
            (
                f"INSERT INTO paper_ledger_transactions "
                f"(id, account_id, order_id, transaction_type, reference_type, "
                f"reference_hash, occurred_at, transaction_hash) VALUES "
                f"('{empty_id}', '{PAPER_ACCOUNT_ID}', NULL, 'funding', "
                f"'initial_funding', '{HASH_C}', now(), '{HASH_D}')",
            ),
        )
    )

    unbalanced_id = "20000000-0000-0000-0000-000000000003"
    asyncio.run(
        _expect_rejected(
            migrated_database_url,
            (
                f"INSERT INTO paper_ledger_transactions "
                f"(id, account_id, order_id, transaction_type, reference_type, "
                f"reference_hash, occurred_at, transaction_hash) VALUES "
                f"('{unbalanced_id}', '{PAPER_ACCOUNT_ID}', NULL, 'funding', "
                f"'initial_funding', '{HASH_E}', now(), '{HASH_F}')",
                f"INSERT INTO paper_ledger_entries "
                f"(id, transaction_id, account_id, entry_seq, ledger_account_id, "
                f"account_code, currency, debit_amount, credit_amount) VALUES "
                f"(gen_random_uuid(), '{unbalanced_id}', '{PAPER_ACCOUNT_ID}', 1, "
                f"'{AVAILABLE_CASH_ID}', 'asset.cash.available.usd', 'USD', 1, 0)",
            ),
        )
    )


def test_duplicate_roots_and_task19_ownership_boundary(
    migrated_database_url: str,
) -> None:
    asyncio.run(_seed_order_graph(migrated_database_url))
    duplicate_statements = (
        f"INSERT INTO paper_orders "
        f"(id, batch_id, account_id, strategy_decision_id, client_order_id, instrument_id, "
        f"side, quantity, target_weight, reference_close, worst_price, risk_outcome) VALUES "
        f"(gen_random_uuid(), '{BATCH_ID}', '{PAPER_ACCOUNT_ID}', '{DECISION_ID}', "
        f"'paper-order-1', '{INSTRUMENT_ID}', 'buy', 1, 0.1, 100, 101, 'approved')",
    )
    asyncio.run(_expect_rejected(migrated_database_url, duplicate_statements))
    counts = asyncio.run(
        _rows(
            migrated_database_url,
            " UNION ALL ".join(
                f"SELECT '{table}', count(*) FROM {table}" for table in sorted(TASK19_TABLES)
            ),
        )
    )
    assert counts == [(table, 0) for table in sorted(TASK19_TABLES)]
