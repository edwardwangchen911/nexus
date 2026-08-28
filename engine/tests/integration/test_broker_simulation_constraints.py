from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine


async def _insert_binding(database_url: str, **overrides: object) -> None:
    values: dict[str, object] = {
        "id": UUID(int=501),
        "provider": "fosun",
        "environment": "simulation",
        "account_type": "simulation",
        "redacted_label": "Fosun SIM ••••01",
        "external_account_id_digest": "a" * 64,
        "snapshot_identity_hash": "b" * 64,
        "capability_subject": "fixture-contract-v1",
        "capability_expires_at": datetime(2026, 8, 28, 15, tzinfo=UTC),
        "reconciliation_status": "matched",
        "locked": False,
        "version": 1,
    }
    values.update(overrides)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO broker_account_bindings "
                    "(id,provider,environment,account_type,redacted_label,"
                    "external_account_id_digest,snapshot_identity_hash,capability_subject,"
                    "capability_expires_at,reconciliation_status,locked,version) VALUES "
                    "(:id,:provider,:environment,:account_type,:redacted_label,"
                    ":external_account_id_digest,:snapshot_identity_hash,:capability_subject,"
                    ":capability_expires_at,:reconciliation_status,:locked,:version)"
                ),
                values,
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("provider", "other"),
        ("environment", "live"),
        ("account_type", "live"),
        ("external_account_id_digest", "not-a-hash"),
        ("reconciliation_status", "unknown"),
        ("version", 0),
    ],
)
async def test_account_binding_is_simulation_only_and_content_addressed(
    migrated_database_url: str, field: str, value: object
) -> None:
    with pytest.raises(IntegrityError):
        await _insert_binding(migrated_database_url, **{field: value})


@pytest.mark.asyncio
async def test_intent_body_hash_and_approval_are_unique(migrated_database_url: str) -> None:
    await _insert_binding(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    intent_values = {
        "id": UUID(int=601),
        "binding": UUID(int=501),
        "proposal": UUID(int=602),
        "decision": UUID(int=603),
        "portfolio": UUID(int=604),
        "body_hash": "c" * 64,
        "risk_hash": "d" * 64,
        "execution_hash": "e" * 64,
    }
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO broker_execution_intents "
                    "(id,account_binding_id,proposal_group_id,strategy_decision_id,"
                    "portfolio_snapshot_id,evidence_hashes,risk_policy_hash,"
                    "execution_model_hash,status,version,body_hash,total_notional,"
                    "prepared_at,expires_at) VALUES "
                    "(:id,:binding,:proposal,:decision,:portfolio,ARRAY[repeat('a',64)],"
                    ":risk_hash,:execution_hash,'prepared',1,:body_hash,100,"
                    "'2026-08-28T14:00:00Z','2026-08-28T14:05:00Z')"
                ),
                intent_values,
            )
        with pytest.raises(IntegrityError):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO broker_execution_intents "
                        "(id,account_binding_id,proposal_group_id,strategy_decision_id,"
                        "portfolio_snapshot_id,evidence_hashes,risk_policy_hash,"
                        "execution_model_hash,status,version,body_hash,total_notional,"
                        "prepared_at,expires_at) VALUES "
                        "('00000000-0000-0000-0000-000000000605',:binding,:proposal,"
                        ":decision,:portfolio,ARRAY[repeat('a',64)],:risk_hash,"
                        ":execution_hash,'prepared',1,:body_hash,100,"
                        "'2026-08-28T14:00:00Z','2026-08-28T14:05:00Z')"
                    ),
                    intent_values,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_admission_enum_cannot_persist_live_capability(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    try:
        with pytest.raises(IntegrityError):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO broker_simulation_admissions "
                        "(id,subject,manifest_hash,artifact_hash,capability_status,"
                        "valid_from,valid_until,revoked) VALUES "
                        "('00000000-0000-0000-0000-000000000701','forbidden',"
                        "repeat('a',64),repeat('b',64),'LIVE_WRITE',"
                        "'2026-08-28T14:00:00Z','2026-08-28T15:00:00Z',false)"
                    )
                )
    finally:
        await engine.dispose()


async def _seed_command_authority(database_url: str, *, with_command: bool = True) -> None:
    await _insert_binding(database_url)
    statements = [
        "INSERT INTO instruments (id,asset_type,exchange,currency,tick_size,risk_bucket) "
        "VALUES ('00000000-0000-0000-0000-000000000801','common_stock','XNYS','USD',"
        "0.01,'large_cap')",
        "INSERT INTO broker_execution_intents "
        "(id,account_binding_id,proposal_group_id,strategy_decision_id,portfolio_snapshot_id,"
        "evidence_hashes,risk_policy_hash,execution_model_hash,status,version,body_hash,"
        "total_notional,prepared_at,expires_at) VALUES "
        "('00000000-0000-0000-0000-000000000802',"
        "'00000000-0000-0000-0000-0000000001f5',"
        "'00000000-0000-0000-0000-000000000803',"
        "'00000000-0000-0000-0000-000000000804',"
        "'00000000-0000-0000-0000-000000000805',ARRAY[repeat('a',64)],"
        "repeat('b',64),repeat('c',64),'approved',1,repeat('d',64),100,"
        "'2026-08-28T14:00:00Z','2026-08-28T14:05:00Z')",
        "INSERT INTO broker_execution_intent_orders "
        "(id,intent_id,ordinal,instrument_id,side,quantity,limit_price,currency,order_type,"
        "time_in_force,session,status,order_hash) VALUES "
        "('00000000-0000-0000-0000-000000000806',"
        "'00000000-0000-0000-0000-000000000802',1,"
        "'00000000-0000-0000-0000-000000000801','buy',10,10,'USD','limit','DAY',"
        "'regular','not_sent',repeat('e',64))",
        "INSERT INTO broker_batch_approvals "
        "(id,intent_id,intent_version,intent_body_hash,request_body_hash,actor_id,"
        "idempotency_key,approved_at,expires_at) VALUES "
        "('00000000-0000-0000-0000-000000000807',"
        "'00000000-0000-0000-0000-000000000802',1,repeat('d',64),repeat('f',64),"
        "'private-user','approve-1','2026-08-28T14:00:01Z','2026-08-28T14:05:00Z')",
    ]
    if with_command:
        statements.append(
            "INSERT INTO broker_commands "
            "(id,intent_order_id,account_binding_id,approval_id,kind,status,version,"
            "expected_event_sequence,command_hash,idempotency_key) VALUES "
            "('00000000-0000-0000-0000-000000000808',"
            "'00000000-0000-0000-0000-000000000806',"
            "'00000000-0000-0000-0000-0000000001f5',"
            "'00000000-0000-0000-0000-000000000807','create','sending',1,0,"
            "repeat('1',64),'dispatch-1')"
        )
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            for statement in statements:
                await connection.execute(text(statement))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_command_has_one_attempt_and_order_events_are_monotonic_immutable(
    migrated_database_url: str,
) -> None:
    await _seed_command_authority(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO broker_command_attempts "
                    "(id,command_id,attempt_number,started_at) VALUES "
                    "('00000000-0000-0000-0000-000000000809',"
                    "'00000000-0000-0000-0000-000000000808',1,"
                    "'2026-08-28T14:01:00Z')"
                )
            )
        with pytest.raises(IntegrityError):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO broker_command_attempts "
                        "(id,command_id,attempt_number,started_at) VALUES "
                        "('00000000-0000-0000-0000-000000000810',"
                        "'00000000-0000-0000-0000-000000000808',1,"
                        "'2026-08-28T14:01:01Z')"
                    )
                )
        with pytest.raises(DBAPIError, match="sequence is not monotonic"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO broker_order_events "
                        "(id,account_binding_id,intent_order_id,command_id,event_sequence,"
                        "source,order_status,occurred_at,payload_hash,previous_hash,event_hash) "
                        "VALUES ('00000000-0000-0000-0000-000000000811',"
                        "'00000000-0000-0000-0000-0000000001f5',"
                        "'00000000-0000-0000-0000-000000000806',"
                        "'00000000-0000-0000-0000-000000000808',2,'internal','not_sent',"
                        "'2026-08-28T14:01:00Z',repeat('2',64),repeat('0',64),repeat('3',64))"
                    )
                )
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO broker_order_events "
                    "(id,account_binding_id,intent_order_id,command_id,event_sequence,source,"
                    "order_status,occurred_at,payload_hash,previous_hash,event_hash) VALUES "
                    "('00000000-0000-0000-0000-000000000812',"
                    "'00000000-0000-0000-0000-0000000001f5',"
                    "'00000000-0000-0000-0000-000000000806',"
                    "'00000000-0000-0000-0000-000000000808',1,'internal','not_sent',"
                    "'2026-08-28T14:01:00Z',repeat('2',64),repeat('0',64),repeat('3',64))"
                )
            )
        with pytest.raises(DBAPIError, match="events are immutable"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE broker_order_events SET order_status='acknowledged' "
                        "WHERE id='00000000-0000-0000-0000-000000000812'"
                    )
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_locked_account_rejects_new_command(migrated_database_url: str) -> None:
    await _seed_command_authority(migrated_database_url, with_command=False)
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE broker_account_bindings SET locked=true,lock_reason='conflict',"
                    "reconciliation_status='conflict' WHERE "
                    "id='00000000-0000-0000-0000-0000000001f5'"
                )
            )
        with pytest.raises(DBAPIError, match="not reconciled for a new command"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO broker_commands "
                        "(id,intent_order_id,account_binding_id,approval_id,kind,status,version,"
                        "expected_event_sequence,command_hash,idempotency_key) VALUES "
                        "('00000000-0000-0000-0000-000000000813',"
                        "'00000000-0000-0000-0000-000000000806',"
                        "'00000000-0000-0000-0000-0000000001f5',"
                        "'00000000-0000-0000-0000-000000000807','create','prepared',1,0,"
                        "repeat('4',64),'dispatch-locked')"
                    )
                )
    finally:
        await engine.dispose()
