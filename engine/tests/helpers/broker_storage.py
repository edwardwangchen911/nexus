import base64
from datetime import UTC, datetime
from uuid import UUID

from nexus.persistence.models.portfolio import Account
from sqlalchemy import text

NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)
ACCOUNT_A = UUID("0198d400-5000-7000-8000-000000000001")
ACCOUNT_B = UUID("0198d400-5000-7000-8000-000000000002")
PORTFOLIO_A = UUID("0198d400-5000-7000-8000-000000000003")
PORTFOLIO_B = UUID("0198d400-5000-7000-8000-000000000004")
OLD_KEY = base64.urlsafe_b64encode(b"o" * 32).decode()
CURRENT_KEY = base64.urlsafe_b64encode(b"c" * 32).decode()
BLIND_KEY = base64.urlsafe_b64encode(b"b" * 32).decode()


async def _seed_accounts(factory, *, include_second: bool = True) -> None:
    async with factory() as session:
        pairs = [(ACCOUNT_A, PORTFOLIO_A)]
        if include_second:
            pairs.append((ACCOUNT_B, PORTFOLIO_B))
        for account_id, portfolio_id in pairs:
            session.add(
                Account(
                    id=account_id,
                    provider="fosun",
                    status="ACTIVE",
                )
            )
            await session.flush()
            await session.execute(
                text(
                    "INSERT INTO portfolios (id, account_id, name) "
                    "VALUES (:id, :account_id, 'Synthetic')"
                ),
                {"id": portfolio_id, "account_id": account_id},
            )
        await session.commit()


async def _insert_run(session, run_id, account_id, portfolio_id, status, suffix) -> None:
    await session.execute(
        text(
            "INSERT INTO broker_sync_runs "
            "(id, account_id, portfolio_id, trigger_kind, actor_id, route, idempotency_key, "
            "request_hash, status, accepted_at, completed_at, attempt_count, failure_reason, "
            "cash_flow_quality) VALUES (:id, :account_id, :portfolio_id, 'AUTOMATIC', "
            ":actor, 'broker-sync', :key, :hash, :status, :at, :completed, 1, NULL, 'NOT_RUN')"
        ),
        {
            "id": run_id,
            "account_id": account_id,
            "portfolio_id": portfolio_id,
            "actor": f"actor-{suffix}",
            "key": f"key-{suffix}",
            "hash": suffix * 64,
            "status": status,
            "at": NOW,
            "completed": NOW if status != "ACTIVE" else None,
        },
    )


async def _insert_snapshot(session, snapshot_id, portfolio_id, run_id, status, stale_id) -> None:
    await session.execute(
        text(
            "INSERT INTO portfolio_snapshots "
            "(id, portfolio_id, sync_run_id, stale_from_snapshot_id, status, as_of, "
            "position_hash_version, position_hash, order_hash_version, order_hash, "
            "source_request_ids, source_observations) VALUES "
            "(:id, :portfolio_id, :run_id, :stale_id, :status, :as_of, "
            "'broker_position_hash_v1', :hash, 'broker_order_hash_v1', :hash, '[]', '[]')"
        ),
        {
            "id": snapshot_id,
            "portfolio_id": portfolio_id,
            "run_id": run_id,
            "stale_id": stale_id,
            "status": status,
            "as_of": NOW,
            "hash": "a" * 64,
        },
    )
