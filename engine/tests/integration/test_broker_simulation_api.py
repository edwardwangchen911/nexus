import json
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import nexus.api.routes.broker_simulation as broker_routes
import pytest
from helpers.broker_simulation import (
    BINDING_ID,
    INSTRUMENT_IDS,
    NOW,
    preparation,
    seed_broker_authority,
)
from helpers.hmac import (
    TEST_HMAC_KEY_ID,
    TEST_HMAC_SECRET,
    build_test_hmac_headers,
)
from nexus.api.routes.broker_simulation import _preparation_from_proposal
from nexus.api.schemas.broker_simulation import BrokerIntentPrepareRequest
from nexus.broker_simulation.domain import canonical_document_hash
from nexus.broker_simulation.intents import (
    AgentPaperIntentSource,
    AgentPaperIntentSourceOrder,
    BrokerIntentError,
    prepare_execution_intent,
)
from nexus.domain.ids import new_uuid7
from nexus.main import create_engine_app
from nexus.persistence.models.broker_simulation import (
    BrokerAccountBindingRecord,
    BrokerBatchApprovalRecord,
    BrokerCommandRecord,
    BrokerExecutionIntentRecord,
)
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

_NOW = datetime(2026, 8, 28, 14, 1, tzinfo=UTC)
_BASE = "/internal/v1/broker-simulation"
_ACCOUNT_ID = new_uuid7()
_PORTFOLIO_ID = new_uuid7()
_SYNC_RUN_ID = new_uuid7()
_SNAPSHOT_ID = new_uuid7()
_CYCLE_ID = new_uuid7()


def _settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


def _body(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _headers(
    method: str,
    path: str,
    body: bytes = b"",
    *,
    idempotency_key: str | None = None,
    if_match: str | None = None,
) -> dict[str, str]:
    headers = build_test_hmac_headers(method, path, body, nonce=str(new_uuid7()))
    if body:
        headers["Content-Type"] = "application/json"
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    if if_match is not None:
        headers["If-Match"] = if_match
    return headers


async def _seed_agent_broker_snapshot(database_url: str) -> None:
    await seed_broker_authority(database_url)
    factory = session_factory(_settings(database_url))
    snapshot_identity = canonical_document_hash(
        {
            "schema": "broker_account_snapshot_identity_v1",
            "portfolio_snapshot_id": _SNAPSHOT_ID,
            "portfolio_id": _PORTFOLIO_ID,
            "as_of": NOW,
            "position_hash": "1" * 64,
            "order_hash": "2" * 64,
            "cash": (
                {
                    "currency": "USD",
                    "cash_balance": Decimal("10000"),
                    "available_cash": Decimal("10000"),
                    "provider_as_of": NOW,
                },
            ),
        }
    )
    async with factory() as session, session.begin():
        binding = await session.get(BrokerAccountBindingRecord, BINDING_ID)
        assert binding is not None
        binding.snapshot_identity_hash = snapshot_identity
        session.add_all(
            (
                Account(id=_ACCOUNT_ID, provider="fosun", status="ACTIVE"),
                Portfolio(id=_PORTFOLIO_ID, account_id=_ACCOUNT_ID, name="Fixture SIM"),
            )
        )
        await session.flush()
        session.add(
            BrokerSyncRun(
                id=_SYNC_RUN_ID,
                account_id=_ACCOUNT_ID,
                portfolio_id=_PORTFOLIO_ID,
                trigger_kind="MANUAL",
                actor_id="fixture",
                route="fixture",
                idempotency_key="agent-paper-api-snapshot",
                request_hash="3" * 64,
                status="READY",
                accepted_at=NOW,
                completed_at=NOW,
                attempt_count=1,
                failure_reason=None,
                cash_flow_quality="PASS",
            )
        )
        await session.flush()
        session.add(
            PortfolioSnapshotRecord(
                id=_SNAPSHOT_ID,
                portfolio_id=_PORTFOLIO_ID,
                sync_run_id=_SYNC_RUN_ID,
                stale_from_snapshot_id=None,
                status="READY",
                as_of=NOW,
                position_hash_version="position_snapshot_v1",
                position_hash="1" * 64,
                order_hash_version="order_snapshot_v1",
                order_hash="2" * 64,
                source_request_ids=["fixture"],
                source_observations=[],
            )
        )
        await session.flush()
        session.add_all(
            (
                CashSnapshot(
                    portfolio_snapshot_id=_SNAPSHOT_ID,
                    currency="USD",
                    cash_balance=Decimal("10000"),
                    available_cash=Decimal("10000"),
                    provider_as_of=NOW,
                ),
                LatestQuote(
                    instrument_id=INSTRUMENT_IDS[0],
                    source="fixture",
                    feed="fixture",
                    delivery_mode="fixture",
                    bid=Decimal("99.99"),
                    ask=Decimal("100.00"),
                    last=Decimal("99.995"),
                    observed_at=NOW,
                    available_at=NOW,
                    received_at=NOW,
                    revision="fixture-v1",
                    quality_flags=[],
                    event_kind="fixture",
                    content_hash="4" * 64,
                    evidence_status="fixture",
                    data_artifact_id=None,
                ),
            )
        )


def _agent_paper_source() -> AgentPaperIntentSource:
    return AgentPaperIntentSource(
        cycle_id=_CYCLE_ID,
        strategy_decision_id=new_uuid7(),
        paper_order_batch_id=new_uuid7(),
        execution_locked=True,
        risk_policy_hash="5" * 64,
        execution_model_hash="6" * 64,
        session_close_at=datetime(2026, 8, 28, 20, tzinfo=UTC),
        evidence_hashes=("5" * 64, "6" * 64, "7" * 64),
        orders=(
            AgentPaperIntentSourceOrder(
                paper_order_id=new_uuid7(),
                instrument_id=INSTRUMENT_IDS[0],
                side="buy",
                quantity=10,
                risk_evaluation_id=new_uuid7(),
                risk_result_hash="7" * 64,
                fill_hash="8" * 64,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_prepare_rejects_display_only_prices_before_loading_proposal_data() -> None:
    class _NoQuerySession:
        async def get(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("display-only price policy must fail before database lookup")

    with pytest.raises(BrokerIntentError, match="^PRICE_AUTHORITY_UNAVAILABLE$"):
        await _preparation_from_proposal(
            _NoQuerySession(),  # type: ignore[arg-type]
            payload=BrokerIntentPrepareRequest(
                proposal_group_id="00000000-0000-0000-0000-000000000001",
                proposal_id="00000000-0000-0000-0000-000000000002",
            ),
            now=_NOW,
        )


def test_broker_simulation_openapi_is_exact_and_closed() -> None:
    document = create_engine_app(EngineSettings.for_test()).openapi()
    paths = {
        path for path in document["paths"] if path.startswith("/internal/v1/broker-simulation")
    }

    assert paths == {
        _BASE,
        f"{_BASE}/intents",
        f"{_BASE}/intents/{{intent_id}}/approve",
        f"{_BASE}/intents/{{intent_id}}/reject",
        f"{_BASE}/orders/{{order_id}}/modify",
        f"{_BASE}/orders/{{order_id}}/cancel",
        f"{_BASE}/reconciliation/{{run_id}}/acknowledge",
    }
    for name in (
        "BrokerSimulationResponse",
        "BrokerIntentResponse",
        "BrokerActionResponse",
    ):
        assert document["components"]["schemas"][name]["additionalProperties"] is False


@pytest.mark.asyncio
async def test_all_broker_simulation_routes_require_authenticated_service_actor(
    migrated_database_url: str,
) -> None:
    app = create_engine_app(_settings(migrated_database_url))
    item_id = "00000000-0000-0000-0000-000000000001"
    requests = (
        ("GET", _BASE, None),
        ("POST", f"{_BASE}/intents", {"proposal_group_id": item_id, "proposal_id": item_id}),
        ("POST", f"{_BASE}/intents/{item_id}/approve", {"version": 1}),
        ("POST", f"{_BASE}/intents/{item_id}/reject", {"version": 1, "reason": "user_rejected"}),
        (
            "POST",
            f"{_BASE}/orders/{item_id}/modify",
            {"expected_event_sequence": 0, "reason": "replace_price"},
        ),
        (
            "POST",
            f"{_BASE}/orders/{item_id}/cancel",
            {"expected_event_sequence": 0, "reason": "user_cancel"},
        ),
        ("POST", f"{_BASE}/reconciliation/{item_id}/acknowledge", {}),
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            responses = []
            for method, path, payload in requests:
                responses.append(
                    await client.request(method, path, json=payload)
                    if payload is not None
                    else await client.get(path)
                )
        assert {response.status_code for response in responses} == {401}
    finally:
        await app.state.session_factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_agent_paper_prepare_is_idempotent_and_visible_without_dispatch(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _seed_agent_broker_snapshot(migrated_database_url)
    source = _agent_paper_source()

    async def _load_source(
        _session: object, *, cycle_id: object, now: datetime
    ) -> AgentPaperIntentSource:
        assert cycle_id == _CYCLE_ID
        assert now == _NOW
        return source

    monkeypatch.setattr(
        broker_routes,
        "load_agent_paper_intent_source",
        _load_source,
        raising=False,
    )
    app = create_engine_app(_settings(migrated_database_url))
    app.state.utc_clock = lambda: _NOW
    path = f"{_BASE}/intents"
    body = _body(
        {
            "agent_paper_cycle_id": str(_CYCLE_ID),
            "account_binding_id": str(BINDING_ID),
            "portfolio_snapshot_id": str(_SNAPSHOT_ID),
        }
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            first = await client.post(
                path,
                content=body,
                headers=_headers(
                    "POST",
                    path,
                    body,
                    idempotency_key="agent-paper-prepare-1",
                ),
            )
            replay = await client.post(
                path,
                content=body,
                headers=_headers(
                    "POST",
                    path,
                    body,
                    idempotency_key="agent-paper-prepare-1",
                ),
            )
            workbench = await client.get(_BASE, headers=_headers("GET", _BASE))

        assert first.status_code == replay.status_code == workbench.status_code == 200
        assert first.json()["data"]["status"] == "prepared"
        order = first.json()["data"]["orders"][0]
        assert {
            key: order[key]
            for key in ("instrument_id", "side", "quantity", "limit_price", "status")
        } == {
            "instrument_id": str(INSTRUMENT_IDS[0]),
            "side": "buy",
            "quantity": "10.000000000000",
            "limit_price": "100.000000000000",
            "status": "not_sent",
        }
        assert replay.json()["data"]["intent_id"] == first.json()["data"]["intent_id"]
        assert workbench.json()["data"]["intent"] == first.json()["data"]
        assert workbench.json()["data"]["actions"]["can_dispatch"] is False

        factory = session_factory(_settings(migrated_database_url))
        async with factory() as session:
            counts: list[int] = []
            for model in (
                BrokerExecutionIntentRecord,
                BrokerBatchApprovalRecord,
                BrokerCommandRecord,
                Job,
            ):
                counts.append(
                    int(await session.scalar(select(func.count()).select_from(model)) or 0)
                )
        assert tuple(counts) == (1, 0, 0, 0)
    finally:
        await app.state.session_factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_offline_view_is_redacted_and_approval_is_idempotent_without_dispatch(
    migrated_database_url: str,
) -> None:
    await seed_broker_authority(migrated_database_url)
    factory = session_factory(_settings(migrated_database_url))
    authority = preparation()
    async with factory() as session, session.begin():
        prepared = await prepare_execution_intent(session, authority)
    app = create_engine_app(_settings(migrated_database_url))
    app.state.utc_clock = lambda: _NOW
    approve_path = f"{_BASE}/intents/{prepared.intent_id}/approve"
    approve_body = _body({"version": prepared.version})
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            view = await client.get(_BASE, headers=_headers("GET", _BASE))
            stale = await client.post(
                approve_path,
                content=approve_body,
                headers=_headers(
                    "POST",
                    approve_path,
                    approve_body,
                    idempotency_key="approve-stale",
                    if_match="0" * 64,
                ),
            )
            first = await client.post(
                approve_path,
                content=approve_body,
                headers=_headers(
                    "POST",
                    approve_path,
                    approve_body,
                    idempotency_key="approve-offline-1",
                    if_match=prepared.body_hash,
                ),
            )
            replay = await client.post(
                approve_path,
                content=approve_body,
                headers=_headers(
                    "POST",
                    approve_path,
                    approve_body,
                    idempotency_key="approve-offline-1",
                    if_match=prepared.body_hash,
                ),
            )
        assert view.status_code == 200
        assert view.json()["data"]["capability"] == {
            "status": "offline_ready",
            "external_activation": "external_activation_blocked",
            "live_trading": False,
            "simulation_write_enabled": False,
        }
        assert view.json()["data"]["account"]["redacted_label"] == "Fixture SIM"
        assert "external_account" not in view.text
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "VERSION_CONFLICT"
        assert first.status_code == replay.status_code == 200
        assert first.json()["data"]["status"] == "approved"
        assert replay.json()["data"]["replayed"] is True
        assert first.json()["data"]["dispatch_created"] is False
    finally:
        await app.state.session_factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_external_order_actions_are_blocked_server_side_before_lookup(
    migrated_database_url: str,
) -> None:
    app = create_engine_app(_settings(migrated_database_url))
    order_id = "00000000-0000-0000-0000-000000000001"
    cases = (
        ("modify", {"expected_event_sequence": 0, "reason": "replace_price"}),
        ("cancel", {"expected_event_sequence": 0, "reason": "user_cancel"}),
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            for operation, payload in cases:
                path = f"{_BASE}/orders/{order_id}/{operation}"
                body = _body(payload)
                response = await client.post(
                    path,
                    content=body,
                    headers=_headers(
                        "POST",
                        path,
                        body,
                        idempotency_key=f"blocked-{operation}",
                        if_match='"0"',
                    ),
                )
                assert response.status_code == 409
                assert response.json()["error"]["code"] == "SIMULATION_WRITE_DISABLED"
                assert order_id not in response.text
    finally:
        await app.state.session_factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_prepare_body_is_bounded_and_rejects_order_authority_fields(
    migrated_database_url: str,
) -> None:
    app = create_engine_app(_settings(migrated_database_url))
    path = f"{_BASE}/intents"
    forbidden = _body(
        {
            "proposal_group_id": "00000000-0000-0000-0000-000000000001",
            "proposal_id": "00000000-0000-0000-0000-000000000002",
            "quantity": 100,
        }
    )
    mixed = _body(
        {
            "proposal_group_id": "00000000-0000-0000-0000-000000000001",
            "proposal_id": "00000000-0000-0000-0000-000000000002",
            "agent_paper_cycle_id": "00000000-0000-0000-0000-000000000003",
            "account_binding_id": "00000000-0000-0000-0000-000000000004",
            "portfolio_snapshot_id": "00000000-0000-0000-0000-000000000005",
        }
    )
    partial_agent = _body(
        {
            "agent_paper_cycle_id": "00000000-0000-0000-0000-000000000003",
            "account_binding_id": "00000000-0000-0000-0000-000000000004",
        }
    )
    oversized = b'{"padding":"' + (b"a" * (64 * 1024)) + b'"}'
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            invalid = await client.post(
                path,
                content=forbidden,
                headers=_headers("POST", path, forbidden, idempotency_key="forbidden-order"),
            )
            too_large = await client.post(
                path,
                content=oversized,
                headers=_headers("POST", path, oversized, idempotency_key="oversized"),
            )
            invalid_mixed = await client.post(
                path,
                content=mixed,
                headers=_headers("POST", path, mixed, idempotency_key="mixed-source"),
            )
            invalid_partial = await client.post(
                path,
                content=partial_agent,
                headers=_headers("POST", path, partial_agent, idempotency_key="partial-agent"),
            )
        assert {
            invalid.status_code,
            too_large.status_code,
            invalid_mixed.status_code,
            invalid_partial.status_code,
        } == {422}
    finally:
        await app.state.session_factory.kw["bind"].dispose()
