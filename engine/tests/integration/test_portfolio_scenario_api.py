"""Authenticated read-only portfolio scenario API contracts."""

import base64
import json
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import PORTFOLIOS, app, headers, seed_portfolio_list, settings
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.market.intraday_ingestion import canonical_quote_observation
from nexus.ops.alpaca_market_stream import ARCHIVE_SCHEMA, SCHEMA
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.market import Instrument, InstrumentAlias, LatestQuote
from nexus.persistence.models.optimization import OptimizationProposalGroupRecord
from nexus.persistence.models.paper import PaperFillRecord, PaperOrderRecord
from nexus.persistence.models.portfolio import PositionSnapshot
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord
from nexus.persistence.models.shadow import ShadowGroupRecord
from nexus.persistence.session import session_factory
from sqlalchemy import func, select, text

SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
OTHER_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000042")
US_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000701")
SECOND_US_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000702")
AS_OF = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
KNOWN_AS_OF = datetime(2026, 9, 10, 15, 0, tzinfo=UTC)


@dataclass
class ScenarioStore:
    objects: dict[str, bytes] = field(default_factory=dict)

    async def read_raw(self, key: str, *, uri: str) -> bytes:
        if uri != f"memory://{key}":
            raise ValueError("object URI mismatch")
        return self.objects[key]


def _timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _quote_records(
    store: ScenarioStore,
    *,
    event_at: datetime,
    bid: str = "39.90",
    ask: str = "40.10",
    sequence: int = 1,
) -> tuple[DataArtifact, LatestQuote]:
    started_at = event_at - timedelta(seconds=1)
    completed_at = event_at + timedelta(seconds=1)
    frames = (
        json.dumps([{"T": "success", "msg": "connected"}], separators=(",", ":")).encode(),
        json.dumps([{"T": "success", "msg": "authenticated"}], separators=(",", ":")).encode(),
        json.dumps(
            [
                {
                    "T": "subscription",
                    "trades": ["SCENARIO"],
                    "quotes": ["SCENARIO"],
                    "bars": [],
                    "updatedBars": [],
                    "dailyBars": [],
                    "statuses": [],
                    "lulds": [],
                    "corrections": ["SCENARIO"],
                    "cancelErrors": ["SCENARIO"],
                }
            ],
            separators=(",", ":"),
        ).encode(),
        json.dumps(
            [
                {
                    "T": "q",
                    "S": "SCENARIO",
                    "bx": "V",
                    "bp": bid,
                    "bs": 1,
                    "ax": "V",
                    "ap": ask,
                    "as": 1,
                    "c": ["R"],
                    "t": _timestamp(event_at),
                    "z": "C",
                }
            ],
            separators=(",", ":"),
        ).encode(),
    )
    received = (started_at, started_at, event_at, completed_at)
    receipt = {
        "schema": SCHEMA,
        "source": "alpaca",
        "transport": "websocket",
        "origin": "wss://stream.data.alpaca.markets/v2/iex",
        "feed": "iex",
        "delivery_mode": "real_time",
        "symbol": "SCENARIO",
        "started_at": _timestamp(started_at),
        "ended_at": _timestamp(completed_at),
        "requested_duration_seconds": 60.0,
        "requested_max_frames": 4,
        "elapsed_monotonic_seconds": 1.0,
        "authenticated": True,
        "subscription_confirmed": True,
        "status": "bounded_complete",
        "stop_reason": "FRAME_LIMIT",
        "trade_count": 0,
        "quote_count": 1,
        "frame_count": 4,
        "frames": [
            {
                "file": f"frame-{index:05}.json",
                "received_at": _timestamp(received[index]),
                "raw_sha256": sha256(frame).hexdigest(),
                "raw_byte_length": len(frame),
            }
            for index, frame in enumerate(frames)
        ],
    }
    receipt_raw = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()
    payload = json.dumps(
        {
            "schema": ARCHIVE_SCHEMA,
            "receipt_base64": base64.b64encode(receipt_raw).decode(),
            "frames_base64": [base64.b64encode(frame).decode() for frame in frames],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    digest = sha256(payload).hexdigest()
    key = f"raw/market/alpaca/stream/{completed_at:%Y/%m/%d}/{digest}.json"
    store.objects[key] = payload
    artifact_id = UUID(f"0198d500-1000-7000-8100-{sequence:012d}")
    quote_id = UUID(f"0198d500-1000-7000-8200-{sequence:012d}")
    content_hash = sha256(
        canonical_quote_observation(
            instrument_id=US_INSTRUMENT_ID,
            source="alpaca",
            feed="iex",
            delivery_mode="real_time",
            event_kind="quote",
            bid=Decimal(bid),
            ask=Decimal(ask),
            last=None,
            observed_at=event_at,
            available_at=completed_at,
            received_at=completed_at,
            revision=digest,
            quality_flags=(),
        )
    ).hexdigest()
    return (
        DataArtifact(
            id=artifact_id,
            uri=f"memory://{key}",
            sha256=digest,
            byte_size=len(payload),
            mime_type="application/json",
            schema_version=ARCHIVE_SCHEMA,
            source="alpaca",
            observed_at=event_at,
            available_at=completed_at,
            received_at=completed_at,
        ),
        LatestQuote(
            id=quote_id,
            instrument_id=US_INSTRUMENT_ID,
            source="alpaca",
            feed="iex",
            delivery_mode="real_time",
            bid=Decimal(bid),
            ask=Decimal(ask),
            last=None,
            observed_at=event_at,
            available_at=completed_at,
            received_at=completed_at,
            revision=digest,
            quality_flags=[],
            event_kind="quote",
            content_hash=content_hash,
            evidence_status="verified",
            data_artifact_id=artifact_id,
        ),
    )


async def _seed_scenario_authority(
    database_url: str,
    *,
    cash_flow_quality: str = "PASS",
    as_of: datetime = AS_OF,
    known_spread_case: bool = False,
) -> UUID:
    await seed_portfolio_list(
        database_url,
        ready_cash_flow_quality=cash_flow_quality,
        ready_usd_cash=Decimal("100") if known_spread_case else Decimal("1000"),
        ready_snapshot_as_of=as_of,
    )
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=US_INSTRUMENT_ID,
                asset_type="equity",
                exchange="XNAS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="us_equity",
                created_at=as_of - timedelta(minutes=1),
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=US_INSTRUMENT_ID,
                symbol="SCENARIO",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
                created_at=as_of - timedelta(minutes=1),
            )
        )
        session.add(
            PositionSnapshot(
                portfolio_snapshot_id=SNAPSHOT_ID,
                instrument_id=US_INSTRUMENT_ID,
                currency="USD",
                quantity=Decimal("15") if known_spread_case else Decimal("100"),
                available_quantity=Decimal("15") if known_spread_case else Decimal("100"),
                frozen_quantity=Decimal("0"),
                avg_cost=Decimal("35"),
                diluted_cost=Decimal("35"),
                current_price=Decimal("40"),
                provider_as_of=as_of,
            )
        )
        if known_spread_case:
            session.add_all(
                (
                    Instrument(
                        id=SECOND_US_INSTRUMENT_ID,
                        asset_type="equity",
                        exchange="XNAS",
                        currency="USD",
                        tick_size=Decimal("0.01"),
                        risk_bucket="us_equity",
                        created_at=as_of - timedelta(minutes=1),
                    ),
                    InstrumentAlias(
                        instrument_id=SECOND_US_INSTRUMENT_ID,
                        symbol="SECOND",
                        alias_type="ticker",
                        valid_from=date(2020, 1, 1),
                        valid_to=None,
                        created_at=as_of - timedelta(minutes=1),
                    ),
                    PositionSnapshot(
                        portfolio_snapshot_id=SNAPSHOT_ID,
                        instrument_id=SECOND_US_INSTRUMENT_ID,
                        currency="USD",
                        quantity=Decimal("10"),
                        available_quantity=Decimal("10"),
                        frozen_quantity=Decimal("0"),
                        avg_cost=Decimal("25"),
                        diluted_cost=Decimal("25"),
                        current_price=Decimal("30"),
                        provider_as_of=as_of,
                    ),
                )
            )
        report = PortfolioDiagnosticReportRecord(
            portfolio_id=PORTFOLIOS[0],
            portfolio_snapshot_id=SNAPSHOT_ID,
            snapshot_as_of=as_of,
            trading_day=as_of.date(),
            policy_id=REAL_ADVISORY_POLICY_ID,
            policy_version="real_advisory_policy_v1",
            optimization_need="RESEARCH_REQUIRED",
            diagnostic_input_hash="d" * 64,
        )
        session.add(report)
        await session.flush()
        item = PortfolioDiagnosticItemRecord(
            report_id=report.id,
            diagnostic_code="CONCENTRATION_TOP1",
            metric_value=(Decimal("0.666666666667") if known_spread_case else Decimal("1")),
            unit="ratio",
            reference_limit=Decimal("0.30"),
            policy_scope="currency:USD",
            severity="critical",
            actionability="reduce_only",
            unknown_reason=None,
        )
        session.add(item)
        await session.flush()
        session.add(
            PortfolioDiagnosticEvidenceRefRecord(
                item_id=item.id,
                source="fixture",
                object_id="scenario-top1",
                available_at=as_of,
                location="fixture://scenario-top1",
                content_hash="e" * 64,
            )
        )
        await session.flush()
        return report.id


def _payload(report_id: UUID, *, snapshot_id: UUID = SNAPSHOT_ID, as_of: datetime = AS_OF) -> bytes:
    return json.dumps(
        {
            "schema_version": "portfolio_scenario_request_v1",
            "portfolio_snapshot_id": str(snapshot_id),
            "diagnostic_report_id": str(report_id),
            "as_of": as_of.isoformat().replace("+00:00", "Z"),
            "constraints": {
                "schema_version": "adjustment_scenario_constraints_v1",
                "objective": "reduce_security_concentration",
                "max_turnover": "0.10",
                "minimum_cash": "0.10",
                "universe_policy": "current_holdings_only",
                "allow_new_positions": False,
            },
        },
        separators=(",", ":"),
    ).encode()


async def _seed_legacy_scenario_claim(
    database_url: str,
    *,
    body: bytes,
    idempotency_key: str,
    request_hash: str | None = None,
) -> None:
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        session.add(
            IdempotencyRecord(
                actor_id="nexus-test-user",
                route=(
                    "/internal/v1/portfolio-scenarios/evaluate:"
                    "portfolio_scenario_evaluation_v2"
                ),
                idempotency_key=idempotency_key,
                request_hash=request_hash or sha256(body).hexdigest(),
                schema_version="portfolio_scenario_v2",
                response_payload={},
            )
        )


@pytest.mark.asyncio
async def test_scenario_evaluation_returns_one_ephemeral_locked_candidate(
    migrated_database_url: str,
) -> None:
    report_id = await _seed_scenario_authority(migrated_database_url)
    body = _payload(report_id)
    path = "/internal/v1/portfolio-scenarios/evaluate"
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = "scenario-evaluate-1"
    store = ScenarioStore()

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app(migrated_database_url, scenario_evidence_store=store)
        ),
        base_url="http://testserver",
    ) as client:
        response = await client.post(path, content=body, headers=request_headers)
        later_artifact, later_quote = _quote_records(
            store, event_at=AS_OF - timedelta(seconds=10), sequence=9
        )
        factory = session_factory(settings(migrated_database_url))
        async with factory() as session, session.begin():
            session.add_all((later_artifact, later_quote))
        replay_headers = headers("POST", path, body)
        replay_headers["Idempotency-Key"] = "scenario-evaluate-1"
        replay = await client.post(path, content=body, headers=replay_headers)
        changed = json.loads(body)
        changed["constraints"]["minimum_cash"] = "0.20"
        changed_body = json.dumps(changed, separators=(",", ":")).encode()
        conflict_headers = headers("POST", path, changed_body)
        conflict_headers["Idempotency-Key"] = "scenario-evaluate-1"
        conflict = await client.post(path, content=changed_body, headers=conflict_headers)

    assert response.status_code == 200
    data = response.json()["data"]
    assert replay.status_code == 200
    assert replay.json()["data"]["result_hash"] == data["result_hash"]
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert data["portfolio_snapshot_id"] == str(SNAPSHOT_ID)
    assert data["schema_version"] == "portfolio_scenario_evaluation_v2"
    assert data["diagnostic_report_id"] == str(report_id)
    assert data["valuation_scope"] == "currency:USD"
    assert data["analysis_status"] == "partial"
    assert data["analysis_comparable"] is True
    assert data["execution_eligible"] is False
    assert data["execution_locked"] is True
    assert data["unknown_metrics"] == ["estimated_cost"]
    assert data["cost_breakdown"] == {
        "currency": "USD",
        "valuation_time": "2026-08-24T03:59:00Z",
        "basis": "portfolio_value_ratio",
        "evidence_version": "us_regulatory_fee_reference_2026_verified_20260911_v1",
        "evidence_hash": "56c13aeb742a7ad616834408ac63dea4210f22fa6c7443ccbb7c082782a60138",
        "available_at": "2026-02-28T00:00:00Z",
        "commission": {
            "status": "unknown",
            "value": None,
            "reason": "BROKER_COMMISSION_SCHEDULE_UNAVAILABLE",
        },
        "spread": {
            "status": "unknown",
            "value": None,
            "reason": "BID_ASK_EVIDENCE_UNAVAILABLE",
        },
        "slippage": {
            "status": "unknown",
            "value": None,
            "reason": "SLIPPAGE_MODEL_UNAVAILABLE",
        },
        "regulatory_fees": {
            "status": "unknown",
            "value": None,
            "reason": "BROKER_REGULATORY_FEE_PASS_THROUGH_UNAVAILABLE",
        },
        "tax_impact": {
            "status": "unknown",
            "value": None,
            "reason": "TAX_DATA_UNAVAILABLE",
        },
        "total_estimated_cost_ex_tax": {
            "status": "unknown",
            "value": None,
            "reason": "COST_EVIDENCE_INCOMPLETE",
        },
    }
    assert data["positions"] == [
        {
            "instrument_id": str(US_INSTRUMENT_ID),
            "current_weight": "0.800000000000",
            "target_weight": "0.800000000000",
            "action": "maintain",
            "reason_code": "BASELINE_WEIGHT",
        }
    ]

    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        audit = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "portfolio_scenario_evaluated")
            )
        )
        business_writes = {
            model.__tablename__: await session.scalar(select(func.count()).select_from(model))
            for model in (
                OptimizationProposalGroupRecord,
                ShadowGroupRecord,
                PaperOrderRecord,
                PaperFillRecord,
            )
        }
        idempotency_records = list(
            await session.scalars(
                select(IdempotencyRecord).where(
                    IdempotencyRecord.idempotency_key == "scenario-evaluate-1"
                )
            )
        )
    assert len(audit) == 1
    assert business_writes == {
        "optimization_proposal_groups": 0,
        "shadow_groups": 0,
        "paper_orders": 0,
        "paper_fills": 0,
    }
    assert len(idempotency_records) == 1
    assert idempotency_records[0].route.endswith(":portfolio_scenario_evaluation_v3")
    assert idempotency_records[0].schema_version == "portfolio_scenario_v3"
    manifest = idempotency_records[0].response_payload
    assert datetime.fromisoformat(manifest["evidence_selected_at"].replace("Z", "+00:00")) >= AS_OF
    assert {**manifest, "evidence_selected_at": "<database-clock>"} == {
        "manifest_schema": "scenario_spread_evidence_manifest_v1",
        "evidence_selected_at": "<database-clock>",
        "policy_status": "unavailable",
        "source_policy_id": None,
        "source_policy_version": None,
        "source_policy_payload_hash": None,
        "calendar_authority": None,
        "calculation_version": "scenario_spread_cost_v1",
        "no_cost_candidate_hash": data["result_hash"],
        "scenario_cost_basis_hash": manifest["scenario_cost_basis_hash"],
        "regulatory_reference_hash": manifest["regulatory_reference_hash"],
        "selected_quotes": [],
        "expected_result_hash": data["result_hash"],
    }
    assert audit[0].payload == {
        "request_hash": data["request_hash"],
        "policy_version": "real_advisory_policy_v1",
        "instrument_count": 1,
        "analysis_status": "partial",
        "objective_status": "infeasible",
        "spread_status": "unknown",
        "spread_policy_version": None,
    }


@pytest.mark.asyncio
async def test_scenario_upgrade_replays_matching_v2_claim_without_v3_claim_or_quote_selection(
    migrated_database_url: str,
) -> None:
    report_id = await _seed_scenario_authority(
        migrated_database_url, as_of=KNOWN_AS_OF, known_spread_case=True
    )
    store = ScenarioStore()
    artifact, quote = _quote_records(
        store, event_at=KNOWN_AS_OF - timedelta(seconds=30), sequence=31
    )
    body = _payload(report_id, as_of=KNOWN_AS_OF)
    idempotency_key = "scenario-upgrade-v2-replay"
    await _seed_legacy_scenario_claim(
        migrated_database_url,
        body=body,
        idempotency_key=idempotency_key,
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        session.add_all((artifact, quote))

    path = "/internal/v1/portfolio-scenarios/evaluate"
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = idempotency_key
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app(
                migrated_database_url,
                now=KNOWN_AS_OF + timedelta(minutes=1),
                scenario_evidence_store=store,
            )
        ),
        base_url="http://testserver",
    ) as client:
        response = await client.post(path, content=body, headers=request_headers)

    assert response.status_code == 200, response.text
    assert response.json()["data"]["cost_breakdown"]["spread"] == {
        "status": "unknown",
        "value": None,
        "reason": "BID_ASK_EVIDENCE_UNAVAILABLE",
    }
    async with factory() as session:
        claims = list(
            await session.scalars(
                select(IdempotencyRecord).where(
                    IdempotencyRecord.actor_id == "nexus-test-user",
                    IdempotencyRecord.idempotency_key == idempotency_key,
                )
            )
        )
        audit_count = await session.scalar(
            select(func.count())
            .select_from(AuditEvent)
            .where(AuditEvent.action == "portfolio_scenario_evaluated")
        )
    assert [(claim.route, claim.schema_version) for claim in claims] == [
        (
            "/internal/v1/portfolio-scenarios/evaluate:portfolio_scenario_evaluation_v2",
            "portfolio_scenario_v2",
        )
    ]
    assert audit_count == 0


@pytest.mark.asyncio
async def test_scenario_upgrade_rejects_changed_body_against_v2_claim_without_v3_claim(
    migrated_database_url: str,
) -> None:
    report_id = await _seed_scenario_authority(migrated_database_url)
    body = _payload(report_id)
    idempotency_key = "scenario-upgrade-v2-conflict"
    await _seed_legacy_scenario_claim(
        migrated_database_url,
        body=body,
        idempotency_key=idempotency_key,
        request_hash="0" * 64,
    )

    path = "/internal/v1/portfolio-scenarios/evaluate"
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = idempotency_key
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.post(path, content=body, headers=request_headers)

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        claims = list(
            await session.scalars(
                select(IdempotencyRecord).where(
                    IdempotencyRecord.actor_id == "nexus-test-user",
                    IdempotencyRecord.idempotency_key == idempotency_key,
                )
            )
        )
    assert [(claim.route, claim.schema_version) for claim in claims] == [
        (
            "/internal/v1/portfolio-scenarios/evaluate:portfolio_scenario_evaluation_v2",
            "portfolio_scenario_v2",
        )
    ]


@pytest.mark.asyncio
async def test_scenario_freezes_known_spread_and_rejects_broken_exact_replay(
    migrated_database_url: str,
) -> None:
    report_id = await _seed_scenario_authority(
        migrated_database_url, as_of=KNOWN_AS_OF, known_spread_case=True
    )
    store = ScenarioStore()
    artifact, quote = _quote_records(
        store, event_at=KNOWN_AS_OF - timedelta(seconds=30), sequence=1
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        session.add_all((artifact, quote))

    body = _payload(report_id, as_of=KNOWN_AS_OF)
    path = "/internal/v1/portfolio-scenarios/evaluate"
    first_headers = headers("POST", path, body)
    first_headers["Idempotency-Key"] = "scenario-known-spread"
    scenario_app = app(
        migrated_database_url,
        now=KNOWN_AS_OF + timedelta(minutes=1),
        scenario_evidence_store=store,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=scenario_app), base_url="http://testserver"
    ) as client:
        first = await client.post(path, content=body, headers=first_headers)

        newer_artifact, newer_quote = _quote_records(
            store,
            event_at=KNOWN_AS_OF - timedelta(seconds=5),
            bid="39.80",
            ask="40.20",
            sequence=2,
        )
        async with factory() as session, session.begin():
            session.add_all((newer_artifact, newer_quote))
        replay_headers = headers("POST", path, body)
        replay_headers["Idempotency-Key"] = "scenario-known-spread"
        replay = await client.post(path, content=body, headers=replay_headers)

        async with factory() as session, session.begin():
            await session.execute(text("SET LOCAL session_replication_role = 'replica'"))
            await session.execute(
                text("UPDATE latest_quotes SET bid = 39.80 WHERE id = :quote_id"),
                {"quote_id": quote.id},
            )
        broken_headers = headers("POST", path, body)
        broken_headers["Idempotency-Key"] = "scenario-known-spread"
        broken = await client.post(path, content=body, headers=broken_headers)

    assert first.status_code == 200, first.text
    data = first.json()["data"]
    assert data["cost_breakdown"]["spread"] == {
        "status": "known",
        "value": "0.000250000000",
        "reason": None,
    }
    assert data["cost_breakdown"]["evidence_version"] == "scenario_cost_evidence_bundle_v1"
    assert data["candidate"]["estimated_cost"]["status"] == "unknown"
    assert data["execution_eligible"] is False
    assert data["execution_locked"] is True
    assert replay.status_code == 200
    assert replay.json()["data"]["result_hash"] == data["result_hash"]
    assert broken.status_code == 409
    assert broken.json()["error"]["code"] == "SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID"

    async with factory() as session:
        claim = await session.scalar(
            select(IdempotencyRecord).where(
                IdempotencyRecord.idempotency_key == "scenario-known-spread"
            )
        )
        audits = list(
            await session.scalars(
                select(AuditEvent).where(AuditEvent.action == "portfolio_scenario_evaluated")
            )
        )
    assert claim is not None
    assert claim.route.endswith(":portfolio_scenario_evaluation_v3")
    assert claim.response_payload["selected_quotes"][0]["latest_quote_id"] == str(quote.id)
    assert claim.response_payload["expected_result_hash"] == data["result_hash"]
    assert len(audits) == 1
    assert audits[0].payload["spread_status"] == "known"
    assert audits[0].payload["spread_policy_version"] == "scenario_spread_cost_policy_v1"
    assert set(audits[0].payload) == {
        "request_hash",
        "policy_version",
        "instrument_count",
        "analysis_status",
        "objective_status",
        "spread_status",
        "spread_policy_version",
    }


@pytest.mark.asyncio
async def test_scenario_rejects_client_weights_and_mismatched_authority(
    migrated_database_url: str,
) -> None:
    report_id = await _seed_scenario_authority(migrated_database_url)
    path = "/internal/v1/portfolio-scenarios/evaluate"
    weighted = json.loads(_payload(report_id))
    weighted["positions"] = [{"instrument_id": str(US_INSTRUMENT_ID), "weight": "1"}]
    cases = (
        (json.dumps(weighted, separators=(",", ":")).encode(), 422, "VALIDATION_ERROR"),
        (
            _payload(report_id, snapshot_id=OTHER_SNAPSHOT_ID),
            409,
            "SCENARIO_DIAGNOSTIC_MISMATCH",
        ),
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        for index, (body, status, code) in enumerate(cases):
            request_headers = headers("POST", path, body)
            request_headers["Idempotency-Key"] = f"scenario-reject-{index}"
            response = await client.post(path, content=body, headers=request_headers)
            assert response.status_code == status
            assert response.json()["error"]["code"] == code

    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "portfolio_scenario_evaluated")
            )
            == 0
        )


@pytest.mark.asyncio
async def test_scenario_rejects_ready_snapshot_without_cash_flow_authority(
    migrated_database_url: str,
) -> None:
    report_id = await _seed_scenario_authority(migrated_database_url, cash_flow_quality="PARTIAL")
    body = _payload(report_id)
    path = "/internal/v1/portfolio-scenarios/evaluate"
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = "scenario-unqualified-baseline"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.post(path, content=body, headers=request_headers)

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "SCENARIO_BASELINE_NOT_READY"
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == "portfolio_scenario_evaluated")
            )
            == 0
        )
