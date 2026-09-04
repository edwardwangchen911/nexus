"""Real frozen portfolio inputs reach research tools without a model or broker call."""

import json
from copy import deepcopy
from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.agents.tools import ToolInputError
from nexus.api.research_inputs import read_research_inputs
from nexus.diagnostics.assembler import _snapshot_bindings
from nexus.diagnostics.models import DiagnosticEvidence
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, REAL_ADVISORY_POLICY_VERSION
from nexus.diagnostics.repository import load_frozen_snapshot
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import (
    DiagnosticPolicy,
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    CashSnapshot,
    Portfolio,
    PortfolioSnapshotRecord,
    PositionSnapshot,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import update


@pytest.mark.asyncio
async def test_existing_comparison_is_frozen_for_research_without_creating_proposals(
    migrated_database_url,
):
    from nexus.optimizer.compare import ComparisonDimensions, TypedMetric
    from nexus.optimizer.cost_policy import COST_POLICY_ID
    from nexus.persistence.models.optimization import (
        OptimizationProposalGroupRecord,
        OptimizationProposalRecord,
        ProposalComparisonRecord,
    )
    from nexus.quality.source_policy import SOURCE_POLICY_ID
    from sqlalchemy import func, select

    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    async with session_factory(settings)() as session, session.begin():
        await seed_filing(session)
        portfolio, report = await seed_context(session)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        data = DataSnapshot(
            source="fixture", provider_object_id="comparison", revision="1",
            observed_at=NOW, available_at=NOW, received_at=NOW,
            schema_version="1", payload={}, created_at=NOW,
        )
        session.add(data)
        await session.flush()
        at = NOW + timedelta(seconds=1)
        group = OptimizationProposalGroupRecord(
            baseline_portfolio_snapshot_id=portfolio.id,
            diagnostic_report_id=report.id, diagnostic_input_hash=report.diagnostic_input_hash,
            data_snapshot_id=data.id, source_policy_id=SOURCE_POLICY_ID,
            cost_policy_id=COST_POLICY_ID,
            policy_id=UUID("e690b91d-1e24-5de3-b2b2-566a463fca25"),
            valuation_time=NOW, baseline_ready=True, diagnostic_complete=False,
            partial=True, created_at=at,
        )
        session.add(group)
        await session.flush()
        dimensions = ComparisonDimensions(
            hard_violations=0,
            **{name: TypedMetric.unknown("MISSING_EVIDENCE") for name in
               ComparisonDimensions.model_fields if name != "hard_violations"},
        )
        for variant in ("hold_current", "minimum_change", "strategy_target"):
            child = OptimizationProposalRecord(
                proposal_group_id=group.id, variant=variant,
                baseline_portfolio_snapshot_id=portfolio.id,
                analysis_only=True, strategy_evidence_complete=False, created_at=at,
            )
            session.add(child)
            await session.flush()
            session.add(ProposalComparisonRecord(
                proposal_id=child.id, metrics={"dimensions": dimensions.model_dump(mode="json")},
                non_executable=True, cost_complete=False,
                created_at=at + timedelta(seconds=2) if variant == "strategy_target" else at,
            ))
        await session.flush()
        later = NOW + timedelta(seconds=4)
        from nexus.agents.proposal_context import read_comparison_for_freeze

        scope = dict(
            portfolio_snapshot_id=portfolio.id, diagnostic_report_id=report.id,
            diagnostic_input_hash=report.diagnostic_input_hash,
            valuation_time=NOW, as_of=later,
        )
        for mismatch in (
            {"as_of": NOW},
            {"as_of": NOW + timedelta(seconds=2)},
            {"portfolio_snapshot_id": UUID(int=999)},
            {"diagnostic_report_id": UUID(int=999)},
            {"diagnostic_input_hash": "0" * 64},
            {"valuation_time": later},
        ):
            assert await read_comparison_for_freeze(session, **(scope | mismatch)) is None
        snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=later)
        inputs = await read_research_inputs(
            session, snapshot_id=snapshot.id, instrument_id=INSTRUMENT, as_of=later,
        )
        frozen = inputs.portfolio_context.proposal_comparison
        assert frozen.proposal_group_id == group.id
        assert len(frozen.variants) == 3
        assert all(row.dimensions.liquidity.status == "unknown" for row in frozen.variants)
        old_inputs = await read_research_inputs(
            session, snapshot_id=old.id, instrument_id=INSTRUMENT, as_of=later,
        )
        assert old_inputs.portfolio_context.proposal_comparison is None
        admission = await admit_research_run(
            session, actor_id="test", route="/internal/v1/research-runs",
            idempotency_key="frozen-comparison", request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT, data_snapshot_id=snapshot.id, decision_time=later,
            ),
            prompt_version_id=None, model_config_id=None,
            daily_limit_usd=Decimal("3"), now=later,
        )
        tools = await database_read_only_tools(session, admission.run, allow_fixture_evidence=False)
        result = await tools.execute("get_proposal_comparison", {
            "proposal_group_id": str(group.id), "as_of": later.isoformat(),
        })
        assert result["status"] == "ok"
        assert result["items"][0]["proposal_group_id"] == str(group.id)
        assert result["items"][0]["execution_locked"] is True
        assert result["items"][0]["economic_comparison"] == {
            "status": "incomparable", "recommendation": None,
            "reasons": ["COST_EVIDENCE_INCOMPLETE", "COMPARISON_EVIDENCE_INCOMPLETE"],
        }
        exposure = await tools.execute("get_portfolio_exposure", {
            "portfolio_snapshot_id": str(portfolio.id), "instrument_id": str(INSTRUMENT),
            "as_of": later.isoformat(),
        })
        assert exposure["items"][0]["proposal_comparison"]["economic_comparison"] == (
            result["items"][0]["economic_comparison"]
        )
        assert exposure["items"][0]["content_hash"] == inputs.portfolio_context.content_hash
        assert "private-" not in json.dumps(result)
        with pytest.raises(ToolInputError, match="proposal"):
            await tools.execute("get_proposal_comparison", {
                "proposal_group_id": str(UUID(int=999)), "as_of": later.isoformat(),
            })
        count = await session.scalar(
            select(func.count()).select_from(OptimizationProposalGroupRecord)
        )
        assert count == 1


async def seed_context(
    session, *, quality="PASS", status="READY", offset=0, price="20", cash="300",
    position_offset=0, provider_offset=0,
):
    at = NOW + timedelta(seconds=offset)
    account = Account(provider="fosun", status="ACTIVE", created_at=at)
    session.add(account)
    await session.flush()
    portfolio = Portfolio(account_id=account.id, name="private-account-canary", created_at=at)
    session.add(portfolio)
    await session.flush()
    sync = BrokerSyncRun(
        account_id=account.id,
        portfolio_id=portfolio.id,
        trigger_kind="MANUAL",
        actor_id="test",
        route="test",
        idempotency_key="test",
        request_hash="c" * 64,
        status=status,
        accepted_at=at,
        completed_at=at,
        attempt_count=1,
        cash_flow_quality=quality,
        created_at=at,
    )
    session.add(sync)
    await session.flush()
    snapshot = PortfolioSnapshotRecord(
        portfolio_id=portfolio.id,
        sync_run_id=sync.id,
        status=status,
        as_of=at,
        position_hash_version="broker_position_hash_v1",
        position_hash="d" * 64,
        order_hash_version="broker_order_hash_v1",
        order_hash="e" * 64,
        source_request_ids=["private-request-canary"],
        source_observations=[],
        created_at=at,
    )
    session.add(snapshot)
    await session.flush()
    session.add_all(
        [
            CashSnapshot(
                portfolio_snapshot_id=snapshot.id,
                currency="USD",
                cash_balance=Decimal(cash),
                available_cash=Decimal(cash),
                provider_as_of=at,
                created_at=at,
            ),
            CashSnapshot(
                portfolio_snapshot_id=snapshot.id,
                currency="HKD",
                cash_balance=9000,
                available_cash=9000,
                provider_as_of=at,
                created_at=at,
            ),
            PositionSnapshot(
                portfolio_snapshot_id=snapshot.id,
                instrument_id=INSTRUMENT,
                currency="USD",
                quantity=10,
                available_quantity=10,
                frozen_quantity=0,
                current_price=None if price is None else Decimal(price),
                provider_as_of=at + timedelta(seconds=provider_offset),
                created_at=at + timedelta(seconds=position_offset),
            ),
        ]
    )
    policy = DiagnosticPolicy(
        id=UUID(int=42), key="test", version="v1", schema_version="test", payload={}, created_at=at
    )
    session.add(policy)
    await session.flush()
    report = PortfolioDiagnosticReportRecord(
        portfolio_id=portfolio.id,
        portfolio_snapshot_id=snapshot.id,
        snapshot_as_of=at,
        policy_id=policy.id,
        policy_version="v1",
        optimization_need="RESEARCH_REQUIRED",
        diagnostic_input_hash="f" * 64,
        created_at=at,
    )
    session.add(report)
    await session.flush()
    session.add(
        PortfolioDiagnosticItemRecord(
            report_id=report.id,
            diagnostic_code="MARKET_EVIDENCE",
            metric_value=None,
            unit="status",
            reference_limit=None,
            policy_scope="portfolio",
            severity="warning",
            actionability="research",
            unknown_reason="MARKET_EVIDENCE_UNAVAILABLE",
            created_at=at,
        )
    )
    await session.flush()
    return snapshot, report


@pytest.mark.asyncio
@pytest.mark.parametrize("cash", ["300", "30000"])
async def test_concentration_question_is_frozen_and_reaches_api_without_rewriting_old_inputs(
    migrated_database_url, cash,
):
    from nexus.agents.portfolio_context import read_portfolio_context

    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    async with session_factory(settings)() as session, session.begin():
        await seed_filing(session)
        portfolio, _ = await seed_context(session, cash=cash)
        await seed_other_position(session, portfolio, currency="HKD")
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        assert old is not None
        old_payload, old_revision = dict(old.payload), old.revision
        report = await seed_concentration_report(session, portfolio)
        later = NOW + timedelta(seconds=2)
        snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=later)
        assert snapshot is not None and snapshot.id != old.id
        inputs = await read_research_inputs(
            session, snapshot_id=snapshot.id, instrument_id=INSTRUMENT, as_of=later,
        )
        assert inputs is not None and inputs.portfolio_context is not None
        context = inputs.portfolio_context
        assert Decimal(context.position_weight) == Decimal(200) / (Decimal(cash) + 200)
        assert context.research_focus is not None
        assert Decimal(context.research_focus.observed_weight) == 1  # Securities only.
        assert Decimal(context.research_focus.reference_limit) == Decimal("0.30")
        assert context.diagnostic_run_id == report.id
        assert context.diagnostic_input_hash == report.diagnostic_input_hash
        admission = await admit_research_run(
            session, actor_id="test", route="/internal/v1/research-runs",
            idempotency_key="concentration-input", request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT, data_snapshot_id=snapshot.id, decision_time=later,
            ),
            prompt_version_id=None, model_config_id=None, daily_limit_usd=Decimal("3"),
            now=later,
        )
        tools = await database_read_only_tools(session, admission.run, allow_fixture_evidence=False)
        exposure = await tools.execute("get_portfolio_exposure", {
            "portfolio_snapshot_id": str(portfolio.id), "instrument_id": str(INSTRUMENT),
            "as_of": later.isoformat(),
        })
        assert exposure["items"][0]["research_focus"] == context.research_focus.model_dump()
        assert exposure["items"][0]["content_hash"] == context.content_hash
        earlier = await read_portfolio_context(session, old.id, INSTRUMENT, later)
        assert earlier is not None and earlier.research_focus is None
        assert old.payload == old_payload and old.revision == old_revision
        assert await read_portfolio_context(session, snapshot.id, INSTRUMENT, NOW) is None


async def seed_concentration_report(session, snapshot, *, defect=None):
    """Persist original P refs using the same binding builder as diagnostics."""
    report = PortfolioDiagnosticReportRecord(
        portfolio_id=snapshot.portfolio_id, portfolio_snapshot_id=snapshot.id,
        snapshot_as_of=snapshot.as_of, policy_id=REAL_ADVISORY_POLICY_ID,
        policy_version=REAL_ADVISORY_POLICY_VERSION, optimization_need="RESEARCH_REQUIRED",
        diagnostic_input_hash="b" * 64, created_at=NOW + timedelta(seconds=1),
    )
    if defect == "wrong_asof":
        report.snapshot_as_of = NOW - timedelta(seconds=1)
    session.add(report)
    await session.flush()
    item = PortfolioDiagnosticItemRecord(
        report_id=report.id, diagnostic_code="CONCENTRATION_TOP1",
        metric_value=Decimal("0.5" if defect == "missing_peer_ref" else "1"),
        unit="ratio", reference_limit=Decimal("0.30"),
        policy_scope="currency:USD", severity="critical", actionability="research",
        unknown_reason=None, created_at=report.created_at,
    )
    session.add(item)
    await session.flush()
    if defect == "invalid_price":
        # The diagnostic input reader also rejects this malformed persisted fact.
        return report
    frozen = await load_frozen_snapshot(
        session, portfolio_id=snapshot.portfolio_id, snapshot_id=snapshot.id,
    )
    bindings, _ = await _snapshot_bindings(session, frozen)
    evidence = DiagnosticEvidence.model_validate(bindings)
    assert evidence.snapshot is not None
    refs = (*evidence.snapshot.refs, *evidence.position_prices[INSTRUMENT].refs)
    for index, ref in enumerate(refs):
        if defect == "missing_position_ref" and index == 1:
            continue
        values = ref.model_dump()
        if index == 1:
            if defect == "wrong_ref_hash":
                values["content_hash"] = "0" * 64
            elif defect == "wrong_ref_snapshot":
                values["object_id"] = f"position-snapshot:{UUID(int=999)}"
            elif defect == "future_ref":
                values["available_at"] = NOW + timedelta(seconds=1)
        session.add(PortfolioDiagnosticEvidenceRefRecord(
            item_id=item.id, **values, created_at=report.created_at,
        ))
    await session.flush()
    return report


async def seed_other_position(session, snapshot, *, currency):
    other = Instrument(
        id=UUID(int=999), asset_type="equity",
        exchange="XHKG" if currency == "HKD" else "XNAS", currency=currency,
        tick_size=Decimal("0.01"), risk_bucket="us_equity", created_at=snapshot.as_of,
    )
    session.add(other)
    await session.flush()
    session.add(PositionSnapshot(
        portfolio_snapshot_id=snapshot.id, instrument_id=other.id, currency=currency,
        quantity=10, available_quantity=10, frozen_quantity=0, current_price=Decimal(20),
        provider_as_of=snapshot.as_of, created_at=snapshot.as_of,
    ))
    await session.flush()


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", [
    "missing_position_ref", "wrong_ref_hash", "wrong_ref_snapshot", "future_ref", "wrong_asof",
    "policy_late", "late_position", "future_position", "not_ready", "cashflow_fail",
    "invalid_price",
    "missing_peer_ref",
])
async def test_new_concentration_focus_rejects_unbound_diagnostic_evidence(
    migrated_database_url, defect,
):
    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    async with session_factory(settings)() as session, session.begin():
        await seed_filing(session)
        options = {
            "policy_late": {"offset": -300 * 86400},
            "late_position": {"position_offset": 3},
            "future_position": {"provider_offset": 1},
            "not_ready": {"status": "PARTIAL"},
            "cashflow_fail": {"quality": "PARTIAL"},
            "invalid_price": {"price": "NaN"},
        }.get(defect, {})
        portfolio, _ = await seed_context(session, **options)
        if defect == "missing_peer_ref":
            await seed_other_position(session, portfolio, currency="USD")
        await seed_concentration_report(session, portfolio, defect=defect)
        later = NOW + timedelta(seconds=2)
        snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=later)
        assert snapshot is not None
        inputs = await read_research_inputs(
            session, snapshot_id=snapshot.id, instrument_id=INSTRUMENT, as_of=later,
        )
        assert inputs is not None
        if defect == "late_position":
            assert inputs.portfolio_context is None
        else:
            assert inputs.portfolio_context is not None
            assert inputs.portfolio_context.research_focus is None
            assert inputs.portfolio_context.concentration_facts is None
            assert inputs.portfolio_context.execution_locked is True


@pytest.mark.asyncio
async def test_legacy_focus_is_read_verbatim_but_wrong_bound_diagnostic_hash_is_rejected(
    migrated_database_url, monkeypatch,
):
    from nexus.agents import portfolio_context

    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    async with session_factory(settings)() as session, session.begin():
        await seed_filing(session)
        await seed_context(session)
        legacy_focus = portfolio_context.ConcentrationResearchFocus(
            question="该持仓触发单一标的集中度预警：现有证据支持维持，还是需要研究减仓？",
            policy_scope="currency:USD", policy_version="v1",
            observed_weight="1", reference_limit="0.35",
        )

        async def legacy_producer(*args, **kwargs):
            return legacy_focus

        with monkeypatch.context() as patch:
            patch.setattr(portfolio_context, "_verified_concentration_focus", legacy_producer)
            old = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        assert old is not None
        payload, revision = deepcopy(old.payload), old.revision
        later = NOW + timedelta(seconds=2)
        current = await prepare_research_snapshot(session, INSTRUMENT, as_of=later)
        assert current is not None
        old_context = await portfolio_context.read_portfolio_context(
            session, old.id, INSTRUMENT, later,
        )
        new_context = await portfolio_context.read_portfolio_context(
            session, current.id, INSTRUMENT, later,
        )
        assert old_context is not None and old_context.research_focus == legacy_focus
        assert new_context is not None and new_context.research_focus is None
        assert old.payload == payload and old.revision == revision
        payload["portfolio_context"]["diagnostic_input_hash"] = "0" * 64
        corrupted = DataSnapshot(
            source=old.source, provider_object_id=old.provider_object_id,
            schema_version=old.schema_version, payload=payload,
            revision=sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
            observed_at=NOW, available_at=NOW, received_at=NOW, created_at=NOW,
        )
        session.add(corrupted)
        await session.flush()
        assert await portfolio_context.read_portfolio_context(
            session, corrupted.id, INSTRUMENT, later,
        ) is None


@pytest.mark.asyncio
async def test_frozen_portfolio_context_reaches_api_and_real_tools(migrated_database_url):
    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    async with session_factory(settings)() as session, session.begin():
        await seed_filing(session)
        portfolio, report = await seed_context(session)
        snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        again = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        assert again.id == snapshot.id
        inputs = await read_research_inputs(
            session,
            snapshot_id=snapshot.id,
            instrument_id=INSTRUMENT,
            as_of=NOW,
        )
        assert inputs.portfolio_context is not None
        context = inputs.portfolio_context.model_dump(mode="json")
        assert context["position_weight"] == "0.4"  # 10 * 20 / (300 + 10 * 20); no HKD FX
        assert context["cash_weight"] == "0.6"
        assert context["currency"] == "USD"
        assert context["snapshot_decision_ready"] is True
        assert context["execution_locked"] is True
        assert context["diagnostics"][0]["unknown_reason"] == "MARKET_EVIDENCE_UNAVAILABLE"
        assert "private-" not in json.dumps(context)
        assert not {"cash_balance", "quantity", "account_id", "current_price"} & set(context)
        admission = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="portfolio-tools",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT, data_snapshot_id=snapshot.id, decision_time=NOW
            ),
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal("3"),
            now=NOW,
        )
        tools = await database_read_only_tools(session, admission.run, allow_fixture_evidence=False)
        exposure = await tools.execute(
            "get_portfolio_exposure",
            {
                "portfolio_snapshot_id": str(portfolio.id),
                "instrument_id": str(INSTRUMENT),
                "as_of": NOW.isoformat(),
            },
        )
        assert exposure["status"] == "ok"
        assert exposure["items"][0]["position_weight"] == "0.4"
        diagnostic = await tools.execute(
            "get_portfolio_diagnostics",
            {
                "portfolio_snapshot_id": str(portfolio.id),
                "diagnostic_run_id": str(report.id),
                "as_of": NOW.isoformat(),
            },
        )
        assert diagnostic["status"] == "ok"
        assert diagnostic["items"][0]["optimization_need"] == "RESEARCH_REQUIRED"
        security = await tools.execute(
            "get_security_snapshot",
            {
                "instrument_id": str(INSTRUMENT),
                "data_snapshot_id": str(snapshot.id),
                "as_of": NOW.isoformat(),
            },
        )
        assert security["portfolio_scope"]["portfolio_snapshot_id"] == str(portfolio.id)
        assert context["evidence_ref_id"] in {
            item["id"] for item in admission.run.evidence_manifest
        }
        with pytest.raises(ToolInputError):
            await tools.execute(
                "get_portfolio_exposure",
                {
                    "portfolio_snapshot_id": str(UUID(int=99)),
                    "as_of": NOW.isoformat(),
                },
            )
        with pytest.raises(ToolInputError):
            await tools.execute(
                "get_portfolio_diagnostics",
                {
                    "portfolio_snapshot_id": str(portfolio.id),
                    "diagnostic_run_id": str(UUID(int=99)),
                    "as_of": NOW.isoformat(),
                },
            )
        with pytest.raises(ToolInputError):
            await tools.execute(
                "get_portfolio_exposure",
                {
                    "portfolio_snapshot_id": str(portfolio.id),
                    "instrument_id": str(UUID(int=99)),
                    "as_of": NOW.isoformat(),
                },
            )
        # A changed inline context must not be trusted even though its SEC links still match.
        damaged = dict(snapshot.payload)
        damaged["portfolio_context"] = {**damaged["portfolio_context"], "position_weight": "0.99"}
        await session.execute(
            update(DataSnapshot).where(DataSnapshot.id == snapshot.id).values(payload=damaged)
        )
        rejected = await tools.execute(
            "get_portfolio_exposure",
            {
                "portfolio_snapshot_id": str(portfolio.id),
                "as_of": NOW.isoformat(),
            },
        )
        assert rejected["status"] == "unavailable" and rejected["items"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["not_run", "partial", "future", "missing_price"])
async def test_unready_or_future_portfolio_is_not_promoted(migrated_database_url, case):
    settings = EngineSettings(database_url=migrated_database_url)
    async with session_factory(settings)() as session, session.begin():
        await seed_filing(session)
        await seed_context(
            session,
            quality="NOT_RUN" if case == "not_run" else "PASS",
            status="PARTIAL" if case == "partial" else "READY",
            offset=1 if case == "future" else 0,
            price=None if case == "missing_price" else "20",
        )
        snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        inputs = await read_research_inputs(
            session,
            snapshot_id=snapshot.id,
            instrument_id=INSTRUMENT,
            as_of=NOW,
        )
        if case == "future":
            assert inputs.portfolio_context is None
        else:
            context = inputs.portfolio_context
            assert context is not None
            if case == "missing_price":
                assert context.position_weight is None and context.cash_weight is None
            else:
                assert context.snapshot_decision_ready is False
                assert context.diagnostic_run_id is None
                assert context.optimization_need is None
            assert context.execution_locked is True
