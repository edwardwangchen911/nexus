import pytest


def test_private_store_claim_is_durable_and_never_retried(tmp_path):
    from nexus.ops.historical_pool_diagnostic import CaseStore

    tmp_path.chmod(0o700)
    with CaseStore.create(tmp_path / "case.sqlite3", {"case": "test"}) as store:
        store.claim()
    with CaseStore.open(tmp_path / "case.sqlite3") as store:
        assert store.summary()["status"] == "unknown"
        with pytest.raises(ValueError, match="HISTORICAL_CASE_ALREADY_ATTEMPTED"):
            store.claim()


def test_store_never_overwrites_any_existing_file(tmp_path):
    from nexus.ops.historical_pool_diagnostic import CaseStore

    tmp_path.chmod(0o700)
    path = tmp_path / "case.sqlite3"
    path.write_text("preserved")
    with pytest.raises(FileExistsError):
        CaseStore.create(path, {})
    assert path.read_text() == "preserved"


def test_store_has_only_diagnostic_tables_and_private_permissions(tmp_path):
    from nexus.ops.historical_pool_diagnostic import CaseStore

    tmp_path.chmod(0o700)
    path = tmp_path / "case.sqlite3"
    with CaseStore.create(path, {"case": "test"}) as store:
        tables = {
            r[0] for r in store.db.execute("select name from sqlite_master where type='table'")
        }
        assert tables == {"historical_case", "historical_outputs"}
        assert store.summary()["formal_acceptance"] is False
        assert store.summary()["execution_status"] == "EXECUTION_WINDOW_MISSING"
    assert path.stat().st_mode & 0o777 == 0o600


def test_store_refuses_symlink_and_non_private_parent(tmp_path):
    from nexus.ops.historical_pool_diagnostic import CaseStore

    tmp_path.chmod(0o755)
    with pytest.raises(ValueError):
        CaseStore.create(tmp_path / "case.sqlite3", {})
    tmp_path.chmod(0o700)
    with CaseStore.create(tmp_path / "real.sqlite3", {}):
        pass
    link = tmp_path / "link.sqlite3"
    link.symlink_to(tmp_path / "real.sqlite3")
    with pytest.raises(ValueError):
        CaseStore.open(link)


def test_readback_detects_changed_case(tmp_path):
    from nexus.ops.historical_pool_diagnostic import CaseStore

    tmp_path.chmod(0o700)
    with CaseStore.create(tmp_path / "case.sqlite3", {"case": "test"}) as store:
        store.db.execute("update historical_case set payload='{}'")
        store.db.commit()
        with pytest.raises(ValueError, match="HISTORICAL_CASE_HASH_MISMATCH"):
            store.summary()


def historical_case():
    import hashlib
    from datetime import UTC, datetime
    from decimal import Decimal
    from uuid import UUID

    from nexus.agents.codex_bounded_budget import bounded_stage_config_hash
    from nexus.agents.codex_sdk import MODEL_ID
    from nexus.agents.portfolio_synthesis_contracts import (
        SYNTHESIS_TOOL_HASH,
        PortfolioSynthesisIntentV1,
        canonical_portfolio_synthesis_hash,
    )
    from nexus.ops.historical_pool_diagnostic import PROFILE
    from nexus.risk.canonical import canonical_paper_projection_hash
    from nexus.risk.models import PaperPortfolioProjection
    from unit.agents.test_codex_sdk import _authority

    members = []
    at = datetime(2026, 10, 16, 14, tzinfo=UTC)
    for i in range(3):
        request, refs, prompts = _authority()
        request = request.model_copy(update={"instrument_id": UUID(int=i + 1), "decision_time": at})
        members.append(
            dict(
                source_run_id=str(UUID(int=100 + i)),
                paper_context_validated=True,
                request=request.model_dump(mode="json"),
                evidence=[r.model_dump(mode="json") for r in refs],
                evidence_ids=[str(UUID(int=10 + i))],
                inputs={"security_snapshot": {"items": []}, "financial_facts": {"items": []}},
                prompts={r: p.model_dump(mode="json") for r, p in prompts.items()},
                symbol=f"TEST{i}",
                snapshot_revision="a" * 64,
                portfolio_context=None,
            )
        )
    projection = PaperPortfolioProjection(
        account_id=UUID(int=20),
        portfolio_id=UUID(int=21),
        as_of=at,
        currency="USD",
        account_model="cash",
        position_model="long_only",
        available_cash=Decimal(100),
        reserved_cash=Decimal(0),
        positions=(),
        open_orders=(),
        nav=Decimal(100),
        peak_nav=Decimal(100),
        drawdown_state="unlocked",
        source_facts_hash="a" * 64,
        projection_hash="a" * 64,
    )
    projection = projection.model_copy(
        update={"projection_hash": canonical_paper_projection_hash(projection)}
    )
    prompt = "Compare the complete frozen historical pool."
    prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()
    return dict(
        profile=PROFILE,
        source_cycle_id=str(UUID(int=22)),
        source_profile="v10",
        evidence_as_of=at.isoformat(),
        compiled_at=datetime(2026, 10, 17, tzinfo=UTC).isoformat(),
        model_id=MODEL_ID,
        model_hash=bounded_stage_config_hash(),
        members=members,
        synthesis_prompt={"content": prompt, "content_hash": prompt_hash},
        synthesis_template=dict(
            schema_version="portfolio_synthesis_input_v1",
            cycle_id=str(UUID(int=23)),
            account_id=str(projection.account_id),
            portfolio_id=str(projection.portfolio_id),
            candidate_batch_id=str(UUID(int=24)),
            candidate_batch_hash="a" * 64,
            candidate_instrument_ids=[m["request"]["instrument_id"] for m in members],
            active_target_version_id=None,
            active_target_content_hash=None,
            active_target_instrument_ids=[],
            active_target_weights=[],
            active_target_cash_weight=None,
            projection=projection.model_dump(mode="json"),
            projection_hash=projection.projection_hash,
            policy_version="portfolio_allocation_v1",
            policy_hash="a" * 64,
            prompt_version_id=str(UUID(int=25)),
            prompt_version="portfolio-management-synthesis-v1",
            prompt_hash=prompt_hash,
            model_config_id=str(UUID(int=26)),
            model_hash=bounded_stage_config_hash(),
            pricing_version="codex-sdk-chatgpt-v11-bounded-stage-budget",
            tool_hash=SYNTHESIS_TOOL_HASH,
            intent_schema_hash=canonical_portfolio_synthesis_hash(
                PortfolioSynthesisIntentV1.model_json_schema()
            ),
            calendar_authority_id=str(UUID(int=27)),
            decision_time=at.isoformat(),
        ),
    )


def install_transport(
    monkeypatch, calls, *, fail_call=None, bad_reference=False, rejection=None, reject_member=1
):
    import json
    from uuid import UUID

    from nexus.agents import codex_staged, portfolio_synthesis_worker
    from nexus.agents.responses_budget import UsageSnapshot
    from unit.agents.test_codex_sdk import _assessment_payload, _staged_batch
    from unit.agents.test_portfolio_synthesis_worker import _decode_portfolio_view

    async def research(payload, **options):
        calls.append(payload["stage"])
        assert options["stage_budget_version"] == "codex-sdk-chatgpt-v11-bounded-stage-budget"
        assert 0 < options["timeout_seconds"] <= 288
        assert options["max_wire_bytes"] == 100479
        if len(calls) == fail_call:
            raise TimeoutError("sensitive text must never escape safe readback")
        if payload["stage"] == "synthesis":
            body = _assessment_payload(payload["previous_claims"])
            assessment = body["synthesis_assessment"]
            assessment["review_conclusion"] = "sufficient"
            for review in assessment["reviews"]:
                review.update(resolved=True, missing_evidence=[], prevented_conclusion=None)
            if len(calls) == reject_member * 3:
                if rejection in {"limited", "unresolved"}:
                    assessment["review_conclusion"] = "limited"
                if rejection == "unresolved":
                    assessment["reviews"][0].update(
                        resolved=False,
                        missing_evidence=["PRIVATE_MISSING_EVIDENCE"],
                        prevented_conclusion="PRIVATE_PREVENTED_CONCLUSION",
                    )
        else:
            body = _staged_batch(payload["stage"], payload["requested_roles"])
            for role in body["roles"]:
                role["vote"] = "support"
                if (
                    rejection == "audit"
                    and len(calls) == reject_member * 3 - 1
                    and role["role"] == "evidence_audit"
                ):
                    role["vote"] = "abstain"
        return json.dumps(body), UsageSnapshot(
            input_tokens=300, output_tokens=100, provider_calls=1
        )

    async def synthesis(payload, **options):
        calls.append("portfolio")
        assert options["max_wire_bytes"] == 64000
        frozen = _decode_portfolio_view(payload["frozen_input"])
        assert frozen["decision_time"].startswith("2026-10-16")
        assert frozen["admitted_at"] != frozen["decision_time"]
        choices = []
        for row in frozen["security_outputs"]:
            ref = dict(
                research_output_id=row["research_output_id"],
                evidence_ref_id=row["evidence_ref_ids"][0],
            )
            if bad_reference:
                ref["evidence_ref_id"] = str(UUID(int=9999))
            statement = dict(text="Frozen source supports this comparison.", references=[ref])
            choices.append(
                dict(
                    instrument_id=row["instrument_id"],
                    action="defer",
                    role="none",
                    rank=None,
                    replaces_instrument_id=None,
                    depends_on_instrument_ids=[],
                    thesis=statement,
                    counter_thesis=statement,
                    invalidation=statement,
                )
            )
        return json.dumps(
            dict(
                schema_version="portfolio_synthesis_intent_v1",
                input_hash=payload["input_hash"],
                compared_instrument_ids=frozen["candidate_instrument_ids"],
                choices=choices,
                cash_rationale=statement,
            )
        ), UsageSnapshot(input_tokens=300, output_tokens=100, provider_calls=1)

    monkeypatch.setattr(codex_staged, "invoke_codex", research)
    monkeypatch.setattr(portfolio_synthesis_worker, "invoke_codex_portfolio", synthesis)


@pytest.mark.asyncio
async def test_three_stock_v11_research_synthesis_private_readback(tmp_path, monkeypatch):
    from nexus.ops.historical_pool_diagnostic import CaseStore, execute_case

    tmp_path.chmod(0o700)
    calls = []
    install_transport(monkeypatch, calls)
    path = tmp_path / "case.sqlite3"
    with CaseStore.create(path, historical_case()) as store:
        result = await execute_case(store)
        assert result["status"] == "observed"
        assert result["output_count"] == 4
        assert calls == ["research", "critique", "synthesis"] * 3 + ["portfolio"]
        assert result["execution_status"] == "EXECUTION_WINDOW_MISSING"
    with CaseStore.open(path) as store:
        assert store.summary() == result
        with pytest.raises(ValueError, match="ALREADY_ATTEMPTED"):
            await execute_case(store)
    assert len(calls) == 10


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "missing_member",
        "prompt_changed",
        "evidence_future",
        "pool_overflow",
        "projection_changed",
        "paper_context_missing",
    ],
)
async def test_complete_pool_gate_before_any_call(tmp_path, monkeypatch, failure):
    from nexus.ops.historical_pool_diagnostic import CaseStore, execute_case

    tmp_path.chmod(0o700)
    case = historical_case()
    if failure == "missing_member":
        case["members"].pop()
    if failure == "prompt_changed":
        case["members"][1]["prompts"]["bull"]["content"] = "changed"
    if failure == "evidence_future":
        case["members"][2]["evidence"][0]["available_at"] = "2099-01-01T00:00:00Z"
    if failure == "pool_overflow":
        case["members"][2]["inputs"]["financial_facts"]["items"] = [{"text": "x" * 150000}]
    if failure == "projection_changed":
        case["synthesis_template"]["projection"]["available_cash"] = "999"
    if failure == "paper_context_missing":
        case["members"][2]["paper_context_validated"] = False
    calls = []
    install_transport(monkeypatch, calls)
    with CaseStore.create(tmp_path / "case.sqlite3", case) as store:
        with pytest.raises(ValueError):
            await execute_case(store)
        assert store.summary()["status"] == "unknown"
    assert calls == []


@pytest.mark.asyncio
async def test_second_stock_unknown_stops_third_and_keeps_first_output(tmp_path, monkeypatch):
    from nexus.ops.historical_pool_diagnostic import CaseStore, execute_case

    tmp_path.chmod(0o700)
    calls = []
    install_transport(monkeypatch, calls, fail_call=4)
    with CaseStore.create(tmp_path / "case.sqlite3", historical_case()) as store:
        with pytest.raises(TimeoutError):
            await execute_case(store)
        result = store.summary()
        assert result["stage"] == "research_2"
        assert result["diagnostic"]["code"] == "TIMEOUT"
        assert result["diagnostic"]["unknown_reservations"] == "held"
        assert result["diagnostic"]["stage_failure"]["stage"] == "research"
        assert result["status"] == "unknown"
        assert result["output_count"] == 1
        with pytest.raises(ValueError, match="ALREADY_ATTEMPTED"):
            await execute_case(store)
    assert calls == ["research", "critique", "synthesis", "research"]
    assert "sensitive" not in str(result)


@pytest.mark.asyncio
async def test_synthesis_cannot_bind_foreign_evidence(tmp_path, monkeypatch):
    from nexus.ops.historical_pool_diagnostic import CaseStore, execute_case

    tmp_path.chmod(0o700)
    calls = []
    install_transport(monkeypatch, calls, bad_reference=True)
    with CaseStore.create(tmp_path / "case.sqlite3", historical_case()) as store:
        with pytest.raises(ValueError, match="unbound"):
            await execute_case(store)
        assert store.summary()["output_count"] == 3
        assert store.summary()["status"] == "unknown"


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["output_digest", "input_binding", "evidence_binding"])
async def test_completed_readback_rejects_changed_results(tmp_path, monkeypatch, mutation):
    import json

    from nexus.ops.historical_pool_diagnostic import CaseStore, _hash, _wire, execute_case

    tmp_path.chmod(0o700)
    calls = []
    install_transport(monkeypatch, calls)
    with CaseStore.create(tmp_path / "case.sqlite3", historical_case()) as store:
        await execute_case(store)
        raw, digest = store.db.execute(
            "select payload,digest from historical_outputs where ordinal=3"
        ).fetchone()
        payload = json.loads(raw)
        if mutation == "input_binding":
            payload["input_hash"] = "f" * 64
        elif mutation == "evidence_binding":
            payload["intent"]["choices"][0]["thesis"]["references"][0]["evidence_ref_id"] = str(
                __import__("uuid").UUID(int=9000)
            )
        else:
            digest = "f" * 64
        store.db.execute(
            "update historical_outputs set payload=?,digest=? where ordinal=3",
            (_wire(payload), digest if mutation == "output_digest" else _hash(payload)),
        )
        store.db.commit()
        with pytest.raises(ValueError):
            store.summary()
    assert len(calls) == 10


@pytest.mark.asyncio
async def test_cancellation_keeps_claim_and_known_usage(tmp_path, monkeypatch):
    import asyncio

    from nexus.ops.historical_pool_diagnostic import CaseStore, FrozenCommittee, execute_case

    async def cancel(*args):
        raise asyncio.CancelledError

    monkeypatch.setattr(FrozenCommittee, "analyze", cancel)
    tmp_path.chmod(0o700)
    path = tmp_path / "case.sqlite3"
    with CaseStore.create(path, historical_case()) as store:
        with pytest.raises(asyncio.CancelledError):
            await execute_case(store)
    with CaseStore.open(path) as store:
        result = store.summary()
        assert result["diagnostic"]["code"] == "CANCELLED"
        assert result["status"] == "unknown"
        assert result["output_count"] == 0
        with pytest.raises(ValueError, match="ALREADY_ATTEMPTED"):
            await execute_case(store)


def test_normal_cli_prepare_run_read_and_repeated_run_rejected(tmp_path, monkeypatch, capsys):
    import json
    import sys
    from uuid import UUID

    from nexus.ops import historical_pool_source as source
    from nexus.ops.historical_pool_diagnostic import main

    root = tmp_path / "repo"
    root.mkdir()
    (root / ".secrets").mkdir(mode=0o700)
    case = historical_case()
    calls = []
    install_transport(monkeypatch, calls)

    async def load(settings, cycle_id):
        assert cycle_id == UUID(case["source_cycle_id"])
        return case

    monkeypatch.setattr(source, "local_settings", lambda *a, **k: object())
    monkeypatch.setattr(source, "load_historical_pool", load)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "historical",
            "prepare",
            "--repo-root",
            str(root),
            "--source-cycle-id",
            case["source_cycle_id"],
        ],
    )
    assert main() == 0
    assert json.loads(capsys.readouterr().out)["status"] == "prepared"
    assert calls == []
    directory = root / ".secrets/nexus-historical-three-stock" / f"{case['source_cycle_id']}-v11"
    for command in ("run", "read"):
        monkeypatch.setattr(sys, "argv", ["historical", command, "--case-dir", str(directory)])
        assert main() == 0
        output = json.loads(capsys.readouterr().out)
        assert output["status"] == "observed"
        assert "account_id" not in str(output)
        assert output["evidence_as_of"] != output["observation_started_at"]
    monkeypatch.setattr(sys, "argv", ["historical", "run", "--case-dir", str(directory)])
    assert main() == 1
    assert len(calls) == 10


@pytest.mark.parametrize(
    "message,expected",
    [
        ("HISTORICAL_CASE_SOURCE_INVALID", "HISTORICAL_CASE_SOURCE_INVALID"),
        ("HISTORICAL_CASE_SOURCE_INVALID password=private", "UNKNOWN"),
        ("raw response or credential", "UNKNOWN"),
    ],
)
def test_only_exact_fixed_failure_codes_are_exposed(message, expected):
    from nexus.ops.historical_pool_diagnostic import safe_error_code

    assert safe_error_code(ValueError(message)) == expected


@pytest.mark.parametrize(
    "rejection,expected",
    [
        ("limited", {"REVIEW_CONCLUSION_NOT_SUFFICIENT"}),
        ("unresolved", {"REVIEW_CONCLUSION_NOT_SUFFICIENT", "UNRESOLVED_REVIEW"}),
        (
            "audit",
            {
                "REVIEW_CONCLUSION_NOT_SUFFICIENT",
                "EVIDENCE_AUDIT_NOT_SUPPORTED",
                "EVIDENCE_AUDIT_INCOMPLETE",
            },
        ),
    ],
)
def test_cli_research_rejection_reasons_survive_readback_without_retry(
    tmp_path, monkeypatch, capsys, rejection, expected
):
    """SDK output -> committee -> intent gates -> SQLite -> CLI read preserves reasons."""
    import json
    import sys

    from nexus.ops.historical_pool_diagnostic import CaseStore, main

    tmp_path.chmod(0o700)
    calls = []
    install_transport(monkeypatch, calls, rejection=rejection, reject_member=2)
    with CaseStore.create(tmp_path / "case.sqlite3", historical_case()):
        pass
    monkeypatch.setattr(sys, "argv", ["historical", "run", "--case-dir", str(tmp_path)])
    assert main() == 1
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "unknown"
    assert result["stage"] == "research_2"
    assert result["output_count"] == 1
    assert result["observation_finished_at"] is not None
    detail = result["diagnostic"]
    assert detail["code"] == "HISTORICAL_RESEARCH_NOT_ACTIONABLE"
    rejection_detail = detail["research_rejection"]
    assert rejection_detail["semantics_valid"] is False
    assert rejection_detail["quality_complete"] is False
    assert expected <= set(rejection_detail["reason_codes"])
    assert {"SEMANTICS_INVALID", "QUALITY_INCOMPLETE"} <= set(rejection_detail["reason_codes"])
    assert rejection_detail["unresolved_review_count"] == (1 if rejection == "unresolved" else 0)
    assert rejection_detail["assessment_conclusion_downgraded"] == (rejection == "audit")
    assert detail["known_usage"]["provider_calls"] == 3
    assert detail["usage_complete"] is False
    assert detail["unknown_reservations"] == "held"
    assert calls == ["research", "critique", "synthesis"] * 2
    assert result["business_writes"] == 0
    assert result["execution_locked"] is True
    assert result["formal_acceptance"] is False
    assert "PRIVATE_" not in json.dumps(result)
    assert "account_id" not in json.dumps(result)
    with CaseStore.open(tmp_path / "case.sqlite3") as store:
        before = list(store.db.iterdump())
        assert "PRIVATE_" not in str(before)
    for command in ("read", "run"):
        monkeypatch.setattr(sys, "argv", ["historical", command, "--case-dir", str(tmp_path)])
        assert main() == 1
        reread = json.loads(capsys.readouterr().out)
        assert reread["diagnostic"] == detail
    with CaseStore.open(tmp_path / "case.sqlite3") as store:
        assert list(store.db.iterdump()) == before
    assert len(calls) == 6


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault,reasons",
    [
        ("exposure", {"EXPOSURE_ACTION_MISMATCH"}),
        ("assessment", {"ASSESSMENT_MISSING", "REVIEW_COVERAGE_INVALID"}),
        ("audit_status", {"EVIDENCE_AUDIT_INCOMPLETE"}),
        ("role_count", {"ROLE_COUNT_INVALID"}),
        ("audit_vote", {"EVIDENCE_AUDIT_NOT_SUPPORTED"}),
        ("stance", {"STANCE_MISMATCH"}),
        ("theses", {"THESES_MISSING"}),
        ("counter_theses", {"COUNTER_THESES_MISSING"}),
        ("review_coverage", {"REVIEW_COVERAGE_INVALID"}),
    ],
)
async def test_semantic_only_rejection_is_not_mislabeled_as_quality_failure(
    tmp_path, monkeypatch, fault, reasons
):
    """Inject an inconsistent intent at synthesis boundary, keep both real gates active."""
    from nexus.ops import historical_pool_diagnostic as module

    original = module.synthesize_intent

    def inconsistent(*args, **kwargs):
        intent = original(*args, **kwargs)
        updates = {
            "exposure": {"exposure_action": "none"},
            "assessment": {"synthesis_assessment": None},
            "audit_status": {"evidence_audit": "partial"},
            "role_count": {"role_votes": tuple(v for v in intent.role_votes if v.role != "bull")},
            "audit_vote": {
                "role_votes": tuple(
                    v.model_copy(update={"vote": "abstain"}) if v.role == "evidence_audit" else v
                    for v in intent.role_votes
                )
            },
            "stance": {
                "synthesis_assessment": intent.synthesis_assessment.model_copy(
                    update={"stance": "hold"}
                )
            },
            "theses": {"theses": ()},
            "counter_theses": {"counter_theses": ()},
            "review_coverage": {
                "synthesis_assessment": intent.synthesis_assessment.model_copy(
                    update={
                        "reviews": tuple(
                            r
                            for r in intent.synthesis_assessment.reviews
                            if r.claim_id.startswith("bear:")
                        )
                    }
                )
            },
        }
        return intent.model_copy(update=updates[fault])

    monkeypatch.setattr(module, "synthesize_intent", inconsistent)
    calls = []
    install_transport(monkeypatch, calls)
    tmp_path.chmod(0o700)
    with module.CaseStore.create(tmp_path / "case.sqlite3", historical_case()) as store:
        with pytest.raises(ValueError, match="HISTORICAL_RESEARCH_NOT_ACTIONABLE"):
            await module.execute_case(store)
        detail = store.summary()["diagnostic"]["research_rejection"]
        assert detail["semantics_valid"] is False
        assert detail["quality_complete"] is True
        assert set(detail["reason_codes"]) == {"SEMANTICS_INVALID"} | reasons
        assert store.summary()["output_count"] == 0
    assert len(calls) == 3


def test_legacy_unknown_readback_never_invents_missing_rejection_detail(
    tmp_path, monkeypatch, capsys
):
    import json
    import sys

    from nexus.ops.historical_pool_diagnostic import CaseStore, main

    tmp_path.chmod(0o700)
    path = tmp_path / "case.sqlite3"
    with CaseStore.create(path, historical_case()) as store:
        store.claim()
        store.stage("research_1")
        store.diagnostic({"code": "HISTORICAL_RESEARCH_NOT_ACTIONABLE", "usage_complete": False})
        store.finish("unknown")
    before = path.read_bytes()
    monkeypatch.setattr(sys, "argv", ["historical", "read", "--case-dir", str(tmp_path)])
    assert main() == 1
    result = json.loads(capsys.readouterr().out)
    assert result["diagnostic"] == {
        "code": "HISTORICAL_RESEARCH_NOT_ACTIONABLE",
        "usage_complete": False,
    }
    assert path.read_bytes() == before
