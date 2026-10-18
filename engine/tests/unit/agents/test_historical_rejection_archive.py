import json
import sqlite3

import pytest
from nexus.ops import historical_pool_diagnostic as diagnostic
from unit.agents.test_historical_nvda_observation import (
    install_config_check,
    invoke,
    source_case,
)
from unit.agents.test_historical_pool_diagnostic import install_transport


def rejected_case(tmp_path, monkeypatch, capsys, *, three_reviews=False):
    root, source, case = source_case(tmp_path)
    original = source.read_bytes()
    install_config_check(monkeypatch)
    calls = []
    install_transport(monkeypatch, calls, rejection="unresolved")
    if three_reviews:
        from nexus.agents import codex_staged

        original_transport = codex_staged.invoke_codex

        async def transport(payload, **options):
            raw, usage = await original_transport(payload, **options)
            body = json.loads(raw)
            if payload["stage"] == "critique":
                bear = next(row for row in body["roles"] if row["role"] == "bear")
                bear["claims"].append(dict(text="PRIVATE_SECOND_BEAR_CLAIM", citations=[0]))
            elif payload["stage"] == "synthesis":
                assessment = body["synthesis_assessment"]
                assessment["review_conclusion"] = "insufficient"
                for i, review in enumerate(assessment["reviews"]):
                    review.update(
                        status="unresolved",
                        resolved=False,
                        rationale=f"PRIVATE_REASON_{i}",
                        missing_evidence=[f"PRIVATE_GAP_{i}"],
                        prevented_conclusion=f"PRIVATE_CONSEQUENCE_{i}",
                    )
            return json.dumps(body), usage

        monkeypatch.setattr(codex_staged, "invoke_codex", transport)
    assert (
        invoke(
            monkeypatch,
            capsys,
            "prepare-nvda-observation",
            "--repo-root",
            root,
            "--source-cycle-id",
            case["source_cycle_id"],
        )[0]
        == 0
    )
    directory = (
        root / ".secrets/nexus-historical-nvda-observations" / f"{case['source_cycle_id']}-v1"
    )
    code, result = invoke(monkeypatch, capsys, "run", "--case-dir", directory)
    assert code == 1
    assert result["status"] == "unknown"
    assert result["diagnostic"]["code"] == "HISTORICAL_RESEARCH_NOT_ACTIONABLE"
    assert result["output_count"] == 0
    assert source.read_bytes() == original
    assert calls == ["research", "critique", "synthesis"]
    return directory, result, calls


def test_rejected_nvda_keeps_private_review_chain_but_cli_never_prints_text(
    tmp_path, monkeypatch, capsys
):
    directory, result, calls = rejected_case(tmp_path, monkeypatch, capsys)
    with diagnostic.CaseStore.open(directory / "case.sqlite3", read_only=True) as store:
        raw = json.loads(store.db.execute("select diagnostic from historical_case").fetchone()[0])
        assert "PRIVATE_" not in json.dumps(raw)
        archive = store.private_rejection()["payload"]
        assert archive["case_hash"] == diagnostic._hash(store.case())
        assert archive["member_ordinal"] == 1
        unresolved = [item for item in archive["items"] if not item["review"]["resolved"]]
        assert len(unresolved) == 1
        item = unresolved[0]
        assert item["review"]["missing_evidence"] == ["PRIVATE_MISSING_EVIDENCE"]
        assert item["review"]["prevented_conclusion"] == "PRIVATE_PREVENTED_CONCLUSION"
        assert item["review"]["rationale"].strip()
        assert item["claim"]["text"].strip()
        assert item["claim"]["evidence_refs"]
        assert archive["review_conclusion"] == "limited"
        assert archive["version"] == 2
        assert archive["outcome"]["stance"] == "watch"
        assert archive["outcome"]["exposure_action"] == "none"
        assert archive["outcome"]["quality_status"] == "partial"
        assert archive["outcome"]["original_stance"] in {
            "watch",
            "hold",
            "buy_candidate",
            "reduce_candidate",
        }
        assert archive["outcome"]["audit_vote"] in {"support", "oppose", "abstain"}
        assert archive["outcome"]["execution_permitted"] is False
    safe = result["diagnostic"]["research_rejection"]["review_details"]
    assert result["diagnostic"]["research_rejection"]["outcome"] == archive["outcome"]
    assert any(not item["resolved"] and item["missing_evidence_count"] == 1 for item in safe)
    assert "PRIVATE_" not in json.dumps(result)
    assert "rationale" not in json.dumps(result)
    before = (directory / "case.sqlite3").read_bytes()
    code, read = invoke(monkeypatch, capsys, "read", "--case-dir", directory)
    assert code == 1
    assert read["diagnostic"] == result["diagnostic"]
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 1
    assert (directory / "case.sqlite3").read_bytes() == before
    assert len(calls) == 3


def test_three_unresolved_reviews_keep_exact_claim_reason_gap_and_consequence(
    tmp_path, monkeypatch, capsys
):
    directory, result, calls = rejected_case(tmp_path, monkeypatch, capsys, three_reviews=True)
    assert result["diagnostic"]["research_rejection"]["unresolved_review_count"] == 3
    with diagnostic.CaseStore.open(directory / "case.sqlite3", read_only=True) as store:
        items = store.private_rejection()["payload"]["items"]
        assert [item["review"]["claim_id"] for item in items] == [
            "bear:0",
            "bear:1",
            "evidence_audit:0",
        ]
        assert items[1]["claim"]["text"] == "PRIVATE_SECOND_BEAR_CLAIM"
        for i, item in enumerate(items):
            assert item["review"]["rationale"] == f"PRIVATE_REASON_{i}"
            assert item["review"]["missing_evidence"] == [f"PRIVATE_GAP_{i}"]
            assert item["review"]["prevented_conclusion"] == f"PRIVATE_CONSEQUENCE_{i}"
    assert "PRIVATE_" not in json.dumps(result)
    assert len(calls) == 3


def test_archive_capture_failure_keeps_primary_refusal_and_no_retry(tmp_path, monkeypatch, capsys):
    from nexus.ops import historical_rejection_archive as archive

    def fail(*args):
        raise RuntimeError("PRIVATE_CAPTURE_FAILURE")

    monkeypatch.setattr(archive, "capture_rejection", fail)
    directory, result, calls = rejected_case(tmp_path, monkeypatch, capsys)
    assert result["diagnostic"]["rejection_archive_status"] == "UNAVAILABLE"
    assert result["diagnostic"]["unknown_reservations"] == "held"
    assert result["diagnostic"]["research_rejection"]["unresolved_review_count"] == 1
    assert "PRIVATE_" not in json.dumps(result)
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 1
    assert len(calls) == 3
    assert "rationale" not in json.dumps(result)
    before = (directory / "case.sqlite3").read_bytes()
    code, read = invoke(monkeypatch, capsys, "read", "--case-dir", directory)
    assert code == 1
    assert read["diagnostic"] == result["diagnostic"]
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 1
    assert (directory / "case.sqlite3").read_bytes() == before
    assert len(calls) == 3


@pytest.mark.parametrize("fault", ["digest", "source", "ordinal", "duplicate", "foreign_evidence"])
def test_corrupt_private_review_chain_is_rejected_without_disclosure(
    tmp_path, monkeypatch, capsys, fault
):
    directory, _, _ = rejected_case(tmp_path, monkeypatch, capsys)
    with diagnostic.CaseStore.open(directory / "case.sqlite3") as store:
        envelope = store.private_rejection()
        archive = envelope["payload"]
        if fault == "digest":
            archive["items"][0]["review"]["rationale"] = "PRIVATE_TAMPER"
        elif fault == "source":
            archive["case_hash"] = "0" * 64
        elif fault == "ordinal":
            archive["member_ordinal"] = 0
        elif fault == "duplicate":
            archive["items"].append(archive["items"][0])
        else:
            archive["items"][0]["review"]["evidence_refs"][0]["source_id"] = "foreign"
        if fault != "digest":
            envelope["digest"] = diagnostic._hash(archive)
        store.db.execute(
            "update historical_rejection_details set payload=?,digest=?",
            (diagnostic._wire(archive), envelope["digest"]),
        )
        store.db.commit()
    code, result = invoke(monkeypatch, capsys, "read", "--case-dir", directory)
    assert code == 1
    assert result["status"] == "blocked"
    assert result["code"] == "HISTORICAL_CASE_READBACK_INVALID"
    assert "PRIVATE_" not in json.dumps(result)


def test_private_archive_and_safe_refusal_are_committed_atomically(tmp_path):
    from unit.agents.test_historical_pool_diagnostic import historical_case

    tmp_path.chmod(0o700)
    with diagnostic.CaseStore.create(tmp_path / "case.sqlite3", historical_case()) as store:
        store.claim()
        store.db.execute(
            "CREATE TRIGGER reject_diagnostic BEFORE UPDATE OF diagnostic "
            "ON historical_case BEGIN SELECT RAISE(ABORT,'synthetic'); END"
        )
        store.db.commit()
        with pytest.raises(sqlite3.IntegrityError):
            store.diagnostic(
                dict(
                    code="HISTORICAL_RESEARCH_NOT_ACTIONABLE",
                    private_research_rejection=dict(
                        payload={"text": "PRIVATE_ARCHIVE"}, digest="0" * 64
                    ),
                )
            )
        assert store.private_rejection() is None
        assert store.summary()["status"] == "unknown"
        assert store.summary()["diagnostic"] == {}
        assert "PRIVATE_ARCHIVE" not in str(list(store.db.iterdump()))


def test_private_rejection_cannot_be_overwritten(tmp_path, monkeypatch, capsys):
    directory, _, _ = rejected_case(tmp_path, monkeypatch, capsys)
    with diagnostic.CaseStore.open(directory / "case.sqlite3") as store:
        before = list(store.db.iterdump())
        with pytest.raises(sqlite3.IntegrityError):
            store.diagnostic(
                dict(
                    code="HISTORICAL_RESEARCH_NOT_ACTIONABLE",
                    private_research_rejection=store.private_rejection(),
                )
            )
        assert list(store.db.iterdump()) == before


def test_legacy_rejection_keeps_unknown_stance_without_backfill(tmp_path, monkeypatch, capsys):
    directory, _, _ = rejected_case(tmp_path, monkeypatch, capsys)
    with diagnostic.CaseStore.open(directory / "case.sqlite3") as store:
        payload = store.private_rejection()["payload"]
        payload["version"] = 1
        payload.pop("outcome")
        store.db.execute(
            "UPDATE historical_rejection_details SET payload=?,digest=?",
            (diagnostic._wire(payload), diagnostic._hash(payload)),
        )
        store.db.commit()
    before = (directory / "case.sqlite3").read_bytes()
    result = invoke(monkeypatch, capsys, "read", "--case-dir", directory)[1]
    assert result["status"] == "unknown"
    assert before == (directory / "case.sqlite3").read_bytes()


@pytest.mark.parametrize(
    "field,value",
    [("audit_vote", "PRIVATE"), ("execution_permitted", True), ("quality_status", "INVALID")],
)
def test_v2_outcome_corruption_is_rejected(tmp_path, monkeypatch, capsys, field, value):
    directory, _, _ = rejected_case(tmp_path, monkeypatch, capsys)
    with diagnostic.CaseStore.open(directory / "case.sqlite3") as store:
        payload = store.private_rejection()["payload"]
        payload["outcome"][field] = value
        store.db.execute(
            "UPDATE historical_rejection_details SET payload=?,digest=?",
            (diagnostic._wire(payload), diagnostic._hash(payload)),
        )
        store.db.commit()
    result = invoke(monkeypatch, capsys, "read", "--case-dir", directory)[1]
    assert result["code"] == "HISTORICAL_CASE_READBACK_INVALID"


def test_private_insert_failure_preserves_primary_refusal_through_cli(
    tmp_path, monkeypatch, capsys
):
    original_open = diagnostic.CaseStore.open

    def open_store(cls, path, *, read_only=False):
        store = original_open(path, read_only=read_only)

        def authorize(action, table, *args):
            if action == sqlite3.SQLITE_INSERT and table == "historical_rejection_details":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.db.set_authorizer(authorize)
        return store

    monkeypatch.setattr(diagnostic.CaseStore, "open", classmethod(open_store))
    directory, result, calls = rejected_case(tmp_path, monkeypatch, capsys)
    assert result["diagnostic"]["rejection_archive_status"] == "UNAVAILABLE"
    assert result["diagnostic"]["unknown_reservations"] == "held"
    assert result["diagnostic"]["known_usage"]["provider_calls"] == 3
    assert result["diagnostic"]["research_rejection"]["unresolved_review_count"] == 1
    assert "PRIVATE_" not in json.dumps(result)
    with diagnostic.CaseStore.open(directory / "case.sqlite3", read_only=True) as store:
        assert store.private_rejection() is None
    before = (directory / "case.sqlite3").read_bytes()
    assert (
        invoke(monkeypatch, capsys, "read", "--case-dir", directory)[1]["diagnostic"]
        == result["diagnostic"]
    )
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 1
    assert (directory / "case.sqlite3").read_bytes() == before
    assert len(calls) == 3
