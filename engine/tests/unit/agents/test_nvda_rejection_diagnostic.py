import asyncio
import json

import pytest
from nexus.ops import historical_pool_diagnostic as diagnostic
from unit.agents.test_historical_nvda_observation import (
    install_config_check,
    invoke,
    source_case,
)
from unit.agents.test_historical_pool_diagnostic import install_transport


def parent_case(tmp_path, monkeypatch, capsys):
    root, source, case = source_case(tmp_path)
    install_config_check(monkeypatch)
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
    parent = (
        root
        / ".secrets/nexus-historical-nvda-observations"
        / f"{case['source_cycle_id']}-v1/case.sqlite3"
    )
    with diagnostic.CaseStore.open(parent) as store:
        store.claim()
        store.stage("research_2")
        store.diagnostic(
            dict(
                code="HISTORICAL_RESEARCH_NOT_ACTIONABLE",
                usage_complete=False,
                research_rejection=dict(unresolved_review_count=3),
            )
        )
        store.finish("unknown")
    return root, source, parent, case


@pytest.mark.parametrize("rejection", [None, "unresolved"])
def test_new_diagnostic_normal_cli_binds_both_parents_and_cannot_repeat(
    tmp_path, monkeypatch, capsys, rejection
):
    root, source, parent, case = parent_case(tmp_path, monkeypatch, capsys)
    old = [source.read_bytes(), parent.read_bytes()]
    calls = []
    install_transport(monkeypatch, calls, rejection=rejection)
    args = (
        "prepare-nvda-rejection-diagnostic",
        "--repo-root",
        root,
        "--source-cycle-id",
        case["source_cycle_id"],
    )
    assert invoke(monkeypatch, capsys, *args)[0] == 0
    directory = parent.parent.parent / f"{case['source_cycle_id']}-rejection-v2"
    with diagnostic.CaseStore.open(directory / "case.sqlite3") as store:
        metadata = store.case()["observation"]
        assert metadata["version"] == 2
        assert len(metadata["diagnostic_code_hash"]) == 64
        assert metadata["parent_observation_id"]
        assert store.private_rejection() is None
    code, result = invoke(monkeypatch, capsys, "run", "--case-dir", directory)
    assert code == (1 if rejection else 0)
    assert calls == ["research", "critique", "synthesis"]
    assert result["output_count"] == (0 if rejection else 1)
    assert result["business_writes"] == 0
    assert "PRIVATE_" not in json.dumps(result)
    if rejection:
        assert result["diagnostic"]["research_rejection"]["private_detail_available"]
    before = (directory / "case.sqlite3").read_bytes()
    assert invoke(monkeypatch, capsys, "read", "--case-dir", directory)[0] == code
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 1
    assert invoke(monkeypatch, capsys, *args)[0] == 1
    assert before == (directory / "case.sqlite3").read_bytes()
    assert old == [source.read_bytes(), parent.read_bytes()]
    assert len(calls) == 3


@pytest.mark.parametrize(
    "fault", ["parent_changed", "code_changed", "storage_failed", "timeout", "cancel"]
)
def test_new_diagnostic_rejects_drift_and_failure_without_retry(
    tmp_path, monkeypatch, capsys, fault
):
    from nexus.ops import nvda_rejection_diagnostic as new

    root, source, parent, case = parent_case(tmp_path, monkeypatch, capsys)
    assert (
        invoke(
            monkeypatch,
            capsys,
            "prepare-nvda-rejection-diagnostic",
            "--repo-root",
            root,
            "--source-cycle-id",
            case["source_cycle_id"],
        )[0]
        == 0
    )
    directory = parent.parent.parent / f"{case['source_cycle_id']}-rejection-v2"
    calls = []
    install_transport(monkeypatch, calls, fail_call=1 if fault == "timeout" else None)
    if fault == "parent_changed":
        with diagnostic.CaseStore.open(parent) as store:
            store.finish("observed")
    elif fault == "code_changed":
        monkeypatch.setattr(new, "diagnostic_code_hash", lambda: "0" * 64)
    elif fault == "storage_failed":

        def fail(*args):
            raise ValueError("HISTORICAL_DIAGNOSTIC_STORAGE_UNAVAILABLE")

        monkeypatch.setattr(new, "check_detail_storage", fail)
    elif fault == "cancel":

        async def cancel(*args):
            raise asyncio.CancelledError

        monkeypatch.setattr(diagnostic.FrozenCommittee, "analyze", cancel)
    code, result = invoke(monkeypatch, capsys, "run", "--case-dir", directory)
    assert code == 1
    assert len(calls) == (1 if fault == "timeout" else 0)
    if fault == "storage_failed":
        assert result["diagnostic"]["code"] == "HISTORICAL_DIAGNOSTIC_STORAGE_UNAVAILABLE"
        assert (
            invoke(monkeypatch, capsys, "read", "--case-dir", directory)[1]["diagnostic"]
            == result["diagnostic"]
        )
    with diagnostic.CaseStore.open(directory / "case.sqlite3") as store:
        assert store.db.execute("select status from historical_case").fetchone()[0] == "unknown"
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 1
    assert len(calls) == (1 if fault == "timeout" else 0)


@pytest.mark.parametrize(
    "fault", ["active_parent", "missing_parent", "pool_overflow", "storage", "configuration"]
)
def test_new_diagnostic_prepare_fails_before_any_model_call(tmp_path, monkeypatch, capsys, fault):
    from nexus.ops import historical_nvda_observation as observation
    from nexus.ops import nvda_rejection_diagnostic as new

    root, source, parent, case = parent_case(tmp_path, monkeypatch, capsys)
    calls = []
    install_transport(monkeypatch, calls)
    if fault == "active_parent":
        with diagnostic.CaseStore.open(parent) as store:
            store.db.execute("update historical_case set finished_at=NULL")
            store.db.commit()
    elif fault == "missing_parent":
        parent.rename(parent.with_suffix(".saved"))
    elif fault == "pool_overflow":

        async def reject(case):
            raise ValueError("HISTORICAL_CASE_POOL_INVALID")

        monkeypatch.setattr(new, "preflight_frozen_pool", reject)
    elif fault == "configuration":

        async def reject(*args):
            raise ValueError("HISTORICAL_CASE_BUDGET_UNAVAILABLE")

        monkeypatch.setattr(observation, "check_current_configuration", reject)
    else:

        def reject(*args):
            raise ValueError("HISTORICAL_DIAGNOSTIC_STORAGE_UNAVAILABLE")

        monkeypatch.setattr(new, "check_detail_storage", reject)
    code, result = invoke(
        monkeypatch,
        capsys,
        "prepare-nvda-rejection-diagnostic",
        "--repo-root",
        root,
        "--source-cycle-id",
        case["source_cycle_id"],
    )
    assert code == 1
    assert calls == []
    directory = parent.parent.parent / f"{case['source_cycle_id']}-rejection-v2"
    if fault == "storage":
        assert result["code"] == "HISTORICAL_DIAGNOSTIC_STORAGE_UNAVAILABLE"
        assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 1
        assert calls == []
    else:
        assert not directory.exists()


@pytest.mark.parametrize(
    "field", ["parent_observation_id", "parent_receipt_hash", "diagnostic_contract"]
)
def test_new_diagnostic_readback_rejects_rehashed_identity_tamper(
    tmp_path, monkeypatch, capsys, field
):
    root, source, parent, case = parent_case(tmp_path, monkeypatch, capsys)
    assert (
        invoke(
            monkeypatch,
            capsys,
            "prepare-nvda-rejection-diagnostic",
            "--repo-root",
            root,
            "--source-cycle-id",
            case["source_cycle_id"],
        )[0]
        == 0
    )
    directory = parent.parent.parent / f"{case['source_cycle_id']}-rejection-v2"
    with diagnostic.CaseStore.open(directory / "case.sqlite3") as store:
        value = store.case()
        value["observation"][field] = "0" * 64
        store.db.execute(
            "update historical_case set payload=?,digest=?",
            (diagnostic._wire(value), diagnostic._hash(value)),
        )
        store.db.commit()
    code, result = invoke(monkeypatch, capsys, "read", "--case-dir", directory)
    assert code == 1
    assert result["status"] == "blocked"
