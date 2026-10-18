import json

import pytest
from nexus.ops import historical_pool_diagnostic as diagnostic
from unit.agents.test_historical_nvda_observation import invoke
from unit.agents.test_historical_pool_diagnostic import install_transport
from unit.agents.test_nvda_rejection_diagnostic import parent_case


def accepted_parent(tmp_path, monkeypatch, capsys):
    root, source, parent, case = parent_case(tmp_path, monkeypatch, capsys)
    calls = []
    install_transport(monkeypatch, calls)
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
    accepted = parent.parent.parent / f"{case['source_cycle_id']}-rejection-v2"
    assert invoke(monkeypatch, capsys, "run", "--case-dir", accepted)[0] == 0
    from nexus.ops import historical_nvda_observation as observation

    async def check(root, case, *, budget_units):
        assert budget_units == 3

    monkeypatch.setattr(observation, "check_current_configuration", check)
    return root, source, parent, accepted / "case.sqlite3", case


def test_completion_uses_accepted_nvda_and_only_two_new_researches(tmp_path, monkeypatch, capsys):
    root, source, parent, accepted, case = accepted_parent(tmp_path, monkeypatch, capsys)
    old = [p.read_bytes() for p in (source, parent, accepted)]
    calls = []
    install_transport(monkeypatch, calls)
    args = (
        "prepare-pool-completion",
        "--repo-root",
        root,
        "--source-cycle-id",
        case["source_cycle_id"],
    )
    code, prepared = invoke(monkeypatch, capsys, *args)
    assert code == 0
    directory = (
        root / ".secrets/nexus-historical-three-stock" / f"{case['source_cycle_id']}-completion-v1"
    )
    code, result = invoke(monkeypatch, capsys, "run", "--case-dir", directory)
    assert code == 0
    assert calls == ["research", "critique", "synthesis"] * 2 + ["portfolio"]
    assert result["output_count"] == 4
    assert result["reused_research_count"] == 1
    assert result["new_provider_calls"] == 7
    assert result["execution_status"] == "HISTORICAL_EXECUTION_NOT_ADMITTED"
    assert result["formal_acceptance"] is False
    assert invoke(monkeypatch, capsys, "read", "--case-dir", directory)[1] == result
    before = (directory / "case.sqlite3").read_bytes()
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 1
    assert invoke(monkeypatch, capsys, *args)[0] == 1
    assert len(calls) == 7
    assert before == (directory / "case.sqlite3").read_bytes()
    assert old == [p.read_bytes() for p in (source, parent, accepted)]


def test_completion_portfolio_reference_failure_retains_stage_and_usage(
    tmp_path, monkeypatch, capsys
):
    root, _, _, _, case = accepted_parent(tmp_path, monkeypatch, capsys)
    calls = []
    install_transport(monkeypatch, calls, bad_reference=True)
    assert (
        invoke(
            monkeypatch,
            capsys,
            "prepare-pool-completion",
            "--repo-root",
            root,
            "--source-cycle-id",
            case["source_cycle_id"],
        )[0]
        == 0
    )
    directory = (
        root / ".secrets/nexus-historical-three-stock" / f"{case['source_cycle_id']}-completion-v1"
    )
    code, result = invoke(monkeypatch, capsys, "run", "--case-dir", directory)
    assert code == 1
    assert result["stage"] == "portfolio_synthesis"
    assert result["diagnostic"]["code"] == "HISTORICAL_SYNTHESIS_EVIDENCE_UNBOUND"
    assert result["new_provider_calls"] == 7
    assert result["output_count"] == 3
    assert result["diagnostic"]["known_usage"]["provider_calls"] == 1
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 1
    assert len(calls) == 7


@pytest.mark.parametrize(
    "fault",
    ["parent", "code", "copied", "metadata", "downgrade", "stripped", "timeout", "rejection"],
)
def test_completion_drift_or_failure_stops_without_replay(tmp_path, monkeypatch, capsys, fault):
    root, source, parent, accepted, case = accepted_parent(tmp_path, monkeypatch, capsys)
    calls = []
    install_transport(
        monkeypatch,
        calls,
        fail_call=1 if fault == "timeout" else None,
        rejection="unresolved" if fault == "rejection" else None,
    )
    assert (
        invoke(
            monkeypatch,
            capsys,
            "prepare-pool-completion",
            "--repo-root",
            root,
            "--source-cycle-id",
            case["source_cycle_id"],
        )[0]
        == 0
    )
    directory = (
        root / ".secrets/nexus-historical-three-stock" / f"{case['source_cycle_id']}-completion-v1"
    )
    if fault == "parent":
        with diagnostic.CaseStore.open(accepted) as store:
            store.db.execute("UPDATE historical_case SET finished_at='changed'")
            store.db.commit()
    elif fault == "code":
        from nexus.ops import historical_pool_completion as completion

        monkeypatch.setattr(completion, "code_hash", lambda: "0" * 64)
    elif fault == "copied":
        import shutil

        target = directory.parent / "copied"
        shutil.copytree(directory, target)
        directory = target
    elif fault in {"metadata", "downgrade", "stripped"}:
        with diagnostic.CaseStore.open(directory / "case.sqlite3") as store:
            value = store.case()
            if fault == "metadata":
                value["completion"]["parent_observation_id"] = "foreign"
            else:
                value["profile"] = diagnostic.PROFILE
                if fault == "stripped":
                    value.pop("completion")
            store.db.execute(
                "UPDATE historical_case SET payload=?,digest=?",
                (json.dumps(value), diagnostic._hash(value)),
            )
            store.db.commit()
    code, result = invoke(monkeypatch, capsys, "run", "--case-dir", directory)
    assert code == 1
    assert len(calls) == (1 if fault == "timeout" else 3 if fault == "rejection" else 0)
    if fault in {"timeout", "rejection"}:
        assert result["status"] == "unknown"
        assert result["diagnostic"]["unknown_reservations"] == "held"
        if fault == "rejection":
            assert result["diagnostic"]["research_rejection"]["private_detail_available"]
    before = (directory / "case.sqlite3").read_bytes()
    invoke(monkeypatch, capsys, "run", "--case-dir", directory)
    assert before == (directory / "case.sqlite3").read_bytes()


def test_completion_read_refuses_shifted_output_ordinals(tmp_path, monkeypatch, capsys):
    root, _, _, _, case = accepted_parent(tmp_path, monkeypatch, capsys)
    install_transport(monkeypatch, [])
    assert (
        invoke(
            monkeypatch,
            capsys,
            "prepare-pool-completion",
            "--repo-root",
            root,
            "--source-cycle-id",
            case["source_cycle_id"],
        )[0]
        == 0
    )
    directory = (
        root / ".secrets/nexus-historical-three-stock" / f"{case['source_cycle_id']}-completion-v1"
    )
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 0
    with diagnostic.CaseStore.open(directory / "case.sqlite3") as store:
        store.db.execute("UPDATE historical_outputs SET ordinal=ordinal+10")
        store.db.commit()
    assert invoke(monkeypatch, capsys, "read", "--case-dir", directory)[0] == 1
