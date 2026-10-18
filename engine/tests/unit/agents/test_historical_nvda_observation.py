import json
import sys

import pytest
from nexus.ops import historical_pool_diagnostic as diagnostic
from unit.agents.test_historical_pool_diagnostic import historical_case, install_transport


def source_case(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    secrets = root / ".secrets"
    secrets.mkdir(mode=0o700)
    parent = secrets / "nexus-historical-three-stock"
    parent.mkdir(mode=0o700)
    case = historical_case()
    case["members"][1]["symbol"] = "NVDA"
    directory = parent / f"{case['source_cycle_id']}-v11"
    directory.mkdir(mode=0o700)
    path = directory / "case.sqlite3"
    with diagnostic.CaseStore.create(path, case) as store:
        store.claim()
        store.stage("research_2")
        store.diagnostic({"code": "HISTORICAL_RESEARCH_NOT_ACTIONABLE", "usage_complete": False})
        store.finish("unknown")
    return root, path, case


def install_config_check(monkeypatch):
    from nexus.ops import historical_nvda_observation as observation

    checked = []

    async def check(root, case):
        checked.append(case["model_hash"])

    monkeypatch.setattr(observation, "check_current_configuration", check)
    return checked


def invoke(monkeypatch, capsys, *args):
    monkeypatch.setattr(sys, "argv", ["historical", *map(str, args)])
    code = diagnostic.main()
    return code, json.loads(capsys.readouterr().out)


@pytest.mark.parametrize("rejection", [None, "unresolved"])
def test_normal_cli_independent_nvda_observation_and_no_retry(
    tmp_path, monkeypatch, capsys, rejection
):
    root, source, case = source_case(tmp_path)
    original = source.read_bytes()
    checked = install_config_check(monkeypatch)
    calls = []
    install_transport(monkeypatch, calls, rejection=rejection)
    code, prepared = invoke(
        monkeypatch,
        capsys,
        "prepare-nvda-observation",
        "--repo-root",
        root,
        "--source-cycle-id",
        case["source_cycle_id"],
    )
    assert code == 0
    assert prepared["profile"] == "historical-nvda-observation-v1"
    assert prepared["status"] == "prepared"
    assert prepared["observation_id"]
    assert calls == []
    directory = (
        root / ".secrets/nexus-historical-nvda-observations" / f"{case['source_cycle_id']}-v1"
    )
    code, result = invoke(monkeypatch, capsys, "run", "--case-dir", directory)
    assert code == (0 if rejection is None else 1)
    assert calls == ["research", "critique", "synthesis"]
    assert len(checked) == 2
    assert result["output_count"] == (1 if rejection is None else 0)
    assert result["stage"] == "research_2"
    assert result["formal_acceptance"] is False
    assert result["business_writes"] == 0
    assert result["execution_locked"] is True
    if rejection:
        assert "UNRESOLVED_REVIEW" in result["diagnostic"]["research_rejection"]["reason_codes"]
    else:
        with diagnostic.CaseStore.open(directory / "case.sqlite3") as store:
            ordinal, raw = store.db.execute(
                "select ordinal,payload from historical_outputs"
            ).fetchone()
            assert ordinal == 1
            assert (
                json.loads(raw)["intent"]["instrument_id"]
                == case["members"][1]["request"]["instrument_id"]
            )
    assert source.read_bytes() == original
    code, reread = invoke(monkeypatch, capsys, "read", "--case-dir", directory)
    assert code == (0 if rejection is None else 1)
    assert reread == {k: v for k, v in result.items() if k != "invocation_status"}
    before = (directory / "case.sqlite3").read_bytes()
    assert invoke(monkeypatch, capsys, "run", "--case-dir", directory)[0] == 1
    assert (directory / "case.sqlite3").read_bytes() == before
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
        == 1
    )
    assert calls == ["research", "critique", "synthesis"]
    assert source.read_bytes() == original


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["source_changed", "other_member_changed", "copied_directory", "timeout", "cancel"]
)
async def test_independent_observation_fail_closed(tmp_path, monkeypatch, failure):
    import asyncio
    import shutil
    from uuid import UUID

    from nexus.ops.historical_nvda_observation import prepare_nvda_observation

    root, source, case = source_case(tmp_path)
    install_config_check(monkeypatch)
    path = await prepare_nvda_observation(root, UUID(case["source_cycle_id"]))
    calls = []
    install_transport(monkeypatch, calls, fail_call=1 if failure == "timeout" else None)
    if failure == "source_changed":
        with diagnostic.CaseStore.open(source) as old:
            old.diagnostic({"code": "changed"})
    elif failure == "other_member_changed":
        with diagnostic.CaseStore.open(path) as store:
            payload = store.case()
            payload["members"][2]["inputs"] = {"changed": True}
            store.db.execute(
                "update historical_case set payload=?,digest=?",
                (diagnostic._wire(payload), diagnostic._hash(payload)),
            )
            store.db.commit()
    elif failure == "copied_directory":
        destination = path.parent.parent / "different"
        destination.mkdir(mode=0o700)
        shutil.copy2(path, destination / path.name)
        path = destination / path.name
    elif failure == "cancel":

        async def cancel(*args):
            raise asyncio.CancelledError

        monkeypatch.setattr(diagnostic.FrozenCommittee, "analyze", cancel)
    with diagnostic.CaseStore.open(path) as store:
        with pytest.raises((ValueError, TimeoutError, asyncio.CancelledError)):
            await diagnostic.execute_case(store)
        assert store.db.execute("select status from historical_case").fetchone()[0] == "unknown"
        assert store.db.execute("select count(*) from historical_outputs").fetchone()[0] == 0
        with pytest.raises(ValueError, match="ALREADY_ATTEMPTED"):
            await diagnostic.execute_case(store)
    assert len(calls) == (1 if failure == "timeout" else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", ["active_source", "missing_target", "ambiguous_target", "other_member_overflow"]
)
async def test_prepare_rejects_invalid_source_or_entire_pool_before_calls(
    tmp_path, monkeypatch, fault
):
    from uuid import UUID

    from nexus.ops.historical_nvda_observation import prepare_nvda_observation

    root, source, case = source_case(tmp_path)
    install_config_check(monkeypatch)
    calls = []
    install_transport(monkeypatch, calls)
    with diagnostic.CaseStore.open(source) as store:
        if fault == "active_source":
            store.db.execute("update historical_case set finished_at=NULL")
        else:
            if fault == "missing_target":
                case["members"][1]["symbol"] = "TEST1"
            elif fault == "ambiguous_target":
                case["members"][2]["symbol"] = "NVDA"
            else:
                case["members"][2]["inputs"] = {
                    "financial_facts": {"items": [{"text": "x" * 150000}]}
                }
            store.db.execute(
                "update historical_case set payload=?,digest=?",
                (diagnostic._wire(case), diagnostic._hash(case)),
            )
        store.db.commit()
    before = source.read_bytes()
    with pytest.raises(ValueError):
        await prepare_nvda_observation(root, UUID(case["source_cycle_id"]))
    assert calls == []
    assert source.read_bytes() == before
    assert not (root / ".secrets/nexus-historical-nvda-observations").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", [None, "account", "model", "hash", "version", "budget", "pool_budget", "clock"]
)
async def test_current_configuration_and_budget_check_is_read_only(tmp_path, monkeypatch, fault):
    from datetime import UTC, datetime
    from decimal import Decimal
    from types import SimpleNamespace
    from uuid import UUID

    from nexus.ops import historical_nvda_observation as observation

    case = historical_case()
    events = []
    settings = SimpleNamespace(
        agent_paper_account_id=UUID(int=20),
        agent_paper_formal_model_config_id=UUID(int=26),
        agent_daily_budget_usd=Decimal(2),
    )
    model = SimpleNamespace(
        version="codex-sdk-chatgpt-v11-bounded-stage-budget",
        config_hash=case["model_hash"],
        model_id=case["model_id"],
        worst_case_cost_usd=Decimal(1),
    )
    if fault == "account":
        settings.agent_paper_account_id = UUID(int=99)
    if fault in {"model", "hash", "version"}:
        setattr(
            model,
            {"model": "model_id", "hash": "config_hash", "version": "version"}[fault],
            "changed",
        )

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        def begin(self):
            return self

        async def execute(self, statement):
            events.append(str(statement))

        async def scalar(self, statement):
            assert "clock_timestamp" in str(statement)
            return None if fault == "clock" else datetime(2026, 10, 18, tzinfo=UTC)

    async def profile(session, **kwargs):
        assert events == ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"]
        return model

    async def used(session, *, business_date):
        assert business_date.isoformat() == "2026-10-18"
        return Decimal(2 if fault == "budget" else 1)

    async def dispose():
        events.append("disposed")

    class Factory:
        kw = {"bind": SimpleNamespace(dispose=dispose)}

        def __call__(self):
            return Session()

    monkeypatch.setattr(observation, "local_settings", lambda *a, **k: settings)
    monkeypatch.setattr(observation, "session_factory", lambda *a: Factory())
    monkeypatch.setattr(observation, "require_formal_stage_model_profile", profile)
    monkeypatch.setattr(observation, "_shared_daily_budget_used", used)
    if fault:
        with pytest.raises(ValueError, match="HISTORICAL_CASE_"):
            await observation.check_current_configuration(
                tmp_path, case, budget_units=3 if fault == "pool_budget" else 1
            )
    else:
        await observation.check_current_configuration(tmp_path, case)
    assert events == ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", "disposed"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "instrument",
        "snapshot",
        "decision_time",
        "usage",
        "ordinal",
        "source_hash",
        "other_member",
        "limits",
    ],
)
async def test_single_result_readback_binds_frozen_target_and_limits(
    tmp_path, monkeypatch, mutation
):
    from uuid import UUID

    from nexus.ops.historical_nvda_observation import prepare_nvda_observation

    root, _, case = source_case(tmp_path)
    install_config_check(monkeypatch)
    calls = []
    install_transport(monkeypatch, calls)
    path = await prepare_nvda_observation(root, UUID(case["source_cycle_id"]))
    with diagnostic.CaseStore.open(path) as store:
        await diagnostic.execute_case(store)
        raw = store.db.execute("select payload from historical_outputs").fetchone()[0]
        result = json.loads(raw)
        if mutation == "instrument":
            result["intent"]["instrument_id"] = case["members"][0]["request"]["instrument_id"]
        elif mutation == "snapshot":
            result["intent"]["data_snapshot_id"] = str(UUID(int=123))
        elif mutation == "decision_time":
            result["intent"]["decision_time"] = "2026-10-17T00:00:00Z"
        elif mutation == "usage":
            result["usage"]["provider_calls"] = 4
        elif mutation == "ordinal":
            store.db.execute("update historical_outputs set ordinal=0")
        else:
            changed = store.case()
            if mutation == "source_hash":
                changed["observation"]["source_case_hash"] = "0" * 64
            elif mutation == "other_member":
                changed["members"][0]["symbol"] = "CHANGED"
            else:
                changed["observation"]["limits"]["research_calls"] = 9
            store.db.execute(
                "update historical_case set payload=?,digest=?",
                (diagnostic._wire(changed), diagnostic._hash(changed)),
            )
        store.db.execute(
            "update historical_outputs set payload=?,digest=?",
            (diagnostic._wire(result), diagnostic._hash(result)),
        )
        store.db.commit()
        with pytest.raises(ValueError):
            store.summary()
