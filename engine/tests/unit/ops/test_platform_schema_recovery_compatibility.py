"""Exact 0105→0106 recovery, with old proofs retained and no transitive permission."""

import importlib.util
import json
import subprocess
import sys

import pytest
from unit.ops import test_cross_schema_exit_reconciliation as cross
from unit.ops import test_managed_exit_reconciliation_control as managed
from unit.ops import test_pending_consumer_exit_reconciliation_control as pending
from unit.ops.test_agent_paper_host_service import ROOT, _invoke, _register
from unit.ops.test_agent_paper_recovery_control import (
    add_activation,
    options,
    recovery_root,
    register_pid,
    starts,
)

SOURCE = "0105_comparison_preclaim"
TARGET = "0106_platform_qualification"


@pytest.mark.parametrize(
    "version,source,target,policy,valid",
    [
        (2, "0099_research_stages", SOURCE, "research_stages_0099_to_0105_v1", True),
        (2, SOURCE, TARGET, "comparison_preclaim_0105_to_0106_v1", True),
        (3, SOURCE, SOURCE, "pending_consumer_exit_0105_v1", True),
        (3, TARGET, TARGET, "pending_consumer_exit_0106_v1", True),
        (2, "0099_research_stages", TARGET, "comparison_preclaim_0105_to_0106_v1", False),
        (2, SOURCE, TARGET, "research_stages_0099_to_0105_v1", False),
        (3, SOURCE, TARGET, "pending_consumer_exit_0106_v1", False),
        (3, TARGET, TARGET, "pending_consumer_exit_0105_v1", False),
        (3, {}, TARGET, "pending_consumer_exit_0106_v1", False),
        (
            3,
            "0107_platform_price_renewal",
            "0107_platform_price_renewal",
            "pending_consumer_exit_0107_v1",
            True,
        ),
        (3, TARGET, "0107_platform_price_renewal", "pending_consumer_exit_0107_v1", False),
        (2, TARGET, "0107_platform_price_renewal", "comparison_preclaim_0105_to_0106_v1", False),
    ],
)
def test_frozen_proof_policy_decoding_is_exact(version, source, target, policy, valid):
    spec = importlib.util.spec_from_file_location(
        "exact_schema_decoder", ROOT / "scripts/macos/agent-paper-reconciliation.py"
    )
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    evidence = {
        "version": version,
        "kind": "pending_consumer_exit",
        "parent_health_observed": False,
        "historical_effects": "not_determined",
        "business_disposition": "unchanged",
        "execution_locked": True,
        "replay_authorized": False,
        "capture": {
            "producer": {"schema": source, "pending_attempt": {}},
            "reconciliation_revision": "a" * 40,
        },
        "recovery_target": {"schema": target, "revision": "a" * 40, "policy": policy},
        "observed_readiness": {
            "status": "ready",
            "migration_head": target,
            "blocking_codes": [],
            "execution_locked": True,
            "external_requests": 0,
        },
    }
    if valid:
        assert helper._target(evidence) == evidence["recovery_target"]
    else:
        with pytest.raises(ValueError, match="AGENT_PAPER_EXIT_RECONCILIATION_PROOF_INVALID"):
            helper._target(evidence)


@pytest.fixture
def engine106(monkeypatch):
    monkeypatch.setattr("nexus.persistence.migrations.CURRENT_MIGRATION_HEAD", TARGET)
    monkeypatch.setattr("nexus.ops.agent_paper_recovery_activation.CURRENT_MIGRATION_HEAD", TARGET)
    monkeypatch.setattr(cross, "SOURCE_SCHEMA", SOURCE)
    monkeypatch.setattr(cross, "TARGET_SCHEMA", TARGET)


def install_schema(root, before, after):
    engine = root / ".venv/bin/nexus-engine-agent-paper-loop"
    source = engine.read_text()
    assert repr(before) in source
    engine.write_text(source.replace(repr(before), repr(after)))
    pending._upgrade(root)


def test_exact_0105_to_0106_publication_and_immutable_readback(tmp_path, engine106):
    cross.test_cross_schema_check_and_publication_keep_old_source(tmp_path)
    root = tmp_path / "repo"
    proof = next(root.rglob("recovery-reconciliation-*.json"))
    assert (
        json.loads(proof.read_text())["recovery_target"]["policy"]
        == "comparison_preclaim_0105_to_0106_v1"
    )


@pytest.mark.parametrize("path", ["consume", "child", "archive"])
def test_new_edge_reuses_original_lifecycle_assertions(tmp_path, monkeypatch, engine106, path):
    cross.test_cross_schema_reuses_complete_lifecycle_assertions(tmp_path, monkeypatch, path)


@pytest.mark.parametrize("case", ["orphan", "revoked", "concurrent"])
def test_new_edge_reuses_interruption_and_concurrency(tmp_path, monkeypatch, engine106, case):
    cross.test_cross_schema_interruption_and_competing_consumers(tmp_path, monkeypatch, case)


@pytest.mark.parametrize(
    "source,target",
    [
        ("0099_research_stages", TARGET),
        (TARGET, SOURCE),
        (SOURCE, "0107_unapproved"),
    ],
)
def test_no_transitive_reverse_or_future_edge(tmp_path, source, target):
    root, env, state_path, receipt = managed.ended_activated_host(tmp_path, schema=target)
    state = json.loads(state_path.read_text())
    state["schema"] = source
    state_path.write_text(json.dumps(state))
    frozen = state_path.read_bytes(), receipt.read_bytes()
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode != 0 and "SCHEMA_MISMATCH" in result.stdout
    assert (state_path.read_bytes(), receipt.read_bytes()) == frozen
    assert not list(receipt.parent.glob("recovery-reconciliation-*.json"))
    assert starts(root) == []


@pytest.mark.parametrize("cross_schema", [False, True])
def test_0106_pending_full_closure_and_next_healthy_identity(
    tmp_path, monkeypatch, engine106, cross_schema
):
    original = pending._ended_pending_consumer
    monkeypatch.setattr(
        pending,
        "_ended_pending_consumer",
        lambda path, **kw: original(path, source_schema=SOURCE, target_schema=TARGET, **kw),
    )
    pending.test_pending_consumer_closes_once_and_next_enable_consumes_its_proof(
        tmp_path, cross_schema
    )
    proofs = [
        json.loads(path.read_text()) for path in tmp_path.rglob("recovery-reconciliation-*.json")
    ]
    v3 = [proof for proof in proofs if proof["version"] == 3]
    assert len(v3) == 1
    assert v3[0]["recovery_target"]["schema"] == TARGET
    assert v3[0]["recovery_target"]["policy"] == "pending_consumer_exit_0106_v1"


def test_unarchived_0105_pending_cannot_migrate_to_0106(tmp_path):
    root, env, state_path, old_receipt, prior, receipt = pending._ended_pending_consumer(
        tmp_path, target_schema=SOURCE
    )
    install_schema(root, SOURCE, TARGET)
    frozen = tuple(path.read_bytes() for path in (state_path, old_receipt, prior, receipt))
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode != 0 and "SCHEMA_MISMATCH" in result.stdout
    assert tuple(path.read_bytes() for path in (state_path, old_receipt, prior, receipt)) == frozen
    assert len(starts(root)) == 1


def test_archived_0105_q_reads_under_0106_without_reopening(tmp_path, monkeypatch):
    original = pending._ended_pending_consumer
    captured = []

    def old_fixture(path, **kw):
        result = original(path, target_schema=SOURCE, **kw)
        captured.append(result)
        return result

    monkeypatch.setattr(pending, "_ended_pending_consumer", old_fixture)
    # Existing assertions prove Q archived only after C activation + parent health,
    # then C's real exit is classified using C, never the old A/B identity.
    pending.test_pending_consumer_closes_once_and_next_enable_consumes_its_proof(tmp_path, False)
    root, env, state_path, *_ = captured[0]
    original_state = json.loads(state_path.read_text())
    assert original_state["attempt"] is None
    assert (
        original_state["exit_reconciliation"]["attempt_id"]
        == original_state["active_attempt"]["id"]
    )
    assert all(
        item["attempt_id"] != original_state["active_attempt"]["id"]
        for item in original_state["exit_reconciliation_history"]
    )
    originals = {path: path.read_bytes() for path in state_path.parent.glob("recovery*.json")}
    install_schema(root, SOURCE, TARGET)
    service, recovery = cross.local_manager(root, env, monkeypatch)
    assert recovery.read(root) == original_state
    for proof_path in state_path.parent.glob("recovery-reconciliation-*.json"):
        proof = json.loads(proof_path.read_text())
        target = service._reconciliation_module()._target(proof)
        if target is not None:
            assert target["schema"] == SOURCE
    refused = _invoke(root, env, "enable-recovery")
    assert refused.returncode != 0 and "BUSINESS_BLOCKED" in refused.stdout
    assert all(path.read_bytes() == raw for path, raw in originals.items())
    assert len(starts(root)) == 2


def test_confirmed_0105_stop_allows_one_explicit_0106_start(tmp_path):
    root, env = recovery_root(tmp_path, schema=SOURCE)
    child = subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.read(); sys.exit(0)"],
        stdin=subprocess.PIPE,
        text=True,
    )
    try:
        register_pid(root, child.pid)
        assert _invoke(root, env, "enable-recovery").returncode == 0
        state_path, receipt = add_activation(root)
        child.communicate(input="", timeout=10)
        assert child.returncode == 0
    finally:
        if child.poll() is None:
            child.terminate()
        child.wait(timeout=10)
    _register(root, state="inactive", exit_code=0)
    before = receipt.read_bytes()
    assert _invoke(root, env, "stop").returncode == 0
    stopped = json.loads(state_path.read_text())
    assert stopped["stopped_confirmed"] is True and stopped["schema"] == SOURCE
    install_schema(root, SOURCE, TARGET)
    options(root, kickstart_state="inactive", kickstart_exit=1, exit=0)
    # Normal enable admits one pending attempt; a launcher alone is not HEALTHY.
    result = _invoke(root, env, "enable-recovery")
    assert result.returncode == 0, result.stdout + result.stderr
    after = json.loads(state_path.read_text())
    assert after["schema"] == TARGET and after["attempt"] is not None
    assert after["attempts"][:-1] == stopped["attempts"]
    assert receipt.read_bytes() == before and len(starts(root)) == 1


def test_confirmed_0106_stop_allows_one_explicit_0107_start(tmp_path, monkeypatch):
    monkeypatch.setitem(globals(), "SOURCE", "0106_platform_qualification")
    monkeypatch.setitem(globals(), "TARGET", "0107_platform_price_renewal")
    test_confirmed_0105_stop_allows_one_explicit_0106_start(tmp_path)


@pytest.mark.parametrize("fault", ["receipt", "target"])
def test_new_edge_rejects_late_identity_or_target_change(tmp_path, engine106, fault):
    cross.test_cross_schema_late_change_cannot_authorize_start(tmp_path, fault)


@pytest.mark.parametrize("committed", [False, True])
def test_0106_pending_interrupted_commit_preserves_one_proof(
    tmp_path, monkeypatch, engine106, committed
):
    original = pending._ended_pending_consumer
    monkeypatch.setattr(
        pending,
        "_ended_pending_consumer",
        lambda path, **kw: original(path, target_schema=TARGET, **kw),
    )
    pending.test_pending_close_interrupted_management_requires_same_proof_readback(
        tmp_path, committed
    )
