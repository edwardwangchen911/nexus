"""The approved 0099→0105 edge preserves both historical and successor identities."""

import json
import subprocess
import time
from pathlib import Path

import pytest
from unit.ops import test_managed_exit_reconciliation_control as existing
from unit.ops.test_agent_paper_host_service import _invoke
from unit.ops.test_agent_paper_recovery_control import options, starts

SOURCE_SCHEMA = "0099_research_stages"
TARGET_SCHEMA = "0105_comparison_preclaim"
_ended_host = existing.ended_activated_host


@pytest.fixture(autouse=True)
def historical_engine_version(monkeypatch):
    # This suite proves the old 0099→0105 contract after the current head advances.
    monkeypatch.setattr("nexus.persistence.migrations.CURRENT_MIGRATION_HEAD", TARGET_SCHEMA)
    monkeypatch.setattr(
        "nexus.ops.agent_paper_recovery_activation.CURRENT_MIGRATION_HEAD", TARGET_SCHEMA
    )


def cross_schema_host(tmp_path):
    root, env, state_path, receipt_path = _ended_host(tmp_path, schema=TARGET_SCHEMA)
    state = json.loads(state_path.read_text())
    state["schema"] = SOURCE_SCHEMA
    state_path.write_text(json.dumps(state))
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "-qm",
            "schema upgrade",
        ],
        check=True,
        capture_output=True,
    )
    return root, env, state_path, receipt_path


def test_cross_schema_check_and_publication_keep_old_source(tmp_path):
    root, env, state_path, receipt_path = cross_schema_host(tmp_path)
    frozen = state_path.read_bytes(), receipt_path.read_bytes()
    checked = _invoke(root, env, "reconcile-exit", "--check-only")
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert (state_path.read_bytes(), receipt_path.read_bytes()) == frozen
    published = _invoke(root, env, "reconcile-exit")
    assert published.returncode == 0, published.stdout + published.stderr
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    value = json.loads(proof.read_text())
    assert value["version"] == 2
    assert value["capture"]["producer"]["schema"] == SOURCE_SCHEMA
    assert value["recovery_target"]["schema"] == TARGET_SCHEMA
    assert (
        value["recovery_target"]["revision"]
        == subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    )
    assert json.loads(state_path.read_text())["schema"] == SOURCE_SCHEMA
    assert receipt_path.read_bytes() == frozen[1]
    before = proof.read_bytes()
    again = _invoke(root, env, "reconcile-exit")
    assert again.returncode == 0, again.stdout + again.stderr
    assert json.loads(again.stdout)["status"] == "already_reconciled"
    assert proof.read_bytes() == before and starts(root) == []


@pytest.mark.parametrize("path", ["consume", "child", "archive"])
def test_cross_schema_reuses_complete_lifecycle_assertions(tmp_path, monkeypatch, path):
    monkeypatch.setattr(existing, "ended_activated_host", cross_schema_host)
    if path == "consume":
        existing.test_apply_is_immutable_and_explicit_enable_consumes_once(tmp_path)
    elif path == "child":
        existing.test_child_activation_rechecks_consumed_original_proof(tmp_path, monkeypatch)
    else:
        existing.test_consumed_proof_archives_only_after_confirmed_successor_and_survives_toggle(
            tmp_path
        )


@pytest.mark.parametrize("source", ["0098_independent_monitor", "9999_future", "unknown"])
def test_unapproved_source_never_publishes_or_starts(tmp_path, source):
    root, env, state_path, receipt_path = cross_schema_host(tmp_path)
    state = json.loads(state_path.read_text())
    state["schema"] = source
    state_path.write_text(json.dumps(state))
    before = state_path.read_bytes(), receipt_path.read_bytes()
    result = _invoke(root, env, "reconcile-exit")
    assert result.returncode != 0
    assert "SCHEMA_MISMATCH" in result.stdout
    assert (state_path.read_bytes(), receipt_path.read_bytes()) == before
    assert not list(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    assert starts(root) == []


def test_supported_edge_is_in_the_actual_migration_lineage():
    from alembic.script import ScriptDirectory

    scripts = ScriptDirectory(str(Path(__file__).resolve().parents[4] / "engine/alembic"))
    current = scripts.get_revision(TARGET_SCHEMA)
    while current.revision != SOURCE_SCHEMA:
        assert isinstance(current.down_revision, str)
        current = scripts.get_revision(current.down_revision)


def test_cross_schema_retirement_preserves_exit_then_enables_once(tmp_path):
    root, env, state_path, receipt_path = cross_schema_host(tmp_path)
    original = json.loads(state_path.read_text())
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    frozen = proof.read_bytes(), receipt_path.read_bytes()
    stopped = _invoke(root, env, "stop")
    assert stopped.returncode == 0, stopped.stdout
    after = json.loads(state_path.read_text())
    assert after["exit_reconciliation"]["registration_retired"] is True
    assert after.get("stop_proof") == original.get("stop_proof")
    assert after.get("stopped_confirmed") == original.get("stopped_confirmed")
    options(root, kickstart_state="inactive", exit=1)
    enabled = _invoke(root, env, "enable-recovery")
    assert enabled.returncode == 0, enabled.stdout
    after = json.loads(state_path.read_text())
    assert after["schema"] == TARGET_SCHEMA
    assert after["exit_reconciliation"]["consumed_by"] == after["attempt"]["id"]
    assert len(starts(root)) == 1
    assert (proof.read_bytes(), receipt_path.read_bytes()) == frozen


def test_rate_limited_target_binding_survives_maintenance_and_exact_readback(tmp_path):
    root, env, state_path, receipt_path = cross_schema_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    frozen = proof.read_bytes()
    state = json.loads(state_path.read_text())
    state["next_start_at"] = time.time() + 600
    state_path.write_text(json.dumps(state))
    assert _invoke(root, env, "enable-recovery").returncode == 0
    assert _invoke(root, env, "maintenance").returncode == 0
    before = state_path.read_bytes()
    inspected = _invoke(root, env, "reconcile-exit", "--check-only")
    assert inspected.returncode == 0, inspected.stdout
    assert state_path.read_bytes() == before
    assert _invoke(root, env, "enable-recovery").returncode == 0
    assert json.loads(state_path.read_text())["next_start_at"] == state["next_start_at"]
    assert starts(root) == [] and proof.read_bytes() == frozen


@pytest.mark.parametrize("fault", ["code", "receipt", "pending", "unknown", "version", "target"])
def test_cross_schema_late_change_cannot_authorize_start(tmp_path, fault):
    root, env, state_path, receipt_path = cross_schema_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    if fault == "code":
        subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "--allow-empty",
                "-qm",
                "unvalidated new target",
            ],
            check=True,
            capture_output=True,
        )
    elif fault == "receipt":
        value = json.loads(receipt_path.read_text())
        value["changed"] = True
        receipt_path.write_text(json.dumps(value))
    elif fault == "pending":
        receipt_path.with_suffix(".write-pending").touch(mode=0o600)
    elif fault == "unknown":
        options(root, recovery_status="blocked", blocking_codes=["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"])
    else:
        # Even matching a damaged file's digest cannot introduce a new policy.
        import hashlib

        value = json.loads(proof.read_text())
        if fault == "version":
            value["version"] = 999
        else:
            value["recovery_target"]["schema"] = "9999_future"
        proof.write_text(json.dumps(value))
        state = json.loads(state_path.read_text())
        state["exit_reconciliation"]["evidence_sha256"] = hashlib.sha256(
            proof.read_bytes()
        ).hexdigest()
        state_path.write_text(json.dumps(state))
    before = state_path.read_bytes(), proof.read_bytes(), receipt_path.read_bytes()
    result = _invoke(root, env, "enable-recovery")
    assert result.returncode != 0
    assert (state_path.read_bytes(), proof.read_bytes(), receipt_path.read_bytes()) == before
    assert starts(root) == []


@pytest.mark.parametrize("case", ["orphan", "revoked", "concurrent"])
def test_cross_schema_interruption_and_competing_consumers(tmp_path, monkeypatch, case):
    monkeypatch.setattr(existing, "ended_activated_host", cross_schema_host)
    if case == "orphan":
        existing.test_orphan_proof_requires_revalidation_before_pointer_link(tmp_path, monkeypatch)
    elif case == "revoked":
        existing.test_consumed_proof_archives_with_its_confirmed_pending_end(tmp_path, "revoked")
    else:
        from concurrent.futures import ThreadPoolExecutor

        root, env, state_path, receipt_path = cross_schema_host(tmp_path)
        assert _invoke(root, env, "reconcile-exit").returncode == 0
        proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
        frozen = proof.read_bytes(), receipt_path.read_bytes()
        options(root, kickstart_state="inactive", exit=1)
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(lambda _: _invoke(root, env, "enable-recovery"), range(4)))
        assert sum(result.returncode == 0 for result in results) == 1
        for result in results:
            if result.returncode != 0:
                assert json.loads(result.stdout)["error_code"] in {
                    "AGENT_PAPER_SERVICE_CONTROL_BUSY",
                    "AGENT_PAPER_RECOVERY_START_UNCONFIRMED",
                }
        after = json.loads(state_path.read_text())
        pending = after["attempt"]
        assert after["exit_reconciliation"]["consumed_by"] == pending["id"]
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(lambda _: _invoke(root, env, "recover"), range(4)))
        # The existing control lock deliberately rejects competing commands;
        # it does not queue them or permit another launch.
        assert any(result.returncode == 0 for result in results)
        for result in results:
            if result.returncode != 0:
                assert json.loads(result.stdout)["error_code"] == "AGENT_PAPER_SERVICE_CONTROL_BUSY"
        assert json.loads(state_path.read_text())["attempt"] == pending
        assert len(starts(root)) == 1
        assert (proof.read_bytes(), receipt_path.read_bytes()) == frozen


def local_manager(root, env, monkeypatch):
    import importlib.util

    for name in ("HOME", "PATH"):
        monkeypatch.setenv(name, env[name])
    spec = importlib.util.spec_from_file_location(
        "cross_schema_service", root / "scripts/macos/agent-paper-loop-service.py"
    )
    service = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(service)
    return service, service._recovery_module()


def test_malformed_v2_sections_report_only_fixed_safe_error(tmp_path, monkeypatch):
    from copy import deepcopy

    root, env, state_path, receipt_path = cross_schema_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    original = json.loads(proof.read_text())
    frozen = proof.read_bytes(), state_path.read_bytes(), receipt_path.read_bytes()
    service, _ = local_manager(root, env, monkeypatch)
    for field in ("capture", "producer", "observed_readiness"):
        malformed = deepcopy(original)
        container = malformed["capture"] if field == "producer" else malformed
        container[field] = None
        with pytest.raises(ValueError, match="^AGENT_PAPER_EXIT_RECONCILIATION_PROOF_INVALID$"):
            service._reconciliation_module()._target(malformed)
    assert (proof.read_bytes(), state_path.read_bytes(), receipt_path.read_bytes()) == frozen
    assert starts(root) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["child", "archive", "generation"])
async def test_broken_consumption_never_becomes_an_ordinary_attempt(tmp_path, monkeypatch, path):
    from copy import deepcopy

    from nexus.ops.agent_paper_loop import acquire_agent_paper_lock
    from nexus.ops.agent_paper_recovery_activation import RecoveryActivation
    from nexus.settings import EngineSettings

    root, env, state_path, receipt_path = cross_schema_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    options(root, kickstart_state="inactive", exit=1)
    assert _invoke(root, env, "enable-recovery").returncode == 0
    state = json.loads(state_path.read_text())
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    if path == "generation":
        state["exit_reconciliation"]["enable_generation"] = "private-invalid-generation"
    else:
        del state["attempt"]["exit_reconciliation_sha256"]
        for attempt in state["attempts"]:
            if attempt["id"] == state["attempt"]["id"]:
                del attempt["exit_reconciliation_sha256"]
    state_path.write_text(json.dumps(state))
    frozen = state_path.read_bytes(), proof.read_bytes(), receipt_path.read_bytes()
    before = deepcopy(state)
    service, _ = local_manager(root, env, monkeypatch)
    helper = service._reconciliation_module()
    if path == "child":
        from nexus.ops import agent_paper_recovery as readiness

        async def ready(_settings):
            return {"status": "ready"}

        monkeypatch.setattr(readiness, "inspect_recovery_readiness", ready)
        monkeypatch.setattr(readiness, "require_recovery_process_quiescence", lambda: None)
        config = EngineSettings(release_revision=state["revision"])
        monkeypatch.setattr(
            "nexus.ops.agent_paper_recovery_activation._boot_id", lambda: state["boot"]
        )
        guard = RecoveryActivation(
            config,
            repo_root=root,
            env_file=root / "infra/.env.test",
            generation=state["generation"],
            attempt_id=state["attempt"]["id"],
        )
        with acquire_agent_paper_lock(runtime_root=tmp_path / "lifetime"):
            with pytest.raises(ValueError, match="RECONCILIATION|RECOVERY"):
                await guard(config)
        assert not guard.receipt_path.exists()
    elif path == "archive":
        with pytest.raises(ValueError, match="RECONCILIATION"):
            helper.archive_consumed(
                root,
                state,
                state["attempt"],
                reason="REVOKED_BEFORE_ACTIVATION",
                observed_at=time.time(),
            )
    else:
        with pytest.raises(ValueError, match="^AGENT_PAPER_EXIT_RECONCILIATION_BINDING_CHANGED$"):
            helper._validate_original(root, state, state["exit_reconciliation"])
    assert state == before
    assert (state_path.read_bytes(), proof.read_bytes(), receipt_path.read_bytes()) == frozen
    assert len(starts(root)) == 1


@pytest.mark.parametrize("stage", ["installed", "ready_to_dispatch"])
@pytest.mark.parametrize("fault", ["maintenance", "receipt", "pending"])
def test_cross_schema_rereads_after_external_boundaries(tmp_path, monkeypatch, stage, fault):
    root, env, state_path, receipt_path = cross_schema_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    proof = next(receipt_path.parent.glob("recovery-reconciliation-*.json"))
    frozen = proof.read_bytes()
    service, recovery = local_manager(root, env, monkeypatch)

    def change():
        if fault == "maintenance":
            state = json.loads(state_path.read_text())
            state["desired"] = "maintenance"
            state_path.write_text(json.dumps(state))
        elif fault == "receipt":
            receipt = json.loads(receipt_path.read_text())
            receipt["late_change"] = True
            receipt_path.write_text(json.dumps(receipt))
        else:
            receipt_path.with_suffix(".write-pending").touch(mode=0o600)

    if stage == "installed":
        original = recovery.install

        def install(*args):
            original(*args)
            change()

        monkeypatch.setattr(recovery, "install", install)
    else:
        original = recovery.engine_check
        checked = []

        def inspect(*args):
            result = original(*args)
            if args[-1] == "--service-recovery-check":
                checked.append(True)
                if len(checked) == 2:
                    change()
            return result

        monkeypatch.setattr(recovery, "engine_check", inspect)
    with pytest.raises(ValueError, match="RECONCILIATION|RECOVERY"):
        recovery.enable(service, root, root / "infra/.env.test", "nexus-test", report=False)
    after = json.loads(state_path.read_text())
    assert after.get("attempt") is None
    assert after["exit_reconciliation"].get("consumed_by") is None
    if fault == "maintenance":
        assert after["desired"] == "maintenance"
    assert starts(root) == [] and proof.read_bytes() == frozen


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["none", "late_unknown", "maintenance", "receipt", "code"])
async def test_cross_schema_child_under_real_lifetime_lock(tmp_path, monkeypatch, change):
    from uuid import UUID

    from nexus.ops import agent_paper_recovery as readiness
    from nexus.ops.agent_paper_loop import acquire_agent_paper_lock
    from nexus.ops.agent_paper_recovery_activation import RecoveryActivation
    from nexus.settings import EngineSettings

    root, env, state_path, receipt_path = cross_schema_host(tmp_path)
    assert _invoke(root, env, "reconcile-exit").returncode == 0
    options(root, kickstart_state="inactive", exit=1)
    assert _invoke(root, env, "enable-recovery").returncode == 0
    state = json.loads(state_path.read_text())
    local_manager(root, env, monkeypatch)
    monkeypatch.setattr("nexus.ops.agent_paper_recovery_activation._boot_id", lambda: state["boot"])
    monkeypatch.setattr(readiness, "require_recovery_process_quiescence", lambda: None)

    async def inspect(_settings):
        if change == "maintenance":
            changed = json.loads(state_path.read_text())
            changed["desired"] = "maintenance"
            state_path.write_text(json.dumps(changed))
        elif change == "receipt":
            changed = json.loads(receipt_path.read_text())
            changed["late_change"] = True
            receipt_path.write_text(json.dumps(changed))
        elif change == "code":
            subprocess.run(
                [
                    "git",
                    "-C",
                    str(root),
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.invalid",
                    "commit",
                    "--allow-empty",
                    "-qm",
                    "changed after child lifetime lock",
                ],
                check=True,
                capture_output=True,
            )
        return {"status": "blocked" if change == "late_unknown" else "ready"}

    monkeypatch.setattr(readiness, "inspect_recovery_readiness", inspect)
    config = EngineSettings(
        agent_runtime_mode="codex_sdk",
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=UUID(int=1),
        agent_paper_candidate_batch_id=UUID(int=2),
        agent_paper_formal_model_config_id=UUID(int=3),
        release_revision=state["revision"],
    )
    guard = RecoveryActivation(
        config,
        repo_root=root,
        env_file=root / "infra/.env.test",
        generation=state["generation"],
        attempt_id=state["attempt"]["id"],
    )
    runtime = tmp_path / "lifetime"
    with acquire_agent_paper_lock(runtime_root=runtime):
        with pytest.raises(RuntimeError, match="AGENT_PAPER"):
            with acquire_agent_paper_lock(runtime_root=runtime):
                pytest.fail("second lifetime owner entered")
        if change == "none":
            await guard(config)
            assert json.loads(guard.receipt_path.read_text())["phase"] == "activated"
            with pytest.raises(ValueError, match="RECOVERY"):
                await guard(config)
        else:
            with pytest.raises(ValueError, match="RECOVERY"):
                await guard(config)
            assert json.loads(guard.receipt_path.read_text())["phase"] != "activated"
    after = json.loads(state_path.read_text())
    assert after["attempt"] == state["attempt"]
    assert after["exit_reconciliation"]["consumed_by"] == state["attempt"]["id"]
    assert len(starts(root)) == 1
