"""The exact 0134 -> 0135 ordinary exit retains all reviewed identity gates."""

import pytest
from unit.ops import test_cross_schema_exit_reconciliation as cross
from unit.ops import test_durable_cross_schema_recovery_control as durable
from unit.ops import test_managed_exit_reconciliation_control as lifecycle

SOURCE = "0134_sec_foundation"
TARGET = "0135_routine_budget"


@pytest.fixture(autouse=True)
def versions(monkeypatch):
    monkeypatch.setattr(cross, "_ended_host", durable._scoped_host)
    monkeypatch.setattr(cross, "SOURCE_SCHEMA", SOURCE)
    monkeypatch.setattr(cross, "TARGET_SCHEMA", TARGET)
    monkeypatch.setattr("nexus.persistence.migrations.CURRENT_MIGRATION_HEAD", TARGET)
    monkeypatch.setattr("nexus.ops.agent_paper_recovery_activation.CURRENT_MIGRATION_HEAD", TARGET)
    monkeypatch.setattr(lifecycle, "ended_activated_host", cross.cross_schema_host)


def test_original_proof_publication(tmp_path):
    cross.test_cross_schema_check_and_publication_keep_old_source(tmp_path)


@pytest.mark.parametrize("path", ("consume", "child", "archive"))
def test_exact_once_lifecycle(tmp_path, monkeypatch, path):
    cross.test_cross_schema_reuses_complete_lifecycle_assertions(tmp_path, monkeypatch, path)


@pytest.mark.parametrize("fault", ("code", "receipt", "pending", "unknown", "version", "target"))
def test_drift_never_consumes(tmp_path, fault):
    cross.test_cross_schema_late_change_cannot_authorize_start(tmp_path, fault)


@pytest.mark.parametrize(
    "source,target",
    (("0125_sec_positive_chain", TARGET), (TARGET, SOURCE), (SOURCE, "9999_future")),
)
def test_no_transitivity_or_reverse(tmp_path, monkeypatch, source, target):
    monkeypatch.setattr(cross, "SOURCE_SCHEMA", source)
    monkeypatch.setattr(cross, "TARGET_SCHEMA", target)
    root, env, state, receipt = cross.cross_schema_host(tmp_path)
    before = state.read_bytes(), receipt.read_bytes()
    result = durable._invoke(root, env, "reconcile-exit")
    assert result.returncode != 0 and "SCHEMA_MISMATCH" in result.stdout
    assert (state.read_bytes(), receipt.read_bytes()) == before
    assert durable.starts(root) == []
