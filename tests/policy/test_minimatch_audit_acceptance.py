"""One approved advisory never accepts new findings or changed inputs."""

import copy
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from scripts.ci import npm_audit_gate as gate

NOW = datetime(2026, 10, 31, 12, tzinfo=UTC)
STATUS = "risk_accepted_minimatch_until_2026-11-01T00:00:00Z"


@pytest.fixture
def accepted(tmp_path, monkeypatch):
    report = json.loads(
        (Path(__file__).parent / "fixtures/npm-audit-minimatch-20261031.json").read_text()
    )
    subprocess.run(["git", "init", "--quiet", str(tmp_path)], check=True)
    (tmp_path / ".gitignore").write_text("node_modules/\n")
    for name in (
        "engine/src/nexus/main.py",
        "engine/src/nexus/settings.py",
        "pyproject.toml",
        "engine/pyproject.toml",
        "uv.lock",
    ):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# synthetic generator input\n")
    packages = {"": {"name": "synthetic"}}
    for item in report["vulnerabilities"].values():
        for node in item["nodes"]:
            packages[node] = {"version": "synthetic", "dev": True}
    for node in gate.MINIMATCH_TREE_SHA256:
        packages[node] = {"version": "synthetic", "dev": True}
    dependencies = {
        "prod": 1,
        "dev": len(packages) - 1,
        "optional": 0,
        "peer": 0,
        "peerOptional": 0,
        "total": len(packages) - 1,
    }
    report["metadata"]["dependencies"] = dependencies
    (tmp_path / "package-lock.json").write_text(json.dumps({"packages": packages}))
    (tmp_path / "package.json").write_text('{"private":true}')
    (tmp_path / "app").mkdir()
    (tmp_path / "app/page.tsx").write_text("export default () => null;")
    (tmp_path / ".github/workflows").mkdir(parents=True)
    (tmp_path / ".github/workflows/ci.yml").write_text("name: synthetic\n")
    for caller, _, _ in gate.MINIMATCH_RESOLUTION_EDGES:
        path = tmp_path / caller
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("// synthetic consumer, resolved but never executed\n")
    trees = {}
    for node in gate.MINIMATCH_TREE_SHA256:
        directory = tmp_path / node
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "minimatch.js").write_text("// synthetic inspected package\n")
        (directory / "package.json").write_text(json.dumps({"main": "minimatch.js"}))
        trees[node] = gate.installed_tree_digest(tmp_path, node)
    monkeypatch.setattr(gate, "MINIMATCH_TREE_SHA256", trees)
    monkeypatch.setattr(
        gate, "MINIMATCH_LOCK_SHA256", gate.file_digest(tmp_path / "package-lock.json")
    )
    monkeypatch.setattr(gate, "MINIMATCH_SURFACE_SHA256", gate.minimatch_surface_digest(tmp_path))
    return tmp_path, report


def test_exact_acceptance_is_named_and_keeps_full_report(accepted, monkeypatch):
    root, report = accepted
    calls = []

    def audit(command, **kwargs):
        calls.append(command)
        # The unchanged CLI requests the high threshold; moderate-only exits 0.
        return subprocess.CompletedProcess(command, 0, json.dumps(report), "")

    monkeypatch.setattr(gate, "run_audit", audit)
    destination = root / "report.txt"
    assert gate.run(root, destination, NOW) == STATUS
    assert json.loads(destination.read_text()) == report
    assert "--audit-level=high" in calls[0]
    assert not any("omit" in value for value in calls[0])


@pytest.mark.parametrize("fault", ["advisory", "extra", "path", "via", "severity", "count"])
def test_same_package_names_never_hide_changed_findings(accepted, fault):
    root, report = accepted
    item = report["vulnerabilities"]["minimatch"]
    if fault == "advisory":
        item["via"][0]["url"] += "-new"
    elif fault == "extra":
        item["via"].append(copy.deepcopy(item["via"][0]))
    elif fault == "path":
        item["nodes"][0] += "-new"
    elif fault == "via":
        report["vulnerabilities"]["eslint"]["via"] = []
    elif fault == "severity":
        item["severity"] = "high"
    else:
        report["metadata"]["vulnerabilities"]["moderate"] += 1
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(report, 0, root, NOW)


@pytest.mark.parametrize(
    "fault",
    ["lock", "source", "installed", "symlink", "workflow", "redocly", "patched_missing", "matcher"],
)
def test_exact_identity_changes_revoke_acceptance(accepted, fault):
    root, report = accepted
    target = root / "node_modules/minimatch/minimatch.js"
    if fault == "lock":
        lock = json.loads((root / "package-lock.json").read_text())
        lock["packages"]["node_modules/minimatch"]["version"] = "different"
        (root / "package-lock.json").write_text(json.dumps(lock))
    elif fault == "source":
        (root / "app/page.tsx").write_text("changed input")
    elif fault == "installed":
        target.write_text("changed installed implementation")
    elif fault == "workflow":
        (root / ".github/workflows/ci.yml").write_text("changed input command")
    elif fault == "redocly":
        (root / "redocly.yaml").write_text("resolve: {}\n")
    elif fault == "patched_missing":
        (
            root
            / "node_modules/@typescript-eslint/typescript-estree"
            / "node_modules/minimatch/minimatch.js"
        ).unlink()
    elif fault == "matcher":
        (root / "node_modules/@isaacs/nonrecursive-glob-match/minimatch.js").write_text("changed")
    else:
        target.unlink()
        target.symlink_to(root / "app/page.tsx")
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(report, 0, root, NOW)


@pytest.mark.parametrize(
    "at", [datetime(2026, 10, 30, tzinfo=UTC), datetime(2026, 11, 1, tzinfo=UTC)]
)
def test_acceptance_has_fixed_exclusive_expiry(accepted, at):
    root, report = accepted
    with pytest.raises(gate.AuditRejected, match="AUDIT_ACCEPTANCE_EXPIRED"):
        gate.evaluate(report, 0, root, at)


def test_explicit_renewal_has_fixed_end_and_does_not_fill_unaccepted_gap(accepted):
    root, report = accepted
    assert gate.evaluate(report, 0, root, datetime(2026, 11, 1, 3, tzinfo=UTC)) == (
        "risk_accepted_minimatch_until_2026-11-02T00:00:00Z"
    )
    for at in (datetime(2026, 11, 1, 1, tzinfo=UTC), datetime(2026, 11, 2, tzinfo=UTC)):
        with pytest.raises(gate.AuditRejected, match="AUDIT_ACCEPTANCE_EXPIRED"):
            gate.evaluate(report, 0, root, at)


def test_expiration_is_sampled_after_slow_identity_reads(accepted, monkeypatch):
    root, report = accepted
    now = [NOW]
    original = gate.installed_tree_digest

    def inspected(*args):
        result = original(*args)
        now[0] = datetime(2026, 11, 1, tzinfo=UTC)
        return result

    monkeypatch.setattr(gate, "installed_tree_digest", inspected)
    with pytest.raises(gate.AuditRejected, match="AUDIT_ACCEPTANCE_EXPIRED"):
        gate.evaluate(report, 0, root, lambda: now[0])


@pytest.mark.parametrize("exit_code", [1, 2, -1])
def test_moderate_report_cannot_explain_failed_high_threshold_command(accepted, exit_code):
    root, report = accepted
    with pytest.raises(gate.AuditRejected):
        gate.evaluate(report, exit_code, root, NOW)


@pytest.mark.parametrize(
    "relative",
    [
        "node_modules/@typescript-eslint/typescript-estree/node_modules/minimatch/node_modules/@isaacs/nonrecursive-glob-match",
        "node_modules/eslint/node_modules/minimatch",
    ],
)
def test_ignored_nested_package_cannot_shadow_approved_installation(accepted, relative):
    root, report = accepted
    nested = root / relative
    nested.mkdir(parents=True)
    (nested / "index.js").write_text("module.exports = {};\n")
    parent = nested.parent.parent if nested.parent.name == "@isaacs" else nested.parent
    consumer = parent.parent / "caller.js"
    package = "@isaacs/nonrecursive-glob-match" if nested.name != "minimatch" else "minimatch"
    result = subprocess.run(
        [
            "node",
            "-e",
            "console.log(require.resolve(process.argv[1], {paths:[process.argv[2]]}))",
            package,
            str(consumer.parent),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert Path(result.stdout.strip()) == nested / "index.js"
    with pytest.raises(gate.AuditRejected, match="AUDIT_ACCEPTANCE_IDENTITY_CHANGED"):
        gate.evaluate(report, 0, root, NOW)


def test_python_schema_export_entry_is_bound(accepted):
    root, report = accepted
    (root / "engine/src/nexus/main.py").write_text("# changed schema source\n")
    with pytest.raises(gate.AuditRejected, match="AUDIT_ACCEPTANCE_IDENTITY_CHANGED"):
        gate.evaluate(report, 0, root, NOW)


def test_node_file_shadow_rejected_even_when_package_location_set_is_unchanged(accepted):
    root, report = accepted
    shadow = root / "node_modules/minimatch.js"
    shadow.write_text("module.exports = {};\n")
    with pytest.raises(gate.AuditRejected, match="AUDIT_ACCEPTANCE_IDENTITY_CHANGED"):
        gate.evaluate(report, 0, root, NOW)


@pytest.mark.parametrize("mode", ["require", "import"])
def test_real_caller_scope_and_import_conditions_cannot_escape_approved_root(accepted, mode):
    root, report = accepted
    relative = (
        "node_modules/eslint/lib/eslint/eslint-helpers.js"
        if mode == "require"
        else "node_modules/@eslint/config-array/dist/esm/index.js"
    )
    caller = root / relative
    scope = caller.parent
    shadow = scope / "self-reference.mjs"
    shadow.write_text("throw new Error('must never execute');\n")
    require_target = "./self-reference.mjs"
    if mode == "import":
        (scope / "approved.cjs").symlink_to(root / "node_modules/minimatch/minimatch.js")
        require_target = "./approved.cjs"
    (scope / "package.json").write_text(
        json.dumps(
            {
                "name": "minimatch",
                "type": "module" if mode == "import" else "commonjs",
                "exports": {".": {"require": require_target, "import": "./self-reference.mjs"}},
            }
        )
    )
    script = """
import {createRequire} from 'node:module';
import {pathToFileURL, fileURLToPath} from 'node:url';
const [caller, mode] = process.argv.slice(1);
console.log(mode === 'import'
  ? fileURLToPath(import.meta.resolve('minimatch', pathToFileURL(caller).href))
  : createRequire(caller).resolve('minimatch'));
"""
    resolved = subprocess.run(
        [
            "node",
            "--experimental-import-meta-resolve",
            "--input-type=module",
            "-e",
            script,
            str(caller),
            mode,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert Path(resolved.stdout.strip()) == shadow
    with pytest.raises(gate.AuditRejected, match="AUDIT_ACCEPTANCE_IDENTITY_CHANGED"):
        gate.evaluate(report, 0, root, NOW)
