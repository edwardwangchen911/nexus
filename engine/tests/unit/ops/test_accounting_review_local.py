"""The operator entry point exports material without loading runtime or calling models."""

import json

from engine.tests.unit.research.test_accounting_review_intake import manifest


def test_export_and_check_use_normal_cli_without_runtime(tmp_path, monkeypatch, capsys):
    from nexus.ops.accounting_review_local import main

    source = tmp_path / "manifest.json"
    source.write_text(json.dumps(manifest(tmp_path)))
    output = tmp_path / "package.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "review",
            "export",
            "--material",
            str(source),
            "--sources",
            str(tmp_path),
            "--output",
            str(output),
        ],
    )
    assert main() == 0
    assert output.stat().st_mode & 0o077 == 0
    assert json.loads(output.read_bytes())["accounting_authority"] is False
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "material_ready"
    assert result["model_calls"] == 0
    monkeypatch.setattr(
        "sys.argv", ["review", "check", "--material", str(output), "--sources", str(tmp_path)]
    )
    assert main() == 0
    assert json.loads(capsys.readouterr().out)["material_sha256"] == result["material_sha256"]
    monkeypatch.setattr(
        "sys.argv",
        [
            "review",
            "export",
            "--material",
            str(source),
            "--sources",
            str(tmp_path),
            "--output",
            str(output),
        ],
    )
    assert main() == 1  # Cannot overwrite an already reviewed artifact.
    assert "ACCOUNTING_REVIEW_INPUT_INVALID" in capsys.readouterr().out
