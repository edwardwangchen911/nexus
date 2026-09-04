"""Explicit local importer errors are safe and never start an external collector."""

import importlib

import pytest


@pytest.mark.parametrize(
    "args", [[], ["--instrument-id", "private-canary"], ["--unknown", "private-canary"]]
)
def test_invalid_import_cli_fails_before_settings_and_has_no_raw_error(args, monkeypatch, capsys):
    name = "nexus.ops.alpaca_prices_import"
    assert importlib.util.find_spec(name) is not None, "normal archived trend import CLI missing"
    entry = importlib.import_module(name)
    monkeypatch.setattr("sys.argv", ["alpaca_prices_import", *args])

    def forbidden(*args):
        raise AssertionError("invalid request must not load credentials")

    monkeypatch.setattr(entry, "load_local_environment", forbidden)
    assert entry.main() == 1
    output = capsys.readouterr()
    assert "private-canary" not in output.out + output.err
    assert '"code": "CURRENT_TRENDS_IMPORT_FAILED"' in output.err
    assert '"status": "rejected"' in output.err


def test_output_failure_after_import_reports_unknown_not_unwritten(tmp_path, monkeypatch, capsys):
    from nexus.ops import alpaca_prices_import as entry

    monkeypatch.setattr(
        "sys.argv",
        [
            "alpaca_prices_import",
            "--instrument-id",
            "00000000-0000-4000-8000-000000000001",
            "--collection-dir",
            str(tmp_path),
            "--output",
            str(tmp_path / "result.json"),
        ],
    )
    monkeypatch.setattr(entry, "load_local_environment", lambda *args: {})

    async def committed(*args, **kwargs):
        return {"status": "stored_context", "evidence_id": "synthetic-private-canary"}

    monkeypatch.setattr(entry, "import_trends", committed)

    def failed_write(*args):
        raise OSError("synthetic-private-canary")

    monkeypatch.setattr(entry, "write_private", failed_write)
    assert entry.main() == 1
    output = capsys.readouterr()
    assert '"status": "unknown"' in output.err
    assert "synthetic-private-canary" not in output.out + output.err
