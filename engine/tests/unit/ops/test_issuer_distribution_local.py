"""Receipt and CLI privacy boundaries use only synthetic local inputs."""

import hashlib
import json
import sys
from datetime import UTC, datetime

import pytest
from nexus.ops import issuer_distribution_local as entry


@pytest.mark.parametrize("invalid", ["hash", "future", "naive"])
def test_receipt_rejects_corruption_or_invented_time(tmp_path, invalid):
    original = tmp_path / "original"
    original.write_bytes(b"synthetic original")
    receipt = tmp_path / "receipt.json"
    receipt.write_text(
        json.dumps(
            {
                "url": "https://www.ssga.com/synthetic",
                "sha256": "a" * 64
                if invalid == "hash"
                else hashlib.sha256(original.read_bytes()).hexdigest(),
                "received_at": "2099-01-01T00:00:00Z"
                if invalid == "future"
                else ("2026-01-01T00:00:00" if invalid == "naive" else "2026-01-01T00:00:00Z"),
            }
        )
    )
    with pytest.raises(ValueError):
        entry.read_original(original, receipt)


def test_cli_failure_never_prints_original_or_connection(tmp_path, monkeypatch, capsys):
    tmp_path.chmod(0o700)
    arguments = [
        "issuer",
        "--repo-root",
        str(tmp_path),
        "--instrument-id",
        "00000000-0000-0000-0000-000000000031",
    ]
    for name in ("workbook", "workbook-receipt", "identity", "identity-receipt", "output"):
        arguments.extend(["--" + name, str(tmp_path / name)])
    monkeypatch.setattr(sys, "argv", arguments)
    monkeypatch.setattr(
        entry,
        "read_original",
        lambda *_: (
            b"synthetic",
            "https://www.ssga.com/synthetic",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
    )

    async def fail(*_args, **_kwargs):
        raise RuntimeError("synthetic private connection or account values")

    monkeypatch.setattr(entry, "import_originals", fail)
    assert entry.main() == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err) == {"status": "failed", "error": "ISSUER_IMPORT_FAILED"}


def test_cli_nav_selection_reaches_existing_import_without_model(tmp_path, monkeypatch, capsys):
    tmp_path.chmod(0o700)
    arguments = [
        "issuer",
        "--nav-history",
        "--repo-root",
        str(tmp_path),
        "--instrument-id",
        "00000000-0000-0000-0000-000000000031",
    ]
    for name in ("workbook", "workbook-receipt", "identity", "identity-receipt", "output"):
        arguments.extend(["--" + name, str(tmp_path / name)])
    monkeypatch.setattr(sys, "argv", arguments)
    monkeypatch.setattr(
        entry,
        "read_original",
        lambda *_: (
            b"synthetic",
            "https://www.ssga.com/synthetic",
            datetime(2026, 1, 1, tzinfo=UTC),
        ),
    )

    async def imported(*_args, **kwargs):
        assert kwargs["nav_history"] is True and kwargs["fund_observation"] is False
        return {"frozen_readback_verified": True, "model_called": False}

    monkeypatch.setattr(entry, "import_originals", imported)
    assert entry.main() == 0
    assert json.loads((tmp_path / "output").read_text())["model_called"] is False
