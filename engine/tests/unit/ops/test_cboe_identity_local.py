"""Private handwritten files only; no source or broker calls."""

import importlib

import pytest


def paths(tmp_path):
    result = {}
    for name, files in {
        "fmp_archive": ["profile.json", "receipt.json"],
        "mic_archive": [
            "publication.html",
            "publication.html.receipt.json",
            "ISO10383_MIC.csv",
            "ISO10383_MIC.csv.receipt.json",
        ],
        "listing_archive": [
            "listed.html",
            "listed.html.receipt.json",
            "listed.csv",
            "listed.csv.receipt.json",
            "product.html",
            "product.html.receipt.json",
        ],
    }.items():
        folder = tmp_path / name
        folder.mkdir(mode=0o700)
        for f in files:
            p = folder / f
            p.write_bytes(f.encode())
            p.chmod(0o600)
        result[name] = folder
    for name in [
        "submission_file",
        "company_directory_file",
        "company_receipt_file",
        "fund_directory_file",
        "fund_receipt_file",
    ]:
        p = tmp_path / name
        p.write_bytes(name.encode())
        p.chmod(0o600)
        result[name] = p
    (tmp_path / "receipt.json").write_bytes(b"submission-receipt")
    (tmp_path / "receipt.json").chmod(0o600)
    tmp_path.chmod(0o700)
    return result


def load(values):
    return importlib.import_module("nexus.ops.cboe_identity_local").load_archives(**values)


def test_archive_loader_keeps_order_and_bytes(tmp_path):
    c, d = load(paths(tmp_path))
    assert len(c) == 14 and len(d) == 4
    assert c[:4] == (b"profile.json", b"receipt.json", b"submission_file", b"submission-receipt")
    assert d == (
        b"company_directory_file",
        b"company_receipt_file",
        b"fund_directory_file",
        b"fund_receipt_file",
    )


@pytest.mark.parametrize("unsafe", ["directory", "file", "symlink"])
def test_archive_loader_refuses_unprotected_or_linked_input(tmp_path, unsafe):
    values = paths(tmp_path)
    target = values["submission_file"]
    if unsafe == "directory":
        tmp_path.chmod(0o755)
    elif unsafe == "file":
        target.chmod(0o644)
    else:
        other = tmp_path / "other"
        target.rename(other)
        target.symlink_to(other)
    with pytest.raises(ValueError, match="CBOE_HELD_ARCHIVE_INVALID"):
        load(values)


def test_existing_etf_cli_parses_cboe_held_entry_without_account_output(
    monkeypatch, tmp_path, capsys
):
    import sys
    from types import SimpleNamespace
    from uuid import uuid4

    from nexus.market.current_identity_storage import CurrentIdentityImportResult
    from nexus.ops import etf_identity_local as cli

    module = importlib.import_module("nexus.ops.cboe_identity_local")
    values = paths(tmp_path)
    output = tmp_path / "result.json"
    calls = []

    async def importer(settings, broker_settings, **kwargs):
        calls.append(kwargs)
        return CurrentIdentityImportResult("created", uuid4(), uuid4())

    monkeypatch.setattr(module, "import_held_identity", importer)
    monkeypatch.setattr(cli, "load_local_environment", lambda root, env: {"synthetic-config": True})
    monkeypatch.setattr(cli.EngineSettings, "model_validate", lambda env: SimpleNamespace())
    monkeypatch.setattr(cli.BrokerWorkerSettings, "model_validate", lambda env: SimpleNamespace())
    argv = [
        "etf-identity",
        "--repo-root",
        str(tmp_path),
        "import-held-cboe",
        "--symbol",
        "TEST",
        "--output",
        str(output),
    ]
    for name, path in values.items():
        argv.extend(["--" + name.replace("_", "-"), str(path)])
    monkeypatch.setattr(sys, "argv", argv)
    cli.main()
    assert len(calls) == 1 and calls[0]["symbol"] == "TEST"
    assert calls[0]["company_receipt_file"] == values["company_receipt_file"]
    assert output.exists() and output.stat().st_mode & 0o077 == 0
    with pytest.raises(SystemExit):
        cli.main()
    assert len(calls) == 1
    assert "account" not in capsys.readouterr().out


@pytest.mark.asyncio
async def test_invalid_sources_stop_before_broker_or_database(tmp_path):
    from nexus.settings import EngineSettings

    module = importlib.import_module("nexus.ops.cboe_identity_local")

    class Broker:
        async def read_accounts(self):
            raise AssertionError("invalid source reached broker")

    with pytest.raises(ValueError, match="CBOE_FUND_SOURCES_INVALID"):
        await module.import_held_identity(
            EngineSettings(
                database_url="postgresql+asyncpg://synthetic@127.0.0.1/unused",
                object_store_endpoint="http://127.0.0.1:9000",
            ),
            None,
            symbol="TEST",
            broker=Broker(),
            store=object(),
            **paths(tmp_path),
        )
