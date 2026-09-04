"""Real importer CLI/transaction, only source HTTP and local S3 transport substituted."""

import asyncio
import json
from datetime import UTC, datetime

import pytest
from nexus.ops import alpaca_prices_import as entry
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.current_price_trends_storage import read_current_price_trends
from sqlalchemy import func, select

from .test_current_price_trends import INSTRUMENT, setup_trends


@pytest.mark.parametrize("output_failure", [False, True])
def test_actual_import_cli_persists_with_safe_success_or_honest_unknown_after_commit(
    output_failure,
    migrated_database_url,
    tmp_path,
    monkeypatch,
    capsys,
):
    directory = tmp_path / "archive"

    async def setup_cli():
        result = await setup_trends(migrated_database_url, directory, monkeypatch)
        await result[1].kw["bind"].dispose()
        return result

    settings, _, store, client, _ = asyncio.run(setup_cli())
    monkeypatch.setattr(entry, "load_local_environment", lambda *args: settings.model_dump())
    monkeypatch.setattr(entry, "S3ProviderEvidenceStore", lambda *args, **kwargs: store)
    output = tmp_path / "result.json"
    monkeypatch.setattr(
        "sys.argv",
        [
            "alpaca_prices_import",
            "--instrument-id",
            str(INSTRUMENT),
            "--collection-dir",
            str(directory),
            "--output",
            str(output),
        ],
    )
    if output_failure:

        def unavailable(*args):
            raise OSError("synthetic-private-canary")

        monkeypatch.setattr(entry, "write_private", unavailable)
    assert entry.main() == (1 if output_failure else 0)
    captured = capsys.readouterr()
    assert (
        '"status": "unknown"' in captured.err
        if output_failure
        else '"status": "stored_context"' in captured.out
    )
    assert str(INSTRUMENT) not in captured.out + captured.err
    assert "synthetic-private-canary" not in captured.out + captured.err

    async def verify():
        factory = session_factory(settings)
        async with factory() as session:
            docs = await read_current_price_trends(
                session, instrument_id=INSTRUMENT, as_of=datetime.now(UTC)
            )
            assert len(docs) == 1
            assert next(iter(docs.values())).features["sma_200"] == "100.500000000000"
            assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 1
            if not output_failure:
                saved = json.loads(output.read_text())
                assert saved["evidence_id"] == str(next(iter(docs)))
                assert (
                    saved["research_created"] is False and saved["market_price_eligible"] is False
                )
        await factory.kw["bind"].dispose()

    asyncio.run(verify())
    assert len(client.objects) == 4


@pytest.mark.asyncio
async def test_canonical_object_readback_failure_rolls_back_db_but_preserves_private_originals(
    migrated_database_url,
    tmp_path,
    monkeypatch,
):
    directory = tmp_path / "archive"
    settings, factory, store, client, _ = await setup_trends(
        migrated_database_url, directory, monkeypatch
    )
    original_get = client.get_object

    def failed_read(**kwargs):
        if "/evidence/" in str(kwargs["Key"]):
            raise OSError("synthetic object readback failed")
        return original_get(**kwargs)

    monkeypatch.setattr(client, "get_object", failed_read)
    with pytest.raises(OSError):
        await entry.import_trends(
            settings, instrument_id=INSTRUMENT, collection_dir=directory, store=store
        )
    async with factory() as session:
        for model in (DataArtifact, DataSnapshot, ResearchEvidenceRef):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
    assert len(client.objects) == 4 and (directory / "manifest.json").exists()
