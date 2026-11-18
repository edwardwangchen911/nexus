"""Canonical bytes stay historical while filing-sized text fits the unchanged gate."""

import asyncio
import importlib.util
import json
import random
from pathlib import Path
from uuid import uuid4

import pytest
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.asyncio
async def test_filing_sized_strings_fit_existing_five_second_gate(migrated_database_url):
    value = {
        "documents": [
            {"text": "a" * 4339},
            {"text": "x\u00a0" * 12676},
            {"text": "z" * 21019},
            {"text": "q" * 12014},
        ]
    }
    expected = json.dumps(value, sort_keys=True, separators=(",", ":"))
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.begin() as db:
            await db.execute(text("SET LOCAL statement_timeout='5s'"))
            actual = (
                await db.execute(
                    text(
                        "SELECT nexus_mixed_receipt_canonical(v), "
                        "nexus_mixed_receipt_canonical(v->'documents'), "
                        "nexus_mixed_receipt_canonical(v#>'{documents,1,text}') "
                        "FROM (SELECT CAST(:v AS jsonb) v) source"
                    ),
                    {"v": json.dumps(value)},
                )
            ).one()
            assert actual == (
                expected,
                json.dumps(value["documents"], sort_keys=True, separators=(",", ":")),
                json.dumps(value["documents"][1]["text"]),
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_current_head_readonly_entries_reach_original_evidence_checks(
    migrated_database_url, tmp_path
):
    from nexus.ops.filing_review_local import prepare_review_material
    from nexus.ops.reviewed_content_local import inspect_reviewed_content
    from nexus.ops.sec_graphic_adoption_local import parser, run

    settings = EngineSettings(database_url=migrated_database_url, agent_paper_account_id=uuid4())
    identifier = uuid4()
    calls = [
        lambda: inspect_reviewed_content(settings, adoption_id=identifier),
        lambda: prepare_review_material(
            settings, snapshot_id=identifier, output_dir=tmp_path / "review"
        ),
        lambda: run(settings, parser().parse_args(["inspect", "--snapshot-id", str(identifier)])),
    ]
    for call in calls:
        with pytest.raises(ValueError) as error:
            await call()
        assert str(error.value) != "MONITOR_SCHEMA_UNAVAILABLE"
    assert not (tmp_path / "review").exists()


def test_migration_restores_exact_predecessor_function(temporary_database_url, monkeypatch):
    from alembic import command
    from alembic.config import Config

    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))

    async def definition():
        engine = create_async_engine(temporary_database_url)
        try:
            async with engine.begin() as db:
                return await db.scalar(
                    text(
                        "SELECT pg_get_functiondef("
                        "'nexus_mixed_receipt_canonical(jsonb)'::regprocedure)"
                    )
                )
        finally:
            await engine.dispose()

    command.upgrade(config, "0136_sec_adopted_content")
    original = asyncio.run(definition())
    command.upgrade(config, "head")
    assert asyncio.run(definition()) != original
    command.downgrade(config, "0136_sec_adopted_content")
    assert asyncio.run(definition()) == original
    command.upgrade(config, "head")


@pytest.mark.asyncio
async def test_canonical_matches_legacy_unicode_and_json_bytes(migrated_database_url):
    source = Path(__file__).resolve().parents[2] / "alembic/versions/0096_monitor_event_receipts.py"
    spec = importlib.util.spec_from_file_location("legacy_canonical", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    rng = random.Random(137)
    alphabet = [chr(i) for i in range(1, 160)] + [
        "中",
        "é",
        "\u07ff",
        "\u0800",
        "\ud7ff",
        "\ue000",
        "\uffff",
        "\U00010000",
        "😀",
        "\U0010ffff",
    ]
    values = [
        None,
        True,
        False,
        0,
        -7,
        "",
        "\\u1234",
        '"\\\b\f\n\r\t',
        "".join(alphabet),
        {"中": [1, None, {"😀": "é\u007f"}], "a": []},
    ]
    values += [{"k": "".join(rng.choices(alphabet, k=50)), "a": [i, False]} for i in range(100)]
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.begin() as db:
            await db.execute(
                text(
                    module._CANONICAL.replace(
                        "nexus_mixed_receipt_canonical", "nexus_legacy_test_canonical"
                    )
                )
            )
            await db.execute(text("SET LOCAL statement_timeout='5s'"))
            for value in values:
                current, legacy = (
                    await db.execute(
                        text(
                            "SELECT nexus_mixed_receipt_canonical(CAST(:v AS jsonb)), "
                            "nexus_legacy_test_canonical(CAST(:v AS jsonb))"
                        ),
                        {"v": json.dumps(value)},
                    )
                ).one()
                assert current == legacy == json.dumps(value, sort_keys=True, separators=(",", ":"))
            assert await db.scalar(text("SELECT nexus_mixed_receipt_canonical(NULL) IS NULL"))
    finally:
        await engine.dispose()
