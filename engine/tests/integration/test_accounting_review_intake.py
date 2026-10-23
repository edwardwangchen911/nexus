"""Normal pending receipt is immutable, idempotent and isolated from financial authority."""

import argparse
import json
from datetime import UTC, datetime

import pytest
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.research.accounting_review import build_material, validate_submission
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text

from engine.tests.unit.research.test_accounting_review_intake import manifest, submission


@pytest.mark.asyncio
async def test_receipt_readback_idempotency_conflict_and_no_financial_authority(
    migrated_database_url, tmp_path
):
    from nexus.research.accounting_review_storage import read_receipt, receive_submission

    now = datetime.now(UTC)
    package = build_material(manifest(tmp_path), source_root=tmp_path, now=now)
    data = submission(package)
    data["reviewed_at"] = now.isoformat()
    opinion = validate_submission(data, package=package, source_root=tmp_path, now=now)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as session, session.begin():
            before = await session.scalar(select(func.count()).select_from(ResearchEvidenceRef))
            first = await receive_submission(session, package, opinion, source_root=tmp_path)
            second = await receive_submission(session, package, opinion, source_root=tmp_path)
            assert first.id == second.id
            assert first.created_at >= now
            assert first.available_at == first.received_at == first.created_at
            assert first.payload["status"] == "received_unverified"
            assert (
                await session.scalar(select(func.count()).select_from(ResearchEvidenceRef))
                == before
            )
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            result = await read_receipt(session, opinion.submission_id)
            assert result.id == first.id
            assert result.payload == first.payload
        async with factory() as session, session.begin():
            conflict = opinion.model_copy(update={"reviewer_name": "Another claimed author"})
            with pytest.raises(ValueError, match="ACCOUNTING_SUBMISSION_CONFLICT"):
                await receive_submission(session, package, conflict, source_root=tmp_path)
            assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_operator_receive_and_readback_use_same_persisted_receipt(
    migrated_database_url, tmp_path, monkeypatch
):
    from nexus.ops.accounting_review_local import database_action

    settings = EngineSettings(database_url=migrated_database_url)
    # The runtime configuration is explicit and isolated; the CLI, transaction,
    # receipt producer and readback remain the normal product implementations.
    monkeypatch.setattr("nexus.ops.openai_local.local_settings", lambda *a, **k: settings)
    monkeypatch.setattr("nexus.ops.local_stack._validate_local_env", lambda *a: None)
    data = manifest(tmp_path)
    material = build_material(data, source_root=tmp_path, now=datetime.now(UTC))
    opinion = submission(material)
    opinion["reviewed_at"] = datetime.now(UTC).isoformat()
    material_path, opinion_path = tmp_path / "material.json", tmp_path / "opinion.json"
    material_path.write_text(json.dumps(data))
    opinion_path.write_text(json.dumps(opinion))
    args = argparse.Namespace(
        action="receive",
        repo_root=tmp_path,
        material=material_path,
        sources=tmp_path,
        submission=opinion_path,
        submission_id=None,
    )
    received = await database_action(args)
    assert received["status"] == "received_unverified"
    assert received["accounting_authority"] is False
    assert received["model_calls"] == received["provider_calls"] == 0
    args.action = "inspect"
    from uuid import UUID

    args.submission_id = UUID(opinion["submission_id"])
    assert await database_action(args) == received
