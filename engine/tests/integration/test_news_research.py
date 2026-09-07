"""Exact news ownership survives newer collections and remains opt-in."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.research_snapshots import seed_filing
from nexus.agents.codex_staged import StagedCodexCommittee
from nexus.agents.committee import load_prompt_assets
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.api.research_inputs import read_research_inputs
from nexus.ops import news_import
from nexus.ops.openai_local import prepare_database
from nexus.research.news import import_news
from nexus.research.news_evidence import read_news_evidence, register_news_evidence
from test_news import ID, archive, setup


@pytest.mark.asyncio
async def test_exact_news_reference_never_uses_latest(migrated_database_url, tmp_path):
    _, factory, _, store = await setup(migrated_database_url)
    archive(tmp_path / "first")
    async with factory.begin() as s:
        snapshot_id = await import_news(s, store, instrument_id=ID, directory=tmp_path / "first")
    async with factory.begin() as s:
        ref = await register_news_evidence(s, store, instrument_id=ID, news_snapshot_id=snapshot_id)
        ref_id = ref.id
    async with factory.begin() as s:
        repeated = await register_news_evidence(
            s, store, instrument_id=ID, news_snapshot_id=snapshot_id
        )
        assert repeated.id == ref_id
    async with factory() as s:
        docs = await read_news_evidence(
            s, instrument_id=ID, as_of=datetime.now(UTC), evidence_ids=(ref_id,)
        )
        assert len(docs) == 1 and docs[ref_id].news_snapshot_id == snapshot_id
        assert docs[ref_id].execution_locked is True
        assert docs[ref_id].primary_source_verified is False
        assert (
            await read_news_evidence(
                s, instrument_id=UUID(int=999), as_of=datetime.now(UTC), evidence_ids=(ref_id,)
            )
            == {}
        )
        assert (
            await read_news_evidence(s, instrument_id=ID, as_of=datetime.now(UTC), evidence_ids=())
            == {}
        )
    archive(tmp_path / "newer", empty=True)
    async with factory.begin() as s:
        await import_news(s, store, instrument_id=ID, directory=tmp_path / "newer")
    async with factory() as s:
        assert (
            await read_news_evidence(
                s, instrument_id=ID, as_of=datetime.now(UTC), evidence_ids=(ref_id,)
            )
            == docs
        )


@pytest.mark.asyncio
async def test_registered_news_is_optional_frozen_research_input(
    migrated_database_url, tmp_path, monkeypatch
):
    settings, factory, _, store = await setup(migrated_database_url)
    await prepare_database(settings)
    archive(tmp_path / "news")
    monkeypatch.setattr(news_import, "S3ProviderEvidenceStore", lambda *args, **kwargs: store)
    result = await news_import.run(settings, ID, tmp_path / "news")
    registered = await news_import.run(
        settings, ID, None, news_snapshot_id=UUID(result["snapshot_id"])
    )
    evidence_id = UUID(registered["evidence_ref_id"])
    async with factory.begin() as session:
        # News is supplementary and cannot create a research base by itself.
        assert await prepare_research_snapshot(session, ID, as_of=datetime.now(UTC)) is None
        frozen = await read_news_evidence(
            session, instrument_id=ID, as_of=datetime.now(UTC), evidence_ids=(evidence_id,)
        )
        assert frozen[evidence_id].news_snapshot_id == UUID(result["snapshot_id"])
        await seed_filing(session, instrument_id=ID, seed_instrument=False)
    async with factory.begin() as session:
        baseline = await prepare_research_snapshot(session, ID, as_of=datetime.now(UTC))
        assert baseline is not None
        assert str(evidence_id) not in baseline.payload["evidence_ids"]
        with pytest.raises(ValueError, match="OPENAI_NEWS_EVIDENCE_UNAVAILABLE"):
            await prepare_research_snapshot(
                session, ID, as_of=datetime.now(UTC), news_evidence_id=UUID(int=999)
            )
        research_snapshot = await prepare_research_snapshot(
            session, ID, as_of=datetime.now(UTC), news_evidence_id=evidence_id
        )
        assert research_snapshot is not None
        assert str(evidence_id) in research_snapshot.payload["evidence_ids"]
        decision = datetime.now(UTC)
        request = ResearchRequest(
            instrument_id=ID, data_snapshot_id=research_snapshot.id, decision_time=decision
        )
        admitted = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="news-frozen-tool",
            request_hash="b" * 64,
            request=request,
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal("3"),
            now=decision,
        )
        tools = await database_read_only_tools(session, admitted.run, allow_fixture_evidence=False)
        result = await tools.execute(
            "get_security_snapshot",
            {
                "instrument_id": str(ID),
                "data_snapshot_id": str(research_snapshot.id),
                "as_of": decision.isoformat(),
            },
        )
        entries = [x for x in result["items"] if "news_observation" in x]
        assert len(entries) == 1 and entries[0]["evidence_ref_id"] == str(evidence_id)
        observation = entries[0]["news_observation"]
        assert observation["articles"][0]["headline"] == "Synthetic announcement"
        assert "raw_text" not in observation and "receipt_text" not in observation
        committee = StagedCodexCommittee(prompts=load_prompt_assets(), tools=tools)
        assert (await committee._inputs(request))["security_snapshot"] == result
        view = await read_research_inputs(
            session, snapshot_id=research_snapshot.id, instrument_id=ID, as_of=decision
        )
        assert view is not None and len(view.news_observations) == 1
        assert view.news_observations[0].evidence_ref_id == evidence_id
        assert "raw_text" not in view.model_dump_json()
        old_view = await read_research_inputs(
            session, snapshot_id=baseline.id, instrument_id=ID, as_of=decision
        )
        assert old_view is not None and old_view.news_observations == ()


def test_news_migration_roundtrip_and_runtime_head(temporary_database_url, monkeypatch):
    from alembic import command
    from alembic.config import Config
    from alembic.script import ScriptDirectory
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    assert ScriptDirectory.from_config(config).get_heads() == [CURRENT_MIGRATION_HEAD]
    command.upgrade(config, "0044_news_research")
    command.downgrade(config, "0043_current_price_trends")
    command.upgrade(config, "0044_news_research")


@pytest.mark.asyncio
async def test_missing_original_is_rejected_by_reader_and_database_guard(
    migrated_database_url, tmp_path
):
    import json

    from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
    from nexus.persistence.models.data import DataSnapshot
    from sqlalchemy import select, text
    from sqlalchemy.exc import IntegrityError

    _, factory, _, store = await setup(migrated_database_url)
    archive(tmp_path / "news")
    async with factory.begin() as session:
        saved = await import_news(session, store, instrument_id=ID, directory=tmp_path / "news")
    async with factory.begin() as session:
        ref = await register_news_evidence(session, store, instrument_id=ID, news_snapshot_id=saved)
        await seed_filing(session, instrument_id=ID, seed_instrument=False)
    async with factory.begin() as session:
        row = await session.scalar(
            select(DataSnapshot).where(
                DataSnapshot.source == ref.source, DataSnapshot.revision == ref.revision
            )
        )
        payload = dict(row.payload)
        del payload["raw_text"]
        # Corrupt only the synthetic source row; leave the frozen ownership guard enabled.
        await session.execute(text("ALTER TABLE data_snapshots DISABLE TRIGGER USER"))
        await session.execute(
            text("UPDATE data_snapshots SET payload=CAST(:p AS jsonb) WHERE id=:id"),
            {"p": json.dumps(payload), "id": row.id},
        )
        await session.execute(text("ALTER TABLE data_snapshots ENABLE TRIGGER USER"))
        session.expire(row)
        assert not await read_news_evidence(
            session, instrument_id=ID, as_of=datetime.now(UTC), evidence_ids=(ref.id,)
        )
        baseline = await prepare_research_snapshot(session, ID, as_of=datetime.now(UTC))
        assert baseline is not None
        with pytest.raises(IntegrityError, match="not owned by the instrument"):
            async with session.begin_nested():
                session.add(
                    AgentDataSnapshotEvidenceRecord(
                        data_snapshot_id=baseline.id,
                        instrument_id=ID,
                        evidence_ref_id=ref.id,
                        authority_kind="research",
                        selected_at=None,
                        source=ref.source,
                        provider_object_id=ref.provider_object_id,
                        revision=ref.revision,
                        available_at=ref.available_at,
                        location=ref.location,
                        content_hash=ref.content_hash,
                    )
                )
                await session.flush()
