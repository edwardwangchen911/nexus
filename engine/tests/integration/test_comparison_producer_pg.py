"""Normal comparison producer against synthetic native PostgreSQL facts."""

import asyncio
import copy
import hashlib
import json
from argparse import Namespace
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from nexus.agents.comparison_inputs import _manifest
from nexus.agents.comparison_manifest import ACTOR, PURPOSE, SDK_HASH, ComparisonManifest
from nexus.agents.comparison_producer import admit_comparison_position
from nexus.agents.persistence import BudgetExhausted, reserve_daily_budget
from nexus.agents.research_only import require_formal_research
from nexus.monitoring.independent_context import IndependentMonitorContext, content
from nexus.ops.research_comparison import native_command, prepare_archive
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from unit.agents.test_comparison_inputs import archive_source, digest


async def _seed_canonical_sdk_profile(factory, **changes):
    from nexus.agents.codex_bounded_budget import bounded_stage_config

    config = bounded_stage_config()
    fields = {
        key: config[key]
        for key in (
            "model_id",
            "version",
            "max_input_tokens",
            "max_output_tokens",
            "max_tool_calls",
        )
    }
    fields.update(config_hash=SDK_HASH)
    for key in ("input_usd_per_million", "output_usd_per_million"):
        fields[key] = Decimal(str(config[key]))
    for key in ("pricing_available_at", "pricing_expires_at"):
        fields[key] = datetime.fromisoformat(str(config[key]))
    fields.update(changes)
    async with factory() as session, session.begin():
        row = AgentModelConfigRecord(**fields)
        session.add(row)
        await session.flush()
        return row.id


@pytest.mark.asyncio
async def test_normal_api_profile_registration_is_idempotent_and_preserves_unknown(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.agents.comparison_profile import API_PROFILE_VERSION, api_profile_hash
    from nexus.ops import openai_local

    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    _, _, _, _, old_id = await seed(factory)
    sdk_id = await _seed_canonical_sdk_profile(factory)
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    args = Namespace(command="register-profile", repo_root=tmp_path, sdk_model_config_id=sdk_id)
    inspection = Namespace(command="inspect-profile", repo_root=tmp_path)
    assert (await native_command(inspection))["status"] == "profile_not_registered"
    first, second = await asyncio.gather(native_command(args), native_command(args))
    assert first == second
    assert first["dispatch_authority"] is False
    assert first["config_hash"] == api_profile_hash()
    readback = await native_command(inspection)
    assert readback["model_config_id"] == first["model_config_id"]
    assert readback["inspection_only"] is True and readback["dispatch_authority"] is False
    async with factory() as session:
        rows = list(
            await session.scalars(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == API_PROFILE_VERSION
                )
            )
        )
        assert len(rows) == 1 and str(rows[0].id) == first["model_config_id"]
        original = await session.get(AgentModelConfigRecord, sdk_id)
        for key in (
            "max_input_tokens",
            "max_output_tokens",
            "max_tool_calls",
            "pricing_available_at",
            "pricing_expires_at",
            "input_usd_per_million",
            "output_usd_per_million",
        ):
            assert getattr(rows[0], key) == getattr(original, key)
        old = await session.get(AgentDailyBudgetReservationRecord, old_id)
        assert (old.state, old.reserved_usd, old.settled_usd) == ("reserved", Decimal(2), None)
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 0
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 0
        assert await session.scalar(text("SELECT count(*) FROM jobs")) == 0
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["source", "existing"])
async def test_api_profile_registration_rejects_changed_authority_without_overwriting(
    migrated_database_url, damage
):
    from nexus.agents.comparison_profile import API_PROFILE_VERSION, register_api_profile

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    source_id = await _seed_canonical_sdk_profile(
        factory, **({"max_input_tokens": 64000} if damage == "source" else {})
    )
    if damage == "existing":
        await _seed_canonical_sdk_profile(
            factory, version=API_PROFILE_VERSION, config_hash="f" * 64
        )
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="COMPARISON_PROFILE_.*INVALID"):
            await register_api_profile(session, sdk_model_config_id=source_id)
        rows = list(
            await session.scalars(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == API_PROFILE_VERSION
                )
            )
        )
        assert len(rows) == (0 if damage == "source" else 1)
        if rows:
            assert rows[0].config_hash == "f" * 64
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_registered_api_profile_is_not_generic_research_authority(migrated_database_url):
    from integration.test_agent_execution_pg import _seed_authority
    from integration.test_candidate_research_authority import admit
    from nexus.agents.comparison_profile import API_PROFILE_VERSION, register_api_profile
    from nexus.jobs.models import JobType
    from nexus.ops.openai_local import prepare_database

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    await _seed_authority(factory)
    await prepare_database(settings)
    sdk_id = await _seed_canonical_sdk_profile(factory)
    # Make the new profile unambiguously newer than the generic model. The
    # suite normally freezes all created_at defaults to one old instant.
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "ALTER TABLE agent_model_configs ALTER COLUMN created_at "
                "SET DEFAULT '2026-09-02T00:00:00+00:00'::timestamptz"
            )
        )
    async with factory() as session, session.begin():
        registered = await register_api_profile(session, sdk_model_config_id=sdk_id)
        api_id = registered.id
    # Same normal producer, both default selection and explicit model selection.
    admission = await admit(factory, None, job_type=JobType.AGENT_RESEARCH, key="default")
    async with factory() as session:
        model = await session.get(AgentModelConfigRecord, admission.run.model_config_id)
        assert model.version != API_PROFILE_VERSION and model.id != api_id
    with pytest.raises(ValueError, match="COMPARISON_MODEL_BUSINESS_FORBIDDEN"):
        await admit(factory, api_id, job_type=JobType.AGENT_RESEARCH, key="explicit-api")
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_profile_registration_wait_is_bounded_and_readback_resolves_commit(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.agents.comparison_profile import register_api_profile
    from nexus.ops import openai_local

    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    sdk_id = await _seed_canonical_sdk_profile(factory)
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    args = Namespace(command="register-profile", repo_root=tmp_path, sdk_model_config_id=sdk_id)
    async with factory() as owner, owner.begin():
        profile = await register_api_profile(owner, sdk_model_config_id=sdk_id)
        with pytest.raises(DBAPIError) as caught:
            await native_command(args)
        assert caught.value.orig.sqlstate == "55P03"
    readback = await native_command(Namespace(command="inspect-profile", repo_root=tmp_path))
    assert readback["model_config_id"] == str(profile.id)
    assert readback["inspection_only"] is True
    await factory.kw["bind"].dispose()


async def seed(factory, *, source_kind="fixture", protect_unknown=True):
    archive = archive_source()
    # Native references are content addressed; no personal or live data is used.
    for member in archive["members"]:
        company = member["evidence"][0]
        company["source"] = source_kind
        company["object_id"] += ":" + member["request"]["instrument_id"]
        company["location"] = "memory://sha256/" + company["content_hash"]
        security = member["inputs"]["security_snapshot"]
        security["items"][0]["provider_object_id"] = company["object_id"]
        security["items"][0]["source"] = source_kind
        ctx = security["independent_monitor_context"]
        when = datetime.fromisoformat(member["request"]["decision_time"])
        base_payload = {
            "purpose": "research_only_v2",
            "as_of": when.isoformat(),
            "instrument_id": member["request"]["instrument_id"],
            "evidence_ids": [security["items"][0]["evidence_ref_id"]],
        }
        ctx["source_snapshot_hash"] = hashlib.sha256(
            json.dumps(base_payload, sort_keys=True).encode()
        ).hexdigest()
        context_hash = hashlib.sha256(
            content(IndependentMonitorContext.model_validate_json(json.dumps(ctx)))
        ).hexdigest()
        member["evidence"][1].update(
            content_hash=context_hash, location=f"memory://sha256/{context_hash}"
        )
        security["items"][1].update(content_hash=context_hash, revision=context_hash)
        for envelope in member["inputs"].values():
            envelope["evidence_hash"] = digest(_manifest(member))
    source = json.dumps(archive).encode()
    prepared = prepare_archive(
        source, expected_source_hash=hashlib.sha256(source).hexdigest(), experiment_id=uuid4()
    )
    manifest = ComparisonManifest(
        experiment_id=prepared["experiment_id"],
        sdk_config_hash=SDK_HASH,
        api_config_hash="a" * 64,
        implementation_sha="b" * 40,
        source_manifest_hash=prepared["source_manifest_hash"],
        identity_equivalence_hash=prepared["identity_equivalence_hash"],
        outbound_safety_hash=prepared["outbound_safety_hash"],
        scoring_hash="c" * 64,
        blinding_hash="d" * 64,
        input_hashes=prepared["input_hashes"],
        instrument_ids=prepared["instrument_ids"],
    )
    async with factory() as session, session.begin():
        now = await session.scalar(text("SELECT clock_timestamp()"))
        # Synthetic preparation and admission use the same authoritative clock;
        # host/VM drift must not turn unrelated financial tests into future input.
        prepared["rebuilt_at"] = now.isoformat()
        models = [
            AgentModelConfigRecord(
                id=uuid4(),
                model_id="gpt-5.6-terra",
                version=f"fixture-comparison-{route}",
                config_hash=hash_,
                max_input_tokens=96000,
                max_output_tokens=8192,
                max_tool_calls=0,
                input_usd_per_million=Decimal("12.5"),
                output_usd_per_million=Decimal(0),
                pricing_available_at=now - timedelta(days=1),
                pricing_expires_at=now + timedelta(days=1),
            )
            for route, hash_ in [("sdk", SDK_HASH), ("api", "a" * 64)]
        ]
        session.add_all(models)
        prompts = {
            role: AgentPromptVersionRecord(id=uuid4(), **value)
            for role, value in archive["members"][0]["prompts"].items()
        }
        session.add_all(prompts.values())
        prompt_manifest = {
            role: {"id": str(row.id), "version": row.version, "content_hash": row.content_hash}
            for role, row in prompts.items()
        }
        for member in archive["members"]:
            request = member["request"]
            when = datetime.fromisoformat(request["decision_time"])
            security = member["inputs"]["security_snapshot"]
            ctx = security["independent_monitor_context"]
            base_payload = {
                "purpose": "research_only_v2",
                "as_of": when.isoformat(),
                "instrument_id": request["instrument_id"],
                "evidence_ids": [security["items"][0]["evidence_ref_id"]],
            }
            session.add(
                DataSnapshot(
                    id=UUID(ctx["source_snapshot_id"]),
                    source="nexus_research_only",
                    provider_object_id=request["instrument_id"],
                    revision=ctx["source_snapshot_hash"],
                    observed_at=when,
                    available_at=when,
                    received_at=when,
                    schema_version="research_only_v2",
                    payload=base_payload,
                )
            )
            original_payload = {
                "purpose": "independent_monitor_research_v1",
                "as_of": when.isoformat(),
                "instrument_id": request["instrument_id"],
                "independent_monitor_context": ctx,
            }
            session.add(
                Instrument(
                    id=UUID(request["instrument_id"]),
                    asset_type="equity",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal(".01"),
                    risk_bucket="equity",
                )
            )
            session.add(
                DataSnapshot(
                    id=UUID(request["data_snapshot_id"]),
                    source="nexus_independent_monitor_research",
                    provider_object_id=request["instrument_id"],
                    revision=hashlib.sha256(
                        json.dumps(original_payload, sort_keys=True).encode()
                    ).hexdigest(),
                    observed_at=when,
                    available_at=when,
                    received_at=when,
                    schema_version="independent_monitor_research_v1",
                    payload=original_payload,
                )
            )
            for row in _manifest(member):
                session.add(
                    ResearchEvidenceRef(
                        id=UUID(row["id"]),
                        source=row["source"],
                        provider_object_id=row["provider_object_id"],
                        revision=row["revision"],
                        available_at=datetime.fromisoformat(row["available_at"]),
                        location=row["location"],
                        content_hash=row["content_hash"],
                        byte_length=1,
                    )
                )
        await session.flush()
        if source_kind != "fixture":
            from nexus.persistence.models.market import InstrumentAlias
            from nexus.persistence.models.research import Filing

            for member in archive["members"]:
                when = datetime.fromisoformat(member["request"]["decision_time"])
                item = _manifest(member)[0]
                instrument = UUID(member["request"]["instrument_id"])
                symbol = member["inputs"]["price_close"]["items"][0]["symbol"]
                session.add(
                    InstrumentAlias(instrument_id=instrument, symbol=symbol, valid_from=when.date())
                )
                session.add(
                    Filing(
                        instrument_id=instrument,
                        evidence_ref_id=UUID(item["id"]),
                        source=item["source"],
                        provider_object_id=item["provider_object_id"],
                        accession_number=instrument.hex,
                        form="10-K",
                        filed_at=when,
                        accepted_at=when,
                        revision=item["revision"],
                    )
                )
            await session.flush()
        for member in archive["members"]:
            request = member["request"]
            evidence = _manifest(member)
            session.add(
                AgentResearchRunRecord(
                    id=UUID(member["run_id"]),
                    instrument_id=UUID(request["instrument_id"]),
                    data_snapshot_id=UUID(request["data_snapshot_id"]),
                    prompt_version_id=prompts["orchestrator"].id,
                    model_config_id=models[0].id,
                    decision_time=datetime.fromisoformat(request["decision_time"]),
                    actor_id="fixture-original",
                    idempotency_key=member["run_id"],
                    status="failed",
                    retry_count=0,
                    schema_hash="f" * 64,
                    model_hash=SDK_HASH,
                    prompt_hash=digest(prompt_manifest),
                    prompt_manifest=prompt_manifest,
                    evidence_hash=digest(evidence),
                    evidence_manifest=evidence,
                    tool_hash="f" * 64,
                    input_hash="f" * 64,
                    reserved_cost_usd=Decimal(2),
                    settled_cost_usd=None,
                    pricing_version=models[0].version,
                    input_tokens=None,
                    output_tokens=None,
                )
            )
        await session.flush()
        old = None
        if protect_unknown:
            old = await reserve_daily_budget(
                session,
                run_id=UUID(archive["members"][0]["run_id"]),
                now=now,
                daily_limit_usd=Decimal(4),
                worst_case_usd=Decimal(2),
                pricing_version=models[0].version,
                pricing_fresh=True,
            )
    return source, prepared, manifest, models[0].id, None if old is None else old.id


@pytest.mark.asyncio
async def test_producer_native_binding_budget_failure_and_reentry(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, manifest, model_id, old_id = await seed(factory)
    async with factory() as session, session.begin():
        # Caller catches the refusal and commits: the producer's savepoint must
        # still remove every partial identity and preserve the original reserve.
        with pytest.raises(BudgetExhausted):
            await admit_comparison_position(
                session,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=1,
                model_config_id=model_id,
                daily_limit_usd=Decimal(3),
            )
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 0
        assert (
            await session.scalar(
                select(func.count())
                .select_from(AgentResearchRunRecord)
                .where(AgentResearchRunRecord.actor_id == ACTOR)
            )
            == 0
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(DataSnapshot)
                .where(DataSnapshot.source == "nexus_research_comparison")
            )
            == 0
        )
    async with factory() as session, session.begin():
        result = await admit_comparison_position(
            session,
            source=source,
            prepared=prepared,
            manifest=manifest,
            ordinal=1,
            model_config_id=model_id,
            daily_limit_usd=Decimal(4),
        )
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="COMPARISON_POSITION_CONSUMED"):
            await admit_comparison_position(
                session,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=1,
                model_config_id=model_id,
                daily_limit_usd=Decimal(4),
            )
        run = await session.get(AgentResearchRunRecord, result.run_id)
        job = await session.get(Job, result.job_id)
        snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
        assert run.input_hash == manifest.input_hash_for(1)
        assert run.created_at > run.decision_time
        assert snapshot.received_at > snapshot.observed_at
        assert snapshot.payload["purpose"] == PURPOSE
        assert job.status == "queued" and job.attempt_count == 0 and job.retry_policy == "never"
        old = await session.get(AgentDailyBudgetReservationRecord, old_id)
        assert (old.state, old.reserved_usd, old.settled_usd) == ("reserved", Decimal(2), None)
        assert await session.scalar(
            select(func.sum(AgentDailyBudgetReservationRecord.reserved_usd))
        ) == Decimal("3.2")
        with pytest.raises(ValueError, match="COMPARISON_BUSINESS_CONSUMPTION_FORBIDDEN"):
            await require_formal_research(session, run)
    with pytest.raises(DBAPIError, match="COMPARISON_DISPATCH_NOT_ENABLED"):
        async with factory() as session, session.begin():
            await session.execute(
                text("UPDATE jobs SET status='running',attempt_count=1 WHERE id=:id"),
                {"id": result.job_id},
            )


@pytest.mark.asyncio
async def test_two_producers_cannot_duplicate_one_position(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, manifest, model_id, _ = await seed(factory)
    barrier = asyncio.Barrier(2)

    async def produce():
        async with factory() as session, session.begin():
            await barrier.wait()
            return await admit_comparison_position(
                session,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=1,
                model_config_id=model_id,
                daily_limit_usd=Decimal(4),
            )

    results = await asyncio.gather(produce(), produce(), return_exceptions=True)
    assert sum(not isinstance(r, BaseException) for r in results) == 1
    failures = [r for r in results if isinstance(r, BaseException)]
    assert len(failures) == 1 and str(failures[0]) == "COMPARISON_POSITION_CONSUMED"
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 1
        assert await session.scalar(
            select(func.sum(AgentDailyBudgetReservationRecord.reserved_usd))
        ) == Decimal("3.2")


@pytest.mark.asyncio
async def test_normal_admit_and_read_only_receipt_after_missing_client_ack(
    migrated_database_url, tmp_path, monkeypatch
):
    settings = EngineSettings(database_url=migrated_database_url, agent_daily_budget_usd=Decimal(4))
    factory = session_factory(settings)
    source, prepared, manifest, model_id, _ = await seed(factory)

    def configured(root, *, load_model_key):
        assert root == tmp_path and load_model_key is False
        return settings

    monkeypatch.setattr("nexus.ops.openai_local.local_settings", configured)
    source_path, prepared_path, manifest_path = [
        tmp_path / name for name in ("source.json", "prepared.json", "manifest.json")
    ]
    source_path.write_bytes(source)
    prepared_path.write_text(json.dumps(prepared))
    manifest_path.write_text(manifest.model_dump_json())
    args = Namespace(
        command="admit-position",
        repo_root=tmp_path,
        source=source_path,
        prepared=prepared_path,
        manifest=manifest_path,
        position=1,
        model_config_id=model_id,
    )
    await native_command(args)  # Deliberately discard the client receipt; never re-submit.
    receipt = await native_command(
        Namespace(
            command="inspect-position",
            repo_root=tmp_path,
            experiment_id=manifest.experiment_id,
            position=1,
        )
    )
    assert receipt["inspection_only"] is True and receipt["status"] == "position_registered"
    assert receipt["stage_route"] == "comparison"
    assert receipt["stage_version"] == "research-stages-v1"
    assert receipt["stage_scope_stopped"] is False
    assert receipt["dispatch_authority"] is False
    assert (
        receipt["job_status"],
        receipt["run_status"],
        receipt["budget_state"],
        receipt["attempt_count"],
    ) == ("queued", "prepared", "reserved", 0)
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 1
        assert await session.scalar(
            select(func.sum(AgentDailyBudgetReservationRecord.reserved_usd))
        ) == Decimal("3.2")


@pytest.mark.asyncio
async def test_exact_context_binding_and_compatible_reader_rollback(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, manifest, model_id, _ = await seed(factory)
    async with factory() as session, session.begin():
        result = await admit_comparison_position(
            session,
            source=source,
            prepared=prepared,
            manifest=manifest,
            ordinal=1,
            model_config_id=model_id,
            daily_limit_usd=Decimal(4),
        )
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, result.run_id)
        snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
        links = list(
            await session.scalars(
                select(AgentDataSnapshotEvidenceRecord).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot.id
                )
            )
        )
        own = next(link for link in links if link.source == "nexus_independent_monitor_research")
        snapshot_values = {
            column.key: getattr(snapshot, column.key) for column in DataSnapshot.__table__.columns
        }
        link_values = {
            column.key: getattr(own, column.key)
            for column in AgentDataSnapshotEvidenceRecord.__table__.columns
        }
    for fault in ("input_hash", "source", "instrument"):
        with pytest.raises(DBAPIError, match="Agent snapshot evidence is not owned"):
            async with factory() as session, session.begin():
                values = copy.deepcopy(snapshot_values)
                values["id"] = uuid4()
                values["revision"] = str(uuid4())
                values["payload"]["frozen"]["request"]["data_snapshot_id"] = str(values["id"])
                if fault == "input_hash":
                    values["payload"]["input_hash"] = "0" * 64
                elif fault == "source":
                    values["source"] = "unregistered-experiment"
                else:
                    values["payload"]["frozen"]["request"]["instrument_id"] = str(uuid4())
                session.add(DataSnapshot(**values))
                await session.flush()
                session.add(
                    AgentDataSnapshotEvidenceRecord(
                        **{**link_values, "data_snapshot_id": values["id"]}
                    )
                )
                await session.flush()
    with pytest.raises(RuntimeError, match="COMPARISON_COMPATIBLE_READER_ROLLBACK_REQUIRED"):
        await asyncio.to_thread(
            command.downgrade, Config("engine/alembic.ini"), "0100_research_comparison"
        )
    async with factory() as session:
        assert (
            await session.scalar(text("SELECT version_num FROM alembic_version"))
            == "0104_comparison_chain_stop"
        )
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 1


@pytest.mark.asyncio
async def test_empty_input_migration_roundtrip_preserves_original_guard(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    query = text(
        "SELECT pg_get_functiondef('public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
    )
    async with factory() as session:
        new = await session.scalar(query)
    await asyncio.to_thread(
        command.downgrade, Config("engine/alembic.ini"), "0100_research_comparison"
    )
    async with factory() as session:
        original = await session.scalar(query)
        assert "nexus_research_comparison" not in original
        assert "Agent price evidence authority is invalid" in original
        assert "JOIN public.manual_paper_requests request" in original
    await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), "head")
    async with factory() as session:
        assert await session.scalar(query) == new


@pytest.mark.asyncio
async def test_future_preparation_is_rejected_without_consuming_position(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, manifest, model_id, old_id = await seed(factory)
    async with factory() as session:
        now = await session.scalar(text("SELECT clock_timestamp()"))
    prepared["rebuilt_at"] = (now + timedelta(days=1)).isoformat()
    with pytest.raises(ValueError, match="COMPARISON_PREPARATION_CLOCK_INVALID"):
        async with factory.begin() as session:
            await admit_comparison_position(
                session,
                source=source,
                prepared=prepared,
                manifest=manifest,
                ordinal=1,
                model_config_id=model_id,
                daily_limit_usd=Decimal(4),
            )
    async with factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 0
        old = await session.get(AgentDailyBudgetReservationRecord, old_id)
        assert (old.state, old.reserved_usd, old.settled_usd) == ("reserved", Decimal(2), None)
    await factory.kw["bind"].dispose()
