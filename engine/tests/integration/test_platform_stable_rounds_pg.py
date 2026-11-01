"""Stable normal registration through the real Worker; only provider HTTP is synthetic."""

import asyncio
import hashlib
import json
from argparse import Namespace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from integration.test_comparison_execution_pg import stage_response
from integration.test_platform_funding_pg import funding_fixture
from integration.test_platform_producer_pg import setup
from nexus.agents import comparison_execution, platform_profile
from nexus.agents.chatgpt_grant import GrantStore
from nexus.agents.comparison_execution import execute_comparison_position
from nexus.agents.comparison_manifest import parse_comparison_manifest
from nexus.agents.platform_schema import CURRENT_PLATFORM_UNITS_VERSION
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.ops import openai_local, platform_round_registration
from nexus.ops.research_comparison import native_command
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from unit.agents.test_http_stage_lifecycle import Body, terminal


@pytest_asyncio.fixture
async def stable_environment(migrated_database_url, monkeypatch, tmp_path, request):
    settings = EngineSettings(
        database_url=migrated_database_url, agent_daily_budget_usd=Decimal(20)
    )
    factory = session_factory(settings)
    if getattr(request, "param", None) == "wide_sdk":
        from integration import test_comparison_producer_pg as legacy_fixtures
        from nexus.agents.committee import INDEPENDENT_MONITOR_PROMPT_VERSION

        original_archive = legacy_fixtures.archive_source

        def wide_archive():
            archive = original_archive()
            member = archive["members"][0]
            for prompt in member["prompts"].values():
                prompt["version"] = INDEPENDENT_MONITOR_PROMPT_VERSION
            member["inputs"]["filing_disclosures"]["items"] = [{"excerpt": "x" * 50000}]
            return archive

        monkeypatch.setattr(legacy_fixtures, "archive_source", wide_archive)
    try:
        with monkeypatch.context() as legacy_seed:
            if getattr(request, "param", None) == "wide_sdk":
                from nexus.agents import comparison_preparation

                legacy_seed.setattr(
                    comparison_preparation, "validate_comparison_capacity", lambda _: None
                )
            source, _, reference, model, old, protected = await setup(
                factory,
                monkeypatch,
                source_kind="SEC",
                price_version=CURRENT_PLATFORM_PRICE_VERSION,
                units_version=CURRENT_PLATFORM_UNITS_VERSION,
            )
        reference, _ = await funding_fixture(
            factory, reference, tmp_path, monkeypatch, explicit_spend=True
        )
        # All clocks/prices below are isolated fixture values, not a live-price exemption.
        monkeypatch.setattr(
            platform_round_registration,
            "platform_model_fields",
            platform_profile.platform_model_fields,
        )
        actual_settings = openai_local.local_settings
        monkeypatch.setattr(
            openai_local,
            "local_settings",
            lambda *a, **k: settings
            if k.get("load_model_key") is False
            else actual_settings(*a, **k),
        )

        async def forbidden(*a, **k):
            raise AssertionError("No SDK or provider grant fallback")

        from nexus.agents import codex_staged

        monkeypatch.setattr(codex_staged, "invoke_codex", forbidden)
        monkeypatch.setattr(GrantStore, "load_verified", forbidden)

        def private(name, raw):
            path = tmp_path / name
            path.write_bytes(raw)
            path.chmod(0o600)
            return path

        source_path = private("source.json", source)
        template = private("template.json", reference.model_dump_json().encode())
        policy = private(
            "task-policy.json", b"Synthetic bounded task policy, multiple independent rounds."
        )
        diagnosis = private(
            "diagnosis.txt", b"Synthetic fix and qualification evidence; no blind retry."
        )

        def registration(identity=None, output=None):
            identity = identity or uuid4()
            return Namespace(
                command="register-round",
                repo_root=tmp_path,
                source=source_path,
                template=template,
                task_policy=policy,
                continuation_evidence=diagnosis,
                source_sha256=hashlib.sha256(source).hexdigest(),
                task_policy_sha256=hashlib.sha256(policy.read_bytes()).hexdigest(),
                implementation_sha="f" * 40,
                experiment_id=identity,
                output=output or tmp_path / str(identity),
            )

        def position(registration, ordinal=1):
            return Namespace(
                command="admit-position",
                repo_root=tmp_path,
                source=source_path,
                prepared=registration.output / "prepared.json",
                manifest=registration.output / "manifest.json",
                funding_proof=registration.output / "funding.json",
                position=ordinal,
                model_config_id=model,
                worker_id="stable-platform-native",
            )

        yield Namespace(
            factory=factory,
            root=tmp_path,
            registration=registration,
            position=position,
            old=old,
            protected=protected,
            source=source,
        )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first_unknown", [False, True], ids=["two-complete-rounds", "closed-unknown-retained"]
)
async def test_same_deployment_normal_rounds_execute_and_preserve_prior_facts(
    stable_environment, monkeypatch, first_unknown
):
    env = stable_environment
    seen, clients, wires = [], [], []
    entered, released = asyncio.Event(), asyncio.Event()

    async def handle(request):
        async with env.factory() as session:
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        "AND pid<>pg_backend_pid() AND xact_start IS NOT NULL"
                    )
                )
                == 0
            )
        assert request.headers["authorization"] == "Bearer synthetic-platform-native-funding"
        wire = json.loads(request.content)
        wires.append(wire)
        payload = json.loads(wire["input"][1]["content"])
        if not seen and first_unknown:
            seen.append(("platform", payload["stage"]))
            return httpx.Response(
                400,
                headers={"content-type": "application/json"},
                stream=Body(b'{"error":{"code":"insufficient_quota"}}'),
            )
        if not seen:
            entered.set()
            await released.wait()
        answer = stage_response(payload, "platform", seen)
        event = terminal("completed")
        event["response"]["service_tier"] = "default"
        event["response"]["output"][0]["content"][0]["text"] = json.dumps(answer)
        event["response"]["usage"] = {
            "input_tokens": 300,
            "output_tokens": 80,
            "total_tokens": 380,
            "input_tokens_details": {
                "cached_tokens": 100,
                "cache_write_tokens": 100,
                "cache_write_tokens_details": {"ttl_30m_tokens": 100, "ttl_12h_tokens": 0},
            },
            "output_tokens_details": {"reasoning_tokens": 60},
        }
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Body(("data: " + json.dumps(event) + "\n\n").encode()),
        )

    def client_factory():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        clients.append(client)
        return client

    async def execute(**kwargs):
        return await execute_comparison_position(**kwargs, client_factory=client_factory)

    monkeypatch.setattr(comparison_execution, "execute_comparison_position", execute)
    first, second = env.registration(), env.registration()
    for item in (first, second):
        result = await native_command(item)
        assert result["status"] == "registered" and result["position_count"] == 0
        assert not result["dispatch_authority"]

    async def facts(identity):
        async with env.factory() as session:
            return (
                await session.scalars(
                    text("""SELECT jsonb_build_object('chain',to_jsonb(c),
              'run',to_jsonb(r),'job',to_jsonb(j),'budget',to_jsonb(b),
              'settled_usd',b.settled_usd::text)
              FROM research_comparison_chain c JOIN agent_research_runs r ON r.id=c.run_id
              JOIN jobs j ON j.id=c.job_id
              JOIN agent_daily_budget_reservations b ON b.run_id=c.run_id
              WHERE c.experiment_id=:id ORDER BY c.ordinal"""),
                    {"id": identity},
                )
            ).all()

    first_facts = None
    for round_index, registration in enumerate((first, second)):
        for ordinal in (1, 2, 3):
            args = env.position(registration, ordinal)
            admitted = await native_command(args)
            assert admitted["status"] == "position_prepared"
            args.command = "run-position"
            if round_index == 0 and ordinal == 1 and not first_unknown:
                task = asyncio.create_task(native_command(args))
                try:
                    await asyncio.wait_for(entered.wait(), 60)
                    async with env.factory() as session:
                        assert (
                            await session.scalar(
                                text("""SELECT count(*) FROM job_leases l
                          JOIN research_comparison_chain c ON c.job_id=l.job_id
                          WHERE c.experiment_id=:id AND l.expires_at>clock_timestamp()"""),
                                {"id": first.experiment_id},
                            )
                            == 1
                        )
                    with pytest.raises(ValueError, match="COMPARISON_OTHER_WORK_UNCONFIRMED"):
                        await native_command(env.position(second))
                finally:
                    released.set()
                reply = await asyncio.wait_for(task, 60)
            else:
                reply = await native_command(args)
            assert reply["status"] == "position_executor_returned" and not reply["replay_allowed"]
            async with env.factory() as session:
                proof = await read_terminal_proof(
                    session, run_id=UUID(admitted["run_id"]), now=datetime.now(UTC)
                )
                assert proof is not None and proof.cleanup_confirmed
                unknown = round_index == 0 and first_unknown
                assert proof.known is (not unknown) and proof.budget_settled is (not unknown)
            before = len(seen)
            with pytest.raises(ValueError, match="COMPARISON_POSITION_CONSUMED|COMPARISON_STOPPED"):
                await native_command(args)
            assert len(seen) == before
            if round_index == 0 and first_unknown:
                break
        if round_index == 0:
            first_facts = await facts(first.experiment_id)
            async with env.factory() as session:
                # Consumed terminal leases can still be chronologically unexpired.
                assert (
                    await session.scalar(
                        text("""SELECT count(*) FROM job_leases l
                  JOIN research_comparison_chain c ON c.job_id=l.job_id
                  WHERE c.experiment_id=:id AND l.expires_at>clock_timestamp()"""),
                        {"id": first.experiment_id},
                    )
                    > 0
                )
    assert await facts(first.experiment_id) == first_facts
    all_facts = first_facts + await facts(second.experiment_id)
    for row in all_facts:
        assert row["job"]["attempt_count"] == 1
        if row["budget"]["state"] == "settled":
            assert Decimal(row["settled_usd"]) == Decimal("0.00429")
        else:
            assert first_unknown and row["budget"]["settled_usd"] is None
    assert len(seen) == len(clients) == (10 if first_unknown else 18)
    assert all(client.is_closed for client in clients)
    assert [wire["max_output_tokens"] for wire in wires] == (
        [8192] if first_unknown else [8192, 8112, 8032] * 3
    ) + [8192, 8112, 8032] * 3
    async with env.factory() as session:
        assert (
            await session.scalar(text("SELECT version_num FROM alembic_version"))
            == CURRENT_MIGRATION_HEAD
        )
        assert (
            await session.scalar(
                text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                {"id": env.old},
            )
            == env.protected
        )


@pytest.mark.asyncio
async def test_parallel_registration_and_distinct_round_admission_share_native_lock(
    stable_environment,
):
    env = stable_environment
    first, second = env.registration(), env.registration()
    alternate = env.registration(first.experiment_id, env.root / "alternate-export")
    results = await asyncio.gather(native_command(first), native_command(alternate))
    assert {result["status"] for result in results} <= {"registered", "registration_unconfirmed"}
    assert any(result["status"] == "registered" for result in results)
    assert len({result["manifest_hash"] for result in results}) == 1
    for name in ("round.json", "manifest.json", "prepared.json", "funding.json"):
        assert (first.output / name).read_bytes() == (alternate.output / name).read_bytes()
    assert (await native_command(second))["status"] == "registered"
    outcomes = await asyncio.gather(
        native_command(env.position(first)),
        native_command(env.position(second)),
        return_exceptions=True,
    )
    assert (
        sum(isinstance(item, dict) and item["status"] == "position_prepared" for item in outcomes)
        == 1
    )
    rejected = [item for item in outcomes if isinstance(item, Exception)]
    assert len(rejected) == 1 and str(rejected[0]) == "COMPARISON_OTHER_WORK_UNCONFIRMED"
    async with env.factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 2
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 1
        assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
        assert (
            await session.scalar(
                text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                {"id": env.old},
            )
            == env.protected
        )


@pytest.mark.asyncio
async def test_interrupted_file_publication_restores_original_bundle_before_registration(
    stable_environment, monkeypatch
):
    env = stable_environment
    args = env.registration()
    original = platform_round_registration._export

    def interrupt(path, raw):
        if path.name == "funding.json":
            raise OSError("synthetic local publication interruption")
        return original(path, raw)

    monkeypatch.setattr(platform_round_registration, "_export", interrupt)
    with pytest.raises(OSError, match="synthetic local publication"):
        await native_command(args)
    frozen = (args.output / "round.json").read_bytes()
    async with env.factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 0
    assert not (args.output / "registration-attempt.json").exists()

    def forbidden(*a, **k):
        raise AssertionError("Cannot regenerate the frozen bundle")

    monkeypatch.setattr(platform_round_registration, "_freeze", forbidden)
    monkeypatch.setattr(platform_round_registration, "_export", original)
    assert (await native_command(args))["status"] == "registered"
    assert (args.output / "round.json").read_bytes() == frozen
    assert (args.output / "funding.json").exists()


async def _sdk_position(env, platform_args, platform_manifest):
    from nexus.agents.comparison_manifest import SDK_HASH, ComparisonManifest
    from nexus.agents.comparison_preparation import prepare_archive

    sdk_prepared = prepare_archive(
        env.source,
        expected_source_hash=hashlib.sha256(env.source).hexdigest(),
        experiment_id=uuid4(),
    )
    async with env.factory() as session:
        sdk_prepared["rebuilt_at"] = (
            await session.scalar(text("SELECT clock_timestamp()"))
        ).isoformat()
        sdk_model = await session.scalar(
            text("SELECT id FROM agent_model_configs WHERE config_hash=:hash"), {"hash": SDK_HASH}
        )
    sdk_data = {
        key: platform_manifest.model_dump(mode="json")[key]
        for key in ComparisonManifest.model_fields
    }
    sdk_data.update(
        {
            key: sdk_prepared[key]
            for key in (
                "experiment_id",
                "source_manifest_hash",
                "identity_equivalence_hash",
                "outbound_safety_hash",
                "input_hashes",
                "instrument_ids",
            )
        }
    )
    sdk_data["api_config_hash"] = "a" * 64
    sdk_manifest = ComparisonManifest.model_validate(sdk_data)
    sdk_dir = env.root / "legacy-sdk"
    sdk_dir.mkdir()
    for name, value in [
        ("manifest", sdk_manifest.model_dump(mode="json")),
        ("prepared", sdk_prepared),
    ]:
        path = sdk_dir / (name + ".json")
        path.write_text(json.dumps(value))
        path.chmod(0o600)
    sdk_args = Namespace(
        **{
            **vars(platform_args),
            "manifest": sdk_dir / "manifest.json",
            "prepared": sdk_dir / "prepared.json",
            "funding_proof": None,
            "model_config_id": sdk_model,
        }
    )
    return sdk_args


@pytest.mark.asyncio
@pytest.mark.parametrize("first_kind", ["platform", "sdk"])
async def test_legacy_sdk_and_stable_api_share_root_claim_and_send_fence(
    stable_environment, monkeypatch, first_kind
):
    from datetime import timedelta
    from functools import partial

    from nexus.agents import comparison_concurrency
    from nexus.agents.comparison_claim import claim_comparison_position
    from nexus.agents.platform_funding import load_bound_platform_funding
    from nexus.agents.research_stages.protocol import StagedProtocol

    env = stable_environment
    registration = env.registration()
    await native_command(registration)
    platform_args = env.position(registration)
    platform_manifest = parse_comparison_manifest(json.loads(platform_args.manifest.read_bytes()))
    sdk_args = await _sdk_position(env, platform_args, platform_manifest)
    first, second = (
        (platform_args, sdk_args) if first_kind == "platform" else (sdk_args, platform_args)
    )
    selected = parse_comparison_manifest(json.loads(first.manifest.read_bytes()))
    await native_command(first)
    with pytest.raises(ValueError, match="COMPARISON_OTHER_WORK_UNCONFIRMED"):
        await native_command(second)

    async def old_version_without_cross_round_guard(*a, **k):
        pass

    # Seed only the backlog/claim that the earlier deployed version could create.
    # All three assertions below use the real new guard, never this fixture bypass.
    with monkeypatch.context() as historical:
        historical.setattr(
            comparison_concurrency,
            "require_comparison_exclusive",
            old_version_without_cross_round_guard,
        )
        await native_command(second)
    loader = (
        partial(
            load_bound_platform_funding, env.root, platform_args.funding_proof, platform_manifest
        )
        if first_kind == "platform"
        else None
    )

    async def claim():
        return await claim_comparison_position(
            env.factory,
            experiment_id=selected.experiment_id,
            ordinal=1,
            worker_id="historical-claim-fixture",
            platform_funding_loader=loader,
        )

    with pytest.raises(ValueError, match="COMPARISON_OTHER_WORK_UNCONFIRMED"):
        await claim()
    with monkeypatch.context() as historical:
        historical.setattr(
            comparison_concurrency,
            "require_comparison_exclusive",
            old_version_without_cross_round_guard,
        )
        claimed = await claim()
    now = claimed.expires_at - timedelta(seconds=330)
    protocol = StagedProtocol(env.factory)
    await protocol.install(
        scope_id=selected.experiment_id,
        claims=(claimed,),
        now=now,
        work_deadline=now + timedelta(seconds=300),
        cleanup_deadline=now + timedelta(seconds=307),
        comparison=True,
    )
    with pytest.raises(ValueError, match="COMPARISON_OTHER_WORK_UNCONFIRMED"):
        await protocol.authorize(claimed, stage=1, now=now, wire=b"never-sent")
    async with env.factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
        assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 1
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 2


@pytest.mark.asyncio
async def test_round_inspection_does_not_require_local_policy_or_key(
    stable_environment, monkeypatch
):
    env = stable_environment
    args = env.registration()
    registered = await native_command(args)

    def forbidden(*a, **k):
        raise AssertionError("Historical inspection cannot consume local funding or credentials")

    from nexus.agents import research_maintenance

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    monkeypatch.setattr(platform_round_registration, "_private_bytes", forbidden)
    monkeypatch.setattr(platform_round_registration, "_resolved_key", forbidden)
    new_round = env.registration()
    with pytest.raises(ValueError, match="^RESEARCH_MAINTENANCE$"):
        await native_command(new_round)
    assert not new_round.output.exists()
    result = await native_command(
        Namespace(command="inspect-round", repo_root=env.root, experiment_id=args.experiment_id)
    )
    assert result["manifest_hash"] == registered["manifest_hash"]
    assert result["inspection_only"] and not result["dispatch_authority"]
    assert result["position_count"] == 0 and result["model_calls"] == 0
    absent = await native_command(
        Namespace(command="inspect-round", repo_root=env.root, experiment_id=uuid4())
    )
    assert absent["status"] == "registration_unconfirmed"
    assert absent["manifest_hash"] is None and absent["budget_status"] == "not_observed"
    async with env.factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("stable_environment", ["wide_sdk"], indirect=True)
@pytest.mark.parametrize("damage", [None, "missing_audit", "skipped"])
async def test_normal_preclaim_history_allows_next_stable_root_without_rewriting_old_facts(
    stable_environment, monkeypatch, damage
):
    from nexus.agents import comparison_preparation

    env = stable_environment
    registration = env.registration()
    await native_command(registration)
    platform_args = env.position(registration)
    manifest = parse_comparison_manifest(json.loads(platform_args.manifest.read_bytes()))
    # Reproduce the original pre-capacity-check producer, then restore every
    # current check before the normal preclaim close and new stable admission.
    with monkeypatch.context() as historical:
        historical.setattr(comparison_preparation, "validate_comparison_capacity", lambda _: None)
        sdk_args = await _sdk_position(env, platform_args, manifest)
        old = await native_command(sdk_args)
    close = Namespace(**{**vars(sdk_args), "command": "close-preclaim"})
    assert (await native_command(close))["status"] == "preclaim_closed"

    async def historical_facts():
        async with env.factory() as session:
            return (
                await session.execute(
                    text("""SELECT to_jsonb(j),to_jsonb(r),to_jsonb(b)
                      FROM jobs j JOIN agent_research_runs r ON r.id=:run
                      JOIN agent_daily_budget_reservations b ON b.run_id=r.id
                      WHERE j.id=:job"""),
                    {"run": UUID(old["run_id"]), "job": UUID(old["job_id"])},
                )
            ).one()

    if damage:
        # Only construct an invalid historical archive in this isolated fixture;
        # all application checks are restored for the actual successor admission.
        async with env.factory.begin() as session:
            await session.execute(text("SET LOCAL session_replication_role='replica'"))
            if damage == "missing_audit":
                await session.execute(
                    text(
                        "DELETE FROM audit_events "
                        "WHERE action='research.comparison.preclaim_closed'"
                    ),
                )
            else:
                await session.execute(
                    text("UPDATE jobs SET status='skipped',started_at=finished_at WHERE id=:job"),
                    {"job": UUID(old["job_id"])},
                )
    frozen = await historical_facts()
    assert frozen[0]["attempt_count"] == 0 and frozen[2]["state"] == "released"
    if damage:
        with pytest.raises(ValueError, match="COMPARISON_OTHER_WORK_UNCONFIRMED"):
            await native_command(platform_args)
    else:
        assert (await native_command(platform_args))["status"] == "position_prepared"
    assert await historical_facts() == frozen
    async with env.factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
        assert await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
        assert (
            await session.scalar(
                text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                {"id": env.old},
            )
            == env.protected
        )


@pytest.mark.asyncio
async def test_hard_exit_publishing_attempt_cannot_grant_registration_in_another_directory(
    stable_environment, monkeypatch
):
    import subprocess
    import sys

    env = stable_environment
    args = env.registration()
    publish = platform_round_registration._publish

    def stop_before_attempt(path, raw):
        if path.name == "registration-attempt.json":
            raise OSError("synthetic stop at publication boundary")
        return publish(path, raw)

    with monkeypatch.context() as boundary:
        boundary.setattr(platform_round_registration, "_publish", stop_before_attempt)
        with pytest.raises(OSError, match="synthetic stop"):
            await native_command(args)
    manifest = parse_comparison_manifest(json.loads((args.output / "manifest.json").read_bytes()))
    attempt = (
        env.root
        / ".secrets/nexus-platform-local/qualification-rounds"
        / str(args.experiment_id)
        / "registration-attempt.json"
    )
    code = """
import os,sys
from pathlib import Path
from nexus.ops import platform_round_registration as m
original=m.os.link
def crash(*a,**kw):
    original(*a,**kw)
    os._exit(71)
m.os.link=crash
m._publish(Path(sys.argv[1]),sys.argv[2].encode())
"""
    wire = json.dumps(
        {"experiment_id": str(args.experiment_id), "manifest_hash": manifest.digest},
        sort_keys=True,
        separators=(",", ":"),
    )
    result = subprocess.run([sys.executable, "-c", code, str(attempt), wire], timeout=60)
    assert result.returncode == 71 and attempt.stat().st_nlink == 2

    def forbidden(*a, **k):
        raise AssertionError("A published attempt cannot grant a new freeze or database INSERT")

    monkeypatch.setattr(platform_round_registration, "_freeze", forbidden)
    monkeypatch.setattr(platform_round_registration, "register_comparison", forbidden)
    args.output = env.root / "another-export-after-process-exit"
    result = await native_command(args)
    assert result["status"] == "registration_unconfirmed" and not result["replay_allowed"]
    assert result["manifest_hash"] == manifest.digest
    assert attempt.stat().st_nlink == 1 and attempt.read_text() == wire
    async with env.factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 0
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_bundle", "conflicting_attempt"])
async def test_retained_attempt_never_refreezes_or_loses_its_manifest_binding(
    stable_environment, monkeypatch, damage
):
    from sqlalchemy.ext.asyncio import AsyncSessionTransaction

    env = stable_environment
    args = env.registration()

    async def unconfirmed_commit(transaction):
        raise ConnectionError("synthetic unapplied commit with lost acknowledgement")

    with monkeypatch.context() as uncertain:
        uncertain.setattr(AsyncSessionTransaction, "commit", unconfirmed_commit)
        assert (await native_command(args))["status"] == "registration_unconfirmed"
    authority = (
        env.root / ".secrets/nexus-platform-local/qualification-rounds" / str(args.experiment_id)
    )
    attempt, bundle = authority / "registration-attempt.json", authority / "round.json"
    if damage == "missing_bundle":
        bundle.unlink()
        expected = "PLATFORM_ROUND_LOCAL_BUNDLE_REQUIRED"
    else:
        attempt.write_bytes(b'{"experiment_id":"wrong-identity","manifest_hash":"wrong-hash"}')
        expected = "PLATFORM_ROUND_BUNDLE_CONFLICT"
    frozen = attempt.read_bytes()

    def forbidden(*a, **k):
        raise AssertionError("A retained attempt forbids re-freeze and database INSERT")

    monkeypatch.setattr(platform_round_registration, "_freeze", forbidden)
    monkeypatch.setattr(platform_round_registration, "register_comparison", forbidden)
    with pytest.raises(ValueError, match=expected):
        await native_command(args)
    assert attempt.read_bytes() == frozen
    assert bundle.exists() is (damage != "missing_bundle")
    async with env.factory() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("sync_failure", [False, True])
async def test_authority_directory_names_are_durable_before_database_registration(
    stable_environment, monkeypatch, sync_failure
):
    import os
    import stat

    env = stable_environment
    args = env.registration()
    authority = (
        env.root / ".secrets/nexus-platform-local/qualification-rounds" / str(args.experiment_id)
    )
    required = {env.root, *list(authority.parents)[:3]}
    assert not authority.exists()
    observed = set()
    original_sync = os.fsync
    original_register = platform_round_registration.register_comparison
    db_attempts = 0

    def sync(fd):
        info = os.fstat(fd)
        if stat.S_ISDIR(info.st_mode):
            for directory in required:
                if directory.exists() and directory.stat().st_ino == info.st_ino:
                    if sync_failure and directory == authority.parent:
                        raise OSError("synthetic parent directory sync failure")
                    original_sync(fd)
                    observed.add(directory)
                    return
        original_sync(fd)

    async def register(*a, **k):
        nonlocal db_attempts
        assert observed == required, "Authority ancestor names must be durable before DB attempt"
        db_attempts += 1
        return await original_register(*a, **k)

    monkeypatch.setattr(os, "fsync", sync)
    monkeypatch.setattr(platform_round_registration, "register_comparison", register)
    if sync_failure:
        with pytest.raises(OSError, match="synthetic parent directory sync failure"):
            await native_command(args)
        assert db_attempts == 0
        async with env.factory() as session:
            assert (
                await session.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 0
            )
    else:
        assert (await native_command(args))["status"] == "registered"
        assert db_attempts == 1
        marker = authority / "registration-attempt.json"
        frozen = marker.read_bytes()
        # Even on reentry, a failed parent sync cannot remove or renew a marker.
        sync_failure = True
        with pytest.raises(OSError, match="synthetic parent directory sync failure"):
            await native_command(args)
        assert marker.read_bytes() == frozen and db_attempts == 1
