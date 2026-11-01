"""Native preparation/transport/settlement boundaries using synthetic frozen identities.

HTTP and identity artifact storage are controlled edges. Observation wrapper,
registry, evidence manifests, native claim and terminal facts are real PG paths.
Real archived identity artifacts are separately checked read-only in acceptance.
"""

import json
from argparse import Namespace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from integration.test_comparison_execution_pg import stage_response
from integration.test_platform_funding_pg import funding_fixture
from integration.test_platform_producer_pg import setup
from nexus.agents import comparison_execution, execution
from nexus.agents.research_stages import executor
from nexus.ops import openai_local
from nexus.ops.research_comparison import native_command
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from unit.agents.test_http_stage_lifecycle import Body, terminal


async def _admit_stable_successor(factory, monkeypatch, root, source, template):
    """Normal new-round registration and root reserve after the retained old defect."""
    import hashlib
    from datetime import timedelta
    from uuid import uuid4

    from nexus.agents import platform_profile
    from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION
    from nexus.ops import platform_round_registration

    async with factory.begin() as session:
        now = await session.scalar(text("SELECT clock_timestamp()"))
        fields = platform_profile.platform_model_fields(CURRENT_PLATFORM_PRICE_VERSION)
        fields.update(
            pricing_available_at=now - timedelta(days=1), pricing_expires_at=now + timedelta(days=1)
        )
        original = platform_profile.platform_model_fields
        monkeypatch.setattr(
            platform_profile,
            "platform_model_fields",
            lambda version=CURRENT_PLATFORM_PRICE_VERSION: fields
            if version == CURRENT_PLATFORM_PRICE_VERSION
            else original(version),
        )
        monkeypatch.setattr(
            platform_round_registration,
            "platform_model_fields",
            platform_profile.platform_model_fields,
        )
        model = await platform_profile.register_platform_profile(
            session, version=CURRENT_PLATFORM_PRICE_VERSION
        )
        model_id = model.id

    def private(name, data):
        path = root / name
        path.write_bytes(data)
        path.chmod(0o600)
        return path

    source_path = private("successor-source.json", source)
    template_path = private("successor-template.json", template.model_dump_json().encode())
    policy = private("successor-policy.txt", b"Synthetic bounded continuation policy")
    diagnosis = private("successor-diagnosis.txt", b"Exact historical no-dispatch proof")
    identity = uuid4()
    registration = Namespace(
        command="register-round",
        repo_root=root,
        source=source_path,
        template=template_path,
        task_policy=policy,
        continuation_evidence=diagnosis,
        source_sha256=hashlib.sha256(source).hexdigest(),
        task_policy_sha256=hashlib.sha256(policy.read_bytes()).hexdigest(),
        implementation_sha="f" * 40,
        experiment_id=identity,
        output=root / str(identity),
    )
    assert (await native_command(registration))["status"] == "registered"
    admitted = await native_command(
        Namespace(
            command="admit-position",
            repo_root=root,
            source=source_path,
            prepared=registration.output / "prepared.json",
            manifest=registration.output / "manifest.json",
            funding_proof=registration.output / "funding.json",
            model_config_id=model_id,
            position=1,
            worker_id="successor-after-historical-isolation",
        )
    )
    async with factory() as session:
        assert (
            await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                {"run": UUID(admitted["run_id"])},
            )
            == "reserved"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "observation",
        "before_send",
        "after_send",
        "after_send_unknown",
        "missing_observation",
        "old_bad_terminal",
        "old_bad_terminal_known_revision",
        "member_without_authorization",
    ],
)
async def test_native_authority_preparation_and_terminal_remain_consistent(
    migrated_database_url, monkeypatch, tmp_path, fault
):
    settings = EngineSettings(
        database_url=migrated_database_url, agent_daily_budget_usd=Decimal(20)
    )
    factory = session_factory(settings)
    source, prepared, manifest, model, held, protected = await setup(
        factory,
        monkeypatch,
        source_kind="research_observation_identity"
        if fault in {"observation", "missing_observation"}
        else "SEC",
    )
    if fault == "old_bad_terminal_known_revision":
        manifest = manifest.model_copy(
            update={"implementation_sha": "28e5ff2fb4770ea415a599c0fa3d3763f1a899bf"}
        )
    manifest, _ = await funding_fixture(factory, manifest, tmp_path, monkeypatch)
    if fault.startswith("old_bad_terminal"):
        # The broad fixture freezes creation defaults at 2026-01-01. This
        # historical lifecycle case needs native event/output chronology.
        async with factory.begin() as session:
            for table in ("agent_research_events", "agent_research_outputs"):
                await session.execute(
                    text(f"ALTER TABLE {table} ALTER COLUMN created_at SET DEFAULT now()")
                )
        # Reproduce the old capture location only in this isolated fixture;
        # public readers must continue to reject the resulting contradiction.
        from nexus.agents.models import ResearchRequest
        from nexus.persistence.models.agents import AgentResearchRunRecord

        async def old_capture(session, **kwargs):
            run = await session.get(AgentResearchRunRecord, kwargs["run_id"])
            request = ResearchRequest(
                instrument_id=run.instrument_id,
                data_snapshot_id=run.data_snapshot_id,
                decision_time=run.decision_time,
            )
            raise executor.Prepared(kwargs, request, ())

        monkeypatch.setattr(executor, "execute_or_capture", old_capture)

        async def old_zero_call_close(session, run, **kwargs):
            # The old producer lacked the installed-stage exclusion. Recreate
            # only its historical writes; all public proof readers stay real.
            from nexus.agents.persistence import release_daily_budget_if_reserved
            from nexus.agents.research_dispatch import stop_current_dispatch

            stop_current_dispatch()
            await release_daily_budget_if_reserved(session, run_id=run.id)
            run.input_tokens = run.output_tokens = 0
            run.settled_cost_usd = Decimal(0)
            return await execution._persist_terminal(session, run, **kwargs)

        monkeypatch.setattr(execution, "_terminal_without_usage", old_zero_call_close)
    seen, clients = [], []
    invalid = [fault == "before_send" or fault.startswith("old_bad_terminal")]
    if fault == "member_without_authorization":
        install = executor.StagedProtocol.install

        async def invalidate_after_install(self, **kwargs):
            await install(self, **kwargs)
            invalid[0] = True

        async def local_input_failure(*args, **kwargs):
            raise ValueError("synthetic local input failure before authorization")

        monkeypatch.setattr(executor.StagedProtocol, "install", invalidate_after_install)
        monkeypatch.setattr(
            comparison_execution.FrozenComparisonCommittee, "_inputs", local_input_failure
        )
    original_loader = execution.load_frozen_evidence
    before_final = {}

    async def stage_facts(session, run_id):
        return {
            table: sorted(
                json.dumps(row, sort_keys=True)
                for row in await session.scalars(
                    text(f"SELECT to_jsonb(t) FROM {table} t WHERE run_id=:run"), {"run": run_id}
                )
            )
            for table in (
                "research_stage_authorization",
                "research_stage_observation",
                "research_stage_verification",
                "research_stage_cleanup",
            )
        }

    async def load(*args, **kwargs):
        if invalid[0] and fault.startswith("after_send"):
            async with factory() as snapshot:
                before_final.update(await stage_facts(snapshot, args[1].id))
        return () if invalid[0] else await original_loader(*args, **kwargs)

    monkeypatch.setattr(execution, "load_frozen_evidence", load)
    if fault in {"observation", "missing_observation"}:
        from nexus.research import observation_identity_storage

        async def controlled_identity_storage(session, *, instrument_id, as_of, evidence_ids):
            assert instrument_id in manifest.instrument_ids
            assert as_of <= datetime.now(UTC)
            return {} if fault == "missing_observation" else dict.fromkeys(evidence_ids)

        monkeypatch.setattr(
            observation_identity_storage,
            "read_observation_identity_bindings",
            controlled_identity_storage,
        )

    async def handle(request):
        payload = json.loads(json.loads(request.content)["input"][1]["content"])
        answer = stage_response(payload, "platform", seen)
        event = terminal("completed")
        event["response"]["service_tier"] = "default"
        event["response"]["output"][0]["content"][0]["text"] = json.dumps(answer)
        event["response"]["usage"] = {
            "input_tokens": 300,
            "output_tokens": 80,
            "total_tokens": 380,
            "input_tokens_details": {
                "cached_tokens": 0,
                "cache_write_tokens": 0,
                "cache_write_tokens_details": {"ttl_30m_tokens": 0, "ttl_12h_tokens": 0},
            },
            "output_tokens_details": {"reasoning_tokens": 60},
        }
        if fault.startswith("after_send"):
            invalid[0] = True
        if fault == "after_send_unknown":
            raise httpx.ReadError("synthetic response lost")
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Body(("data: " + json.dumps(event) + "\n\n").encode()),
        )

    execute = comparison_execution.execute_comparison_position

    async def transport(**kwargs):
        def client():
            item = httpx.AsyncClient(transport=httpx.MockTransport(handle))
            clients.append(item)
            return item

        return await execute(
            **kwargs,
            client_factory=client,
        )

    monkeypatch.setattr(comparison_execution, "execute_comparison_position", transport)
    real_settings = openai_local.local_settings
    monkeypatch.setattr(
        openai_local,
        "local_settings",
        lambda *a, **k: settings if k.get("load_model_key") is False else real_settings(*a, **k),
    )
    paths = {}
    for name, body in {
        "source": source,
        "prepared": json.dumps(prepared).encode(),
        "manifest": manifest.model_dump_json().encode(),
    }.items():
        paths[name] = tmp_path / (name + ".json")
        paths[name].write_bytes(body)
    args = Namespace(
        command="admit-position",
        repo_root=tmp_path,
        position=1,
        model_config_id=model,
        funding_proof=tmp_path / "funding.json",
        worker_id="platform-preparation-regression",
        **paths,
    )
    try:
        admitted = await native_command(args)
        args.command = "run-position"
        error = None
        try:
            await native_command(args)
        except executor.StagedFinalizationFailed as caught:
            error = caught
        async with factory() as session:
            state = (
                (
                    await session.execute(
                        text("""
                SELECT r.status, r.input_tokens, r.output_tokens, r.settled_cost_usd,
                       b.state AS budget_state, b.settled_usd
                FROM agent_research_runs r JOIN agent_daily_budget_reservations b ON b.run_id=r.id
                WHERE r.id=:run
            """),
                        {"run": UUID(admitted["run_id"])},
                    )
                )
                .mappings()
                .one()
            )
            count = {}
            for table in (
                "research_stage_member",
                "research_stage_authorization",
                "research_stage_terminal",
            ):
                count[table] = await session.scalar(
                    text(f"SELECT count(*) FROM {table} WHERE run_id=:run"),
                    {"run": UUID(admitted["run_id"])},
                )
            assert (
                await session.scalar(
                    text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                    {"id": held},
                )
                == protected
            )
        if fault in {"before_send", "missing_observation"}:
            # Review 16: the existing exact uncalled checker does not grant
            # comparison preparation refusals a new zero-call proof. Roll back
            # the entire close, preserving the original claim and reservation.
            assert isinstance(error, executor.StagedFinalizationFailed)
            assert str(error) == "UNCALLED_TRANSACTION_FAILED"
            assert isinstance(error.__cause__, ValueError)
            assert str(error.__cause__) == "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"
            assert seen == []
            assert count == dict.fromkeys(count, 0)
            assert state["status"] == "prepared"
            assert state["budget_state"] == "reserved"
            assert (state["input_tokens"], state["output_tokens"], state["settled_cost_usd"]) == (
                None,
                None,
                None,
            )
        elif fault.startswith("old_bad_terminal"):
            from integration.test_comparison_closeout_pg import facts

            assert seen == [] and clients == [] and error is None
            assert count == {
                "research_stage_member": 1,
                "research_stage_authorization": 0,
                "research_stage_terminal": 1,
            }
            assert state["status"] == "abstained" and state["budget_state"] == "released"
            before = await facts(factory)
            with pytest.raises(ValueError, match="COMPARISON_TERMINAL_PROOF_INVALID"):
                await native_command(
                    Namespace(
                        command="inspect-position",
                        repo_root=tmp_path,
                        experiment_id=manifest.experiment_id,
                        position=1,
                    )
                )
            assert await facts(factory) == before
            args.position = 2
            args.command = "admit-position"
            with pytest.raises(ValueError):
                await native_command(args)
            assert await facts(factory) == before
            from uuid import uuid4

            from nexus.agents.comparison_concurrency import require_comparison_exclusive

            async with factory.begin() as session:
                if fault == "old_bad_terminal_known_revision":
                    await require_comparison_exclusive(session, uuid4())
                else:
                    with pytest.raises(ValueError, match="COMPARISON_OTHER_WORK_UNCONFIRMED"):
                        await require_comparison_exclusive(session, uuid4())
            assert await facts(factory) == before
            if fault == "old_bad_terminal_known_revision":
                from nexus.agents.comparison_inputs import PreparedComparisonInput
                from nexus.agents.comparison_legacy_preparation import (
                    historical_platform_preparation_isolated,
                )
                from nexus.agents.platform_stage_accounting import platform_authorization_binding
                from nexus.agents.platform_wire import compile_platform_stage
                from nexus.persistence.models.agents import AgentResearchRunRecord
                from sqlalchemy.exc import DBAPIError

                run_id, job_id = UUID(admitted["run_id"]), UUID(admitted["job_id"])
                member = prepared["members"][0]
                compiled = compile_platform_stage(
                    PreparedComparisonInput(
                        member["payload"], member["proof"], tuple(member["forbidden"])
                    ),
                    allocation=manifest.allocations[0],
                    expected_allocation_hash=manifest.allocations[0].allocation_hash,
                    stage="research",
                    previous=[],
                    prior_usage=[],
                )
                authorization = text("""INSERT INTO research_stage_authorization
                  (run_id,stage,job_id,attempt_no,worker_id,lease_token,fence_token,
                   reserved_id,wire_hash,schema_hash,prior_hash,authorized_at,platform_binding)
                  SELECT r.id,1,j.id,a.attempt_no,a.worker_id,a.lease_token,a.fence_token,
                    b.id,:wire,r.schema_hash,NULL,j.started_at,CAST(:binding AS jsonb)
                  FROM agent_research_runs r JOIN jobs j ON j.payload->>'run_id'=r.id::text
                  JOIN job_attempts a ON a.job_id=j.id
                  JOIN agent_daily_budget_reservations b ON b.run_id=r.id WHERE r.id=:run""")
                async with factory.begin() as session:
                    run = await session.get(AgentResearchRunRecord, run_id)
                    binding = await platform_authorization_binding(
                        session, run, stage=1, wire=compiled.wire, compiled=compiled
                    )
                    parameters = {
                        "run": run_id,
                        "job": job_id,
                        "scope": manifest.experiment_id,
                        "wire": compiled.body_sha256,
                        "binding": json.dumps(binding),
                    }
                    # Normal SQL still permanently rejects a late original authorization.
                    with pytest.raises(DBAPIError, match="COMPARISON_CHAIN_STOPPED"):
                        async with session.begin_nested():
                            await session.execute(authorization, parameters)
                    now = await session.scalar(text("SELECT clock_timestamp()"))
                    for damage in (
                        "UPDATE job_leases SET fence_token=fence_token+1 WHERE job_id=:job",
                        "UPDATE research_stage_scope SET stopped=false WHERE id=:scope",
                        "DELETE FROM research_comparison_chain_stop WHERE run_id=:run",
                        "UPDATE research_stage_terminal SET chain_hash=repeat('0',64) "
                        "WHERE run_id=:run",
                        "UPDATE agent_research_events SET event_hash=repeat('0',64) "
                        "WHERE run_id=:run",
                        "UPDATE research_comparison_manifest SET payload=jsonb_set(payload,"
                        "'{implementation_sha}',to_jsonb(repeat('f',40))) WHERE id=:scope",
                        authorization,
                    ):
                        savepoint = await session.begin_nested()
                        try:
                            # Reconstruct corrupt/late historical facts only in the
                            # isolated DB, then immediately restore real guards.
                            await session.execute(
                                text("SET LOCAL session_replication_role=replica")
                            )
                            await session.execute(
                                text(damage) if isinstance(damage, str) else damage,
                                parameters,
                            )
                            await session.execute(text("SET LOCAL session_replication_role=origin"))
                            session.expire_all()
                            assert not await historical_platform_preparation_isolated(
                                session, run_id=run_id, job_id=job_id, now=now
                            )
                            expected = (
                                "COMPARISON_REGISTRATION_CONFLICT"
                                if isinstance(damage, str) and "implementation_sha" in damage
                                else "COMPARISON_OTHER_WORK_UNCONFIRMED"
                            )
                            with pytest.raises(ValueError, match=expected):
                                await require_comparison_exclusive(session, uuid4())
                        finally:
                            await savepoint.rollback()
                            session.expire_all()
                    await require_comparison_exclusive(session, uuid4())
                replay = Namespace(**{**vars(args), "command": "run-position", "position": 1})
                with pytest.raises(ValueError):
                    await native_command(replay)
                assert seen == []
                assert await facts(factory) == before
                await _admit_stable_successor(factory, monkeypatch, tmp_path, source, manifest)
                after = await facts(factory)
                for table, rows in before.items():
                    assert all(row in after[table] for row in rows)
        elif fault == "member_without_authorization":
            assert isinstance(error, executor.StagedFinalizationFailed)
            assert seen == []
            assert count == {
                "research_stage_member": 1,
                "research_stage_authorization": 0,
                "research_stage_terminal": 0,
            }
            assert state["status"] == "prepared" and state["budget_state"] == "reserved"
            assert (state["input_tokens"], state["output_tokens"], state["settled_cost_usd"]) == (
                None,
                None,
                None,
            )
        elif fault.startswith("after_send"):
            assert isinstance(error, executor.StagedFinalizationFailed)
            assert seen
            assert state["status"] == "prepared" and state["budget_state"] == "reserved"
            assert (
                state["input_tokens"],
                state["output_tokens"],
                state["settled_cost_usd"],
                state["settled_usd"],
            ) == (None, None, None, None)
            assert count["research_stage_authorization"] == len(seen)
            assert count["research_stage_terminal"] == 0
            async with factory() as session:
                assert (
                    before_final
                    and await stage_facts(session, UUID(admitted["run_id"])) == before_final
                )
                for table in ("agent_research_outputs", "agent_research_events"):
                    assert (
                        await session.scalar(
                            text(f"SELECT count(*) FROM {table} WHERE run_id=:run"),
                            {"run": UUID(admitted["run_id"])},
                        )
                        == 0
                    )
                job = (
                    await session.execute(
                        text(
                            "SELECT status,attempt_count,manual_retry_count FROM jobs WHERE id=:job"
                        ),
                        {"job": UUID(admitted["job_id"])},
                    )
                ).one()
                assert tuple(job) == ("running", 1, 0)
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM job_attempts WHERE job_id=:job"),
                        {"job": UUID(admitted["job_id"])},
                    )
                    == 1
                )
            sent = tuple(seen)
            with pytest.raises(ValueError, match="COMPARISON_POSITION_CONSUMED"):
                await native_command(args)
            assert tuple(seen) == sent
        else:
            assert error is None
            assert [stage for _, stage in seen] == ["research", "critique", "synthesis"]
            assert state["budget_state"] == "settled"
            readback = await native_command(
                Namespace(
                    command="inspect-position",
                    repo_root=tmp_path,
                    experiment_id=manifest.experiment_id,
                    position=1,
                )
            )
            assert readback["terminal_proof"]["known"] is True
            # Exact basis binding must survive the wrapper; none of these
            # in-memory corruptions are flushed or change frozen DB records.
            from copy import deepcopy
            from datetime import timedelta
            from uuid import uuid4

            from nexus.agents import execution_authority
            from nexus.agents.comparison_inputs import _validate
            from nexus.persistence.models.agents import AgentResearchRunRecord
            from nexus.persistence.models.data import DataSnapshot
            from nexus.persistence.models.research import ResearchEvidenceRef

            async with factory() as session:
                run = await session.get(AgentResearchRunRecord, UUID(admitted["run_id"]))
                snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
                context = _validate(snapshot.payload["frozen"])
                basis = await session.get(DataSnapshot, context.source_snapshot_id)
                current = ResearchEvidenceRef(
                    id=uuid4(),
                    source=execution_authority.RESEARCH_IDENTITY_SOURCE,
                    provider_object_id=str(run.instrument_id),
                    revision="c" * 64,
                    available_at=run.decision_time,
                    location="memory://sha256/" + "c" * 64,
                    content_hash="c" * 64,
                    byte_length=1,
                )
                session.add(current)
                await session.flush()

                async def controlled_current_identity(*args, **kwargs):
                    return {current.id: object()}

                monkeypatch.setattr(
                    execution_authority, "read_identity_bindings", controlled_current_identity
                )
                with session.no_autoflush:
                    for target, attribute, changed in (
                        (run, "actor_id", "unregistered-experiment"),
                        (basis, "provider_object_id", str(uuid4())),
                        (basis, "observed_at", basis.observed_at + timedelta(seconds=1)),
                        (basis, "revision", "0" * 64),
                        (basis, "payload", {**basis.payload, "evidence_ids": []}),
                    ):
                        previous = deepcopy(getattr(target, attribute))
                        setattr(target, attribute, changed)
                        assert (
                            await original_loader(session, run, allow_fixture_evidence=False) == ()
                        )
                        setattr(target, attribute, previous)
                    assert (
                        len(await original_loader(session, run, allow_fixture_evidence=False)) == 2
                    )
                    original_manifest, original_hash = run.evidence_manifest, run.evidence_hash
                    run.evidence_manifest = [
                        *original_manifest,
                        {
                            "id": str(current.id),
                            "source": current.source,
                            "provider_object_id": current.provider_object_id,
                            "revision": current.revision,
                            "location": current.location,
                            "content_hash": current.content_hash,
                            "available_at": current.available_at.isoformat(),
                        },
                    ]
                    run.evidence_hash = execution._hash(run.evidence_manifest)
                    assert await original_loader(session, run, allow_fixture_evidence=False) == ()
                    run.evidence_manifest, run.evidence_hash = original_manifest, original_hash
    finally:
        await factory.kw["bind"].dispose()
