import hashlib
import json
from argparse import Namespace
from datetime import timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from integration.test_platform_producer_pg import setup
from nexus.agents.comparison_producer import admit_comparison_position
from nexus.agents.platform_funding import load_bound_platform_funding, require_platform_funding
from nexus.ops.platform_credentials import (
    PLATFORM_KEY_PATH,
    credential_fingerprint,
    resolve_platform_credential,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


async def funding_fixture(
    factory, manifest, tmp_path, monkeypatch, *, balance=None, explicit_spend=False
):
    key = "synthetic-platform-native-funding"
    for name in (
        "OPENAI_API_KEY",
        "OPENAI_BASE_URL",
        "OPENAI_API_BASE",
        "OPENAI_ORG_ID",
        "OPENAI_ORGANIZATION",
        "OPENAI_PROJECT_ID",
        "OPENAI_PROJECT",
    ):
        monkeypatch.delenv(name, raising=False)
    (tmp_path / "engine").mkdir(exist_ok=True)
    (tmp_path / "engine/pyproject.toml").write_text("# synthetic fixture")
    target = tmp_path / PLATFORM_KEY_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(key)
    target.chmod(0o600)
    credential = resolve_platform_credential(
        tmp_path,
        expected_fingerprint=credential_fingerprint(key),
        organization_id=None if explicit_spend else "org-fixture",
        project_id=None if explicit_spend else "proj-fixture",
        key_scope_only=explicit_spend,
    )
    async with factory() as s:
        now = await s.scalar(text("SELECT clock_timestamp()"))
    original = tmp_path / "console.txt"
    original.write_text("synthetic financial qualification")
    original.chmod(0o600)
    body = dict(
        version=1,
        kind="manual_platform_console",
        purpose=manifest.purpose,
        endpoint=credential.endpoint,
        credential_fingerprint=credential.fingerprint,
        organization_id="org-fixture",
        project_id="proj-fixture",
        key_ownership_confirmed=True,
        gift_claim_active=True,
        gift_balance_usd=str(balance or manifest.total_reservation_usd),
        gift_expires_at=(now + timedelta(hours=1)).isoformat(),
        purchased_balance_usd="0",
        automatic_recharge_enabled=False,
        other_paid_fallback_enabled=False,
        organization_exclusive_window_confirmed=True,
        other_usage_bounded=True,
        other_usage_upper_bound_usd="0",
        inflight_usage_bounded=True,
        unknown_inflight_present=False,
        unreflected_usage_upper_bound_usd="0",
        verified_at=now.isoformat(),
        window_start=now.isoformat(),
        window_end=(now + timedelta(minutes=30)).isoformat(),
        console_evidence_file=original.name,
        console_evidence_sha256=hashlib.sha256(original.read_bytes()).hexdigest(),
    )
    if explicit_spend:
        from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION
        from nexus.ops import platform_credentials

        renewed = manifest.price_version == CURRENT_PLATFORM_PRICE_VERSION

        # Like the synthetic model price window, isolate lifecycle tests from
        # today's date. Unit tests enforce the real fixed cutoff permanently.
        monkeypatch.setattr(
            platform_credentials,
            "RENEWED_EXPLICIT_SPEND_DEADLINE" if renewed else "EXPLICIT_SPEND_DEADLINE",
            now + timedelta(hours=1),
        )
        body = dict(
            version=3 if manifest.purpose == "platform-terra-v3-20261101" else 2 if renewed else 1,
            kind="explicit_platform_spend_authorization",
            purpose=manifest.purpose,
            endpoint=credential.endpoint,
            credential_fingerprint=credential.fingerprint,
            experiment_id=str(manifest.experiment_id),
            input_hashes=manifest.input_hashes,
            max_total_usd="6.42",
            authorized_at=now.isoformat(),
            window_start=now.isoformat(),
            window_end=(now + timedelta(minutes=30)).isoformat(),
            authorization_file=original.name,
            authorization_sha256=hashlib.sha256(original.read_bytes()).hexdigest(),
        )
    path = tmp_path / "funding.json"
    path.write_text(json.dumps(body))
    path.chmod(0o600)
    manifest = type(manifest).model_validate(
        {
            **manifest.model_dump(),
            "funding_proof_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
            "credential_binding_hash": credential.binding_hash,
        }
    )
    return manifest, load_bound_platform_funding(tmp_path, path, manifest)


@pytest.mark.asyncio
async def test_missing_funding_cannot_create_manifest_run_or_reservation(
    migrated_database_url, monkeypatch
):
    f = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, m, model, _, _ = await setup(f, monkeypatch)
    try:
        with pytest.raises(ValueError, match="PLATFORM_FUNDING_REQUIRED"):
            async with f() as s, s.begin():
                await admit_comparison_position(
                    s,
                    source=source,
                    prepared=prepared,
                    manifest=m,
                    ordinal=1,
                    model_config_id=model,
                    daily_limit_usd=Decimal(10),
                )
        async with f() as s:
            assert await s.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 0
            assert await s.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 0
            assert await s.scalar(text("SELECT count(*) FROM agent_daily_budget_reservations")) == 1
    finally:
        await f.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_spend", [False, True], ids=["gift", "explicit-spend"])
async def test_same_frozen_total_remains_covered_after_native_reservations(
    migrated_database_url, monkeypatch, tmp_path, explicit_spend
):
    f = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, m, model, _, _ = await setup(f, monkeypatch)
    m, funding = await funding_fixture(f, m, tmp_path, monkeypatch, explicit_spend=explicit_spend)
    try:
        for i in (1, 2, 3):
            async with f() as s, s.begin():
                await admit_comparison_position(
                    s,
                    source=source,
                    prepared=prepared,
                    manifest=m,
                    ordinal=i,
                    model_config_id=model,
                    daily_limit_usd=Decimal(10),
                    platform_funding=funding,
                )
        async with f() as s, s.begin():
            await require_platform_funding(s, m, funding)
        # A changed finite funding snapshot never replaces the registered proof.
        from dataclasses import replace

        with pytest.raises(ValueError, match="PLATFORM_FUNDING_BINDING_CHANGED"):
            async with f() as s, s.begin():
                await require_platform_funding(
                    s, m, replace(funding, proof=replace(funding.proof, proof_hash="0" * 64))
                )
    finally:
        await f.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_spend", [False, True], ids=["gift", "explicit-spend"])
async def test_normal_cli_admission_consumes_real_bound_funding_and_never_grant(
    migrated_database_url, monkeypatch, tmp_path, explicit_spend
):
    from nexus.agents.chatgpt_grant import GrantStore
    from nexus.ops import openai_local
    from nexus.ops.research_comparison import native_command
    from pydantic import SecretStr

    settings = EngineSettings(
        database_url=migrated_database_url,
        openai_api_key=SecretStr("synthetic-platform-native-funding"),
    )
    f = session_factory(settings)
    source, prepared, m, model, _, _ = await setup(f, monkeypatch)
    m, _ = await funding_fixture(f, m, tmp_path, monkeypatch, explicit_spend=explicit_spend)
    paths = {}
    for name, content in {
        "source": source,
        "prepared": json.dumps(prepared).encode(),
        "manifest": m.model_dump_json().encode(),
    }.items():
        paths[name] = tmp_path / (name + ".json")
        paths[name].write_bytes(content)
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **k: settings)

    async def forbidden(*args, **kwargs):
        raise AssertionError("Platform must not load Grant")

    monkeypatch.setattr(GrantStore, "load_verified", forbidden)
    args = Namespace(
        command="admit-position",
        repo_root=tmp_path,
        position=1,
        model_config_id=model,
        funding_proof=tmp_path / "funding.json",
        **paths,
    )
    try:
        result = await native_command(args)
        assert result["status"] == "position_prepared" and result["dispatch_authority"] is False
        args.command = "run-position"
        args.funding_proof = None
        with pytest.raises(ValueError, match="PLATFORM_FUNDING_REQUIRED"):
            await native_command(args)
        async with f() as s:
            assert await s.scalar(text("SELECT count(*) FROM job_attempts")) == 0
            assert await s.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
    finally:
        await f.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_insufficient_actual_proof_cannot_reserve(
    migrated_database_url, monkeypatch, tmp_path
):
    f = session_factory(EngineSettings(database_url=migrated_database_url))
    source, prepared, m, model, _, _ = await setup(f, monkeypatch)
    m, funding = await funding_fixture(
        f, m, tmp_path, monkeypatch, balance=m.total_reservation_usd - Decimal("0.0000001")
    )
    try:
        with pytest.raises(ValueError, match="PLATFORM_BALANCE_INSUFFICIENT"):
            async with f() as s, s.begin():
                await admit_comparison_position(
                    s,
                    source=source,
                    prepared=prepared,
                    manifest=m,
                    ordinal=1,
                    model_config_id=model,
                    daily_limit_usd=Decimal(10),
                    platform_funding=funding,
                )
        async with f() as s:
            assert await s.scalar(text("SELECT count(*) FROM research_comparison_manifest")) == 0
            assert await s.scalar(text("SELECT count(*) FROM agent_daily_budget_reservations")) == 1
    finally:
        await f.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("settled", [False, True])
@pytest.mark.parametrize(
    "history_version",
    [
        "platform-terra-default-implicit30m-20261031-v1",
        "platform-terra-default-implicit30m-20261101-v2",
    ],
)
async def test_other_platform_prior_day_unknown_or_spend_reduces_gift_coverage(
    migrated_database_url, monkeypatch, tmp_path, settled, history_version
):
    from nexus.agents.persistence import reserve_daily_budget
    from nexus.agents.platform_profile import platform_model_fields
    from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION
    from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord

    f = session_factory(EngineSettings(database_url=migrated_database_url))
    source, _, m, model_id, held, protected = await setup(
        f, monkeypatch, price_version=CURRENT_PLATFORM_PRICE_VERSION
    )
    m, funding = await funding_fixture(f, m, tmp_path, monkeypatch)
    try:
        async with f() as s, s.begin():
            original = await s.get(
                AgentResearchRunRecord, UUID(json.loads(source)["members"][0]["run_id"])
            )
            model = await s.get(AgentModelConfigRecord, model_id)
            if history_version != model.version:
                model = AgentModelConfigRecord(**platform_model_fields(history_version))
                s.add(model)
                await s.flush()
            values = {
                c.key: getattr(original, c.key) for c in AgentResearchRunRecord.__table__.columns
            }
            extra_id = uuid4()
            values.update(
                id=extra_id,
                idempotency_key="isolated-platform-prior-" + str(extra_id),
                model_config_id=model.id,
                model_hash=model.config_hash,
                pricing_version=model.version,
                reserved_cost_usd=Decimal(2),
                settled_cost_usd=Decimal(1) if settled else None,
            )
            s.add(AgentResearchRunRecord(**values))
            await s.flush()
            now = await s.scalar(text("SELECT clock_timestamp()"))
            budget = await reserve_daily_budget(
                s,
                run_id=extra_id,
                now=now - timedelta(days=1),
                daily_limit_usd=Decimal(10),
                worst_case_usd=Decimal(2),
                pricing_version=model.version,
                pricing_fresh=True,
            )
            assert budget.business_date < now.date()
            if settled:
                budget.state = "settled"
                budget.settled_usd = Decimal(1)
        with pytest.raises(ValueError, match="PLATFORM_BALANCE_INSUFFICIENT"):
            async with f() as s, s.begin():
                await require_platform_funding(s, m, funding)
        async with f() as s:
            assert (
                await s.scalar(
                    text("SELECT to_jsonb(b) FROM agent_daily_budget_reservations b WHERE id=:id"),
                    {"id": held},
                )
                == protected
            )
    finally:
        await f.kw["bind"].dispose()
