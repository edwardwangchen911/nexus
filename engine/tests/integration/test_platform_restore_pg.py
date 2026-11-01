"""Isolated synthetic manifests exercise the actual restore-time CHECK."""

import asyncio
import hashlib
import json
import sys
from copy import deepcopy
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from integration.test_platform_producer_pg import setup
from nexus.agents.platform_profile import platform_profile_hash
from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION, PLATFORM_PRICE_VERSION
from nexus.persistence.platform_restore import PLATFORM_LOOKUP_REPAIR
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from unit.agents.test_platform_manifest import platform_fixture


@pytest.fixture
def backup_docker_gateway(monkeypatch):
    # Docker Desktop supplies this name on the supported Mac runtime. A Linux
    # CI runner needs an explicit alias on its isolated test containers only.
    if sys.platform != "linux":
        return
    from nexus.ops.local_recovery import LocalRecovery

    original = LocalRecovery.command

    async def command(self, *args, **kwargs):
        if args[0] == "create":
            args = ("create", "--add-host=host.docker.internal:host-gateway", *args[1:])
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(LocalRecovery, "command", command)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["wrapper_body", "v1_body", "wrapper_attributes"])
async def test_lookup_repair_refuses_unrecognized_definition_without_replacing_it(
    migrated_database_url, change
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    function = (
        "research_platform_manifest_v1_valid"
        if change == "v1_body"
        else "research_platform_manifest_valid"
    )
    try:
        async with factory() as session, session.begin():
            if change == "wrapper_attributes":
                await session.execute(text(f"ALTER FUNCTION public.{function}(jsonb) COST 1"))
            else:
                await session.execute(
                    text(f"""CREATE OR REPLACE FUNCTION public.{function}(p jsonb)
                  RETURNS boolean LANGUAGE sql IMMUTABLE AS $$ SELECT true $$""")
                )
            definition = text("SELECT pg_get_functiondef(CAST(:f AS regprocedure))")
            signature = {"f": f"public.{function}(jsonb)"}
            before = await session.scalar(definition, signature)
            with pytest.raises(DBAPIError, match="PLATFORM_RESTORE_FUNCTION_UNEXPECTED"):
                async with session.begin_nested():
                    await session.execute(text(PLATFORM_LOOKUP_REPAIR))
            assert await session.scalar(definition, signature) == before
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "revision,fault",
    [
        ("0107_platform_price_renewal", None),
        ("head", None),
        ("0107_platform_price_renewal", "unknown_body"),
        ("0107_platform_price_renewal", "post_data"),
    ],
)
@pytest.mark.usefixtures("backup_docker_gateway")
async def test_normal_backup_restore_keeps_nonempty_profiles_and_original_source(
    temporary_database_url, monkeypatch, tmp_path, revision, fault
):
    from nexus.agents.comparison_manifest import parse_comparison_manifest
    from nexus.ops import local_backup, local_recovery
    from nexus.ops.local_backup import LocalBackup
    from nexus.ops.local_recovery import LocalRecovery, RecoveryError

    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), revision)
    factory = session_factory(EngineSettings(database_url=temporary_database_url))
    manifests = [
        parse_comparison_manifest(platform_fixture(version)[2])
        for version in (PLATFORM_PRICE_VERSION, CURRENT_PLATFORM_PRICE_VERSION)
    ]
    try:
        async with factory() as session, session.begin():
            for manifest in manifests:
                await session.execute(
                    text("""INSERT INTO research_comparison_manifest
                    (id,purpose,manifest_hash,payload)
                    VALUES(:id,:purpose,:digest,CAST(:p AS jsonb))"""),
                    {
                        "id": manifest.experiment_id,
                        "purpose": manifest.purpose,
                        "digest": manifest.digest,
                        "p": manifest.model_dump_json(),
                    },
                )
            before = list(
                await session.scalars(
                    text("SELECT to_jsonb(m) FROM research_comparison_manifest m ORDER BY id")
                )
            )
            if fault == "unknown_body":
                await session.execute(
                    text("""CREATE OR REPLACE FUNCTION
                    public.research_platform_manifest_valid(p jsonb) RETURNS boolean
                    LANGUAGE sql IMMUTABLE AS $$ SELECT true $$""")
                )
        environment = {
            "DATABASE_URL": temporary_database_url,
            "OBJECT_STORE_ENDPOINT": "http://localhost:59999",
            "OBJECT_STORE_REGION": "us-east-1",
            "OBJECT_STORE_BUCKET": "synthetic",
            "OBJECT_STORE_ACCESS_KEY_ID": "synthetic",
            "OBJECT_STORE_SECRET_ACCESS_KEY": "synthetic",
        }
        monkeypatch.setattr(local_backup, "load_local_environment", lambda *a: environment)
        # No source objects exist in this synthetic case; even constructing an
        # external client is unnecessary. The original object case stays covered.
        monkeypatch.setattr(local_recovery, "S3ProviderEvidenceStore", lambda *a, **kw: None)
        backup = LocalBackup(tmp_path / "bundle", tmp_path)
        assert (await backup.execute())["status"] == "complete"
        digest = hashlib.sha256((backup.backup / "database.dump").read_bytes()).hexdigest()

        def forbidden(*a, **kw):
            pytest.fail("complete recovery must not access source configuration or objects")

        monkeypatch.setattr(local_recovery, "load_local_environment", forbidden)
        monkeypatch.setattr(local_recovery, "S3ProviderEvidenceStore", forbidden)
        recovery = LocalRecovery(backup.backup, tmp_path / "absent-source-config")
        original = recovery.restore_database
        original_command = recovery.command

        async def fail_after_post_data(*args, **kwargs):
            result = await original_command(*args, **kwargs)
            if "--section=post-data" in args:
                await original_command(
                    "exec",
                    "-i",
                    recovery.container,
                    "tee",
                    "-a",
                    "/tmp/recovery-data.sql",
                    input_bytes=b"\nSELECT 1 / 0;\n",
                )
            return result

        if fault == "post_data":
            monkeypatch.setattr(recovery, "command", fail_after_post_data)

        async def verify_rows(*args):
            try:
                rows = await original(*args)
            except RecoveryError:
                # The first failure must roll back pre-data AND any copied data,
                # before the normal owner finally removes the disposable target.
                target = create_async_engine(args[1])
                try:
                    async with target.connect() as connection:
                        assert (
                            await connection.scalar(
                                text(
                                    "SELECT count(*) FROM pg_class c JOIN pg_namespace n "
                                    "ON n.oid=c.relnamespace WHERE n.nspname='public'"
                                )
                            )
                            == 0
                        )
                finally:
                    await target.dispose()
                raise
            assert all(row in rows for row in before)
            return rows

        monkeypatch.setattr(recovery, "restore_database", verify_rows)
        if fault:
            monkeypatch.setattr(recovery, "recover_local_objects", forbidden)
            with pytest.raises(RecoveryError, match="RECOVERY_DOCKER_COMMAND_FAILED"):
                await recovery.execute()
            result = json.loads((recovery.output / "result.json").read_bytes())
            assert result["status"] == "failed" and result["target_residue"] == 0
            assert not result.get("object_bindings_verified", False)
        else:
            result = await recovery.execute()
            assert result["status"] == "verified" and result["target_residue"] == 0
            assert result["source_object_store_required"] is False
            if revision == "0107_platform_price_renewal":
                assert result["backup_schema"] == revision
                assert result["schema_compatibility"] == ["0107_platform_manifest_qualified_lookup"]
            else:
                assert result.get("schema_compatibility", []) == []
        assert hashlib.sha256((backup.backup / "database.dump").read_bytes()).hexdigest() == digest
        async with factory() as session:
            assert (
                list(
                    await session.scalars(
                        text("SELECT to_jsonb(m) FROM research_comparison_manifest m ORDER BY id")
                    )
                )
                == before
            )
        if revision == "0107_platform_price_renewal" and not fault:
            # Upgrading populated old storage and rolling its marker back must
            # retain the compatible lookup correction, not resurrect the bug.
            await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), "head")
            await asyncio.to_thread(command.downgrade, Config("engine/alembic.ini"), revision)
            async with factory() as session, session.begin():
                await session.execute(text("SELECT pg_catalog.set_config('search_path','',true)"))
                assert list(
                    await session.scalars(
                        text(
                            "SELECT public.research_platform_manifest_valid(payload) "
                            "FROM public.research_comparison_manifest"
                        )
                    )
                ) == [True, True]
                assert (
                    list(
                        await session.scalars(
                            text(
                                "SELECT to_jsonb(m) FROM public.research_comparison_manifest m "
                                "ORDER BY id"
                            )
                        )
                    )
                    == before
                )
                assert (
                    await session.scalar(text("SELECT version_num FROM public.alembic_version"))
                    == revision
                )
            await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), "head")
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_empty_restore_search_path_copies_both_profiles_and_rejects_invalid_manifest(
    migrated_database_url, monkeypatch
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        _, _, manifest, _, _, _ = await setup(
            factory, monkeypatch, price_version=CURRENT_PLATFORM_PRICE_VERSION
        )
        current = manifest.model_dump(mode="json")
        old = deepcopy(current)
        old["api_config_hash"] = platform_profile_hash()
        old["platform_contract"] = "platform-gifted-terra-v1-20261031"
        for allocation in old["allocations"]:
            allocation["profile"]["price_version"] = PLATFORM_PRICE_VERSION
        async with factory() as session, session.begin():
            await session.execute(text("SELECT pg_catalog.set_config('search_path','',true)"))
            for payload in (old, current):
                assert (
                    await session.scalar(
                        text("SELECT public.research_platform_manifest_valid(CAST(:p AS jsonb))"),
                        {"p": json.dumps(payload)},
                    )
                    is True
                )
            raw = await (await session.connection()).get_raw_connection()
            await raw.driver_connection.copy_records_to_table(
                "research_comparison_manifest",
                schema_name="public",
                columns=("id", "purpose", "manifest_hash", "payload"),
                records=[
                    (uuid4(), p["platform_contract"], "a" * 64, json.dumps(p))
                    for p in (old, current)
                ],
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM public.research_comparison_manifest")
                )
                == 2
            )
        async with factory() as session:
            with pytest.raises(DBAPIError) as rejected:
                async with session.begin():
                    await session.execute(
                        text("SELECT pg_catalog.set_config('search_path','',true)")
                    )
                    await session.execute(
                        text("""INSERT INTO public.research_comparison_manifest
                        (id,purpose,manifest_hash,payload) VALUES
                        (:id,'platform-terra-v2-20261101',:hash,'{}'::jsonb)"""),
                        {"id": uuid4(), "hash": "b" * 64},
                    )
            assert rejected.value.orig.sqlstate == "23514"
    finally:
        await factory.kw["bind"].dispose()
