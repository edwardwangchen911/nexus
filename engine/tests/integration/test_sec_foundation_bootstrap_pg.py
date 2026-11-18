"""No-plan foundation registration through the normal entry and immutable SQL gates."""

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from helpers.database_clock import is_fixture_clock_read
from helpers.disclosure_observations import append_capture
from helpers.durable_test_clock import DisposableClock
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import AT
from integration.test_monitor_batches_pg import installed
from nexus.monitoring.auto_increment_policy import enable_scope
from nexus.ops.research_monitor import parser, run
from nexus.persistence.models.sec_monitor import SecMonitorRule
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
@pytest.mark.parametrize("capture_kind", ["v1", "listed_v2"])
async def test_no_plan_bootstrap_is_forward_scoped_audited_and_never_paid(
    migrated_database_url, monkeypatch, tmp_path, capture_kind
):
    factory, settings, store, captured, pointer = await installed(
        DisposableClock.uncached_url(migrated_database_url), monkeypatch, tmp_path, kinds=()
    )
    monkeypatch.setattr(
        "nexus.monitoring.sec_foundation.configured_exhibit_store", lambda db: store
    )
    monkeypatch.setattr(
        "nexus.monitoring.durable_sec_host.configured_exhibit_store", lambda db: store
    )
    normal_scalar = AsyncSession.scalar

    async def synchronized_clock(session, statement, *args, **kwargs):
        if is_fixture_clock_read(statement):
            return captured["clock"][0]
        return await normal_scalar(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", synchronized_clock)
    controller = DisposableClock(migrated_database_url)
    async with factory.begin() as db:
        logical = AT + timedelta(days=7, hours=1)
        await controller.set(db, logical, running=False)
    await factory.kw["bind"].dispose()
    captured["clock"][0] = logical
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord

    next_day = logical + timedelta(days=1)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    async with factory.begin() as db:
        for day in (logical, next_day):
            db.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=day.date(),
                    decision_at=day.replace(hour=21, minute=0),
                    market_open_at=day.replace(hour=13, minute=30),
                    market_close_at=day.replace(hour=20, minute=0),
                    created_at=logical - timedelta(seconds=1),
                )
            )
    try:
        async with factory() as db:
            at = await db.scalar(text("SELECT clock_timestamp()"))
        fresh = await append_capture(
            settings,
            store,
            captured,
            factory,
            tmp_path,
            "foundation-fresh",
            added=0,
            received=at,
            imported_at=at,
        )
        if capture_kind == "listed_v2":
            import json
            import shutil
            from hashlib import sha256

            from nexus.ops.sec_index_local import import_index

            archive = tmp_path / "foundation-fresh"
            path = archive / "AAPL-submissions.json"
            value = json.loads(path.read_bytes())
            value["filings"]["files"] = []
            path.write_text(json.dumps(value))
            receipt = archive / "AAPL-submissions-receipt.json"
            proof = json.loads(receipt.read_bytes())
            proof.update(
                sha256=sha256(path.read_bytes()).hexdigest(), byte_length=path.stat().st_size
            )
            receipt.write_text(json.dumps(proof))
            shutil.copyfile(path, archive / "AAPL-submissions-close.json")
            shutil.copyfile(receipt, archive / "AAPL-submissions-close-receipt.json")
            (archive / "history-manifest.json").write_text(
                json.dumps({"schema": "sec_listed_history_archive_v2", "files": []})
            )
            fresh = await import_index(
                settings,
                identity_snapshot_id=captured["identity"].snapshot_id,
                archive_dir=archive,
                store=store,
                listed_history=True,
            )
        async with factory.begin() as db:
            await enable_scope(
                db,
                account_id=pointer.account_id,
                candidate_batch_id=settings.agent_paper_candidate_batch_id,
            )
        args = parser().parse_args(
            [
                "enable-sec-foundation",
                "--instrument-id",
                str(pointer.instrument_id),
                "--identity-snapshot-id",
                str(captured["identity"].snapshot_id),
                "--capture-id",
                fresh["snapshot_id"],
            ]
        )
        before = None
        async with factory() as db:
            before = [
                await db.scalar(text(f"SELECT count(*) FROM {t}"))
                for t in (
                    "research_monitor_plans",
                    "agent_research_runs",
                    "jobs",
                    "outbox_events",
                    "agent_daily_budget_reservations",
                    "sec_auto_increment_day_claims",
                )
            ]
        import copy

        bad = copy.copy(args)
        bad.instrument_id = uuid4()
        with pytest.raises(ValueError, match="SEC_FOUNDATION_SCOPE_INVALID"):
            await run(settings, bad)
        bad = copy.copy(args)
        bad.identity_snapshot_id = uuid4()
        with pytest.raises(ValueError, match="SEC_FOUNDATION_SCOPE_INVALID"):
            await run(settings, bad)
        bad = copy.copy(args)
        bad.capture_id = pointer.baseline_snapshot_id
        with pytest.raises(ValueError, match="SEC_FOUNDATION_SOURCE_STALE"):
            await run(settings, bad)

        async def missing_audit(*args, **kwargs):
            return None

        with monkeypatch.context() as patch:
            patch.setattr("nexus.monitoring.sec_foundation.append_audit_event", missing_audit)
            with pytest.raises(DBAPIError, match="SEC_FOUNDATION_SCOPE_INVALID"):
                await run(settings, args)
        result = await run(settings, args)
        assert result["status"] == "active" and result["research_authority_granted"] is False
        assert result["provider_requests"] == result["model_requests"] == 0
        repeated = await run(settings, args)
        assert repeated == result
        if capture_kind == "v1":
            from helpers.filing_relations import assert_relation_cli

            await assert_relation_cli(
                factory,
                settings,
                store,
                fresh["snapshot_id"],
                tmp_path / "graphic-relations",
                monkeypatch,
            )
        async with factory.begin() as db:
            rule = await db.get(SecMonitorRule, UUID(result["rule_id"]))
            assert rule.anchor_plan_id is None
            assert rule.forms == ["8-K", "8-K/A"]
            assert rule.created_at == rule.effective_at
            assert before == [
                await db.scalar(text(f"SELECT count(*) FROM {t}"))
                for t in (
                    "research_monitor_plans",
                    "agent_research_runs",
                    "jobs",
                    "outbox_events",
                    "agent_daily_budget_reservations",
                    "sec_auto_increment_day_claims",
                )
            ]
            from nexus.monitoring.durable_sec_research import prepare_durable_sec_research_batch

            with pytest.raises(ValueError, match="MONITOR_AWAITING_RECONCILIATION"):
                await prepare_durable_sec_research_batch(
                    db, account_id=rule.account_id, rule_id=rule.id
                )
        # One genuinely later controlled source traverses the existing NULL-anchor
        # discovery/notice path, while absent research qualification still refuses.
        import json
        import shutil
        from hashlib import sha256

        from nexus.monitoring.durable_sec_storage import record_discovery
        from nexus.ops.durable_sec_index_local import import_durable_index

        monkeypatch.setattr(
            "nexus.monitoring.durable_sec_storage.configured_exhibit_store", lambda db: store
        )
        archive = tmp_path / "null-anchor-positive"
        shutil.copytree(tmp_path / "foundation-fresh", archive)
        captured["clock"][0] = logical + timedelta(minutes=1)
        async with factory.begin() as db:
            await controller.set(db, captured["clock"][0], running=False)
            received = await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
        assert received > rule.effective_at
        path = archive / "AAPL-submissions.json"
        doc = json.loads(path.read_bytes())
        recent = doc["filings"]["recent"]
        for key in recent:
            recent[key].append(recent[key][0])
        recent["accessionNumber"][-1] = "0000123456-26-000099"
        recent["acceptanceDateTime"][-1] = received.isoformat()
        recent["filingDate"][-1] = received.date().isoformat()
        recent["primaryDocument"][-1] = "new99.htm"
        recent["acceptanceDateTime"][0] = (received + timedelta(days=1)).isoformat()
        path.write_text(json.dumps(doc))
        for name in ("directory", "AAPL-submissions"):
            raw = (archive / f"{name}.json").read_bytes()
            receipt = archive / f"{name}-receipt.json"
            binding = json.loads(receipt.read_bytes())
            binding.update(
                received_at=received.isoformat(),
                sha256=sha256(raw).hexdigest(),
                byte_length=len(raw),
            )
            receipt.write_text(json.dumps(binding))
        imported = await import_durable_index(
            settings,
            identity_snapshot_id=captured["identity"].snapshot_id,
            archive_dir=archive,
            store=store,
        )
        from nexus.ops.monitor_disclosure_refresh import refresh_disclosure_observations
        from nexus.persistence.models.agents import AgentResearchRunRecord

        async with factory() as db:
            original_run = await db.get(AgentResearchRunRecord, pointer.source_run_id)
        observation_settings = settings.model_copy(
            update={
                "agent_paper_cycle_enabled": True,
                "agent_paper_formal_model_config_id": original_run.model_config_id,
            }
        )
        observed = await refresh_disclosure_observations(
            observation_settings,
            account_id=pointer.account_id,
            calendar_authority_id=CALENDAR_ID,
            scheduled_at=received.replace(minute=received.minute // 5 * 5, second=0, microsecond=0),
            environment={"SEC_CONTACT": "test@example.invalid"},
            repo_root=tmp_path,
        )
        assert observed["status"] == "unchanged" and observed["provider_requests"] == 0
        from nexus.monitoring.scanning import scan_account
        from nexus.persistence.models.sec_monitor import SecDiscoveryCheck
        from sqlalchemy import select

        async with factory.begin() as db:
            now = await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            report = await scan_account(
                db,
                account_id=rule.account_id,
                calendar_authority_id=CALENDAR_ID,
                now=now,
                instrument_ids=(rule.instrument_id,),
            )
            assert (
                report["status"] == "scanned"
                and report["checks"] == 1
                and report["notifications"] == 1
            )
            assert report["coverage_gap"] == 1, "base discovery cannot invent an investment plan"
            check = await db.scalar(
                select(SecDiscoveryCheck).where(SecDiscoveryCheck.rule_id == rule.id)
            )
            assert (check.result, check.coverage) == ("true", "partial")
            assert await db.scalar(text("SELECT count(*) FROM sec_discovery_events")) == 1
            assert await db.scalar(text("SELECT count(*) FROM sec_notification_claims")) == 1
            with pytest.raises(ValueError, match="MONITOR_AWAITING_RECONCILIATION"):
                await prepare_durable_sec_research_batch(
                    db, account_id=rule.account_id, rule_id=rule.id
                )
        async with factory.begin() as db:
            repeated_check = await record_discovery(
                db,
                account_id=rule.account_id,
                rule_id=rule.id,
                capture_id=UUID(imported["snapshot_id"]),
                scheduled_at=check.scheduled_at,
            )
            assert repeated_check.id == check.id
            assert await db.scalar(text("SELECT count(*) FROM sec_notification_claims")) == 1
        # The database independently rejects altered bindings, including SQL NULL.
        import json

        async with factory.begin() as db:
            for field, value in (
                ("issuer_cik", "0000000001"),
                ("forms", []),
                ("identity_snapshot_id", None),
                ("account_id", str(uuid4())),
            ):
                with pytest.raises(DBAPIError):
                    async with db.begin_nested():
                        await db.execute(
                            text(
                                "SELECT sec_foundation_require("
                                "to_jsonb(r) || CAST(:patch AS jsonb)) "
                                "FROM sec_monitor_rules r WHERE id=:id"
                            ),
                            {"id": rule.id, "patch": json.dumps({field: value})},
                        )
        async with factory.begin() as db:
            with pytest.raises(DBAPIError):
                async with db.begin_nested():
                    await db.execute(
                        text(
                            "UPDATE data_snapshots SET schema_version='wrong_version' WHERE id=:id"
                        ),
                        {"id": rule.identity_snapshot_id},
                    )
                    await db.execute(
                        text(
                            "SELECT sec_foundation_require(to_jsonb(r)) "
                            "FROM sec_monitor_rules r WHERE id=:id"
                        ),
                        {"id": rule.id},
                    )
            assert before[1:3] == [
                await db.scalar(text(f"SELECT count(*) FROM {t}"))
                for t in ("agent_research_runs", "jobs")
            ]
            assert before[4] == await db.scalar(
                text("SELECT count(*) FROM agent_daily_budget_reservations")
            )
        # An orphan source cannot commit, even though it carries a syntactic hash.
        async with factory.begin() as db:
            with pytest.raises(DBAPIError):
                async with db.begin_nested():
                    await db.execute(
                        text(
                            """INSERT INTO data_snapshots(
                            id,source,provider_object_id,revision,schema_version,payload,
                            created_at,observed_at,available_at,received_at)
                            SELECT :id,source,:other,revision,schema_version,payload,
                            clock_timestamp(),clock_timestamp(),clock_timestamp(),clock_timestamp()
                            FROM data_snapshots WHERE source='nexus_sec_foundation_scope'
                            AND provider_object_id=:old"""
                        ),
                        {"id": uuid4(), "other": str(uuid4()), "old": str(rule.id)},
                    )
                    await db.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        paused = await run(
            settings,
            parser().parse_args(["pause-sec-monitor-rule", "--rule-id", result["rule_id"]]),
        )
        assert paused["status"] == "paused"
        assert (await run(settings, args))["status"] == "paused"
        async with factory.begin() as db:
            now = await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            stopped = await scan_account(
                db,
                account_id=rule.account_id,
                calendar_authority_id=CALENDAR_ID,
                now=now,
                instrument_ids=(rule.instrument_id,),
            )
            assert (
                stopped["checks"] == 0
                and stopped["notifications"] == 0
                and stopped["inactive"] == 1
            )

        # A NULL anchor without the exact origin remains rejected by SQL.
        async with factory.begin() as db:
            with pytest.raises(DBAPIError):
                async with db.begin_nested():
                    await db.execute(
                        text(
                            "INSERT INTO sec_monitor_rules(id,account_id,instrument_id,issuer_cik,"
                            "identity_snapshot_id,anchor_plan_id,effective_at,forms,"
                            "policy_version,created_at) "
                            "SELECT :id,account_id,instrument_id,issuer_cik,"
                            "identity_snapshot_id,NULL,"
                            "clock_timestamp(),'[\"8-K\"]'::jsonb,"
                            "'durable_sec_v1',clock_timestamp() "
                            "FROM sec_monitor_rules WHERE id=:old"
                        ),
                        {"id": uuid4(), "old": rule.id},
                    )
    finally:
        await factory.kw["bind"].dispose()
