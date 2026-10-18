"""Database authority cannot turn archive normalization into execution."""

from datetime import timedelta
from uuid import uuid4

import pytest
from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis
from nexus.paper.historical_assessment import copy_columns
from nexus.persistence.models.historical_assessment import HistoricalAssessmentSourceRecord
from nexus.persistence.models.paper import AgentPaperCycleRecord
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
async def test_archive_source_is_immutable_and_never_grants_execution(migrated_database_url):
    engine, sessions, args = await seed_synthesis(migrated_database_url)
    new_cycle_id, source_id = uuid4(), uuid4()
    outputs = [uuid4() for _ in range(3)]
    try:
        async with sessions.begin() as session:
            original = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            session.add(
                AgentPaperCycleRecord(
                    **copy_columns(
                        original,
                        id=new_cycle_id,
                        decision_slot_at=original.decision_slot_at + timedelta(seconds=1),
                    )
                )
            )
            await session.flush()
            at = args["now"]
            session.add(
                HistoricalAssessmentSourceRecord(
                    id=source_id,
                    cycle_id=new_cycle_id,
                    source_cycle_id=original.id,
                    archive_hash="a" * 64,
                    content_hash="b" * 64,
                    evidence_at=at,
                    observed_started_at=at,
                    observed_finished_at=at,
                    imported_at=at,
                    payload={
                        "archive": {},
                        "projection": {
                            "research": [
                                {
                                    "run_id": str(uuid4()),
                                    "output_id": str(output_id),
                                    "source_run_id": str(uuid4()),
                                    "source_output_id": str(uuid4()),
                                }
                                for output_id in outputs
                            ]
                        },
                    },
                )
            )
        for statement in (
            "UPDATE historical_assessment_sources SET content_hash=repeat('c',64) WHERE id=:id",
            "DELETE FROM historical_assessment_sources WHERE id=:id",
        ):
            with pytest.raises(DBAPIError):
                async with sessions.begin() as session:
                    await session.execute(text(statement), {"id": source_id})
        async with sessions.begin() as session:
            await session.execute(
                text("CREATE TABLE archive_decision_probe(agent_paper_cycle_id uuid)")
            )
            await session.execute(
                text(
                    "CREATE TRIGGER archive_decision_probe BEFORE INSERT ON archive_decision_probe "
                    "FOR EACH ROW EXECUTE FUNCTION nexus_historical_assessment_execution_guard()"
                )
            )
            await session.execute(
                text("CREATE TABLE archive_output_probe(agent_research_output_id uuid)")
            )
            await session.execute(
                text(
                    "CREATE TRIGGER archive_output_probe BEFORE INSERT ON archive_output_probe "
                    "FOR EACH ROW EXECUTE FUNCTION nexus_historical_assessment_output_guard()"
                )
            )
        for table, value in (
            ("archive_decision_probe", new_cycle_id),
            ("archive_output_probe", outputs[0]),
        ):
            with pytest.raises(DBAPIError, match="HISTORICAL_ASSESSMENT_.*FORBIDDEN"):
                async with sessions.begin() as session:
                    await session.execute(text(f"INSERT INTO {table} VALUES (:id)"), {"id": value})
    finally:
        await engine.dispose()
