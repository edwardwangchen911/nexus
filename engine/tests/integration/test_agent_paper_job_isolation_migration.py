"""Legacy Agent paper jobs are normalized into the explicit worker namespace."""

from io import StringIO
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD

ENGINE_ROOT = Path(__file__).parents[2]
MIGRATION_HEAD = "0061_agent_paper_job_isolation"
PREVIOUS_HEAD = "0060_agent_paper_loop_heartbeat"


def test_0061_is_the_single_head() -> None:
    scripts = ScriptDirectory.from_config(Config(str(ENGINE_ROOT / "alembic.ini")))

    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]
    revision = scripts.get_revision(MIGRATION_HEAD)
    assert revision is not None
    assert revision.down_revision == PREVIOUS_HEAD


def test_0061_offline_sql_retypes_only_agent_paper_actor_jobs_and_reverses() -> None:
    output = StringIO()
    config = Config(str(ENGINE_ROOT / "alembic.ini"), output_buffer=output)
    command.upgrade(config, f"{PREVIOUS_HEAD}:{MIGRATION_HEAD}", sql=True)

    sql = output.getvalue()
    assert "DISABLE TRIGGER trg_jobs_state" in sql
    assert "run.actor_id='local-agent-paper-cycle'" in sql
    assert "job.job_type='agent.research'" in sql
    assert "SET job_type='agent.paper.research'" in sql
    assert "job_key='agent.paper.research' || substring" in sql

    rollback = StringIO()
    rollback_config = Config(str(ENGINE_ROOT / "alembic.ini"), output_buffer=rollback)
    command.downgrade(rollback_config, f"{MIGRATION_HEAD}:{PREVIOUS_HEAD}", sql=True)
    rollback_sql = rollback.getvalue()
    assert "agent paper snapshots prevent downgrade" in rollback_sql
    assert "source='nexus_agent_paper'" in rollback_sql
    assert "run.actor_id='local-agent-paper-cycle'" in rollback_sql
    assert "job.job_type='agent.paper.research'" in rollback_sql
    assert "SET job_type='agent.research'" in rollback_sql
    assert "job_key='agent.research' || substring" in rollback_sql
