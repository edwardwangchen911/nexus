"""Test-only DBAPI COMMIT acknowledgement fault, never a production retry hook.

Inject inside SQLAlchemy's dialect commit boundary so Session.commit never
returns success. The server_commits branch still calls the real PostgreSQL
adapter; it differs from raising after application-level commit returned.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine


class CommitAcknowledgementLost(RuntimeError):
    pass


@dataclass
class CommitFaultEvidence:
    boundary_calls: int = 0
    server_commit_completed: bool = False


@contextmanager
def lose_commit_ack(engine: AsyncEngine, *, server_commits: bool) -> Iterator[CommitFaultEvidence]:
    """Fault exactly one commit; caller must not retry or infer its outcome.

    Use a dedicated engine with no concurrent writer. A separate engine must
    read the durable result, including in the actual-commit branch.
    """
    dialect = engine.sync_engine.dialect
    original = dialect.do_commit
    evidence = CommitFaultEvidence()

    def fail_commit(connection):
        evidence.boundary_calls += 1
        if evidence.boundary_calls != 1:
            raise AssertionError("unexpected repeated COMMIT during acknowledgement fault")
        # Restore immediately: a later safe stop transaction is not a retry
        # of the uncertain authorization/observation commit.
        dialect.do_commit = original
        if server_commits:
            original(connection)
            evidence.server_commit_completed = True
        else:
            # The application receives the identical failure although no COMMIT
            # reached PostgreSQL; explicitly end this isolated transaction.
            dialect.do_rollback(connection)
        raise CommitAcknowledgementLost("COMMIT_OUTCOME_UNCONFIRMED")

    dialect.do_commit = fail_commit
    try:
        yield evidence
    finally:
        dialect.do_commit = original
