"""Small reusable assertions for reflected migration schemas."""

from sqlalchemy import text
from sqlalchemy.engine import Connection


def created_at_trigger_exists(connection: Connection, table_name: str) -> bool:
    trigger_name = f"trg_{table_name}_created_at_immutable"
    return bool(
        connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = :trigger_name "
                "AND NOT tgisinternal)"
            ),
            {"trigger_name": trigger_name},
        ).scalar_one()
    )
