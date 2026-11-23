"""Use indexed lookups in the deferred SEC discovery audit check."""

from alembic import op
from sqlalchemy import text

revision = "0139_sec_audit_lookup"
down_revision = "0138_sec_accepted_cutoff"
branch_labels = None
depends_on = None

_ROW_LOOKUP = (
    "  EXECUTE format('SELECT to_jsonb(x) FROM public.%I x "
    "WHERE public.sec_durable_row_id(%L,to_jsonb(x))=$1',t,t) INTO v USING rid;"
)
_INDEXED_ROW_LOOKUP = f"""  IF t='sec_discovery_checks' THEN
   SELECT to_jsonb(x) INTO v FROM public.sec_discovery_checks x
   WHERE x.id=CASE WHEN rid ~ '^[0-9a-f]{{8}}(-[0-9a-f]{{4}}){{3}}-[0-9a-f]{{12}}$'
                   THEN rid::uuid END;
  ELSE
{_ROW_LOOKUP}
  END IF;"""

_AUDIT_MATCH = (
    "  AND e.occurred_at=(v->>'created_at')::timestamptz "
    "AND e.payload=jsonb_build_object('table',t,'row_id',rid,'binding',v-'created_at');"
)
_INDEXED_AUDIT_MATCH = "  AND e.payload->>'table'=t AND e.payload->>'row_id'=rid\n" + _AUDIT_MATCH


def _rewrite(*, forward: bool) -> None:
    definition = op.get_bind().scalar(
        text("SELECT pg_get_functiondef('public.sec_durable_complete()'::regprocedure)")
    )
    if not isinstance(definition, str):
        raise RuntimeError("SEC_DURABLE_AUDIT_LOOKUP_PREDECESSOR_INVALID")
    for original, indexed in (
        (_ROW_LOOKUP, _INDEXED_ROW_LOOKUP),
        (_AUDIT_MATCH, _INDEXED_AUDIT_MATCH),
    ):
        old, new = (original, indexed) if forward else (indexed, original)
        if definition.count(old) != 1:
            raise RuntimeError("SEC_DURABLE_AUDIT_LOOKUP_PREDECESSOR_INVALID")
        definition = definition.replace(old, new)
    op.get_bind().exec_driver_sql(definition)


def upgrade() -> None:
    _rewrite(forward=True)


def downgrade() -> None:
    _rewrite(forward=False)
