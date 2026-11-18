"""Preserve canonical JSON bytes without quadratic UTF-8 string traversal."""

from alembic import op
from sqlalchemy import text

revision = "0137_linear_json_canonical"
down_revision = "0136_sec_adopted_content"
branch_labels = None
depends_on = None

_CANONICAL = r"""
CREATE OR REPLACE FUNCTION public.nexus_mixed_receipt_canonical(value jsonb)
RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT SET search_path=pg_catalog,public AS $$
DECLARE result text; raw text;
BEGIN
 CASE jsonb_typeof(value)
 WHEN 'object' THEN
  SELECT '{'||coalesce(string_agg(public.nexus_mixed_receipt_canonical(to_jsonb(key))||':'||public.nexus_mixed_receipt_canonical(v),',' ORDER BY key COLLATE "C"),'')||'}'
   INTO result FROM jsonb_each(value) x(key,v);
 WHEN 'array' THEN
  SELECT '['||coalesce(string_agg(public.nexus_mixed_receipt_canonical(v),',' ORDER BY ord),'')||']'
   INTO result FROM jsonb_array_elements(value) WITH ORDINALITY x(v,ord);
 WHEN 'string' THEN
  raw:=to_json(value #>> '{}')::text;
  -- JSON already escapes control characters. Printable ASCII needs no rewrite.
  -- DEL (127) must still take the historical \u007f path.
  IF raw COLLATE "C" !~ '[^ -~]' THEN RETURN raw; END IF;
  SELECT string_agg(CASE
    WHEN cp<127 THEN ch
    WHEN cp<=65535 THEN E'\\u'||lpad(to_hex(cp),4,'0')
    ELSE E'\\u'||to_hex(55296+((cp-65536)/1024))||E'\\u'||to_hex(56320+((cp-65536)%1024))
   END,'' ORDER BY ord) INTO result
   FROM (SELECT ch,ord,ascii(ch) cp
     FROM regexp_split_to_table(raw,'') WITH ORDINALITY x(ch,ord)) chars;
 ELSE result:=value::text;
 END CASE;
 RETURN result;
END $$;
"""


def upgrade() -> None:
    previous = op.get_bind().scalar(
        text(
            "SELECT pg_get_functiondef('public.nexus_mixed_receipt_canonical(jsonb)'::regprocedure)"
        )
    )
    if not isinstance(previous, str) or "FOR i IN 1..length(raw) LOOP" not in previous:
        raise RuntimeError("JSON_CANONICAL_PREDECESSOR_INVALID")
    op.execute("CREATE TABLE json_canonical_function_backup (definition text NOT NULL)")
    op.get_bind().execute(
        text("INSERT INTO json_canonical_function_backup VALUES (:definition)"),
        {"definition": previous},
    )
    op.execute(_CANONICAL)


def downgrade() -> None:
    previous = op.get_bind().scalar(text("SELECT definition FROM json_canonical_function_backup"))
    if not isinstance(previous, str):
        raise RuntimeError("JSON_CANONICAL_PREDECESSOR_INVALID")
    op.execute(previous)
    op.execute("DROP TABLE json_canonical_function_backup")
