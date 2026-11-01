"""Frozen, guarded lookup correction for 0107 backup recovery and migration.

The prosrc hashes come from the literal SQL bodies in migrations 0106/0107.
Only the wrapper's two references change; its policy and the v1 body stay frozen.
"""

PLATFORM_LOOKUP_REPAIR = """
DO $platform_lookup$
DECLARE
  wrapper oid := pg_catalog.to_regprocedure('public.research_platform_manifest_valid(jsonb)');
  original oid := pg_catalog.to_regprocedure('public.research_platform_manifest_v1_valid(jsonb)');
  source text;
  original_source text;
  fingerprint text;
  definition text;
BEGIN
  SELECT prosrc INTO source FROM pg_catalog.pg_proc
    WHERE oid=wrapper AND prokind='f' AND provolatile='i' AND NOT prosecdef
      AND NOT proisstrict AND NOT proleakproof AND proconfig IS NULL
      AND NOT proretset AND proparallel='u' AND procost=100 AND prorows=0
      AND pronargdefaults=0 AND prosupport=0 AND provariadic=0 AND proargnames=ARRAY['p']
      AND prorettype='pg_catalog.bool'::pg_catalog.regtype
      AND prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='sql');
  SELECT prosrc INTO original_source FROM pg_catalog.pg_proc
    WHERE oid=original AND prokind='f' AND provolatile='i' AND NOT prosecdef
      AND NOT proisstrict AND NOT proleakproof AND proconfig IS NULL
      AND NOT proretset AND proparallel='u' AND procost=100 AND prorows=0
      AND pronargdefaults=0 AND prosupport=0 AND provariadic=0 AND proargnames=ARRAY['p']
      AND prorettype='pg_catalog.bool'::pg_catalog.regtype
      AND prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='sql');
  fingerprint := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(source,'UTF8')),'hex');
  IF source IS NULL OR original_source IS NULL OR fingerprint NOT IN (
      '8b80d91bbf5a0d052d3fd1d19433232d571864a36cde64b04b8746a35266ea6b',
      '679a8c66646cf26501795ee5985ad64108c2c5a8c8b189a43b597afa08e821d8')
    OR pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(original_source,'UTF8')),'hex')
      <> '58ec2c802622361809de1ea252acec25e870aa870e169d3957f074bda6bba4ae' THEN
    RAISE EXCEPTION 'PLATFORM_RESTORE_FUNCTION_UNEXPECTED';
  END IF;
  IF fingerprint='8b80d91bbf5a0d052d3fd1d19433232d571864a36cde64b04b8746a35266ea6b' THEN
    definition := pg_catalog.pg_get_functiondef(wrapper);
    IF (length(source)-length(replace(source,'research_platform_manifest_v1_valid(','')))
      /length('research_platform_manifest_v1_valid(') <> 2 THEN
      RAISE EXCEPTION 'PLATFORM_RESTORE_FUNCTION_UNEXPECTED';
    END IF;
    EXECUTE replace(definition,'research_platform_manifest_v1_valid(',
      'public.research_platform_manifest_v1_valid(');
  END IF;
END $platform_lookup$;
"""
