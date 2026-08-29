\if :{?provider_role}
\else
\echo 'provider_role psql variable is required'
\quit
\endif
\set ON_ERROR_STOP on
BEGIN;

-- The login role must be created and its credential delivered outside this repository.
-- Run this script as the schema owner after migrations, for the target database only.
DO $$ BEGIN
  IF current_user <> (
    SELECT pg_get_userbyid(relation.relowner)
    FROM pg_class AS relation WHERE relation.oid = 'public.jobs'::regclass
  ) THEN
    RAISE EXCEPTION 'provider grant template must run as the jobs table owner';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_policies AS policy
    WHERE policy.schemaname = 'public'
      AND policy.tablename IN (
        'jobs','job_leases','job_attempts','job_dependencies','worker_heartbeats'
      )
      AND (policy.tablename, policy.policyname) NOT IN (
        ('jobs', 'nexus_provider_jobs_scope'),
        ('jobs', 'nexus_non_provider_jobs_bypass'),
        ('job_leases', 'nexus_provider_job_leases_scope'),
        ('job_leases', 'nexus_non_provider_job_leases_bypass'),
        ('job_attempts', 'nexus_provider_job_attempts_scope'),
        ('job_attempts', 'nexus_non_provider_job_attempts_bypass'),
        ('job_dependencies', 'nexus_provider_job_dependencies_scope'),
        ('job_dependencies', 'nexus_non_provider_job_dependencies_bypass'),
        ('worker_heartbeats', 'nexus_provider_worker_heartbeats_scope'),
        ('worker_heartbeats', 'nexus_non_provider_worker_heartbeats_bypass')
      )
  ) THEN
    RAISE EXCEPTION 'unexpected shared-runtime RLS policy requires manual review';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM pg_proc AS procedure
    WHERE procedure.pronamespace = 'public'::regnamespace
      AND procedure.proname IN (
        'nexus_provider_job_visible','nexus_provider_job_writable',
        'nexus_provider_job_owned','nexus_provider_queue'
      )
      AND (procedure.proname, pg_get_function_identity_arguments(procedure.oid))
        NOT IN (
          ('nexus_provider_job_visible', 'p_queue text, p_job_type text'),
          ('nexus_provider_job_writable', 'p_queue text, p_job_type text'),
          ('nexus_provider_job_owned', 'p_job_id uuid'),
          ('nexus_provider_queue', 'p_queue text')
        )
  ) THEN
    RAISE EXCEPTION 'unexpected provider policy function overload requires manual review';
  END IF;
END $$;
SELECT format($guard$
DO $provider_role_guard$
DECLARE provider pg_roles%ROWTYPE;
BEGIN
  SELECT * INTO provider FROM pg_roles WHERE rolname = %1$L;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'provider role does not exist';
  END IF;
  IF provider.oid = current_user::regrole THEN
    RAISE EXCEPTION 'provider role must not own the shared runtime schema';
  END IF;
  IF provider.rolsuper OR provider.rolbypassrls OR provider.rolcreatedb
     OR provider.rolcreaterole OR provider.rolreplication THEN
    RAISE EXCEPTION 'provider role has forbidden role attributes';
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_roles AS granted_role
    WHERE granted_role.oid <> provider.oid
      AND pg_has_role(provider.oid, granted_role.oid, 'MEMBER')
  ) THEN
    RAISE EXCEPTION 'provider role memberships require manual review';
  END IF;
END $provider_role_guard$
$guard$, :'provider_role')
\gexec
SELECT format('REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM %I', :'provider_role')
\gexec
SELECT format(
  'REVOKE ALL PRIVILEGES (%I) ON TABLE public.%I FROM %I',
  column_name, table_name, :'provider_role'
)
FROM information_schema.columns
WHERE table_schema = 'public'
\gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'provider_role')
\gexec
SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'provider_role')
\gexec
SELECT format('REVOKE CREATE ON SCHEMA public FROM %I', :'provider_role')
\gexec

SELECT format(
  'GRANT SELECT ON TABLE %s TO %I',
  string_agg(format('public.%I', table_name), ', ' ORDER BY table_name),
  :'provider_role'
)
FROM (VALUES
  ('jobs'), ('job_leases'), ('job_attempts'), ('job_dependencies'),
  ('worker_heartbeats'), ('integration_status'), ('provider_capabilities'),
  ('source_policies'), ('instruments'), ('instrument_aliases'),
  ('universe_snapshots'), ('universe_members'), ('watchlists'),
  ('watchlist_members'), ('exchange_calendar_authorities'),
  ('exchange_calendar_sessions'), ('data_artifacts'),
  ('research_evidence_refs'), ('price_series_manifests'),
  ('price_bar_records'), ('price_bar_set_authorities'),
  ('intraday_bar_set_authorities'), ('latest_quotes'), ('filings'),
  ('financial_facts'), ('corporate_actions'), ('macro_observations')
) AS allowed(table_name)
\gexec

SELECT format(
  'GRANT INSERT ON TABLE %s TO %I',
  string_agg(format('public.%I', table_name), ', ' ORDER BY table_name),
  :'provider_role'
)
FROM (VALUES
  ('jobs'), ('job_leases'), ('job_attempts'), ('worker_heartbeats'),
  ('data_artifacts'), ('research_evidence_refs'), ('price_series_manifests'),
  ('price_bar_records'), ('price_bar_set_authorities'),
  ('intraday_bar_set_authorities'), ('latest_quotes'), ('filings'),
  ('financial_facts'), ('corporate_actions'), ('macro_observations')
) AS allowed(table_name)
\gexec

SELECT format(
  'GRANT UPDATE ON TABLE public.%I TO %I', table_name, :'provider_role'
)
FROM (VALUES
  ('jobs'), ('job_leases'), ('job_attempts'), ('worker_heartbeats')
) AS allowed(table_name)
\gexec

CREATE OR REPLACE FUNCTION public.nexus_provider_job_visible(
  p_queue text, p_job_type text
) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE SECURITY INVOKER
SET search_path = public, pg_catalog
AS $$
  SELECT (p_queue = 'provider' AND p_job_type IN
    ('collect.market','collect.sec_macro','collect.intraday_snapshot'))
    OR (p_queue = 'general' AND p_job_type = 'daily.pipeline')
$$;
CREATE OR REPLACE FUNCTION public.nexus_provider_job_writable(
  p_queue text, p_job_type text
) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE SECURITY INVOKER
SET search_path = public, pg_catalog
AS $$
  SELECT p_queue = 'provider' AND p_job_type IN
    ('collect.market','collect.sec_macro','collect.intraday_snapshot')
$$;
CREATE OR REPLACE FUNCTION public.nexus_provider_job_owned(
  p_job_id uuid
) RETURNS boolean
LANGUAGE sql STABLE PARALLEL SAFE SECURITY INVOKER
SET search_path = public, pg_catalog
AS $$
  SELECT EXISTS (
    SELECT 1 FROM public.jobs AS job
    WHERE job.id = p_job_id
      AND public.nexus_provider_job_writable(job.queue, job.job_type)
  )
$$;
CREATE OR REPLACE FUNCTION public.nexus_provider_queue(
  p_queue text
) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE SECURITY INVOKER
SET search_path = public, pg_catalog
AS $$
  SELECT p_queue = 'provider'
$$;
ALTER FUNCTION public.nexus_provider_job_visible(text, text) OWNER TO CURRENT_USER;
ALTER FUNCTION public.nexus_provider_job_writable(text, text) OWNER TO CURRENT_USER;
ALTER FUNCTION public.nexus_provider_job_owned(uuid) OWNER TO CURRENT_USER;
ALTER FUNCTION public.nexus_provider_queue(text) OWNER TO CURRENT_USER;
REVOKE ALL ON FUNCTION public.nexus_provider_job_visible(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.nexus_provider_job_writable(text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.nexus_provider_job_owned(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.nexus_provider_queue(text) FROM PUBLIC;
SELECT format(
  'REVOKE ALL PRIVILEGES ON FUNCTION public.%I(%s) FROM %I',
  procedure.proname,
  pg_get_function_identity_arguments(procedure.oid),
  grantee.rolname
)
FROM pg_proc AS procedure
CROSS JOIN LATERAL aclexplode(
  COALESCE(procedure.proacl, acldefault('f', procedure.proowner))
) AS acl
JOIN pg_roles AS grantee ON grantee.oid = acl.grantee
WHERE procedure.pronamespace = 'public'::regnamespace
  AND procedure.proname IN (
    'nexus_provider_job_visible','nexus_provider_job_writable',
    'nexus_provider_job_owned','nexus_provider_queue'
  )
  AND acl.privilege_type = 'EXECUTE'
  AND acl.grantee <> procedure.proowner
  AND grantee.rolname <> :'provider_role'
\gexec
SELECT format(
  'REVOKE ALL PRIVILEGES ON FUNCTION public.%I(%s) FROM %I',
  function_name, arguments, :'provider_role'
)
FROM (VALUES
  ('nexus_provider_job_visible', 'text, text'),
  ('nexus_provider_job_writable', 'text, text'),
  ('nexus_provider_job_owned', 'uuid'),
  ('nexus_provider_queue', 'text')
) AS allowed(function_name, arguments)
\gexec
SELECT format(
  'GRANT EXECUTE ON FUNCTION public.%I(%s) TO %I', function_name, arguments,
  :'provider_role'
)
FROM (VALUES
  ('nexus_provider_job_visible', 'text, text'),
  ('nexus_provider_job_writable', 'text, text'),
  ('nexus_provider_job_owned', 'uuid'),
  ('nexus_provider_queue', 'text')
) AS allowed(function_name, arguments)
\gexec

ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs FORCE ROW LEVEL SECURITY;
ALTER TABLE job_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE job_leases FORCE ROW LEVEL SECURITY;
ALTER TABLE job_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE job_attempts FORCE ROW LEVEL SECURITY;
ALTER TABLE job_dependencies ENABLE ROW LEVEL SECURITY;
ALTER TABLE job_dependencies FORCE ROW LEVEL SECURITY;
ALTER TABLE worker_heartbeats ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_heartbeats FORCE ROW LEVEL SECURITY;

SELECT format(
  'DROP POLICY IF EXISTS nexus_provider_%1$I_scope ON public.%1$I', table_name
)
FROM (VALUES ('jobs'), ('job_leases'), ('job_attempts'), ('job_dependencies'),
             ('worker_heartbeats')) AS scoped(table_name)
\gexec
SELECT format(
  'DROP POLICY IF EXISTS nexus_non_provider_%1$I_bypass ON public.%1$I', table_name
)
FROM (VALUES ('jobs'), ('job_leases'), ('job_attempts'), ('job_dependencies'),
             ('worker_heartbeats')) AS scoped(table_name)
\gexec

SELECT format(
  'CREATE POLICY nexus_non_provider_jobs_bypass ON public.jobs TO PUBLIC '
  'USING (NOT pg_has_role(current_user, %L, ''MEMBER'')) '
  'WITH CHECK (NOT pg_has_role(current_user, %L, ''MEMBER''))',
  :'provider_role', :'provider_role'
)
\gexec
SELECT format(
  'CREATE POLICY nexus_provider_jobs_scope ON public.jobs TO %I '
  'USING (public.nexus_provider_job_visible(queue, job_type)) '
  'WITH CHECK (public.nexus_provider_job_writable(queue, job_type))',
  :'provider_role'
)
\gexec

SELECT format(
  'CREATE POLICY nexus_non_provider_%1$I_bypass ON public.%1$I TO PUBLIC '
  'USING (NOT pg_has_role(current_user, %2$L, ''MEMBER'')) '
  'WITH CHECK (NOT pg_has_role(current_user, %2$L, ''MEMBER''))',
  table_name, :'provider_role'
)
FROM (VALUES ('job_leases'), ('job_attempts'), ('job_dependencies')) AS scoped(table_name)
\gexec
SELECT format(
  'CREATE POLICY nexus_provider_%1$I_scope ON public.%1$I TO %2$I '
  'USING (public.nexus_provider_job_owned(job_id)) '
  'WITH CHECK (public.nexus_provider_job_owned(job_id))',
  table_name, :'provider_role'
)
FROM (VALUES ('job_leases'), ('job_attempts'), ('job_dependencies')) AS scoped(table_name)
\gexec

SELECT format(
  'CREATE POLICY nexus_non_provider_worker_heartbeats_bypass '
  'ON public.worker_heartbeats TO PUBLIC '
  'USING (NOT pg_has_role(current_user, %L, ''MEMBER'')) '
  'WITH CHECK (NOT pg_has_role(current_user, %L, ''MEMBER''))',
  :'provider_role', :'provider_role'
)
\gexec
SELECT format(
  'CREATE POLICY nexus_provider_worker_heartbeats_scope '
  'ON public.worker_heartbeats TO %I '
  'USING (public.nexus_provider_queue(queue)) '
  'WITH CHECK (public.nexus_provider_queue(queue))',
  :'provider_role'
)
\gexec
COMMIT;
