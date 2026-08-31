import { spawn } from "node:child_process";

/** @typedef {Record<string, string | undefined>} Environment */
/** @typedef {(executable: string, args: string[], env: Record<string, string>) => Promise<number>} ChildRunner */

const FIXTURE_HMAC_SECRET = "nexus-real-e2e-synthetic-hmac-secret-32-bytes";
const FIXTURE_HMAC_KEY_ID = "real-fixture";
const ENGINE_URL = "http://127.0.0.1:8100";
const SAFE_PARENT_ENVIRONMENT_KEYS = ["PATH", "LANG", "LC_ALL", "TZ", "TMPDIR", "TMP", "TEMP"];

export class RealFixtureRunnerError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

/** @param {Environment} env */
export function validateRealFixtureInput(env) {
  if (env.NEXUS_ENV === "production") {
    throw new RealFixtureRunnerError("PRODUCTION_ENVIRONMENT", "real fixture E2E environment is disallowed");
  }
  const value = env.NEXUS_REAL_E2E_DATABASE_URL;
  if (typeof value !== "string" || value.length === 0) {
    throw new RealFixtureRunnerError("INVALID_DATABASE_URL", "real fixture E2E database URL is invalid");
  }

  let url;
  try {
    url = new URL(value);
  } catch {
    throw new RealFixtureRunnerError("INVALID_DATABASE_URL", "real fixture E2E database URL is invalid");
  }
  const databaseName = url.pathname.slice(1);
  const loopbackHost = url.hostname === "127.0.0.1" || url.hostname === "[::1]";
  if (
    url.protocol !== "postgresql+asyncpg:"
    || !loopbackHost
    || !databaseName.startsWith("nexus_e2e_")
    || databaseName.includes("/")
  ) {
    throw new RealFixtureRunnerError("INVALID_DATABASE_URL", "real fixture E2E database URL is invalid");
  }
  return value;
}

/** @param {Environment} env */
export function buildRealFixtureEnvironment(env) {
  const databaseUrl = validateRealFixtureInput(env);
  const child = {};
  for (const key of SAFE_PARENT_ENVIRONMENT_KEYS) {
    if (typeof env[key] === "string" && env[key].length > 0) child[key] = env[key];
  }
  return {
    ...child,
    DATABASE_URL: databaseUrl,
    NEXUS_ENV: "test",
    AGENT_RUNTIME_MODE: "fixture",
    NEXUS_PROVIDER_JOBS_ENABLED: "false",
    NEXUS_ENGINE_BASE_URL: ENGINE_URL,
    NEXUS_ENGINE_HMAC_CURRENT_KEY_ID: FIXTURE_HMAC_KEY_ID,
    NEXUS_ENGINE_HMAC_CURRENT_SECRET: FIXTURE_HMAC_SECRET,
    NEXUS_ALLOWED_USER_ID: "real-fixture-e2e-user",
  };
}

/** @param {string[]} args */
export function validatePlaywrightArguments(args) {
  if (!Array.isArray(args) || args.some((arg) => typeof arg !== "string" || arg === "--config" || arg.startsWith("--config=") || arg === "-c" || arg.startsWith("-c="))) {
    throw new RealFixtureRunnerError("INVALID_ARGUMENTS", "real fixture E2E arguments are invalid");
  }
  return [...args];
}

/** @type {ChildRunner} */
export function runSubprocess(executable, args, env) {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, { env, stdio: "ignore" });
    child.once("error", () => reject(new RealFixtureRunnerError("SPAWN_FAILED", "real fixture E2E runner could not start")));
    child.once("exit", (code, signal) => resolve(typeof code === "number" ? code : signal ? 1 : 1));
  });
}

/**
 * @param {{ env?: Environment, args?: string[], run?: ChildRunner }} options
 */
export async function runRealFixture({ env = process.env, args = [], run = runSubprocess } = {}) {
  const childEnvironment = buildRealFixtureEnvironment(env);
  const playwrightArguments = validatePlaywrightArguments(args);
  const migrationExitCode = await run(
    "uv",
    ["run", "--package", "nexus-engine", "alembic", "-c", "engine/alembic.ini", "upgrade", "head"],
    childEnvironment,
  );
  if (migrationExitCode !== 0) return migrationExitCode;
  return run(
    process.execPath,
    ["./node_modules/@playwright/test/cli.js", "test", "--config", "playwright.real.config.ts", ...playwrightArguments],
    childEnvironment,
  );
}

export const realFixtureConstants = {
  ENGINE_URL,
  FIXTURE_HMAC_KEY_ID,
  FIXTURE_HMAC_SECRET,
};
