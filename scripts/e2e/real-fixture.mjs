import { spawn } from "node:child_process";
import { readdirSync, rmSync } from "node:fs";
import { join } from "node:path";

/** @typedef {Record<string, string | undefined>} Environment */
/** @typedef {import("node:events").EventEmitter & {kill?: (signal?: NodeJS.Signals) => boolean, exitCode?: number | null, signalCode?: NodeJS.Signals | null, stdin?: {end: () => void}, stdout?: import("node:events").EventEmitter}} ChildProcessLike */
/** @typedef {(executable: string, args: string[], options: Record<string, unknown>) => ChildProcessLike} SpawnProcess */
/** @typedef {(executable: string, args: string[], env: Record<string, string>, options: {stage: string, abortSignal: AbortSignal}) => Promise<number>} ChildRunner */
/** @typedef {{stop: () => Promise<void>, failure?: Promise<never>}} DatabaseGuard */

const FIXTURE_HMAC_SECRET = "nexus-real-e2e-synthetic-hmac-secret-32-bytes";
const FIXTURE_HMAC_KEY_ID = "real-fixture";
const ENGINE_URL = "http://127.0.0.1:8100";
const RESULT_PATH = "test-results/real-e2e-results.json";
const SAFE_PARENT_ENVIRONMENT_KEYS = ["PATH", "LANG", "LC_ALL", "TZ", "TMPDIR", "TMP", "TEMP"];
const GUARD_READY = "NEXUS_REAL_E2E_GUARD_READY";

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
  let databaseName;
  try {
    databaseName = decodeURIComponent(url.pathname.slice(1));
  } catch {
    throw new RealFixtureRunnerError("INVALID_DATABASE_URL", "real fixture E2E database URL is invalid");
  }
  const loopbackHost = url.hostname === "127.0.0.1" || url.hostname === "[::1]";
  if (
    url.protocol !== "postgresql+asyncpg:"
    || !loopbackHost
    || !databaseName.startsWith("nexus_e2e_")
    || databaseName.includes("/")
    || url.search !== ""
    || url.hash !== ""
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
    CLOUDFLARE_LOAD_DEV_VARS_FROM_DOT_ENV: "false",
    CLOUDFLARE_INCLUDE_PROCESS_ENV: "false",
  };
}

/** @param {string[]} args */
export function validatePlaywrightArguments(args) {
  if (!Array.isArray(args) || args.length !== 0) {
    throw new RealFixtureRunnerError("INVALID_ARGUMENTS", "real fixture E2E arguments are invalid");
  }
  return [];
}

/** @param {string} projectRoot */
export function validateProjectRoot(projectRoot) {
  let entries;
  try {
    entries = readdirSync(projectRoot);
  } catch {
    throw new RealFixtureRunnerError("INVALID_PROJECT_ROOT", "real fixture E2E project root is invalid");
  }
  if (entries.some((entry) => (
    entry === ".env"
    || entry.startsWith(".env.")
    || entry === ".dev.vars"
    || entry.startsWith(".dev.vars.")
  ))) {
    throw new RealFixtureRunnerError(
      "PROJECT_CONFIGURATION_PRESENT",
      "real fixture E2E project configuration is disallowed",
    );
  }
}

/**
 * @param {string} executable
 * @param {string[]} args
 * @param {Record<string, string>} env
 * @param {{spawnProcess?: SpawnProcess, signalSource?: NodeJS.Process | import("node:events").EventEmitter, stage?: string, writeDiagnostic?: (line: string) => void, abortSignal?: AbortSignal}} options
 */
export function runSubprocess(executable, args, env, {
  spawnProcess = spawn,
  signalSource = process,
  stage = "subprocess",
  writeDiagnostic = (line) => process.stderr.write(`${line}\n`),
  abortSignal,
} = {}) {
  return new Promise((resolve, reject) => {
    writeDiagnostic(`[real-e2e] ${stage} started`);
    const child = spawnProcess(executable, args, { env, stdio: "ignore" });
    let settled = false;
    const forwardSignal = (signal) => {
      if (!settled && typeof child.kill === "function") child.kill(signal);
    };
    const onSigint = () => forwardSignal("SIGINT");
    const onSigterm = () => forwardSignal("SIGTERM");
    const onAbort = () => forwardSignal("SIGTERM");
    signalSource.on("SIGINT", onSigint);
    signalSource.on("SIGTERM", onSigterm);
    abortSignal?.addEventListener("abort", onAbort, { once: true });
    if (abortSignal?.aborted) onAbort();
    const cleanup = () => {
      signalSource.off("SIGINT", onSigint);
      signalSource.off("SIGTERM", onSigterm);
      abortSignal?.removeEventListener("abort", onAbort);
    };
    child.once("error", () => {
      settled = true;
      cleanup();
      writeDiagnostic(`[real-e2e] ${stage} failed to start`);
      reject(new RealFixtureRunnerError("SPAWN_FAILED", "real fixture E2E runner could not start"));
    });
    child.once("close", (code) => {
      if (settled) return;
      settled = true;
      cleanup();
      const exitCode = typeof code === "number" ? code : 1;
      writeDiagnostic(`[real-e2e] ${stage} exited with code ${exitCode}`);
      resolve(exitCode);
    });
  });
}

/**
 * @param {Record<string, string>} env
 * @param {{spawnProcess?: SpawnProcess, signalSource?: NodeJS.Process | import("node:events").EventEmitter}} options
 * @returns {Promise<DatabaseGuard>}
 */
export function startDatabaseGuard(env, { spawnProcess = spawn, signalSource = process } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawnProcess(
      "uv",
      ["run", "--package", "nexus-engine", "python", "scripts/e2e/real_fixture_db_guard.py"],
      { env, stdio: ["pipe", "pipe", "ignore"] },
    );
    let settled = false;
    let ready = false;
    let stopping = false;
    let output = "";
    let rejectFailure;
    const failure = new Promise((_, rejectGuardFailure) => {
      rejectFailure = rejectGuardFailure;
    });
    const forwardSignal = (signal) => {
      if (typeof child.kill === "function") child.kill(signal);
    };
    const onSigint = () => forwardSignal("SIGINT");
    const onSigterm = () => forwardSignal("SIGTERM");
    signalSource.on("SIGINT", onSigint);
    signalSource.on("SIGTERM", onSigterm);
    const cleanupListeners = () => {
      signalSource.off("SIGINT", onSigint);
      signalSource.off("SIGTERM", onSigterm);
    };
    const fail = () => {
      if (settled) return;
      settled = true;
      cleanupListeners();
      reject(new RealFixtureRunnerError("DATABASE_GUARD_FAILED", "real fixture E2E database guard failed"));
    };
    const timeout = setTimeout(() => {
      forwardSignal("SIGTERM");
      fail();
    }, 15_000);
    child.once("error", () => {
      clearTimeout(timeout);
      fail();
    });
    child.once("close", () => {
      clearTimeout(timeout);
      if (!ready) {
        fail();
      } else if (!stopping) {
        rejectFailure(new RealFixtureRunnerError(
          "DATABASE_GUARD_FAILED",
          "real fixture E2E database guard failed",
        ));
      }
    });
    child.stdout?.on("data", (chunk) => {
      if (settled) return;
      output += String(chunk);
      if (output.length > 128) {
        clearTimeout(timeout);
        forwardSignal("SIGTERM");
        fail();
        return;
      }
      const newline = output.indexOf("\n");
      if (newline < 0) return;
      clearTimeout(timeout);
      if (output.slice(0, newline).trim() !== GUARD_READY) {
        forwardSignal("SIGTERM");
        fail();
        return;
      }
      ready = true;
      settled = true;
      let stopPromise;
      resolve({
        failure,
        stop: () => {
          if (stopPromise) return stopPromise;
          stopPromise = new Promise((stopResolve) => {
            stopping = true;
            cleanupListeners();
            if (child.exitCode !== null || child.signalCode !== null) {
              stopResolve();
              return;
            }
            child.once("close", () => stopResolve());
            child.stdin?.end();
          });
          return stopPromise;
        },
      });
    });
  });
}

/**
 * @param {{ env?: Environment, args?: string[], projectRoot?: string, run?: ChildRunner, startGuard?: (env: Record<string, string>) => Promise<DatabaseGuard> }} options
 */
export async function runRealFixture({
  env = process.env,
  args = [],
  projectRoot = process.cwd(),
  run = runSubprocess,
  startGuard = startDatabaseGuard,
} = {}) {
  const childEnvironment = buildRealFixtureEnvironment(env);
  validatePlaywrightArguments(args);
  validateProjectRoot(projectRoot);
  rmSync(join(projectRoot, RESULT_PATH), { force: true });

  const guard = await startGuard(childEnvironment);
  const abortController = new AbortController();
  const runStage = (executable, stageArgs, stage) => {
    const stageRun = run(executable, stageArgs, childEnvironment, {
      abortSignal: abortController.signal,
      stage,
    });
    if (!guard.failure) return stageRun;
    return Promise.race([
      stageRun,
      guard.failure.catch((error) => {
        abortController.abort();
        throw error;
      }),
    ]);
  };
  try {
    const migrationExitCode = await runStage(
      "uv",
      ["run", "--package", "nexus-engine", "alembic", "-c", "engine/alembic.ini", "upgrade", "head"],
      "migration",
    );
    if (migrationExitCode !== 0) return migrationExitCode;
    const playwrightExitCode = await runStage(
      process.execPath,
      ["./node_modules/@playwright/test/cli.js", "test", "--config", "playwright.real.config.ts"],
      "playwright",
    );
    if (playwrightExitCode !== 0) return playwrightExitCode;
    return runStage(
      process.execPath,
      ["scripts/e2e/verify-real-fixture-result.mjs"],
      "result-verifier",
    );
  } finally {
    await guard.stop();
  }
}

export const realFixtureConstants = {
  ENGINE_URL,
  FIXTURE_HMAC_KEY_ID,
  FIXTURE_HMAC_SECRET,
  GUARD_READY,
  RESULT_PATH,
};
