import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const checkOnly = process.argv.includes("--check");
const targets = {
  openapi: join(projectRoot, "contracts/openapi.json"),
  schema: join(projectRoot, "app/lib/generated/schema.d.ts"),
};
const temporaryRoot = mkdtempSync(join(tmpdir(), "nexus-openapi-"));

try {
  const openapi = generateOpenApi();
  const temporaryOpenApi = join(temporaryRoot, "openapi.json");
  const temporarySchema = join(temporaryRoot, "schema.d.ts");
  writeFileSync(temporaryOpenApi, openapi);
  execFileSync(
    join(projectRoot, "node_modules/.bin/openapi-typescript"),
    [temporaryOpenApi, "--output", temporarySchema, "--alphabetize"],
    { cwd: projectRoot, env: { ...process.env, NO_COLOR: "1" }, stdio: ["ignore", "pipe", "pipe"] },
  );
  const schema = normalize(readFileSync(temporarySchema, "utf8"));

  if (checkOnly) {
    const stale = [
      ...mismatch(targets.openapi, openapi),
      ...mismatch(targets.schema, schema),
    ];
    if (stale.length > 0) {
      process.stderr.write(`Generated contracts are missing or stale:\n${stale.join("\n")}\n`);
      process.exitCode = 1;
    }
  } else {
    writeTarget(targets.openapi, openapi);
    writeTarget(targets.schema, schema);
  }
} finally {
  rmSync(temporaryRoot, { recursive: true, force: true });
}

function generateOpenApi() {
  const source = execFileSync(
    "uv",
    [
      "run",
      "--package",
      "nexus-engine",
      "python",
      "-c",
      [
        "import json",
        "from nexus.main import create_engine_app",
        "from nexus.settings import EngineSettings",
        "print(json.dumps(create_engine_app(EngineSettings.for_test()).openapi(), sort_keys=True, separators=(',', ':')))",
      ].join("; "),
    ],
    { cwd: projectRoot, encoding: "utf8", env: process.env, stdio: ["ignore", "pipe", "pipe"] },
  );
  return `${JSON.stringify(JSON.parse(source), null, 2)}\n`;
}

function mismatch(path, expected) {
  if (!existsSync(path)) return [`- missing ${path.slice(projectRoot.length + 1)}`];
  return readFileSync(path, "utf8") === expected
    ? []
    : [`- stale ${path.slice(projectRoot.length + 1)}`];
}

function normalize(value) {
  return `${value.replaceAll("\r\n", "\n").trimEnd()}\n`;
}

function writeTarget(path, contents) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, contents);
}
