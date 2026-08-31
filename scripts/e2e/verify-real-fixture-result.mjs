import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const RESULT_PATH = "test-results/real-e2e-results.json";
const EXPECTED_TITLE = "proves the real BFF fixture CAS replay and persisted integration state";

export class RealFixtureResultError extends Error {
  constructor() {
    super("real fixture E2E result is invalid");
  }
}

export function validateRealFixtureReport(report) {
  if (!report || typeof report !== "object") throw new RealFixtureResultError();
  const stats = report.stats;
  if (
    !stats
    || stats.expected !== 1
    || stats.skipped !== 0
    || stats.unexpected !== 0
    || stats.flaky !== 0
  ) throw new RealFixtureResultError();

  const specs = collectSpecs(report.suites);
  if (specs.length !== 1) throw new RealFixtureResultError();
  const spec = specs[0];
  const tests = Array.isArray(spec.tests) ? spec.tests : [];
  if (
    spec.title !== EXPECTED_TITLE
    || spec.ok !== true
    || tests.length !== 1
    || tests[0]?.status !== "expected"
    || !Array.isArray(tests[0]?.results)
    || tests[0].results.length !== 1
    || tests[0].results[0]?.status !== "passed"
  ) throw new RealFixtureResultError();
}

function collectSpecs(suites) {
  if (!Array.isArray(suites)) return [];
  return suites.flatMap((suite) => [
    ...(Array.isArray(suite?.specs) ? suite.specs : []),
    ...collectSpecs(suite?.suites),
  ]);
}

function main() {
  try {
    const report = JSON.parse(readFileSync(RESULT_PATH, "utf8"));
    validateRealFixtureReport(report);
  } catch {
    process.stderr.write("real fixture E2E result is invalid\n");
    process.exitCode = 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
