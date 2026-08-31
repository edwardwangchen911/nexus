import { describe, expect, it } from "vitest";

import { validateRealFixtureReport } from "../../../scripts/e2e/verify-real-fixture-result.mjs";

const journeyTitle = "proves the real BFF fixture CAS replay and persisted integration state";

function report(overrides: Record<string, unknown> = {}) {
  return {
    stats: { expected: 1, skipped: 0, unexpected: 0, flaky: 0 },
    suites: [{
      suites: [],
      specs: [{
        title: journeyTitle,
        ok: true,
        tests: [{ status: "expected", results: [{ status: "passed" }] }],
      }],
    }],
    ...overrides,
  };
}

describe("real fixture result verifier", () => {
  it("accepts exactly the one expected passed journey", () => {
    expect(() => validateRealFixtureReport(report())).not.toThrow();
  });

  it.each([
    { overrides: { stats: { expected: 0, skipped: 0, unexpected: 0, flaky: 0 }, suites: [] }, scenario: "zero journeys" },
    { overrides: { stats: { expected: 2, skipped: 0, unexpected: 0, flaky: 0 } }, scenario: "two journeys" },
    { overrides: { stats: { expected: 0, skipped: 1, unexpected: 0, flaky: 0 } }, scenario: "skipped journey" },
    { overrides: { stats: { expected: 0, skipped: 0, unexpected: 1, flaky: 0 } }, scenario: "unexpected journey" },
    { overrides: { suites: [{ suites: [], specs: [{ title: "different journey", ok: true, tests: [{ status: "expected", results: [{ status: "passed" }] }] }] }] }, scenario: "renamed journey" },
    { overrides: { suites: [{ suites: [], specs: [{ title: journeyTitle, ok: false, tests: [{ status: "unexpected", results: [{ status: "failed" }] }] }] }] }, scenario: "failed journey" },
  ])("rejects $scenario", ({ overrides }) => {
    expect(() => validateRealFixtureReport(report(overrides))).toThrow(
      "real fixture E2E result is invalid",
    );
  });
});
