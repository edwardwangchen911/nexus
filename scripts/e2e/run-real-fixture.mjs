import { RealFixtureRunnerError, runRealFixture } from "./real-fixture.mjs";

try {
  const exitCode = await runRealFixture({ args: process.argv.slice(2) });
  process.exitCode = exitCode;
} catch (error) {
  const message = error instanceof RealFixtureRunnerError
    ? error.message
    : "real fixture E2E runner failed";
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
}
