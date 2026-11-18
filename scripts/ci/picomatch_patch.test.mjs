import assert from "node:assert/strict";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { test } from "node:test";
import { processTree, targets } from "./picomatch_patch.mjs";

function fixture(t) {
  const root = mkdtempSync(path.join(os.tmpdir(), "nexus-picomatch-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const identity = JSON.parse(readFileSync(new URL("./picomatch_sources.json", import.meta.url)));
  const files = new Set([...Object.keys(targets), ...Object.values(identity.packages).flatMap(Object.keys)]);
  for (const file of files) {
    const destination = path.join(root, "node_modules", file);
    mkdirSync(path.dirname(destination), { recursive: true });
    copyFileSync(path.join("node_modules", file), destination);
  }
  copyFileSync("package-lock.json", path.join(root, "package-lock.json"));
  return root;
}

test("all installed and bundled parsers reject the bounded regression and preserve normal matching", () => {
  const result = spawnSync(process.execPath, ["scripts/ci/picomatch_probe.mjs", process.cwd()], { timeout: 15000, encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(JSON.parse(result.stdout).parsers, 10);
});

test("application is idempotent and actual source tampering is rejected", (t) => {
  const root = fixture(t);
  assert.deepEqual(processTree(root, true), processTree(root, true));
  const file = path.join(root, "node_modules/vite/node_modules/picomatch/lib/parse.js");
  writeFileSync(file, readFileSync(file, "utf8") + "\n");
  assert.throws(() => processTree(root), /UPSTREAM_IDENTITY_CHANGED/);
});

test("a changed package entrypoint or lock cannot certify a parser fix", (t) => {
  const root = fixture(t);
  writeFileSync(path.join(root, "node_modules/picomatch/index.js"), "module.exports = () => true");
  assert.throws(() => processTree(root), /PACKAGE_IDENTITY_CHANGED/);
  writeFileSync(path.join(root, "package-lock.json"), "{}");
  assert.throws(() => processTree(root), /LOCK_IDENTITY_CHANGED/);
});

test("apply refuses shared dependency symlinks", (t) => {
  const root = mkdtempSync(path.join(os.tmpdir(), "nexus-picomatch-link-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  copyFileSync("package-lock.json", path.join(root, "package-lock.json"));
  symlinkSync(path.resolve("node_modules"), path.join(root, "node_modules"));
  assert.throws(() => processTree(root, true), /REFUSE_SHARED_DEPENDENCY_MUTATION/);
});
