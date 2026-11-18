import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { Worker, isMainThread, parentPort, workerData } from "node:worker_threads";

// Exercise the bundled modules themselves, without executing their host tools.
export function matchers(root) {
  const modules = path.join(root, "node_modules");
  const result = [];
  for (const name of ["picomatch", "vite/node_modules/picomatch", "tinyglobby/node_modules/picomatch", "vitest/node_modules/picomatch"]) {
    const filename = path.join(modules, name, "index.js");
    result.push([name, createRequire(filename)(filename)]);
  }
  const common = `var __commonJS = (cb, mod) => function() { if (!mod) { mod = { exports: {} }; Object.values(cb)[0](mod.exports, mod); } return mod.exports; };`;
  for (const file of ["playwright-core/lib/utilsBundle.js", "playwright-core/lib/serverRegistry.js", "playwright/lib/matchers/expect.js"]) {
    const source = readFileSync(path.join(modules, file), "utf8");
    const headers = [...source.matchAll(/^\/\/ (node_modules\/.*)\n/gm)];
    const groups = new Map();
    for (let i = 0; i < headers.length; i++) {
      const header = headers[i];
      const match = /^(.*\/picomatch)\/(?:lib\/(?:constants|utils|scan|parse|picomatch)|index)\.js$/.exec(header[1]);
      if (!match) continue;
      const chunk = source.slice(header.index, headers[i + 1]?.index ?? source.length);
      const group = groups.get(match[1]) ?? [];
      group.push(chunk);
      groups.set(match[1], group);
    }
    for (const [name, chunks] of groups) {
      assert.equal(chunks.length, 6, `${file}:${name} module inventory changed`);
      const entry = /var (require_picomatch\d*) = __commonJS/.exec(chunks.at(-1))[1];
      result.push([`${file}:${name}`, vm.runInNewContext(`${common}\n${chunks.join("\n")}\n${entry}()`, { require: createRequire(path.join(modules, file)), process }, { timeout: 2000 })]);
    }
  }
  const cloud = readFileSync(path.join(modules, "@cloudflare/vite-plugin/dist/index.mjs"), "utf8");
  const start = cloud.indexOf("//#region ../../node_modules/.pnpm/picomatch@");
  const endMarker = "//#region ../../node_modules/.pnpm/picomatch@4.0.5/node_modules/picomatch/index.js";
  const end = cloud.indexOf("//#endregion", cloud.indexOf(endMarker));
  assert.ok(start > 0 && end > start, "Cloudflare module inventory changed");
  result.push(["@cloudflare/vite-plugin:embedded", vm.runInNewContext(`${common.replaceAll("__commonJS", "__commonJS$3")}\n${cloud.slice(start, end)}\nrequire_picomatch()`, { process }, { timeout: 2000 })]);
  assert.equal(result.length, 10, "picomatch parser inventory changed");
  return result;
}

export function verify(root) {
  for (const [name, pm] of matchers(root)) {
    for (const pattern of ["+(*)", "+(*a)", "*(*a)", "+(@(*a))", "prefix+(*a)suffix", '+(["]*a)', '*(["]*a)', '+(["a]*a)', "+(a+)", "+([ab]+)"]) {
      for (const options of [{}, { maxExtglobRecursion: false }, { capture: true }]) {
        // The existing parser's safe fallback treats an ambiguous repeat as literal.
        assert.equal(pm.isMatch(pattern, pattern, options), true, `${name}: ${pattern} must be literalized`);
        assert.equal(pm.isMatch("a".repeat(1000) + "!", pattern, options), false, name);
      }
    }
    for (const [pattern, options] of [["+(a|A)", { nocase: true }], ["+(a|A)", { flags: "i" }], ["+([*]a)", { nobracket: true }], ["+(a\\*)", { unescape: true }], ["+(a\\+)", { unescape: true }]]) {
      assert.equal(pm.isMatch(pattern, pattern, options), true, `${name}: option bypass ${pattern}`);
      assert.equal(pm.isMatch("a".repeat(1000) + "!", pattern, options), false, name);
    }
    for (const [input, pattern, expected] of [
      ["src/a/file.ts", "**/*.{ts,tsx}", true], ["src/a/file.js", "**/*.ts", false],
      ["foo.js", "!(*.test).js", true], ["foo.test.js", "!(*.test).js", false],
      ["abab", "+(ab|cd)", true], ["cdab", "*(ab|cd)", true],
      ["aaa", "+(a)", true], ["b", "+([ab])", true],
      ["***", "+(\\*)", true], ["***", "+([*])", true],
      ["foo.ts", "@(foo|bar).ts", true], ["a/file.ts", "**/*.ts", true],
    ]) assert.equal(pm.isMatch(input, pattern), expected, `${name}: normal ${pattern}`);
    assert.equal(pm.isMatch("a\\b.ts", "**/*.ts", { windows: true }), true, `${name}: windows`);
    assert.equal(pm.isMatch("a/b.ts", ["**/*.js", "**/*.ts"]), true, `${name}: array`);
  }
  return { status: "verified", parsers: 10 };
}

if (!isMainThread && workerData?.picomatchRoot) {
  parentPort.postMessage(verify(workerData.picomatchRoot));
} else if (isMainThread && process.argv[1] === fileURLToPath(import.meta.url)) {
  const worker = new Worker(new URL(import.meta.url), { workerData: { picomatchRoot: path.resolve(process.argv[2] ?? ".") } });
  const timer = setTimeout(() => {
    console.error("PICOMATCH_PROBE_TIME_LIMIT");
    process.exitCode = 1;
    void worker.terminate();
  }, 10000);
  worker.once("message", (proof) => { clearTimeout(timer); console.log(JSON.stringify(proof)); });
  worker.once("error", (error) => { clearTimeout(timer); console.error(error.message); process.exitCode = 1; });
}
