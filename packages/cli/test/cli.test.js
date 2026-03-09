const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");

test("cli reports the published version", () => {
  const cliPath = path.join(__dirname, "..", "dist", "index.js");
  const packageJson = JSON.parse(
    fs.readFileSync(path.join(__dirname, "..", "package.json"), "utf-8"),
  );
  const output = execFileSync(process.execPath, [cliPath, "--version"], {
    encoding: "utf-8",
  });

  assert.match(output, new RegExp(`practor v${packageJson.version.replace(/\./g, "\\.")}`));
});
