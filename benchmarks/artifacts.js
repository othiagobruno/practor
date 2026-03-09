const { spawnSync } = require("node:child_process");

function shouldAutoBuild() {
  return process.env.PRACTOR_BENCHMARK_AUTO_BUILD !== "false";
}

function runRootScript(config, scriptName) {
  const result = spawnSync("npm", ["run", scriptName], {
    cwd: config.rootDir,
    encoding: "utf-8",
    env: process.env,
    timeout: 300_000,
  });

  if (result.stdout) {
    process.stdout.write(result.stdout);
  }
  if (result.stderr) {
    process.stderr.write(result.stderr);
  }

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`${scriptName} exited with code ${result.status}`);
  }
}

function ensureBenchmarkArtifacts(config) {
  if (!shouldAutoBuild()) {
    return;
  }

  console.log("Ensuring benchmark artifacts are built...");
  runRootScript(config, "build");
  runRootScript(config, "build:engine");
}

module.exports = {
  ensureBenchmarkArtifacts,
  shouldAutoBuild,
};
