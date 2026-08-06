import { execFileSync } from "node:child_process";

const output = execFileSync(
  "npm",
  ["pack", "--dry-run", "--json", "--ignore-scripts"],
  {
    cwd: process.cwd(),
    encoding: "utf8",
  },
);
const [report] = JSON.parse(output);

if (!report || !Array.isArray(report.files)) {
  throw new Error("npm pack did not return a file manifest");
}

const paths = new Set(report.files.map((file) => file.path));
const required = [
  "package.json",
  "README.md",
  "LICENSE",
  "dist/index.js",
  "dist/index.cjs",
  "dist/index.d.ts",
  "dist/index.d.cts",
  "docs/architecture.md",
  "docs/migration.md",
  "docs/roadmap.md",
];
const forbiddenPrefixes = [
  ".github/",
  "coverage/",
  "examples/",
  "scripts/",
  "src/",
  "tests/",
];

for (const path of required) {
  if (!paths.has(path)) {
    throw new Error(`Required packed file is missing: ${path}`);
  }
}

for (const path of paths) {
  if (forbiddenPrefixes.some((prefix) => path.startsWith(prefix))) {
    throw new Error(`Development-only file was packed: ${path}`);
  }
}

process.stdout.write(
  `Validated ${paths.size} packed files (${report.size} bytes).\n`,
);
