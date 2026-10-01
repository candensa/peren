import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const parts = [
  "prelude",
  "crypto",
  "channel",
  "global",
  "source",
  "storage",
  "object",
  "media",
  "ai",
  "vector",
  "binding",
  "workflow",
  "event",
  "cache",
  "hydrate",
];

const root = join("crates", "runtime", "src", "bootstrap");
const header =
  "// Generated from crates/runtime/src/bootstrap/*.js. Edit those files and rebuild the checked-in bundle.\n";
const body = parts
  .map((part) => readFileSync(join(root, `${part}.js`), "utf8"))
  .join("");

writeFileSync(join("crates", "runtime", "src", "bootstrap.js"), header + body);
