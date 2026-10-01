import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const declaration = join("dist", "index.d.ts");
let source = readFileSync(declaration, "utf8");
source = source.replace("import { Plugin } from 'vite';\n\n", "");
source = source.replace(/: Plugin/g, ': import("vite").Plugin');
source = source.replace(/declare function perenTest/, "function perenTest");

const exportLine = source.match(/export \{[^;]+\};\n?$/s);
if (!exportLine) throw new Error("could not find generated export line");
const exports = exportLine[0]
  .replace(/^export \{\s*/s, "")
  .replace(/\s*\};\n?$/s, "")
  .split(/,\s*/)
  .map((entry) => entry.trim())
  .filter(Boolean)
  .map((entry) => entry.replace(/^type /, ""));

const declarations = source.slice(0, exportLine.index).trimEnd();
const moduleExports = exports
  .map((entry) => {
    const match = entry.match(/^(.+) as (.+)$/);
    if (match) return `  export { ${match[1]} as ${match[2]} };`;
    if (entry === "perenTest") return "  export { perenTest };";
    return `  export type { ${entry} };`;
  })
  .join("\n");

const modules = readFileSync(join("src", "modules.d.ts"), "utf8")
  .replaceAll('import("./types.js")', 'import("@peren/vitest-plugin")');

writeFileSync(
  declaration,
  `declare module "@peren/vitest-plugin" {\n${declarations}\n\n${moduleExports}\n}\n\n${modules}`,
);
