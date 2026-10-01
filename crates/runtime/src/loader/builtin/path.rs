pub(super) const NODE_PATH_SOURCE: &str = r#"
function assertPath(path) { if (typeof path !== "string") throw new TypeError(`The "path" argument must be of type string. Received ${typeof path}`); }
export function normalize(path) {
  assertPath(path);
  if (path.length === 0) return ".";
  const absolute = path.startsWith("/");
  const trailing = path.endsWith("/");
  const out = [];
  for (const part of path.split("/")) {
    if (!part || part === ".") continue;
    if (part === "..") { if (out.length && out[out.length - 1] !== "..") out.pop(); else if (!absolute) out.push(".."); }
    else out.push(part);
  }
  let result = `${absolute ? "/" : ""}${out.join("/")}` || (absolute ? "/" : ".");
  if (trailing && result !== "/" && !result.endsWith("/")) result += "/";
  return result;
}
export function join(...parts) { return normalize(parts.filter((part) => { assertPath(part); return part.length > 0; }).join("/") || "."); }
export function resolve(...parts) {
  let resolved = "";
  let absolute = false;
  for (let i = parts.length - 1; i >= -1 && !absolute; i--) {
    const part = i >= 0 ? parts[i] : "/";
    assertPath(part);
    if (!part) continue;
    resolved = `${part}/${resolved}`;
    absolute = part.startsWith("/");
  }
  const out = normalize(resolved);
  return absolute && !out.startsWith("/") ? `/${out}` : out;
}
export function isAbsolute(path) { assertPath(path); return path.startsWith("/"); }
export function relative(from, to) {
  assertPath(from); assertPath(to);
  const left = resolve(from).split("/").filter(Boolean);
  const right = resolve(to).split("/").filter(Boolean);
  let i = 0;
  while (i < left.length && i < right.length && left[i] === right[i]) i++;
  return [...Array(left.length - i).fill(".."), ...right.slice(i)].join("/");
}
export function dirname(path) {
  assertPath(path);
  if (!path) return ".";
  const normalized = path.replace(/\/+$|^(?=\/+$)/g, "");
  const index = normalized.lastIndexOf("/");
  if (index < 0) return ".";
  if (index === 0) return "/";
  return normalized.slice(0, index);
}
export function basename(path, suffix = "") {
  assertPath(path); assertPath(suffix);
  const clean = path.replace(/\/+$|^(?=\/+$)/g, "");
  const base = clean.slice(clean.lastIndexOf("/") + 1);
  return suffix && base.endsWith(suffix) && base.length > suffix.length ? base.slice(0, -suffix.length) : base;
}
export function extname(path) { const base = basename(path); const index = base.lastIndexOf("."); return index <= 0 ? "" : base.slice(index); }
export function format(value) {
  if (value === null || typeof value !== "object") throw new TypeError('The "pathObject" argument must be of type Object');
  const dir = value.dir || value.root || "";
  const base = value.base || `${value.name || ""}${value.ext || ""}`;
  return dir ? (dir === value.root ? `${dir}${base}` : `${dir}/${base}`) : base;
}
export function parse(path) { assertPath(path); const root = path.startsWith("/") ? "/" : ""; const base = basename(path); const ext = extname(path); return { root, dir: dirname(path), base, ext, name: ext ? base.slice(0, -ext.length) : base }; }
export function toNamespacedPath(path) { return path; }
export const sep = "/";
export const delimiter = ":";
export const win32 = new Proxy({}, { get(_target, property) { if (typeof property === "symbol" || property === "then") return undefined; throw new Error(`peren: node:path win32.${String(property)} is unavailable in Worker isolates`); } });
const posix = { normalize, join, resolve, isAbsolute, relative, dirname, basename, extname, format, parse, toNamespacedPath, sep, delimiter };
posix.posix = posix;
posix.win32 = win32;
export { posix };
export default posix;
"#;
