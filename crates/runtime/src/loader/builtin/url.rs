pub(super) const NODE_URL_SOURCE: &str = r##"
import path from "node:path";
export const URL = globalThis.URL;
export const URLSearchParams = globalThis.URLSearchParams;
function queryObject(search) {
  const out = {};
  for (const [key, value] of new URLSearchParams(search)) {
    if (Object.hasOwn(out, key)) out[key] = Array.isArray(out[key]) ? [...out[key], value] : [out[key], value];
    else out[key] = value;
  }
  return out;
}
function queryString(query) {
  if (typeof query === "string") return query;
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query ?? {})) {
    if (Array.isArray(value)) for (const item of value) params.append(key, item);
    else if (value != null) params.append(key, value);
  }
  return params.toString();
}
export function parse(value, parseQueryString = false, slashesDenoteHost = false) {
  const input = String(value);
  let protocol = null, slashes = false, auth = null, host = null, hostname = null, port = null;
  let rest = input;
  const protocolMatch = /^([a-zA-Z][a-zA-Z0-9+.-]*:)(\/\/)?/.exec(rest);
  if (protocolMatch) { protocol = protocolMatch[1]; slashes = Boolean(protocolMatch[2]); rest = rest.slice(protocolMatch[0].length); }
  else if (slashesDenoteHost && rest.startsWith("//")) { slashes = true; rest = rest.slice(2); }
  if (slashes) {
    const head = /^[^/?#]*/.exec(rest)[0];
    rest = rest.slice(head.length);
    let hostPart = head;
    const at = hostPart.lastIndexOf("@");
    if (at >= 0) { auth = hostPart.slice(0, at); hostPart = hostPart.slice(at + 1); }
    host = hostPart || null;
    if (host) {
      const match = /:(\d+)$/.exec(host);
      if (match) { port = match[1]; hostname = host.slice(0, -match[0].length); }
      else hostname = host;
    }
  }
  let hash = null;
  const hashIndex = rest.indexOf("#");
  if (hashIndex >= 0) { hash = rest.slice(hashIndex); rest = rest.slice(0, hashIndex); }
  let search = null, query = parseQueryString ? {} : null;
  const queryIndex = rest.indexOf("?");
  if (queryIndex >= 0) { search = rest.slice(queryIndex); const raw = rest.slice(queryIndex + 1); rest = rest.slice(0, queryIndex); query = parseQueryString ? queryObject(raw) : raw; }
  const pathname = rest.length > 0 ? rest : host ? "/" : null;
  const pathValue = search !== null ? `${pathname ?? ""}${search}` : pathname;
  return { protocol, slashes, auth, host, port, hostname, hash, search, query, pathname, path: pathValue, href: `${protocol ?? ""}${slashes ? "//" : ""}${auth ? `${auth}@` : ""}${host ?? ""}${pathname ?? ""}${search ?? ""}${hash ?? ""}` };
}
export const decode = parse;
export function format(value) {
  if (typeof value === "string") return format(parse(value));
  if (value instanceof URL) return value.href;
  const protocol = value.protocol ?? "";
  const host = value.host || (value.hostname ? `${value.hostname}${value.port ? `:${value.port}` : ""}` : "");
  const search = value.search || (value.query ? `?${queryString(value.query)}` : "");
  return `${protocol}${(value.slashes || (protocol && host)) ? "//" : ""}${value.auth ? `${value.auth}@` : ""}${host}${value.pathname ?? ""}${search}${value.hash ?? ""}`;
}
export function resolve(from, to) { try { return new URL(String(to), String(from)).href; } catch { return String(to); } }
export function fileURLToPath(value) { const url = value instanceof URL ? value : new URL(String(value)); if (url.protocol !== "file:") throw new TypeError(`The URL must be of scheme file; received ${url.protocol}`); if (url.hostname && url.hostname !== "localhost") throw new TypeError("file URL host must be local"); return decodeURIComponent(url.pathname); }
export function pathToFileURL(value) { const absolute = path.isAbsolute(String(value)) ? String(value) : path.resolve(String(value)); return new URL(`file://${absolute.split("/").map(encodeURIComponent).join("/")}`); }
export function urlToHttpOptions(value) { const url = value instanceof URL ? value : new URL(String(value)); const out = { protocol: url.protocol, hostname: url.hostname, hash: url.hash, search: url.search, pathname: url.pathname, path: `${url.pathname}${url.search}`, href: url.href }; if (url.port) out.port = Number(url.port); if (url.username || url.password) out.auth = `${url.username}:${url.password}`; return out; }
export function domainToASCII(value) { return new URL(`http://${String(value)}`).hostname; }
export function domainToUnicode(value) { return new URL(`http://${String(value)}`).hostname; }
export default { URL, URLSearchParams, domainToASCII, domainToUnicode, parse, decode, format, resolve, fileURLToPath, pathToFileURL, urlToHttpOptions };
"##;
