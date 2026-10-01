pub(super) const NODE_PUNYCODE_SOURCE: &str = r#"
const decoder = new TextDecoder();
const encoder = new TextEncoder();
export const ucs2 = Object.freeze({
  decode(value) { return Array.from(String(value), (character) => character.codePointAt(0)); },
  encode(points) { return Array.from(points ?? [], (point) => String.fromCodePoint(Number(point))).join(""); },
});
export function toASCII(value) { return new URL(`http://${String(value)}`).hostname; }
export function toUnicode(value) { return new URL(`http://${String(value)}`).hostname; }
export function encode(value) { return btoa(String.fromCharCode(...encoder.encode(String(value)))); }
export function decode(value) { return decoder.decode(Uint8Array.from(atob(String(value)), (character) => character.charCodeAt(0))); }
export const version = "2.3.1-peren";
export default { decode, encode, toASCII, toUnicode, ucs2, version };
"#;
