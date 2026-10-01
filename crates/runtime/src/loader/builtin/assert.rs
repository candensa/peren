pub(super) const NODE_ASSERT_SOURCE: &str = r#"
export class AssertionError extends Error {
  constructor(options = {}) { super(options.message || `${options.operator || "assert"} failed`); this.name = "AssertionError"; this.code = "ERR_ASSERTION"; this.actual = options.actual; this.expected = options.expected; this.operator = options.operator; }
}
function failWith(message, actual, expected, operator) { throw new AssertionError({ message, actual, expected, operator }); }
function same(left, right, strict, seen = new Map()) {
  if (Object.is(left, right)) return true;
  if (left === null || right === null || typeof left !== "object" || typeof right !== "object") return strict ? false : left == right;
  if (strict && Object.getPrototypeOf(left) !== Object.getPrototypeOf(right)) return false;
  if (seen.get(left) === right) return true;
  seen.set(left, right);
  if (Array.isArray(left) !== Array.isArray(right)) return false;
  if (ArrayBuffer.isView(left) || ArrayBuffer.isView(right)) { if (!ArrayBuffer.isView(left) || !ArrayBuffer.isView(right) || left.length !== right.length) return false; for (let i = 0; i < left.length; i++) if (left[i] !== right[i]) return false; return true; }
  const keys = Object.keys(left);
  if (keys.length !== Object.keys(right).length) return false;
  return keys.every((key) => Object.hasOwn(right, key) && same(left[key], right[key], strict, seen));
}
function assert(value, message) { if (!value) failWith(message, value, true, "=="); }
export { assert as ok };
export function equal(a, b, message) { if (!(a == b)) failWith(message, a, b, "=="); }
export function notEqual(a, b, message) { if (a == b) failWith(message, a, b, "!="); }
export function strictEqual(a, b, message) { if (!Object.is(a, b)) failWith(message, a, b, "strictEqual"); }
export function notStrictEqual(a, b, message) { if (Object.is(a, b)) failWith(message, a, b, "notStrictEqual"); }
export function deepEqual(a, b, message) { if (!same(a, b, false)) failWith(message, a, b, "deepEqual"); }
export function deepStrictEqual(a, b, message) { if (!same(a, b, true)) failWith(message, a, b, "deepStrictEqual"); }
export function notDeepEqual(a, b, message) { if (same(a, b, false)) failWith(message, a, b, "notDeepEqual"); }
export function notDeepStrictEqual(a, b, message) { if (same(a, b, true)) failWith(message, a, b, "notDeepStrictEqual"); }
export function fail(message) { failWith(message || "Failed", undefined, undefined, "fail"); }
export function throws(fn, expected, message) { let caught; try { fn(); } catch (error) { caught = error; } if (!caught) failWith(message || "Missing expected exception", undefined, expected, "throws"); if (expected instanceof RegExp && !expected.test(String(caught.message))) failWith(message, caught, expected, "throws"); if (typeof expected === "function" && !(caught instanceof expected)) failWith(message, caught, expected, "throws"); }
export async function rejects(fn, expected, message) { let caught; try { await (typeof fn === "function" ? fn() : fn); } catch (error) { caught = error; } if (!caught) failWith(message || "Missing expected rejection", undefined, expected, "rejects"); if (expected instanceof RegExp && !expected.test(String(caught.message))) failWith(message, caught, expected, "rejects"); }
export function doesNotThrow(fn, message) { try { fn(); } catch (error) { failWith(message || `Got unwanted exception: ${error.message}`, error, undefined, "doesNotThrow"); } }
export async function doesNotReject(fn, message) { try { await (typeof fn === "function" ? fn() : fn); } catch (error) { failWith(message || `Got unwanted rejection: ${error.message}`, error, undefined, "doesNotReject"); } }
Object.assign(assert, { AssertionError, ok: assert, equal, notEqual, strictEqual, notStrictEqual, deepEqual, deepStrictEqual, notDeepEqual, notDeepStrictEqual, fail, throws, rejects, doesNotThrow, doesNotReject });
export default assert;
"#;
