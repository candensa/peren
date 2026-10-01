pub(super) const NODE_TIMERS_SOURCE: &str = r"
const { setTimeout, clearTimeout, setInterval, clearInterval, setImmediate, clearImmediate } = globalThis;
export { setTimeout, clearTimeout, setInterval, clearInterval, setImmediate, clearImmediate };
export default { setTimeout, clearTimeout, setInterval, clearInterval, setImmediate, clearImmediate };
";

pub(super) const NODE_TIMERS_PROMISES_SOURCE: &str = r#"
export function setTimeout(delay = 1, value, options = {}) {
  return new Promise((resolve, reject) => {
    if (options?.signal?.aborted) { reject(options.signal.reason ?? new Error("aborted")); return; }
    const id = globalThis.setTimeout(() => resolve(value), Number(delay));
    options?.signal?.addEventListener?.("abort", () => {
      globalThis.clearTimeout(id);
      reject(options.signal.reason ?? new Error("aborted"));
    }, { once: true });
  });
}
export function setImmediate(value, options = {}) { return setTimeout(0, value, options); }
export async function* setInterval(delay = 1, value, options = {}) {
  while (!options?.signal?.aborted) {
    yield await setTimeout(delay, value, options);
  }
}
export default { setTimeout, setImmediate, setInterval };
"#;
