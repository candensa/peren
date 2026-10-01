pub(super) const NODE_OS_SOURCE: &str = r##"
import { platform as processPlatform, arch as processArch } from "node:process";
export const EOL = "\n";
export const devNull = "/dev/null";
export const constants = Object.freeze({ signals: Object.freeze({}), errno: Object.freeze({}), priority: Object.freeze({ PRIORITY_LOW: 19, PRIORITY_NORMAL: 0, PRIORITY_HIGH: -14 }) });
export const platform = () => processPlatform;
export const arch = () => processArch;
export const type = () => "Linux";
export const release = () => "0.0.0-peren";
export const version = () => "#1 SMP peren";
export const machine = () => "x86_64";
export const hostname = () => "localhost";
export const homedir = () => "/";
export const tmpdir = () => "/tmp";
export const uptime = () => 0;
export const loadavg = () => [0, 0, 0];
export const cpus = () => [];
export const totalmem = () => 0;
export const freemem = () => 0;
export const networkInterfaces = () => ({});
export const userInfo = () => ({ username: "peren", uid: -1, gid: -1, shell: null, homedir: "/" });
export const endianness = () => "LE";
export const availableParallelism = () => 1;
export const getPriority = () => 0;
export function setPriority() { throw new Error("os.setPriority() is not supported inside a Worker isolate"); }
export default { platform, arch, type, release, version, machine, hostname, homedir, tmpdir, uptime, loadavg, cpus, totalmem, freemem, networkInterfaces, userInfo, endianness, availableParallelism, getPriority, setPriority, EOL, devNull, constants };
"##;

pub(super) const NODE_PERF_HOOKS_SOURCE: &str = r#"
export const performance = globalThis.performance;
function refuse(name) { return function () { throw new Error(`perf_hooks.${name} is not supported inside a Worker isolate`); }; }
export const PerformanceObserver = refuse("PerformanceObserver");
export const monitorEventLoopDelay = refuse("monitorEventLoopDelay");
export const createHistogram = refuse("createHistogram");
export const timerify = refuse("timerify");
export const constants = Object.freeze({});
export default { performance, PerformanceObserver, monitorEventLoopDelay, createHistogram, timerify, constants };
"#;

pub(super) const NODE_PROCESS_SOURCE: &str = r#"
const started = Date.now();
export const env = Object.create(null);
export const argv = ["peren"];
export const argv0 = "peren";
export const execArgv = [];
export const platform = "linux";
export const arch = "x64";
export const version = "v22.0.0-peren-nodejs-compat";
export const versions = Object.freeze({ node: "22.0.0-peren-nodejs-compat" });
export const pid = 1;
export const ppid = 0;
export const title = "peren";
export const browser = false;
export function cwd() { return "/"; }
export function chdir() { throw new Error("process.chdir() is not supported inside a Worker isolate"); }
export function nextTick(callback, ...args) {
  if (typeof callback !== "function") throw new TypeError("callback must be a function");
  Promise.resolve().then(() => callback(...args));
}
export function uptime() { return (Date.now() - started) / 1000; }
export function hrtime(previous) {
  const ns = BigInt(Date.now() - started) * 1000000n;
  let seconds = Number(ns / 1000000000n);
  let nanos = Number(ns % 1000000000n);
  if (previous) {
    seconds -= previous[0];
    nanos -= previous[1];
    if (nanos < 0) { seconds -= 1; nanos += 1000000000; }
  }
  return [seconds, nanos];
}
hrtime.bigint = () => BigInt(Date.now() - started) * 1000000n;
export function memoryUsage() { return { rss: 0, heapTotal: 0, heapUsed: 0, external: 0, arrayBuffers: 0 }; }
memoryUsage.rss = () => 0;
export function exit(code = 0) { throw new Error(`process.exit(${code}) is not supported inside a Worker isolate`); }
export function abort() { throw new Error("process.abort() is not supported inside a Worker isolate"); }
const process = { env, argv, argv0, execArgv, platform, arch, version, versions, pid, ppid, title, browser, cwd, chdir, nextTick, uptime, hrtime, memoryUsage, exit, abort };
export default process;
"#;
