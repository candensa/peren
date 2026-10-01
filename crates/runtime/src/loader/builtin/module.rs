pub(super) const NODE_MODULE_SOURCE: &str = r#"

import assertModule from "node:assert";
import bufferModule from "node:buffer";
import consoleModule from "node:console";
import cryptoModule from "node:crypto";
import eventsModule from "node:events";
import asyncHooksModule from "node:async_hooks";
import diagnosticsChannelModule from "node:diagnostics_channel";
import dnsModule from "node:dns";
import dnsPromisesModule from "node:dns/promises";
import fsModule from "node:fs";
import fsPromisesModule from "node:fs/promises";
import httpModule from "node:http";
import httpsModule from "node:https";
import netModule from "node:net";
import osModule from "node:os";
import pathModule from "node:path";
import perfHooksModule from "node:perf_hooks";
import processModule from "node:process";
import querystringModule from "node:querystring";
import punycodeModule from "node:punycode";
import streamModule from "node:stream";
import streamWebModule from "node:stream/web";
import streamConsumersModule from "node:stream/consumers";
import streamPromisesModule from "node:stream/promises";
import stringDecoderModule from "node:string_decoder";
import timersModule from "node:timers";
import timersPromisesModule from "node:timers/promises";
import testModule from "node:test";
import tlsModule from "node:tls";
import urlModule from "node:url";
import utilModule from "node:util";
import zlibModule from "node:zlib";

const modules = Object.freeze({
  "node:assert": assertModule,
  "assert": assertModule,
  "node:buffer": bufferModule,
  "buffer": bufferModule,
  "node:console": consoleModule,
  "console": consoleModule,
  "node:crypto": cryptoModule,
  "crypto": cryptoModule,
  "node:events": eventsModule,
  "events": eventsModule,
  "node:async_hooks": asyncHooksModule,
  "async_hooks": asyncHooksModule,
  "node:diagnostics_channel": diagnosticsChannelModule,
  "diagnostics_channel": diagnosticsChannelModule,
  "node:dns": dnsModule,
  "dns": dnsModule,
  "node:dns/promises": dnsPromisesModule,
  "dns/promises": dnsPromisesModule,
  "node:fs": fsModule,
  "fs": fsModule,
  "node:fs/promises": fsPromisesModule,
  "fs/promises": fsPromisesModule,
  "node:http": httpModule,
  "http": httpModule,
  "node:https": httpsModule,
  "https": httpsModule,
  "node:net": netModule,
  "net": netModule,
  "node:os": osModule,
  "os": osModule,
  "node:path": pathModule,
  "path": pathModule,
  "node:perf_hooks": perfHooksModule,
  "perf_hooks": perfHooksModule,
  "node:process": processModule,
  "process": processModule,
  "node:querystring": querystringModule,
  "querystring": querystringModule,
  "node:punycode": punycodeModule,
  "punycode": punycodeModule,
  "node:stream": streamModule,
  "stream": streamModule,
  "node:stream/web": streamWebModule,
  "stream/web": streamWebModule,
  "node:stream/consumers": streamConsumersModule,
  "stream/consumers": streamConsumersModule,
  "node:stream/promises": streamPromisesModule,
  "stream/promises": streamPromisesModule,
  "node:string_decoder": stringDecoderModule,
  "string_decoder": stringDecoderModule,
  "node:timers": timersModule,
  "timers": timersModule,
  "node:timers/promises": timersPromisesModule,
  "timers/promises": timersPromisesModule,
  "node:test": testModule,
  "test": testModule,
  "node:tls": tlsModule,
  "tls": tlsModule,
  "node:url": urlModule,
  "url": urlModule,
  "node:util": utilModule,
  "util": utilModule,
  "node:zlib": zlibModule,
  "zlib": zlibModule,
});

export const builtinModules = Object.freeze(Object.keys(modules).filter((name) => !name.startsWith("node:")));
export function isBuiltin(name) { return Object.hasOwn(modules, String(name)); }
export function createRequire() {
  return function require(name) {
    const module = modules[String(name)];
    if (module === undefined) throw new Error(`CommonJS require for ${name} is not available inside a Worker isolate`);
    return module;
  };
}
export function syncBuiltinESMExports() {}
export default { builtinModules, createRequire, isBuiltin, syncBuiltinESMExports };
"#;
