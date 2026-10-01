use super::support::*;

#[tokio::test(flavor = "current_thread")]
async fn node_events_path_and_util_cover_ported_worker_libraries() {
    let bundle = WorkerBundle::new(
        name("main.js"),
        BTreeMap::from([(
            name("main.js"),
            javascript(
                r#"
            import asyncHooks from "node:async_hooks";
            import diagnostics from "node:diagnostics_channel";
            import { EventEmitter, once } from "node:events";
            import path, { join, relative, parse } from "node:path";
            import util, { format, promisify, callbackify, types } from "node:util";
            export default {
              async fetch() {
                const storage = new asyncHooks.AsyncLocalStorage();
                const asyncContext = storage.run({ request: 'one' }, () => storage.getStore().request);
                const resource = new asyncHooks.AsyncResource('worker');
                const scoped = resource.runInAsyncScope((value) => `${storage.getStore()?.request ?? 'none'}:${value}`, null, 'ok');
                const hook = asyncHooks.createHook({});
                hook.enable().disable();
                const emitter = new EventEmitter();
                const seen = [];
                emitter.on("event", (value) => seen.push(value));
                emitter.once("event", (value) => seen.push(`once:${value}`));
                emitter.emit("event", "a");
                emitter.emit("event", "b");
                const waiting = once(emitter, "ready");
                emitter.emit("ready", "done");
                const promised = promisify((value, cb) => cb(null, value + 1));
                const callbackValue = await new Promise((resolve, reject) => callbackify(async (v) => v * 2)(3, (err, value) => err ? reject(err) : resolve(value)));
                const channel = diagnostics.channel('peren.event');
                const diagnostic = [];
                const listener = (message, name) => diagnostic.push(`${name}:${message.step}`);
                channel.subscribe(listener);
                channel.publish({ step: 'one' });
                channel.unsubscribe(listener);
                channel.publish({ step: 'two' });
                const trace = diagnostics.tracingChannel('task');
                return new Response(JSON.stringify({
                  asyncHooks: { context: asyncContext, scoped, id: resource.asyncId() > 0, execution: typeof asyncHooks.executionAsyncId() === 'number' },
                  defaultEventEmitter: typeof EventEmitter === "function",
                  seen,
                  waited: await waiting,
                  listenerCount: emitter.listenerCount("event"),
                  joined: join("/tmp", "../app", "worker.js"),
                  relative: relative("/app/src", "/app/test/file.js"),
                  parsed: parse("/app/file.test.js"),
                  pathDefault: path.sep,
                  formatted: format("hello %s %j", "peren", { ok: true }),
                  inspected: util.inspect({ nested: { value: 1 } }),
                  promised: await promised(4),
                  callbackValue,
                  typed: types.isUint8Array(new Uint8Array()) && types.isMap(new Map()),
                  diagnostic,
                  hasSubscribers: diagnostics.hasSubscribers('peren.event'),
                  tracing: Object.keys(trace).sort(),
                }));
              }
            };
        "#,
            ),
        )]),
    )
    .unwrap();
    let mut runtime = WorkerRuntime::load(bundle, limits()).await.unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/node-more".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(2048, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "asyncHooks": { "context": "one", "scoped": "none:ok", "id": true, "execution": true },
            "defaultEventEmitter": true,
            "seen": ["a", "once:a", "b"],
            "waited": "done",
            "listenerCount": 1,
            "joined": "/app/worker.js",
            "relative": "../test/file.js",
            "parsed": { "root": "/", "dir": "/app", "base": "file.test.js", "ext": ".js", "name": "file.test" },
            "pathDefault": "/",
            "formatted": "hello peren {\"ok\":true}",
            "inspected": "{ nested: { value: 1 } }",
            "promised": 5,
            "callbackValue": 6,
            "typed": true,
            "diagnostic": ["peren.event:one"],
            "hasSubscribers": false,
            "tracing": ["asyncEnd", "asyncStart", "end", "error", "start"],
        })
    );
}
