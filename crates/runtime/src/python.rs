use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

use crate::{
    AiHost, CacheHost, Capabilities, DurableObjectHost, DurableStorageHost, EngineError, HostError,
    HttpRequest, HttpResponse, InvocationLimits, IsolateLimits, KvHost, OutboundFetchHost,
    QueueDispatch, QueueEvent, QueueProducerHost, R2BucketHost, ScheduledEvent, ServiceBindingHost,
    TailEvent, WebSocketCloseEvent, WebSocketDispatch, WebSocketMessageEvent, WorkerBundle,
    WorkerEnvironment, WorkerLogEvent, WorkerLogLevel, WorkflowActivityEvent, WorkflowEvent,
    wire::{
        AiRun, CacheGet, CachePut, DurableObjectFetch, KvGet, KvList, KvPut, QueueSend, R2Delete,
        R2Get, R2List, R2Put, ServiceFetch, SqlQuery, SqlValue,
    },
};

const RUNNER: &str = r#"
import asyncio
import builtins
import contextlib
import importlib.abc
import importlib.util
import io
import json
import os
import signal
import sys
import tempfile
import types

try:
    import resource as _RESOURCE
except ImportError:
    _RESOURCE = None

BLOCKED_MODULES = {
    "curses",
    "dbm",
    "ensurepip",
    "fcntl",
    "grp",
    "idlelib",
    "lib2to3",
    "multiprocessing",
    "msvcrt",
    "pty",
    "pwd",
    "resource",
    "socket",
    "subprocess",
    "syslog",
    "termios",
    "threading",
    "tkinter",
    "turtle",
    "turtledemo",
    "tty",
    "venv",
    "webbrowser",
    "winreg",
    "winsound",
}

SAFE_ROOT = tempfile.TemporaryDirectory(prefix="peren-python-")
os.chdir(SAFE_ROOT.name)
_OPEN = builtins.open
_IO_OPEN = io.open
_STDLIB_ROOTS = tuple(
    os.path.realpath(path)
    for path in (sys.base_prefix, sys.prefix, sys.exec_prefix)
    if path
)

def _under(path, root):
    try:
        return os.path.commonpath([path, root]) == root
    except ValueError:
        return False

def _safe_path(file):
    if not isinstance(file, (str, bytes, os.PathLike)):
        return None
    return os.path.realpath(os.fspath(file))

def _sandboxed_open(file, mode="r", *args, **kwargs):
    path = _safe_path(file)
    if path is not None:
        writes = any(flag in mode for flag in ("w", "a", "x", "+"))
        in_safe_root = _under(path, os.path.realpath(SAFE_ROOT.name))
        in_stdlib = any(_under(path, root) for root in _STDLIB_ROOTS)
        if writes and not in_safe_root:
            raise PermissionError("Python Workers filesystem writes must stay in the invocation filesystem")
        if not writes and os.path.isabs(path) and not in_safe_root and not in_stdlib:
            raise PermissionError("Python Workers filesystem reads must stay in the invocation filesystem")
    return _OPEN(file, mode, *args, **kwargs)

builtins.open = _sandboxed_open
io.open = _sandboxed_open

class BlockedModuleImporter(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        root = fullname.split(".", 1)[0]
        if root in BLOCKED_MODULES:
            raise ModuleNotFoundError(f"No module named '{fullname}'")
        return None

sys.meta_path.insert(0, BlockedModuleImporter())
for _name in list(sys.modules):
    if _name.split(".", 1)[0] in BLOCKED_MODULES:
        del sys.modules[_name]
os.environ.clear()

class Headers:
    def __init__(self, values=None):
        self._values = {}
        for name, value in values or []:
            self._values[str(name).lower()] = str(value)

    def get(self, name, default=None):
        return self._values.get(str(name).lower(), default)

    def items(self):
        return list(self._values.items())

class Request:
    def __init__(self, value):
        self.method = value.get("method", "GET")
        self.url = value.get("url", "")
        self.headers = Headers(value.get("headers", []))
        self.body = bytes(value.get("body", []))

    async def text(self):
        return self.body.decode("utf-8")

    async def json(self):
        return json.loads(await self.text())

class ResponseJson:
    def __get__(self, instance, owner):
        if instance is None:
            def create(value, status=200, headers=None):
                merged = {"content-type": "application/json"}
                if headers:
                    merged.update(dict(headers))
                return owner(json.dumps(value), status=status, headers=merged)
            return create

        async def parse():
            return json.loads(await instance.text())
        return parse

class Response:
    def __init__(self, body=b"", status=200, headers=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.body = bytes(body or b"")
        self.status = int(status)
        values = headers.items() if isinstance(headers, dict) else (headers or [])
        self.headers = Headers(values)

    json = ResponseJson()

    async def text(self):
        return self.body.decode("utf-8")

    async def arrayBuffer(self):
        return self.body

def response_from_host(value):
    return Response(bytes(value.get("body", [])), status=value.get("status", 200), headers=value.get("headers", []))

class Context:
    def __init__(self):
        self._background = []

    def waitUntil(self, value):
        self._background.append(value)

    async def drain(self):
        await asyncio.gather(*[item for item in self._background if hasattr(item, "__await__")])

class WorkerEntrypoint:
    def __init__(self, env=None, ctx=None):
        self.env = env
        self.ctx = ctx

class HostBridge:
    def __init__(self):
        self._next_id = 1

    def call(self, op, payload=None):
        call_id = self._next_id
        self._next_id += 1
        sys.__stdout__.write(json.dumps({"kind": "host", "id": call_id, "op": op, "payload": payload or {}}) + "\n")
        sys.__stdout__.flush()
        line = sys.stdin.readline()
        if not line:
            raise RuntimeError("host bridge closed")
        envelope = json.loads(line)
        if envelope.get("id") != call_id:
            raise RuntimeError("host bridge response id mismatch")
        if not envelope.get("ok", False):
            raise RuntimeError(envelope.get("error") or "host operation failed")
        return envelope.get("result")

HOST = HostBridge()

class QueueMessage:
    def __init__(self, value):
        self.id = value.get("id")
        self.timestamp = value.get("timestamp")
        self.attempts = value.get("attempts", 1)
        raw_body = value.get("body", b"")
        if isinstance(raw_body, str):
            self.body = raw_body.encode("utf-8")
        else:
            self.body = bytes(raw_body)
        self._dispositions = []

    async def text(self):
        return self.body.decode("utf-8")

    async def json(self):
        return json.loads(await self.text())

    def ack(self):
        self._dispositions.append({"id": self.id, "outcome": "ack"})

    def retry(self, options=None):
        options = options or {}
        self._dispositions.append({
            "id": self.id,
            "outcome": "retry",
            "delaySeconds": options.get("delaySeconds"),
            "dedupId": options.get("dedupId"),
        })

    def dispositions(self):
        return list(self._dispositions)

class QueueBatch:
    def __init__(self, value):
        self.queue = value.get("queue")
        self.messages = [QueueMessage(message) for message in value.get("messages", [])]

    def ackAll(self):
        for message in self.messages:
            message.ack()

    def retryAll(self, options=None):
        for message in self.messages:
            message.retry(options)

    def dispositions(self):
        return [disposition for message in self.messages for disposition in message.dispositions()]

class Scheduled:
    def __init__(self, value):
        self.cron = value.get("cron")
        self.scheduledTime = value.get("scheduledTime")

class WorkflowEvent:
    def __init__(self, value):
        self.instance = value.get("instance")
        self.payload = value.get("payload")

class WorkflowActivityEvent:
    def __init__(self, value):
        self.instance = value.get("instance")
        self.name = value.get("name")
        self.task = value.get("task")
        self.payload = value.get("payload")

class WebSocket:
    CONNECTING = 0
    OPEN = 1
    CLOSING = 2
    CLOSED = 3

    def __init__(self, id):
        self.id = str(id)
        self.readyState = WebSocket.OPEN
        self._outbound = []

    def accept(self):
        if self.readyState == WebSocket.CONNECTING:
            self.readyState = WebSocket.OPEN

    def send(self, message):
        if self.readyState != WebSocket.OPEN:
            raise RuntimeError("WebSocket is not open")
        self._outbound.append(str(message))

    def close(self, code=1000, reason=""):
        if self.readyState == WebSocket.CLOSED:
            return
        self.readyState = WebSocket.CLOSED

    def drain(self):
        outbound = self._outbound
        self._outbound = []
        return outbound

class WebSocketClose:
    def __init__(self, value):
        self.code = int(value.get("code", 1000))
        self.reason = value.get("reason", "")
        self.wasClean = bool(value.get("wasClean", False))

def bytes_body(value):
    if value is None:
        return []
    if isinstance(value, bytes):
        return list(value)
    if isinstance(value, bytearray):
        return list(bytes(value))
    if isinstance(value, str):
        return list(value.encode("utf-8"))
    return list(json.dumps(value).encode("utf-8"))

class KvNamespace:
    def __init__(self, scope, provider=None):
        self.scope = str(scope)
        self.provider = provider or {"kind": "native"}

    async def get(self, key, options=None):
        value = HOST.call("kv_get", {"namespace": self.scope, "key": str(key), "provider": self.provider})
        if value is None:
            return None
        raw = bytes(value)
        kind = (options or {}).get("type", "text")
        if kind == "json":
            return json.loads(raw.decode("utf-8"))
        if kind in ("bytes", "arrayBuffer"):
            return raw
        return raw.decode("utf-8")

    async def put(self, key, value, options=None):
        HOST.call("kv_put", {"namespace": self.scope, "key": str(key), "value": bytes_body(value), "provider": self.provider})

    async def delete(self, key):
        HOST.call("kv_delete", {"namespace": self.scope, "key": str(key), "provider": self.provider})

    async def list(self, options=None):
        options = options or {}
        return HOST.call("kv_list", {"namespace": self.scope, "prefix": options.get("prefix"), "cursor": options.get("cursor"), "limit": options.get("limit"), "provider": self.provider})

class R2Object:
    def __init__(self, value):
        self.key = value.get("key")
        self.size = value.get("size", len(value.get("body", [])))
        self.httpMetadata = types.SimpleNamespace(contentType=value.get("contentType"))
        self.customMetadata = value.get("customMetadata", {})
        self._body = bytes(value.get("body", []))

    async def text(self):
        return self._body.decode("utf-8")

    async def json(self):
        return json.loads(await self.text())

    async def arrayBuffer(self):
        return self._body

class R2Bucket:
    def __init__(self, bucket, prefix="", provider=None):
        self.bucket = str(bucket)
        self.prefix = str(prefix or "")
        self.provider = provider or {"kind": "memory"}

    def _key(self, key):
        return self.prefix + str(key)

    async def put(self, key, body, options=None):
        options = options or {}
        HOST.call("r2_put", {"bucket": self.bucket, "key": self._key(key), "body": bytes_body(body), "contentType": (options.get("httpMetadata") or {}).get("contentType"), "customMetadata": options.get("customMetadata") or {}})

    async def get(self, key):
        value = HOST.call("r2_get", {"bucket": self.bucket, "key": self._key(key)})
        return None if value is None else R2Object(value)

    async def delete(self, key):
        keys = key if isinstance(key, list) else [key]
        for item in keys:
            HOST.call("r2_delete", {"bucket": self.bucket, "key": self._key(item)})

    async def list(self, options=None):
        options = options or {}
        return HOST.call("r2_list", {"bucket": self.bucket, "prefix": self._key(options.get("prefix") or ""), "cursor": options.get("cursor"), "limit": options.get("limit")})

class Queue:
    def __init__(self, queue, provider=None):
        self.queue = str(queue)
        self.provider = provider or {"kind": "memory"}

    async def send(self, body, options=None):
        options = options or {}
        HOST.call("queue_send", {"queue": self.queue, "body": bytes_body(body), "contentType": options.get("contentType"), "delaySeconds": options.get("delaySeconds"), "dedupId": options.get("dedupId"), "partition": options.get("partition")})

    async def sendBatch(self, messages):
        for message in messages or []:
            await self.send(message.get("body"), message)

class D1PreparedStatement:
    def __init__(self, database, sql, parameters=None):
        self.database = database
        self.sql = str(sql)
        self.parameters = parameters or []

    def bind(self, *parameters):
        return D1PreparedStatement(self.database, self.sql, list(parameters))

    async def all(self):
        return HOST.call("d1_query", {"database": self.database, "sql": self.sql, "parameters": self.parameters})

    async def run(self):
        return await self.all()

    async def first(self, column=None):
        result = await self.all()
        rows = result.get("results", [])
        if not rows:
            return None
        row = rows[0]
        return row if column is None else row.get(column)

    async def raw(self):
        return (await self.all()).get("raw", [])

class D1Database:
    def __init__(self, database, provider=None):
        self.database = str(database)
        self.provider = provider or {"kind": "native_sqlite"}

    def prepare(self, sql):
        return D1PreparedStatement(self.database, sql)

    async def exec(self, sql):
        return HOST.call("d1_exec", {"database": self.database, "sql": str(sql)})

class ServiceBinding:
    def __init__(self, service):
        self.service = str(service)

    async def fetch(self, input, init=None):
        request = input if isinstance(input, Request) else Request({"url": str(input), "method": (init or {}).get("method", "GET"), "headers": list(((init or {}).get("headers") or {}).items()) if isinstance((init or {}).get("headers"), dict) else ((init or {}).get("headers") or []), "body": bytes_body((init or {}).get("body"))})
        return response_from_host(HOST.call("service_fetch", {"service": self.service, "request": encode_request(request)}))

class DurableObjectId:
    def __init__(self, namespace, value, name=None):
        self.namespace = namespace
        self.value = value
        self.name = name

    def __str__(self):
        return f"{self.namespace}:{self.value}"

class DurableObjectStub:
    def __init__(self, id, class_name):
        self.id = id
        self.className = class_name
        self.name = id.name

    async def fetch(self, input, init=None):
        request = input if isinstance(input, Request) else Request({"url": str(input), "method": (init or {}).get("method", "GET"), "headers": list(((init or {}).get("headers") or {}).items()) if isinstance((init or {}).get("headers"), dict) else ((init or {}).get("headers") or []), "body": bytes_body((init or {}).get("body"))})
        return response_from_host(HOST.call("durable_object_fetch", {"namespace": self.id.namespace, "id": self.id.value, "name": self.id.name, "className": self.className, "request": encode_request(request)}))

class DurableObjectNamespace:
    def __init__(self, binding, class_name):
        self.binding = binding
        self.className = class_name

    def idFromName(self, name):
        return DurableObjectId(self.binding, "name:" + str(name), str(name))

    def idFromString(self, id):
        text = str(id)
        prefix = self.binding + ":"
        if not text.startswith(prefix):
            raise TypeError("Durable Object id belongs to a different namespace")
        raw = text[len(prefix):]
        return DurableObjectId(self.binding, raw, raw[5:] if raw.startswith("name:") else None)

    def newUniqueId(self):
        return DurableObjectId(self.binding, "unique:" + str(HOST.call("workflow_id", {})))

    def get(self, id):
        return DurableObjectStub(id, self.className)

class Cache:
    def __init__(self, name="default"):
        self.name = name

    async def match(self, request):
        key = request.url if isinstance(request, Request) else str(request)
        value = HOST.call("cache_match", {"cache": self.name, "key": key})
        return None if value is None else response_from_host(value)

    async def put(self, request, response):
        key = request.url if isinstance(request, Request) else str(request)
        HOST.call("cache_put", {"cache": self.name, "key": key, "status": response.status, "headers": list(response.headers.items()), "body": list(response.body)})

    async def delete(self, request):
        key = request.url if isinstance(request, Request) else str(request)
        return HOST.call("cache_delete", {"cache": self.name, "key": key})

class CacheStorage:
    def __init__(self):
        self.default = Cache("default")

    async def open(self, name):
        return Cache(str(name))

class Ai:
    async def run(self, model, input, options=None):
        return HOST.call("ai_run", {"command": "run", "model": str(model), "input": input, "options": options or {}})

class Workflow:
    def __init__(self, binding, id):
        self.binding = binding
        self.id = id

    async def status(self):
        return HOST.call("workflow_status", {"binding": self.binding, "id": self.id})

    async def terminate(self, reason=None):
        return HOST.call("workflow_write", {"binding": self.binding, "id": self.id, "status": "terminated", "reason": reason})

    async def restart(self):
        return HOST.call("workflow_write", {"binding": self.binding, "id": self.id, "status": "running"})

class WorkflowBinding:
    def __init__(self, binding):
        self.binding = str(binding)

    async def create(self, options=None):
        options = options or {}
        id = options.get("id") or str(HOST.call("workflow_id", {}))
        HOST.call("workflow_write", {"binding": self.binding, "id": id, "status": "running"})
        return Workflow(self.binding, id)

    def get(self, id):
        return Workflow(self.binding, str(id))

def encode_request(request):
    return {"method": request.method, "url": request.url, "headers": list(request.headers.items()), "body": list(request.body), "mtls": None}

async def fetch(input, init=None):
    request = input if isinstance(input, Request) else Request({"url": str(input), "method": (init or {}).get("method", "GET"), "headers": list(((init or {}).get("headers") or {}).items()) if isinstance((init or {}).get("headers"), dict) else ((init or {}).get("headers") or []), "body": bytes_body((init or {}).get("body"))})
    return response_from_host(HOST.call("outbound_fetch", encode_request(request)))

def hydrate_env(values):
    raw = dict(values or {})
    bindings = json.loads(raw.pop("__perenBindings", "{}"))
    raw.pop("__perenCache", None)
    env = types.SimpleNamespace(**raw)
    for name, binding in bindings.items():
        kind = binding.get("type")
        if kind == "kv":
            setattr(env, name, KvNamespace(binding.get("scope"), binding.get("provider")))
        elif kind == "d1":
            setattr(env, name, D1Database(name, binding.get("provider")))
        elif kind == "r2":
            setattr(env, name, R2Bucket(binding.get("bucket"), binding.get("prefix", ""), binding.get("provider")))
        elif kind == "queue":
            setattr(env, name, Queue(binding.get("queue"), binding.get("provider")))
        elif kind == "service":
            setattr(env, name, ServiceBinding(binding.get("service")))
        elif kind == "durable_object_namespace":
            setattr(env, name, DurableObjectNamespace(name, binding.get("className")))
        elif kind == "workflow":
            setattr(env, name, WorkflowBinding(name))
        elif kind == "ai":
            setattr(env, name, Ai())
        elif kind in ("outbound", "aws_sigv4", "cache", "rate_limiter", "analytics_engine", "vectorize", "images", "container", "hyperdrive", "loader", "dispatcher", "mtls_certificate"):
            setattr(env, name, types.SimpleNamespace(type=kind, metadata=binding))
    return env

async def maybe_await(value):
    if hasattr(value, "__await__"):
        return await value
    return value

class BundleImporter(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, modules):
        self._modules = {}
        for name, source in modules.items():
            if name.endswith(".py"):
                self._modules[name[:-3].replace("/", ".")] = (name, source)
            if name.endswith("/__init__.py"):
                self._modules[name[:-12].replace("/", ".")] = (name, source)

    def find_spec(self, fullname, path=None, target=None):
        if fullname not in self._modules:
            return None
        return importlib.util.spec_from_loader(fullname, self)

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        name, source = self._modules[module.__name__]
        install_worker_globals(module)
        exec(compile(source, name, "exec"), module.__dict__)

def install_workers_module():
    workers = types.ModuleType("workers")
    workers.Headers = Headers
    workers.Request = Request
    workers.Response = Response
    workers.WorkerEntrypoint = WorkerEntrypoint
    workers.KvNamespace = KvNamespace
    workers.R2Bucket = R2Bucket
    workers.Queue = Queue
    workers.D1Database = D1Database
    workers.DurableObjectNamespace = DurableObjectNamespace
    workers.WebSocket = WebSocket
    workers.NonRetryableError = RuntimeError
    sys.modules["workers"] = workers

def install_ffi_modules():
    js = types.ModuleType("js")
    js.Headers = Headers
    js.Request = Request
    js.Response = Response
    js.WebSocket = WebSocket
    js.fetch = fetch
    js.caches = CacheStorage()
    js.Object = types.SimpleNamespace(fromEntries=lambda entries: dict(entries))
    sys.modules["js"] = js

    pyodide = types.ModuleType("pyodide")
    ffi = types.ModuleType("pyodide.ffi")
    ffi.to_js = lambda value, **kwargs: value
    ffi.to_py = lambda value, **kwargs: value
    pyodide.ffi = ffi
    sys.modules["pyodide"] = pyodide
    sys.modules["pyodide.ffi"] = ffi

def install_worker_globals(module):
    module.Headers = Headers
    module.Request = Request
    module.Response = Response
    module.WorkerEntrypoint = WorkerEntrypoint
    module.KvNamespace = KvNamespace
    module.R2Bucket = R2Bucket
    module.Queue = Queue
    module.D1Database = D1Database
    module.DurableObjectNamespace = DurableObjectNamespace
    module.WebSocket = WebSocket
    module.caches = CacheStorage()
    module.fetch = fetch
    module.NonRetryableError = RuntimeError

def load_module(payload):
    install_workers_module()
    install_ffi_modules()
    importer = BundleImporter(payload.get("modules", {}))
    sys.meta_path.insert(0, importer)
    module = types.ModuleType("worker")
    install_worker_globals(module)
    exec(compile(payload["source"], "worker.py", "exec"), module.__dict__)
    return module

def entrypoint(module, env, ctx):
    class_name = globals().get("__perenDurableClass")
    if class_name:
        entry = getattr(module, class_name, None)
        if entry is None:
            raise TypeError(f"Python entrypoint must define {class_name}")
    else:
        entry = getattr(module, "Default", None)
    if entry is None:
        return None
    instance = entry()
    instance.env = env
    instance.ctx = ctx
    return instance

def handler(module, instance, name):
    if instance is not None and hasattr(instance, name):
        return ("entrypoint", getattr(instance, name))
    selected = getattr(module, name, None)
    if selected is None:
        return None
    return ("module", selected)

def call(selected, *args, env, ctx):
    kind, fn = selected
    if kind == "entrypoint":
        return fn(*args)
    return fn(*args, env, ctx)

def encode_response(value):
    if isinstance(value, Response):
        return {
            "status": value.status,
            "headers": list(value.headers.items()),
            "body": list(value.body),
            "upgrade": False,
            "websocketId": None,
        }
    if isinstance(value, str):
        return encode_response(Response(value, headers={"content-type": "text/plain; charset=utf-8"}))
    if value is None:
        return encode_response(Response(b"", status=204))
    return encode_response(Response.json(value))

async def invoke(payload):
    memory_limit = payload.get("memoryLimitBytes")
    if memory_limit is not None and _RESOURCE is not None:
        try:
            _RESOURCE.setrlimit(_RESOURCE.RLIMIT_AS, (int(memory_limit), int(memory_limit)))
        except (OSError, ValueError):
            pass
    timeout = payload.get("timeoutSeconds")
    if timeout is not None:
        def timeout_handler(signum, frame):
            raise TimeoutError("Python invocation exceeded its execution deadline")
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.setitimer(signal.ITIMER_REAL, max(float(timeout), 0.001))
    module = load_module(payload)
    globals()["__perenDurableClass"] = payload.get("durableClass")
    env = hydrate_env(payload.get("env", {}))
    ctx = Context()
    instance = entrypoint(module, env, ctx)
    event = payload["event"]
    kind = event["kind"]
    if kind == "fetch":
        selected = handler(module, instance, "fetch")
        if selected is None:
            raise TypeError("python worker must define Default.fetch(request) or fetch(request, env, ctx)")
        result = await maybe_await(call(selected, Request(event["request"]), env=env, ctx=ctx))
        await ctx.drain()
        return {"kind": "http", "response": encode_response(result)}
    if kind == "scheduled":
        selected = handler(module, instance, "scheduled")
        if selected is not None:
            await maybe_await(call(selected, Scheduled(event["event"]), env=env, ctx=ctx))
        await ctx.drain()
        return {"kind": "unit"}
    if kind == "queue":
        selected = handler(module, instance, "queue")
        batch = QueueBatch(event["event"])
        if selected is not None:
            await maybe_await(call(selected, batch, env=env, ctx=ctx))
        await ctx.drain()
        return {"kind": "queue", "dispositions": batch.dispositions()}
    if kind == "alarm":
        selected = handler(module, instance, "alarm")
        if selected is not None:
            await maybe_await(call(selected, env=env, ctx=ctx))
        await ctx.drain()
        return {"kind": "unit"}
    if kind == "tail":
        selected = handler(module, instance, "tail")
        if selected is not None:
            await maybe_await(call(selected, event["event"], env=env, ctx=ctx))
        await ctx.drain()
        return {"kind": "unit"}
    if kind == "workflow":
        selected = handler(module, instance, "workflow")
        if selected is not None:
            await maybe_await(call(selected, WorkflowEvent(event["event"]), env=env, ctx=ctx))
        await ctx.drain()
        return {"kind": "unit"}
    if kind == "activity":
        selected = handler(module, instance, "activity")
        if selected is None:
            raise TypeError("python worker must define Default.activity(event) for workflow activity dispatch")
        result = await maybe_await(call(selected, WorkflowActivityEvent(event["event"]), env=env, ctx=ctx))
        await ctx.drain()
        return {"kind": "activity", "value": result}
    if kind == "websocketMessage":
        selected = handler(module, instance, "webSocketMessage")
        socket = WebSocket(event["event"]["id"])
        if selected is not None:
            await maybe_await(call(selected, socket, event["event"].get("message", ""), env=env, ctx=ctx))
        await ctx.drain()
        return {"kind": "websocket", "outbound": socket.drain()}
    if kind == "websocketClose":
        selected = handler(module, instance, "webSocketClose")
        socket = WebSocket(event["event"]["id"])
        if selected is not None:
            await maybe_await(call(selected, socket, WebSocketClose(event["event"]), env=env, ctx=ctx))
        socket.close(event["event"].get("code", 1000), event["event"].get("reason", ""))
        await ctx.drain()
        return {"kind": "websocket", "outbound": socket.drain()}
    raise TypeError(f"unsupported python event: {kind}")

def main():
    payload = json.loads(sys.stdin.readline())
    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            result = asyncio.run(invoke(payload))
        result["logs"] = [{"level": "info", "message": line} for line in output.getvalue().splitlines()]
        sys.__stdout__.write(json.dumps({"kind": "result", "ok": True, "result": result}) + "\n")
        sys.__stdout__.flush()
    except BaseException as error:
        sys.__stdout__.write(json.dumps({"kind": "result", "ok": False, "error": f"{type(error).__name__}: {error}"}) + "\n")
        sys.__stdout__.flush()

main()
"#;

pub struct PythonRuntime {
    source: String,
    modules: std::collections::BTreeMap<String, String>,
    execution_timeout: std::time::Duration,
    execution_memory_limit: usize,
    environment: WorkerEnvironment,
    hosts: PythonHosts,
    logs: Vec<WorkerLogEvent>,
    committed_revision: peren_primitives::StorageRevision,
    durable_class: Option<String>,
}

#[derive(Clone, Default)]
struct PythonHosts {
    storage: Option<Arc<dyn DurableStorageHost>>,
    fetch: Option<Arc<dyn OutboundFetchHost>>,
    queue: Option<Arc<dyn QueueProducerHost>>,
    r2: Option<Arc<dyn R2BucketHost>>,
    service: Option<Arc<dyn ServiceBindingHost>>,
    durable: Option<Arc<dyn DurableObjectHost>>,
    cache: Option<Arc<dyn CacheHost>>,
    kv: Option<Arc<dyn KvHost>>,
    ai: Option<Arc<dyn AiHost>>,
}

impl PythonHosts {
    fn from_capabilities(capabilities: Capabilities) -> Self {
        Self {
            storage: Some(capabilities.storage),
            fetch: capabilities.fetch,
            queue: capabilities.queue,
            r2: capabilities.r2,
            service: capabilities.service,
            durable: capabilities.durable,
            cache: capabilities.cache,
            kv: capabilities.kv,
            ai: capabilities.ai,
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StorageRequest {
    scope: String,
    key: Vec<u8>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoragePutRequest {
    scope: String,
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StorageListRequest {
    scope: String,
    options: crate::ListOptions,
}

#[derive(serde::Deserialize)]
struct PythonKvGet {
    namespace: String,
    key: String,
    #[serde(default)]
    provider: serde_json::Value,
}

#[derive(serde::Deserialize)]
struct PythonKvPut {
    namespace: String,
    key: String,
    value: Vec<u8>,
    #[serde(default)]
    provider: serde_json::Value,
}

#[derive(serde::Deserialize)]
struct PythonKvList {
    namespace: String,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    provider: serde_json::Value,
}

#[derive(serde::Deserialize)]
struct D1Request {
    database: String,
    sql: String,
    #[serde(default)]
    parameters: Vec<serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct WorkflowStatusRequest {
    binding: String,
    id: String,
}

#[derive(serde::Deserialize)]
struct WorkflowWriteRequest {
    binding: String,
    id: String,
    status: String,
    #[serde(default)]
    reason: Option<String>,
}

impl PythonRuntime {
    #[allow(
        clippy::needless_pass_by_value,
        reason = "runtime adapters consume owned packages at the shared service-runtime boundary"
    )]
    pub fn load(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
    ) -> Result<Self, EngineError> {
        Self::load_inner(bundle, limits, environment, PythonHosts::default())
    }

    pub fn load_with_storage(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        storage: Arc<dyn DurableStorageHost>,
    ) -> Result<Self, EngineError> {
        Self::load_inner(
            bundle,
            limits,
            environment,
            PythonHosts {
                storage: Some(storage),
                ..PythonHosts::default()
            },
        )
    }

    pub fn load_with_r2(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        storage: Arc<dyn DurableStorageHost>,
        r2: Arc<dyn R2BucketHost>,
    ) -> Result<Self, EngineError> {
        Self::load_inner(
            bundle,
            limits,
            environment,
            PythonHosts {
                storage: Some(storage),
                r2: Some(r2),
                ..PythonHosts::default()
            },
        )
    }

    pub fn load_with_capabilities(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        capabilities: Capabilities,
    ) -> Result<Self, EngineError> {
        Self::load_inner(
            bundle,
            limits,
            environment,
            PythonHosts::from_capabilities(capabilities),
        )
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "runtime adapters consume owned packages at the shared service-runtime boundary"
    )]
    fn load_inner(
        bundle: WorkerBundle,
        limits: IsolateLimits,
        environment: WorkerEnvironment,
        hosts: PythonHosts,
    ) -> Result<Self, EngineError> {
        if !unsafe_python_compat_enabled(&environment) {
            return Err(EngineError::Python(
                "python:compat requires PEREN_UNSAFE_PYTHON_COMPAT=1; use python:pyodide for production execution".into(),
            ));
        }
        let module = bundle
            .module(bundle.entry())
            .ok_or_else(|| EngineError::Python("python entry module is missing".into()))?;
        let source = String::from_utf8(module.source().to_vec())
            .map_err(|error| EngineError::Python(error.to_string()))?;
        let modules = bundle
            .modules()
            .filter(|(name, _)| *name != bundle.entry())
            .filter(|(_, module)| module.kind() == crate::ModuleKind::Python)
            .map(|(name, module)| {
                String::from_utf8(module.source().to_vec())
                    .map(|source| (name.as_ref().to_string(), source))
                    .map_err(|error| EngineError::Python(error.to_string()))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>, _>>()?;
        Ok(Self {
            source,
            modules,
            execution_timeout: limits.execution_time(),
            execution_memory_limit: limits.heap_bytes(),
            environment,
            hosts,
            logs: Vec::new(),
            committed_revision: peren_primitives::StorageRevision::default(),
            durable_class: None,
        })
    }

    pub fn dispatch_http(
        &mut self,
        request: HttpRequest,
        limits: InvocationLimits,
    ) -> Result<HttpResponse, EngineError> {
        limits.validate_request(u64::try_from(request.body.len()).unwrap_or(u64::MAX))?;
        let event = serde_json::json!({
            "kind": "fetch",
            "request": serde_json::to_value(request)
                .map_err(|error| EngineError::Request(error.to_string()))?,
        });
        let value = self.invoke(&event, Some(limits.cpu_time()))?;
        let response = serde_json::from_value::<HttpResponse>(
            value
                .get("response")
                .cloned()
                .ok_or_else(|| EngineError::Response("missing python response".into()))?,
        )
        .map_err(|error| EngineError::Response(error.to_string()))?;
        limits.validate_response(u64::try_from(response.body.len()).unwrap_or(u64::MAX))?;
        Ok(response)
    }

    pub fn dispatch_alarm(&mut self) -> Result<(), EngineError> {
        let event = serde_json::json!({ "kind": "alarm" });
        self.invoke(&event, None)?;
        Ok(())
    }

    pub fn dispatch_scheduled(&mut self, scheduled: ScheduledEvent) -> Result<(), EngineError> {
        let event = serde_json::json!({
            "kind": "scheduled",
            "event": serde_json::to_value(scheduled)
                .map_err(|error| EngineError::Request(error.to_string()))?,
        });
        self.invoke(&event, None)?;
        Ok(())
    }

    pub fn dispatch_queue(&mut self, queue: QueueEvent) -> Result<QueueDispatch, EngineError> {
        let event = serde_json::json!({
            "kind": "queue",
            "event": serde_json::to_value(queue)
                .map_err(|error| EngineError::Request(error.to_string()))?,
        });
        let value = self.invoke(&event, None)?;
        Ok(QueueDispatch {
            dispositions: serde_json::from_value(
                value
                    .get("dispositions")
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::Array(Vec::new())),
            )
            .map_err(|error| EngineError::Response(error.to_string()))?,
        })
    }

    pub fn dispatch_tail(&mut self, tail: TailEvent) -> Result<(), EngineError> {
        let event = serde_json::json!({
            "kind": "tail",
            "event": serde_json::to_value(tail)
                .map_err(|error| EngineError::Request(error.to_string()))?,
        });
        self.invoke(&event, None)?;
        Ok(())
    }

    pub fn dispatch_websocket_message(
        &mut self,
        event: WebSocketMessageEvent,
    ) -> Result<WebSocketDispatch, EngineError> {
        let event = serde_json::json!({
            "kind": "websocketMessage",
            "event": serde_json::to_value(event)
                .map_err(|error| EngineError::Request(error.to_string()))?,
        });
        let value = self.invoke(&event, None)?;
        serde_json::from_value(value).map_err(|error| EngineError::Response(error.to_string()))
    }

    pub fn dispatch_websocket_close(
        &mut self,
        event: WebSocketCloseEvent,
    ) -> Result<WebSocketDispatch, EngineError> {
        let event = serde_json::json!({
            "kind": "websocketClose",
            "event": serde_json::to_value(event)
                .map_err(|error| EngineError::Request(error.to_string()))?,
        });
        let value = self.invoke(&event, None)?;
        serde_json::from_value(value).map_err(|error| EngineError::Response(error.to_string()))
    }

    pub fn dispatch_workflow(&mut self, event: WorkflowEvent) -> Result<(), EngineError> {
        let event = serde_json::json!({
            "kind": "workflow",
            "event": serde_json::to_value(event)
                .map_err(|error| EngineError::Request(error.to_string()))?,
        });
        self.invoke(&event, None)?;
        Ok(())
    }

    pub fn dispatch_workflow_activity(
        &mut self,
        event: WorkflowActivityEvent,
    ) -> Result<serde_json::Value, EngineError> {
        let event = serde_json::json!({
            "kind": "activity",
            "event": serde_json::to_value(event)
                .map_err(|error| EngineError::Request(error.to_string()))?,
        });
        let value = self.invoke(&event, None)?;
        Ok(value
            .get("value")
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }

    #[must_use]
    pub fn committed_revision(&self) -> peren_primitives::StorageRevision {
        self.committed_revision
    }

    #[allow(
        clippy::unnecessary_wraps,
        reason = "runtime adapters share a fallible durable-class binding contract"
    )]
    pub fn bind_durable_class(&mut self, class_name: &str) -> Result<(), EngineError> {
        self.durable_class = Some(class_name.to_string());
        self.logs.clear();
        Ok(())
    }

    pub fn take_console_events(&mut self) -> Vec<WorkerLogEvent> {
        std::mem::take(&mut self.logs)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the protocol loop keeps one child process invocation and host-call replies together"
    )]
    fn invoke(
        &mut self,
        event: &serde_json::Value,
        timeout: Option<std::time::Duration>,
    ) -> Result<serde_json::Value, EngineError> {
        let timeout = timeout
            .unwrap_or(self.execution_timeout)
            .min(self.execution_timeout);
        let timeout_seconds = if timeout == std::time::Duration::MAX {
            serde_json::Value::Null
        } else {
            serde_json::Number::from_f64(timeout.as_secs_f64())
                .map_or(serde_json::Value::Null, serde_json::Value::Number)
        };
        let invocation_dir = tempfile::tempdir()
            .map_err(|error| EngineError::Python(format!("create python sandbox: {error}")))?;
        let mut child = Command::new("python3")
            .arg("-I")
            .arg("-S")
            .arg("-c")
            .arg(RUNNER)
            .env_clear()
            .current_dir(invocation_dir.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| EngineError::Python(error.to_string()))?;
        let input = serde_json::to_vec(&serde_json::json!({
            "source": self.source,
            "modules": self.modules,
            "env": self.environment.values(),
            "event": event,
            "timeoutSeconds": timeout_seconds,
            "memoryLimitBytes": self.execution_memory_limit,
            "durableClass": self.durable_class,
        }))
        .map_err(|error| EngineError::Request(error.to_string()))?;
        child
            .stdin
            .as_mut()
            .ok_or_else(|| EngineError::Python("python stdin unavailable".into()))?
            .write_all(&input)
            .map_err(|error| EngineError::Python(error.to_string()))?;
        child
            .stdin
            .as_mut()
            .ok_or_else(|| EngineError::Python("python stdin unavailable".into()))?
            .write_all(b"\n")
            .map_err(|error| EngineError::Python(error.to_string()))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| EngineError::Python("python stdout unavailable".into()))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| EngineError::Python("python stdin unavailable".into()))?;
        let mut child = ChildGuard::new(child);
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        let _ = tx.send(Ok(None));
                        break;
                    }
                    Ok(_) => {
                        if tx.send(Ok(Some(line.clone()))).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = tx.send(Err(error.to_string()));
                        break;
                    }
                }
            }
        });
        let deadline = deadline(timeout);
        while let Some(line) = recv_python_line(&rx, deadline, &mut child)? {
            let envelope = serde_json::from_str::<serde_json::Value>(&line)
                .map_err(|error| EngineError::Python(error.to_string()))?;
            match envelope.get("kind").and_then(serde_json::Value::as_str) {
                Some("host") => {
                    let id = envelope
                        .get("id")
                        .cloned()
                        .ok_or_else(|| EngineError::Python("host call id missing".into()))?;
                    let op = envelope
                        .get("op")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| EngineError::Python("host call op missing".into()))?;
                    let payload = envelope
                        .get("payload")
                        .cloned()
                        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
                    let response = match self.host_call(op, payload) {
                        Ok(result) => serde_json::json!({ "id": id, "ok": true, "result": result }),
                        Err(error) => {
                            serde_json::json!({ "id": id, "ok": false, "error": error.to_string() })
                        }
                    };
                    serde_json::to_writer(&mut stdin, &response)
                        .map_err(|error| EngineError::Python(error.to_string()))?;
                    stdin
                        .write_all(b"\n")
                        .map_err(|error| EngineError::Python(error.to_string()))?;
                    stdin
                        .flush()
                        .map_err(|error| EngineError::Python(error.to_string()))?;
                }
                Some("result") => {
                    let status = child
                        .wait()
                        .map_err(|error| EngineError::Python(error.to_string()))?;
                    if !status.success() {
                        return Err(EngineError::Python(format!(
                            "python exited with status {status}"
                        )));
                    }
                    if !envelope
                        .get("ok")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false)
                    {
                        return Err(EngineError::Python(
                            envelope
                                .get("error")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("python invocation failed")
                                .to_string(),
                        ));
                    }
                    let result = envelope
                        .get("result")
                        .cloned()
                        .ok_or_else(|| EngineError::Python("python result missing".into()))?;
                    if let Some(logs) = result.get("logs") {
                        let events = serde_json::from_value::<Vec<PythonLog>>(logs.clone())
                            .map_err(|error| EngineError::Python(error.to_string()))?;
                        self.logs.extend(events.into_iter().map(Into::into));
                    }
                    return Ok(result);
                }
                _ => {
                    return Err(EngineError::Python(
                        "unknown python protocol envelope".into(),
                    ));
                }
            }
        }
        let status = child
            .wait()
            .map_err(|error| EngineError::Python(error.to_string()))?;
        Err(EngineError::Python(format!(
            "python exited before returning a result: {status}"
        )))
    }

    fn host_call(
        &mut self,
        op: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, EngineError> {
        block_on_host(self.host_call_async(op, payload))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the runtime host-call dispatch table is intentionally centralized for parity review"
    )]
    async fn host_call_async(
        &mut self,
        op: &str,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, EngineError> {
        match op {
            "storage_begin" => {
                self.storage()?.begin().await.map_err(host_error)?;
                Ok(serde_json::Value::Null)
            }
            "storage_get" => {
                let request: StorageRequest = parse_payload(payload)?;
                to_json(
                    self.storage()?
                        .load(&request.scope, &request.key)
                        .await
                        .map_err(host_error)?,
                )
            }
            "storage_put" => {
                let request: StoragePutRequest = parse_payload(payload)?;
                self.storage()?
                    .put(&request.scope, &request.key, &request.value)
                    .await
                    .map_err(host_error)?;
                Ok(serde_json::Value::Null)
            }
            "storage_delete" => {
                let request: StorageRequest = parse_payload(payload)?;
                to_json(
                    self.storage()?
                        .delete(&request.scope, &request.key)
                        .await
                        .map_err(host_error)?,
                )
            }
            "storage_list" => {
                let request: StorageListRequest = parse_payload(payload)?;
                to_json(
                    self.storage()?
                        .list(&request.scope, request.options)
                        .await
                        .map_err(host_error)?,
                )
            }
            "storage_sql" => {
                let request: SqlQuery = parse_payload(payload)?;
                to_json(self.storage()?.sql(request).await.map_err(host_error)?)
            }
            "storage_commit" => {
                let revision = self.storage()?.commit().await.map_err(host_error)?;
                self.committed_revision = revision;
                to_json(revision.get())
            }
            "storage_rollback" => {
                self.storage()?.rollback().await.map_err(host_error)?;
                Ok(serde_json::Value::Null)
            }
            "kv_get" => {
                let request: PythonKvGet = parse_payload(payload)?;
                if is_native(&request.provider) {
                    to_json(
                        self.storage()?
                            .load(&request.namespace, request.key.as_bytes())
                            .await
                            .map_err(host_error)?,
                    )
                } else {
                    to_json(
                        self.kv()?
                            .get(KvGet {
                                namespace: request.namespace,
                                key: request.key,
                            })
                            .await
                            .map_err(host_error)?,
                    )
                }
            }
            "kv_put" => {
                let request: PythonKvPut = parse_payload(payload)?;
                if is_native(&request.provider) {
                    self.storage()?.begin().await.map_err(host_error)?;
                    let mut committed = false;
                    let result = async {
                        self.storage()?
                            .put(&request.namespace, request.key.as_bytes(), &request.value)
                            .await
                            .map_err(host_error)?;
                        self.committed_revision =
                            self.storage()?.commit().await.map_err(host_error)?;
                        committed = true;
                        Ok::<(), EngineError>(())
                    }
                    .await;
                    if !committed {
                        let _ = self.storage()?.rollback().await;
                    }
                    result?;
                } else {
                    self.kv()?
                        .put(KvPut {
                            namespace: request.namespace,
                            key: request.key,
                            value: request.value,
                        })
                        .await
                        .map_err(host_error)?;
                }
                Ok(serde_json::Value::Null)
            }
            "kv_delete" => {
                let request: PythonKvGet = parse_payload(payload)?;
                if is_native(&request.provider) {
                    self.storage()?.begin().await.map_err(host_error)?;
                    let mut committed = false;
                    let result = async {
                        let deleted = self
                            .storage()?
                            .delete(&request.namespace, request.key.as_bytes())
                            .await
                            .map_err(host_error)?;
                        self.committed_revision =
                            self.storage()?.commit().await.map_err(host_error)?;
                        committed = true;
                        Ok::<_, EngineError>(serde_json::Value::Bool(deleted))
                    }
                    .await;
                    if !committed {
                        let _ = self.storage()?.rollback().await;
                    }
                    result
                } else {
                    to_json(
                        self.kv()?
                            .delete(KvGet {
                                namespace: request.namespace,
                                key: request.key,
                            })
                            .await
                            .map_err(host_error)?,
                    )
                }
            }
            "kv_list" => {
                let request: PythonKvList = parse_payload(payload)?;
                if is_native(&request.provider) {
                    to_json(
                        self.storage()?
                            .list(
                                &request.namespace,
                                crate::ListOptions {
                                    prefix: request.prefix.map(String::into_bytes),
                                    cursor: request.cursor.map(String::into_bytes),
                                    limit: request.limit,
                                },
                            )
                            .await
                            .map_err(host_error)?,
                    )
                } else {
                    to_json(
                        self.kv()?
                            .list(KvList {
                                namespace: request.namespace,
                                prefix: request.prefix,
                                cursor: request.cursor,
                                limit: request.limit,
                            })
                            .await
                            .map_err(host_error)?,
                    )
                }
            }
            "r2_put" => {
                self.r2()?
                    .put(parse_payload::<R2Put>(payload)?)
                    .await
                    .map_err(host_error)?;
                Ok(serde_json::Value::Null)
            }
            "r2_get" => to_json(
                self.r2()?
                    .get(parse_payload::<R2Get>(payload)?)
                    .await
                    .map_err(host_error)?,
            ),
            "r2_delete" => {
                self.r2()?
                    .delete(parse_payload::<R2Delete>(payload)?)
                    .await
                    .map_err(host_error)?;
                Ok(serde_json::Value::Null)
            }
            "r2_list" => to_json(
                self.r2()?
                    .list(parse_payload::<R2List>(payload)?)
                    .await
                    .map_err(host_error)?,
            ),
            "queue_send" => {
                self.queue()?
                    .send(parse_payload::<QueueSend>(payload)?)
                    .await
                    .map_err(host_error)?;
                Ok(serde_json::Value::Null)
            }
            "outbound_fetch" => to_json(
                self.fetch()?
                    .fetch(parse_payload::<HttpRequest>(payload)?)
                    .await
                    .map_err(host_error)?,
            ),
            "service_fetch" => to_json(
                self.service()?
                    .fetch(parse_payload::<ServiceFetch>(payload)?)
                    .await
                    .map_err(host_error)?,
            ),
            "durable_object_fetch" => to_json(
                self.durable()?
                    .fetch(parse_payload::<DurableObjectFetch>(payload)?)
                    .await
                    .map_err(host_error)?,
            ),
            "cache_match" => to_json(
                self.cache()?
                    .match_entry(parse_payload::<CacheGet>(payload)?)
                    .await
                    .map_err(host_error)?,
            ),
            "cache_put" => {
                self.cache()?
                    .put_entry(parse_payload::<CachePut>(payload)?)
                    .await
                    .map_err(host_error)?;
                Ok(serde_json::Value::Null)
            }
            "cache_delete" => to_json(
                self.cache()?
                    .delete_entry(parse_payload::<CacheGet>(payload)?)
                    .await
                    .map_err(host_error)?,
            ),
            "ai_run" => to_json(
                self.ai()?
                    .run(parse_payload::<AiRun>(payload)?)
                    .await
                    .map_err(host_error)?,
            ),
            "d1_query" => {
                let request: D1Request = parse_payload(payload)?;
                self.d1_query(request).await
            }
            "d1_exec" => {
                let request: D1Request = parse_payload(payload)?;
                to_json(
                    self.storage()?
                        .sql(SqlQuery {
                            database: Some(request.database),
                            sql: request.sql,
                            parameters: Vec::new(),
                        })
                        .await
                        .map_err(host_error)?,
                )
            }
            "workflow_id" => Ok(serde_json::Value::String(format!(
                "workflow-{}",
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
            ))),
            "workflow_status" => {
                let request: WorkflowStatusRequest = parse_payload(payload)?;
                let key = format!("{}/{}.json", request.binding, request.id);
                let bytes = self
                    .storage()?
                    .load("workflows", key.as_bytes())
                    .await
                    .map_err(host_error)?;
                Ok(bytes
                    .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                    .unwrap_or_else(
                        || serde_json::json!({ "id": request.id, "status": "unknown" }),
                    ))
            }
            "workflow_write" => {
                let request: WorkflowWriteRequest = parse_payload(payload)?;
                let state = serde_json::json!({
                    "id": request.id,
                    "status": request.status,
                    "reason": request.reason,
                    "updatedAt": chrono::Utc::now().timestamp_millis(),
                });
                let key = format!("{}/{}.json", request.binding, request.id);
                self.storage()?.begin().await.map_err(host_error)?;
                let mut committed = false;
                let result = async {
                    self.storage()?
                        .put(
                            "workflows",
                            key.as_bytes(),
                            &serde_json::to_vec(&state)
                                .map_err(|error| EngineError::Response(error.to_string()))?,
                        )
                        .await
                        .map_err(host_error)?;
                    self.committed_revision = self.storage()?.commit().await.map_err(host_error)?;
                    committed = true;
                    Ok::<_, EngineError>(state)
                }
                .await;
                if !committed {
                    let _ = self.storage()?.rollback().await;
                }
                result
            }
            _ => Err(EngineError::UnsupportedRuntimeFeature {
                runtime: "python",
                feature: "host operation",
            }),
        }
    }

    async fn d1_query(&self, request: D1Request) -> Result<serde_json::Value, EngineError> {
        let result = self
            .storage()?
            .sql(SqlQuery {
                database: Some(request.database),
                sql: request.sql,
                parameters: request
                    .parameters
                    .into_iter()
                    .map(sql_value)
                    .collect::<Result<Vec<_>, _>>()?,
            })
            .await
            .map_err(host_error)?;
        let rows = result
            .rows
            .iter()
            .map(|row| {
                result
                    .columns
                    .iter()
                    .zip(row)
                    .map(|(column, value)| Ok((column.clone(), sql_json(value)?)))
                    .collect::<Result<serde_json::Map<_, _>, EngineError>>()
                    .map(serde_json::Value::Object)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let raw = result
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(sql_json)
                    .collect::<Result<Vec<_>, EngineError>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(serde_json::json!({
            "success": true,
            "results": rows,
            "raw": raw,
            "meta": {
                "changes": result.changes,
                "last_row_id": result.last_insert_rowid,
            },
        }))
    }

    fn storage(&self) -> Result<Arc<dyn DurableStorageHost>, EngineError> {
        self.hosts
            .storage
            .clone()
            .ok_or(EngineError::UnsupportedRuntimeFeature {
                runtime: "python",
                feature: "durable storage",
            })
    }

    fn fetch(&self) -> Result<Arc<dyn OutboundFetchHost>, EngineError> {
        self.hosts
            .fetch
            .clone()
            .ok_or(EngineError::UnsupportedRuntimeFeature {
                runtime: "python",
                feature: "outbound fetch",
            })
    }

    fn queue(&self) -> Result<Arc<dyn QueueProducerHost>, EngineError> {
        self.hosts
            .queue
            .clone()
            .ok_or(EngineError::UnsupportedRuntimeFeature {
                runtime: "python",
                feature: "queue producer",
            })
    }

    fn r2(&self) -> Result<Arc<dyn R2BucketHost>, EngineError> {
        self.hosts
            .r2
            .clone()
            .ok_or(EngineError::UnsupportedRuntimeFeature {
                runtime: "python",
                feature: "R2",
            })
    }

    fn service(&self) -> Result<Arc<dyn ServiceBindingHost>, EngineError> {
        self.hosts
            .service
            .clone()
            .ok_or(EngineError::UnsupportedRuntimeFeature {
                runtime: "python",
                feature: "service binding",
            })
    }

    fn durable(&self) -> Result<Arc<dyn DurableObjectHost>, EngineError> {
        self.hosts
            .durable
            .clone()
            .ok_or(EngineError::UnsupportedRuntimeFeature {
                runtime: "python",
                feature: "Durable Object binding",
            })
    }

    fn cache(&self) -> Result<Arc<dyn CacheHost>, EngineError> {
        self.hosts
            .cache
            .clone()
            .ok_or(EngineError::UnsupportedRuntimeFeature {
                runtime: "python",
                feature: "cache",
            })
    }

    fn kv(&self) -> Result<Arc<dyn KvHost>, EngineError> {
        self.hosts
            .kv
            .clone()
            .ok_or(EngineError::UnsupportedRuntimeFeature {
                runtime: "python",
                feature: "KV",
            })
    }

    fn ai(&self) -> Result<Arc<dyn AiHost>, EngineError> {
        self.hosts
            .ai
            .clone()
            .ok_or(EngineError::UnsupportedRuntimeFeature {
                runtime: "python",
                feature: "AI",
            })
    }
}

#[derive(serde::Deserialize)]
struct PythonLog {
    level: String,
    message: String,
}

impl From<PythonLog> for WorkerLogEvent {
    fn from(value: PythonLog) -> Self {
        let level = match value.level.as_str() {
            "debug" => WorkerLogLevel::Debug,
            "warn" | "warning" => WorkerLogLevel::Warn,
            "error" => WorkerLogLevel::Error,
            _ => WorkerLogLevel::Info,
        };
        Self {
            level,
            message: value.message,
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
        }
    }
}

struct ChildGuard {
    child: Option<Child>,
}

impl ChildGuard {
    const fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn kill(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
        }
    }

    fn wait(&mut self) -> std::io::Result<ExitStatus> {
        let mut child = self
            .child
            .take()
            .expect("python child guard cannot be waited twice");
        child.wait()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn deadline(timeout: Duration) -> Option<Instant> {
    (timeout != Duration::MAX)
        .then(|| Instant::now().checked_add(timeout))
        .flatten()
}

fn recv_python_line(
    rx: &mpsc::Receiver<Result<Option<String>, String>>,
    deadline: Option<Instant>,
    child: &mut ChildGuard,
) -> Result<Option<String>, EngineError> {
    let message = if let Some(deadline) = deadline {
        let now = Instant::now();
        if now >= deadline {
            child.kill();
            return Err(EngineError::Python(
                "python invocation exceeded its execution deadline".into(),
            ));
        }
        rx.recv_timeout(deadline.saturating_duration_since(now))
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => {
                    child.kill();
                    EngineError::Python("python invocation exceeded its execution deadline".into())
                }
                mpsc::RecvTimeoutError::Disconnected => {
                    EngineError::Python("python stdout reader disconnected".into())
                }
            })?
    } else {
        rx.recv()
            .map_err(|_| EngineError::Python("python stdout reader disconnected".into()))?
    };
    message.map_err(EngineError::Python)
}

fn unsafe_python_compat_enabled(environment: &WorkerEnvironment) -> bool {
    if cfg!(test) {
        return true;
    }
    if environment
        .values()
        .get("PEREN_UNSAFE_PYTHON_COMPAT")
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
    {
        return true;
    }
    std::env::var("PEREN_UNSAFE_PYTHON_COMPAT")
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

fn block_on_host<F>(future: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("host-call runtime should build")
                    .block_on(future)
            })
            .join()
            .expect("host-call runtime thread should not panic")
    })
}

fn parse_payload<T: serde::de::DeserializeOwned>(
    payload: serde_json::Value,
) -> Result<T, EngineError> {
    serde_json::from_value(payload).map_err(|error| EngineError::Request(error.to_string()))
}

fn to_json<T: serde::Serialize>(value: T) -> Result<serde_json::Value, EngineError> {
    serde_json::to_value(value).map_err(|error| EngineError::Response(error.to_string()))
}

fn host_error(error: HostError) -> EngineError {
    EngineError::Python(error.to_string())
}

fn is_native(provider: &serde_json::Value) -> bool {
    provider
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|kind| kind == "native")
}

fn sql_value(value: serde_json::Value) -> Result<SqlValue, EngineError> {
    match value {
        serde_json::Value::Null => Ok(SqlValue::Null),
        serde_json::Value::Bool(value) => Ok(SqlValue::Integer(i64::from(value))),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(SqlValue::Integer(value))
            } else if let Some(value) = value.as_f64() {
                Ok(SqlValue::Real(value))
            } else {
                Err(EngineError::Request("unsupported numeric SQL value".into()))
            }
        }
        serde_json::Value::String(value) => Ok(SqlValue::Text(value)),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|value| u8::try_from(value).ok())
                    .ok_or_else(|| EngineError::Request("SQL blob values must be bytes".into()))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(SqlValue::Blob),
        serde_json::Value::Object(_) => Err(EngineError::Request(
            "object SQL parameters are not supported".into(),
        )),
    }
}

fn sql_json(value: &SqlValue) -> Result<serde_json::Value, EngineError> {
    match value {
        SqlValue::Null => Ok(serde_json::Value::Null),
        SqlValue::Integer(value) => Ok(serde_json::Value::Number((*value).into())),
        SqlValue::Real(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| EngineError::Response("SQL real value is not finite".into())),
        SqlValue::Text(value) => Ok(serde_json::Value::String(value.clone())),
        SqlValue::Blob(value) => to_json(value),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex};

    use async_trait::async_trait;

    use crate::{
        HostError, ListEntry, ListOptions, ListPage, Module, ModuleKind, ModuleName,
        QueueDispositionKind, QueueMessage, QueueSend, R2Delete, R2Get, R2List, R2ListPage,
        R2Object, R2ObjectEntry, R2Put, ServiceFetch, SqlResult,
    };

    use super::*;

    fn bundle(source: &str) -> WorkerBundle {
        let entry = ModuleName::parse("worker.py").unwrap();
        WorkerBundle::new(
            entry.clone(),
            BTreeMap::from([(
                entry,
                Module::new(ModuleKind::Python, source.as_bytes()).unwrap(),
            )]),
        )
        .unwrap()
    }

    fn bundle_with_module(entry_source: &str, name: &str, module_source: &str) -> WorkerBundle {
        let entry = ModuleName::parse("worker.py").unwrap();
        let module = ModuleName::parse(name).unwrap();
        WorkerBundle::new(
            entry.clone(),
            BTreeMap::from([
                (
                    entry,
                    Module::new(ModuleKind::Python, entry_source.as_bytes()).unwrap(),
                ),
                (
                    module,
                    Module::new(ModuleKind::Python, module_source.as_bytes()).unwrap(),
                ),
            ]),
        )
        .unwrap()
    }

    fn limits() -> IsolateLimits {
        IsolateLimits::new(128 * 1024 * 1024, std::time::Duration::from_secs(30))
    }

    fn invocation_limits() -> InvocationLimits {
        InvocationLimits::new(32 * 1024 * 1024, 10_000)
    }

    #[tokio::test]
    async fn python_fetch_returns_http_response() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
async def fetch(request, env, ctx):
    body = await request.text()
    print("handled " + body)
    return Response("hello " + body, status=203, headers={"x-runtime": "python"})
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "POST".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: b"peren".to_vec(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.status, 203);
        assert_eq!(response.body, b"hello peren");
        assert_eq!(runtime.take_console_events()[0].message, "handled peren");
    }

    #[tokio::test]
    async fn python_fetch_receives_environment_values() {
        let mut env = BTreeMap::new();
        env.insert("GREETING".to_string(), "hello".to_string());
        let mut runtime = PythonRuntime::load(
            bundle(
                r"
def fetch(request, env, ctx):
    return Response(env.GREETING)
",
            ),
            limits(),
            WorkerEnvironment::new(env),
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.body, b"hello");
    }

    #[tokio::test]
    async fn python_fetch_supports_workers_entrypoint_class() {
        let mut env = BTreeMap::new();
        env.insert("GREETING".to_string(), "hello".to_string());
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        name = (await request.json())["name"]
        return Response(f"{self.env.GREETING} {name}", status=202)
"#,
            ),
            limits(),
            WorkerEnvironment::new(env),
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "POST".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: br#"{"name":"Python"}"#.to_vec(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.status, 202);
        assert_eq!(response.body, b"hello Python");
    }

    #[tokio::test]
    async fn python_fetch_uses_bound_durable_class() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        return Response("default")

class Counter(WorkerEntrypoint):
    async def fetch(self, request):
        return Response("counter")
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();
        runtime.bind_durable_class("Counter").unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.body, b"counter");
    }

    #[tokio::test]
    async fn python_fetch_imports_bundled_modules() {
        let mut runtime = PythonRuntime::load(
            bundle_with_module(
                r#"
from hello import hello
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    def fetch(self, request):
        return Response(hello("World"))
"#,
                "hello.py",
                r#"
def hello(name):
    return "Hello, " + name + "!"
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.body, b"Hello, World!");
    }

    #[tokio::test]
    async fn python_fetch_supports_pyodide_style_runtime_imports() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from js import Response
from pyodide.ffi import to_js

def fetch(request, env, ctx):
    return Response.json(to_js({"ok": True, "runtime": "python"}))
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
            serde_json::json!({ "ok": true, "runtime": "python" })
        );
    }

    #[tokio::test]
    async fn python_fetch_preserves_user_type_errors() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    def fetch(self, request):
        raise TypeError("application bug")
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let error = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap_err();

        assert!(error.to_string().contains("application bug"), "{error}");
    }

    #[tokio::test]
    async fn python_blocks_workers_incompatible_modules() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    def fetch(self, request):
        try:
            import subprocess
        except ModuleNotFoundError:
            return Response("blocked")
        return Response("available", status=500)
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"blocked");
    }

    #[tokio::test]
    async fn python_uses_ephemeral_invocation_filesystem() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    def fetch(self, request):
        with open("scratch.txt", "w") as file:
            file.write("hello")
        with open("scratch.txt") as file:
            return Response(file.read())
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.body, b"hello");
    }

    #[tokio::test]
    async fn python_refuses_filesystem_escape() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    def fetch(self, request):
        try:
            open("/tmp/peren-python-escape.txt", "w").write("bad")
        except PermissionError:
            return Response("blocked")
        return Response("escaped", status=500)
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.status, 200);
        assert_eq!(response.body, b"blocked");
    }

    #[tokio::test]
    async fn python_scrubs_host_environment() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    def fetch(self, request):
        import os
        return Response.json({"keys": sorted(os.environ.keys())})
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        let body = serde_json::from_slice::<serde_json::Value>(&response.body).unwrap();
        assert_eq!(body, serde_json::json!({ "keys": [] }));
    }

    #[tokio::test]
    async fn python_invocation_timeout_interrupts_runaway_code() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    def fetch(self, request):
        while True:
            pass
        return Response("unreachable")
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let error = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                InvocationLimits::new(1024, 10).with_cpu_time(std::time::Duration::from_millis(50)),
            )
            .unwrap_err();

        assert!(
            error.to_string().contains("execution deadline")
                || error.to_string().contains("TimeoutError"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn python_fetch_uses_native_kv_binding_through_host_bridge() {
        let mut env = BTreeMap::new();
        env.insert(
            "__perenBindings".to_string(),
            r#"{"CACHE":{"type":"kv","scope":"cache","provider":{"kind":"native"}}}"#.to_string(),
        );
        let storage = Arc::new(MemoryStorage::default());
        let mut runtime = PythonRuntime::load_with_storage(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        await self.env.CACHE.put("alpha", "one")
        first = await self.env.CACHE.get("alpha")
        await self.env.CACHE.delete("alpha")
        second = await self.env.CACHE.get("alpha")
        return Response(f"{first}:{second}")
"#,
            ),
            limits(),
            WorkerEnvironment::new(env),
            storage,
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.body, b"one:None");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn python_fetch_sends_queue_messages_through_host_bridge() {
        let mut env = BTreeMap::new();
        env.insert(
            "__perenBindings".to_string(),
            r#"{"JOBS":{"type":"queue","queue":"jobs","provider":{"kind":"memory"}}}"#.to_string(),
        );
        let queue = Arc::new(RecordingQueue::default());
        let mut runtime = PythonRuntime::load_with_capabilities(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        await self.env.JOBS.send("one", {"contentType": "text/plain", "delaySeconds": 3})
        await self.env.JOBS.sendBatch([{"body": {"id": 2}}, {"body": b"\x03"}])
        return Response("queued")
"#,
            ),
            limits(),
            WorkerEnvironment::new(env),
            Capabilities {
                storage: Arc::new(MemoryStorage::default()),
                fetch: None,
                queue: Some(queue.clone()),
                r2: None,
                service: None,
                durable: None,
                cache: None,
                kv: None,
                ai: None,
            },
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.body, b"queued");
        let messages = queue.messages.lock().unwrap().clone();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].queue, "jobs");
        assert_eq!(messages[0].body, b"one");
        assert_eq!(messages[0].content_type.as_deref(), Some("text/plain"));
        assert_eq!(messages[0].delay_seconds, Some(3));
        assert_eq!(messages[1].body, br#"{"id": 2}"#);
        assert_eq!(messages[2].body, vec![3]);
    }

    #[tokio::test]
    async fn python_queue_handlers_return_message_dispositions() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint

class Default(WorkerEntrypoint):
    async def queue(self, batch):
        batch.messages[0].ack()
        batch.messages[1].retry({"delaySeconds": 5, "dedupId": "retry-two"})
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let dispatch = runtime
            .dispatch_queue(QueueEvent {
                queue: "jobs".into(),
                messages: vec![
                    QueueMessage {
                        id: "one".into(),
                        body: b"one".to_vec(),
                        attempts: 1,
                        timestamp: 10,
                    },
                    QueueMessage {
                        id: "two".into(),
                        body: b"two".to_vec(),
                        attempts: 2,
                        timestamp: 11,
                    },
                ],
                metrics: crate::QueueMetrics::default(),
            })
            .unwrap();

        assert_eq!(dispatch.dispositions.len(), 2);
        assert_eq!(dispatch.dispositions[0].id, "one");
        assert_eq!(dispatch.dispositions[0].outcome, QueueDispositionKind::Ack);
        assert_eq!(dispatch.dispositions[1].id, "two");
        assert_eq!(
            dispatch.dispositions[1].outcome,
            QueueDispositionKind::Retry
        );
        assert_eq!(dispatch.dispositions[1].delay_seconds, Some(5));
        assert_eq!(
            dispatch.dispositions[1].dedup_id.as_deref(),
            Some("retry-two")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn python_fetch_uses_r2_binding_through_host_bridge() {
        let mut env = BTreeMap::new();
        env.insert(
            "__perenBindings".to_string(),
            r#"{"BUCKET":{"type":"r2","bucket":"files","prefix":"app/","provider":{"kind":"memory"}}}"#
                .to_string(),
        );
        let r2 = Arc::new(MemoryR2::default());
        let mut runtime = PythonRuntime::load_with_r2(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        await self.env.BUCKET.put("hello.txt", "hello")
        obj = await self.env.BUCKET.get("hello.txt")
        page = await self.env.BUCKET.list()
        return Response((await obj.text()) + ":" + page["objects"][0]["key"])
"#,
            ),
            limits(),
            WorkerEnvironment::new(env),
            Arc::new(MemoryStorage::default()),
            r2,
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.body, b"hello:app/hello.txt");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn python_fetch_uses_service_binding_through_host_bridge() {
        let mut env = BTreeMap::new();
        env.insert(
            "__perenBindings".to_string(),
            r#"{"AUTH":{"type":"service","service":"auth"}}"#.to_string(),
        );
        let service = Arc::new(RecordingService::default());
        let mut runtime = PythonRuntime::load_with_capabilities(
            bundle(
                r#"
from workers import WorkerEntrypoint, Response

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        response = await self.env.AUTH.fetch("https://auth.internal/session", {"method": "POST", "body": "check"})
        await Response.json({"ok": True}).json()
        return Response(await response.text(), status=response.status)
"#,
            ),
            limits(),
            WorkerEnvironment::new(env),
            Capabilities {
                storage: Arc::new(MemoryStorage::default()),
                fetch: None,
                queue: None,
                r2: None,
                service: Some(service.clone()),
                durable: None,
                cache: None,
                kv: None,
                ai: None,
            },
        )
        .unwrap();

        let response = runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "http://worker.invalid/".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                invocation_limits(),
            )
            .unwrap();

        assert_eq!(response.status, 209);
        assert_eq!(response.body, b"service:auth:check");
        let calls = service.calls.lock().unwrap();
        assert_eq!(calls[0].service, "auth");
        assert_eq!(calls[0].request.url, "https://auth.internal/session");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn python_dispatches_workflow_and_activity_handlers() {
        let storage = Arc::new(MemoryStorage::default());
        let mut runtime = PythonRuntime::load_with_storage(
            bundle(
                r#"
from workers import WorkerEntrypoint

seen = []

class Default(WorkerEntrypoint):
    def workflow(self, event):
        seen.append(event.instance + ":" + event.payload["kind"])
        print("workflow " + seen[-1])

    def activity(self, event):
        return {"instance": event.instance, "name": event.name, "task": event.task, "payload": event.payload}
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
            storage,
        )
        .unwrap();

        runtime
            .dispatch_workflow(WorkflowEvent {
                instance: "order-1".into(),
                payload: serde_json::json!({ "kind": "created" }),
            })
            .unwrap();

        let result = runtime
            .dispatch_workflow_activity(WorkflowActivityEvent {
                instance: "order-1".into(),
                name: "charge".into(),
                task: "task-1".into(),
                payload: serde_json::json!({ "amount": 42 }),
            })
            .unwrap();

        assert_eq!(
            result,
            serde_json::json!({
                "instance": "order-1",
                "name": "charge",
                "task": "task-1",
                "payload": { "amount": 42 },
            })
        );
        assert_eq!(
            runtime.take_console_events()[0].message,
            "workflow order-1:created"
        );
    }

    #[tokio::test]
    async fn python_dispatches_websocket_message_handlers() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint

class Default(WorkerEntrypoint):
    def webSocketMessage(self, socket, message):
        socket.send("echo:" + message + ":" + socket.id)
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let dispatch = runtime
            .dispatch_websocket_message(WebSocketMessageEvent {
                id: "session-1".into(),
                message: "hello".into(),
            })
            .unwrap();

        assert_eq!(dispatch.outbound, vec!["echo:hello:session-1"]);
    }

    #[tokio::test]
    async fn python_dispatches_websocket_close_handlers() {
        let mut runtime = PythonRuntime::load(
            bundle(
                r#"
from workers import WorkerEntrypoint

class Default(WorkerEntrypoint):
    def webSocketClose(self, socket, event):
        socket.send(f"closed:{event.code}:{event.reason}:{event.wasClean}")
"#,
            ),
            limits(),
            WorkerEnvironment::empty(),
        )
        .unwrap();

        let dispatch = runtime
            .dispatch_websocket_close(WebSocketCloseEvent {
                id: "session-1".into(),
                code: 1001,
                reason: "going away".into(),
                was_clean: true,
            })
            .unwrap();

        assert_eq!(dispatch.outbound, vec!["closed:1001:going away:True"]);
    }

    #[derive(Default)]
    struct RecordingQueue {
        messages: Mutex<Vec<QueueSend>>,
    }

    #[async_trait]
    impl QueueProducerHost for RecordingQueue {
        async fn send(&self, message: QueueSend) -> Result<(), HostError> {
            self.messages.lock().unwrap().push(message);
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingService {
        calls: Mutex<Vec<ServiceFetch>>,
    }

    #[async_trait]
    impl ServiceBindingHost for RecordingService {
        async fn fetch(&self, request: ServiceFetch) -> Result<HttpResponse, HostError> {
            self.calls.lock().unwrap().push(request.clone());
            Ok(HttpResponse {
                status: 209,
                headers: Vec::new(),
                body: format!(
                    "service:{}:{}",
                    request.service,
                    String::from_utf8_lossy(&request.request.body)
                )
                .into_bytes(),
                upgrade: false,
                websocket_id: None,
            })
        }
    }

    #[derive(Default)]
    struct MemoryR2 {
        objects: Mutex<BTreeMap<(String, String), R2Object>>,
    }

    #[async_trait]
    impl R2BucketHost for MemoryR2 {
        async fn put(&self, object: R2Put) -> Result<(), HostError> {
            let size = object.body.len();
            self.objects.lock().unwrap().insert(
                (object.bucket.clone(), object.key.clone()),
                R2Object {
                    key: object.key,
                    size,
                    body: object.body,
                    content_type: object.content_type,
                    custom_metadata: object.custom_metadata,
                },
            );
            Ok(())
        }

        async fn get(&self, object: R2Get) -> Result<Option<R2Object>, HostError> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(&(object.bucket, object.key))
                .cloned())
        }

        async fn delete(&self, object: R2Delete) -> Result<(), HostError> {
            self.objects
                .lock()
                .unwrap()
                .remove(&(object.bucket, object.key));
            Ok(())
        }

        async fn list(&self, request: R2List) -> Result<R2ListPage, HostError> {
            let prefix = request.prefix.unwrap_or_default();
            let objects = self
                .objects
                .lock()
                .unwrap()
                .iter()
                .filter(|((bucket, key), _)| bucket == &request.bucket && key.starts_with(&prefix))
                .map(|(_, object)| R2ObjectEntry {
                    key: object.key.clone(),
                    size: object.size,
                    custom_metadata: object.custom_metadata.clone(),
                })
                .collect();
            Ok(R2ListPage {
                objects,
                cursor: None,
                list_complete: true,
            })
        }
    }

    type StorageMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

    #[derive(Default)]
    struct MemoryStorage {
        values: Mutex<StorageMap>,
        revision: Mutex<u64>,
    }

    #[async_trait]
    impl DurableStorageHost for MemoryStorage {
        async fn begin(&self) -> Result<(), HostError> {
            Ok(())
        }

        async fn load(&self, scope: &str, key: &[u8]) -> Result<Option<Vec<u8>>, HostError> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .get(&(scope.to_string(), key.to_vec()))
                .cloned())
        }

        async fn put(&self, scope: &str, key: &[u8], value: &[u8]) -> Result<(), HostError> {
            self.values
                .lock()
                .unwrap()
                .insert((scope.to_string(), key.to_vec()), value.to_vec());
            Ok(())
        }

        async fn delete(&self, scope: &str, key: &[u8]) -> Result<bool, HostError> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .remove(&(scope.to_string(), key.to_vec()))
                .is_some())
        }

        async fn list(&self, scope: &str, options: ListOptions) -> Result<ListPage, HostError> {
            let prefix = options.prefix.unwrap_or_default();
            let keys = self
                .values
                .lock()
                .unwrap()
                .keys()
                .filter(|(candidate_scope, key)| {
                    candidate_scope == scope && key.starts_with(&prefix)
                })
                .map(|(_, key)| ListEntry {
                    name: String::from_utf8_lossy(key).into_owned(),
                })
                .collect();
            Ok(ListPage {
                keys,
                cursor: None,
                list_complete: true,
            })
        }

        async fn sql(&self, _query: SqlQuery) -> Result<SqlResult, HostError> {
            Err(HostError)
        }

        async fn commit(&self) -> Result<peren_primitives::StorageRevision, HostError> {
            let mut revision = self.revision.lock().unwrap();
            *revision += 1;
            Ok(peren_primitives::StorageRevision::new(*revision))
        }

        async fn rollback(&self) -> Result<(), HostError> {
            Ok(())
        }
    }
}
