use super::support::*;

fn websocket_upgrade_headers() -> Vec<(String, String)> {
    vec![
        ("connection".into(), "keep-alive, Upgrade".into()),
        ("upgrade".into(), "h2c, websocket".into()),
    ]
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_response_requires_upgrade_request() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              fetch() {
                const pair = new WebSocketPair();
                const response = new Response(null, { status: 101 });
                response.webSocket = pair[1];
                return response;
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    assert!(matches!(
        runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "https://worker.invalid/plain".into(),
                    headers: Vec::new(),
                    body: Vec::new(),
                    mtls: None,
                },
                InvocationLimits::new(4096, 10),
            )
            .await,
        Err(EngineError::JavaScript(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_response_requires_connection_upgrade_token() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              fetch() {
                const pair = new WebSocketPair();
                const response = new Response(null, { status: 101 });
                response.webSocket = pair[1];
                return response;
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    assert!(matches!(
        runtime
            .dispatch_http(
                HttpRequest {
                    method: "GET".into(),
                    url: "https://worker.invalid/plain".into(),
                    headers: vec![("upgrade".into(), "websocket".into())],
                    body: Vec::new(),
                    mtls: None,
                },
                InvocationLimits::new(4096, 10),
            )
            .await,
        Err(EngineError::JavaScript(_))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_response_accepts_comma_separated_upgrade_tokens() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              fetch() {
                const pair = new WebSocketPair();
                const response = new Response(null, { status: 101 });
                response.webSocket = pair[1];
                return response;
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/ws".into(),
                headers: websocket_upgrade_headers(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert!(response.upgrade);
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_attachment_methods_persist_through_storage() {
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async fetch() {
                const pair = new WebSocketPair();
                const socket = pair[1];
                await socket.serializeAttachment({ tenant: 'acme', count: 2 });
                const first = await socket.deserializeAttachment();
                await socket.deleteAttachment();
                const second = await socket.deserializeAttachment();
                return Response.json({ first, missing: second === undefined });
              }
            };",
        ),
        limits(),
        Arc::new(SqlHost::new()),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/websocket-attachment".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "first": { "tenant": "acme", "count": 2 },
            "missing": true,
        })
    );
    assert!(runtime.committed_revision() >= StorageRevision::new(2));
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_close_orders_pending_messages_and_cleans_registry() {
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async fetch() {
                const pair = new WebSocketPair();
                const client = pair[0];
                const server = pair[1];
                const events = [];
                client.onmessage = (event) => events.push(`message:${event.data}`);
                client.onclose = (event) => events.push(`close:${event.code}:${event.reason}:${event.wasClean}`);
                client.accept();
                server.accept();
                server.send('before-close');
                server.close(1001, 'done');
                let sendAfterClose = 'accepted';
                try { server.send('after-close'); } catch (error) { sendAfterClose = error.message; }
                await Promise.resolve();
                await Promise.resolve();
                return Response.json({
                  events,
                  sendAfterClose,
                  serverState: server.readyState,
                  clientState: client.readyState,
                  session: globalThis.__perenWebSocketAttachmentId(server),
                });
              }
            };",
        ),
        limits(),
        Arc::new(SqlHost::new()),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/ws-close".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "events": ["message:before-close", "close:1001:done:true"],
            "sendAfterClose": "WebSocket is not open",
            "serverState": 3,
            "clientState": 3,
            "session": "socket-2",
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn durable_object_state_rehydrates_accepted_websockets_from_storage() {
    let host = Arc::new(SqlHost::new());
    let mut first = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              fetch() {
                const pair = new WebSocketPair();
                const state = new DurableObjectState();
                state.acceptWebSocket(pair[1], ['room:alpha', 'tenant:acme']);
                const response = new Response(null);
                response.webSocket = pair[0];
                return response;
              }
            };",
        ),
        limits(),
        host.clone(),
    )
    .await
    .unwrap();

    let accepted = first
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/accept".into(),
                headers: websocket_upgrade_headers(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert!(accepted.upgrade);
    assert!(accepted.websocket_id.is_some());

    let mut second = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              fetch() {
                const state = new DurableObjectState();
                const sockets = state.getWebSockets('room:alpha');
                for (const socket of sockets) socket.send('rehydrated');
                return Response.json({ count: sockets.length });
              }
            };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    let response = second
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/list".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({ "count": 1 })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn durable_object_state_rehydrates_websocket_auto_response() {
    let host = Arc::new(SqlHost::new());
    let mut first = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              fetch() {
                const state = new DurableObjectState();
                state.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
                return Response.json({
                  response: state.getWebSocketAutoResponse().response,
                  timestamp: typeof state.getWebSocketAutoResponseTimestamp(),
                });
              }
            };",
        ),
        limits(),
        host.clone(),
    )
    .await
    .unwrap();

    let stored = first
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/auto-response".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stored.body).unwrap(),
        serde_json::json!({ "response": "pong", "timestamp": "number" })
    );

    let mut second = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              fetch() {
                const state = new DurableObjectState();
                const pair = state.getWebSocketAutoResponse();
                return Response.json({
                  request: pair.request,
                  response: pair.response,
                  timestamp: typeof state.getWebSocketAutoResponseTimestamp(),
                });
              }
            };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    let restored = second
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/auto-response".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&restored.body).unwrap(),
        serde_json::json!({ "request": "ping", "response": "pong", "timestamp": "number" })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_auto_response_answers_without_running_message_handler() {
    let host = Arc::new(SqlHost::new());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "globalThis.count = 0;
             export default {
               fetch() {
                 const state = new DurableObjectState();
                 state.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
                 return Response.json({ count: globalThis.count });
               },
               webSocketMessage(socket, message) {
                 globalThis.count += 1;
                 socket.send(`handled:${message}`);
               }
             };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/auto-response".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    let matched = runtime
        .dispatch_websocket_message(peren_runtime::WebSocketMessageEvent {
            id: "socket-auto-1".into(),
            message: "ping".into(),
        })
        .await
        .unwrap();
    assert_eq!(matched.outbound, vec!["pong"]);

    let unmatched = runtime
        .dispatch_websocket_message(peren_runtime::WebSocketMessageEvent {
            id: "socket-auto-1".into(),
            message: "work".into(),
        })
        .await
        .unwrap();
    assert_eq!(unmatched.outbound, vec!["handled:work"]);

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/count".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({ "count": 1 })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_close_removes_hibernation_record_from_storage() {
    let host = Arc::new(SqlHost::new());
    let mut first = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              fetch() {
                const pair = new WebSocketPair();
                const state = new DurableObjectState();
                state.acceptWebSocket(pair[1], ['room:closed']);
                const response = new Response(null);
                response.webSocket = pair[0];
                return response;
              },
              webSocketClose(socket) {
                socket.close();
              }
            };",
        ),
        limits(),
        host.clone(),
    )
    .await
    .unwrap();

    let accepted = first
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/accept".into(),
                headers: websocket_upgrade_headers(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();
    let id = accepted.websocket_id.unwrap();

    first
        .dispatch_websocket_close(peren_runtime::WebSocketCloseEvent {
            id,
            code: 1000,
            reason: String::new(),
            was_clean: true,
        })
        .await
        .unwrap();

    let mut second = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              fetch() {
                const state = new DurableObjectState();
                return Response.json({ count: state.getWebSockets('room:closed').length });
              }
            };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    let response = second
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/list".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({ "count": 0 })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_upgrade_response_carries_attachment_identity() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              fetch() {
                const pair = new WebSocketPair();
                pair[1].accept();
                const response = new Response(null);
                response.webSocket = pair[0];
                return response;
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/websocket-upgrade".into(),
                headers: websocket_upgrade_headers(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(1024, 10),
        )
        .await
        .unwrap();

    assert!(response.upgrade);
    assert!(
        response
            .websocket_id
            .as_deref()
            .is_some_and(|value| value.starts_with("socket-")),
        "{:?}",
        response.websocket_id
    );
}

#[tokio::test(flavor = "current_thread")]
async fn websocket_pair_delivers_messages_and_close_events() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              async fetch() {
                const pair = new WebSocketPair();
                const client = pair[0];
                const server = pair[1];
                const events = [];
                server.addEventListener('message', (event) => events.push(`server:${event.data}`));
                client.onmessage = (event) => events.push(`client:${event.data}`);
                client.onclose = (event) => events.push(`client-close:${event.code}:${event.reason}:${event.wasClean}`);
                server.onclose = (event) => events.push(`server-close:${event.code}:${event.reason}:${event.wasClean}`);
                server.accept();
                server.send('queued-before-accept');
                client.accept();
                client.send('hello');
                await Promise.resolve();
                server.send('world');
                await Promise.resolve();
                client.close(1000, 'done');
                await Promise.resolve();
                return new Response(JSON.stringify({
                  states: [client.readyState, server.readyState],
                  events,
                  constants: [WebSocket.OPEN, WebSocket.CLOSED],
                }));
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/websocket".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&response.body).unwrap(),
        serde_json::json!({
            "states": [3, 3],
            "events": [
                "client:queued-before-accept",
                "server:hello",
                "client:world",
                "client-close:1000:done:true",
                "server-close:1000:done:true"
            ],
            "constants": [1, 3],
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn host_websocket_message_dispatch_rehydrates_socket_and_collects_outbound_frames() {
    let host = Arc::new(SqlHost::new());
    let mut runtime = WorkerRuntime::load_with_host(
        bundle(
            "export default {
              async fetch() { return new Response('ok'); },
              async webSocketMessage(socket, message) {
                await socket.serializeAttachment({ seen: message });
                socket.send(`echo:${message}`);
              }
            };",
        ),
        limits(),
        host,
    )
    .await
    .unwrap();

    let dispatch = runtime
        .dispatch_websocket_message(peren_runtime::WebSocketMessageEvent {
            id: "socket-host-1".into(),
            message: "hello".into(),
        })
        .await
        .unwrap();

    assert_eq!(dispatch.outbound, vec!["echo:hello"]);

    let response = runtime
        .dispatch_http(
            HttpRequest {
                method: "GET".into(),
                url: "https://worker.invalid/attachment".into(),
                headers: Vec::new(),
                body: Vec::new(),
                mtls: None,
            },
            InvocationLimits::new(4096, 10),
        )
        .await
        .unwrap();

    assert_eq!(response.status, 200);
}

#[tokio::test(flavor = "current_thread")]
async fn host_websocket_close_dispatch_runs_handler_and_closes_rehydrated_socket() {
    let mut runtime = WorkerRuntime::load(
        bundle(
            "export default {
              async fetch() { return new Response('ok'); },
              async webSocketClose(socket, code, reason, wasClean) {
                socket.send(`${code}:${reason}:${wasClean}`);
              }
            };",
        ),
        limits(),
    )
    .await
    .unwrap();

    let dispatch = runtime
        .dispatch_websocket_close(peren_runtime::WebSocketCloseEvent {
            id: "socket-host-2".into(),
            code: 1001,
            reason: "client-left".into(),
            was_clean: true,
        })
        .await
        .unwrap();

    assert_eq!(dispatch.outbound, vec!["1001:client-left:true"]);
}
