# Peren observability example

This package runs Prometheus and Grafana against a Peren node that exposes `/metrics`.

Start Peren on a network name that Prometheus can reach as `peren:8080`, or edit `prometheus/prometheus.yml` to point at the node address you use locally.

```sh
docker compose -f packaging/observability/compose.yml up
```

Grafana is available at <http://127.0.0.1:3000>. The bundled dashboard covers readiness, process uptime, admission lifecycle, HTTP throughput and latency, queue throughput and latency, durable storage commits, provider operations, service and Durable Object calls, outbound fetches, WebSocket sessions, and subsystem errors.

The `/metrics` endpoint is intentionally safe to expose to a local monitoring stack. It reports runtime counters, gauges, and histograms. It does not include secrets, provider credentials, provider endpoints, certificate material, or filesystem paths.

## Reverse proxy examples

The `caddy/Caddyfile` and `nginx/nginx.conf` examples show a small production-shaped front door for the packaged stack. They forward normal HTTP traffic and WebSocket upgrades to Peren on `peren:8080`, while keeping `/metrics` explicit so operators can place it on a private listener or add network restrictions before exposing it.

