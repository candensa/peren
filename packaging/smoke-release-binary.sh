#!/usr/bin/env bash
set -euo pipefail

peren="${1:-}"
smoke="${2:-${RUNNER_TEMP:-/tmp}/peren-smoke}"
server_pid=""

die() {
  echo "error: $*" >&2
  exit 1
}

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

[[ -n "$peren" ]] || die "usage: packaging/smoke-release-binary.sh /path/to/peren [work-dir]"
[[ -x "$peren" ]] || die "missing executable: $peren"

rm -rf "$smoke"
mkdir -p "$smoke"

cat >"$smoke/worker.js" <<'JS'
export default {
  fetch() {
    return new Response("hello from Peren\n");
  },
};
JS

cat >"$smoke/config.toml" <<'TOML'
[node]
node_id = "00000000-0000-0000-0000-000000000101"
advertise_addr = "127.0.0.1:7101"
listen = "127.0.0.1:7101"

[bucket]
kind = "memory"

[mtls]
ca_cert_path = "certs/ca.pem"
leaf_cert_path = "certs/leaf-cert.pem"
leaf_key_path = "certs/leaf-key.pem"

[[services]]
name = "hello"
worker_bundle_path = "worker.js"
compatibility_date = "2026-01-01"

[[sockets]]
name = "public"
listen = "127.0.0.1:8101"
service = "hello"
TOML

"$peren" test-server --json "$smoke/config.toml" >"$smoke/ready.json" 2>"$smoke/server.log" &
server_pid=$!

for _ in {1..60}; do
  if [[ -s "$smoke/ready.json" ]]; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$smoke/server.log" >&2
    die "test server exited before reporting readiness"
  fi
  sleep 1
done

[[ -s "$smoke/ready.json" ]] || {
  cat "$smoke/server.log" >&2
  die "test server did not report readiness"
}

public_url="$(python3 -c 'import json,sys; print("http://" + json.load(open(sys.argv[1]))["sockets"]["public"])' "$smoke/ready.json")"
if ! response="$(curl -fsSL "$public_url/")"; then
  cat "$smoke/server.log" >&2
  die "worker dispatch failed"
fi

[[ "$response" == "hello from Peren" ]] || die "unexpected response: $response"
