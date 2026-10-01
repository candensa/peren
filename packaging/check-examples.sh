#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
peren="${PEREN:-$root/target/release/peren}"
log_dir="$root/target/example-check"
pid=""

usage() {
  cat <<EOF
usage: packaging/check-examples.sh [example ...]

Check one example, several, or every example under examples/.

  packaging/check-examples.sh
  packaging/check-examples.sh hello
  packaging/check-examples.sh examples/javascript/kv
  packaging/check-examples.sh examples/javascript/queue/kafka.toml

A directory uses its config.toml. That file is started and requested.
Other toml files in the same directory are validated and not started.
A toml path is validated and started itself.

PEREN overrides the binary. The default is target/release/peren.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

cleanup() {
  if [[ -n "$pid" ]]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    pid=""
  fi
}
trap cleanup EXIT

require_binary() {
  [[ -x "$peren" ]] || die "missing release binary: $peren"
}

prepare_certs() {
  "$peren" devcert "$root/certs" >/dev/null
}

resolve_target() {
  local arg="$1"
  local candidate

  if [[ -d "$arg" ]]; then
    [[ -f "$arg/config.toml" ]] || die "$arg has no config.toml"
    printf '%s\n' "$arg/config.toml"
    return
  fi
  if [[ -f "$arg" ]]; then
    printf '%s\n' "$arg"
    return
  fi

  for candidate in \
    "$root/examples/javascript/$arg" \
    "$root/examples/javascript/$arg/config.toml" \
    "$root/examples/$arg" \
    "$root/examples/$arg/config.toml" \
    "$root/$arg"
  do
    if [[ -d "$candidate" && -f "$candidate/config.toml" ]]; then
      printf '%s\n' "$candidate/config.toml"
      return
    fi
    if [[ -f "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return
    fi
  done

  die "unknown example: $arg"
}

selected_configs() {
  local arg
  if [[ $# -eq 0 ]]; then
    find "$root/examples" -name config.toml | sort
    return
  fi
  for arg in "$@"; do
    resolve_target "$arg"
  done
}

validate_config() {
  local config="$1"
  "$peren" config validate "$config" >/dev/null
  echo "valid  ${config#"$root"/}"
}

validate_related() {
  local config="$1"
  local dir file
  dir="$(dirname "$config")"
  if [[ "$(basename "$config")" == "config.toml" ]]; then
    while IFS= read -r file; do
      validate_config "$file"
    done < <(find "$dir" -maxdepth 1 -name '*.toml' | sort)
    return
  fi
  validate_config "$config"
}

field_value() {
  local line="$1"
  printf '%s\n' "$line" | sed -E 's/^[^=]*=[[:space:]]*"([^"]*)".*/\1/'
}

public_url() {
  local config="$1"
  local url
  url="$(
    awk '
      /^\[\[sockets\]\]/ { socket = 1; name = ""; address = ""; next }
      socket && /^\[/ {
        if (name == "public") public = address
        socket = 0
      }
      socket && /^name[[:space:]]*=/ { name = $3; gsub(/"/, "", name) }
      socket && /^listen[[:space:]]*=/ { address = $3; gsub(/"/, "", address) }
      END { if (name == "public") public = address; print public }
    ' "$config"
  )"
  [[ -n "$url" ]] || die "no public socket in $config"
  printf 'http://%s/\n' "$url"
}

prepare_bucket() {
  local config="$1"
  local path
  path="$(
    awk '
      /^\[bucket\]/ { bucket = 1; next }
      bucket && /^\[/ { bucket = 0 }
      bucket && /^kind[[:space:]]*=/ { kind = $3; gsub(/"/, "", kind) }
      bucket && /^path[[:space:]]*=/ { path = $3; gsub(/"/, "", path) }
      END { if (kind == "file" && path != "") print path }
    ' "$config"
  )"
  if [[ -n "$path" ]]; then
    mkdir -p "$root/$path"
  fi
}

export_if_unset() {
  local name="$1"
  local value="$2"
  if [[ -z "${!name:-}" ]]; then
    printf -v "$name" '%s' "$value"
    export "$name"
  fi
}

value_for() {
  local name="$1"
  case "$name" in
    *PEM*)
      if [[ "$name" == *KEY* ]]; then
        cat "$root/certs/leaf-key.pem"
      else
        cat "$root/certs/leaf-cert.pem"
      fi
      ;;
    *URL*) printf '%s\n' "https://example.com" ;;
    *) printf '%s\n' "example" ;;
  esac
}

prepare_env() {
  local config="$1"
  local section="" line name
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^\[ ]]; then
      section="$line"
      continue
    fi
    if [[ "$line" =~ _env[[:space:]]*= ]]; then
      name="$(printf '%s\n' "$line" | sed -E 's/.*_env[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/')"
      export_if_unset "$name" "$(value_for "$name")"
      continue
    fi
    if [[ "$section" == "[services.secrets]" || "$section" == "[secrets_store]" ]]; then
      [[ "$line" == *=* ]] || continue
      name="$(field_value "$line")"
      [[ "$name" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
      export_if_unset "$name" "$(value_for "$name")"
    fi
  done <"$config"
}

wait_for_http() {
  local url="$1"
  local config="$2"
  local attempt code
  for attempt in $(seq 1 80); do
    code="$(curl -sS --connect-timeout 1 --max-time 2 -o "$log_dir/body" -w '%{http_code}' "$url" 2>/dev/null || true)"
    if [[ "$code" =~ ^[1-5][0-9][0-9]$ ]]; then
      printf '%s\n' "$code"
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "peren exited before $url answered" >&2
      cat "$log_dir/serve.log" >&2 || true
      pid=""
      return 1
    fi
    sleep 0.25
  done
  echo "timed out waiting for $url (${config#"$root"/})" >&2
  cat "$log_dir/serve.log" >&2 || true
  return 1
}

run_config() {
  local config="$1"
  local url code label
  label="$(basename "$(dirname "$config")")"
  if [[ "$(basename "$config")" != "config.toml" ]]; then
    label="$label/$(basename "$config" .toml)"
  fi
  prepare_bucket "$config"
  prepare_env "$config"
  url="$(public_url "$config")"
  cleanup
  "$peren" serve "$config" >"$log_dir/serve.log" 2>&1 &
  pid=$!
  code="$(wait_for_http "$url" "$config")" || return 1
  cleanup
  echo "ran    $label  $url  $code"
}

check_config() {
  local config="$1"
  validate_related "$config"
  run_config "$config"
}

main() {
  local config
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi
  require_binary
  mkdir -p "$log_dir" 
  cd "$root"
  prepare_certs
  while IFS= read -r config; do
    check_config "$config"
  done < <(selected_configs "$@")
}

main "$@"
