#!/usr/bin/env sh
set -eu

repo=${PEREN_REPO:-candensa/peren}
version=${PEREN_VERSION:-latest}
prefix=${PREFIX:-/usr/local}
bindir=${BINDIR:-$prefix/bin}
binary=${PEREN_BINARY:-}
base_url=${PEREN_DOWNLOAD_BASE:-https://github.com/$repo/releases/download}
quiet=0

usage() {
  cat <<'USAGE'
Install Peren.

Usage:
  curl -fsSL https://peren.dev/install | sh

Options:
  --version VERSION   Release tag to install. Defaults to latest.
  --prefix PATH      Install prefix. Defaults to /usr/local.
  --bindir PATH      Binary directory. Defaults to PREFIX/bin.
  --binary PATH      Install an existing local binary instead of downloading.
  --quiet            Print only errors.
  -h, --help         Show this help.

Environment:
  PEREN_VERSION, PREFIX, BINDIR, PEREN_BINARY, PEREN_REPO, PEREN_DOWNLOAD_BASE
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version) version=${2:?missing version}; shift 2 ;;
    --prefix) prefix=${2:?missing prefix}; bindir=${BINDIR:-$prefix/bin}; shift 2 ;;
    --bindir) bindir=${2:?missing bindir}; shift 2 ;;
    --binary) binary=${2:?missing binary}; shift 2 ;;
    --quiet) quiet=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown option: %s
' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

say() {
  [ "$quiet" -eq 1 ] || printf '%s
' "$1"
}

fail() {
  printf 'peren install: %s
' "$1" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

machine=$(uname -m)
system=$(uname -s)
case "$system:$machine" in
  Linux:x86_64) target=x86_64-unknown-linux-gnu ;;
  Linux:aarch64|Linux:arm64) target=aarch64-unknown-linux-gnu ;;
  Darwin:x86_64) target=x86_64-apple-darwin ;;
  Darwin:arm64) target=aarch64-apple-darwin ;;
  *) fail "unsupported platform: $system $machine" ;;
esac

checksum_command() {
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' sha256sum
  elif command -v shasum >/dev/null 2>&1; then
    printf '%s' 'shasum -a 256'
  else
    fail 'missing required command: sha256sum or shasum'
  fi
}

verify_checksum() {
  file=$1
  checksums=$2
  name=${3:-$(basename "$file")}
  expected=$(awk -v name="$name" '$2 == name || $2 == "dist/" name { print $1; found = 1 } END { if (!found) exit 1 }' "$checksums")     || fail "checksum for $name not found"
  actual=$($(checksum_command) "$file" | awk '{ print $1 }')
  [ "$actual" = "$expected" ] || fail "checksum mismatch for $name"
}

install_from=$binary
tmp=
if [ -z "$install_from" ]; then
  need curl
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT HUP INT TERM
  asset=peren-$target
  if [ "$version" = latest ]; then
    download_url=https://github.com/$repo/releases/latest/download
  else
    download_url=$base_url/$version
  fi
  install_from=$tmp/peren
  checksums=$tmp/checksums.txt
  say "Downloading Peren from $download_url/$asset"
  curl -fsSL "$download_url/$asset" -o "$install_from"
  say "Downloading checksums from $download_url/checksums.txt"
  curl -fsSL "$download_url/checksums.txt" -o "$checksums"
  verify_checksum "$install_from" "$checksums" "$asset"
fi

[ -f "$install_from" ] || fail "binary not found: $install_from"
mkdir -p "$bindir"
cp "$install_from" "$bindir/peren"
chmod 0755 "$bindir/peren"

say "Peren installed at $bindir/peren"
say 'Run: peren --help'
