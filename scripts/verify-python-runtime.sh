#!/usr/bin/env bash
set -euo pipefail

PYODIDE_VERSION="${PYODIDE_VERSION:-0.28.3}"
ROOT="${PEREN_PYODIDE_ROOT:-}"
TEMP_DIR=""

if [[ -z "${ROOT}" ]]; then
  TEMP_DIR="$(mktemp -d)"
  trap 'rm -rf "${TEMP_DIR}"' EXIT
  npm pack "pyodide@${PYODIDE_VERSION}" --pack-destination "${TEMP_DIR}" >/dev/null
  tar -xf "${TEMP_DIR}/pyodide-${PYODIDE_VERSION}.tgz" -C "${TEMP_DIR}"
  ROOT="${TEMP_DIR}/package"
fi

PEREN_PYODIDE_ROOT="${ROOT}" cargo test -p peren-runtime pyodide_executor_ -- --ignored --nocapture
