#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
WITH_PLATFORM="${WITH_PLATFORM:-0}"

if ! "${PYTHON_BIN}" -c "import venv" >/dev/null 2>&1; then
  echo "error: python3-venv is required. Install it first, for example:" >&2
  echo "  sudo apt install -y python3-venv python3-pip" >&2
  exit 1
fi

if ! "${PYTHON_BIN}" -m pip --version >/dev/null 2>&1; then
  echo "error: python3-pip is required. Install it first, for example:" >&2
  echo "  sudo apt install -y python3-venv python3-pip" >&2
  exit 1
fi

"${PYTHON_BIN}" -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -e . -r requirements-dev.txt

if [[ "${WITH_PLATFORM}" == "1" ]]; then
  .venv/bin/pip install -r requirements-platform.txt
fi
