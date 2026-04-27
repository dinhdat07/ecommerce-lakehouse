#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 {baseline|optimized}" >&2
  exit 1
fi

MODE="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_DIR="$REPO_ROOT/infra/server/env"
TARGET="$ENV_DIR/server.env"

case "$MODE" in
  baseline|optimized)
    SOURCE="$ENV_DIR/server.env.$MODE"
    ;;
  *)
    echo "Invalid mode: $MODE" >&2
    echo "Usage: $0 {baseline|optimized}" >&2
    exit 1
    ;;
esac

if [ ! -f "$SOURCE" ]; then
  echo "Missing config profile: $SOURCE" >&2
  exit 1
fi

cp "$SOURCE" "$TARGET"
echo "Active server config: $MODE ($TARGET)"
