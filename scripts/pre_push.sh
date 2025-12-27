#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(git rev-parse --show-toplevel)"
cd "$ROOT_DIR"

run() {
  echo "[pre-push] $*"
  "$@"
}

run uv run ruff check --select I --fix python tests scripts || {
  echo "[pre-push] Ruff import sorting failed" >&2
  exit 1
}

run uv run ruff check python tests scripts || {
  echo "[pre-push] Ruff linting failed" >&2
  exit 1
}

run uv run ruff format python tests scripts || {
  echo "[pre-push] Ruff format failed" >&2
  exit 1
}

if ! git diff --quiet --exit-code; then
  cat <<'MSG'
[pre-push] Formatting introduced changes. Review/stage them and re-run push.
MSG
  exit 1
fi

run env UV_CACHE_DIR="${UV_CACHE_DIR:-.uv_cache}" uv run pytest

run env UV_CACHE_DIR="${UV_CACHE_DIR:-.uv_cache}" UV_TOOL_DIR="${UV_TOOL_DIR:-.uv_tools}" uvx ty check
