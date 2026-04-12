#!/usr/bin/env bash
set -euo pipefail

if ROOT_DIR="$(git rev-parse --show-toplevel 2>/dev/null)"; then
  :
else
  ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
fi

cd "$ROOT_DIR"

UV_CACHE_DIR="${UV_CACHE_DIR:-.uv_cache}"
PORT="${PORT:-8000}"
PAGE="benchmark.html"
BUILD=1
SERVE=1
REFRESH_BENCHMARK=0
REPEATS=3
COOLDOWN_SECONDS=30

usage() {
  cat <<'EOF'
Usage: scripts/preview_docs.sh [options]

Build the docs site, optionally refresh benchmark results, and serve the
site locally so the benchmark page is easy to view in a browser.

Options:
  --refresh-benchmark        Re-run scripts/run_benchmark.py before building.
  --repeats N                Repeats to pass through with --refresh-benchmark.
  --cooldown-seconds N       Cooldown seconds to pass through with
                             --refresh-benchmark.
  --page PATH                Page to print/open after build. Default:
                             benchmark.html
  --port PORT                Local port for the preview server. Default: 8000
  --build-only               Build the site and print the URL without serving.
  --no-build                 Skip the build step and just serve the existing
                             site directory.
  -h, --help                 Show this help message.
EOF
}

run() {
  echo "[preview-docs] $*"
  "$@"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --refresh-benchmark)
      REFRESH_BENCHMARK=1
      shift
      ;;
    --repeats)
      REPEATS="$2"
      shift 2
      ;;
    --cooldown-seconds)
      COOLDOWN_SECONDS="$2"
      shift 2
      ;;
    --page)
      PAGE="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --build-only)
      SERVE=0
      shift
      ;;
    --no-build)
      BUILD=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[preview-docs] Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "$BUILD" -eq 1 ]]; then
  run env UV_CACHE_DIR="$UV_CACHE_DIR" uv sync --extra docs --extra benchmark
  if [[ "$REFRESH_BENCHMARK" -eq 1 ]]; then
    run env UV_CACHE_DIR="$UV_CACHE_DIR" uv run --extra benchmark \
      python scripts/run_benchmark.py \
      --repeats "$REPEATS" \
      --cooldown-seconds "$COOLDOWN_SECONDS"
  fi
  run env UV_CACHE_DIR="$UV_CACHE_DIR" uv run --extra docs --extra benchmark \
    sphinx-build -b html docs_api/source site
fi

if [[ ! -d site ]]; then
  echo "[preview-docs] No site/ directory found. Build the docs first." >&2
  exit 1
fi

URL="http://127.0.0.1:${PORT}/${PAGE}"
echo "[preview-docs] Benchmark page: ${URL}"

if [[ "$SERVE" -eq 0 ]]; then
  exit 0
fi

echo "[preview-docs] Serving site/ at http://127.0.0.1:${PORT}/"
exec env UV_CACHE_DIR="$UV_CACHE_DIR" uv run python -m http.server \
  "$PORT" --directory site
