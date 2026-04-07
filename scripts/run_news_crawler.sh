#!/usr/bin/env bash
set -euo pipefail

SITES="${SITES:-myjoyonline,graphic}"
MAX_PAGES="${MAX_PAGES:-3}"
MAX_ARTICLES="${MAX_ARTICLES:-0}"
CRAWL_CONCURRENCY="${CRAWL_CONCURRENCY:-4}"
CRAWL_TIMEOUT_SECONDS="${CRAWL_TIMEOUT_SECONDS:-45}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${REPO_ROOT}/scheduler_logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/news_crawler_${TIMESTAMP}.log"

mkdir -p "${LOG_DIR}"

find_python() {
  if [[ -n "${PYTHON_BIN:-}" ]]; then
    printf '%s\n' "${PYTHON_BIN}"
    return
  fi
  local candidates=(
    "${REPO_ROOT}/.conda/bin/python"
    "${REPO_ROOT}/.venv/bin/python"
    "$(command -v python3 2>/dev/null || true)"
    "$(command -v python 2>/dev/null || true)"
  )
  local candidate
  for candidate in "${candidates[@]}"; do
    if [[ -n "${candidate}" && -x "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return
    fi
  done
  return 1
}

PYTHON_EXE="$(find_python)" || {
  echo "No suitable Python interpreter found." >&2
  exit 1
}

export MAX_PAGES
export MAX_ARTICLES
export CRAWL_CONCURRENCY
export CRAWL_TIMEOUT_SECONDS

cd "${REPO_ROOT}"
{
  echo "[$(date --iso-8601=seconds)] Starting news crawler for sites: ${SITES}"
  "${PYTHON_EXE}" news_crawler.py --sites "${SITES}"
  EXIT_CODE=$?
  echo "[$(date --iso-8601=seconds)] News crawler finished with exit code: ${EXIT_CODE}"
  exit "${EXIT_CODE}"
} >> "${LOG_FILE}" 2>&1
