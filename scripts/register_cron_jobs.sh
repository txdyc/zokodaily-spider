#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOG_DIR="${REPO_ROOT}/scheduler_logs"
LOCK_DIR="${LOG_DIR}/locks"
NEWS_SCRIPT="${SCRIPT_DIR}/run_news_crawler.sh"
JIJI_SCRIPT="${SCRIPT_DIR}/run_jiji_crawler.sh"

START_MARKER="# >>> news_spider cron jobs >>>"
END_MARKER="# <<< news_spider cron jobs <<<"

mkdir -p "${LOG_DIR}" "${LOCK_DIR}"
chmod +x "${NEWS_SCRIPT}" "${JIJI_SCRIPT}"

NEWS_JOB="0 * * * * flock -n \"${LOCK_DIR}/news_crawler.lock\" \"${NEWS_SCRIPT}\""
JIJI_JOB="30 */2 * * * flock -n \"${LOCK_DIR}/jiji_crawler.lock\" \"${JIJI_SCRIPT}\""

CURRENT_CRON="$(crontab -l 2>/dev/null || true)"
CLEANED_CRON="$(printf '%s\n' "${CURRENT_CRON}" | sed "/^${START_MARKER//\//\\/}$/,/^${END_MARKER//\//\\/}$/d")"

NEW_CRON="$(cat <<EOF
${CLEANED_CRON}
${START_MARKER}
${NEWS_JOB}
${JIJI_JOB}
${END_MARKER}
EOF
)"

printf '%s\n' "${NEW_CRON}" | awk 'NF || prev {print} {prev=NF}' | crontab -

echo "Cron jobs installed successfully."
echo "News crawler: every hour at minute 0"
echo "Jiji crawler: every 2 hours at minute 30"
echo "View with: crontab -l"
