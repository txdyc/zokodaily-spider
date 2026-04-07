#!/usr/bin/env bash
set -euo pipefail

START_MARKER="# >>> news_spider cron jobs >>>"
END_MARKER="# <<< news_spider cron jobs <<<"

CURRENT_CRON="$(crontab -l 2>/dev/null || true)"
if [[ -z "${CURRENT_CRON}" ]]; then
  echo "No crontab entries found."
  exit 0
fi

CLEANED_CRON="$(printf '%s\n' "${CURRENT_CRON}" | sed "/^${START_MARKER//\//\\/}$/,/^${END_MARKER//\//\\/}$/d")"
printf '%s\n' "${CLEANED_CRON}" | awk 'NF || prev {print} {prev=NF}' | crontab -

echo "News spider cron jobs removed."
