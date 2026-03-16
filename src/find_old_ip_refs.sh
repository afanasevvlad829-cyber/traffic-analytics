#!/usr/bin/env bash
set -euo pipefail

OLD_IPS=(
  "62.84.121.112"
  "158.160.77.231"
)

NEW_DOMAIN="ai.aidaplus.ru"

SEARCH_PATHS=(
  "/home/kv145/traffic-analytics"
  "/etc/nginx"
  "/etc/systemd/system"
  "/var/spool/cron"
)

EXCLUDES=(
  "--exclude-dir=.git"
  "--exclude-dir=.venv"
  "--exclude=*.pyc"
  "--exclude=*.log"
  "--exclude=find_old_ip_refs.sh"
)

echo "=== SEARCHING FOR OLD IP REFERENCES ==="
echo

for ip in "${OLD_IPS[@]}"; do
  echo "--- IP: $ip ---"
  found=0
  for path in "${SEARCH_PATHS[@]}"; do
    if [ -e "$path" ]; then
      if grep -RIn "${EXCLUDES[@]}" -- "$ip" "$path" 2>/dev/null; then
        found=1
      fi
    fi
  done
  if [ "$found" -eq 0 ]; then
    echo "No references found for $ip"
  fi
  echo
done

echo "=== SEARCHING FOR DOMAIN REFERENCES ==="
echo

domain_found=0
for path in "${SEARCH_PATHS[@]}"; do
  if [ -e "$path" ]; then
    if grep -RIn "${EXCLUDES[@]}" -- "$NEW_DOMAIN" "$path" 2>/dev/null; then
      domain_found=1
    fi
  fi
done

if [ "$domain_found" -eq 0 ]; then
  echo "No references found for $NEW_DOMAIN"
fi

echo
echo "=== COMMON FILES TO REVIEW MANUALLY ==="
for f in \
  "/home/kv145/traffic-analytics/.env" \
  "/home/kv145/traffic-analytics/webapp/app.py" \
  "/home/kv145/traffic-analytics/webapp/templates/index.html" \
  "/etc/nginx/sites-available/direct-ai" \
  "/etc/nginx/sites-enabled/direct-ai" \
  "/etc/systemd/system/direct-ai-webapp.service"
do
  [ -e "$f" ] && echo "$f"
done
