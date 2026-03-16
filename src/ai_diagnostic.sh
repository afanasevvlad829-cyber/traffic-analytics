#!/usr/bin/env bash
set -u

REPORT="/tmp/direct_ai_diagnostic_report.txt"
exec > >(tee "$REPORT") 2>&1

echo "========================================"
echo " DIRECT AI DIAGNOSTIC REPORT"
echo "========================================"
date -u
echo

echo "=== SERVER ==="
hostname || true
whoami || true
uptime || true
echo

echo "=== BACKEND LOCAL ==="
echo "-- health:"
curl -s http://127.0.0.1:8088/health || true
echo
echo "-- config:"
curl -s http://127.0.0.1:8088/api/config || true
echo
echo "-- full dashboard:"
curl -s -o /tmp/direct_ai_dashboard_check.json -w "HTTP_CODE=%{http_code}\n" http://127.0.0.1:8088/api/full-dashboard || true
head -c 400 /tmp/direct_ai_dashboard_check.json 2>/dev/null || true
echo
echo

echo "=== SYSTEMD ==="
echo "-- direct-ai-webapp:"
sudo systemctl status direct-ai-webapp.service --no-pager -l | head -n 30 || true
echo
echo "-- nginx:"
sudo systemctl status nginx --no-pager -l | head -n 30 || true
echo
echo "-- certbot timer:"
sudo systemctl status certbot.timer --no-pager -l | head -n 20 || true
echo

echo "=== PORTS ==="
sudo ss -tlnp | grep -E ':80 |:443 |:8088 ' || true
echo

echo "=== NGINX TEST ==="
sudo nginx -t || true
echo

echo "=== NGINX SERVER_NAME ==="
sudo grep -RIn "server_name" /etc/nginx 2>/dev/null || true
echo

echo "=== NGINX PROXY_PASS ==="
sudo grep -RIn "proxy_pass" /etc/nginx 2>/dev/null || true
echo

echo "=== NGINX ERROR LOG ==="
sudo tail -n 40 /var/log/nginx/error.log 2>/dev/null || true
echo

echo "=== NGINX ACCESS LOG ==="
sudo tail -n 20 /var/log/nginx/access.log 2>/dev/null || true
echo

echo "=== APP LOG FILE ==="
tail -n 60 /home/kv145/traffic-analytics/direct_ai.log 2>/dev/null || true
echo

echo "=== CRON ==="
crontab -l 2>/dev/null || true
echo

echo "=== DOMAIN CHECK: ai.aidaplus.ru ==="
dig +short ai.aidaplus.ru || true
echo "-- root:"
curl -k -s -o /tmp/ai_aidaplus_root.html -w "HTTP_CODE=%{http_code}\n" https://ai.aidaplus.ru/ || true
head -n 3 /tmp/ai_aidaplus_root.html 2>/dev/null || true
echo
echo "-- webapp:"
curl -k -s -o /tmp/ai_aidaplus_webapp.html -w "HTTP_CODE=%{http_code}\n" https://ai.aidaplus.ru/webapp || true
head -n 3 /tmp/ai_aidaplus_webapp.html 2>/dev/null || true
echo
echo "-- api/full-dashboard:"
curl -k -s -o /tmp/ai_aidaplus_api.json -w "HTTP_CODE=%{http_code}\n" https://ai.aidaplus.ru/api/full-dashboard || true
head -c 400 /tmp/ai_aidaplus_api.json 2>/dev/null || true
echo
echo "-- health:"
curl -k -s -o /tmp/ai_aidaplus_health.json -w "HTTP_CODE=%{http_code}\n" https://ai.aidaplus.ru/health || true
cat /tmp/ai_aidaplus_health.json 2>/dev/null || true
echo
echo

echo "=== TELEGRAM MENU BUTTON ==="
BOT_TOKEN=$(python3 - <<'PY'
path="/home/kv145/traffic-analytics/.env"
token=""
try:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s=line.strip()
            if not s or "=" not in s:
                continue
            if s.startswith("export "):
                s=s[len("export "):].strip()
            k,v=s.split("=",1)
            if k.strip() in ("TG_TOKEN","BOT_TOKEN"):
                token=v.strip().strip('"').strip("'")
                break
except FileNotFoundError:
    pass
print(token)
PY
)
if [ -n "${BOT_TOKEN:-}" ]; then
  curl -s "https://api.telegram.org/bot${BOT_TOKEN}/getChatMenuButton" || true
else
  echo "BOT TOKEN NOT FOUND"
fi
echo
echo

echo "=== QUICK SUMMARY ==="
ROOT_CODE=$(curl -k -s -o /dev/null -w "%{http_code}" https://ai.aidaplus.ru/ || true)
WEBAPP_CODE=$(curl -k -s -o /dev/null -w "%{http_code}" https://ai.aidaplus.ru/webapp || true)
API_CODE=$(curl -k -s -o /dev/null -w "%{http_code}" https://ai.aidaplus.ru/api/full-dashboard || true)
HEALTH_CODE=$(curl -k -s -o /dev/null -w "%{http_code}" https://ai.aidaplus.ru/health || true)

echo "ROOT_CODE=$ROOT_CODE"
echo "WEBAPP_CODE=$WEBAPP_CODE"
echo "API_CODE=$API_CODE"
echo "HEALTH_CODE=$HEALTH_CODE"

echo
echo "=== REPORT PATH ==="
echo "$REPORT"

echo
echo "========================================"
echo " END DIAGNOSTIC REPORT"
echo "========================================"
