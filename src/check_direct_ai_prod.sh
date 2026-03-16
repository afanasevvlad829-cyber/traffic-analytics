#!/usr/bin/env bash
set -u

DOMAINS=("ai.aidaplus.ru" "ai.educamp.ru")
REPORT="/tmp/direct_ai_prod_report.txt"

exec > >(tee "$REPORT") 2>&1

echo "=== DIRECT AI PROD CHECK START ==="
date -u
echo

echo "=== 1. LOCAL BACKEND ==="
echo "-- health:"
curl -s http://127.0.0.1:8088/health || true
echo
echo "-- config:"
curl -s http://127.0.0.1:8088/api/config || true
echo
echo "-- webapp route:"
curl -s -o /tmp/direct_webapp_local.html -w "HTTP_CODE=%{http_code}\n" http://127.0.0.1:8088/webapp || true
echo "-- first lines of local webapp:"
head -n 5 /tmp/direct_webapp_local.html 2>/dev/null || true
echo

echo "=== 2. SYSTEMD SERVICES ==="
echo "-- direct-ai-webapp:"
sudo systemctl status direct-ai-webapp.service --no-pager -l | head -n 20 || true
echo
echo "-- nginx:"
sudo systemctl status nginx --no-pager -l | head -n 20 || true
echo
echo "-- certbot timer:"
sudo systemctl status certbot.timer --no-pager -l | head -n 20 || true
echo

echo "=== 3. LISTEN PORTS ==="
sudo ss -tlnp | grep -E ':80 |:443 |:8088 ' || true
echo

echo "=== 4. NGINX CONFIG TEST ==="
sudo nginx -t || true
echo

echo "=== 5. NGINX SERVER_NAME SEARCH ==="
sudo grep -RIn "server_name" /etc/nginx 2>/dev/null || true
echo

echo "=== 6. NGINX PROXY_PASS SEARCH ==="
sudo grep -RIn "proxy_pass" /etc/nginx 2>/dev/null || true
echo

for d in "${DOMAINS[@]}"; do
  echo "=== DOMAIN CHECK: $d ==="
  echo "-- DNS:"
  dig +short "$d" || true
  echo

  echo "-- HTTP root:"
  curl -s -o "/tmp/${d}.root.http" -w "HTTP_CODE=%{http_code}\n" "http://$d/" || true
  head -n 3 "/tmp/${d}.root.http" 2>/dev/null || true
  echo

  echo "-- HTTPS root:"
  curl -k -s -o "/tmp/${d}.root.https" -w "HTTP_CODE=%{http_code}\n" "https://$d/" || true
  head -n 3 "/tmp/${d}.root.https" 2>/dev/null || true
  echo

  echo "-- HTTPS /webapp:"
  curl -k -s -o "/tmp/${d}.webapp.https" -w "HTTP_CODE=%{http_code}\n" "https://$d/webapp" || true
  head -n 5 "/tmp/${d}.webapp.https" 2>/dev/null || true
  echo

  echo "-- HTTPS /health:"
  curl -k -s -o "/tmp/${d}.health.https" -w "HTTP_CODE=%{http_code}\n" "https://$d/health" || true
  cat "/tmp/${d}.health.https" 2>/dev/null || true
  echo
done

echo "=== 7. SSL CERTS PRESENT ==="
sudo ls -lah /etc/letsencrypt/live/ 2>/dev/null || true
echo

echo "=== 8. CERTBOT RENEW DRY RUN ==="
sudo certbot renew --dry-run || true
echo

echo "=== 9. TELEGRAM MENU BUTTON (if token exists) ==="
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
  echo "BOT TOKEN NOT FOUND IN .env"
fi
echo
echo "=== 10. QUICK DIAGNOSIS ==="

AIDAPLUS_ROOT_CODE=$(curl -k -s -o /dev/null -w "%{http_code}" https://ai.aidaplus.ru/ || true)
AIDAPLUS_HEALTH_CODE=$(curl -k -s -o /dev/null -w "%{http_code}" https://ai.aidaplus.ru/health || true)
AIDAPLUS_WEBAPP_CODE=$(curl -k -s -o /dev/null -w "%{http_code}" https://ai.aidaplus.ru/webapp || true)

EDUCAMP_ROOT_CODE=$(curl -k -s -o /dev/null -w "%{http_code}" https://ai.educamp.ru/ || true)
EDUCAMP_HEALTH_CODE=$(curl -k -s -o /dev/null -w "%{http_code}" https://ai.educamp.ru/health || true)
EDUCAMP_WEBAPP_CODE=$(curl -k -s -o /dev/null -w "%{http_code}" https://ai.educamp.ru/webapp || true)

echo "AIDAPLUS_ROOT_CODE=$AIDAPLUS_ROOT_CODE"
echo "AIDAPLUS_HEALTH_CODE=$AIDAPLUS_HEALTH_CODE"
echo "AIDAPLUS_WEBAPP_CODE=$AIDAPLUS_WEBAPP_CODE"
echo "EDUCAMP_ROOT_CODE=$EDUCAMP_ROOT_CODE"
echo "EDUCAMP_HEALTH_CODE=$EDUCAMP_HEALTH_CODE"
echo "EDUCAMP_WEBAPP_CODE=$EDUCAMP_WEBAPP_CODE"
echo

echo "=== STATUS SUMMARY ==="
if [ "$AIDAPLUS_ROOT_CODE" = "200" ] || [ "$AIDAPLUS_WEBAPP_CODE" = "200" ]; then
  echo "ai.aidaplus.ru: OK_OR_PARTIAL"
else
  echo "ai.aidaplus.ru: NOT_OK"
fi

if [ "$EDUCAMP_ROOT_CODE" = "200" ] || [ "$EDUCAMP_WEBAPP_CODE" = "200" ]; then
  echo "ai.educamp.ru: OK_OR_PARTIAL"
else
  echo "ai.educamp.ru: NOT_OK"
fi

if [ "$AIDAPLUS_HEALTH_CODE" = "200" ]; then
  echo "backend via ai.aidaplus.ru/health: OK"
else
  echo "backend via ai.aidaplus.ru/health: NOT_OK"
fi

echo
echo "=== REPORT FILE ==="
echo "$REPORT"
echo
echo "=== DIRECT AI PROD CHECK END ==="
