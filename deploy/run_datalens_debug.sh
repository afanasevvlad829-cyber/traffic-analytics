#!/bin/bash
set -euo pipefail

cd ~/traffic-analytics
source deploy/.env_datalens

echo "=== ENV ==="
echo "ORG: $DATALENS_ORG_ID"
echo "WORKBOOK: $DATALENS_WORKBOOK_ID"

echo
echo "=== IAM TOKEN ==="
IAM_TOKEN=$(yc iam create-token)
if [ -z "$IAM_TOKEN" ]; then
  echo "ERROR: cannot get IAM token"
  exit 1
fi
echo "TOKEN OK"

mkdir -p deploy/datalens_templates/debug

echo
echo "=== REQUEST 1: current endpoint ==="
curl -i \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "X-Org-Id: $DATALENS_ORG_ID" \
  "https://datalens.yandex.cloud/api/v1/workbooks/$DATALENS_WORKBOOK_ID/entries" \
  -o deploy/datalens_templates/debug/entries_response.txt \
  -sS || true

echo "Saved: deploy/datalens_templates/debug/entries_response.txt"

echo
echo "=== FIRST 80 LINES ==="
sed -n '1,80p' deploy/datalens_templates/debug/entries_response.txt || true

echo
echo "=== REQUEST 2: rpc listDirectory ==="
curl -i \
  -X POST \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "X-Org-Id: $DATALENS_ORG_ID" \
  -H "Content-Type: application/json" \
  "https://api.datalens.yandex.net/rpc/listDirectory" \
  -d '{"path":"/","page":0,"pageSize":100,"includePermissionsInfo":false}' \
  -o deploy/datalens_templates/debug/listDirectory_response.txt \
  -sS || true

echo "Saved: deploy/datalens_templates/debug/listDirectory_response.txt"

echo
echo "=== FIRST 120 LINES ==="
sed -n '1,120p' deploy/datalens_templates/debug/listDirectory_response.txt || true

echo
echo "=== FILES ==="
ls -lh deploy/datalens_templates/debug
