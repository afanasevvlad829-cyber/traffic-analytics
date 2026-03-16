#!/bin/bash
set -euo pipefail

cd ~/traffic-analytics
source deploy/.env_datalens

echo "=== ENV ==="
echo "ORG: $DATALENS_ORG_ID"
echo "WORKBOOK: $DATALENS_WORKBOOK_ID"
echo "PATH: ${DATALENS_PATH:-/}"

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
echo "=== REQUEST: listDirectory via api.datalens.tech ==="
HTTP_CODE=$(curl -sS \
  -o deploy/datalens_templates/debug/listDirectory_body.json \
  -w "%{http_code}" \
  -X POST \
  -H "Authorization: Bearer $IAM_TOKEN" \
  -H "x-dl-org-id: $DATALENS_ORG_ID" \
  -H "x-dl-api-version: 1" \
  -H "Content-Type: application/json" \
  "https://api.datalens.tech/rpc/listDirectory" \
  -d '{"path":"/","page":0,"pageSize":200,"includePermissionsInfo":false}'
)

echo "HTTP_CODE=$HTTP_CODE"
echo "Saved body: deploy/datalens_templates/debug/listDirectory_body.json"

echo
echo "=== FIRST 120 LINES OF BODY ==="
sed -n '1,120p' deploy/datalens_templates/debug/listDirectory_body.json || true

if [ "$HTTP_CODE" = "200" ]; then
  cp deploy/datalens_templates/debug/listDirectory_body.json deploy/datalens_templates/entries_manifest.json
  echo
  echo "Snapshot saved to deploy/datalens_templates/entries_manifest.json"
else
  echo
  echo "ERROR: API did not return 200"
  exit 1
fi
