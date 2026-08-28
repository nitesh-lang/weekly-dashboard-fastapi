#!/bin/bash
# Bulk-backfill July 2026 for Nexlev + Audio Array.
# Each POST calls the SP-API pullers for every account attached to the brand
# and lands rows in Neon.
API="https://sales-dashboard-api-23ii.onrender.com"
COOKIE=/tmp/bulk_cookies.txt

# Login once
rm -f "$COOKIE"
curl -sf -c "$COOKIE" -X POST -H 'Content-Type: application/json' \
  -d '{"email":"info@cambiumretail.com","password":"Cambium@109"}' \
  "$API/api/auth/login" > /dev/null || { echo "LOGIN_FAILED"; exit 1; }

for d in $(seq 1 28); do
  DATE=$(printf "2026-07-%02d" "$d")
  for BRAND in nexlev audio_array; do
    START=$(date +%s)
    RESP=$(curl -s -b "$COOKIE" -X POST --max-time 300 \
             "$API/api/$BRAND/pull-sales?date=$DATE" 2>&1)
    END=$(date +%s)
    ELAPSED=$((END-START))
    SUMMARY=$(echo "$RESP" | python -c "
import json,sys
try:
    d=json.loads(sys.stdin.read())
except Exception as e:
    print('parse_err:'+str(e)[:80]); raise SystemExit
if 'error' in d:
    print('ERR:'+str(d['error'])[:120])
else:
    tr=d.get('total_rows'); acc=d.get('rows_by_account',{}); oop=d.get('out_of_plan_count',0)
    print(f'total={tr} out_of_plan={oop} by_account={acc}')
" 2>&1)
    echo "[$DATE][$BRAND] ${ELAPSED}s  $SUMMARY"
  done
done
echo "DONE_ALL"
