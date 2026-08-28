#!/bin/bash
# Retry AA missing-vendor days after the salesByAsin fix. 5-min pacing keeps
# us under Amazon's report quota; safe-replace guard ensures a partial
# response never wipes clean seller rows.
API="https://sales-dashboard-api-23ii.onrender.com"
COOKIE=/tmp/vr.txt

rm -f "$COOKIE"
curl -sf -c "$COOKIE" -X POST -H 'Content-Type: application/json' \
  -d '{"email":"info@cambiumretail.com","password":"Cambium@109"}' \
  "$API/api/auth/login" > /dev/null || { echo "LOGIN_FAILED"; exit 1; }

for D in 2026-07-13 2026-07-14 2026-07-15 2026-07-20 2026-07-27 2026-07-28; do
  START=$(date +%s)
  RESP=$(curl -s -b "$COOKIE" -X POST --max-time 600 \
           "$API/api/audio_array/pull-sales?date=$D" 2>&1)
  END=$(date +%s)
  SUMMARY=$(echo "$RESP" | python -c "
import json,sys
try: d=json.loads(sys.stdin.read())
except: print('parse_err'); raise SystemExit
if 'ok' in d and d['ok']:
    acc=d.get('rows_by_account',{})
    print(f\"total={d.get('total_rows')} vendor={acc.get('Vendor Central', 0)} accts={acc}\")
else:
    err=(d.get('detail',d) or {}).get('error',d) if isinstance(d,dict) else d
    print(f'ERR:{str(err)[:200]}')
" 2>&1)
  echo "[AA $D] $((END-START))s  $SUMMARY"
  sleep 300
done
echo "DONE_VENDOR_RETRY"
