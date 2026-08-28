#!/bin/bash
# Backfill AA Vendor Central for days where the migrated source DB has gaps.
# Uses the existing /pull-sales endpoint which fetches EVERY account attached
# to the brand + replace_day=true so the day's rows are fully refreshed.
API="https://sales-dashboard-api-23ii.onrender.com"
COOKIE=/tmp/aa_bf.txt

rm -f "$COOKIE"
curl -sf -c "$COOKIE" -X POST -H 'Content-Type: application/json' \
  -d '{"email":"info@cambiumretail.com","password":"Cambium@109"}' \
  "$API/api/auth/login" > /dev/null || { echo "LOGIN_FAILED"; exit 1; }

for DATE in 2026-07-07 2026-07-08 2026-07-11 2026-07-12 2026-07-13 2026-07-14 2026-07-15 2026-07-16 2026-07-18 2026-07-27 2026-07-28; do
  START=$(date +%s)
  RESP=$(curl -s -b "$COOKIE" -X POST --max-time 300 \
           "$API/api/audio_array/pull-sales?date=$DATE" 2>&1)
  END=$(date +%s)
  SUMMARY=$(echo "$RESP" | python -c "
import json,sys
try:
    d=json.loads(sys.stdin.read())
    if 'error' in d: print('ERR:'+str(d['error'])[:120])
    else:
        acc=d.get('rows_by_account',{})
        vc=acc.get('Vendor Central', 0)
        print(f\"total={d.get('total_rows')} vendor={vc} accts={acc}\")
except Exception as e:
    print('parse_err:'+str(e)[:100])
")
  echo "[AA $DATE] $((END-START))s  $SUMMARY"
done
echo "DONE_AA_BACKFILL"
