#!/bin/bash
# Fill remaining AA/Nex July gaps via SP-API with 5-min pacing between calls
# to stay under Amazon's report quota. Safe-replace guard is now live so any
# empty response leaves existing rows untouched.
API="https://sales-dashboard-api-23ii.onrender.com"
COOKIE=/tmp/gap.txt

rm -f "$COOKIE"
curl -sf -c "$COOKIE" -X POST -H 'Content-Type: application/json' \
  -d '{"email":"info@cambiumretail.com","password":"Cambium@109"}' \
  "$API/api/auth/login" > /dev/null || { echo "LOGIN_FAILED"; exit 1; }

do_pull () {
  local BRAND="$1"; local DATE="$2"
  START=$(date +%s)
  RESP=$(curl -s -b "$COOKIE" -X POST --max-time 300 \
           "$API/api/$BRAND/pull-sales?date=$DATE" 2>&1)
  END=$(date +%s)
  SUMMARY=$(echo "$RESP" | python -c "
import json,sys
try: d=json.loads(sys.stdin.read())
except: print('parse_err'); raise SystemExit
if 'ok' in d and d['ok']:
    acc=d.get('rows_by_account',{})
    sk=d.get('skipped_empty_accounts',[])
    print(f\"total={d.get('total_rows')} kept_from={acc} skipped={sk}\")
else:
    err=d.get('detail',d).get('error',d) if isinstance(d,dict) else d
    print(f'ERR:{str(err)[:200]}')
" 2>&1)
  echo "[$BRAND $DATE] $((END-START))s  $SUMMARY"
}

# AA missing 1P (Vendor Central) days
for D in 2026-07-13 2026-07-14 2026-07-15 2026-07-20 2026-07-27 2026-07-28; do
  do_pull audio_array "$D"
  sleep 300
done

# Jul 29 fresh for both brands
do_pull nexlev 2026-07-29
sleep 300
do_pull audio_array 2026-07-29

echo "DONE_FILL"
