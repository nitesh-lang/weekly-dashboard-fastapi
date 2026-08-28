#!/bin/bash
# Wipe + fresh SP-API pull for Nexlev & Audio Array, Jul 2-27.
# Plan-aware ingest is live → only planned ASINs land in the ledger.
# 5-min pacing between calls keeps us under Amazon's report quota.
API="https://sales-dashboard-api-23ii.onrender.com"
COOKIE=/tmp/rb.txt
NEON='postgresql://neondb_owner:npg_DFo7HqWST0hs@ep-noisy-rain-aztnfk88.c-3.ap-southeast-1.aws.neon.tech/neondb?sslmode=require'

rm -f "$COOKIE"
curl -sf -c "$COOKIE" -X POST -H 'Content-Type: application/json' \
  -d '{"email":"info@cambiumretail.com","password":"Cambium@109"}' \
  "$API/api/auth/login" > /dev/null || { echo "LOGIN_FAILED"; exit 1; }

wipe_and_pull () {
  local BRAND="$1"; local DATE="$2"
  # Wipe every row for this (brand, date) so migrated data doesn't linger
  python -c "
import psycopg2
c=psycopg2.connect('$NEON'); cur=c.cursor()
cur.execute(\"DELETE FROM ledger WHERE brand=%s AND date=%s\", ('$BRAND','$DATE'))
c.commit(); print(f'wiped={cur.rowcount}', end='')
c.close()
" > /tmp/wc_$$; WIPED=$(cat /tmp/wc_$$); rm -f /tmp/wc_$$

  START=$(date +%s)
  RESP=$(curl -s -b "$COOKIE" -X POST --max-time 900 \
           "$API/api/$BRAND/pull-sales?date=$DATE" 2>&1)
  END=$(date +%s)
  SUMMARY=$(echo "$RESP" | python -c "
import json,sys
try: d=json.loads(sys.stdin.read())
except: print('parse_err'); raise SystemExit
if 'ok' in d and d['ok']:
    acc=d.get('rows_by_account',{}) or {}
    print(f\"total={d.get('total_rows')} kept={acc}\")
else:
    err=(d.get('detail',d) or {}).get('error',d) if isinstance(d,dict) else d
    print(f'ERR:{str(err)[:200]}')
")
  echo "[$BRAND $DATE] ${WIPED}  ${SUMMARY}  ($((END-START))s)"
}

# Interleave brands so a rate-limit hit doesn't strand all of one brand
for D in 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27; do
  wipe_and_pull nexlev "2026-07-$D"
  sleep 300
  wipe_and_pull audio_array "2026-07-$D"
  sleep 300
done
echo "DONE_JULY_REBUILD"
