"""
Debug helper — dump the raw structure of a few SB ads to figure out
where the ASIN field actually lives in the v4 schema.
"""
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env", override=False)

from weekly_app.etl.ads_api_pull import (
    get_access_token, base_headers, _post_paginated_v4,
    ADS_API_BASE, SB_CAMPAIGNS_CT, SB_ADS_CT,
)

# Pick the Nexlev vendor profile — has 74 campaigns so plenty of variety
profile_id = sys.argv[1] if len(sys.argv) > 1 else "1122755254933937"

token, _ = get_access_token()
h = base_headers(profile_id, token)
cam_h = {**h, "Accept": SB_CAMPAIGNS_CT, "Content-Type": SB_CAMPAIGNS_CT}
ads_h = {**h, "Accept": SB_ADS_CT,       "Content-Type": SB_ADS_CT}

campaigns = _post_paginated_v4(f"{ADS_API_BASE}/sb/v4/campaigns/list",
                                cam_h, {}, "campaigns")
ads = _post_paginated_v4(f"{ADS_API_BASE}/sb/v4/ads/list", ads_h, {}, "ads")

print(f"Profile: {profile_id}")
print(f"Campaigns: {len(campaigns)} · Ads: {len(ads)}\n")

# Dump 3 ads in full
for ad in ads[:3]:
    print("=" * 70)
    print(json.dumps(ad, indent=2, default=str))
    print()
