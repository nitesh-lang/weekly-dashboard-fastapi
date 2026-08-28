"""Thin async SP-API client — LWA token exchange + report submit/poll/download.

Shared by the seller (3P) and vendor (1P) pullers. Marketplace + endpoint
default to India / EU host but are env-overridable.
"""
from __future__ import annotations

import gzip
import json
import os
import time
from typing import Any

import httpx

LWA_URL = "https://api.amazon.com/auth/o2/token"
SPAPI_HOST = os.getenv("SP_API_ENDPOINT", "https://sellingpartnerapi-eu.amazon.com")
MARKETPLACE = os.getenv("SP_MARKETPLACE_ID", "A21TJRUUN4KGV")  # India


class SpApiError(RuntimeError):
    pass


def get_access_token(
    client_id: str | None,
    client_secret: str | None,
    refresh_token: str | None,
) -> str:
    if not (client_id and client_secret):
        raise SpApiError("SP_LWA_CLIENT_ID / SP_LWA_CLIENT_SECRET not set")
    if not refresh_token:
        raise SpApiError("refresh_token not set")
    r = httpx.post(
        LWA_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise SpApiError(f"LWA token exchange failed: HTTP {r.status_code} {r.text[:200]}")
    return r.json()["access_token"]


def submit_and_download(
    token: str,
    body: dict[str, Any],
    poll_seconds: int = 5,
    poll_max: int = 60,
) -> dict:
    """Submit a report, poll until DONE, download & decode JSON.

    `body` is the full request body for POST /reports/2021-06-30/reports —
    caller supplies reportType, marketplaceIds, window, reportOptions.
    """
    h = {"x-amz-access-token": token, "Content-Type": "application/json"}
    r = httpx.post(f"{SPAPI_HOST}/reports/2021-06-30/reports", json=body, headers=h, timeout=30)
    if r.status_code != 202:
        raise SpApiError(f"create report failed: HTTP {r.status_code} {r.text[:300]}")
    rid = r.json()["reportId"]

    doc_id: str | None = None
    for _ in range(poll_max):
        time.sleep(poll_seconds)
        rr = httpx.get(
            f"{SPAPI_HOST}/reports/2021-06-30/reports/{rid}",
            headers={"x-amz-access-token": token},
            timeout=30,
        )
        if rr.status_code != 200:
            continue
        js = rr.json()
        st = js.get("processingStatus")
        if st == "DONE":
            doc_id = js.get("reportDocumentId")
            break
        if st in ("FATAL", "CANCELLED"):
            raise SpApiError(f"report {st} (id={rid})")
    if not doc_id:
        raise SpApiError(f"timed out waiting for report {rid}")

    rd = httpx.get(
        f"{SPAPI_HOST}/reports/2021-06-30/documents/{doc_id}",
        headers={"x-amz-access-token": token},
        timeout=30,
    )
    rd.raise_for_status()
    doc = rd.json()
    dl = httpx.get(doc["url"], timeout=120)
    dl.raise_for_status()
    raw = dl.content
    if doc.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))
