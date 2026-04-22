"""
zoho_client.py
──────────────
Shared Zoho CRM API layer for all NZF dashboard scripts.

Strategy
────────
1. COQL  (POST /crm/v7/coql)  — primary.
   - Single filtered query, proper LIMIT/OFFSET pagination.
   - Works with ZohoCRM.modules.ALL scope.
   - ~6-15 API calls per full refresh.

2. Chunked list API  (GET /crm/v3/{module})  — automatic fallback.
   - Sorted by Created_Time desc, stops once records are older than
     the requested cutoff.  No silent 2000-record truncation risk.
   - Activated automatically if COQL returns a 401/403/400.

Why not the Search API (/search)?
   - Hard limit of 2000 total records across all pages (Zoho documented).
   - NZF case volume could breach this silently. Never used here.

API rate limits (Zoho CRM Enterprise)
   - 5,000 calls/day per org
   - COQL approach: ~15 calls/refresh × 4 refreshes/day = 60 calls/day
   - Old approach: up to 250 calls/refresh × 4 = 1,000 calls/day
"""

import os, time, requests
from datetime import datetime, timezone

ACCOUNTS_URL = os.environ.get("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com")
API_DOMAIN   = os.environ.get("ZOHO_API_DOMAIN",   "https://www.zohoapis.com")

# ── Auth ──────────────────────────────────────────────────────────
def get_access_token():
    client_id     = os.environ["ZOHO_CLIENT_ID"]
    client_secret = os.environ["ZOHO_CLIENT_SECRET"]
    refresh_token = os.environ["ZOHO_REFRESH_TOKEN"]

    res = requests.post(f"{ACCOUNTS_URL}/oauth/v2/token", params={
        "refresh_token": refresh_token,
        "client_id":     client_id,
        "client_secret": client_secret,
        "grant_type":    "refresh_token",
    })
    res.raise_for_status()
    data  = res.json()
    token = data.get("access_token")
    if not token:
        raise ValueError(f"No access_token in response: {data}")
    print("✓ Access token obtained")
    return token

# ── Helpers ───────────────────────────────────────────────────────
def _headers(token):
    return {"Authorization": f"Zoho-oauthtoken {token}"}

def _sleep_on_rate_limit(res):
    if res.status_code == 429:
        retry_after = int(res.headers.get("Retry-After", 15))
        print(f"  Rate limited — waiting {retry_after}s...")
        time.sleep(retry_after)
        return True
    return False

def _retry_get(url, headers, params, max_retries=3):
    """GET with automatic retry on rate-limit and transient errors."""
    for attempt in range(max_retries):
        res = requests.get(url, headers=headers, params=params)
        if _sleep_on_rate_limit(res):
            continue
        if res.status_code in (500, 502, 503, 504) and attempt < max_retries - 1:
            time.sleep(2 ** attempt)
            continue
        return res
    return res   # return last response for caller to handle

def _retry_post(url, headers, json_body, max_retries=3):
    """POST with automatic retry."""
    for attempt in range(max_retries):
        res = requests.post(url, headers=headers, json=json_body)
        if _sleep_on_rate_limit(res):
            continue
        if res.status_code in (500, 502, 503, 504) and attempt < max_retries - 1:
            time.sleep(2 ** attempt)
            continue
        return res
    return res

# ── Parse datetime safely ─────────────────────────────────────────
def parse_dt(s):
    if not s:
        return None
    for fmt in ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(s, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

# ─────────────────────────────────────────────────────────────────
# PRIMARY: COQL query — POST /crm/v7/coql
# ─────────────────────────────────────────────────────────────────
def coql_query(token, base_query, max_records=20000):
    """
    Execute a COQL SELECT query with automatic LIMIT/OFFSET pagination.

    base_query should NOT include LIMIT or OFFSET — they are added here.
    Example:
        SELECT id, Deal_Name, Contact_Name, Created_Time
        FROM Deals
        WHERE Created_Time >= '2025-01-01T00:00:00+00:00'

    Returns list of record dicts, or raises CoqlUnavailable if the
    org/edition does not support COQL (triggers fallback).
    """
    url     = f"{API_DOMAIN}/crm/v7/coql"
    headers = {**_headers(token), "Content-Type": "application/json"}
    records = []
    offset  = 0
    limit   = 200

    while len(records) < max_records:
        query = f"{base_query} LIMIT {limit} OFFSET {offset}"
        res   = _retry_post(url, headers, {"select_query": query})

        # 204 = no (more) results
        if res.status_code == 204:
            break

        # 401/403/400 with INVALID_QUERY → COQL not available on this plan
        if res.status_code in (400, 401, 403):
            err = res.json() if res.content else {}
            code = err.get("code", "")
            raise CoqlUnavailable(
                f"COQL unavailable (HTTP {res.status_code}, code={code}). "
                "Falling back to list API."
            )

        res.raise_for_status()
        data  = res.json()
        batch = data.get("data", [])
        if not batch:
            break

        records.extend(batch)

        if not data.get("info", {}).get("more_records", False):
            break

        offset += limit
        time.sleep(0.1)   # 100ms between pages — well within rate limits

    return records


class CoqlUnavailable(Exception):
    pass

# ─────────────────────────────────────────────────────────────────
# FALLBACK: Chunked list API — GET /crm/v3/{module}
# ─────────────────────────────────────────────────────────────────
def list_records(token, module, fields, cutoff_dt=None, max_records=20000):
    """
    Fetch records using the plain list endpoint, sorted by Created_Time desc.
    If cutoff_dt is supplied, stops paginating once records are older than it.

    This approach avoids the Search API's 2000-record hard limit.
    """
    url     = f"{API_DOMAIN}/crm/v3/{module}"
    headers = _headers(token)
    records = []
    page    = 1

    while len(records) < max_records:
        params = {
            "fields":     ",".join(fields),
            "page":       page,
            "per_page":   200,
            "sort_by":    "Created_Time",
            "sort_order": "desc",
        }
        res = _retry_get(url, headers, params)

        if res.status_code == 204:
            break

        res.raise_for_status()
        data  = res.json()
        batch = data.get("data", [])
        if not batch:
            break

        if cutoff_dt:
            keep, stop = [], False
            for r in batch:
                dt = parse_dt(r.get("Created_Time"))
                if dt and dt >= cutoff_dt:
                    keep.append(r)
                else:
                    stop = True
            records.extend(keep)
            if stop:
                break
        else:
            records.extend(batch)

        if not data.get("info", {}).get("more_records", False):
            break

        page += 1
        time.sleep(0.15)

    return records

# ─────────────────────────────────────────────────────────────────
# PUBLIC API — auto-selects COQL or fallback
# ─────────────────────────────────────────────────────────────────
_coql_available = True   # module-level flag; set False after first failure

def fetch(token, coql_query_str, fallback_module, fallback_fields,
          fallback_cutoff_dt=None, label="records", max_records=20000):
    """
    Fetch records using COQL, automatically falling back to list API
    if COQL is not available.

    Args:
        token:              OAuth access token
        coql_query_str:     Full COQL SELECT (no LIMIT/OFFSET)
        fallback_module:    Zoho module API name for list fallback
        fallback_fields:    List of field API names for list fallback
        fallback_cutoff_dt: datetime — stop list pagination at this date
        label:              Human-readable label for logging
        max_records:        Safety cap
    """
    global _coql_available

    if _coql_available:
        try:
            print(f"  [{label}] Using COQL...")
            records = coql_query(token, coql_query_str, max_records)
            print(f"  [{label}] → {len(records)} records via COQL")
            return records
        except CoqlUnavailable as e:
            print(f"  [{label}] {e}")
            _coql_available = False   # Don't try COQL again this run

    # Fallback
    print(f"  [{label}] Using list API (fallback)...")
    records = list_records(
        token, fallback_module, fallback_fields,
        cutoff_dt=fallback_cutoff_dt,
        max_records=max_records,
    )
    print(f"  [{label}] → {len(records)} records via list API")
    return records
