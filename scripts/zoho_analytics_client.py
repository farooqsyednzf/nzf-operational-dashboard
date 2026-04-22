"""
zoho_analytics_client.py
────────────────────────
Shared client for all NZF dashboard data fetch scripts.

Uses the Zoho Analytics Bulk Export API:
  1. POST  /bulk/workspaces/{ws}/exportjobs   → create job, receive job_id
  2. GET   /bulk/workspaces/{ws}/exportjobs/{id} → poll until jobCode = 1004
  3. GET   /bulk/workspaces/{ws}/exportjobs/{id}/data → download CSV

Why this beats the Zoho CRM API for dashboards
────────────────────────────────────────────────
  CRM API   → 27+ paginated calls, 2,000-record limits, 400 errors at page 11
  Analytics → 3 calls, no record limits, full MySQL SQL with JOINs

Required GitHub Secrets:
  ZOHO_CLIENT_ID       From Zoho API Console Self Client
  ZOHO_CLIENT_SECRET   From Zoho API Console Self Client
  ZOHO_REFRESH_TOKEN   Scope: ZohoAnalytics.data.read
  ZOHO_ACCOUNTS_URL    https://accounts.zoho.com

Hardcoded (not sensitive — just workspace identifiers):
  ORG_ID         668395719
  WORKSPACE_ID   1715382000001002475
"""

import os, time, json, csv, io, requests
from datetime import datetime, timezone

# ── Org / workspace (hardcoded — not credentials) ─────────────────
ORG_ID       = "668395719"
WORKSPACE_ID = "1715382000001002475"

ACCOUNTS_URL = os.environ.get("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com")
ANALYTICS_URL = "https://analytics.zoho.com/restapi/v2"

# jobCode meanings
JOB_NOT_INITIATED = "1001"
JOB_IN_PROGRESS   = "1002"
JOB_ERROR         = "1003"
JOB_COMPLETED     = "1004"
JOB_NOT_FOUND     = "1005"

# ── Auth ──────────────────────────────────────────────────────────
def get_access_token():
    res = requests.post(f"{ACCOUNTS_URL}/oauth/v2/token", params={
        "refresh_token": os.environ["ZOHO_REFRESH_TOKEN"],
        "client_id":     os.environ["ZOHO_CLIENT_ID"],
        "client_secret": os.environ["ZOHO_CLIENT_SECRET"],
        "grant_type":    "refresh_token",
    })
    res.raise_for_status()
    data  = res.json()
    token = data.get("access_token")
    if not token:
        raise ValueError(f"No access_token in response: {data}")
    print("✓ Access token obtained")
    return token

# ── Core export job ───────────────────────────────────────────────
def run_query(token, sql, label="query", poll_interval=3, max_wait=120):
    """
    Execute a MySQL-compatible SELECT query against Zoho Analytics
    and return the result as a list of dicts (parsed from CSV).

    Args:
        token:         OAuth access token (ZohoAnalytics.data.read scope)
        sql:           MySQL SELECT statement
        label:         Human-readable label for logging
        poll_interval: Seconds between status polls
        max_wait:      Max seconds to wait before timing out

    Returns:
        List of row dicts (column name → value)
    """
    headers = {
        "Authorization":   f"Zoho-oauthtoken {token}",
        "ZANALYTICS-ORGID": ORG_ID,
    }
    base = f"{ANALYTICS_URL}/bulk/workspaces/{WORKSPACE_ID}/exportjobs"

    # ── Step 1: Create export job ─────────────────────────────────
    config  = json.dumps({"sqlQuery": sql.strip(), "responseFormat": "csv"})
    res     = requests.post(base, headers=headers, params={"CONFIG": config})

    if not res.ok:
        raise RuntimeError(
            f"[{label}] Failed to create export job: "
            f"HTTP {res.status_code} — {res.text}"
        )

    job_id = res.json()["data"]["jobId"]
    print(f"  [{label}] Export job created: {job_id}")

    # ── Step 2: Poll until complete ───────────────────────────────
    waited  = 0
    status_url = f"{base}/{job_id}"

    while waited < max_wait:
        time.sleep(poll_interval)
        waited += poll_interval

        poll = requests.get(status_url, headers=headers)
        poll.raise_for_status()
        info     = poll.json()["data"]
        job_code = info.get("jobCode", "")

        if job_code == JOB_COMPLETED:
            break
        elif job_code == JOB_ERROR:
            raise RuntimeError(f"[{label}] Export job failed: {info}")
        elif job_code == JOB_NOT_FOUND:
            raise RuntimeError(f"[{label}] Export job not found: {job_id}")
        # JOB_NOT_INITIATED or JOB_IN_PROGRESS → keep polling
    else:
        raise TimeoutError(f"[{label}] Export job timed out after {max_wait}s")

    # ── Step 3: Download CSV ──────────────────────────────────────
    dl  = requests.get(f"{status_url}/data", headers=headers)
    dl.raise_for_status()

    # Parse CSV → list of dicts
    reader = csv.DictReader(io.StringIO(dl.text))
    rows   = list(reader)
    print(f"  [{label}] {len(rows):,} rows downloaded")
    return rows

# ── Convenience: parse Analytics datetime strings ─────────────────
def parse_dt(s):
    """
    Parse Analytics datetime formats:
      'Feb 26, 2026 10:35 AM'   (Cases Created Time)
      '21 Sep, 2023 00:00:00'   (Cases x Distribution x Notes)
      'Oct 05, 2023 02:17 PM'   (Distributions Paid Date)
    Returns datetime (UTC) or None.
    """
    if not s or not s.strip():
        return None
    for fmt in [
        "%b %d, %Y %I:%M %p",    # Feb 26, 2026 10:35 AM
        "%d %b, %Y %H:%M:%S",    # 21 Sep, 2023 00:00:00
        "%b %d, %Y",              # Feb 26, 2026
    ]:
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

def month_key(dt):
    """datetime → 'YYYY-MM' string."""
    return dt.strftime("%Y-%m") if dt else None
