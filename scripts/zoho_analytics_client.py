"""
zoho_analytics_client.py
────────────────────────
Shared Zoho Analytics client. View IDs and org settings are loaded
from config/nzf_rules.json — the single source of truth for all
NZF business rules and configuration.

Endpoint:  GET /workspaces/{ws}/views/{viewId}/data?CONFIG={responseFormat:csv}
Base URL:  https://analyticsapi.zoho.com/restapi/v2
Scope:     ZohoAnalytics.data.read
"""

import os, csv, io, json, requests, urllib.parse
from datetime import datetime, timezone

# ── Load rules from central config ───────────────────────────────
_RULES_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "nzf_rules.json")
with open(_RULES_PATH) as _f:
    RULES = json.load(_f)

_modules = RULES["zoho_modules"]

ORG_ID         = RULES["_meta"]["analytics_org_id"]
WORKSPACE_ID   = RULES["_meta"]["analytics_workspace_id"]
ANALYTICS_BASE = "https://analyticsapi.zoho.com/restapi/v2"
ACCOUNTS_URL   = os.environ.get("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com")

# View IDs sourced from nzf_rules.json — change them there, not here
VIEW_CASES         = _modules["cases"]["analytics_view_id"]
VIEW_DISTRIBUTIONS = _modules["distributions"]["analytics_view_id"]
VIEW_CLIENTS       = _modules["clients"]["analytics_view_id"]
VIEW_CASE_NOTES    = _modules["notes"]["analytics_view_id"]

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

# ── Core fetch ────────────────────────────────────────────────────
def run_sql_query(token, sql, label="SQL", poll_interval=8, max_polls=20):
    """
    Execute an arbitrary SQL query against Zoho Analytics via the async export job API.
    Returns a list of dicts (column names are NOT normalised — kept as-is from CSV headers
    but stripped of the 'alias.' prefix added by the SQL engine e.g. 'd.Distribution ID'
    becomes 'dist_id' if aliased in the query, otherwise 'd.Distribution ID').

    Uses the same create → poll → download pattern as the standalone dashboard.
    """
    import time
    headers = {
        "Authorization":    f"Zoho-oauthtoken {token}",
        "ZANALYTICS-ORGID": ORG_ID,
    }
    base_ws = f"{ANALYTICS_BASE}/workspaces/{WORKSPACE_ID}"

    # Step 1 — Create export job
    config  = json.dumps({"sqlQuery": sql, "responseFormat": "csv"})
    res     = requests.post(
        f"{base_ws}/exportjobs",
        headers=headers,
        params={"CONFIG": config},
    )
    if not res.ok:
        raise RuntimeError(f"[{label}] Export job create failed {res.status_code}: {res.text[:300]}")
    job_id = res.json().get("data", {}).get("jobId")
    if not job_id:
        raise RuntimeError(f"[{label}] No jobId in response: {res.text[:200]}")
    print(f"  [{label}] Export job created: {job_id}")

    # Step 2 — Poll until complete
    for attempt in range(1, max_polls + 1):
        time.sleep(poll_interval)
        r = requests.get(f"{base_ws}/exportjobs/{job_id}", headers=headers)
        if not r.ok:
            raise RuntimeError(f"[{label}] Poll failed {r.status_code}")
        info     = r.json().get("data", {})
        job_code = info.get("jobCode")
        if job_code == 1004:   # JOB COMPLETED
            print(f"  [{label}] Job complete (attempt {attempt})")
            break
        if job_code in (1003, 1005):
            raise RuntimeError(f"[{label}] Job failed: {info}")
    else:
        raise RuntimeError(f"[{label}] Job did not complete after {max_polls} polls")

    # Step 3 — Download
    dl = requests.get(f"{base_ws}/exportjobs/{job_id}/data", headers=headers)
    if not dl.ok:
        raise RuntimeError(f"[{label}] Download failed {dl.status_code}: {dl.text[:200]}")

    # Step 4 — Parse CSV with alias-aware column names
    rows = _parse_csv(dl.text)
    print(f"  [{label}] {len(rows):,} rows returned")
    return rows


    """
    Fetch an entire Analytics view/table as a list of dicts.
    Column names are normalised: lowercase, spaces → underscores.
    """
    config = urllib.parse.quote(json.dumps({"responseFormat": "csv"}))
    url    = (f"{ANALYTICS_BASE}/workspaces/{WORKSPACE_ID}"
              f"/views/{view_id}/data?CONFIG={config}")
    headers = {
        "Authorization":    f"Zoho-oauthtoken {token}",
        "ZANALYTICS-ORGID": ORG_ID,
    }
    res = requests.get(url, headers=headers)
    if not res.ok:
        raise RuntimeError(
            f"[{label}] Analytics view {view_id} "
            f"HTTP {res.status_code}: {res.text[:300]}"
        )
    rows = _parse_csv(res.text)
    print(f"  [{label}] {len(rows):,} rows")
    return rows

# ── CSV parser ────────────────────────────────────────────────────
def _parse_csv(text):
    text   = text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    def norm(h):
        h = h.strip().lower()
        for ch in (" ", "-", ".", "(", ")", "/"):
            h = h.replace(ch, "_")
        return h
    return [{norm(k): (v or "").strip() for k, v in raw.items()} for raw in reader]

# ── Datetime helpers ──────────────────────────────────────────────
def parse_dt(s):
    """Parse datetime strings into UTC datetime.
    Handles both Analytics format ('Apr 23, 2026 02:30 PM')
    and CRM API ISO 8601 format ('2026-04-23T14:30:00+05:30').
    """
    if not s:
        return None
    s = s.strip()
    # ISO 8601 (CRM API) — try first as it's unambiguous
    if "T" in s:
        try:
            # Python 3.7+ handles timezone offsets in fromisoformat
            # Replace +05:30 style — works natively in 3.11+, needs workaround for 3.7-3.10
            from datetime import timezone as _tz
            import re as _re
            # Normalise: replace +HH:MM or -HH:MM suffix
            iso = _re.sub(r'([+-]\d{2}):(\d{2})$', r'\1\2', s)
            iso = iso.replace('Z', '+0000')
            try:
                dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S%z")
            except ValueError:
                dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S.%f%z")
            return dt.astimezone(timezone.utc).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    # Analytics formats
    for fmt in [
        "%b %d, %Y %I:%M %p",
        "%b %d, %Y %H:%M:%S",
        "%d %b, %Y %H:%M:%S",
        "%d %b, %Y %I:%M %p",
        "%b %d, %Y",
    ]:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

def month_key(dt):
    return dt.strftime("%Y-%m") if dt else None
