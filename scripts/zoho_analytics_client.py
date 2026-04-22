"""
zoho_analytics_client.py
────────────────────────
Shared Zoho Analytics client — confirmed working approach.

Endpoint:  GET /workspaces/{ws}/views/{viewId}/data?CONFIG={responseFormat:csv}
Base URL:  https://analyticsapi.zoho.com/restapi/v2
Scope:     ZohoAnalytics.data.read

This is the same pattern used in the NZF community map project which
runs successfully in production. It fetches entire views as CSV and
filters/joins in Python — no SQL, no async bulk-export jobs.

View IDs (from Zoho Analytics workspace 1715382000001002475):
  Cases         1715382000001002494
  Distributions 1715382000001002628
  Case Notes    1715382000012507001
"""

import os, csv, io, json, time, requests, urllib.parse
from datetime import datetime, timezone

# ── Constants ─────────────────────────────────────────────────────
ORG_ID       = "668395719"
WORKSPACE_ID = "1715382000001002475"
ANALYTICS_BASE = "https://analyticsapi.zoho.com/restapi/v2"
ACCOUNTS_URL   = os.environ.get("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com")

VIEW_CASES         = "1715382000001002494"
VIEW_DISTRIBUTIONS = "1715382000001002628"
VIEW_CASE_NOTES    = "1715382000012507001"

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
def fetch_view(token, view_id, label="view"):
    """
    Fetch an entire Analytics view/table as a list of dicts.

    Uses GET /workspaces/{ws}/views/{viewId}/data with responseFormat=csv.
    This is the confirmed-working endpoint — same as NZF map project.
    Column names are normalised to lowercase with spaces → underscores.
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
    """Parse CSV text → list of dicts with normalised column names."""
    text    = text.lstrip("\ufeff")                    # strip BOM
    reader  = csv.DictReader(io.StringIO(text))
    # Normalise headers: lowercase, spaces/hyphens/dots → underscores
    def norm(h):
        h = h.strip().lower()
        for ch in (" ", "-", ".", "(", ")", "/"):
            h = h.replace(ch, "_")
        return h
    rows = []
    for raw in reader:
        rows.append({norm(k): (v or "").strip() for k, v in raw.items()})
    return rows

# ── Datetime helpers ──────────────────────────────────────────────
def parse_dt(s):
    """
    Parse Analytics datetime strings into UTC datetime.
    Handles formats seen in the NZF Analytics workspace:
      'Feb 26, 2026 10:35 AM'
      'Mar 02, 2026 02:48 PM'
      'Oct 05, 2023 02:17 PM'
      '21 Sep, 2023 00:00:00'
    Returns None if unparseable.
    """
    if not s:
        return None
    for fmt in [
        "%b %d, %Y %I:%M %p",    # Feb 26, 2026 10:35 AM
        "%b %d, %Y %H:%M:%S",    # Feb 26, 2026 00:00:00
        "%d %b, %Y %H:%M:%S",    # 21 Sep, 2023 00:00:00
        "%d %b, %Y %I:%M %p",    # 21 Sep, 2023 02:17 PM
        "%b %d, %Y",              # Feb 26, 2026
    ]:
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

def month_key(dt):
    return dt.strftime("%Y-%m") if dt else None
