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
def fetch_view(token, view_id, label="view"):
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
    """Parse Analytics datetime strings into UTC datetime."""
    if not s:
        return None
    for fmt in [
        "%b %d, %Y %I:%M %p",
        "%b %d, %Y %H:%M:%S",
        "%d %b, %Y %H:%M:%S",
        "%d %b, %Y %I:%M %p",
        "%b %d, %Y",
    ]:
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

def month_key(dt):
    return dt.strftime("%Y-%m") if dt else None
