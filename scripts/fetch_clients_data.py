"""
fetch_clients_data.py
─────────────────────
Pulls client report data from Zoho CRM using the standard REST API.
No COQL — works with scope: ZohoCRM.modules.ALL

Module mapping (NZF renames standard Zoho modules):
  Clients       = Contacts        (api_name: Contacts)
  Cases         = Deals           (api_name: Deals)
  Distributions = Purchase_Orders (api_name: Purchase_Orders)

New clients    = Cases where New_or_existing = "New"
Returning      = Cases where New_or_existing = "Existing"
                 EXCLUDING ongoing funding stages (ILA / recurring)
Paid dist      = Status in ("Paid", "Extracted")
Last assistance= Latest Paid_Date or Extracted_Date per client contact

Required GitHub Secrets:
  ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REFRESH_TOKEN
  ZOHO_ACCOUNTS_URL  (https://accounts.zoho.com)
  ZOHO_API_DOMAIN    (https://www.zohoapis.com)
"""

import os, json, requests, time
from datetime import datetime, timezone, timedelta
from collections import defaultdict

CLIENT_ID      = os.environ["ZOHO_CLIENT_ID"]
CLIENT_SECRET  = os.environ["ZOHO_CLIENT_SECRET"]
REFRESH_TOKEN  = os.environ["ZOHO_REFRESH_TOKEN"]
ACCOUNTS_URL   = os.environ.get("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com")
API_DOMAIN     = os.environ.get("ZOHO_API_DOMAIN",   "https://www.zohoapis.com")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Stages considered "ongoing funding" — excluded from returning client counts
ONGOING_STAGES = {
    "Ongoing Funding",
    "Post Funding - Follow Up",
    "Post=Follow-Up",
    "Post- Follow-Up",
    "Phase 4: Monitoring & Impact",
}

# ─────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────
def get_access_token():
    res = requests.post(f"{ACCOUNTS_URL}/oauth/v2/token", params={
        "refresh_token": REFRESH_TOKEN,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type":    "refresh_token",
    })
    res.raise_for_status()
    data = res.json()
    token = data.get("access_token")
    if not token:
        raise ValueError(f"Failed to get access token: {data}")
    print("✓ Access token obtained")
    return token

# ─────────────────────────────────────────────────────────────────
# Standard REST API — search with criteria (no COQL needed)
# ─────────────────────────────────────────────────────────────────
def search_records(token, module, criteria, fields, max_records=5000):
    """
    Use Zoho CRM search API with criteria string.
    e.g. criteria="(Created_Time:greater_than:2025-01-01T00:00:00+00:00)"
    Works with ZohoCRM.modules.ALL scope.
    """
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    records = []
    page    = 1

    while len(records) < max_records:
        params = {
            "criteria": criteria,
            "fields":   ",".join(fields),
            "page":     page,
            "per_page": 200,
        }
        res = requests.get(
            f"{API_DOMAIN}/crm/v3/{module}/search",
            headers=headers,
            params=params,
        )

        # 204 = no records found
        if res.status_code == 204:
            break

        if res.status_code == 429:
            # Rate limited — wait and retry
            print("  Rate limited, waiting 10s...")
            time.sleep(10)
            continue

        res.raise_for_status()
        data  = res.json()
        batch = data.get("data", [])
        if not batch:
            break

        records.extend(batch)

        if not data.get("info", {}).get("more_records", False):
            break

        page += 1
        time.sleep(0.2)   # be polite to the API

    return records


def get_all_records(token, module, fields, max_records=5000):
    """
    Fetch ALL records from a module using standard pagination.
    Used for distributions where we need everything.
    """
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    records = []
    page    = 1

    while len(records) < max_records:
        params = {
            "fields":     ",".join(fields),
            "page":       page,
            "per_page":   200,
            "sort_by":    "id",
            "sort_order": "desc",
        }
        res = requests.get(
            f"{API_DOMAIN}/crm/v3/{module}",
            headers=headers,
            params=params,
        )

        if res.status_code == 204:
            break

        if res.status_code == 429:
            print("  Rate limited, waiting 10s...")
            time.sleep(10)
            continue

        res.raise_for_status()
        data  = res.json()
        batch = data.get("data", [])
        if not batch:
            break

        records.extend(batch)

        if not data.get("info", {}).get("more_records", False):
            break

        page += 1
        time.sleep(0.2)

    return records

# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────
def month_key(dt_str):
    if not dt_str:
        return None
    return str(dt_str)[:7]   # "YYYY-MM"

def last_13_months():
    now    = datetime.now(timezone.utc)
    seen   = set()
    result = []
    for i in range(14, -1, -1):
        d  = now.replace(day=1) - timedelta(days=1)
        d  = (now.replace(day=1) - timedelta(days=30 * i))
        mk = f"{d.year}-{d.month:02d}"
        if mk not in seen:
            seen.add(mk)
            result.append(mk)
    return result[-13:]

def start_of_14_months_ago():
    now   = datetime.now(timezone.utc)
    month = now.month - 13
    year  = now.year
    while month <= 0:
        month += 12
        year  -= 1
    return f"{year}-{month:02d}-01"

def parse_dt(s):
    if not s:
        return None
    for fmt in ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None

def days_between(dt1_str, dt2_str):
    d1 = parse_dt(dt1_str)
    d2 = parse_dt(dt2_str)
    if d1 and d2:
        return abs((d2 - d1).days)
    return None

def return_gap_band(days):
    if days is None:         return "Unknown"
    if days < 30:            return "< 1 month"
    if days < 90:            return "1–3 months"
    if days < 180:           return "3–6 months"
    if days < 365:           return "6–12 months"
    if days < 730:           return "1–2 years"
    return "2+ years"

BAND_ORDER = ["< 1 month","1–3 months","3–6 months",
              "6–12 months","1–2 years","2+ years","Unknown"]

# ─────────────────────────────────────────────────────────────────
# Fetch Cases using search API (no COQL)
# ─────────────────────────────────────────────────────────────────
def fetch_cases(token):
    since = start_of_14_months_ago()
    print(f"  Fetching Cases created since {since}...")

    # Standard search criteria — supported by ZohoCRM.modules.ALL
    criteria = f"(Created_Time:greater_than:{since}T00:00:00+00:00)"

    records = search_records(
        token    = token,
        module   = "Deals",
        criteria = criteria,
        fields   = [
            "id", "Deal_Name", "Contact_Name", "Created_Time",
            "Stage", "New_or_existing", "Description",
            "Case_Notes_Summary", "CASE_ID",
        ],
    )
    print(f"  → {len(records)} cases fetched")
    return records

# ─────────────────────────────────────────────────────────────────
# Fetch paid Distributions using search API (no COQL)
# ─────────────────────────────────────────────────────────────────
def fetch_paid_distributions(token):
    print("  Fetching paid Distributions (Status: Extracted)...")

    # Fetch Extracted first
    extracted = search_records(
        token    = token,
        module   = "Purchase_Orders",
        criteria = "(Status:equals:Extracted)",
        fields   = [
            "id", "Contact_Name", "Deal_Name", "Status",
            "Paid_Date", "Extracted_Date", "Created_Time",
        ],
        max_records = 20000,
    )
    print(f"    → {len(extracted)} extracted")

    print("  Fetching paid Distributions (Status: Paid)...")
    paid = search_records(
        token    = token,
        module   = "Purchase_Orders",
        criteria = "(Status:equals:Paid)",
        fields   = [
            "id", "Contact_Name", "Deal_Name", "Status",
            "Paid_Date", "Extracted_Date", "Created_Time",
        ],
        max_records = 20000,
    )
    print(f"    → {len(paid)} paid")

    all_dists = extracted + paid
    print(f"  → {len(all_dists)} total paid distributions")
    return all_dists

# ─────────────────────────────────────────────────────────────────
# Build last-paid-date index: contact_id → latest paid date string
# ─────────────────────────────────────────────────────────────────
def build_last_paid_index(distributions):
    index = {}

    for d in distributions:
        contact    = d.get("Contact_Name") or {}
        contact_id = contact.get("id") if isinstance(contact, dict) else None
        if not contact_id:
            continue

        status = d.get("Status", "")
        if status == "Paid":
            paid_dt = d.get("Paid_Date") or d.get("Created_Time")
        elif status == "Extracted":
            paid_dt = d.get("Extracted_Date") or d.get("Created_Time")
        else:
            paid_dt = d.get("Created_Time")

        if not paid_dt:
            continue

        existing = index.get(contact_id)
        if not existing or paid_dt > existing:
            index[contact_id] = paid_dt

    return index

# ─────────────────────────────────────────────────────────────────
# Build report
# ─────────────────────────────────────────────────────────────────
def build_clients_report(token):
    cases         = fetch_cases(token)
    distributions = fetch_paid_distributions(token)
    last_paid     = build_last_paid_index(distributions)

    months         = last_13_months()
    current_month  = months[-1]
    previous_month = months[-2]

    new_by_month       = defaultdict(int)
    returning_by_month = defaultdict(int)
    returning_cases    = []
    gap_bands          = defaultdict(int)

    for case in cases:
        mk = month_key(case.get("Created_Time"))
        if not mk or mk not in months:
            continue

        status     = (case.get("New_or_existing") or "").strip()
        stage      = (case.get("Stage") or "").strip()
        contact    = case.get("Contact_Name") or {}
        contact_id = contact.get("id")   if isinstance(contact, dict) else None
        contact_nm = contact.get("name") if isinstance(contact, dict) else ""

        if status == "New":
            new_by_month[mk] += 1

        elif status == "Existing" and stage not in ONGOING_STAGES:
            returning_by_month[mk] += 1

            last_paid_dt = last_paid.get(contact_id) if contact_id else None
            case_created = case.get("Created_Time")
            gap_days     = days_between(last_paid_dt, case_created) if last_paid_dt else None
            band         = return_gap_band(gap_days)
            gap_bands[band] += 1

            returning_cases.append({
                "case_id":        case.get("CASE_ID", ""),
                "case_name":      case.get("Deal_Name", ""),
                "client_name":    contact_nm,
                "created":        case_created,
                "month":          mk,
                "stage":          stage,
                "description":    (case.get("Description") or "")[:500],
                "notes_summary":  (case.get("Case_Notes_Summary") or "")[:500],
                "last_paid_date": last_paid_dt,
                "return_gap_days":gap_days,
                "return_gap_band":band,
            })

    # Monthly trend series
    trend = []
    for m in months:
        trend.append({
            "month":     m,
            "new":       new_by_month.get(m, 0),
            "returning": returning_by_month.get(m, 0),
            "total":     new_by_month.get(m, 0) + returning_by_month.get(m, 0),
        })

    # KPI summary
    def pct_change(curr, prev):
        if not prev:
            return None
        return round(((curr - prev) / prev) * 100, 1)

    new_curr = new_by_month.get(current_month, 0)
    new_prev = new_by_month.get(previous_month, 0)
    ret_curr = returning_by_month.get(current_month, 0)
    ret_prev = returning_by_month.get(previous_month, 0)

    gap_with_days = [c for c in returning_cases if c["return_gap_days"] is not None]
    avg_gap = (
        round(sum(c["return_gap_days"] for c in gap_with_days) / len(gap_with_days))
        if gap_with_days else 0
    )

    gap_distribution = [
        {"band": b, "count": gap_bands.get(b, 0)}
        for b in BAND_ORDER
        if gap_bands.get(b, 0) > 0
    ]

    qual_sample = [
        c for c in sorted(
            returning_cases,
            key=lambda x: x["created"] or "",
            reverse=True
        )
        if c["description"] or c["notes_summary"]
    ][:50]

    return {
        "meta": {
            "last_updated":   datetime.now(timezone.utc).isoformat(),
            "record_count":   len(cases),
            "months_covered": months,
            "current_month":  current_month,
            "previous_month": previous_month,
        },
        "summary": {
            "new_clients_current_month":        new_curr,
            "new_clients_previous_month":       new_prev,
            "new_clients_pct_change":           pct_change(new_curr, new_prev),
            "returning_clients_current_month":  ret_curr,
            "returning_clients_previous_month": ret_prev,
            "returning_clients_pct_change":     pct_change(ret_curr, ret_prev),
            "total_returning_in_period":        sum(returning_by_month.values()),
            "avg_return_gap_days":              avg_gap,
        },
        "trend":            trend,
        "gap_distribution": gap_distribution,
        "returning_cases":  qual_sample,
    }

# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────
def main():
    print("═" * 55)
    print("NZF Dashboard — Client Report Data Refresh")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 55)

    token = get_access_token()

    print("\n📥 Fetching data from Zoho CRM...")
    data  = build_clients_report(token)

    path = os.path.join(DATA_DIR, "clients.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"\n✅ Written: clients.json")
    print(f"   New clients     (this month): {data['summary']['new_clients_current_month']}")
    print(f"   Returning       (this month): {data['summary']['returning_clients_current_month']}")
    print(f"   Total cases fetched:          {data['meta']['record_count']}")
    print("═" * 55)

if __name__ == "__main__":
    main()
