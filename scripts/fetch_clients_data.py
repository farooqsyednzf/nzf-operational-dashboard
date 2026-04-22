"""
fetch_clients_data.py
─────────────────────
Pulls client report data from Zoho CRM and writes /data/clients.json.

Module mapping (Zoho renames standard modules):
  Clients      = Contacts   (api_name: Contacts)
  Cases        = Deals      (api_name: Deals)
  Distributions= Purchase_Orders (api_name: Purchase_Orders)

New clients    = Cases where New_or_existing = "New"
Returning      = Cases where New_or_existing = "Existing"
                 EXCLUDING cases in Ongoing Funding stages (ILA / recurring)
Paid dist      = Status in ("Paid", "Extracted")
Last assistance= Latest Paid_Date or Extracted_Date per client contact

Required GitHub Secrets: same as fetch_zoho_data.py
"""

import os, json, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

CLIENT_ID      = os.environ["ZOHO_CLIENT_ID"]
CLIENT_SECRET  = os.environ["ZOHO_CLIENT_SECRET"]
REFRESH_TOKEN  = os.environ["ZOHO_REFRESH_TOKEN"]
ACCOUNTS_URL   = os.environ.get("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com")
API_DOMAIN     = os.environ.get("ZOHO_API_DOMAIN",   "https://www.zohoapis.com")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Stages considered "ongoing funding" (not a genuine new return visit) ──────
ONGOING_STAGES = {
    "Ongoing Funding",
    "Post Funding - Follow Up",
    "Post=Follow-Up",
    "Post- Follow-Up",
    "Phase 4: Monitoring & Impact",
}

# ─────────────────────────────────────────────────────────────────────────────
# Auth
# ─────────────────────────────────────────────────────────────────────────────
def get_access_token():
    res = requests.post(f"{ACCOUNTS_URL}/oauth/v2/token", params={
        "refresh_token": REFRESH_TOKEN,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type":    "refresh_token",
    })
    res.raise_for_status()
    token = res.json().get("access_token")
    if not token:
        raise ValueError(f"Failed to get access token: {res.json()}")
    print("✓ Access token obtained")
    return token

# ─────────────────────────────────────────────────────────────────────────────
# COQL query (paginated) — more efficient than listing all records
# ─────────────────────────────────────────────────────────────────────────────
def coql_query(token, query, max_records=10000):
    """Execute a COQL SELECT query and return all matching records."""
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Content-Type":  "application/json",
    }
    records = []
    offset  = 0
    limit   = 200  # Zoho COQL max per page

    while len(records) < max_records:
        paginated = f"{query} LIMIT {limit} OFFSET {offset}"
        res = requests.post(
            f"{API_DOMAIN}/crm/v7/coql",
            headers=headers,
            json={"select_query": paginated},
        )
        res.raise_for_status()
        data = res.json()
        batch = data.get("data", [])
        if not batch:
            break
        records.extend(batch)
        if not data.get("info", {}).get("more_records", False):
            break
        offset += limit

    return records

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def month_key(dt_str):
    """Return 'YYYY-MM' from an ISO datetime string."""
    if not dt_str:
        return None
    return str(dt_str)[:7]

def last_12_months():
    """Return list of 'YYYY-MM' strings for the last 12 completed months + current."""
    now = datetime.now(timezone.utc)
    months = []
    for i in range(13, -1, -1):
        d = now - timedelta(days=30 * i)
        months.append(f"{d.year}-{d.month:02d}")
    # Deduplicate while preserving order
    seen = set()
    result = []
    for m in months:
        if m not in seen:
            seen.add(m)
            result.append(m)
    return result[-13:]  # Last 13 months

def start_of_14_months_ago():
    """ISO date string for the 1st of the month 14 months ago (query window)."""
    now = datetime.now(timezone.utc)
    month = now.month - 13
    year  = now.year
    while month <= 0:
        month += 12
        year  -= 1
    return f"{year}-{month:02d}-01"

# ─────────────────────────────────────────────────────────────────────────────
# Fetch Cases
# ─────────────────────────────────────────────────────────────────────────────
def fetch_cases(token):
    since = start_of_14_months_ago()
    print(f"  Fetching Cases created since {since}...")
    query = f"""
        SELECT id, Deal_Name, Contact_Name, Created_Time, Stage,
               New_or_existing, Description, Case_Notes_Summary, CASE_ID
        FROM Deals
        WHERE Created_Time >= '{since}T00:00:00+00:00'
    """
    records = coql_query(token, query.strip())
    print(f"  → {len(records)} cases fetched")
    return records

# ─────────────────────────────────────────────────────────────────────────────
# Fetch paid Distributions (all time — needed to calc return gap)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_paid_distributions(token):
    print("  Fetching paid Distributions (Status: Paid / Extracted)...")
    query = """
        SELECT id, Contact_Name, Deal_Name, Status,
               Paid_Date, Extracted_Date, Created_Time, Grand_Total
        FROM Purchase_Orders
        WHERE Status = 'Paid' OR Status = 'Extracted'
    """
    records = coql_query(token, query.strip(), max_records=50000)
    print(f"  → {len(records)} paid distributions fetched")
    return records

# ─────────────────────────────────────────────────────────────────────────────
# Build last-paid-date index: contact_id → latest paid/extracted date
# ─────────────────────────────────────────────────────────────────────────────
def build_last_paid_index(distributions):
    """
    For each client (contact), find the latest date a distribution was paid.
    Uses Paid_Date if status=Paid, Extracted_Date if status=Extracted,
    falls back to Created_Time.
    Returns dict: contact_id → ISO datetime string
    """
    index = {}  # contact_id -> latest paid date string

    for d in distributions:
        contact = d.get("Contact_Name") or {}
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

# ─────────────────────────────────────────────────────────────────────────────
# Calculate return gap in days
# ─────────────────────────────────────────────────────────────────────────────
def days_between(dt1_str, dt2_str):
    """Return integer days between two ISO datetime strings, or None."""
    try:
        fmt = "%Y-%m-%dT%H:%M:%S%z"
        # Try with microseconds
        def parse(s):
            for fmt in ["%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d"]:
                try:
                    dt = datetime.strptime(s, fmt)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    continue
            return None
        d1 = parse(dt1_str)
        d2 = parse(dt2_str)
        if d1 and d2:
            return abs((d2 - d1).days)
    except Exception:
        pass
    return None

# ─────────────────────────────────────────────────────────────────────────────
# Bucket return gaps into human-readable bands
# ─────────────────────────────────────────────────────────────────────────────
def return_gap_band(days):
    if days is None:
        return "Unknown"
    if days < 30:
        return "< 1 month"
    elif days < 90:
        return "1–3 months"
    elif days < 180:
        return "3–6 months"
    elif days < 365:
        return "6–12 months"
    elif days < 730:
        return "1–2 years"
    else:
        return "2+ years"

BAND_ORDER = ["< 1 month","1–3 months","3–6 months","6–12 months","1–2 years","2+ years","Unknown"]

# ─────────────────────────────────────────────────────────────────────────────
# Main build
# ─────────────────────────────────────────────────────────────────────────────
def build_clients_report(token):
    cases         = fetch_cases(token)
    distributions = fetch_paid_distributions(token)
    last_paid     = build_last_paid_index(distributions)

    months         = last_12_months()
    current_month  = months[-1]
    previous_month = months[-2]

    new_by_month      = defaultdict(int)   # month_key -> count
    returning_by_month= defaultdict(int)
    returning_cases   = []                 # full detail for qualitative analysis
    gap_bands         = defaultdict(int)   # band label -> count

    for case in cases:
        mk = month_key(case.get("Created_Time"))
        if not mk or mk not in months:
            continue

        status    = (case.get("New_or_existing") or "").strip()
        stage     = (case.get("Stage") or "").strip()
        contact   = case.get("Contact_Name") or {}
        contact_id= contact.get("id") if isinstance(contact, dict) else None
        contact_nm= contact.get("name") if isinstance(contact, dict) else ""

        if status == "New":
            new_by_month[mk] += 1

        elif status == "Existing" and stage not in ONGOING_STAGES:
            returning_by_month[mk] += 1

            # Calculate return gap
            last_paid_dt  = last_paid.get(contact_id) if contact_id else None
            case_created  = case.get("Created_Time")
            gap_days      = days_between(last_paid_dt, case_created) if last_paid_dt else None
            band          = return_gap_band(gap_days)
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

    # ── Monthly trend series ─────────────────────────────────────────────────
    trend = []
    for m in months:
        trend.append({
            "month":     m,
            "new":       new_by_month.get(m, 0),
            "returning": returning_by_month.get(m, 0),
            "total":     new_by_month.get(m, 0) + returning_by_month.get(m, 0),
        })

    # ── KPI summary ──────────────────────────────────────────────────────────
    def pct_change(curr, prev):
        if prev == 0:
            return None
        return round(((curr - prev) / prev) * 100, 1)

    new_curr  = new_by_month.get(current_month, 0)
    new_prev  = new_by_month.get(previous_month, 0)
    ret_curr  = returning_by_month.get(current_month, 0)
    ret_prev  = returning_by_month.get(previous_month, 0)

    # ── Gap band distribution ────────────────────────────────────────────────
    gap_distribution = [
        {"band": b, "count": gap_bands.get(b, 0)}
        for b in BAND_ORDER
        if gap_bands.get(b, 0) > 0
    ]

    # ── Qualitative sample (most recent 50 returning cases with descriptions) ─
    qual_sample = [
        c for c in sorted(returning_cases, key=lambda x: x["created"] or "", reverse=True)
        if c["description"] or c["notes_summary"]
    ][:50]

    return {
        "meta": {
            "last_updated":    datetime.now(timezone.utc).isoformat(),
            "record_count":    len(cases),
            "months_covered":  months,
            "current_month":   current_month,
            "previous_month":  previous_month,
        },
        "summary": {
            "new_clients_current_month":     new_curr,
            "new_clients_previous_month":    new_prev,
            "new_clients_pct_change":        pct_change(new_curr, new_prev),
            "returning_clients_current_month":  ret_curr,
            "returning_clients_previous_month": ret_prev,
            "returning_clients_pct_change":     pct_change(ret_curr, ret_prev),
            "total_returning_in_period":        sum(returning_by_month.values()),
            "avg_return_gap_days": (
                round(sum(c["return_gap_days"] for c in returning_cases
                          if c["return_gap_days"] is not None) /
                      max(1, sum(1 for c in returning_cases
                                 if c["return_gap_days"] is not None)), 0)
                if returning_cases else 0
            ),
        },
        "trend":            trend,
        "gap_distribution": gap_distribution,
        "returning_cases":  qual_sample,
    }

# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("═" * 55)
    print("NZF Dashboard — Client Report Data Refresh")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 55)

    token  = get_access_token()
    print("\n📥 Fetching data from Zoho CRM...")
    data   = build_clients_report(token)

    path = os.path.join(DATA_DIR, "clients.json")
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"\n✅ Written: clients.json")
    print(f"   New clients  (current month): {data['summary']['new_clients_current_month']}")
    print(f"   Returning    (current month): {data['summary']['returning_clients_current_month']}")
    print("═" * 55)

if __name__ == "__main__":
    main()
