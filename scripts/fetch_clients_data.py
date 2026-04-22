"""
fetch_clients_data.py
─────────────────────
Builds /data/clients.json for the Client Report dashboard.

Data pulled from Zoho CRM:
  Cases (Deals)               — last 14 months
  Distributions (Purchase_Orders) — all paid/extracted, all time

New clients    = Cases where New_or_existing = "New"
Returning      = Cases where New_or_existing = "Existing"
                 AND Stage NOT IN ongoing funding stages
Last assistance= Latest Paid_Date (Status=Paid)
                 or Extracted_Date (Status=Extracted) per client
Return gap     = Days between last paid distribution and new case
"""

import os, json
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# Add scripts dir to path so we can import zoho_client
import sys
sys.path.insert(0, os.path.dirname(__file__))
import zoho_client as zc

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Stages that represent ongoing/ILA funding — NOT genuine new returns ──
ONGOING_STAGES = {
    "Ongoing Funding",
    "Post Funding - Follow Up",
    "Post=Follow-Up",
    "Post- Follow-Up",
    "Phase 4: Monitoring & Impact",
}

# ── Date helpers ──────────────────────────────────────────────────
def cutoff_14_months():
    now = datetime.now(timezone.utc)
    m, y = now.month - 13, now.year
    while m <= 0:
        m += 12; y -= 1
    return datetime(y, m, 1, tzinfo=timezone.utc)

def last_13_months():
    now, result, seen = datetime.now(timezone.utc), [], set()
    for i in range(14):
        m, y = now.month - i, now.year
        while m <= 0:
            m += 12; y -= 1
        mk = f"{y}-{m:02d}"
        if mk not in seen:
            seen.add(mk); result.append(mk)
    result.reverse()
    return result[-13:]

def month_key(dt_str):
    return str(dt_str)[:7] if dt_str else None

def days_between(s1, s2):
    d1, d2 = zc.parse_dt(s1), zc.parse_dt(s2)
    return abs((d2 - d1).days) if d1 and d2 else None

def return_gap_band(days):
    if days is None:  return "Unknown"
    if days < 30:     return "< 1 month"
    if days < 90:     return "1–3 months"
    if days < 180:    return "3–6 months"
    if days < 365:    return "6–12 months"
    if days < 730:    return "1–2 years"
    return "2+ years"

BAND_ORDER = [
    "< 1 month","1–3 months","3–6 months",
    "6–12 months","1–2 years","2+ years","Unknown"
]

# ── Fetch Cases ───────────────────────────────────────────────────
def fetch_cases(token):
    cutoff = cutoff_14_months()
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    print(f"  Fetching Cases created since {cutoff.strftime('%Y-%m-%d')}...")

    fields = [
        "id", "Deal_Name", "Contact_Name", "Created_Time",
        "Stage", "New_or_existing", "Description",
        "Case_Notes_Summary", "CASE_ID",
    ]

    return zc.fetch(
        token        = token,
        coql_query_str = f"""
            SELECT {', '.join(fields)}
            FROM Deals
            WHERE Created_Time >= '{cutoff_str}'
        """.strip(),
        fallback_module    = "Deals",
        fallback_fields    = fields,
        fallback_cutoff_dt = cutoff,
        label              = "Cases",
    )

# ── Fetch paid Distributions ──────────────────────────────────────
def fetch_paid_distributions(token):
    """
    Fetch all Paid + Extracted distributions.

    COQL path:  Single query with WHERE Status IN ('Paid','Extracted')
                ~9 API calls for typical NZF data volume.

    Fallback:   Two separate list fetches (all distributions, no date
                filter) filtered to Paid/Extracted in Python.
                More calls, but guaranteed complete.

    We need full history (not just 14 months) because return-gap
    calculation looks back as far as the client's first ever payment.
    """
    print("  Fetching Distributions (Status: Paid or Extracted)...")

    fields = [
        "id", "Contact_Name", "Deal_Name", "Status",
        "Paid_Date", "Extracted_Date", "Created_Time",
    ]

    # COQL can filter on Status directly — huge saving vs fetching all
    records = zc.fetch(
        token          = token,
        coql_query_str = f"""
            SELECT {', '.join(fields)}
            FROM Purchase_Orders
            WHERE Status = 'Paid' OR Status = 'Extracted'
        """.strip(),
        fallback_module    = "Purchase_Orders",
        fallback_fields    = fields,
        fallback_cutoff_dt = None,     # No date cutoff — need full history
        label              = "Distributions",
        max_records        = 50000,
    )

    # If we used the fallback (list API, no filter), filter here
    paid = [r for r in records if r.get("Status") in ("Paid", "Extracted")]
    if len(paid) < len(records):
        print(f"  [Distributions] Filtered to {len(paid)} paid/extracted "
              f"(from {len(records)} total fetched)")

    return paid

# ── Build last-paid-date index per client ─────────────────────────
def build_last_paid_index(distributions):
    """
    Returns dict: contact_id → ISO string of their latest paid date.
    - Status=Paid      → uses Paid_Date
    - Status=Extracted → uses Extracted_Date
    Both fall back to Created_Time if the specific date field is empty.
    """
    index = {}
    for d in distributions:
        contact    = d.get("Contact_Name") or {}
        contact_id = contact.get("id") if isinstance(contact, dict) else None
        if not contact_id:
            continue

        paid_dt = (
            d.get("Paid_Date")      if d.get("Status") == "Paid"
            else d.get("Extracted_Date")
        ) or d.get("Created_Time")

        if not paid_dt:
            continue

        existing = index.get(contact_id)
        if not existing or paid_dt > existing:
            index[contact_id] = paid_dt

    return index

# ── Build report ──────────────────────────────────────────────────
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

            last_dt      = last_paid.get(contact_id) if contact_id else None
            case_created = case.get("Created_Time")
            gap_days     = days_between(last_dt, case_created) if last_dt else None
            band         = return_gap_band(gap_days)
            gap_bands[band] += 1

            returning_cases.append({
                "case_id":         case.get("CASE_ID", ""),
                "case_name":       case.get("Deal_Name", ""),
                "client_name":     contact_nm,
                "created":         case_created,
                "month":           mk,
                "stage":           stage,
                "description":     (case.get("Description") or "")[:500],
                "notes_summary":   (case.get("Case_Notes_Summary") or "")[:500],
                "last_paid_date":  last_dt,
                "return_gap_days": gap_days,
                "return_gap_band": band,
            })

    trend = [
        {
            "month":     m,
            "new":       new_by_month.get(m, 0),
            "returning": returning_by_month.get(m, 0),
            "total":     new_by_month.get(m, 0) + returning_by_month.get(m, 0),
        }
        for m in months
    ]

    def pct(c, p):
        return round(((c - p) / p) * 100, 1) if p else None

    new_curr = new_by_month.get(current_month, 0)
    new_prev = new_by_month.get(previous_month, 0)
    ret_curr = returning_by_month.get(current_month, 0)
    ret_prev = returning_by_month.get(previous_month, 0)

    gap_days_list = [c["return_gap_days"] for c in returning_cases
                     if c["return_gap_days"] is not None]
    avg_gap = round(sum(gap_days_list) / len(gap_days_list)) if gap_days_list else 0

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
            "new_clients_pct_change":           pct(new_curr, new_prev),
            "returning_clients_current_month":  ret_curr,
            "returning_clients_previous_month": ret_prev,
            "returning_clients_pct_change":     pct(ret_curr, ret_prev),
            "total_returning_in_period":        sum(returning_by_month.values()),
            "avg_return_gap_days":              avg_gap,
        },
        "trend":            trend,
        "gap_distribution": [
            {"band": b, "count": gap_bands[b]}
            for b in BAND_ORDER if gap_bands.get(b, 0) > 0
        ],
        "returning_cases":  sorted(
            [c for c in returning_cases if c["description"] or c["notes_summary"]],
            key=lambda x: x["created"] or "",
            reverse=True,
        )[:50],
    }

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("═" * 55)
    print("NZF — Client Report Data Refresh")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("═" * 55)

    token = zc.get_access_token()
    print("\n📥 Fetching from Zoho CRM...")
    data = build_clients_report(token)

    out = os.path.join(DATA_DIR, "clients.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    s = data["summary"]
    print(f"\n✅ clients.json written")
    print(f"   Cases fetched:          {data['meta']['record_count']}")
    print(f"   New this month:         {s['new_clients_current_month']}")
    print(f"   Returning this month:   {s['returning_clients_current_month']}")
    print(f"   Avg return gap:         {s['avg_return_gap_days']} days")
    print("═" * 55)

if __name__ == "__main__":
    main()
