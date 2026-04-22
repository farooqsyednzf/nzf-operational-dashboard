"""
fetch_clients_data.py
─────────────────────
Builds /data/clients.json for the Client Report dashboard.

Fetches two full Analytics views, filters and joins in Python:
  Cases         → filter to last 14 months, exclude ongoing stages
  Distributions → filter to Paid/Extracted, build last-paid-date index

New vs Returning is determined by distribution history:
  New       = client has no paid/extracted distribution before this case
  Returning = client has at least one prior paid/extracted distribution
"""

import os, json, sys
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

ONGOING_STAGES = {
    "Ongoing Funding",
    "Post Funding - Follow Up",
    "Post=Follow-Up",
    "Post- Follow-Up",
    "Phase 4: Monitoring & Impact",
}

BAND_ORDER = [
    "< 1 month", "1–3 months", "3–6 months",
    "6–12 months", "1–2 years", "2+ years", "Unknown",
]

# ── Helpers ───────────────────────────────────────────────────────
def cutoff_14_months():
    n = datetime.now(timezone.utc)
    m, y = n.month - 13, n.year
    while m <= 0: m += 12; y -= 1
    return datetime(y, m, 1, tzinfo=timezone.utc)

def last_13_months():
    n = datetime.now(timezone.utc)
    result, seen = [], set()
    for i in range(14):
        m, y = n.month - i, n.year
        while m <= 0: m += 12; y -= 1
        mk = f"{y}-{m:02d}"
        if mk not in seen:
            seen.add(mk); result.append(mk)
    result.reverse()
    return result[-13:]

def days_between(s1, s2):
    d1, d2 = zac.parse_dt(s1), zac.parse_dt(s2)
    return abs((d2 - d1).days) if d1 and d2 else None

def return_gap_band(days):
    if days is None:  return "Unknown"
    if days < 30:     return "< 1 month"
    if days < 90:     return "1–3 months"
    if days < 180:    return "3–6 months"
    if days < 365:    return "6–12 months"
    if days < 730:    return "1–2 years"
    return "2+ years"

# ── Build last-paid-date index ─────────────────────────────────────
def build_last_paid_index(dist_rows):
    """
    Returns dict: client_id → latest effective paid date string.
    effective_paid_date = paid_date or extracted_date or created_time
    (handles old records where date fields were blank)
    """
    index = {}
    for d in dist_rows:
        status = d.get("status", "").strip()
        if status not in ("Paid", "Extracted"):
            continue

        client_id = d.get("client_name", "").strip()
        if not client_id:
            continue

        # Effective paid date: prefer specific date, fall back to created_time
        paid_dt = (
            d.get("paid_date") if status == "Paid"
            else d.get("extracted_date")
        )
        if not paid_dt or not paid_dt.strip():
            paid_dt = d.get("created_time", "")

        if not paid_dt:
            continue

        existing = index.get(client_id)
        if not existing or paid_dt > existing:
            index[client_id] = paid_dt

    print(f"  Last-paid index built: {len(index):,} clients with paid distributions")
    return index

# ── Build report ──────────────────────────────────────────────────
def build_clients_report(token):
    cutoff = cutoff_14_months()

    # Fetch full views
    print("\n  Fetching Analytics views...")
    all_cases = zac.fetch_view(token, zac.VIEW_CASES, label="Cases")
    all_dists = zac.fetch_view(token, zac.VIEW_DISTRIBUTIONS, label="Distributions")

    # Filter cases to last 14 months, exclude ongoing stages
    cases = []
    for c in all_cases:
        dt = zac.parse_dt(c.get("created_time", ""))
        if not dt or dt < cutoff:
            continue
        stage = c.get("stage", "").strip()
        if stage in ONGOING_STAGES:
            continue
        cases.append(c)

    print(f"  Cases after filtering: {len(cases):,} "
          f"(from {len(all_cases):,} total)")

    # Build last-paid index from distributions
    last_paid = build_last_paid_index(all_dists)
    del all_dists   # free memory

    months         = last_13_months()
    current_month  = months[-1]
    previous_month = months[-2]

    new_by_month       = defaultdict(int)
    returning_by_month = defaultdict(int)
    returning_cases    = []
    gap_bands          = defaultdict(int)

    for c in cases:
        created_dt = zac.parse_dt(c.get("created_time", ""))
        mk         = zac.month_key(created_dt)
        if not mk or mk not in months:
            continue

        client_id    = c.get("client_name", "").strip()
        stage        = c.get("stage", "").strip()
        description  = c.get("description", "").strip()
        case_id      = c.get("case_id") or c.get("case-id", "")

        # Determine new vs returning from distribution history
        last_paid_dt  = last_paid.get(client_id)
        is_returning  = False

        if last_paid_dt:
            # Only count as returning if last payment was BEFORE this case
            lp_dt = zac.parse_dt(last_paid_dt)
            if lp_dt and created_dt and lp_dt < created_dt:
                is_returning = True

        if is_returning:
            returning_by_month[mk] += 1

            gap_days = days_between(last_paid_dt, c.get("created_time"))
            band     = return_gap_band(gap_days)
            gap_bands[band] += 1

            returning_cases.append({
                "case_id":         case_id,
                "client_id":       client_id,
                "created":         c.get("created_time", ""),
                "month":           mk,
                "stage":           stage,
                "description":     description[:500],
                "last_paid_date":  last_paid_dt,
                "return_gap_days": gap_days,
                "return_gap_band": band,
                "notes_summary":   "",  # populated below if notes available
            })
        else:
            new_by_month[mk] += 1

    # Trend series
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

    qual_sample = sorted(
        [c for c in returning_cases if c.get("description")],
        key=lambda x: x["created"] or "",
        reverse=True,
    )[:50]

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
        "trend":  trend,
        "gap_distribution": [
            {"band": b, "count": gap_bands[b]}
            for b in BAND_ORDER if gap_bands.get(b, 0) > 0
        ],
        "returning_cases": qual_sample,
    }

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("═" * 55)
    print("NZF — Client Report  |  Zoho Analytics")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("═" * 55)

    token = zac.get_access_token()
    data  = build_clients_report(token)

    out = os.path.join(DATA_DIR, "clients.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    s = data["summary"]
    print(f"\n✅ clients.json written")
    print(f"   Cases in window:        {data['meta']['record_count']:,}")
    print(f"   New this month:         {s['new_clients_current_month']}")
    print(f"   Returning this month:   {s['returning_clients_current_month']}")
    print(f"   Avg return gap:         {s['avg_return_gap_days']} days")
    print("═" * 55)

if __name__ == "__main__":
    main()
