"""
fetch_cases_data.py
────────────────────
Builds /data/cases.json for the Cases Report dashboard.

Metrics:
  - Cases created: current month, previous month, 12-month total, monthly avg
  - Monthly trend (total)
  - Priority breakdown (P1–P5, No Priority) for each metric
  - Monthly trend by priority

Priority normalisation is loaded from config/nzf_rules.json.
Raw Case Urgency values are inconsistent strings — we normalise by
prefix matching (e.g. "Priority 3-  (4-6 days)" → "P3").
"""

import os, json, sys
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Load rules ────────────────────────────────────────────────────
RULES          = zac.RULES
_pri_rules     = RULES["case_priorities"]
_periods       = RULES["reporting_periods"]

PRIORITY_MAP   = [
    (entry["prefix"].upper(), entry["label"])
    for entry in _pri_rules["prefix_map"]
]
NO_PRIORITY    = _pri_rules["no_priority_label"]
PRIORITY_ORDER = _pri_rules["order"]
PRIORITY_COLS  = _pri_rules["colours"]
WINDOW_MONTHS  = _periods["client_report_window_months"]
TREND_MONTHS   = _periods["trend_display_months"]

# ── Priority normalisation ────────────────────────────────────────
def normalise_priority(raw):
    """
    Map raw Case Urgency string → clean priority label.
    Uses prefix matching from nzf_rules.json so new variants
    are handled automatically.
    """
    if not raw or not raw.strip():
        return NO_PRIORITY
    s = raw.strip().upper()
    for prefix, label in PRIORITY_MAP:
        if s.startswith(prefix):
            return label
    return NO_PRIORITY

# ── Date helpers ──────────────────────────────────────────────────
def cutoff_n_months(n):
    now = datetime.now(timezone.utc)
    m, y = now.month - (n - 1), now.year
    while m <= 0: m += 12; y -= 1
    return datetime(y, m, 1, tzinfo=timezone.utc)

def last_n_months(n):
    now = datetime.now(timezone.utc)
    result, seen = [], set()
    for i in range(n + 1):
        m, y = now.month - i, now.year
        while m <= 0: m += 12; y -= 1
        mk = f"{y}-{m:02d}"
        if mk not in seen:
            seen.add(mk); result.append(mk)
    result.reverse()
    return result[-n:]

# ── Build report ──────────────────────────────────────────────────
def build_cases_report(token):
    cutoff = cutoff_n_months(WINDOW_MONTHS)
    months = last_n_months(TREND_MONTHS)

    print("\n  Fetching Analytics views...")
    all_cases = zac.fetch_view(token, zac.VIEW_CASES, label="Cases")

    # Filter to window
    window_cases = []
    for c in all_cases:
        dt = zac.parse_dt(c.get("created_time", ""))
        if dt and dt >= cutoff:
            window_cases.append((c, dt))
    print(f"  Cases in window: {len(window_cases):,} (of {len(all_cases):,} total)")

    current_month  = months[-1]
    previous_month = months[-2]

    # Counters
    total_by_month    = defaultdict(int)                          # month → count
    priority_by_month = defaultdict(lambda: defaultdict(int))     # month → priority → count

    for c, dt in window_cases:
        mk       = zac.month_key(dt)
        if not mk or mk not in months:
            continue
        priority = normalise_priority(c.get("case_urgency", "") or c.get("priority", ""))
        total_by_month[mk]               += 1
        priority_by_month[mk][priority]  += 1

    # ── Monthly trend series ──────────────────────────────────────
    trend = [
        {"month": m, "count": total_by_month.get(m, 0)}
        for m in months
    ]

    # ── Priority trend series (Chart.js ready) ────────────────────
    priority_trend = {
        "months":  months,
        "series": [
            {
                "priority": p,
                "colour":   PRIORITY_COLS.get(p, "#9F9393"),
                "data":     [priority_by_month.get(m, {}).get(p, 0) for m in months],
            }
            for p in PRIORITY_ORDER
        ]
    }

    # ── KPI summaries ─────────────────────────────────────────────
    curr_total = total_by_month.get(current_month, 0)
    prev_total = total_by_month.get(previous_month, 0)
    total_12m  = sum(total_by_month.get(m, 0) for m in months)
    avg_12m    = round(total_12m / len(months), 1)

    def pct(c, p):
        return round(((c - p) / p) * 100, 1) if p else None

    # Priority KPIs — count per priority for each period
    def priority_kpis(month_key_val):
        return {
            p: priority_by_month.get(month_key_val, {}).get(p, 0)
            for p in PRIORITY_ORDER
        }

    priority_12m = {
        p: sum(priority_by_month.get(m, {}).get(p, 0) for m in months)
        for p in PRIORITY_ORDER
    }

    return {
        "meta": {
            "last_updated":   datetime.now(timezone.utc).isoformat(),
            "record_count":   len(window_cases),
            "months_covered": months,
            "current_month":  current_month,
            "previous_month": previous_month,
        },
        "summary": {
            "current_month":  curr_total,
            "previous_month": prev_total,
            "pct_change":     pct(curr_total, prev_total),
            "total_12m":      total_12m,
            "monthly_avg":    avg_12m,
        },
        "trend":   trend,
        "priority": {
            "order":          PRIORITY_ORDER,
            "colours":        PRIORITY_COLS,
            "current_month":  priority_kpis(current_month),
            "previous_month": priority_kpis(previous_month),
            "total_12m":      priority_12m,
            "monthly_avg":    {p: round(priority_12m[p] / len(months), 1) for p in PRIORITY_ORDER},
        },
        "priority_trend":  priority_trend,
    }

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("═" * 55)
    print("NZF — Cases Report  |  Zoho Analytics")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("═" * 55)

    token = zac.get_access_token()
    data  = build_cases_report(token)

    out = os.path.join(DATA_DIR, "cases.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    s = data["summary"]
    p = data["priority"]
    print(f"\n✅ cases.json written")
    print(f"   Cases in window:   {data['meta']['record_count']:,}")
    print(f"   Current month:     {s['current_month']}")
    print(f"   Previous month:    {s['previous_month']}")
    print(f"   12-month total:    {s['total_12m']:,}")
    print(f"   Monthly avg:       {s['monthly_avg']}")
    print(f"   Priority breakdown (current month):")
    for pri in data["priority"]["order"]:
        print(f"     {pri:12}: {p['current_month'].get(pri,0)}")
    print("═" * 55)

if __name__ == "__main__":
    main()
