"""
fetch_distributions_data.py
────────────────────────────
Builds /data/distributions.json for the Distributions dashboard.

Metrics:
  - Total AUD distributed: current month, prev month, 12m total, monthly avg
  - Count of distributions: same periods
  - Avg distribution amount per case
  - Monthly trend (amount + count)
  - Breakdown by Distribution Type (Zakat / Non-Zakat)
  - Breakdown by Program (top 10 + Other)
  - Breakdown by State (normalised AU states)
  - Breakdown by Subject/Category (top 10 + Other)
  - Monthly trend by Distribution Type

Grand Total field contains "AU$ 1,000.00" — stripped and parsed as float.
"""

import os, json, sys, re
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

RULES         = zac.RULES
_dist_rules   = RULES["distributions"]
_periods      = RULES["reporting_periods"]
_au_rules     = RULES.get("client_definitions", {})

PAID_STATUSES = set(_dist_rules["paid_statuses"])
WINDOW_MONTHS = _periods["client_report_window_months"]
TREND_MONTHS  = _periods["trend_display_months"]

# AU state normalisation (reuse same logic as clients script)
_AU_STATES = {'NSW','VIC','QLD','WA','SA','TAS','ACT','NT'}
_AU_STATE_MAP = {
    'NEW SOUTH WALES':'NSW','VICTORIA':'VIC','QUEENSLAND':'QLD',
    'WESTERN AUSTRALIA':'WA','SOUTH AUSTRALIA':'SA','TASMANIA':'TAS',
    'AUSTRALIAN CAPITAL TERRITORY':'ACT','NORTHERN TERRITORY':'NT',
    'N.S.W':'NSW','N.S.W.':'NSW','V.I.C':'VIC','V.I.C.':'VIC',
    'Q.L.D':'QLD','Q.L.D.':'QLD','W.A':'WA','W.A.':'WA',
    'S.A':'SA','S.A.':'SA','T.A.S':'TAS','T.A.S.':'TAS',
    'A.C.T':'ACT','A.C.T.':'ACT','N.T':'NT','N.T.':'NT',
}
def normalise_state(raw):
    if not raw: return None
    s = raw.strip().upper()
    if s in _AU_STATES:    return s
    if s in _AU_STATE_MAP: return _AU_STATE_MAP[s]
    return None

def parse_amount(raw):
    """Strip 'AU$ ' prefix, commas, and parse to float."""
    if not raw: return 0.0
    cleaned = re.sub(r'[^\d.]', '', str(raw))
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def effective_date(row):
    """Return the effective paid date string for a distribution row."""
    status = row.get("status","").strip()
    if status == "Paid":
        d = row.get("paid_date","").strip()
    elif status == "Extracted":
        d = row.get("extracted_date","").strip()
    else:
        d = ""
    return d if d else row.get("created_time","")

def cutoff_n_months(n):
    now = datetime.now(timezone.utc)
    m, y = now.month - (n-1), now.year
    while m <= 0: m += 12; y -= 1
    return datetime(y, m, 1, tzinfo=timezone.utc)

def last_n_months(n):
    now = datetime.now(timezone.utc)
    result, seen = [], set()
    for i in range(n+1):
        m, y = now.month-i, now.year
        while m <= 0: m += 12; y -= 1
        mk = f"{y}-{m:02d}"
        if mk not in seen:
            seen.add(mk); result.append(mk)
    result.reverse()
    return result[-n:]

def top_n(counter, n=10):
    """Counter dict → top-n items + 'Other' bucket, sorted desc."""
    sorted_items = sorted(counter.items(), key=lambda x: -x[1])
    top          = sorted_items[:n]
    other_sum    = sum(v for _, v in sorted_items[n:])
    result       = [{"label": k, "value": v} for k, v in top if k]
    if other_sum > 0:
        result.append({"label": "Other", "value": other_sum})
    return result

def top_n_amount(counter, n=10):
    """Same but for amount dict."""
    sorted_items = sorted(counter.items(), key=lambda x: -x[1])
    top          = sorted_items[:n]
    other_sum    = sum(v for _, v in sorted_items[n:])
    result       = [{"label": k, "value": round(v, 2)} for k, v in top if k]
    if other_sum > 0:
        result.append({"label": "Other", "value": round(other_sum, 2)})
    return result

def pct(c, p):
    return round(((c-p)/p)*100, 1) if p else None

# ── Build report ──────────────────────────────────────────────────
def build_distributions_report(token):
    cutoff = cutoff_n_months(WINDOW_MONTHS)
    months = last_n_months(TREND_MONTHS)
    current_month  = months[-1]
    previous_month = months[-2]

    print("\n  Fetching Analytics views...")
    all_dists = zac.fetch_view(token, zac.VIEW_DISTRIBUTIONS, label="Distributions")

    # Filter to paid/extracted within window
    window_dists = []
    for d in all_dists:
        if d.get("status","").strip() not in PAID_STATUSES:
            continue
        eff_date = effective_date(d)
        dt = zac.parse_dt(eff_date)
        if dt and dt >= cutoff:
            window_dists.append((d, dt))

    print(f"  Distributions in window: {len(window_dists):,} (of {len(all_dists):,} total)")

    # ── Accumulators ──────────────────────────────────────────────
    amount_by_month  = defaultdict(float)
    count_by_month   = defaultdict(int)
    type_amount      = defaultdict(float)   # distribution_type → AUD
    type_count       = defaultdict(int)
    type_monthly     = defaultdict(lambda: defaultdict(float))  # type → month → AUD
    program_amount   = defaultdict(float)
    program_count    = defaultdict(int)
    state_amount     = defaultdict(float)
    state_count      = defaultdict(int)
    subject_amount   = defaultdict(float)
    subject_count    = defaultdict(int)
    unique_cases     = set()
    unique_clients   = set()

    for d, dt in window_dists:
        mk     = zac.month_key(dt)
        if not mk or mk not in months:
            continue

        amount = parse_amount(d.get("grand_total",""))
        dtype  = (d.get("distribution_type","") or "Unclassified").strip() or "Unclassified"
        prog   = (d.get("program","") or "No Program").strip() or "No Program"
        state  = normalise_state(d.get("billing_state",""))
        subj   = (d.get("subject","") or "").strip() or "Unspecified"
        case   = (d.get("case_name","") or "").strip()
        client = (d.get("client_name","") or "").strip()

        amount_by_month[mk]  += amount
        count_by_month[mk]   += 1
        type_amount[dtype]   += amount
        type_count[dtype]    += 1
        type_monthly[dtype][mk] += amount
        program_amount[prog] += amount
        program_count[prog]  += 1
        if state:
            state_amount[state] += amount
            state_count[state]  += 1
        subject_amount[subj] += amount
        subject_count[subj]  += 1
        if case:   unique_cases.add(case)
        if client: unique_clients.add(client)

    # ── KPIs ──────────────────────────────────────────────────────
    curr_amount  = round(amount_by_month.get(current_month, 0), 2)
    prev_amount  = round(amount_by_month.get(previous_month, 0), 2)
    total_12m    = round(sum(amount_by_month.get(m,0) for m in months), 2)
    avg_monthly  = round(total_12m / len(months), 2)
    curr_count   = count_by_month.get(current_month, 0)
    prev_count   = count_by_month.get(previous_month, 0)
    total_count  = sum(count_by_month.get(m,0) for m in months)
    avg_dist_amt = round(total_12m / total_count, 2) if total_count else 0

    # ── Trend series ──────────────────────────────────────────────
    trend = [
        {
            "month":  m,
            "amount": round(amount_by_month.get(m,0), 2),
            "count":  count_by_month.get(m,0),
        }
        for m in months
    ]

    # ── Type trend (Chart.js ready) ────────────────────────────────
    sorted_types  = sorted(type_amount.items(), key=lambda x: -x[1])
    type_colours  = ['#EE3526','#0081C6','#FDB913','#49A942','#231F1F',
                     '#0099B4','#9B2335','#E8732A','#4C6060','#B5BD00']
    type_trend_series = {
        "months": months,
        "series": [
            {
                "type":   t,
                "colour": type_colours[i % len(type_colours)],
                "data":   [round(type_monthly[t].get(m,0), 2) for m in months],
            }
            for i, (t, _) in enumerate(sorted_types)
        ]
    }

    return {
        "meta": {
            "last_updated":   datetime.now(timezone.utc).isoformat(),
            "record_count":   len(window_dists),
            "months_covered": months,
            "current_month":  current_month,
            "previous_month": previous_month,
        },
        "summary": {
            "current_month_amount":  curr_amount,
            "previous_month_amount": prev_amount,
            "amount_pct_change":     pct(curr_amount, prev_amount),
            "total_12m_amount":      total_12m,
            "monthly_avg_amount":    avg_monthly,
            "current_month_count":   curr_count,
            "previous_month_count":  prev_count,
            "count_pct_change":      pct(curr_count, prev_count),
            "total_12m_count":       total_count,
            "avg_distribution_amount": avg_dist_amt,
            "unique_cases_assisted":   len(unique_cases),
            "unique_clients_assisted": len(unique_clients),
        },
        "trend":              trend,
        "by_type": {
            "amount": top_n_amount(type_amount, n=10),
            "count":  top_n(type_count, n=10),
        },
        "type_trend_series":  type_trend_series,
        "by_program": {
            "amount": top_n_amount(program_amount, n=10),
            "count":  top_n(program_count, n=10),
        },
        "by_state": {
            "amount": sorted(
                [{"state": s, "amount": round(v,2)} for s,v in state_amount.items()],
                key=lambda x: -x["amount"]
            ),
            "count": sorted(
                [{"state": s, "count": v} for s,v in state_count.items()],
                key=lambda x: -x["count"]
            ),
        },
        "by_subject": {
            "amount": top_n_amount(subject_amount, n=10),
            "count":  top_n(subject_count, n=10),
        },
    }

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("═" * 55)
    print("NZF — Distributions Report  |  Zoho Analytics")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("═" * 55)

    token = zac.get_access_token()
    data  = build_distributions_report(token)

    out = os.path.join(DATA_DIR, "distributions.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    s = data["summary"]
    print(f"\n✅ distributions.json written")
    print(f"   Distributions in window:  {data['meta']['record_count']:,}")
    print(f"   Current month (AUD):      ${s['current_month_amount']:,.2f}")
    print(f"   Previous month (AUD):     ${s['previous_month_amount']:,.2f}")
    print(f"   12-month total (AUD):     ${s['total_12m_amount']:,.2f}")
    print(f"   Monthly avg (AUD):        ${s['monthly_avg_amount']:,.2f}")
    print(f"   Unique clients assisted:  {s['unique_clients_assisted']:,}")
    print(f"   Avg distribution amount:  ${s['avg_distribution_amount']:,.2f}")
    print("═" * 55)

if __name__ == "__main__":
    main()
