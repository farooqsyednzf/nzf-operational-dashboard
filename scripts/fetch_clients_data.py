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

AI qualitative analysis runs server-side here using ANTHROPIC_API_KEY,
stores the result in clients.json — no API key needed in the browser.
"""

import os, json, sys, requests
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR          = os.path.join(os.path.dirname(__file__), "..", "data")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
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
    index = {}
    for d in dist_rows:
        status = d.get("status", "").strip()
        if status not in ("Paid", "Extracted"):
            continue
        client_id = d.get("client_name", "").strip()
        if not client_id:
            continue
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
    print(f"  Last-paid index: {len(index):,} clients with paid distributions")
    return index

# ── AI qualitative analysis (server-side) ─────────────────────────
def run_ai_analysis(returning_cases):
    """
    Call Anthropic API server-side to analyse why clients are returning.
    Returns a dict with 'summary' and 'themes', or None if unavailable.
    Stored in clients.json — no browser API call needed.
    """
    if not ANTHROPIC_API_KEY:
        print("  ⚠ ANTHROPIC_API_KEY not set — skipping AI analysis")
        return None

    # Build case text from descriptions
    cases_with_text = [c for c in returning_cases if c.get("description")]
    if not cases_with_text:
        print("  ⚠ No case descriptions available for AI analysis")
        return None

    case_texts = "\n\n".join(
        f"Case {i+1} (gap: {c.get('return_gap_band','unknown')}): {c['description']}"
        for i, c in enumerate(cases_with_text[:50])
    )

    prompt = f"""You are an analyst reviewing returning client cases for NZF (National Zakat Foundation), a charity providing Zakat-based financial assistance to people in need in Australia.

Below are {len(cases_with_text)} case descriptions where clients have returned for additional assistance after previously receiving help. Analyse these to understand WHY clients are returning.

Respond ONLY with a valid JSON object in this exact format (no markdown, no preamble):
{{
  "summary": "2-3 sentence overall summary of why clients are returning",
  "themes": [
    {{"label": "Theme Name", "description": "1-2 sentence description", "count_estimate": "~40% of cases"}},
    {{"label": "Theme Name", "description": "1-2 sentence description", "count_estimate": "~25% of cases"}},
    {{"label": "Theme Name", "description": "1-2 sentence description", "count_estimate": "~20% of cases"}},
    {{"label": "Theme Name", "description": "1-2 sentence description", "count_estimate": "~15% of cases"}}
  ]
}}

Case data:
{case_texts[:4000]}"""

    try:
        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 1000,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        res.raise_for_status()
        raw    = res.json()["content"][0]["text"]
        parsed = json.loads(raw.replace("```json", "").replace("```", "").strip())
        print(f"  ✓ AI analysis complete — {len(parsed.get('themes', []))} themes identified")
        return {**parsed, "generated_at": datetime.now(timezone.utc).isoformat()}

    except Exception as e:
        print(f"  ⚠ AI analysis failed: {e}")
        return None

# ── Build report ──────────────────────────────────────────────────
def build_clients_report(token):
    cutoff = cutoff_14_months()

    print("\n  Fetching Analytics views...")
    all_cases = zac.fetch_view(token, zac.VIEW_CASES, label="Cases")
    all_dists = zac.fetch_view(token, zac.VIEW_DISTRIBUTIONS, label="Distributions")

    # Filter cases to last 14 months, exclude ongoing stages
    cases = []
    for c in all_cases:
        dt = zac.parse_dt(c.get("created_time", ""))
        if not dt or dt < cutoff:
            continue
        if c.get("stage", "").strip() in ONGOING_STAGES:
            continue
        cases.append(c)
    print(f"  Cases after filtering: {len(cases):,} (from {len(all_cases):,} total)")

    last_paid = build_last_paid_index(all_dists)
    del all_dists

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

        client_id   = c.get("client_name", "").strip()
        description = c.get("description", "").strip()
        case_id     = c.get("case_id") or c.get("case-id", "")

        last_paid_dt = last_paid.get(client_id)
        is_returning = False
        if last_paid_dt:
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
                "stage":           c.get("stage", "").strip(),
                "description":     description[:500],
                "last_paid_date":  last_paid_dt,
                "return_gap_days": gap_days,
                "return_gap_band": band,
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

    # AI analysis — runs server-side, stored in JSON
    print("\n  Running AI qualitative analysis...")
    ai_analysis = run_ai_analysis(qual_sample)

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
        "ai_analysis":     ai_analysis,   # None if key not set or call failed
    }

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("═" * 55)
    print("NZF — Client Report  |  Zoho Analytics")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"AI analysis: {'enabled' if ANTHROPIC_API_KEY else 'disabled (no API key)'}")
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
    print(f"   AI analysis:            {'✓ included' if data['ai_analysis'] else '✗ not available'}")
    print("═" * 55)

if __name__ == "__main__":
    main()
