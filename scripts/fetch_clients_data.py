"""
fetch_clients_data.py
─────────────────────
Builds /data/clients.json for the Client Report dashboard.

Definitions
───────────
New client
  A client whose case in the reporting window is their FIRST EVER case.
  Determined by checking all cases across all time (not just 14 months).
  The case's Created Time determines which month the new client is attributed to.

Returning client
  A client who has at least one prior case AND the new case is a genuine
  new application — not a continuation of the same assistance instance.

Same instance (excluded from returning count)
  Cases that are continuations of previous assistance rather than new
  applications. Detected by:
    1. Stage is in the known ongoing-funding stage list.
    2. Description contains keywords suggesting linkage / continuation
       (ILA payments, re-opened cases, follow-up installments, etc.).
  These are excluded from both new and returning counts.
"""

import os, json, sys, requests
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR          = os.path.join(os.path.dirname(__file__), "..", "data")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Stages that indicate ongoing / linked assistance ──────────────
ONGOING_STAGES = {
    "Ongoing Funding",
    "Post Funding - Follow Up",
    "Post=Follow-Up",
    "Post- Follow-Up",
    "Phase 4: Monitoring & Impact",
}

# ── Keywords in description suggesting same-instance continuation ──
# These indicate the case is linked to a previous one, not a new
# application — ILA instalments, re-opened cases, follow-up payments.
SAME_INSTANCE_KEYWORDS = [
    "ila payment",
    "ila instalment",
    "ila installment",
    "interest-free loan",
    "interest free loan",
    "ila repayment",
    "ongoing ila",
    "continuation of",
    "continuation of previous",
    "continuing support",
    "linked to previous case",
    "linked to case",
    "linked to prior case",
    "same case",
    "re-open",
    "reopen",
    "reopened",
    "re-opened",
    "reopening of",
    "same application",
    "follow-up payment",
    "follow up payment",
    "follow-up instalment",
    "follow up instalment",
    "second instalment",
    "third instalment",
    "second payment",
    "third payment",
    "instalment of previous",
]

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

# ── Same-instance detection ───────────────────────────────────────
def is_same_instance(stage, description):
    """
    Returns True if this case is a continuation of a previous assistance
    instance rather than a genuine new application.

    Checks:
      1. Stage is in the known ongoing-stage list.
      2. Description contains keywords indicating linkage / continuation.
    """
    if stage in ONGOING_STAGES:
        return True
    desc_lower = (description or "").lower()
    return any(kw in desc_lower for kw in SAME_INSTANCE_KEYWORDS)

# ── Build case history index for all clients ──────────────────────
def build_client_case_history(all_cases):
    """
    Build a dict: client_id → sorted list of case Created Time datetimes.

    Uses ALL cases (not just the 14-month window) so we can determine
    whether a case in the reporting window is the client's first ever.
    """
    history = defaultdict(list)
    for c in all_cases:
        client_id = c.get("client_name", "").strip()
        dt        = zac.parse_dt(c.get("created_time", ""))
        if client_id and dt:
            history[client_id].append(dt)

    # Sort each client's cases chronologically
    for client_id in history:
        history[client_id].sort()

    return history

# ── Build last-paid-date index (for return gap calculation) ────────
def build_last_paid_index(dist_rows):
    """
    contact_id → latest effective paid date.
    Used for calculating how long since last assistance.
    """
    index = {}
    for d in dist_rows:
        status    = d.get("status", "").strip()
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
    Calls Anthropic API to analyse why clients are returning.
    Runs server-side during GitHub Actions refresh — result stored in JSON.
    Returns None if ANTHROPIC_API_KEY not set or call fails.
    """
    if not ANTHROPIC_API_KEY:
        print("  ⚠ ANTHROPIC_API_KEY not set — skipping AI analysis")
        return None

    cases_with_text = [c for c in returning_cases if c.get("description")]
    if not cases_with_text:
        print("  ⚠ No case descriptions available for AI analysis")
        return None

    case_texts = "\n\n".join(
        f"Case {i+1} (gap: {c.get('return_gap_band','unknown')}): {c['description']}"
        for i, c in enumerate(cases_with_text[:50])
    )

    prompt = f"""You are an analyst reviewing returning client cases for NZF (National Zakat Foundation), a charity providing Zakat-based financial assistance in Australia.

Below are {len(cases_with_text)} case descriptions where clients have returned for assistance after previously receiving help. Note: same-instance cases (ILA instalments, re-opened cases, follow-up payments) have already been filtered out — these are all genuine new applications from returning clients.

Analyse these to understand WHY clients are genuinely returning.

Respond ONLY with valid JSON (no markdown, no preamble):
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
    all_cases = zac.fetch_view(token, zac.VIEW_CASES,         label="Cases")
    all_dists = zac.fetch_view(token, zac.VIEW_DISTRIBUTIONS, label="Distributions")

    # Build full case history for ALL clients (used to determine first-ever case)
    case_history = build_client_case_history(all_cases)

    # Build last-paid index for return gap calculation
    last_paid = build_last_paid_index(all_dists)
    del all_dists

    # Filter cases to the 14-month reporting window
    window_cases = []
    for c in all_cases:
        dt = zac.parse_dt(c.get("created_time", ""))
        if dt and dt >= cutoff:
            window_cases.append(c)

    print(f"  Cases in reporting window: {len(window_cases):,} "
          f"(of {len(all_cases):,} total)")

    months         = last_13_months()
    current_month  = months[-1]
    previous_month = months[-2]

    new_by_month       = defaultdict(int)
    returning_by_month = defaultdict(int)
    same_instance_count= 0
    returning_cases    = []
    gap_bands          = defaultdict(int)

    for c in window_cases:
        created_dt  = zac.parse_dt(c.get("created_time", ""))
        mk          = zac.month_key(created_dt)
        if not mk or mk not in months:
            continue

        client_id   = c.get("client_name", "").strip()
        stage       = c.get("stage", "").strip()
        description = c.get("description", "").strip()
        case_id     = c.get("case_id") or c.get("case-id", "")

        # ── Same-instance check ───────────────────────────────────
        # Exclude continuations (ILA payments, re-opened cases, etc.)
        if is_same_instance(stage, description):
            same_instance_count += 1
            continue

        # ── New vs returning ──────────────────────────────────────
        # New    = this is the client's first ever case across all time
        # Return = client has at least one prior case before this one
        client_all_case_dates = case_history.get(client_id, [])
        is_first_ever_case    = (
            not client_all_case_dates                         # no history found
            or client_all_case_dates[0] == created_dt        # this IS the earliest
        )

        if is_first_ever_case:
            # ── New client ────────────────────────────────────────
            new_by_month[mk] += 1

        else:
            # ── Returning client ──────────────────────────────────
            returning_by_month[mk] += 1

            # Time since last paid distribution
            last_paid_dt = last_paid.get(client_id)
            gap_days     = days_between(last_paid_dt, c.get("created_time")) \
                           if last_paid_dt else None
            band         = return_gap_band(gap_days)
            gap_bands[band] += 1

            # Days since their previous case
            prior_dates      = [d for d in client_all_case_dates if d < created_dt]
            last_case_dt     = max(prior_dates) if prior_dates else None
            days_since_case  = abs((created_dt - last_case_dt).days) \
                               if last_case_dt else None

            returning_cases.append({
                "case_id":              case_id,
                "client_id":            client_id,
                "created":              c.get("created_time", ""),
                "month":                mk,
                "stage":                stage,
                "description":          description[:500],
                "last_paid_date":       last_paid_dt,
                "return_gap_days":      gap_days,
                "return_gap_band":      band,
                "days_since_last_case": days_since_case,
                "prior_case_count":     len(prior_dates),
            })

    print(f"  Same-instance cases excluded: {same_instance_count:,}")
    print(f"  New clients (in window):      {sum(new_by_month.values()):,}")
    print(f"  Returning clients (in window):{sum(returning_by_month.values()):,}")

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

    print("\n  Running AI qualitative analysis...")
    ai_analysis = run_ai_analysis(qual_sample)

    return {
        "meta": {
            "last_updated":             datetime.now(timezone.utc).isoformat(),
            "record_count":             len(window_cases),
            "months_covered":           months,
            "current_month":            current_month,
            "previous_month":           previous_month,
            "same_instance_excluded":   same_instance_count,
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
        "ai_analysis":     ai_analysis,
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
    print(f"   Cases in window:         {data['meta']['record_count']:,}")
    print(f"   Same-instance excluded:  {data['meta']['same_instance_excluded']:,}")
    print(f"   New this month:          {s['new_clients_current_month']}")
    print(f"   Returning this month:    {s['returning_clients_current_month']}")
    print(f"   Avg return gap:          {s['avg_return_gap_days']} days")
    print(f"   AI analysis:             {'✓ included' if data['ai_analysis'] else '✗ not available'}")
    print("═" * 55)

if __name__ == "__main__":
    main()
