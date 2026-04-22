"""
fetch_clients_data.py
─────────────────────
Builds /data/clients.json for the Client Report dashboard.

All business rules loaded from config/nzf_rules.json.

Collects:
  - New / returning client counts by month
  - Return gap distribution
  - Clients by state (current month, previous month, last 12 months)
  - AI qualitative analysis of returning case descriptions
"""

import os, json, sys, requests
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR          = os.path.join(os.path.dirname(__file__), "..", "data")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Load all business rules from central config ───────────────────
RULES = zac.RULES

_client_rules = RULES["client_definitions"]
_dist_rules   = RULES["distributions"]
_periods      = RULES["reporting_periods"]

ONGOING_STAGES         = set(_client_rules["same_instance_exclusions"]["stage_exclusions"])
SAME_INSTANCE_KEYWORDS = _client_rules["same_instance_exclusions"]["description_keywords"]
PAID_STATUSES          = set(_dist_rules["paid_statuses"])
WINDOW_MONTHS          = _periods["client_report_window_months"]
TREND_MONTHS           = _periods["trend_display_months"]

_gap_bands_cfg = _dist_rules["return_gap_bands"]["bands"]
BAND_ORDER     = [b["label"] for b in _gap_bands_cfg] + ["Unknown"]

# ── Helpers ───────────────────────────────────────────────────────
def return_gap_band(days):
    if days is None: return "Unknown"
    for band in _gap_bands_cfg:
        lo, hi = band["days_from"], band["days_to"]
        if hi is None and days >= lo:       return band["label"]
        if hi is not None and lo <= days <= hi: return band["label"]
    return "Unknown"

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

def days_between(s1, s2):
    d1, d2 = zac.parse_dt(s1), zac.parse_dt(s2)
    return abs((d2 - d1).days) if d1 and d2 else None

# ── Same-instance detection ───────────────────────────────────────
def is_same_instance(stage, description):
    if stage in ONGOING_STAGES:
        return True
    desc_lower = (description or "").lower()
    return any(kw in desc_lower for kw in SAME_INSTANCE_KEYWORDS)

# ── Australian state validation ────────────────────────────────────
_AU_STATES = {'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'ACT', 'NT'}

_AU_STATE_MAP = {
    'NEW SOUTH WALES': 'NSW', 'VICTORIA': 'VIC', 'QUEENSLAND': 'QLD',
    'WESTERN AUSTRALIA': 'WA', 'SOUTH AUSTRALIA': 'SA', 'TASMANIA': 'TAS',
    'AUSTRALIAN CAPITAL TERRITORY': 'ACT', 'NORTHERN TERRITORY': 'NT',
    'N.S.W': 'NSW', 'N.S.W.': 'NSW', 'V.I.C': 'VIC', 'V.I.C.': 'VIC',
    'Q.L.D': 'QLD', 'Q.L.D.': 'QLD', 'W.A': 'WA', 'W.A.': 'WA',
    'S.A': 'SA', 'S.A.': 'SA', 'T.A.S': 'TAS', 'T.A.S.': 'TAS',
    'A.C.T': 'ACT', 'A.C.T.': 'ACT', 'N.T': 'NT', 'N.T.': 'NT',
}

def _normalise_state(raw):
    """Normalise to standard AU state code or 'Other'."""
    if not raw:
        return 'Other'
    s = raw.strip().upper()
    if s in _AU_STATES:    return s
    if s in _AU_STATE_MAP: return _AU_STATE_MAP[s]
    return 'Other'

# ── Client state index ─────────────────────────────────────────────
def build_client_state_index(client_rows):
    """client_id → validated Australian state code (or 'Other')."""
    index = {}
    for c in client_rows:
        client_id = c.get("id", "").strip()
        raw_state = c.get("mailing_state") or c.get("state", "")
        state     = _normalise_state(raw_state)
        if client_id:
            index[client_id] = state
    print(f"  State index: {len(index):,} clients with state data")
    return index

# ── Client case history — full detail index ───────────────────────
def build_client_case_history(all_cases):
    """
    client_id → sorted list of case dicts, chronological.
    Each entry: {dt, case_id, stage, description, created_time}
    Used to determine first-ever case AND to look up previous case details.
    """
    history = defaultdict(list)
    for c in all_cases:
        client_id = c.get("client_name", "").strip()
        dt        = zac.parse_dt(c.get("created_time", ""))
        if client_id and dt:
            history[client_id].append({
                "dt":           dt,
                "case_id":      c.get("case_id") or c.get("case-id", ""),
                "stage":        c.get("stage", "").strip(),
                "description":  (c.get("description") or "").strip()[:300],
                "created_time": c.get("created_time", ""),
            })
    for cid in history:
        history[cid].sort(key=lambda x: x["dt"])
    return history

# ── Last-paid-date index ───────────────────────────────────────────
def build_last_paid_index(dist_rows):
    index = {}
    for d in dist_rows:
        status    = d.get("status", "").strip()
        if status not in PAID_STATUSES: continue
        client_id = d.get("client_name", "").strip()
        if not client_id: continue
        paid_dt = (
            d.get("paid_date") if status == "Paid" else d.get("extracted_date")
        )
        if not paid_dt or not paid_dt.strip():
            paid_dt = d.get("created_time", "")
        if not paid_dt: continue
        existing = index.get(client_id)
        if not existing or paid_dt > existing:
            index[client_id] = paid_dt
    print(f"  Last-paid index: {len(index):,} clients")
    return index

# ── State distribution helper ──────────────────────────────────────
def state_counts_sorted(counter):
    """Counter dict → sorted list of {state, count} dicts, descending. Excludes Other."""
    return sorted(
        [{"state": s, "count": n} for s, n in counter.items() if s and s != "Other"],
        key=lambda x: -x["count"]
    )

# ── AI qualitative analysis ────────────────────────────────────────
def run_ai_analysis(returning_cases):
    if not ANTHROPIC_API_KEY:
        print("  ⚠ ANTHROPIC_API_KEY not set — skipping AI analysis")
        return None
    cases_with_text = [c for c in returning_cases if c.get("description")]
    if not cases_with_text:
        print("  ⚠ No case descriptions for AI analysis")
        return None
    case_texts = "\n\n".join(
        f"Case {i+1} (gap: {c.get('return_gap_band','unknown')}): {c['description']}"
        for i, c in enumerate(cases_with_text[:50])
    )
    prompt = f"""You are an analyst reviewing returning client cases for NZF (National Zakat Foundation), a charity providing Zakat-based financial assistance in Australia.

Below are {len(cases_with_text)} case descriptions where clients have returned for assistance. Same-instance cases (ILA instalments, re-opened cases, follow-up payments) have already been filtered out — these are genuine new applications from returning clients.

Analyse these to understand WHY clients are genuinely returning for additional assistance.

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
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        res.raise_for_status()
        raw    = res.json()["content"][0]["text"]
        parsed = json.loads(raw.replace("```json","").replace("```","").strip())
        print(f"  ✓ AI analysis — {len(parsed.get('themes',[]))} themes")
        return {**parsed, "generated_at": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        print(f"  ⚠ AI analysis failed: {e}")
        return None

# ── Per-case AI summaries ──────────────────────────────────────────
def run_case_summaries(cases):
    """
    Generate a 2-sentence summary per returning case in a single batch call.
    Sentence 1: what the previous case was about and its outcome.
    Sentence 2: what the new case is requesting.

    Returns dict: case_id → summary string.
    """
    if not ANTHROPIC_API_KEY:
        return {}

    cases_with_data = [
        c for c in cases
        if c.get("description") or c.get("last_case_description")
    ]
    if not cases_with_data:
        return {}

    case_lines = "\n".join(
        "ID:{id} | PREV_STAGE:{ps} | PREV_DESC:{pd} | NEW_DESC:{nd}".format(
            id=c["case_id"],
            ps=c.get("last_case_stage", "Unknown"),
            pd=(c.get("last_case_description") or "Not available")[:200],
            nd=(c.get("description") or "Not available")[:200],
        )
        for c in cases_with_data[:50]
    )

    prompt = f"""You are a caseworker assistant for NZF (National Zakat Foundation), an Australian Zakat charity.

For each case below, write exactly 2 sentences (max 30 words total):
- Sentence 1: What their previous case was for and its outcome (use PREV_STAGE and PREV_DESC).
- Sentence 2: What they are now requesting (use NEW_DESC).

Write in third person, professional tone. If a field says "Not available", omit that part gracefully.

Respond ONLY with valid JSON — no markdown, no extra text:
{{"CASE_ID": "Two sentence summary.", ...}}

Cases:
{case_lines}"""

    try:
        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":    "claude-haiku-4-5-20251001",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        res.raise_for_status()
        raw    = res.json()["content"][0]["text"]
        parsed = json.loads(raw.replace("```json","").replace("```","").strip())
        print(f"  ✓ Case summaries — {len(parsed)} generated")
        return parsed
    except Exception as e:
        print(f"  ⚠ Case summaries failed: {e}")
        return {}

# ── Monthly state series builder ──────────────────────────────────
def _build_state_monthly_series(state_monthly, state_12m, months):
    """
    Build a Chart.js-ready series structure for the stacked bar chart.

    Returns:
      {
        "months": ["2025-05", ...],   # x-axis labels
        "series": [
          {"state": "NSW", "data": [12, 15, 10, ...]},
          ...
        ]
      }

    States ordered by 12-month total descending so the most significant
    states stack at the bottom of the chart.
    """
    # States ranked by 12-month total, most active first — exclude Other/invalid
    ranked_states = [
        s for s, _ in sorted(state_12m.items(), key=lambda x: -x[1])
        if s and s != "Other"
    ]

    series = []
    for state in ranked_states:
        series.append({
            "state": state,
            "data":  [state_monthly.get(m, {}).get(state, 0) for m in months],
        })

    return {"months": months, "series": series}

# ── Build report ───────────────────────────────────────────────────
def build_clients_report(token):
    cutoff = cutoff_n_months(WINDOW_MONTHS)
    months = last_n_months(TREND_MONTHS)

    print("\n  Fetching Analytics views...")
    all_cases   = zac.fetch_view(token, zac.VIEW_CASES,         label="Cases")
    all_dists   = zac.fetch_view(token, zac.VIEW_DISTRIBUTIONS, label="Distributions")
    all_clients = zac.fetch_view(token, zac.VIEW_CLIENTS,       label="Clients")

    case_history  = build_client_case_history(all_cases)
    last_paid     = build_last_paid_index(all_dists)
    client_states = build_client_state_index(all_clients)
    del all_dists, all_clients

    window_cases = [
        c for c in all_cases
        if (zac.parse_dt(c.get("created_time","")) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff
    ]
    print(f"  Cases in window: {len(window_cases):,} (of {len(all_cases):,} total)")

    current_month  = months[-1]
    previous_month = months[-2]

    new_by_month       = defaultdict(int)
    returning_by_month = defaultdict(int)
    same_instance_count = 0
    returning_cases    = []
    gap_bands          = defaultdict(int)

    # State counters for three periods
    state_current  = defaultdict(int)              # current month only
    state_previous = defaultdict(int)              # previous month only
    state_12m      = defaultdict(int)              # all 12 months
    state_monthly  = defaultdict(lambda: defaultdict(int))  # month → state → count

    for c in window_cases:
        created_dt  = zac.parse_dt(c.get("created_time",""))
        mk          = zac.month_key(created_dt)
        if not mk or mk not in months:
            continue

        client_id   = c.get("client_name","").strip()
        stage       = c.get("stage","").strip()
        description = c.get("description","").strip()
        case_id     = c.get("case_id") or c.get("case-id","")
        state       = client_states.get(client_id, "")

        # Same-instance exclusion
        if is_same_instance(stage, description):
            same_instance_count += 1
            continue

        # New vs returning
        client_cases       = case_history.get(client_id, [])
        client_dates       = [e["dt"] for e in client_cases]
        is_first_ever_case = not client_cases or client_cases[0]["dt"] == created_dt

        if is_first_ever_case:
            new_by_month[mk] += 1
        else:
            returning_by_month[mk] += 1
            last_paid_dt    = last_paid.get(client_id)
            gap_days        = days_between(last_paid_dt, c.get("created_time")) if last_paid_dt else None
            band            = return_gap_band(gap_days)
            gap_bands[band] += 1

            # Most recent previous case (before this one)
            prior_cases     = [e for e in client_cases if e["dt"] < created_dt]
            last_case       = prior_cases[-1] if prior_cases else None
            last_case_dt    = last_case["dt"] if last_case else None
            days_since_case = abs((created_dt - last_case_dt).days) if last_case_dt else None

            returning_cases.append({
                "case_id":               case_id,
                "client_id":             client_id,
                "created":               c.get("created_time",""),
                "month":                 mk,
                "stage":                 stage,
                "description":           description[:500],
                "last_paid_date":        last_paid_dt,
                "return_gap_days":       gap_days,
                "return_gap_band":       band,
                "days_since_last_case":  days_since_case,
                "prior_case_count":      len(prior_cases),
                # Previous case detail — used for table display + AI summary
                "last_case_date":        last_case["created_time"] if last_case else "",
                "last_case_stage":       last_case["stage"]        if last_case else "",
                "last_case_description": last_case["description"]  if last_case else "",
            })

        # State counts — total clients (new + returning) per period
        state = client_states.get(client_id, 'Other')
        state_12m[state] += 1
        state_monthly[mk][state] += 1
        if mk == current_month:
            state_current[state] += 1
        elif mk == previous_month:
            state_previous[state] += 1

    print(f"  Same-instance excluded: {same_instance_count:,}")
    print(f"  New (in window):        {sum(new_by_month.values()):,}")
    print(f"  Returning (in window):  {sum(returning_by_month.values()):,}")

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

    gap_days_list = [c["return_gap_days"] for c in returning_cases if c["return_gap_days"] is not None]
    avg_gap = round(sum(gap_days_list) / len(gap_days_list)) if gap_days_list else 0

    qual_sample = sorted(
        returning_cases,
        key=lambda x: zac.parse_dt(x["created"]) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )[:50]

    print("\n  Running AI analysis...")
    ai_analysis    = run_ai_analysis(qual_sample)
    case_summaries = run_case_summaries(qual_sample)

    # Attach per-case summary to each case dict
    for c in qual_sample:
        c["ai_case_summary"] = case_summaries.get(c["case_id"], "")

    return {
        "meta": {
            "last_updated":           datetime.now(timezone.utc).isoformat(),
            "record_count":           len(window_cases),
            "months_covered":         months,
            "current_month":          current_month,
            "previous_month":         previous_month,
            "same_instance_excluded": same_instance_count,
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
            {"band": b, "count": gap_bands.get(b, 0)}
            for b in BAND_ORDER if gap_bands.get(b, 0) > 0
        ],
        "clients_by_state": {
            "current_month":  state_counts_sorted(state_current),
            "previous_month": state_counts_sorted(state_previous),
            "last_12_months": state_counts_sorted(state_12m),
        },
        "clients_by_state_monthly": _build_state_monthly_series(
            state_monthly, state_12m, months
        ),
        "returning_cases": qual_sample,
        "ai_analysis":     ai_analysis,
    }

# ── Main ───────────────────────────────────────────────────────────
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
    print(f"   States (12m):            {len(data['clients_by_state']['last_12_months'])} states")
    print(f"   AI analysis:             {'✓ included' if data['ai_analysis'] else '✗ not available'}")
    print("═" * 55)

if __name__ == "__main__":
    main()
