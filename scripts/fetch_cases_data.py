"""
fetch_cases_data.py
────────────────────
Builds /data/cases.json for the Cases Report dashboard.

Priority Intelligence:
  1. Unprioritized alert — cases >24h with no priority, latest 20, AI summaries
  2. Priority accuracy analysis — AI review of recent assignments vs framework

PII policy:
  No client names, caseworker names, or personal identifiers are stored.
  All records identified by CRM record IDs only.
  AI summaries are instructed to omit personal names.
"""

import os, json, sys, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac
import zoho_crm_client as zcrm

DATA_DIR          = os.path.join(os.path.dirname(__file__), "..", "data")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Load rules ────────────────────────────────────────────────────
RULES          = zac.RULES
_pri_rules     = RULES["case_priorities"]
_periods       = RULES["reporting_periods"]
_meta          = RULES["_meta"]

PRIORITY_MAP         = [(e["prefix"].upper(), e["label"]) for e in _pri_rules["prefix_map"]]
NO_PRIORITY          = _pri_rules["no_priority_label"]
PRIORITY_ORDER       = _pri_rules["order"]
PRIORITY_COLS        = _pri_rules["colours"]
WINDOW_MONTHS        = _periods["client_report_window_months"]
TREND_MONTHS         = _periods["trend_display_months"]
UNPRIORITIZED_HOURS  = _pri_rules.get("unprioritized_alert_hours", 24)
CLASSIFICATION_GUIDE = _pri_rules.get("classification_guide", {})
CRM_BASE_URL         = _meta.get("zoho_crm_base_url", "")
AI_MODEL             = RULES.get("ai", {}).get("model", "claude-sonnet-4-20250514")

# ── Priority normalisation ────────────────────────────────────────
def normalise_priority(raw):
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

# ── Anthropic API helper ──────────────────────────────────────────
def call_claude(prompt, max_tokens=1500):
    """Single Claude API call. Returns parsed JSON dict or None on failure."""
    if not ANTHROPIC_API_KEY:
        return None
    try:
        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":    AI_MODEL,
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        res.raise_for_status()
        raw = res.json()["content"][0]["text"]
        return json.loads(raw.replace("```json", "").replace("```", "").strip())
    except Exception as e:
        print(f"  WARNING: Claude API call failed: {e}")
        return None

# ── Unprioritized alert ────────────────────────────────────────────
def _build_unprioritized_from_crm(crm_cases, max_rows=20):
    """
    Convert live CRM unprioritized cases into the shape used by the
    combined table. Sorted newest first (most recently submitted = most urgent).
    Capped at max_rows.
    """
    now     = datetime.now(timezone.utc)
    result  = []
    for c in crm_cases:  # Already sorted newest-first by CRM API
        created_dt = zac.parse_dt(c.get("created_time", ""))
        age_h      = round((now - created_dt).total_seconds() / 3600, 1) if created_dt else None
        result.append({
            "zoho_record_id": c.get("id", ""),
            "case_id":        c.get("case_id", ""),
            "client_id":      c.get("client_name", ""),
            "created":        c.get("created_time", ""),
            "age_hours":      age_h,
            "stage":          c.get("stage", ""),
            "description":    c.get("description", ""),
        })
    return result[:max_rows]

def run_unprioritized_summaries(cases):
    """
    Batch AI call: generate a 1-sentence urgency summary for each
    unprioritized case. Explicitly no personal names in output.
    Returns dict: case_id -> summary string.
    """
    cases_with_desc = [c for c in cases if c.get("description")]
    if not cases_with_desc or not ANTHROPIC_API_KEY:
        return {}

    lines = "\n".join(
        f'ID:{c["case_id"]} DESC:{c["description"][:300]}'
        for c in cases_with_desc
    )

    prompt = f"""You are a caseworker assistant for NZF (National Zakat Foundation Australia).

For each case below, write ONE concise sentence (max 20 words) describing:
- What assistance is needed
- How urgent it appears to be

IMPORTANT: Do NOT include any personal names, locations or identifying details.
Write in third person, professional tone.

Respond ONLY with valid JSON:
{{"CASE_ID": "one sentence summary", ...}}

Cases:
{lines}"""

    result = call_claude(prompt, max_tokens=1000)
    if result:
        print(f"  Case summaries: {len(result)} generated")
    return result or {}

# ── AI priority accuracy analysis ─────────────────────────────────
def run_priority_analysis(recent_cases):
    """
    Reviews recent cases (any priority including No Priority) against
    the classification framework to detect misassignment.
    """
    if not ANTHROPIC_API_KEY:
        print("  INFO: ANTHROPIC_API_KEY not set — skipping priority analysis")
        return None

    # Include ALL cases with descriptions — both assigned and unassigned
    cases_with_desc = [c for c in recent_cases if c.get("description")][:60]

    if len(cases_with_desc) < 3:
        print(f"  INFO: Only {len(cases_with_desc)} cases with descriptions — skipping analysis")
        return None

    guide_text = "\n".join(
        f"  {p}: {desc}"
        for p, desc in CLASSIFICATION_GUIDE.items()
        if not p.startswith("_")
    )

    case_lines = "\n".join(
        f'ID:{c["case_id"]} PRIORITY:{c["priority"]} DESC:{c["description"][:250]}'
        for c in cases_with_desc
    )

    prompt = f"""You are a quality assurance analyst for NZF (National Zakat Foundation Australia), a Zakat charity.

PRIORITY FRAMEWORK:
{guide_text}

Review {len(cases_with_desc)} recent cases. Each shows the assigned priority and the client's description.

Identify cases where the priority appears INCORRECT — especially:
1. Descriptions showing P1-level crisis (homeless, no food, DV, eviction today) assigned P3/P4/No Priority
2. Cases assigned too high a priority given the description
3. No Priority cases that clearly should have been prioritised immediately

Respond ONLY with valid JSON (no markdown):
{{
  "quality_score": "Good|Fair|Poor",
  "quality_summary": "2-3 sentence assessment of priority assignment quality across these cases",
  "total_reviewed": {len(cases_with_desc)},
  "flags": [
    {{
      "case_id": "string",
      "assigned_priority": "P1/P2/P3/P4/P5/No Priority",
      "suggested_priority": "P1/P2/P3/P4/P5",
      "severity": "High|Medium|Low",
      "reason": "One sentence — no personal names"
    }}
  ],
  "patterns": ["pattern observed", "another pattern if applicable"]
}}

Limit flags to the 10 most significant. Do not include personal names in any field.

Cases:
{case_lines}"""

    result = call_claude(prompt, max_tokens=2000)
    if result:
        print(f"  AI analysis: {result.get('quality_score')} — "
              f"{len(result.get('flags', []))} flags from {len(cases_with_desc)} cases")
        return {**result, "generated_at": datetime.now(timezone.utc).isoformat()}
    return None

# ── Build report ──────────────────────────────────────────────────
def build_cases_report(token):
    cutoff = cutoff_n_months(WINDOW_MONTHS)
    months = last_n_months(TREND_MONTHS)

    print("\n  Fetching Analytics views...")
    all_cases = zac.fetch_view(token, zac.VIEW_CASES, label="Cases")

    window_cases = []
    for c in all_cases:
        dt = zac.parse_dt(c.get("created_time", ""))
        if dt and dt >= cutoff:
            window_cases.append((c, dt))
    print(f"  Cases in window: {len(window_cases):,} (of {len(all_cases):,} total)")

    current_month  = months[-1]
    previous_month = months[-2]

    total_by_month    = defaultdict(int)
    priority_by_month = defaultdict(lambda: defaultdict(int))

    for c, dt in window_cases:
        mk = zac.month_key(dt)
        if not mk or mk not in months:
            continue
        priority = normalise_priority(c.get("case_urgency", "") or c.get("priority", ""))
        total_by_month[mk]              += 1
        priority_by_month[mk][priority] += 1

    trend = [{"month": m, "count": total_by_month.get(m, 0)} for m in months]

    priority_trend = {
        "months": months,
        "series": [
            {
                "priority": p,
                "colour":   PRIORITY_COLS.get(p, "#9F9393"),
                "data":     [priority_by_month.get(m, {}).get(p, 0) for m in months],
            }
            for p in PRIORITY_ORDER
        ]
    }

    curr_total = total_by_month.get(current_month, 0)
    prev_total = total_by_month.get(previous_month, 0)
    total_12m  = sum(total_by_month.get(m, 0) for m in months)

    def pct(c, p):
        return round(((c - p) / p) * 100, 1) if p else None

    def priority_kpis(mk):
        return {p: priority_by_month.get(mk, {}).get(p, 0) for p in PRIORITY_ORDER}

    priority_12m = {
        p: sum(priority_by_month.get(m, {}).get(p, 0) for m in months)
        for p in PRIORITY_ORDER
    }

    # ── Priority intelligence — LIVE CRM DATA ────────────────────
    # Deliberately uses Zoho CRM REST API (not Analytics) because:
    #   Analytics sync can be up to 24 hours delayed.
    #   A P1 case created 2 hours ago would be invisible in Analytics.
    #   Priority intelligence must detect urgent cases in near real-time.
    print("\n  Building priority intelligence (live CRM data)...")

    # 1. Unprioritized open cases — directly from CRM, all-time (not just window)
    crm_unprioritized_raw = zcrm.fetch_all_open_cases_no_priority(
        token,
        threshold_hours=UNPRIORITIZED_HOURS,
        max_pages=5,
    )
    unprioritized = _build_unprioritized_from_crm(crm_unprioritized_raw)
    print(f"  Unprioritized (>{UNPRIORITIZED_HOURS}h): {len(unprioritized):,} shown (of {len(crm_unprioritized_raw):,} total)")

    # 2. AI accuracy analysis — last 30 days, live from CRM
    crm_recent = zcrm.fetch_recent_cases(token, days=30, max_pages=5)
    recent_sample = [
        {
            "case_id":        c.get("case_id", ""),
            "zoho_record_id": c.get("id", ""),
            "priority":       normalise_priority(c.get("case_urgency", "")),
            "description":    c.get("description", "").strip(),
        }
        for c in crm_recent
        if c.get("description", "").strip()
    ]
    case_id_lookup = {
        s["case_id"]: s["zoho_record_id"]
        for s in recent_sample if s["case_id"] and s["zoho_record_id"]
    }
    print(f"  Cases for AI review: {len(recent_sample):,} (last 30 days, live CRM)")
    ai_analysis = run_priority_analysis(recent_sample)

    # 3. AI summaries — run against BOTH unprioritized (all-time banner) AND
    #    last-30-day unassigned cases used in the combined table
    summaries = run_unprioritized_summaries(unprioritized)

    # ── Build combined cases table ────────────────────────────────
    # Source: live CRM (last 30 days) — no Analytics latency here.
    #
    # Includes:
    #   A. AI-flagged misclassifications where suggested priority is P1 or P2
    #   B. Cases from last 30 days with NO priority assigned (potential P1/P2)
    #
    # Sort: severity (High → Medium → Low), then created_date descending
    # within each severity group. No row cap.
    #
    # Separate from the all-time unprioritized alert banner count (kept above).

    SEV_ORDER     = {"High": 0, "Medium": 1, "Low": 2}
    HIGH_PRI      = {"P1", "P2"}
    now           = datetime.now(timezone.utc)
    crm_index     = {c.get("case_id", ""): c for c in crm_recent if c.get("case_id")}
    flags         = (ai_analysis or {}).get("flags", [])
    combined      = []
    seen_ids      = set()

    # A. Misclassified cases where AI suggests P1 or P2
    p1p2_flags = [
        f for f in flags
        if f.get("suggested_priority", "") in HIGH_PRI
    ]
    for f in sorted(p1p2_flags, key=lambda x: (
        SEV_ORDER.get(x.get("severity", "Low"), 2),
        -(zac.parse_dt(crm_index.get(x.get("case_id",""), {}).get("created_time","")) or now).timestamp()
    )):
        cid = f.get("case_id", "")
        if not cid or cid in seen_ids:
            continue
        crm_c   = crm_index.get(cid, {})
        created = crm_c.get("created_time", "")
        crdt    = zac.parse_dt(created)
        age_h   = round((now - crdt).total_seconds() / 3600, 1) if crdt else None
        combined.append({
            "zoho_record_id":     crm_c.get("id", "") or case_id_lookup.get(cid, ""),
            "case_id":            cid,
            "client_id":          crm_c.get("client_name", ""),
            "created":            created,
            "created_ts":         crdt.timestamp() if crdt else 0,
            "age_hours":          age_h,
            "stage":              crm_c.get("stage", ""),
            "assigned_priority":  f.get("assigned_priority", ""),
            "suggested_priority": f.get("suggested_priority", ""),
            "flag_type":          "misclassified",
            "flag_severity":      f.get("severity", "Medium"),
            "ai_summary":         f.get("reason", ""),
        })
        seen_ids.add(cid)

    # B. Last-30-day unassigned cases (potential P1/P2 — not yet assessed)
    unassigned_30d = [
        c for c in crm_recent
        if normalise_priority(c.get("case_urgency", "")) == NO_PRIORITY
        and c.get("case_id", "") not in seen_ids
    ]
    # Get AI summaries for these too
    unassigned_summaries = run_unprioritized_summaries(unassigned_30d)

    for c in unassigned_30d:
        cid     = c.get("case_id", "")
        created = c.get("created_time", "")
        crdt    = zac.parse_dt(created)
        age_h   = round((now - crdt).total_seconds() / 3600, 1) if crdt else None
        # Severity based on age: High if >48h unassigned, Medium if >24h
        sev     = "High" if (age_h or 0) > 48 else "Medium" if (age_h or 0) > 24 else "Low"
        combined.append({
            "zoho_record_id":     c.get("id", ""),
            "case_id":            cid,
            "client_id":          c.get("client_name", ""),
            "created":            created,
            "created_ts":         crdt.timestamp() if crdt else 0,
            "age_hours":          age_h,
            "stage":              c.get("stage", ""),
            "assigned_priority":  NO_PRIORITY,
            "suggested_priority": "Assign Priority",
            "flag_type":          "unassigned",
            "flag_severity":      sev,
            "ai_summary":         unassigned_summaries.get(cid, (c.get("description") or "")[:100]),
        })
        seen_ids.add(cid)

    # Sort: severity first, then created_date descending within severity
    combined.sort(key=lambda x: (
        SEV_ORDER.get(x.get("flag_severity", "Low"), 2),
        -x.get("created_ts", 0)
    ))

    # Strip the helper timestamp — not needed in JSON
    for row in combined:
        row.pop("created_ts", None)

    p1p2_count   = len([f for f in flags if f.get("suggested_priority","") in HIGH_PRI])
    unassign_count = len(unassigned_30d)
    print(f"  Combined table: {len(combined)} rows "
          f"({p1p2_count} flagged P1/P2, {unassign_count} unassigned last 30d)")

    return {
        "meta": {
            "last_updated":   datetime.now(timezone.utc).isoformat(),
            "record_count":   len(window_cases),
            "months_covered": months,
            "current_month":  current_month,
            "previous_month": previous_month,
            "crm_base_url":   CRM_BASE_URL,
        },
        "summary": {
            "current_month":  curr_total,
            "previous_month": prev_total,
            "pct_change":     pct(curr_total, prev_total),
            "total_12m":      total_12m,
            "monthly_avg":    round(total_12m / len(months), 1),
        },
        "trend":          trend,
        "priority": {
            "order":          PRIORITY_ORDER,
            "colours":        PRIORITY_COLS,
            "current_month":  priority_kpis(current_month),
            "previous_month": priority_kpis(previous_month),
            "total_12m":      priority_12m,
            "monthly_avg":    {p: round(priority_12m[p] / len(months), 1) for p in PRIORITY_ORDER},
        },
        "priority_trend": priority_trend,
        "priority_intelligence": {
            "unprioritized_count": len(unprioritized),
            "flagged_count":       len(flags),
            "combined_cases":      combined,
            "ai_analysis":         ai_analysis,
        },
    }

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("NZF -- Cases Report  |  Zoho Analytics")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"AI analysis: {'enabled (' + AI_MODEL + ')' if ANTHROPIC_API_KEY else 'disabled (no API key)'}")
    print("=" * 55)

    token = zac.get_access_token()
    data  = build_cases_report(token)

    out = os.path.join(DATA_DIR, "cases.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    s  = data["summary"]
    pi = data["priority_intelligence"]
    ai = pi["ai_analysis"]
    print(f"\nDone. cases.json written")
    print(f"  Cases in window:     {data['meta']['record_count']:,}")
    print(f"  Current month:       {s['current_month']}")
    print(f"  12-month total:      {s['total_12m']:,}")
    print(f"  Unprioritized >24h:  {pi['unprioritized_count']}")
    print(f"  Flagged by AI:       {pi['flagged_count']}")
    print(f"  Combined table rows: {len(pi['combined_cases'])}")
    if ai:
        print(f"  Priority quality:    {ai.get('quality_score')} "
              f"({len(ai.get('flags', []))} flags)")
    print("=" * 55)

if __name__ == "__main__":
    main()
