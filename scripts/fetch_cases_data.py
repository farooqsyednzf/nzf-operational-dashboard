"""
fetch_cases_data.py
────────────────────
Builds /data/cases.json for the Cases Report dashboard.

Includes:
  - Cases created KPIs and trend
  - Priority breakdown and trend
  - Priority intelligence:
      1. Unprioritized alert — cases >24h old with no priority assigned
      2. AI accuracy analysis — detects potential misclassification
         by comparing case descriptions against the priority framework
"""

import os, json, sys, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR          = os.path.join(os.path.dirname(__file__), "..", "data")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Load rules ────────────────────────────────────────────────────
RULES          = zac.RULES
_pri_rules     = RULES["case_priorities"]
_periods       = RULES["reporting_periods"]

PRIORITY_MAP         = [(e["prefix"].upper(), e["label"]) for e in _pri_rules["prefix_map"]]
NO_PRIORITY          = _pri_rules["no_priority_label"]
PRIORITY_ORDER       = _pri_rules["order"]
PRIORITY_COLS        = _pri_rules["colours"]
WINDOW_MONTHS        = _periods["client_report_window_months"]
TREND_MONTHS         = _periods["trend_display_months"]
UNPRIORITIZED_HOURS  = _pri_rules.get("unprioritized_alert_hours", 24)
CLASSIFICATION_GUIDE = _pri_rules.get("classification_guide", {})

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

# ── Unprioritized alert ────────────────────────────────────────────
def build_unprioritized_alert(all_cases):
    """
    Find cases with no priority assigned that are older than the
    threshold (default 24 hours). These should have been actioned.
    Returns sorted list of cases (oldest first) capped at 50.
    """
    now       = datetime.now(timezone.utc)
    threshold = timedelta(hours=UNPRIORITIZED_HOURS)
    flagged   = []

    for c in all_cases:
        priority = normalise_priority(c.get("case_urgency", ""))
        if priority != NO_PRIORITY:
            continue
        created_dt = zac.parse_dt(c.get("created_time", ""))
        if not created_dt:
            continue
        age = now - created_dt
        if age <= threshold:
            continue  # Within window — not yet overdue

        stage = c.get("stage", "").strip()
        flagged.append({
            "case_id":           c.get("case_id") or c.get("case-id", ""),
            "created":           c.get("created_time", ""),
            "age_hours":         round(age.total_seconds() / 3600, 1),
            "age_days":          round(age.total_seconds() / 86400, 1),
            "stage":             stage,
            "description":       (c.get("description") or "")[:120].strip(),
        })

    # Sort oldest first so the most overdue are visible
    flagged.sort(key=lambda x: x["age_hours"], reverse=True)
    return flagged[:50]

# ── AI priority accuracy analysis ─────────────────────────────────
def run_priority_analysis(recent_cases):
    """
    Send a sample of recent cases (last 30 days) with their
    descriptions and assigned priorities to Claude for analysis.

    Detects:
      - Cases where description suggests higher urgency than assigned
        (especially P1-level need assigned at P3/P4/No Priority)
      - Overall quality of priority assignment
    """
    if not ANTHROPIC_API_KEY:
        print("  INFO: ANTHROPIC_API_KEY not set — skipping priority analysis")
        return None

    cases_with_desc = [
        c for c in recent_cases
        if c.get("description") and c.get("priority") != NO_PRIORITY
    ][:60]

    if len(cases_with_desc) < 5:
        print("  INFO: Not enough cases with descriptions for AI analysis")
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

    prompt = f"""You are a quality assurance analyst reviewing priority assignments for NZF (National Zakat Foundation Australia), a Zakat charity providing financial assistance.

PRIORITY FRAMEWORK:
{guide_text}

TASK: Review the following {len(cases_with_desc)} recent cases. For each case, the assigned priority is shown alongside the client's own description of their situation.

Identify cases where the priority assignment appears INCORRECT — particularly:
1. Cases that appear to need P1 same-day response but are assigned P3/P4/No Priority
2. Cases clearly assigned too high a priority given the description
3. Overall assessment of how consistently priorities are being applied

Respond ONLY with valid JSON (no markdown, no preamble):
{{
  "quality_score": "Good|Fair|Poor",
  "quality_summary": "2-3 sentence assessment of overall priority assignment quality",
  "total_reviewed": {len(cases_with_desc)},
  "flags": [
    {{
      "case_id": "case ID string",
      "assigned_priority": "P1/P2/P3/P4/P5/No Priority",
      "suggested_priority": "P1/P2/P3/P4/P5",
      "severity": "High|Medium|Low",
      "reason": "One sentence explaining the mismatch"
    }}
  ],
  "patterns": [
    "Pattern 1 observed across multiple cases",
    "Pattern 2 if applicable"
  ]
}}

Only include cases in 'flags' where there is a clear discrepancy. Limit to the 10 most significant flags.

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
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 2000,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=45,
        )
        res.raise_for_status()
        raw    = res.json()["content"][0]["text"]
        parsed = json.loads(raw.replace("```json", "").replace("```", "").strip())
        flags  = parsed.get("flags", [])
        print(f"  AI analysis: {parsed.get('quality_score')} quality — "
              f"{len(flags)} flags from {len(cases_with_desc)} cases reviewed")
        return {**parsed, "generated_at": datetime.now(timezone.utc).isoformat()}
    except Exception as e:
        print(f"  WARNING: AI analysis failed: {e}")
        return None

# ── Build report ──────────────────────────────────────────────────
def build_cases_report(token):
    cutoff = cutoff_n_months(WINDOW_MONTHS)
    months = last_n_months(TREND_MONTHS)

    print("\n  Fetching Analytics views...")
    all_cases = zac.fetch_view(token, zac.VIEW_CASES, label="Cases")

    # Window filter
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

    trend = [
        {"month": m, "count": total_by_month.get(m, 0)}
        for m in months
    ]

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

    # ── Priority intelligence ─────────────────────────────────────
    print("\n  Building priority intelligence...")

    # 1. Unprioritized alert — run across ALL cases not just window
    unprioritized = build_unprioritized_alert(all_cases)
    print(f"  Unprioritized (>{UNPRIORITIZED_HOURS}h): {len(unprioritized):,}")

    # 2. AI analysis — last 30 days with descriptions
    cutoff_30d    = datetime.now(timezone.utc) - timedelta(days=30)
    recent_sample = [
        {
            "case_id":     c.get("case_id") or c.get("case-id", ""),
            "priority":    normalise_priority(c.get("case_urgency", "")),
            "description": (c.get("description") or "").strip(),
            "stage":       c.get("stage", "").strip(),
        }
        for c, dt in window_cases
        if dt >= cutoff_30d and (c.get("description") or "").strip()
    ]
    print(f"  Cases for AI review: {len(recent_sample):,} (last 30 days with descriptions)")
    ai_analysis = run_priority_analysis(recent_sample)

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
            "monthly_avg":    round(total_12m / len(months), 1),
        },
        "trend":  trend,
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
            "unprioritized_alert": {
                "threshold_hours": UNPRIORITIZED_HOURS,
                "count":           len(unprioritized),
                "cases":           unprioritized,
            },
            "ai_analysis": ai_analysis,
        },
    }

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("NZF -- Cases Report  |  Zoho Analytics")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"AI analysis: {'enabled' if ANTHROPIC_API_KEY else 'disabled'}")
    print("=" * 55)

    token = zac.get_access_token()
    data  = build_cases_report(token)

    out = os.path.join(DATA_DIR, "cases.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    s   = data["summary"]
    pi  = data["priority_intelligence"]
    ai  = pi["ai_analysis"]
    print(f"\nDone. cases.json written")
    print(f"  Cases in window:     {data['meta']['record_count']:,}")
    print(f"  Current month:       {s['current_month']}")
    print(f"  12-month total:      {s['total_12m']:,}")
    print(f"  Unprioritized >24h:  {pi['unprioritized_alert']['count']}")
    if ai:
        print(f"  Priority quality:    {ai.get('quality_score')} "
              f"({len(ai.get('flags',[]))} flags)")
    print("=" * 55)

if __name__ == "__main__":
    main()
