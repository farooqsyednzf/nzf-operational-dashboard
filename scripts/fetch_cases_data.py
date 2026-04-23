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
                "model":      AI_MODEL,
                "max_tokens": max_tokens,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=90,
        )
        res.raise_for_status()
        raw = res.json()["content"][0]["text"]
        cleaned = raw.replace("```json", "").replace("```", "").strip()
        result = json.loads(cleaned)
        return result
    except json.JSONDecodeError as e:
        print(f"  WARNING: Claude returned invalid JSON: {e}")
        print(f"  Raw response (first 300 chars): {raw[:300] if 'raw' in dir() else 'unavailable'}")
        return None
    except Exception as e:
        print(f"  WARNING: Claude API call failed: {type(e).__name__}: {e}")
        return None


def _clean_description(desc, max_len=150):
    """
    Strip common Islamic/informal greetings from the start of a description
    and return a clean summary-ready excerpt. Used as fallback when AI is unavailable.
    """
    if not desc:
        return ""
    greetings = [
        "salam alakoum", "salamu alaikum", "assalamu alaikum", "assalamualaikum",
        "assalamualkum", "dear brothers and sisters", "dear sir", "dear madam",
        "to whom it may concern", "hi,", "hello,", "hi ", "hello ",
    ]
    cleaned = desc.strip()
    lower   = cleaned.lower()
    for g in greetings:
        if lower.startswith(g):
            # Skip past the greeting and any punctuation/newline
            cleaned = cleaned[len(g):].lstrip(" ,.\n\r")
            break
    # Take first max_len chars, end on a word boundary
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rsplit(" ", 1)[0] + "..."
    return cleaned.strip()


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

def run_combined_analysis(recent_cases):
    """
    Single merged AI call returning accuracy analysis + per-case enrichment.

    For every case with a description returns:
      quality_score, quality_summary, flags, patterns  (accuracy analysis)
      per_case: case_id -> {recommended_priority, summary}

    Batches capped at 40 cases to avoid token truncation.
    If the API call fails, returns None — downstream code uses raw
    description as fallback summary.
    """
    if not ANTHROPIC_API_KEY:
        print("  INFO: ANTHROPIC_API_KEY not set — skipping AI analysis")
        return None

    cases_with_desc = [c for c in recent_cases if (c.get("description") or "").strip()][:40]
    if len(cases_with_desc) < 3:
        print(f"  INFO: Only {len(cases_with_desc)} cases with descriptions — skipping")
        return None

    guide_text = "\n".join(
        f"  {p}: {desc}"
        for p, desc in CLASSIFICATION_GUIDE.items()
        if not p.startswith("_")
    )

    case_lines = "\n".join(
        (
            f'ID:{c["case_id"]} ASSIGNED:{c.get("priority","No Priority")} '
            f'STAGE:{c.get("stage","?")} '
            f'INTERACTION:{"YES" if c.get("has_cw_notes") else "NO - no caseworker notes"} '
            f'DESC:{(c.get("description") or "")[:200]}'
            + (f' | LATEST NOTE ({c.get("latest_note_title","")}):{c.get("latest_note","")[:150]}' if c.get("latest_note") else "")
            + (f' | CW REC:{c.get("cw_recommendation","")[:100]}' if c.get("cw_recommendation") else "")
            + (f' | NOT FUNDED:{c.get("reason_not_funded","")}' if c.get("reason_not_funded") else "")
        )
        for c in cases_with_desc
    )

    prompt = f"""You are a quality assurance analyst and caseworker assistant for NZF (National Zakat Foundation Australia).

PRIORITY FRAMEWORK:
{guide_text}

Review these {len(cases_with_desc)} cases. Each case may include:
- ASSIGNED: current priority, STAGE: workflow stage
- INTERACTION: whether a caseworker has interacted (YES/NO)
- DESC: client's application description
- LATEST NOTE: most recent caseworker note (if any)
- CW REC: caseworker recommendation
- NOT FUNDED: reason if closed not funded

For EACH case provide:
1. recommended_priority: Correct priority P1-P5 based on description. Default P3 if vague.
2. summary: 2-3 sentences covering ALL that apply:
   - What the client needs and urgency level
   - Current status (based on stage, latest note, CW recommendation)
   - If INTERACTION is NO: state "No caseworker interaction recorded"
   - If closed not funded: include the reason
   - If closed funded: state the outcome
   No personal names, locations, or identifying details. Third person, professional.

Also assess overall priority assignment quality and flag significant misclassifications.

Respond ONLY with valid JSON (no markdown):
{{
  "quality_score": "Good|Fair|Poor",
  "quality_summary": "2-3 sentence overall assessment",
  "total_reviewed": {len(cases_with_desc)},
  "flags": [
    {{
      "case_id": "string",
      "assigned_priority": "P1/P2/P3/P4/P5/No Priority",
      "suggested_priority": "P1/P2/P3/P4/P5",
      "severity": "High|Medium|Low",
      "reason": "One sentence, no personal names"
    }}
  ],
  "patterns": ["pattern 1"],
  "per_case": {{
    "201730421": {{"recommended_priority": "P3", "summary": "Client requires rent assistance; caseworker has made contact and assessment is in progress."}},
    "201730432": {{"recommended_priority": "P1", "summary": "Family violence situation with housing crisis; no caseworker interaction recorded."}}
  }}
}}

Rules:
- per_case MUST contain ALL {len(cases_with_desc)} case IDs exactly as given
- flags: top 10 most significant only
- No personal names in any field

Cases:
{case_lines}"""

    print(f"  Calling Claude for {len(cases_with_desc)} cases...")
    result = call_claude(prompt, max_tokens=8000)
    if result:
        n_flags    = len(result.get("flags", []))
        n_per_case = len(result.get("per_case", {}))
        print(f"  Combined AI: {result.get('quality_score','?')} quality — "
              f"{n_flags} flags, {n_per_case}/{len(cases_with_desc)} cases enriched")
        if n_per_case == 0:
            print("  WARNING: per_case is empty — summaries will fall back to raw description")
        return {**result, "generated_at": datetime.now(timezone.utc).isoformat()}
    print("  WARNING: Combined AI call returned None — check API key and logs above")
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
    # Scope: cases created in the last 30 days.
    # Notes on those cases are by definition also within that window.
    # Unassigned = no priority set on a case from the last 30 days.
    # No interaction = zero genuine caseworker notes on a last-30-day case.
    print("\n  Building priority intelligence (live CRM data)...")

    SKIP_STAGES = {"Closed - Funded", "Funding", "Ongoing Funding"}

    # 1. All cases from the last 30 days, excluding funded/funding
    crm_recent = [
        c for c in zcrm.fetch_recent_cases(token, days=30, max_pages=5)
        if c.get("stage","").strip() not in SKIP_STAGES
    ]

    # 2. Notes for those cases — scoped to Potentials module so pages aren't
    #    wasted on notes from Contacts, Leads, etc.
    case_zoho_ids = [c.get("id","") for c in crm_recent if c.get("id")]
    notes_index   = zcrm.fetch_notes_for_cases(token, case_zoho_ids, days=30, max_pages=5)

    # 3. Determine genuine caseworker interaction per case
    AUTO_TITLES   = {t.lower() for t in RULES["case_performance"]["automated_note_titles"]["exact_match"]}
    AUTO_PREFIXES = [p.lower() for p in RULES["case_performance"]["automated_note_titles"]["prefix_match"]]

    def get_caseworker_notes(zoho_id):
        return [
            n for n in notes_index.get(zoho_id, [])
            if n["title"].lower() not in AUTO_TITLES
            and not any(n["title"].lower().startswith(p) for p in AUTO_PREFIXES)
        ]

    # 4. Build context for AI — cases with descriptions
    recent_sample = []
    for c in crm_recent:
        if not c.get("description","").strip():
            continue
        cw_notes    = get_caseworker_notes(c.get("id",""))
        latest_note = cw_notes[0] if cw_notes else None
        recent_sample.append({
            "case_id":           c.get("case_id",""),
            "zoho_record_id":    c.get("id",""),
            "priority":          normalise_priority(c.get("case_urgency","")),
            "description":       c.get("description","").strip(),
            "stage":             c.get("stage",""),
            "cw_recommendation": c.get("cw_recommendation",""),
            "reason_not_funded": c.get("reason_not_funded",""),
            "has_cw_notes":      len(cw_notes) > 0,
            "latest_note_title": latest_note["title"] if latest_note else "",
            "latest_note":       (latest_note["content"] or "")[:200] if latest_note else "",
        })

    case_id_lookup = {s["case_id"]: s["zoho_record_id"] for s in recent_sample if s["case_id"]}

    # 5. Single AI call
    print(f"  Running combined AI analysis ({len(recent_sample):,} cases)...")
    combined_result = run_combined_analysis(recent_sample)
    ai_analysis     = combined_result
    per_case        = (combined_result or {}).get("per_case", {})
    sample_idx      = {s["case_id"]: s for s in recent_sample}

    # ── Build combined cases table ────────────────────────────────
    SEV_ORDER = {"High": 0, "Medium": 1, "Low": 2}
    HIGH_PRI  = {"P1", "P2"}
    now       = datetime.now(timezone.utc)
    crm_index = {c.get("case_id",""): c for c in crm_recent if c.get("case_id")}
    flags     = (ai_analysis or {}).get("flags", [])
    combined  = []
    seen_ids  = set()

    def build_row(cid, crm_c, flag_type, flag_severity, assigned_pri, suggested_pri, enr):
        created  = crm_c.get("created_time","")
        crdt     = zac.parse_dt(created)
        age_h    = round((now - crdt).total_seconds() / 3600, 1) if crdt else None
        smp      = sample_idx.get(cid, {})
        has_notes= smp.get("has_cw_notes", False)
        # Summary: AI first, then structured fallback
        summary  = enr.get("summary","")
        if not summary:
            parts = [_clean_description(crm_c.get("description",""))]
            if crm_c.get("cw_recommendation"): parts.append(f'CW: {crm_c["cw_recommendation"][:100]}')
            if crm_c.get("reason_not_funded"): parts.append(f'Not funded: {crm_c["reason_not_funded"]}')
            if not has_notes: parts.append("No caseworker interaction recorded.")
            summary = " | ".join(p for p in parts if p)
        return {
            "zoho_record_id":    crm_c.get("id","") or case_id_lookup.get(cid,""),
            "case_id":           cid,
            "client_id":         crm_c.get("client_name",""),
            "created":           created,
            "created_ts":        crdt.timestamp() if crdt else 0,
            "age_hours":         age_h,
            "stage":             crm_c.get("stage",""),
            "assigned_priority": assigned_pri,
            "suggested_priority":suggested_pri,
            "flag_type":         flag_type,
            "flag_severity":     flag_severity,
            "ai_summary":        summary,
        }

    # A. No interaction — cases with zero caseworker notes
    for c in sorted(crm_recent, key=lambda x: zac.parse_dt(x.get("created_time","")) or now, reverse=True):
        cid = c.get("case_id","")
        if not cid or cid in seen_ids: continue
        smp = sample_idx.get(cid, {})
        if smp.get("has_cw_notes", True): continue   # has notes — skip
        enr = per_case.get(cid, {})
        pri = normalise_priority(c.get("case_urgency",""))
        combined.append(build_row(cid, c, "no_interaction", "High",
                                  pri or NO_PRIORITY,
                                  enr.get("recommended_priority","Assign Priority"), enr))
        seen_ids.add(cid)

    # B. AI-flagged misclassifications suggesting P1 or P2
    for f in sorted([f for f in flags if f.get("suggested_priority","") in HIGH_PRI],
                    key=lambda x: SEV_ORDER.get(x.get("severity","Low"), 2)):
        cid   = f.get("case_id","")
        if not cid or cid in seen_ids: continue
        crm_c = crm_index.get(cid, {})
        combined.append(build_row(cid, crm_c, "misclassified", f.get("severity","Medium"),
                                  f.get("assigned_priority",""), f.get("suggested_priority",""),
                                  per_case.get(cid,{})))
        seen_ids.add(cid)

    # C. Unassigned — last 30 days, no priority set
    for c in crm_recent:
        cid = c.get("case_id","")
        if not cid or cid in seen_ids: continue
        if normalise_priority(c.get("case_urgency","")) != NO_PRIORITY: continue
        crdt  = zac.parse_dt(c.get("created_time",""))
        age_h = round((now - crdt).total_seconds() / 3600, 1) if crdt else None
        sev   = "High" if (age_h or 0) > 48 else "Medium" if (age_h or 0) > 24 else "Low"
        enr   = per_case.get(cid, {})
        combined.append(build_row(cid, c, "unassigned", sev,
                                  NO_PRIORITY, enr.get("recommended_priority","Assign Priority"), enr))
        seen_ids.add(cid)

    # Sort: newest first, then recommended priority P1→P5
    PRI_ORDER = {"P1":0,"P2":1,"P3":2,"P4":3,"P5":4}
    combined.sort(key=lambda x: (
        -x.get("created_ts", 0),
        PRI_ORDER.get(x.get("suggested_priority",""), 9)
    ))
    for row in combined:
        row.pop("created_ts", None)

    n_no_int = sum(1 for r in combined if r["flag_type"] == "no_interaction")
    n_misc   = sum(1 for r in combined if r["flag_type"] == "misclassified")
    n_unasn  = sum(1 for r in combined if r["flag_type"] == "unassigned")
    print(f"  Table: {len(combined)} rows — {n_no_int} no interaction, "
          f"{n_misc} misclassified, {n_unasn} unassigned")

    # B. AI-flagged misclassifications suggesting P1 or P2
    p1p2_flags = [f for f in flags if f.get("suggested_priority","") in HIGH_PRI]
    for f in sorted(p1p2_flags, key=lambda x: (
        SEV_ORDER.get(x.get("severity","Low"), 2),
        -(zac.parse_dt(crm_index.get(x.get("case_id",""),{}).get("created_time","")) or now).timestamp()
    )):
        cid = f.get("case_id","")
        if not cid or cid in seen_ids: continue
        crm_c = crm_index.get(cid, {})
        row   = build_row(cid, crm_c, "misclassified",
                          f.get("severity","Medium"),
                          f.get("assigned_priority",""),
                          f.get("suggested_priority",""),
                          per_case.get(cid,{}),
                          f.get("reason",""))
        combined.append(row)
        seen_ids.add(cid)

    # C. Last-30-day unassigned cases
    unassigned_30d = [
        c for c in crm_recent
        if normalise_priority(c.get("case_urgency","")) == NO_PRIORITY
        and c.get("case_id","") not in seen_ids
    ]
    for c in unassigned_30d:
        cid  = c.get("case_id","")
        if not cid or cid in seen_ids: continue
        crdt = zac.parse_dt(c.get("created_time",""))
        age_h= round((now-crdt).total_seconds()/3600,1) if crdt else None
        sev  = "High" if (age_h or 0)>48 else "Medium" if (age_h or 0)>24 else "Low"
        enr  = per_case.get(cid,{})
        row  = build_row(cid, c, "unassigned", sev,
                         NO_PRIORITY,
                         enr.get("recommended_priority","Assign Priority"),
                         enr)
        combined.append(row)
        seen_ids.add(cid)

    # Sort: no_interaction first, then by severity, then newest
    combined.sort(key=lambda x: (
        0 if x["flag_type"]=="no_interaction" else SEV_ORDER.get(x["flag_severity"],"Low")+1,
        -x.get("created_ts", 0)
    ))
    for row in combined:
        row.pop("created_ts", None)

    n_no_interaction = len([r for r in combined if r["flag_type"]=="no_interaction"])
    n_misclassified  = len([r for r in combined if r["flag_type"]=="misclassified"])
    n_unassigned     = len([r for r in combined if r["flag_type"]=="unassigned"])
    print(f"  Combined table: {len(combined)} rows — "
          f"{n_no_interaction} no interaction, {n_misclassified} misclassified, "
          f"{n_unassigned} unassigned")

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
            "monthly_avg":    {p: round(priority_12m[p]/len(months),1) for p in PRIORITY_ORDER},
        },
        "priority_trend": priority_trend,
        "priority_intelligence": {
            "no_interaction_count": n_no_int,
            "misclassified_count":  n_misc,
            "unassigned_count":     n_unasn,
            "combined_cases":       combined,
            "ai_analysis":          ai_analysis,
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
    print(f"  No interaction:      {pi['no_interaction_count']}")
    print(f"  Misclassified P1/P2: {pi['misclassified_count']}")
    print(f"  Unassigned 30d:      {pi['unassigned_count']}")
    print(f"  Combined table rows: {len(pi['combined_cases'])}")
    if ai:
        print(f"  Priority quality:    {ai.get('quality_score')} "
              f"({len(ai.get('flags', []))} flags)")
    print("=" * 55)

if __name__ == "__main__":
    main()
