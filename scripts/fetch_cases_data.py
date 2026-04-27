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

# Load dedicated priority classification rules (modular, versioned)
_PRI_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "priority_rules.json")
try:
    with open(_PRI_CONFIG_PATH) as _f:
        PRIORITY_CONFIG = json.load(_f)
    print(f"  Loaded priority rules v{PRIORITY_CONFIG.get('version','?')} from {_PRI_CONFIG_PATH}")
except Exception as _e:
    print(f"  WARNING: Could not load {_PRI_CONFIG_PATH}: {_e} — using defaults from nzf_rules.json")
    PRIORITY_CONFIG = {}

PRIORITY_RULES_VERSION = PRIORITY_CONFIG.get("version", "0.0.0-fallback")

# Pipeline version — bump when the script's data-shaping logic changes meaningfully.
# Lets the dashboard prove which version of the code generated the JSON it's rendering.
PIPELINE_VERSION = "2.1.0-coql-notes-diagnostics"

PRIORITY_MAP         = [(e["prefix"].upper(), e["label"]) for e in _pri_rules["prefix_map"]]
NO_PRIORITY          = _pri_rules["no_priority_label"]
PRIORITY_ORDER       = _pri_rules["order"]
PRIORITY_COLS        = _pri_rules["colours"]
WINDOW_MONTHS        = _periods["client_report_window_months"]
TREND_MONTHS         = _periods["trend_display_months"]
UNPRIORITIZED_HOURS  = _pri_rules.get("unprioritized_alert_hours", 24)
CLASSIFICATION_GUIDE = _pri_rules.get("classification_guide", {})
CRM_BASE_URL         = _meta.get("zoho_crm_base_url", "")
# Model settings prefer priority_rules.json (v1.0+); fall back to nzf_rules.json
_model_cfg     = PRIORITY_CONFIG.get("model", {})
AI_MODEL       = _model_cfg.get("id") or RULES.get("ai", {}).get("model", "claude-sonnet-4-20250514")
AI_TEMPERATURE = _model_cfg.get("temperature", 0)   # Deterministic by default — same input → same output
AI_MAX_TOKENS  = _model_cfg.get("max_tokens", 8000)

# SLA targets from rules — response and resolution hours per priority
_sla_resp = RULES["case_performance"]["sla_response"]
_sla_resol = RULES["case_performance"]["sla_resolution"]
RESPONSE_SLA_HOURS   = {p: (_sla_resp[p]["hours"] if _sla_resp.get(p) else None) for p in ["P1","P2","P3","P4"]}
RESOLUTION_SLA_HOURS = {p: (_sla_resol[p]["hours"] if _sla_resol.get(p) else None) for p in ["P1","P2","P3","P4"]}

def calc_sla(age_hours, sla_hours):
    """Return (status, hours_remaining) for a given age and SLA target.
    Status: 'overdue' | 'at_risk' | 'ok' | None
    hours_remaining: positive = remaining, negative = overdue by that many hours
    """
    if sla_hours is None or age_hours is None:
        return None, None
    remaining = sla_hours - age_hours
    if remaining < 0:
        return "overdue", remaining          # negative = hours overdue
    if remaining < sla_hours * 0.2:          # within 20% of deadline
        return "at_risk", remaining
    return "ok", remaining

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
def call_claude(prompt, max_tokens=1500, system=None):
    """Single Claude API call. Returns parsed JSON dict or None on failure.

    Uses temperature from PRIORITY_CONFIG (default 0 = deterministic).
    Same input + same model + temperature 0 = identical output every run.
    """
    if not ANTHROPIC_API_KEY:
        return None
    try:
        payload = {
            "model":       AI_MODEL,
            "max_tokens":  max_tokens,
            "temperature": AI_TEMPERATURE,
            "messages":    [{"role": "user", "content": prompt}],
        }
        if system:
            payload["system"] = system
        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json=payload,
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

    # Build framework text — prefer priority_rules.json; fall back to nzf_rules.json
    framework = PRIORITY_CONFIG.get("priority_framework") or {
        k: v for k, v in CLASSIFICATION_GUIDE.items() if not k.startswith("_")
    }
    framework_text = "\n".join(f"  {p}: {desc}" for p, desc in framework.items())

    # Tiebreaker rules — applied when evidence is mixed or vague
    tiebreakers     = PRIORITY_CONFIG.get("tiebreaker_rules", [])
    tiebreaker_text = "\n".join(f"  {i+1}. {rule}" for i, rule in enumerate(tiebreakers))

    # Few-shot examples — anchor the model against real precedent
    examples     = PRIORITY_CONFIG.get("few_shot_examples", [])
    examples_text = "\n".join(
        f"  Example {i+1}: \"{ex.get('description','')}\"\n"
        f"    → {ex.get('expected_priority','')}: {ex.get('rationale','')}"
        for i, ex in enumerate(examples)
    )

    # Closure codes — caseworker shorthand titles indicating case state changes.
    # AI should highlight these in summaries when they appear in the latest note.
    closure_codes = {k: v for k, v in
                     RULES.get("case_performance", {}).get("closure_codes", {}).items()
                     if not k.startswith("_")}
    closure_text  = "\n".join(f"  {k} = {v}" for k, v in closure_codes.items()) \
                    if closure_codes else "  (none configured)"

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

    system_prompt = PRIORITY_CONFIG.get("system_prompt",
        "You are a quality assurance analyst and caseworker assistant for NZF.")

    prompt = f"""PRIORITY FRAMEWORK:
{framework_text}

TIEBREAKER RULES (apply when evidence is mixed or vague):
{tiebreaker_text}

REFERENCE EXAMPLES:
{examples_text}

CLOSURE CODES (caseworker shorthand note titles — highlight in summary if present in LATEST NOTE):
{closure_text}

Review these {len(cases_with_desc)} cases. Each case may include:
- ASSIGNED: current priority, STAGE: workflow stage
- INTERACTION: whether a caseworker has interacted (YES/NO)
- DESC: client's application description
- LATEST NOTE: most recent caseworker note (if any)
- CW REC: caseworker recommendation
- NOT FUNDED: reason if closed not funded

For EACH case provide:
1. recommended_priority: Correct priority P1-P5 based on framework + tiebreakers + reference examples.
2. rationale: One short sentence stating which framework rule or tiebreaker applied.
3. summary: 2-3 sentences covering ALL that apply:
   - What the client needs and urgency level
   - Current status (based on stage, latest note, CW recommendation)
   - If INTERACTION is NO: state "No caseworker interaction recorded"
   - If LATEST NOTE title is a closure code (CCNR, CCUFR, CCF, RFA): explicitly state it
     in the summary, e.g. "Caseworker marked CCNR (no response after follow-up)"
   - If closed not funded: include the reason
   - If closed funded: state the outcome
   No personal names, locations, or identifying details. Third person, professional.

Also assess overall priority assignment quality and flag significant misclassifications.

CRITICAL: Apply rules consistently — the same description must always produce the same recommended_priority. Do not let surrounding cases influence your judgment of any individual case.

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
    "201730421": {{"recommended_priority": "P3", "rationale": "General financial hardship, no acute crisis = P3 per framework.", "summary": "Client requires rent assistance; caseworker has made contact and assessment is in progress."}},
    "201730432": {{"recommended_priority": "P1", "rationale": "Active homelessness with children = P1 per framework.", "summary": "Family violence situation with housing crisis; no caseworker interaction recorded."}}
  }}
}}

Rules:
- per_case MUST contain ALL {len(cases_with_desc)} case IDs exactly as given
- flags: top 10 most significant only
- No personal names in any field

Cases:
{case_lines}"""

    print(f"  Calling Claude (rules v{PRIORITY_RULES_VERSION}, temp={AI_TEMPERATURE}) for {len(cases_with_desc)} cases...")
    result = call_claude(prompt, max_tokens=AI_MAX_TOKENS, system=system_prompt)
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

    # Skip stages — read from config so closed cases are properly excluded.
    # Previously hardcoded {"Closed - Funded", "Funding", "Ongoing Funding"} which
    # missed "Closed - Not Funded" and "Closed - NO Response" → those flooded the
    # attention table even though the cases were closed.
    _cs = RULES.get("case_stages", {})
    SKIP_STAGES = set(_cs.get("closed_stages", [])) | set(_cs.get("skip_for_attention", []))

    # 1. All cases from the last 30 days, excluding funded/funding
    crm_recent = [
        c for c in zcrm.fetch_recent_cases(token, days=30, max_pages=5)
        if c.get("stage","").strip() not in SKIP_STAGES
    ]

    # 2. Notes for those cases — COQL with WHERE Parent_Id IN (...) so we get
    #    server-side filtering with no cross-module pagination cap.
    case_zoho_ids = [c.get("id","") for c in crm_recent if c.get("id")]
    notes_index   = zcrm.fetch_notes_for_cases(token, case_zoho_ids, days=30)

    # Diagnostic stats — surfaced on cases.json so the dashboard can verify
    # which code path ran. If notes_total is 0 or notes_indexed_for is 0 across
    # 30+ in-window cases, something is wrong with the fetch (auth, scope, etc).
    NOTES_DIAG = {
        "fetch_method":       "coql",
        "fetch_window_days":  30,
        "cases_in_window":    len(crm_recent),
        "cases_queried":      len(case_zoho_ids),
        "notes_total":        sum(len(v) for v in notes_index.values()),
        "cases_with_notes":   len(notes_index),
    }
    print(f"  Notes diagnostic: {NOTES_DIAG}")

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

    # ── Diagnostic probe ──────────────────────────────────────────
    # For a fixed set of test cases, dump the full pipeline state.
    # This is the smoking gun — if a case has notes in CRM but appears
    # interaction-less here, this probe tells us EXACTLY which step failed:
    #   - in_crm_recent: was the case fetched at all?
    #   - notes_in_index: how many notes did the COQL fetch return?
    #   - cw_notes_after_filter: how many survived the auto-title filter?
    #   - has_cw_notes: final flag value used by the dashboard
    PROBE_CASE_IDS = ["201730297", "201730385", "201730438"]
    case_probes = {}
    crm_by_case_id = {c.get("case_id",""): c for c in crm_recent}
    sample_by_case_id = {s["case_id"]: s for s in recent_sample}
    for pid in PROBE_CASE_IDS:
        crm_c = crm_by_case_id.get(pid)
        smp   = sample_by_case_id.get(pid)
        zid   = crm_c.get("id","") if crm_c else None
        raw_notes      = notes_index.get(zid, []) if zid else []
        filtered_notes = get_caseworker_notes(zid) if zid else []
        case_probes[pid] = {
            "in_crm_recent":          bool(crm_c),
            "stage":                  crm_c.get("stage","") if crm_c else None,
            "zoho_record_id":         zid,
            "has_description":        bool(crm_c and crm_c.get("description","").strip()) if crm_c else False,
            "in_recent_sample":       bool(smp),
            "notes_in_index":         len(raw_notes),
            "raw_note_titles":        [n["title"] for n in raw_notes],
            "cw_notes_after_filter":  len(filtered_notes),
            "filtered_note_titles":   [n["title"] for n in filtered_notes],
            "has_cw_notes_final":     smp.get("has_cw_notes", False) if smp else False,
        }
        print(f"  Probe {pid}: in_crm={bool(crm_c)}, "
              f"notes_idx={len(raw_notes)}, after_filter={len(filtered_notes)}, "
              f"has_cw_notes={case_probes[pid]['has_cw_notes_final']}")

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
        # Use recommended priority for SLA targets (it's what should apply)
        pri_for_sla = suggested_pri if suggested_pri in RESPONSE_SLA_HOURS else \
                      (assigned_pri if assigned_pri in RESPONSE_SLA_HOURS else None)
        resp_status, resp_remaining   = calc_sla(age_h, RESPONSE_SLA_HOURS.get(pri_for_sla))
        resol_status, resol_remaining = calc_sla(age_h, RESOLUTION_SLA_HOURS.get(pri_for_sla))
        # If responded, override response SLA to show "responded"
        if has_notes:
            resp_status, resp_remaining = "responded", None
        # Summary: AI first, then structured fallback
        summary = enr.get("summary","")
        if not summary:
            parts = [_clean_description(crm_c.get("description",""))]
            if crm_c.get("cw_recommendation"): parts.append(f'CW: {crm_c["cw_recommendation"][:100]}')
            if crm_c.get("reason_not_funded"): parts.append(f'Not funded: {crm_c["reason_not_funded"]}')
            if not has_notes: parts.append("No caseworker interaction recorded.")
            summary = " | ".join(p for p in parts if p)
        return {
            "zoho_record_id":      crm_c.get("id","") or case_id_lookup.get(cid,""),
            "case_id":             cid,
            "client_id":           crm_c.get("client_name",""),
            "created":             created,
            "created_ts":          crdt.timestamp() if crdt else 0,
            "age_hours":           age_h,
            "stage":               crm_c.get("stage",""),
            "assigned_priority":   assigned_pri,
            "suggested_priority":  suggested_pri,  # may be None when AI returned no data
            "flag_type":           flag_type,
            "flag_severity":       flag_severity,
            "response_sla_status":    resp_status,
            "response_sla_remaining": round(resp_remaining, 1) if resp_remaining is not None else None,
            "resolution_sla_status":    resol_status,
            "resolution_sla_remaining": round(resol_remaining, 1) if resol_remaining is not None else None,
            "ai_summary":          summary,
        }

    # ── Build attention table with three independent inclusion rules ──
    # Each case is evaluated against three rules. If ANY rule matches, the
    # case is included. flag_type is set to the FIRST matching rule, in this
    # priority order:
    #   1. priority_mismatch — assigned ≠ AI recommendation (classification issue)
    #   2. no_interaction    — no caseworker notes AND breaching at least one SLA
    #   3. unassigned        — no priority assigned at all
    #
    # Notes:
    # - When AI returns no per_case data, suggested_priority is None (not the
    #   placeholder string "Assign Priority"). The dashboard renders this as "—".
    # - "no_interaction" is only flagged when SLA is breached (per spec — keeps
    #   the table actionable, otherwise list grows too long).
    HIGH_PRI = {"P1", "P2"}

    def is_breaching_sla(crm_c, has_notes, suggested_pri, assigned_pri):
        """Returns True if the case is breaching response or resolution SLA."""
        crdt = zac.parse_dt(crm_c.get("created_time",""))
        if not crdt:
            return False
        age_h = (now - crdt).total_seconds() / 3600
        # Use recommended priority for SLA if available, else assigned
        pri = suggested_pri if suggested_pri in RESPONSE_SLA_HOURS else \
              (assigned_pri if assigned_pri in RESPONSE_SLA_HOURS else None)
        if not pri:
            return False
        # Response SLA: only counts as breach if no caseworker notes yet
        if not has_notes:
            resp_sla = RESPONSE_SLA_HOURS.get(pri)
            if resp_sla is not None and age_h > resp_sla:
                return True
        # Resolution SLA breach is always relevant
        resol_sla = RESOLUTION_SLA_HOURS.get(pri)
        if resol_sla is not None and age_h > resol_sla:
            return True
        return False

    for c in crm_recent:
        cid = c.get("case_id","")
        if not cid or cid in seen_ids:
            continue

        assigned_pri = normalise_priority(c.get("case_urgency",""))
        enr          = per_case.get(cid, {})
        # AI recommendation is None if the model did not return per_case data
        # for this case. Do NOT substitute the placeholder "Assign Priority"
        # string — the dashboard handles None explicitly.
        ai_recommended = enr.get("recommended_priority")
        if ai_recommended in ("", "Assign Priority"):
            ai_recommended = None

        smp       = sample_idx.get(cid, {})
        has_notes = smp.get("has_cw_notes", False)

        # Rule 1: Priority mismatch — assigned exists, AI disagrees
        # Only flag when AI has a confident recommendation that differs
        is_mismatch = (
            assigned_pri != NO_PRIORITY
            and ai_recommended in PRIORITY_ORDER
            and ai_recommended != assigned_pri
        )
        # Rule 2: No interaction + SLA breach
        is_no_interaction = (
            not has_notes
            and is_breaching_sla(c, has_notes, ai_recommended, assigned_pri)
        )
        # Rule 3: Unassigned — no priority set at all
        is_unassigned = (assigned_pri == NO_PRIORITY)

        if not (is_mismatch or is_no_interaction or is_unassigned):
            continue

        # Pick primary flag_type (one row per case)
        if is_mismatch:
            flag_type = "priority_mismatch"
            # Severity: P1/P2 mismatches are High, others Medium
            severity = "High" if ai_recommended in HIGH_PRI else "Medium"
            display_suggested = ai_recommended
        elif is_no_interaction:
            flag_type = "no_interaction"
            severity  = "High"
            display_suggested = ai_recommended  # may be None
        else:  # is_unassigned
            flag_type = "unassigned"
            crdt  = zac.parse_dt(c.get("created_time",""))
            age_h = (now - crdt).total_seconds() / 3600 if crdt else 0
            severity = "High" if age_h > 48 else ("Medium" if age_h > 24 else "Low")
            display_suggested = ai_recommended  # may be None

        combined.append(build_row(
            cid, c, flag_type, severity,
            assigned_pri, display_suggested, enr
        ))
        seen_ids.add(cid)

    # Sort: priority_mismatch first (classification issue), then no_interaction
    # (operational urgency), then unassigned. Within each, P1/P2-overdue first,
    # then severity, then newest.
    FLAG_ORDER = {"priority_mismatch": 0, "no_interaction": 1, "unassigned": 2}
    PRI_ORDER  = {"P1": 0, "P2": 1, "P3": 2, "P4": 3}
    def sort_key(x):
        flag_rank   = FLAG_ORDER.get(x.get("flag_type",""), 9)
        is_p1_overdue = (
            x.get("suggested_priority") == "P1"
            and x.get("response_sla_status") == "overdue"
        )
        return (
            flag_rank,
            0 if is_p1_overdue else 1,
            SEV_ORDER.get(x.get("flag_severity","Low"), 2),
            -x.get("created_ts", 0),
            PRI_ORDER.get(x.get("suggested_priority","") or "", 9),
        )
    combined.sort(key=sort_key)
    for row in combined:
        row.pop("created_ts", None)

    n_mismatch = sum(1 for r in combined if r["flag_type"] == "priority_mismatch")
    n_no_int   = sum(1 for r in combined if r["flag_type"] == "no_interaction")
    n_unasn    = sum(1 for r in combined if r["flag_type"] == "unassigned")
    print(f"  Attention table: {len(combined)} rows — "
          f"{n_mismatch} priority mismatch, {n_no_int} no interaction (SLA breach), "
          f"{n_unasn} unassigned")

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
            "pipeline_version":        PIPELINE_VERSION,
            "rules_version":           PRIORITY_RULES_VERSION,
            "rules_updated":           PRIORITY_CONFIG.get("last_updated", ""),
            "model":                   AI_MODEL,
            "temperature":             AI_TEMPERATURE,
            "notes_diagnostic":        NOTES_DIAG,
            "case_probes":             case_probes,
            "priority_mismatch_count": n_mismatch,
            "no_interaction_count":    n_no_int,
            "unassigned_count":        n_unasn,
            "combined_cases":          combined,
            "ai_analysis":             ai_analysis,
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
    print(f"  Priority mismatches:  {pi['priority_mismatch_count']}")
    print(f"  Unassigned 30d:      {pi['unassigned_count']}")
    print(f"  Combined table rows: {len(pi['combined_cases'])}")
    if ai:
        print(f"  Priority quality:    {ai.get('quality_score')} "
              f"({len(ai.get('flags', []))} flags)")
    print("=" * 55)

if __name__ == "__main__":
    main()
