"""
fetch_cases_perf_data.py
────────────────────────
Builds /data/cases_perf.json for the Cases Performance dashboard.

Metrics produced
────────────────
Response time
  Time between case creation and the first GENUINE caseworker note.
  Automated/intake notes (Case Allocation Notes, Online Application etc.)
  are excluded — titles defined in config/nzf_rules.json.

Closure time
  Time between case creation and case closure, using Modified Time of
  cases currently in a closed stage as the closure date proxy.
  Broken down by outcome (Funded / Not Funded / No Response).

SLA compliance
  % of cases where first response arrived within the priority target
  window (P1=24h, P2=72h, P3=144h, P4=240h, P5=336h).

Throughput
  Cases created vs cases closed per month — reveals if backlog is
  structurally growing.

Funding rate
  % of closed cases that are Closed - Funded.

Backlog
  All currently open cases (not in closed stages) by age band.

Trend indicators
  Each metric compares last N months vs prior N months (N from rules).
  Returns "improving", "stable", or "regressing".
"""

import os, json, sys
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import statistics

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Load rules ────────────────────────────────────────────────────
RULES        = zac.RULES
_perf        = RULES["case_performance"]
_pri         = RULES["case_priorities"]
_periods     = RULES["reporting_periods"]

SLA_TARGETS         = _perf["sla_targets_hours"]
AUTO_NOTES_EXACT    = set(t.lower() for t in _perf["automated_note_titles"]["exact_match"])
AUTO_NOTES_PREFIX   = [p.lower() for p in _perf["automated_note_titles"]["prefix_match"]]
CLOSED_STAGES       = set(_perf["closed_stages"])
OUTCOME_COLOURS     = _perf["closure_outcome_colours"]
OUTCOME_SHORT       = _perf["closure_outcome_short"]
TREND_COMPARE_MONTHS= _perf["trend_comparison_months"]
PRIORITY_ORDER      = _pri["order"]
PRIORITY_COLOURS    = _pri["colours"]
PRIORITY_MAP        = [(e["prefix"].upper(), e["label"]) for e in _pri["prefix_map"]]
NO_PRIORITY         = _pri["no_priority_label"]
TREND_MONTHS_COUNT  = _periods["trend_display_months"]
WINDOW_MONTHS       = _periods["client_report_window_months"]

# ── Helpers ───────────────────────────────────────────────────────
def normalise_priority(raw):
    if not raw or not raw.strip():
        return NO_PRIORITY
    s = raw.strip().upper()
    for prefix, label in PRIORITY_MAP:
        if s.startswith(prefix):
            return label
    return NO_PRIORITY

def is_automated_note(title):
    t = (title or "").strip().lower()
    if t in AUTO_NOTES_EXACT:
        return True
    return any(t.startswith(p) for p in AUTO_NOTES_PREFIX)

def hours_between(dt1, dt2):
    """Return hours between two datetimes (positive only)."""
    if not dt1 or not dt2:
        return None
    diff = (dt2 - dt1).total_seconds() / 3600
    return diff if diff >= 0 else None

def days_between(dt1, dt2):
    h = hours_between(dt1, dt2)
    return round(h / 24, 1) if h is not None else None

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

def safe_median(vals):
    vals = [v for v in vals if v is not None]
    return round(statistics.median(vals), 1) if vals else None

def safe_mean(vals):
    vals = [v for v in vals if v is not None]
    return round(sum(vals) / len(vals), 1) if vals else None

def safe_p75(vals):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    idx = int(len(vals) * 0.75)
    return round(vals[min(idx, len(vals)-1)], 1)

def trend_direction(recent_avg, prior_avg, lower_is_better=True):
    """Compare two averages. Return 'improving', 'stable', or 'regressing'."""
    if recent_avg is None or prior_avg is None or prior_avg == 0:
        return "stable"
    pct_change = (recent_avg - prior_avg) / prior_avg
    threshold  = 0.05  # 5% change = meaningful
    if abs(pct_change) < threshold:
        return "stable"
    improving = pct_change < 0 if lower_is_better else pct_change > 0
    return "improving" if improving else "regressing"

def compute_trend_kpi(monthly_values, months, lower_is_better=True):
    """
    Compare avg of last TREND_COMPARE_MONTHS vs prior TREND_COMPARE_MONTHS.
    Returns dict with recent_avg, prior_avg, direction.
    """
    n = TREND_COMPARE_MONTHS
    if len(months) < n * 2:
        return {"recent_avg": None, "prior_avg": None, "direction": "stable"}
    recent_months = months[-n:]
    prior_months  = months[-n*2:-n]
    recent_vals   = [monthly_values.get(m) for m in recent_months if monthly_values.get(m) is not None]
    prior_vals    = [monthly_values.get(m) for m in prior_months  if monthly_values.get(m) is not None]
    recent_avg    = safe_mean(recent_vals)
    prior_avg     = safe_mean(prior_vals)
    return {
        "recent_avg": recent_avg,
        "prior_avg":  prior_avg,
        "direction":  trend_direction(recent_avg, prior_avg, lower_is_better),
    }

# ── Build report ──────────────────────────────────────────────────
def build_cases_perf_report(token):
    months       = last_n_months(TREND_MONTHS_COUNT)
    cutoff       = cutoff_n_months(WINDOW_MONTHS)
    current_month= months[-1]
    now          = datetime.now(timezone.utc)

    # ── Fetch data ────────────────────────────────────────────────
    print("\n  Fetching Analytics views...")
    all_cases = zac.fetch_view(token, zac.VIEW_CASES,      label="Cases")
    all_notes = zac.fetch_view(token, zac.VIEW_CASE_NOTES, label="Case Notes")

    # ── Index: case_id → case data ────────────────────────────────
    # Use Zoho internal record ID (`id`) as the join key with Notes `parent_id`
    case_index = {}
    for c in all_cases:
        cid = c.get("id", "").strip()
        if cid:
            case_index[cid] = c

    # ── Index: case_id → first genuine note datetime ──────────────
    first_note_dt = {}
    excluded_note_count = 0
    for n in all_notes:
        cid   = n.get("parent_id", "").strip()
        title = n.get("note_title", "").strip()
        if not cid:
            continue
        if is_automated_note(title):
            excluded_note_count += 1
            continue
        dt = zac.parse_dt(n.get("created_time", ""))
        if not dt:
            continue
        if cid not in first_note_dt or dt < first_note_dt[cid]:
            first_note_dt[cid] = dt

    print(f"  First-note index: {len(first_note_dt):,} cases with genuine notes "
          f"({excluded_note_count:,} automated notes excluded)")

    # ── Compute per-case metrics ───────────────────────────────────
    response_times   = []      # all response times in window
    closure_times    = []      # all closure times
    monthly_resp     = defaultdict(list)   # month → [response_hours]
    monthly_close    = defaultdict(list)   # month → [closure_days]
    pri_resp         = defaultdict(list)   # priority → [response_hours]
    pri_close        = defaultdict(list)   # priority → [closure_days]
    pri_resp_monthly = defaultdict(lambda: defaultdict(list))  # priority → month → [hrs]
    pri_close_monthly= defaultdict(lambda: defaultdict(list))  # priority → month → [days]
    outcome_close    = defaultdict(list)   # outcome → [closure_days]
    outcome_monthly  = defaultdict(lambda: defaultdict(list))  # outcome → month → [days]
    monthly_created  = defaultdict(int)    # month → count created
    monthly_closed   = defaultdict(int)    # month → count closed (by closure month)
    sla_data         = defaultdict(lambda: {"within": 0, "total": 0})

    backlog_cases    = []      # open cases with age info
    funding_total    = 0       # funded cases count (12m)
    closed_total     = 0       # all closed cases count (12m)

    for c in all_cases:
        cid          = c.get("id", "").strip()
        created_dt   = zac.parse_dt(c.get("created_time", ""))
        modified_dt  = zac.parse_dt(c.get("modified_time", ""))
        stage        = (c.get("stage") or "").strip()
        priority     = normalise_priority(c.get("case_urgency", ""))
        is_closed    = stage in CLOSED_STAGES

        if not created_dt:
            continue

        created_mk = zac.month_key(created_dt)

        # ── Monthly created count (in window) ─────────────────────
        if created_mk in months:
            monthly_created[created_mk] += 1

        # ── Response time (cases created in window) ────────────────
        if created_dt >= cutoff and created_mk in months:
            first_note = first_note_dt.get(cid)
            resp_hours = hours_between(created_dt, first_note) if first_note else None

            if resp_hours is not None:
                response_times.append(resp_hours)
                monthly_resp[created_mk].append(resp_hours)
                pri_resp[priority].append(resp_hours)
                pri_resp_monthly[priority][created_mk].append(resp_hours)

                # SLA compliance
                sla_target = SLA_TARGETS.get(priority)
                sla_data[priority]["total"] += 1
                if sla_target is not None and resp_hours <= sla_target:
                    sla_data[priority]["within"] += 1

        # ── Closure time (cases closed in window) ─────────────────
        if is_closed and modified_dt:
            closure_mk = zac.month_key(modified_dt)
            if closure_mk in months:
                monthly_closed[closure_mk] += 1
                closed_total += 1
                if stage == "Closed - Funded":
                    funding_total += 1

                close_days = days_between(created_dt, modified_dt)
                if close_days is not None and close_days >= 0:
                    closure_times.append(close_days)
                    monthly_close[closure_mk].append(close_days)
                    pri_close[priority].append(close_days)
                    pri_close_monthly[priority][closure_mk].append(close_days)
                    outcome_close[stage].append(close_days)
                    outcome_monthly[stage][closure_mk].append(close_days)

        # ── Backlog (currently open cases) ─────────────────────────
        if not is_closed:
            age_days = (now - created_dt).days
            backlog_cases.append({
                "age_days": age_days,
                "priority": priority,
                "month":    created_mk,
            })

    print(f"  Cases processed:")
    print(f"    In window (created):     {sum(monthly_created.values()):,}")
    print(f"    With response time:      {len(response_times):,}")
    print(f"    Closed (in window):      {len(closure_times):,}")
    print(f"    Currently open (backlog):{len(backlog_cases):,}")

    # ── Trend series ───────────────────────────────────────────────
    resp_trend_monthly  = {m: safe_median(monthly_resp[m])  for m in months}
    close_trend_monthly = {m: safe_median(monthly_close[m]) for m in months}
    funding_rate_monthly= {
        m: round(
            sum(1 for c in all_cases
                if zac.month_key(zac.parse_dt(c.get("modified_time",""))) == m
                and c.get("stage","") == "Closed - Funded") /
            max(monthly_closed.get(m, 0), 1) * 100, 1
        )
        for m in months
        if monthly_closed.get(m, 0) > 0
    }

    # ── Priority response trend series ────────────────────────────
    pri_resp_trend = {
        p: {m: safe_median(pri_resp_monthly[p].get(m, [])) for m in months}
        for p in PRIORITY_ORDER
    }
    pri_close_trend = {
        p: {m: safe_median(pri_close_monthly[p].get(m, [])) for m in months}
        for p in PRIORITY_ORDER
    }

    # ── Outcome closure trend ─────────────────────────────────────
    outcome_close_trend = {
        stage: {m: safe_median(outcome_monthly[stage].get(m, [])) for m in months}
        for stage in CLOSED_STAGES
    }

    # ── Backlog age bands ──────────────────────────────────────────
    age_bands = [
        ("0–7 days",   0,  7),
        ("8–14 days",  8,  14),
        ("15–30 days", 15, 30),
        ("31–60 days", 31, 60),
        ("60+ days",   61, None),
    ]
    def age_band(days):
        for label, lo, hi in age_bands:
            if hi is None and days >= lo: return label
            if hi is not None and lo <= days <= hi: return label
        return "0–7 days"

    backlog_by_age      = defaultdict(int)
    backlog_by_pri      = defaultdict(int)
    backlog_by_age_pri  = defaultdict(lambda: defaultdict(int))
    backlog_over_30     = 0
    for bc in backlog_cases:
        band = age_band(bc["age_days"])
        backlog_by_age[band]             += 1
        backlog_by_pri[bc["priority"]]   += 1
        backlog_by_age_pri[band][bc["priority"]] += 1
        if bc["age_days"] > 30:
            backlog_over_30 += 1

    # ── SLA compliance ────────────────────────────────────────────
    sla_compliance = {}
    for p in PRIORITY_ORDER:
        if SLA_TARGETS.get(p) is None:
            sla_compliance[p] = None
            continue
        d = sla_data[p]
        sla_compliance[p] = (
            round(d["within"] / d["total"] * 100, 1) if d["total"] > 0 else None
        )
    overall_sla_total  = sum(v["total"]  for p, v in sla_data.items() if SLA_TARGETS.get(p))
    overall_sla_within = sum(v["within"] for p, v in sla_data.items() if SLA_TARGETS.get(p))
    overall_sla = round(overall_sla_within / overall_sla_total * 100, 1) if overall_sla_total else None

    # ── Executive summary (current month) ─────────────────────────
    curr_resp_hrs  = safe_median(monthly_resp.get(current_month, []))
    curr_close_days= safe_median(monthly_close.get(current_month, []))
    funding_rate   = round(funding_total / closed_total * 100, 1) if closed_total else None

    # ── Trend directions ──────────────────────────────────────────
    resp_trend   = compute_trend_kpi(resp_trend_monthly,  months, lower_is_better=True)
    close_trend  = compute_trend_kpi(close_trend_monthly, months, lower_is_better=True)

    # ── Build output ──────────────────────────────────────────────
    def pri_close_kpis():
        return {
            p: {
                "median_days": safe_median(pri_close[p]),
                "avg_days":    safe_mean(pri_close[p]),
                "count":       len(pri_close[p]),
            }
            for p in PRIORITY_ORDER
        }

    def outcome_kpis():
        return {
            OUTCOME_SHORT.get(s, s): {
                "stage":       s,
                "median_days": safe_median(outcome_close[s]),
                "avg_days":    safe_mean(outcome_close[s]),
                "p75_days":    safe_p75(outcome_close[s]),
                "count":       len(outcome_close[s]),
                "colour":      OUTCOME_COLOURS.get(s, "#9F9393"),
            }
            for s in CLOSED_STAGES
        }

    # Trend series for Chart.js
    def make_trend_series(pri_trend_dict):
        return {
            "months": months,
            "series": [
                {
                    "priority": p,
                    "colour":   PRIORITY_COLOURS.get(p, "#9F9393"),
                    "data": [pri_trend_dict[p].get(m) for m in months],
                }
                for p in PRIORITY_ORDER
            ]
        }

    def make_outcome_series():
        return {
            "months": months,
            "series": [
                {
                    "stage":   s,
                    "label":   OUTCOME_SHORT.get(s, s),
                    "colour":  OUTCOME_COLOURS.get(s, "#9F9393"),
                    "data":    [outcome_close_trend[s].get(m) for m in months],
                }
                for s in CLOSED_STAGES
            ]
        }

    return {
        "meta": {
            "last_updated":      datetime.now(timezone.utc).isoformat(),
            "months_covered":    months,
            "current_month":     current_month,
            "cases_analysed":    len(all_cases),
            "notes_analysed":    len(all_notes),
            "automated_excluded":excluded_note_count,
        },

        "executive_summary": {
            "median_response_hours": curr_resp_hrs,
            "median_closure_days":   curr_close_days,
            "sla_compliance_pct":    overall_sla,
            "funding_rate_pct":      funding_rate,
            "backlog_over_30d":      backlog_over_30,
            "response_trend":        resp_trend["direction"],
            "closure_trend":         close_trend["direction"],
        },

        "response_time": {
            "overall": {
                "median_hours": safe_median(response_times),
                "avg_hours":    safe_mean(response_times),
                "p75_hours":    safe_p75(response_times),
                "count":        len(response_times),
                "trend":        resp_trend,
            },
            "by_priority": {
                p: {
                    "median_hours":    safe_median(pri_resp[p]),
                    "avg_hours":       safe_mean(pri_resp[p]),
                    "count":          len(pri_resp[p]),
                    "sla_target_hours":SLA_TARGETS.get(p),
                    "sla_compliance":  sla_compliance.get(p),
                }
                for p in PRIORITY_ORDER
            },
            "monthly_trend": {
                m: safe_median(monthly_resp.get(m, []))
                for m in months
            },
            "priority_trend_series": make_trend_series(pri_resp_trend),
            "sla_targets": SLA_TARGETS,
            "sla_compliance_by_priority": sla_compliance,
        },

        "closure_time": {
            "overall": {
                "median_days": safe_median(closure_times),
                "avg_days":    safe_mean(closure_times),
                "p75_days":    safe_p75(closure_times),
                "count":       len(closure_times),
                "trend":       close_trend,
            },
            "by_outcome": outcome_kpis(),
            "by_priority": pri_close_kpis(),
            "monthly_trend": {
                m: safe_median(monthly_close.get(m, []))
                for m in months
            },
            "priority_trend_series": make_trend_series(pri_close_trend),
            "outcome_trend_series":  make_outcome_series(),
        },

        "throughput": {
            "monthly": [
                {
                    "month":   m,
                    "created": monthly_created.get(m, 0),
                    "closed":  monthly_closed.get(m, 0),
                    "net":     monthly_created.get(m, 0) - monthly_closed.get(m, 0),
                }
                for m in months
            ],
            "funding_rate_monthly": {
                m: funding_rate_monthly.get(m) for m in months
            },
            "funding_rate_12m": funding_rate,
        },

        "backlog": {
            "total_open":   len(backlog_cases),
            "over_30d":     backlog_over_30,
            "by_age": [
                {
                    "band":  band,
                    "count": backlog_by_age.get(band, 0),
                    "by_priority": {
                        p: backlog_by_age_pri[band].get(p, 0)
                        for p in PRIORITY_ORDER
                    }
                }
                for band, _, _ in age_bands
            ],
            "by_priority": {
                p: backlog_by_pri.get(p, 0) for p in PRIORITY_ORDER
            },
        },
    }


def main():
    print("═" * 55)
    print("NZF — Cases Performance  |  Zoho Analytics")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("═" * 55)

    token = zac.get_access_token()
    data  = build_cases_perf_report(token)

    out = os.path.join(DATA_DIR, "cases_perf.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    es = data["executive_summary"]
    rt = data["response_time"]
    ct = data["closure_time"]
    bl = data["backlog"]

    print(f"\n✅ cases_perf.json written")
    print(f"   Cases analysed:          {data['meta']['cases_analysed']:,}")
    print(f"   Notes analysed:          {data['meta']['notes_analysed']:,}")
    print(f"   Automated excluded:      {data['meta']['automated_excluded']:,}")
    print(f"\n   ── Executive Summary ──")
    print(f"   Median response:         {es['median_response_hours']} hrs")
    print(f"   Median closure:          {es['median_closure_days']} days")
    print(f"   SLA compliance:          {es['sla_compliance_pct']}%")
    print(f"   Funding rate:            {es['funding_rate_pct']}%")
    print(f"   Backlog >30d:            {es['backlog_over_30d']}")
    print(f"\n   ── Trends ──")
    print(f"   Response time:           {es['response_trend']}")
    print(f"   Closure time:            {es['closure_trend']}")
    print(f"\n   ── By Priority (Median Response Hrs) ──")
    for p in data["response_time"]["by_priority"]:
        d = data["response_time"]["by_priority"][p]
        sla = d.get("sla_compliance")
        sla_str = f"  SLA: {sla}%" if sla is not None else ""
        print(f"   {p:12}: {d['median_hours']} hrs  (n={d['count']}){sla_str}")
    print("═" * 55)


if __name__ == "__main__":
    main()
