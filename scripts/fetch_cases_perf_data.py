"""
fetch_cases_perf_data.py
────────────────────────
Builds /data/cases_perf.json for the Cases Performance dashboard.

Two SLA types (targets from config/nzf_rules.json):

  Response SLA
    Time from case creation → first genuine caseworker note.
    P1: 1 business hour (9AM–5PM Mon–Fri AEST clock pauses outside this).
    P2–P4: calendar hours (48h / 72h / 72h).
    Automated notes excluded (titles in nzf_rules.json).

  Resolution SLA
    Time from case creation → first resolution event, whichever comes first:
      • Case moves to a Closed stage, OR
      • First paid/extracted distribution is made for that case.
    All priorities: calendar hours (24h / 72h / 144h / 240h).

Additional metrics:
  • Closure time by outcome and priority
  • Throughput (intake vs closures)
  • Funding rate trend
  • Backlog age distribution
  • Trend direction (last 3m vs prior 3m) on every KPI
"""

import os, json, sys, statistics
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Load rules ────────────────────────────────────────────────────
RULES        = zac.RULES
_perf        = RULES["case_performance"]
_pri         = RULES["case_priorities"]
_periods     = RULES["reporting_periods"]

# SLA config
SLA_RESPONSE   = _perf["sla_response"]
SLA_RESOLUTION = _perf["sla_resolution"]
WORK_START     = _perf["working_hours"]["start_hour"]
WORK_END       = _perf["working_hours"]["end_hour"]

AUTO_NOTES_EXACT  = set(t.lower() for t in _perf["automated_note_titles"]["exact_match"])
AUTO_NOTES_PREFIX = [p.lower() for p in _perf["automated_note_titles"]["prefix_match"]]
CLOSED_STAGES     = set(_perf["closed_stages"])
OUTCOME_COLOURS   = _perf["closure_outcome_colours"]
OUTCOME_SHORT     = _perf["closure_outcome_short"]
TREND_N           = _perf["trend_comparison_months"]

PRIORITY_ORDER  = _pri["order"]
PRIORITY_COLOURS= _pri["colours"]
PRIORITY_MAP    = [(e["prefix"].upper(), e["label"]) for e in _pri["prefix_map"]]
NO_PRIORITY     = _pri["no_priority_label"]

TREND_MONTHS_COUNT = _periods["trend_display_months"]
WINDOW_MONTHS      = _periods["client_report_window_months"]
PAID_STATUSES      = set(RULES["distributions"]["paid_statuses"])

# ── Helpers ───────────────────────────────────────────────────────
def normalise_priority(raw):
    if not raw or not raw.strip(): return NO_PRIORITY
    s = raw.strip().upper()
    for prefix, label in PRIORITY_MAP:
        if s.startswith(prefix): return label
    return NO_PRIORITY

def is_automated_note(title):
    t = (title or "").strip().lower()
    if t in AUTO_NOTES_EXACT: return True
    return any(t.startswith(p) for p in AUTO_NOTES_PREFIX)

def calendar_hours(dt1, dt2):
    """Calendar hours between two datetimes (non-negative)."""
    if not dt1 or not dt2: return None
    diff = (dt2 - dt1).total_seconds() / 3600
    return diff if diff >= 0 else None

def business_hours(dt1, dt2):
    """
    Elapsed business hours between dt1 and dt2.
    Business hours: WORK_START–WORK_END, Mon–Fri AEST (UTC+10).
    The clock pauses outside working hours.
    """
    if not dt1 or not dt2 or dt2 <= dt1: return None

    AEST_OFFSET = timedelta(hours=10)

    def to_aest(dt):
        return dt.astimezone(timezone(AEST_OFFSET))

    start = to_aest(dt1)
    end   = to_aest(dt2)

    total   = 0.0
    current = start

    while current < end:
        # Skip weekends
        if current.weekday() >= 5:
            next_day = current + timedelta(days=1)
            current  = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
            continue

        day_open  = current.replace(hour=WORK_START, minute=0, second=0, microsecond=0)
        day_close = current.replace(hour=WORK_END,   minute=0, second=0, microsecond=0)

        # Before opening — jump to opening
        if current < day_open:
            current = day_open
            continue

        # After closing — jump to next day
        if current >= day_close:
            next_day = current + timedelta(days=1)
            current  = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
            continue

        # In business hours — accumulate
        period_end = min(end, day_close)
        total     += (period_end - current).total_seconds() / 3600

        if period_end >= end:
            break

        next_day = current + timedelta(days=1)
        current  = next_day.replace(hour=0, minute=0, second=0, microsecond=0)

    return total

def days_between(dt1, dt2):
    h = calendar_hours(dt1, dt2)
    return round(h / 24, 1) if h is not None else None

def cutoff_n(n):
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

def safe_median(vals):
    v = [x for x in vals if x is not None]
    return round(statistics.median(v), 1) if v else None

def safe_mean(vals):
    v = [x for x in vals if x is not None]
    return round(sum(v)/len(v), 1) if v else None

def safe_p75(vals):
    v = sorted(x for x in vals if x is not None)
    if not v: return None
    return round(v[min(int(len(v)*0.75), len(v)-1)], 1)

def trend_direction(recent, prior, lower_is_better=True):
    if recent is None or prior is None or prior == 0: return "stable"
    pct = (recent - prior) / prior
    if abs(pct) < 0.05: return "stable"
    return "improving" if (pct < 0) == lower_is_better else "regressing"

def compute_trend(monthly_vals, months, lower_is_better=True):
    n = TREND_N
    if len(months) < n*2: return {"recent_avg":None,"prior_avg":None,"direction":"stable"}
    recent = [monthly_vals.get(m) for m in months[-n:]   if monthly_vals.get(m) is not None]
    prior  = [monthly_vals.get(m) for m in months[-n*2:-n] if monthly_vals.get(m) is not None]
    ra, pa = safe_mean(recent), safe_mean(prior)
    return {"recent_avg": ra, "prior_avg": pa,
            "direction": trend_direction(ra, pa, lower_is_better)}

def sla_check_response(hours_elapsed, priority):
    """
    Returns (within_sla: bool, target_hours: float|None, used_business_hours: bool).
    None if no SLA defined for this priority.
    """
    cfg = SLA_RESPONSE.get(priority)
    if not cfg: return (None, None, False)
    target    = cfg["hours"]
    biz_only  = cfg.get("business_hours_only", False)
    within    = hours_elapsed <= target if hours_elapsed is not None else None
    return (within, target, biz_only)

def sla_check_resolution(hours_elapsed, priority):
    cfg = SLA_RESOLUTION.get(priority)
    if not cfg: return (None, None)
    target = cfg["hours"]
    within = hours_elapsed <= target if hours_elapsed is not None else None
    return (within, target)

def effective_dist_date(d):
    status = d.get("status","").strip()
    raw = d.get("paid_date","") if status=="Paid" else d.get("extracted_date","")
    return raw if raw and raw.strip() else d.get("created_time","")

# ── Build report ──────────────────────────────────────────────────
def build_report(token):
    months        = last_n_months(TREND_MONTHS_COUNT)
    cutoff        = cutoff_n(WINDOW_MONTHS)
    current_month = months[-1]
    now           = datetime.now(timezone.utc)

    print("\n  Fetching Analytics views...")
    all_cases = zac.fetch_view(token, zac.VIEW_CASES,      label="Cases")
    all_notes = zac.fetch_view(token, zac.VIEW_CASE_NOTES, label="Case Notes")
    all_dists = zac.fetch_view(token, zac.VIEW_DISTRIBUTIONS, label="Distributions")

    # ── Index: case_id → first genuine note datetime ──────────────
    first_note_dt  = {}
    auto_excluded  = 0
    for n in all_notes:
        cid   = n.get("parent_id","").strip()
        title = n.get("note_title","").strip()
        if not cid: continue
        if is_automated_note(title):
            auto_excluded += 1
            continue
        dt = zac.parse_dt(n.get("created_time",""))
        if not dt: continue
        if cid not in first_note_dt or dt < first_note_dt[cid]:
            first_note_dt[cid] = dt

    print(f"  First-note index: {len(first_note_dt):,} cases ({auto_excluded:,} auto notes excluded)")

    # ── Index: case_id → first paid/extracted distribution datetime ──
    first_dist_dt = {}
    for d in all_dists:
        if d.get("status","").strip() not in PAID_STATUSES: continue
        case_id = d.get("case_name","").strip()
        if not case_id: continue
        dt = zac.parse_dt(effective_dist_date(d))
        if not dt: continue
        if case_id not in first_dist_dt or dt < first_dist_dt[case_id]:
            first_dist_dt[case_id] = dt

    print(f"  First-dist index: {len(first_dist_dt):,} cases with distributions")

    # ── Per-case metrics ──────────────────────────────────────────
    # Response
    resp_hours_all   = []
    resp_monthly     = defaultdict(list)
    resp_pri         = defaultdict(list)
    resp_pri_monthly = defaultdict(lambda: defaultdict(list))
    resp_sla_data    = defaultdict(lambda: {"within":0,"total":0})

    # Resolution
    resol_hours_all   = []
    resol_monthly     = defaultdict(list)
    resol_pri         = defaultdict(list)
    resol_pri_monthly = defaultdict(lambda: defaultdict(list))
    resol_sla_data    = defaultdict(lambda: {"within":0,"total":0})

    # Closure time (for trend charts + exec summary)
    close_times       = []
    close_monthly     = defaultdict(list)
    close_pri         = defaultdict(list)
    close_pri_monthly = defaultdict(lambda: defaultdict(list))
    outcome_close     = defaultdict(list)
    outcome_monthly   = defaultdict(lambda: defaultdict(list))

    # Throughput
    monthly_created = defaultdict(int)
    monthly_closed  = defaultdict(int)
    funding_total   = 0
    closed_total    = 0

    # Backlog
    backlog_cases = []

    for c in all_cases:
        cid         = c.get("id","").strip()
        created_dt  = zac.parse_dt(c.get("created_time",""))
        stage       = (c.get("stage") or "").strip()
        priority    = normalise_priority(c.get("case_urgency",""))
        is_closed   = stage in CLOSED_STAGES
        closing_raw = c.get("closing_date","") or c.get("modified_time","")
        closure_dt  = zac.parse_dt(closing_raw)

        if not created_dt: continue

        created_mk = zac.month_key(created_dt)

        # Throughput — created count
        if created_mk in months:
            monthly_created[created_mk] += 1

        # ── Response time ─────────────────────────────────────────
        if created_dt >= cutoff and created_mk in months:
            first_note = first_note_dt.get(cid)
            if first_note:
                cfg = SLA_RESPONSE.get(priority)
                if cfg and cfg.get("business_hours_only"):
                    resp_h = business_hours(created_dt, first_note)
                else:
                    resp_h = calendar_hours(created_dt, first_note)

                if resp_h is not None:
                    resp_hours_all.append(resp_h)
                    resp_monthly[created_mk].append(resp_h)
                    resp_pri[priority].append(resp_h)
                    resp_pri_monthly[priority][created_mk].append(resp_h)

                    within, target, _ = sla_check_response(resp_h, priority)
                    if target is not None:
                        resp_sla_data[priority]["total"] += 1
                        if within: resp_sla_data[priority]["within"] += 1

        # ── Resolution time ───────────────────────────────────────
        if created_dt >= cutoff and created_mk in months:
            # Resolution = earliest of: closure OR first distribution
            resolution_candidates = []
            if closure_dt and is_closed:
                resolution_candidates.append(closure_dt)
            dist_dt = first_dist_dt.get(cid)
            if dist_dt:
                resolution_candidates.append(dist_dt)

            if resolution_candidates:
                resolution_dt = min(resolution_candidates)
                resol_h = calendar_hours(created_dt, resolution_dt)

                if resol_h is not None:
                    resol_hours_all.append(resol_h)
                    resol_monthly[created_mk].append(resol_h)
                    resol_pri[priority].append(resol_h)
                    resol_pri_monthly[priority][created_mk].append(resol_h)

                    within, target = sla_check_resolution(resol_h, priority)
                    if target is not None:
                        resol_sla_data[priority]["total"] += 1
                        if within: resol_sla_data[priority]["within"] += 1

        # ── Closure time (closed cases only) ─────────────────────
        if is_closed and closure_dt:
            closure_mk = zac.month_key(closure_dt)
            if closure_mk in months:
                monthly_closed[closure_mk] += 1
                closed_total += 1
                if stage == "Closed - Funded": funding_total += 1

                close_d = days_between(created_dt, closure_dt)
                if close_d is not None and close_d >= 0:
                    close_times.append(close_d)
                    close_monthly[closure_mk].append(close_d)
                    close_pri[priority].append(close_d)
                    close_pri_monthly[priority][closure_mk].append(close_d)
                    outcome_close[stage].append(close_d)
                    outcome_monthly[stage][closure_mk].append(close_d)

        # ── Backlog ───────────────────────────────────────────────
        if not is_closed:
            backlog_cases.append({
                "age_days": (now - created_dt).days,
                "priority": priority,
            })

    print(f"  Cases processed: {len(all_cases):,}")
    print(f"    With response time:       {len(resp_hours_all):,}")
    print(f"    With resolution time:     {len(resol_hours_all):,}")
    print(f"    Closed in window:         {len(close_times):,}")
    print(f"    Open (backlog):           {len(backlog_cases):,}")

    # ── SLA compliance ────────────────────────────────────────────
    def sla_pct(sla_data):
        result = {}
        for p in PRIORITY_ORDER:
            d = sla_data[p]
            result[p] = round(d["within"]/d["total"]*100, 1) if d["total"] else None
        return result

    resp_sla_pct   = sla_pct(resp_sla_data)
    resol_sla_pct  = sla_pct(resol_sla_data)

    def overall_sla_pct(sla_data, sla_config):
        total = sum(v["total"]  for p,v in sla_data.items() if sla_config.get(p))
        within= sum(v["within"] for p,v in sla_data.items() if sla_config.get(p))
        return round(within/total*100, 1) if total else None

    resp_overall   = overall_sla_pct(resp_sla_data,  SLA_RESPONSE)
    resol_overall  = overall_sla_pct(resol_sla_data, SLA_RESOLUTION)

    # ── Trend series ──────────────────────────────────────────────
    resp_monthly_median  = {m: safe_median(resp_monthly.get(m,[])) for m in months}
    resol_monthly_median = {m: safe_median(resol_monthly.get(m,[])) for m in months}
    close_monthly_median = {m: safe_median(close_monthly.get(m,[])) for m in months}

    def make_pri_trend(pri_monthly_dict):
        return {
            "months": months,
            "series": [
                {
                    "priority": p,
                    "colour":   PRIORITY_COLOURS.get(p,"#9F9393"),
                    "data":     [safe_median(pri_monthly_dict[p].get(m,[])) for m in months],
                }
                for p in PRIORITY_ORDER
            ]
        }

    # Throughput / funding rate
    funding_rate_monthly = {}
    for m in months:
        n_closed = monthly_closed.get(m,0)
        if n_closed > 0:
            n_funded = sum(
                1 for c in all_cases
                if zac.month_key(zac.parse_dt(
                    c.get("closing_date","") or c.get("modified_time","")
                )) == m and c.get("stage","") == "Closed - Funded"
            )
            funding_rate_monthly[m] = round(n_funded/n_closed*100, 1)

    # Backlog bands
    age_bands = [("0–7 days",0,7),("8–14 days",8,14),("15–30 days",15,30),
                 ("31–60 days",31,60),("60+ days",61,None)]
    def age_band(d):
        for lbl,lo,hi in age_bands:
            if hi is None and d>=lo: return lbl
            if hi is not None and lo<=d<=hi: return lbl
        return "0–7 days"

    backlog_by_age = defaultdict(int)
    backlog_by_pri = defaultdict(int)
    for bc in backlog_cases:
        backlog_by_age[age_band(bc["age_days"])] += 1
        backlog_by_pri[bc["priority"]]           += 1

    curr_resp_h   = safe_median(resp_monthly.get(current_month,[]))
    curr_resol_h  = safe_median(resol_monthly.get(current_month,[]))

    return {
        "meta": {
            "last_updated":      datetime.now(timezone.utc).isoformat(),
            "months_covered":    months,
            "current_month":     current_month,
            "cases_analysed":    len(all_cases),
            "notes_analysed":    len(all_notes),
            "automated_excluded":auto_excluded,
        },

        "executive_summary": {
            "resp_median_hours":     curr_resp_h,
            "resol_median_hours":    curr_resol_h,
            "resp_sla_pct":          resp_overall,
            "resol_sla_pct":         resol_overall,
            "funding_rate_pct":      round(funding_total/closed_total*100,1) if closed_total else None,
            "backlog_over_30d":      sum(v for b,v in backlog_by_age.items() if "31" in b or "60+" in b),
            "resp_trend":            compute_trend(resp_monthly_median,  months)["direction"],
            "resol_trend":           compute_trend(resol_monthly_median, months)["direction"],
            "close_trend":           compute_trend(close_monthly_median, months)["direction"],
        },

        "sla_definitions": {
            "response":   {p: SLA_RESPONSE[p]   for p in PRIORITY_ORDER},
            "resolution": {p: SLA_RESOLUTION[p] for p in PRIORITY_ORDER},
        },

        "response_sla": {
            "overall_compliance_pct": resp_overall,
            "by_priority": {
                p: {
                    "median_hours":     safe_median(resp_pri[p]),
                    "avg_hours":        safe_mean(resp_pri[p]),
                    "count":            len(resp_pri[p]),
                    "sla_config":       SLA_RESPONSE.get(p),
                    "sla_compliance":   resp_sla_pct[p],
                }
                for p in PRIORITY_ORDER
            },
            "monthly_trend":       resp_monthly_median,
            "priority_trend_series": make_pri_trend(resp_pri_monthly),
        },

        "resolution_sla": {
            "overall_compliance_pct": resol_overall,
            "by_priority": {
                p: {
                    "median_hours":     safe_median(resol_pri[p]),
                    "avg_hours":        safe_mean(resol_pri[p]),
                    "count":            len(resol_pri[p]),
                    "sla_config":       SLA_RESOLUTION.get(p),
                    "sla_compliance":   resol_sla_pct[p],
                }
                for p in PRIORITY_ORDER
            },
            "monthly_trend":       resol_monthly_median,
            "priority_trend_series": make_pri_trend(resol_pri_monthly),
        },

        "closure_time": {
            "overall": {
                "median_days": safe_median(close_times),
                "p75_days":    safe_p75(close_times),
                "count":       len(close_times),
                "trend":       compute_trend(close_monthly_median, months),
            },
            "by_outcome": {
                OUTCOME_SHORT.get(s,s): {
                    "stage":       s,
                    "median_days": safe_median(outcome_close[s]),
                    "avg_days":    safe_mean(outcome_close[s]),
                    "p75_days":    safe_p75(outcome_close[s]),
                    "count":       len(outcome_close[s]),
                    "colour":      OUTCOME_COLOURS.get(s,"#9F9393"),
                }
                for s in CLOSED_STAGES
            },
            "by_priority": {
                p: {
                    "median_days": safe_median(close_pri[p]),
                    "avg_days":    safe_mean(close_pri[p]),
                    "count":       len(close_pri[p]),
                }
                for p in PRIORITY_ORDER
            },
            "monthly_trend":         close_monthly_median,
            "priority_trend_series": make_pri_trend(close_pri_monthly),
            "outcome_trend_series":  {
                "months": months,
                "series": [
                    {
                        "stage":  s,
                        "label":  OUTCOME_SHORT.get(s,s),
                        "colour": OUTCOME_COLOURS.get(s,"#9F9393"),
                        "data":   [safe_median(outcome_monthly[s].get(m,[])) for m in months],
                    }
                    for s in CLOSED_STAGES
                ]
            },
        },

        "throughput": {
            "monthly": [
                {
                    "month":   m,
                    "created": monthly_created.get(m,0),
                    "closed":  monthly_closed.get(m,0),
                }
                for m in months
            ],
            "funding_rate_monthly": {m: funding_rate_monthly.get(m) for m in months},
            "funding_rate_12m":     round(funding_total/closed_total*100,1) if closed_total else None,
        },

        "backlog": {
            "total_open": len(backlog_cases),
            "over_30d":   sum(v for b,v in backlog_by_age.items() if "31" in b or "60+" in b),
            "by_age": [
                {"band": band, "count": backlog_by_age.get(band,0)}
                for band,_,_ in age_bands
            ],
            "by_priority": {p: backlog_by_pri.get(p,0) for p in PRIORITY_ORDER},
        },
    }


def main():
    print("═" * 55)
    print("NZF — Cases Performance  |  Zoho Analytics")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("═" * 55)

    token = zac.get_access_token()
    data  = build_report(token)

    out = os.path.join(DATA_DIR, "cases_perf.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    es = data["executive_summary"]
    print(f"\n✅ cases_perf.json written")
    print(f"\n   ── Executive Summary ──")
    print(f"   Response SLA compliance:   {es['resp_sla_pct']}%  (trend: {es['resp_trend']})")
    print(f"   Resolution SLA compliance: {es['resol_sla_pct']}%  (trend: {es['resol_trend']})")
    print(f"   Median response (curr mo): {es['resp_median_hours']} hrs")
    print(f"   Median resolution (curr):  {es['resol_median_hours']} hrs")
    print(f"   Funding rate:              {es['funding_rate_pct']}%")
    print(f"   Backlog >30d:              {es['backlog_over_30d']}")
    print(f"\n   ── Response SLA by Priority ──")
    for p in data["response_sla"]["by_priority"]:
        d   = data["response_sla"]["by_priority"][p]
        cfg = d.get("sla_config") or {}
        tgt = f"{cfg.get('display','—')}" if cfg else "No SLA"
        sla = d.get("sla_compliance")
        biz = " (business hrs)" if cfg.get("business_hours_only") else ""
        print(f"   {p:12}: {d['median_hours']} hrs median  "
              f"SLA={sla}%  target={tgt}{biz}  n={d['count']}")
    print(f"\n   ── Resolution SLA by Priority ──")
    for p in data["resolution_sla"]["by_priority"]:
        d   = data["resolution_sla"]["by_priority"][p]
        cfg = d.get("sla_config") or {}
        tgt = cfg.get("display","—") if cfg else "No SLA"
        sla = d.get("sla_compliance")
        print(f"   {p:12}: {d['median_hours']} hrs median  "
              f"SLA={sla}%  target={tgt}  n={d['count']}")
    print("═" * 55)


if __name__ == "__main__":
    main()
