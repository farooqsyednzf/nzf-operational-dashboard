"""
fetch_crm_distributions_data.py
────────────────────────────────
Builds /data/crm_distributions.json for the CRM Distributions dashboard.
Pure CRM data — no Xero references.

Sections of data produced:
  - meta:        FY context, last updated, available types
  - ytd:         Current FY YTD vs prior FY YTD (5 KPIs + cumulative)
  - monthly:     12-month trend by distribution type and transfer type
  - breakdowns:  By Zakat Category, by Program, by State
  - alerts:      Rule-based fraud detection (7 heuristics)
"""

import os, sys, json, re
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from statistics import median

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR     = os.path.join(os.path.dirname(__file__), "..", "data")
CRM_BASE_URL = zac.RULES["_meta"].get("zoho_crm_base_url", "")

# Status filter — only relevant for in-flight distributions
INCLUDED_STATUSES = {"approved", "extracted", "paid"}
LOOKBACK_MONTHS   = 13   # 12 months trend + current month for FY rollover edge cases

# Fraud thresholds (anchor amounts where threshold-splitting tends to occur)
THRESHOLD_AMOUNTS = [500, 1000, 1500, 2000, 3000, 5000]
THRESHOLD_TOLERANCE = 50  # $X within $50 below threshold is suspicious

# ── Helpers ────────────────────────────────────────────────────────

def parse_amount(s):
    if s is None or s == "":
        return 0.0
    return float(re.sub(r"[^0-9.]", "", str(s))) or 0.0


def to_month(s):
    """Return 'YYYY-MM' for a string OR a datetime, or None."""
    if s is None or s == "":
        return None
    # If already a datetime, format directly
    if isinstance(s, datetime):
        return s.strftime("%Y-%m")
    dt = zac.parse_dt(s)
    return dt.strftime("%Y-%m") if dt else None


def effective_date(d):
    """Use Paid_Date for Paid, Extracted_Date for Extracted, fallback to Created_Time."""
    status = (d.get("status") or "").strip()
    if status == "Paid":
        dt = zac.parse_dt(d.get("paid_date") or "")
        if dt: return dt
    elif status == "Extracted":
        dt = zac.parse_dt(d.get("extracted_date") or "")
        if dt: return dt
    return zac.parse_dt(d.get("created_time") or "")


def fy_start_year(dt):
    """Return the calendar year that this FY started (Jul-Jun). dt is a datetime."""
    return dt.year if dt.month >= 7 else dt.year - 1


def fy_label(start_year):
    return f"FY{str(start_year)[2:]}/{str(start_year + 1)[2:]}"


def fy_months(start_year):
    """All 12 months of a FY in YYYY-MM format, Jul through Jun."""
    months = []
    for i in range(12):
        m = 7 + i
        y = start_year + (1 if m > 12 else 0)
        if m > 12: m -= 12
        months.append(f"{y}-{m:02d}")
    return months


def normalise_state(s):
    if not s:
        return "Unknown"
    s = str(s).upper().strip()
    state_map = {
        "NEW SOUTH WALES": "NSW", "VICTORIA": "VIC", "QUEENSLAND": "QLD",
        "WESTERN AUSTRALIA": "WA", "SOUTH AUSTRALIA": "SA", "TASMANIA": "TAS",
        "AUSTRALIAN CAPITAL TERRITORY": "ACT", "NORTHERN TERRITORY": "NT",
    }
    if s in state_map: return state_map[s]
    if s in {"NSW","VIC","QLD","WA","SA","TAS","ACT","NT"}: return s
    return "Unknown"


# ── Main build ─────────────────────────────────────────────────────

def build_crm_distributions_report(token):
    print("\n=== CRM Distributions Report ===")
    now_utc = datetime.now(timezone.utc)
    cutoff  = now_utc - timedelta(days=LOOKBACK_MONTHS * 31)

    # Fetch full Distributions view
    all_dists = zac.fetch_view(token, zac.VIEW_DISTRIBUTIONS, label="Distributions")

    # Fetch Programs (id → human-readable name lookup)
    all_programs = zac.fetch_view(token, zac.VIEW_PROGRAMS, label="Programs")
    program_name_map = {}  # program_id → program_name
    for p in all_programs:
        pid  = (p.get("id") or "").strip()
        name = (p.get("program_name") or "").strip()
        if pid and name:
            program_name_map[pid] = name
    print(f"  Programs in lookup: {len(program_name_map):,}")

    # Fetch Clients (for State — distribution joins via Client Name FK)
    all_clients = zac.fetch_view(token, zac.VIEW_CLIENTS, label="Clients")
    client_state_map = {}  # client_id → mailing state
    for c in all_clients:
        cid   = (c.get("id") or "").strip()
        state = (c.get("mailing_state") or c.get("state") or "").strip()
        if cid and state:
            client_state_map[cid] = state
    print(f"  Clients with state: {len(client_state_map):,}")

    # Fetch Purchase Items (for product-level breakdown)
    all_items = zac.fetch_view(token, zac.VIEW_PURCHASE_ITEMS, label="Purchase Items")
    items_by_dist = defaultdict(list)  # parent_id (distribution id) → list of items
    for it in all_items:
        pid = (it.get("parent_id") or "").strip()
        if pid:
            items_by_dist[pid].append(it)
    print(f"  Purchase Items rows: {len(all_items):,} across {len(items_by_dist):,} distributions")

    # Filter to in-scope: Approved/Extracted/Paid, last 13 months by effective date
    in_scope = []
    for d in all_dists:
        status_l = (d.get("status") or "").lower().strip()
        if status_l not in INCLUDED_STATUSES:
            continue
        ed = effective_date(d)
        if not ed or ed < cutoff:
            continue
        in_scope.append(d)
    print(f"  In scope: {len(in_scope):,} distributions (last {LOOKBACK_MONTHS} months)")

    # Available distribution types and transfer types (for picker)
    distribution_types = sorted({(d.get("distribution_type") or "").strip()
                                  for d in in_scope if d.get("distribution_type")})
    transfer_types     = sorted({(d.get("transfer_type") or "").strip()
                                  for d in in_scope if d.get("transfer_type")})

    # FY context
    cur_fy = fy_start_year(now_utc)
    prv_fy = cur_fy - 1
    cur_fy_months = fy_months(cur_fy)
    prv_fy_months = fy_months(prv_fy)

    # Months we have actual data for
    actual_months = sorted({to_month(effective_date(d)) for d in in_scope if effective_date(d)})

    # ── YTD: cumulative by month, this FY vs prior FY ─────────────
    # For each (FY, type, month) = total Paid amount and count
    print("  Computing YTD comparison…")

    def ytd_data(dist_type_filter):
        """Per-FY cumulative series with unique client/case tracking."""
        result = {cur_fy: [], prv_fy: []}
        for fy in [cur_fy, prv_fy]:
            cum_count, cum_amount = 0, 0.0
            cum_clients, cum_cases = set(), set()
            for m in fy_months(fy):
                rows = [d for d in in_scope
                        if to_month(effective_date(d)) == m
                        and (d.get("status") or "").strip() == "Paid"
                        and (dist_type_filter is None or
                             (d.get("distribution_type") or "").strip() == dist_type_filter)]
                cnt = len(rows)
                amt = sum(parse_amount(r.get("grand_total")) for r in rows)
                month_clients = {(r.get("client_name") or "").strip() for r in rows}
                month_cases   = {(r.get("case_name")   or "").strip() for r in rows}
                month_clients.discard("")
                month_cases.discard("")
                cum_count  += cnt
                cum_amount += amt
                cum_clients.update(month_clients)
                cum_cases.update(month_cases)
                result[fy].append({
                    "month": m, "count": cnt, "amount": round(amt, 2),
                    "cum_count": cum_count, "cum_amount": round(cum_amount, 2),
                    "unique_clients":     len(month_clients),
                    "unique_cases":       len(month_cases),
                    "cum_unique_clients": len(cum_clients),
                    "cum_unique_cases":   len(cum_cases),
                })
        return result

    ytd = {"All": ytd_data(None)}
    for dt in distribution_types:
        ytd[dt] = ytd_data(dt)

    # Months elapsed in current FY (for fair comparison header)
    months_elapsed = len([m for m in cur_fy_months if m in actual_months])

    # ── Monthly trend (last 12 months) by type and transfer ────────
    print("  Computing monthly trend…")
    trend_months = sorted(set(actual_months))[-12:] if actual_months else []

    monthly = {}
    for m in trend_months:
        total_clients, total_cases = set(), set()
        bucket = {
            "total":     {"count": 0, "amount": 0.0},
            "transfer":  defaultdict(lambda: {"count": 0, "amount": 0.0,
                                                "clients": set(), "cases": set()}),
            "dist_type": defaultdict(lambda: {"count": 0, "amount": 0.0,
                                                "clients": set(), "cases": set()}),
        }
        for d in in_scope:
            if (d.get("status") or "").strip() != "Paid":
                continue
            if to_month(effective_date(d)) != m:
                continue
            amt = parse_amount(d.get("grand_total"))
            tt  = (d.get("transfer_type") or "Not specified").strip()
            dt  = (d.get("distribution_type") or "Unknown").strip()
            client = (d.get("client_name") or "").strip()
            case   = (d.get("case_name")   or "").strip()
            bucket["total"]["count"]  += 1
            bucket["total"]["amount"] += amt
            bucket["transfer"][tt]["count"]  += 1
            bucket["transfer"][tt]["amount"] += amt
            bucket["dist_type"][dt]["count"]  += 1
            bucket["dist_type"][dt]["amount"] += amt
            if client:
                total_clients.add(client)
                bucket["transfer"][tt]["clients"].add(client)
                bucket["dist_type"][dt]["clients"].add(client)
            if case:
                total_cases.add(case)
                bucket["transfer"][tt]["cases"].add(case)
                bucket["dist_type"][dt]["cases"].add(case)
        # Round and convert to dict
        bucket["total"]["amount"]         = round(bucket["total"]["amount"], 2)
        bucket["total"]["unique_clients"] = len(total_clients)
        bucket["total"]["unique_cases"]   = len(total_cases)
        bucket["transfer"]  = {k: {"count": v["count"], "amount": round(v["amount"], 2),
                                     "unique_clients": len(v["clients"]),
                                     "unique_cases":   len(v["cases"])}
                                for k, v in bucket["transfer"].items()}
        bucket["dist_type"] = {k: {"count": v["count"], "amount": round(v["amount"], 2),
                                     "unique_clients": len(v["clients"]),
                                     "unique_cases":   len(v["cases"])}
                                for k, v in bucket["dist_type"].items()}
        monthly[m] = bucket

    # ── Breakdowns by category, program, state ─────────────────────
    # Computed for every (month, distribution_type) combination so the dashboard
    # can show the right slice for any tab + filter selection.
    print("  Computing breakdowns…")

    paid_in_scope = [d for d in in_scope if (d.get("status") or "").strip() == "Paid"]

    def compute_breakdowns_for(dists):
        """Compute all 4 breakdowns from a filtered list of paid distributions.
        Each breakdown row carries amount, count, unique_clients, unique_cases."""
        dist_id_set = {(d.get("id") or "").strip() for d in dists}

        # Zakat Category — split amount equally across multi-select categories.
        zakat_agg = defaultdict(lambda: {"count": 0.0, "amount": 0.0,
                                          "clients": set(), "cases": set()})
        for d in dists:
            raw = (d.get("zakat_category_ies") or "").strip()
            if not raw: continue
            cats = [c.strip() for c in raw.split(";") if c.strip()]
            if not cats: continue
            amt   = parse_amount(d.get("grand_total"))
            share = amt / len(cats)
            cnt_s = 1 / len(cats)
            client = (d.get("client_name") or "").strip()
            case   = (d.get("case_name")   or "").strip()
            for c in cats:
                zakat_agg[c]["count"]  += cnt_s
                zakat_agg[c]["amount"] += share
                if client: zakat_agg[c]["clients"].add(client)
                if case:   zakat_agg[c]["cases"].add(case)
        zakat_rows = [{"label": k, "count": round(v["count"], 1),
                       "amount": round(v["amount"], 2),
                       "unique_clients": len(v["clients"]),
                       "unique_cases":   len(v["cases"])}
                      for k, v in zakat_agg.items()]
        zakat_rows.sort(key=lambda r: r["amount"], reverse=True)

        # Product Category — line items via Purchase Items
        product_agg = defaultdict(lambda: {"count": 0, "amount": 0.0,
                                            "clients": set(), "cases": set()})
        # Build dist_id → row for client/case lookup on items
        dist_lookup = {(d.get("id") or "").strip(): d for d in dists}
        for dist_id in dist_id_set:
            if dist_id not in items_by_dist: continue
            d = dist_lookup.get(dist_id, {})
            client = (d.get("client_name") or "").strip()
            case   = (d.get("case_name")   or "").strip()
            for it in items_by_dist[dist_id]:
                label = (it.get("product_display_name") or "").strip()
                if not label: continue
                amt = parse_amount(it.get("amount") or it.get("total_after_discount"))
                product_agg[label]["count"]  += 1
                product_agg[label]["amount"] += amt
                if client: product_agg[label]["clients"].add(client)
                if case:   product_agg[label]["cases"].add(case)
        product_rows = [{"label": k, "count": v["count"],
                         "amount": round(v["amount"], 2),
                         "unique_clients": len(v["clients"]),
                         "unique_cases":   len(v["cases"])}
                        for k, v in product_agg.items()]
        product_rows.sort(key=lambda r: r["amount"], reverse=True)

        # Program — resolve ID to readable name
        program_agg = defaultdict(lambda: {"count": 0, "amount": 0.0,
                                             "clients": set(), "cases": set()})
        for d in dists:
            pid = (d.get("program") or "").strip()
            if not pid: continue
            name   = program_name_map.get(pid, f"Unknown ({pid[-6:]})")
            amt    = parse_amount(d.get("grand_total"))
            client = (d.get("client_name") or "").strip()
            case   = (d.get("case_name")   or "").strip()
            program_agg[name]["count"]  += 1
            program_agg[name]["amount"] += amt
            if client: program_agg[name]["clients"].add(client)
            if case:   program_agg[name]["cases"].add(case)
        program_rows = [{"label": k, "count": v["count"],
                         "amount": round(v["amount"], 2),
                         "unique_clients": len(v["clients"]),
                         "unique_cases":   len(v["cases"])}
                        for k, v in program_agg.items()]
        program_rows.sort(key=lambda r: r["amount"], reverse=True)

        # State — via Client.Mailing_State lookup
        state_agg = defaultdict(lambda: {"count": 0, "amount": 0.0,
                                           "clients": set(), "cases": set()})
        for d in dists:
            cid  = (d.get("client_name") or "").strip()
            norm = normalise_state(client_state_map.get(cid, ""))
            if norm == "Unknown": continue
            amt    = parse_amount(d.get("grand_total"))
            client = cid
            case   = (d.get("case_name") or "").strip()
            state_agg[norm]["count"]  += 1
            state_agg[norm]["amount"] += amt
            if client: state_agg[norm]["clients"].add(client)
            if case:   state_agg[norm]["cases"].add(case)
        state_rows = [{"label": k, "count": v["count"],
                       "amount": round(v["amount"], 2),
                       "unique_clients": len(v["clients"]),
                       "unique_cases":   len(v["cases"])}
                      for k, v in state_agg.items()]
        state_rows.sort(key=lambda r: r["amount"], reverse=True)

        return {
            "zakat_category":   zakat_rows[:15],
            "product_category": product_rows[:15],
            "program":          program_rows[:15],
            "state":            state_rows[:15],
        }

    # Pre-bucket paid distributions by (month, type) for fast lookup
    by_m_t = defaultdict(list)
    for d in paid_in_scope:
        m = to_month(effective_date(d))
        t = (d.get("distribution_type") or "").strip()
        if not m: continue
        by_m_t[(m, t)].append(d)

    # All combinations of {Total, each month} × {All, each type}
    breakdowns_by_month = {}
    type_options = ["All"] + sorted({t for _, t in by_m_t.keys() if t})
    for m_key in ["Total"] + trend_months:
        breakdowns_by_month[m_key] = {}
        for t_key in type_options:
            if m_key == "Total" and t_key == "All":
                dists = paid_in_scope
            elif m_key == "Total":
                dists = [d for d in paid_in_scope
                         if (d.get("distribution_type") or "").strip() == t_key]
            elif t_key == "All":
                dists = [d for d in paid_in_scope
                         if to_month(effective_date(d)) == m_key]
            else:
                dists = by_m_t.get((m_key, t_key), [])
            breakdowns_by_month[m_key][t_key] = compute_breakdowns_for(dists)
    print(f"  Breakdowns: {len(breakdowns_by_month)} month keys × "
          f"{len(type_options)} type keys = {len(breakdowns_by_month)*len(type_options)} cells")

    # Keep top-level "breakdowns" as Total/All for any consumer that needs the simple shape
    breakdowns = breakdowns_by_month["Total"]["All"]

    # ── Fraud / anomaly alerts (rule-based) ────────────────────────
    print("  Computing fraud alerts…")
    alerts = []
    crm_link = lambda rec_id: f"{CRM_BASE_URL}/PurchaseOrders/{rec_id}" if rec_id and CRM_BASE_URL else ""

    # Helper: build alert distribution detail
    def alert_dist(d):
        return {
            "dist_id":     (d.get("distribution_id") or "").strip(),
            "record_id":   (d.get("id") or "").strip(),
            "amount":      parse_amount(d.get("grand_total")),
            "payee":       d.get("acc_name", "") or d.get("vendor_name", ""),
            "date":        d.get("paid_date") or d.get("approved_date") or d.get("created_time", ""),
            "status":      (d.get("status") or "").strip(),
            "owner":       (d.get("owner_name") or d.get("owner") or "").strip(),
        }

    # Rule 1: Repeat recipient — same payee in 14 days, 3+ times
    fourteen_days_ago = now_utc - timedelta(days=14)
    payee_recent = defaultdict(list)
    for d in in_scope:
        ed = effective_date(d)
        if not ed or ed < fourteen_days_ago:
            continue
        payee = (d.get("acc_name", "") or d.get("vendor_name", "")).strip().upper()
        if payee:
            payee_recent[payee].append(d)
    for payee, dists in payee_recent.items():
        if len(dists) >= 3:
            total = sum(parse_amount(d.get("grand_total")) for d in dists)
            alerts.append({
                "severity": "high",
                "type": "repeat_recipient",
                "title": f"Repeat recipient: {payee.title()}",
                "description": f"{len(dists)} distributions to same payee in last 14 days totalling ${total:,.0f}",
                "distributions": [alert_dist(d) for d in dists[:10]],
            })

    # Rule 2: Threshold splitting — multiple dists just under approval thresholds
    threshold_violations = defaultdict(list)
    thirty_days_ago = now_utc - timedelta(days=30)
    for d in in_scope:
        ed = effective_date(d)
        if not ed or ed < thirty_days_ago:
            continue
        amount = parse_amount(d.get("grand_total"))
        for thresh in THRESHOLD_AMOUNTS:
            if thresh - THRESHOLD_TOLERANCE <= amount < thresh:
                payee = (d.get("acc_name", "") or d.get("vendor_name", "")).strip().upper()
                threshold_violations[(thresh, payee)].append(d)
                break
    for (thresh, payee), dists in threshold_violations.items():
        if len(dists) >= 2 and payee:
            alerts.append({
                "severity": "high",
                "type": "threshold_splitting",
                "title": f"Possible threshold splitting (${thresh:,.0f} threshold)",
                "description": f"Payee {payee.title()} had {len(dists)} distributions just under ${thresh:,.0f} in last 30 days",
                "distributions": [alert_dist(d) for d in dists],
            })

    # Rule 3: Quick approval — Approved within 5 minutes of Created
    quick_approvals = []
    for d in in_scope:
        ed = effective_date(d)
        if not ed or ed < thirty_days_ago: continue
        ct = zac.parse_dt(d.get("created_time") or "")
        ad = zac.parse_dt(d.get("approved_date") or "")
        if not ct or not ad: continue
        delta_min = (ad - ct).total_seconds() / 60
        if 0 <= delta_min < 5:
            quick_approvals.append((d, round(delta_min, 1)))
    if quick_approvals:
        alerts.append({
            "severity": "medium",
            "type": "quick_approval",
            "title": f"{len(quick_approvals)} distributions approved within 5 minutes of creation",
            "description": "Rapid approval may indicate bypassed review process",
            "distributions": [{**alert_dist(d), "minutes_to_approve": mins}
                              for d, mins in quick_approvals[:20]],
        })

    # Rule 4: After-hours creation
    after_hours = []
    for d in in_scope:
        ct = zac.parse_dt(d.get("created_time") or "")
        if not ct or ct < thirty_days_ago: continue
        # Convert to AEST (UTC+10, ignore DST nuance for fraud detection)
        aest_hour = (ct.hour + 10) % 24
        is_weekend = ct.weekday() >= 5
        if aest_hour < 7 or aest_hour >= 19 or is_weekend:
            after_hours.append(d)
    if len(after_hours) >= 5:
        alerts.append({
            "severity": "low",
            "type": "after_hours",
            "title": f"{len(after_hours)} distributions created outside business hours",
            "description": "Distributions created outside 7AM-7PM AEST or on weekends in last 30 days",
            "distributions": [alert_dist(d) for d in after_hours[:20]],
        })

    # Rule 5: Outlier amounts (> 3× median for distribution type, last 90 days)
    ninety_days_ago = now_utc - timedelta(days=90)
    by_type = defaultdict(list)
    for d in in_scope:
        ed = effective_date(d)
        if not ed or ed < ninety_days_ago: continue
        dt = (d.get("distribution_type") or "Unknown").strip()
        amt = parse_amount(d.get("grand_total"))
        if amt > 0:
            by_type[dt].append((d, amt))
    outliers = []
    for dt, items in by_type.items():
        if len(items) < 10: continue
        amounts = [a for _, a in items]
        med = median(amounts)
        threshold = med * 3
        for d, amt in items:
            if amt > threshold:
                ed = effective_date(d)
                if ed and ed >= thirty_days_ago:  # only recent outliers
                    outliers.append((d, amt, med))
    if outliers:
        outliers.sort(key=lambda x: x[1], reverse=True)
        alerts.append({
            "severity": "medium",
            "type": "outlier_amount",
            "title": f"{len(outliers)} distributions with unusually large amounts",
            "description": "Amount exceeds 3× the median for that distribution type (last 90 days)",
            "distributions": [{**alert_dist(d), "median_for_type": round(med)}
                              for d, _, med in outliers[:20]],
        })

    # Rule 6: Status mismatch — Paid without Paid_Date or Paid_Date in future
    status_mismatch = []
    for d in in_scope:
        if (d.get("status") or "").strip() != "Paid": continue
        pd = d.get("paid_date") or ""
        if not pd:
            status_mismatch.append((d, "no_paid_date"))
            continue
        pd_dt = zac.parse_dt(pd)
        if pd_dt and pd_dt > now_utc:
            status_mismatch.append((d, "future_paid_date"))
    if status_mismatch:
        alerts.append({
            "severity": "high",
            "type": "status_mismatch",
            "title": f"{len(status_mismatch)} Paid distributions with date issues",
            "description": "Status is Paid but Paid_Date is missing or set to a future date",
            "distributions": [{**alert_dist(d), "issue": issue}
                              for d, issue in status_mismatch[:20]],
        })

    # Rule 7: Same-day caseworker spike
    by_owner_day = defaultdict(list)
    for d in in_scope:
        ct = zac.parse_dt(d.get("created_time") or "")
        if not ct or ct < ninety_days_ago: continue
        owner = (d.get("owner_name") or d.get("owner") or "").strip()
        if not owner: continue
        day = ct.strftime("%Y-%m-%d")
        by_owner_day[(owner, day)].append(d)
    # Compute per-owner median daily volume
    owner_daily_counts = defaultdict(list)
    for (owner, day), dists in by_owner_day.items():
        owner_daily_counts[owner].append(len(dists))
    owner_medians = {o: median(c) for o, c in owner_daily_counts.items() if len(c) >= 5}
    spike_days = []
    for (owner, day), dists in by_owner_day.items():
        med = owner_medians.get(owner, 0)
        if med >= 1 and len(dists) > med * 3:
            ct_first = zac.parse_dt(dists[0].get("created_time") or "")
            if ct_first and ct_first >= thirty_days_ago:
                spike_days.append((owner, day, dists, med))
    if spike_days:
        spike_days.sort(key=lambda x: len(x[2]), reverse=True)
        alerts.append({
            "severity": "low",
            "type": "owner_spike",
            "title": f"Caseworker volume spikes on {len(spike_days)} day(s)",
            "description": "Single caseworker created >3× their median daily volume",
            "distributions": [
                {**alert_dist(d), "owner_day": f"{owner} · {day}", "median_daily": round(med, 1)}
                for owner, day, dists, med in spike_days[:5]
                for d in dists[:5]
            ],
        })

    print(f"  Generated {len(alerts)} alerts")

    # Sort alerts by severity (high → medium → low)
    severity_order = {"high": 0, "medium": 1, "low": 2}
    alerts.sort(key=lambda a: severity_order.get(a["severity"], 99))

    # ── Pipeline / aging summary ───────────────────────────────────
    pipeline = {
        "approved_unpaid":  {"count": 0, "amount": 0.0},
        "extracted_unpaid": {"count": 0, "amount": 0.0},
        "ages": {"0_3d": 0, "3_7d": 0, "7_14d": 0, "14d_plus": 0},
    }
    pipe_clients = {"approved": set(), "extracted": set()}
    pipe_cases   = {"approved": set(), "extracted": set()}
    for d in in_scope:
        status_l = (d.get("status") or "").lower().strip()
        if status_l == "paid": continue
        amt = parse_amount(d.get("grand_total"))
        base = (zac.parse_dt(d.get("approved_date") or "")
                or zac.parse_dt(d.get("created_time") or ""))
        if not base: continue
        hours  = (now_utc - base).total_seconds() / 3600
        client = (d.get("client_name") or "").strip()
        case   = (d.get("case_name")   or "").strip()
        if status_l == "approved":
            pipeline["approved_unpaid"]["count"]  += 1
            pipeline["approved_unpaid"]["amount"] += amt
            if client: pipe_clients["approved"].add(client)
            if case:   pipe_cases["approved"].add(case)
        elif status_l == "extracted":
            pipeline["extracted_unpaid"]["count"]  += 1
            pipeline["extracted_unpaid"]["amount"] += amt
            if client: pipe_clients["extracted"].add(client)
            if case:   pipe_cases["extracted"].add(case)
        # Age bucket (only for non-paid)
        if hours < 72:           pipeline["ages"]["0_3d"] += 1
        elif hours < 168:        pipeline["ages"]["3_7d"] += 1
        elif hours < 336:        pipeline["ages"]["7_14d"] += 1
        else:                    pipeline["ages"]["14d_plus"] += 1
    pipeline["approved_unpaid"]["amount"]         = round(pipeline["approved_unpaid"]["amount"], 2)
    pipeline["approved_unpaid"]["unique_clients"] = len(pipe_clients["approved"])
    pipeline["approved_unpaid"]["unique_cases"]   = len(pipe_cases["approved"])
    pipeline["extracted_unpaid"]["amount"]        = round(pipeline["extracted_unpaid"]["amount"], 2)
    pipeline["extracted_unpaid"]["unique_clients"]= len(pipe_clients["extracted"])
    pipeline["extracted_unpaid"]["unique_cases"]  = len(pipe_cases["extracted"])
    # Combined unique across both stages
    pipeline["unique_clients"] = len(pipe_clients["approved"] | pipe_clients["extracted"])
    pipeline["unique_cases"]   = len(pipe_cases["approved"]   | pipe_cases["extracted"])

    # ── Velocity: avg days from Approved to Paid ───────────────────
    velocities = []
    for d in in_scope:
        if (d.get("status") or "").strip() != "Paid": continue
        ad = zac.parse_dt(d.get("approved_date") or "")
        pd = zac.parse_dt(d.get("paid_date") or "")
        if ad and pd and pd >= ad:
            velocities.append((pd - ad).total_seconds() / 86400)
    avg_velocity_days = round(sum(velocities) / len(velocities), 1) if velocities else None
    median_velocity_days = round(median(velocities), 1) if velocities else None

    return {
        "meta": {
            "last_updated":     now_utc.isoformat(),
            "current_fy":       cur_fy,
            "previous_fy":      prv_fy,
            "current_fy_label": fy_label(cur_fy),
            "previous_fy_label":fy_label(prv_fy),
            "current_fy_months":cur_fy_months,
            "previous_fy_months":prv_fy_months,
            "months_elapsed":   months_elapsed,
            "trend_months":     trend_months,
            "crm_base_url":     CRM_BASE_URL,
            "distribution_types": ["All"] + distribution_types,
            "transfer_types":   ["All"] + transfer_types,
        },
        "ytd": ytd,
        "monthly": monthly,
        "breakdowns": breakdowns,
        "breakdowns_by_month": breakdowns_by_month,
        "alerts": alerts,
        "pipeline": pipeline,
        "velocity": {
            "avg_days":    avg_velocity_days,
            "median_days": median_velocity_days,
            "sample_size": len(velocities),
        },
    }


def main():
    print("=" * 55)
    print("NZF — CRM Distributions Report")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 55)

    token = zac.get_access_token()
    data  = build_crm_distributions_report(token)

    out_path = os.path.join(DATA_DIR, "crm_distributions.json")
    with open(out_path, "w") as f:
        json.dump(data, f, default=str)

    print(f"\nDone. crm_distributions.json written")
    print(f"  Distribution types:  {data['meta']['distribution_types']}")
    print(f"  Transfer types:      {data['meta']['transfer_types']}")
    print(f"  Months in trend:     {len(data['meta']['trend_months'])}")
    print(f"  Pipeline AUD:        ${data['pipeline']['approved_unpaid']['amount']:,.0f} approved + "
          f"${data['pipeline']['extracted_unpaid']['amount']:,.0f} extracted")
    print(f"  Avg velocity:        {data['velocity']['avg_days']} days "
          f"(median {data['velocity']['median_days']})")
    print(f"  Fraud alerts:        {len(data['alerts'])}")
    print("=" * 55)


if __name__ == "__main__":
    main()
