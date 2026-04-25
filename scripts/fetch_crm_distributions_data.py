"""
fetch_crm_distributions_data.py
────────────────────────────────
Builds /data/crm_distributions.json for the CRM Distributions dashboard.
Pure CRM data — no Xero references.

Sections of data produced:
  - meta:        FY context, last updated, available types
  - ytd:         Current FY YTD vs prior FY YTD (5 KPIs + cumulative)
  - monthly:     12-month trend by distribution type and transfer type
  - data_quality: Approved/Extracted unpaid > 3 days (monthly buckets + detail rows)
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

    # Fetch Cases (for Zakat Category at case level — distribution-level field is mostly null)
    all_cases = zac.fetch_view(token, zac.VIEW_CASES, label="Cases")
    case_zakat_cat = {}  # case_id (str) → list of zakat categories
    for c in all_cases:
        cid = (c.get("id") or "").strip()
        zc  = (c.get("zakat_category") or "").strip()
        if cid and zc:
            case_zakat_cat[cid] = [v.strip() for v in zc.split(";") if v.strip()]
    print(f"  Cases with Zakat Category: {len(case_zakat_cat):,}")

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
        """Returns {fy_year: [(month, count, amount, cum_count, cum_amount), ...]}."""
        result = {cur_fy: [], prv_fy: []}
        for fy in [cur_fy, prv_fy]:
            cum_count, cum_amount = 0, 0.0
            for m in fy_months(fy):
                rows = [d for d in in_scope
                        if to_month(effective_date(d)) == m
                        and (d.get("status") or "").strip() == "Paid"
                        and (dist_type_filter is None or
                             (d.get("distribution_type") or "").strip() == dist_type_filter)]
                cnt = len(rows)
                amt = sum(parse_amount(r.get("grand_total")) for r in rows)
                cum_count += cnt
                cum_amount += amt
                result[fy].append({
                    "month": m, "count": cnt, "amount": round(amt, 2),
                    "cum_count": cum_count, "cum_amount": round(cum_amount, 2),
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
        bucket = {
            "total":     {"count": 0, "amount": 0.0},
            "transfer":  defaultdict(lambda: {"count": 0, "amount": 0.0}),
            "dist_type": defaultdict(lambda: {"count": 0, "amount": 0.0}),
        }
        for d in in_scope:
            if (d.get("status") or "").strip() != "Paid":
                continue
            if to_month(effective_date(d)) != m:
                continue
            amt = parse_amount(d.get("grand_total"))
            tt  = (d.get("transfer_type") or "Not specified").strip()
            dt  = (d.get("distribution_type") or "Unknown").strip()
            bucket["total"]["count"] += 1
            bucket["total"]["amount"] += amt
            bucket["transfer"][tt]["count"] += 1
            bucket["transfer"][tt]["amount"] += amt
            bucket["dist_type"][dt]["count"] += 1
            bucket["dist_type"][dt]["amount"] += amt
        # Round and convert to dict
        bucket["total"]["amount"] = round(bucket["total"]["amount"], 2)
        bucket["transfer"]  = {k: {"count": v["count"], "amount": round(v["amount"], 2)}
                               for k, v in bucket["transfer"].items()}
        bucket["dist_type"] = {k: {"count": v["count"], "amount": round(v["amount"], 2)}
                               for k, v in bucket["dist_type"].items()}
        monthly[m] = bucket

    # ── Data Quality: stalled distributions ─────────────────────────
    print("  Computing data quality stalls…")
    THREE_DAYS_HOURS = 72
    quality_by_month = {}
    for m in trend_months:
        approved_unpaid = []   # Approved but not Paid > 3 days
        extracted_unpaid = []  # Extracted but not Paid > 3 days
        for d in in_scope:
            if to_month(effective_date(d)) != m:
                continue
            status_l = (d.get("status") or "").lower().strip()
            if status_l == "paid":
                continue
            base = (zac.parse_dt(d.get("approved_date") or "")
                    or zac.parse_dt(d.get("created_time") or ""))
            if not base:
                continue
            hours_elapsed = (now_utc - base).total_seconds() / 3600
            if hours_elapsed <= THREE_DAYS_HOURS:
                continue
            row = {
                "dist_id":      (d.get("distribution_id") or "").strip(),
                "record_id":    (d.get("id") or "").strip(),
                "subject":      d.get("subject", ""),
                "payee":        d.get("acc_name", "") or d.get("vendor_name", ""),
                "amount":       parse_amount(d.get("grand_total")),
                "distribution_type": (d.get("distribution_type") or "").strip(),
                "transfer_type":(d.get("transfer_type") or "").strip(),
                "crm_status":   (d.get("status") or "").strip(),
                "created_time": d.get("created_time", ""),
                "approved_date":d.get("approved_date", ""),
                "extracted_date":d.get("extracted_date", ""),
                "_hours":       round(hours_elapsed),
                "_days":        round(hours_elapsed / 24, 1),
            }
            if status_l == "approved":
                approved_unpaid.append(row)
            elif status_l == "extracted":
                extracted_unpaid.append(row)
        quality_by_month[m] = {
            "approved_unpaid_3d": {
                "count": len(approved_unpaid),
                "amount": round(sum(r["amount"] for r in approved_unpaid), 2),
                "rows": approved_unpaid[:200],  # cap detail rows
            },
            "extracted_unpaid_3d": {
                "count": len(extracted_unpaid),
                "amount": round(sum(r["amount"] for r in extracted_unpaid), 2),
                "rows": extracted_unpaid[:200],
            },
        }

    # ── Breakdowns by category, program, state ─────────────────────
    print("  Computing breakdowns…")

    def breakdown(field_extractor, top_n=15):
        agg = defaultdict(lambda: {"count": 0, "amount": 0.0})
        for d in in_scope:
            if (d.get("status") or "").strip() != "Paid":
                continue
            label = field_extractor(d)
            if not label:
                continue
            agg[label]["count"] += 1
            agg[label]["amount"] += parse_amount(d.get("grand_total"))
        rows = [{"label": k, "count": v["count"], "amount": round(v["amount"], 2)}
                for k, v in agg.items()]
        rows.sort(key=lambda r: r["amount"], reverse=True)
        return rows[:top_n]

    # Zakat Category — sourced from the parent Case (Deal), not the Distribution.
    # Multi-select: if a case has 2 categories (e.g. "Masakin;Fuqarah"), the distribution
    # amount is split evenly across them — accurate, sums to 100% of total.
    zakat_agg = defaultdict(lambda: {"count": 0, "amount": 0.0})
    for d in in_scope:
        if (d.get("status") or "").strip() != "Paid":
            continue
        case_id = (d.get("case_name") or "").strip()  # FK to Cases table
        cats    = case_zakat_cat.get(case_id, [])
        if not cats:
            continue
        amt   = parse_amount(d.get("grand_total"))
        share = amt / len(cats)
        for c in cats:
            zakat_agg[c]["count"]  += 1 / len(cats)  # fractional count
            zakat_agg[c]["amount"] += share
    zakat_rows = [{"label": k, "count": round(v["count"], 1), "amount": round(v["amount"], 2)}
                  for k, v in zakat_agg.items()]
    zakat_rows.sort(key=lambda r: r["amount"], reverse=True)

    # Product Category — sourced from Purchase Items (line item Product Display Name)
    # A distribution can have multiple line items (e.g. Rent + Groceries on same dist).
    # Each line item attributes its own Amount to its own product category.
    product_agg = defaultdict(lambda: {"count": 0, "amount": 0.0})
    paid_dist_ids = {(d.get("id") or "").strip()
                     for d in in_scope
                     if (d.get("status") or "").strip() == "Paid"}
    for dist_id, items in items_by_dist.items():
        if dist_id not in paid_dist_ids:
            continue
        for it in items:
            label = (it.get("product_display_name") or "").strip()
            if not label:
                continue
            amt = parse_amount(it.get("amount") or it.get("total_after_discount"))
            product_agg[label]["count"]  += 1
            product_agg[label]["amount"] += amt
    product_rows = [{"label": k, "count": v["count"], "amount": round(v["amount"], 2)}
                    for k, v in product_agg.items()]
    product_rows.sort(key=lambda r: r["amount"], reverse=True)

    breakdowns = {
        "zakat_category": zakat_rows[:15],
        "product_category": product_rows[:15],
        "program": breakdown(lambda d: (d.get("program") or "").strip() or None),
        "state":   breakdown(lambda d: normalise_state(d.get("contact_name_state")
                                                       or d.get("state") or "")),
    }

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
    for d in in_scope:
        status_l = (d.get("status") or "").lower().strip()
        if status_l == "paid": continue
        amt = parse_amount(d.get("grand_total"))
        base = (zac.parse_dt(d.get("approved_date") or "")
                or zac.parse_dt(d.get("created_time") or ""))
        if not base: continue
        hours = (now_utc - base).total_seconds() / 3600
        if status_l == "approved":
            pipeline["approved_unpaid"]["count"] += 1
            pipeline["approved_unpaid"]["amount"] += amt
        elif status_l == "extracted":
            pipeline["extracted_unpaid"]["count"] += 1
            pipeline["extracted_unpaid"]["amount"] += amt
        # Age bucket (only for non-paid)
        if hours < 72:           pipeline["ages"]["0_3d"] += 1
        elif hours < 168:        pipeline["ages"]["3_7d"] += 1
        elif hours < 336:        pipeline["ages"]["7_14d"] += 1
        else:                    pipeline["ages"]["14d_plus"] += 1
    pipeline["approved_unpaid"]["amount"]  = round(pipeline["approved_unpaid"]["amount"], 2)
    pipeline["extracted_unpaid"]["amount"] = round(pipeline["extracted_unpaid"]["amount"], 2)

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
        "data_quality": quality_by_month,
        "breakdowns": breakdowns,
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
