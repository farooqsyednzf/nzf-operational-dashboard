"""
fetch_distributions_monthly_recon.py
─────────────────────────────────────
Builds /data/distributions_monthly_recon.json for the Monthly Reconciliation
section of the Distributions dashboard.

Covers 4 distribution types:
  Type 1: Created in CRM + paid by Salma through Xero   → CRM dist with matching Xero bill
  Type 2: Created in CRM + paid by distribution team    → CRM dist with no Xero bill (non-CC)
  Type 3: Not in CRM + paid by Salma through Xero       → Xero bill with no matching CRM dist (NZF-xxx pattern)
  Type 4: Paid in Xero first, CRM created retro         → Surfaces via timing analysis

For CRM distributions, breaks down by Transfer Type showing Xero match status.
For Xero-only bills, categorises as: "Other (NZF-xxx)" or "Non-case related".

Output structure per month:
  crm_dists.by_transfer_type[type] → {count, aud, matched, unmatched}
  xero_only.by_category[cat]       → {count, aud, bills[]}
"""

import os, sys, json, re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR     = os.path.join(os.path.dirname(__file__), "..", "data")
CRM_BASE_URL = zac.RULES["_meta"].get("zoho_crm_base_url", "")

VIEW_BILLS_XERO    = "1715382000005868510"
VIEW_CONTACTS_XERO = "1715382000005868254"

INCLUDED_STATUSES = {"approved", "extracted", "paid"}
LOOKBACK_MONTHS   = 12

# --- Helpers ---

def parse_amount(s):
    if not s:
        return 0.0
    return float(re.sub(r"[^0-9.]", "", str(s))) or 0.0

def to_month(s):
    """Return 'YYYY-MM' for a date/datetime string, or None."""
    dt = zac.parse_dt(s)
    if not dt:
        return None
    return dt.strftime("%Y-%m")

def effective_month(d):
    """
    Return the month key for a distribution row using the same logic as the
    Distributions dashboard — so both dashboards bucket into the same month.
    Priority: Paid_Date (if Paid) → Extracted_Date (if Extracted) → Created_Time.
    """
    status = (d.get("status") or "").strip()
    if status == "Paid":
        m = to_month(d.get("paid_date") or "")
        if m: return m
    elif status == "Extracted":
        m = to_month(d.get("extracted_date") or "")
        if m: return m
    return to_month(d.get("created_time") or "")

def categorise_xero_bill(bill_number):
    """
    Classify a Xero bill that has no matching CRM distribution:
      "Other (NZF-xxx)"  — NZF-prefixed bills (Salma to investigate)
      "Non-case related" — operational expenses, invoices, payroll etc.
    """
    bn = (bill_number or "").strip()
    if re.match(r'^NZF', bn, re.IGNORECASE):
        return "Other (NZF-xxx)"
    if re.match(r'^D\d+$', bn):
        # D-series with no CRM match — edge case, surface separately
        return "Unmatched D-series"
    return "Non-case related"

# --- Main build ---

def build_monthly_recon(token):
    print("\n=== Distributions Monthly Reconciliation ===")
    now_utc = datetime.now(timezone.utc)

    # Build list of last 12 month keys in order, oldest first
    months = []
    for i in range(LOOKBACK_MONTHS - 1, -1, -1):
        dt = now_utc.replace(day=1) - timedelta(days=i * 28)
        months.append(dt.strftime("%Y-%m"))
    # Dedupe while preserving order (timedelta day approach can repeat)
    seen = set()
    months_ordered = []
    for m in months:
        if m not in seen:
            seen.add(m)
            months_ordered.append(m)
    # Ensure we have exactly 12 unique months ending at current month
    cur = now_utc.strftime("%Y-%m")
    if cur not in seen:
        months_ordered.append(cur)
    months_ordered = sorted(set(months_ordered))[-12:]

    cutoff_month = months_ordered[0]  # e.g. "2025-05"

    print(f"  Range: {months_ordered[0]} → {months_ordered[-1]}")

    # --- Fetch views ---
    all_dists    = zac.fetch_view(token, zac.VIEW_DISTRIBUTIONS, label="Distributions")
    all_bills    = zac.fetch_view(token, VIEW_BILLS_XERO,        label="Bills (Xero)")
    all_contacts = zac.fetch_view(token, VIEW_CONTACTS_XERO,     label="Contacts (Xero)")

    contacts_by_id = {
        (c.get("contact_id") or "").strip(): c.get("name", "")
        for c in all_contacts if (c.get("contact_id") or "").strip()
    }

    # --- Filter CRM distributions to scope ---
    dists_in_scope = [
        d for d in all_dists
        if (d.get("status") or "").lower().strip() in INCLUDED_STATUSES
        and effective_month(d) is not None
        and effective_month(d) >= cutoff_month
    ]
    print(f"  CRM dists in scope: {len(dists_in_scope):,}")

    # Build CRM dist_id → dist row lookup
    crm_by_id = {}
    for d in dists_in_scope:
        did = (d.get("distribution_id") or "").strip()
        if did:
            crm_by_id[did] = d

    crm_ids = set(crm_by_id.keys())

    # --- Build Xero bills lookup (best-bill-wins for duplicates) ---
    # NOTE: No date filter here — bills_by_num covers ALL time.
    # A CRM distribution created in March can legitimately have a Xero payment in April
    # or later, so we must check across all bills when matching CRM distributions.
    def _bill_priority(b):
        if b.get("fully_paid_on_date"):
            return 0
        if (b.get("status") or "").upper() == "VOIDED":
            return 2
        return 1

    bills_by_num = {}
    for b in all_bills:
        num = (b.get("bill_number") or "").strip()
        if not num:
            continue
        if num not in bills_by_num or _bill_priority(b) < _bill_priority(bills_by_num[num]):
            bills_by_num[num] = b

    # --- Filter Xero bills to 12-month scope (by bill date) ---
    bills_in_scope = [
        b for b in bills_by_num.values()
        if to_month(b.get("date") or b.get("fully_paid_on_date") or "") is not None
        and to_month(b.get("date") or b.get("fully_paid_on_date") or "") >= cutoff_month
        and (b.get("status") or "").upper() != "VOIDED"
    ]
    print(f"  Xero bills in scope: {len(bills_in_scope):,}")

    # --- Build monthly buckets ---
    by_month = {m: {
        "crm_dists": {
            "total_count": 0, "total_aud": 0.0,
            "by_transfer_type": defaultdict(lambda: {
                "count": 0, "aud": 0.0,
                "matched_count": 0, "matched_aud": 0.0,
                "unmatched_count": 0, "unmatched_aud": 0.0,
                "unmatched_rows": [],  # individual dist detail for verification table
            })
        },
        "xero_only": {
            "total_count": 0, "total_aud": 0.0,
            "by_category": defaultdict(lambda: {"count": 0, "aud": 0.0, "bills": []})
        }
    } for m in months_ordered}

    # --- Process CRM distributions ---
    for d in dists_in_scope:
        m = effective_month(d)
        if m not in by_month:
            continue

        dist_id  = (d.get("distribution_id") or "").strip()
        transfer = (d.get("transfer_type") or "").strip() or "Not specified"
        amount   = parse_amount(d.get("grand_total"))
        xero_bill = bills_by_num.get(dist_id)
        matched   = bool(xero_bill and xero_bill.get("fully_paid_on_date"))
        # Credit card: expected no Xero — mark as "matched by design"
        if transfer.lower() == "credit card":
            matched = True  # CC intentionally not in Xero

        bucket = by_month[m]["crm_dists"]
        bucket["total_count"] += 1
        bucket["total_aud"]   += amount
        tb = bucket["by_transfer_type"][transfer]
        tb["count"] += 1
        tb["aud"]   += amount
        if matched:
            tb["matched_count"] += 1
            tb["matched_aud"]   += amount
        else:
            tb["unmatched_count"] += 1
            tb["unmatched_aud"]   += amount
            if len(tb["unmatched_rows"]) < 200:  # cap per transfer type per month
                _base_dt = (zac.parse_dt(d.get("approved_date") or "")
                           or zac.parse_dt(d.get("created_time") or ""))
                _hrs = round((_base_dt and (now_utc - _base_dt).total_seconds() / 3600) or 0)
                tb["unmatched_rows"].append({
                    "dist_id":          dist_id,
                    "record_id":        (d.get("id") or "").strip(),
                    "subject":          d.get("subject", ""),
                    "payee":            d.get("acc_name", "") or d.get("vendor_name", ""),
                    "amount":           amount,
                    "crm_status":       d.get("status", ""),
                    "created_time":     d.get("created_time", ""),
                    "paid_date":        d.get("paid_date", ""),
                    "extracted_date":   d.get("extracted_date", ""),
                    "approved_date":    d.get("approved_date", ""),
                    "transfer_type":    transfer,
                    "distribution_type": (d.get("distribution_type") or "").strip(),
                    "xero_bill_exists": bool(xero_bill),
                    "_hours":           _hrs,
                    "_overdue":         _hrs > 72,
                })

    # --- Process Xero-only bills ---
    for b in bills_in_scope:
        bill_num = (b.get("bill_number") or "").strip()
        m = to_month(b.get("date") or b.get("fully_paid_on_date") or "")
        if not m or m not in by_month:
            continue
        # Skip if this bill is matched to a CRM distribution
        if bill_num in crm_ids:
            continue

        amount   = parse_amount(b.get("total__fcy_") or b.get("total_(fcy)") or "")
        category = categorise_xero_bill(bill_num)
        payee    = contacts_by_id.get((b.get("contact_id") or "").strip(), "")

        bucket = by_month[m]["xero_only"]
        bucket["total_count"] += 1
        bucket["total_aud"]   += amount
        cat = bucket["by_category"][category]
        cat["count"] += 1
        cat["aud"]   += amount
        if len(cat["bills"]) < 50:  # cap detail rows per category per month
            cat["bills"].append({
                "bill_number":    bill_num,
                "date":           b.get("date", ""),
                "amount":         amount,
                "status":         b.get("status", ""),
                "payment_date":   b.get("fully_paid_on_date", ""),
                "payee":          payee,
            })

    # --- Serialise (convert defaultdicts → regular dicts, round floats) ---
    def _clean(d):
        if isinstance(d, defaultdict):
            d = dict(d)
        if isinstance(d, dict):
            return {k: _clean(v) for k, v in d.items()}
        if isinstance(d, float):
            return round(d, 2)
        return d

    by_month_clean = {}
    for m, mb in by_month.items():
        cm = {
            "crm_dists": {
                "total_count": mb["crm_dists"]["total_count"],
                "total_aud":   round(mb["crm_dists"]["total_aud"], 2),
                "by_transfer_type": _clean(mb["crm_dists"]["by_transfer_type"]),
            },
            "xero_only": {
                "total_count": mb["xero_only"]["total_count"],
                "total_aud":   round(mb["xero_only"]["total_aud"], 2),
                "by_category": _clean(mb["xero_only"]["by_category"]),
            }
        }
        by_month_clean[m] = cm

    # --- Summary stats ---
    total_crm  = sum(v["crm_dists"]["total_count"] for v in by_month_clean.values())
    total_xero = sum(v["xero_only"]["total_count"] for v in by_month_clean.values())
    print(f"  Processed: {total_crm} CRM dists, {total_xero} Xero-only bills across {len(months_ordered)} months")

    return {
        "meta": {
            "last_updated":    now_utc.isoformat(),
            "months":          months_ordered,
            "lookback_months": LOOKBACK_MONTHS,
            "crm_base_url":    CRM_BASE_URL,
        },
        "by_month": by_month_clean,
    }


def main():
    print("=" * 55)
    print("NZF — Distributions Monthly Reconciliation")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 55)

    token = zac.get_access_token()
    data  = build_monthly_recon(token)

    out_path = os.path.join(DATA_DIR, "distributions_monthly_recon.json")
    with open(out_path, "w") as f:
        json.dump(data, f, default=str)

    print(f"\nDone. distributions_monthly_recon.json written")
    print("=" * 55)


if __name__ == "__main__":
    main()
