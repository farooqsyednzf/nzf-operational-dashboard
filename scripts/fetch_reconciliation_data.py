"""
fetch_reconciliation_data.py
────────────────────────────
Builds /data/reconciliation.json for the Distribution Reconciliation dashboard.

Approach: fetch three Analytics views directly with fetch_view() (no async
SQL export job), then JOIN in Python. Uses the same proven pattern as every
other data script.

Views fetched:
  - Distributions     (1715382000001002628)  CRM Purchase Orders
  - Bills (Xero)      (1715382000005868510)  Xero bills synced to Analytics
  - Contacts (Xero)   (1715382000005868254)  Xero contacts for payee name

Join: Bill.bill_number == Distribution.distribution_id (100% accuracy confirmed)

Status logic (Xero is source of truth, CRM status ignored):
  paid    — Xero fully_paid_on_date present
  nobill  — no matching Xero bill (control bypass)
  overdue — >72h elapsed, bill exists but unpaid
  urgent  — 48-72h elapsed
  pending — bill exists, <48h
"""

import os, sys, json, re
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR     = os.path.join(os.path.dirname(__file__), "..", "data")
CRM_BASE_URL = zac.RULES["_meta"].get("zoho_crm_base_url", "")

VIEW_BILLS_XERO    = "1715382000005868510"
VIEW_CONTACTS_XERO = "1715382000005868254"
INCLUDED_STATUSES  = {"approved", "extracted", "paid"}
LOOKBACK_DAYS      = 30


def parse_amount(s):
    if not s:
        return 0.0
    return float(re.sub(r"[^0-9.]", "", str(s))) or 0.0


def get_status(xero_bill, base_dt, now_utc):
    if xero_bill and xero_bill.get("fully_paid_on_date"):
        return "paid"
    if not xero_bill:
        return "nobill"
    if not base_dt:
        return "pending"
    hours = (now_utc - base_dt).total_seconds() / 3600
    if hours > 72:
        return "overdue"
    if hours > 48:
        return "urgent"
    return "pending"


def get_hours(base_dt, xero_bill, now_utc):
    if not base_dt:
        return None
    end_dt = None
    if xero_bill:
        end_dt = zac.parse_dt(xero_bill.get("fully_paid_on_date") or "")
    return round(((end_dt or now_utc) - base_dt).total_seconds() / 3600)


def build_reconciliation_report(token):
    print("\n=== Distribution Reconciliation ===")
    now_utc = datetime.now(timezone.utc)
    cutoff  = now_utc - timedelta(days=LOOKBACK_DAYS)

    # Fetch all three views
    all_dists    = zac.fetch_view(token, zac.VIEW_DISTRIBUTIONS, label="Distributions")
    all_bills    = zac.fetch_view(token, VIEW_BILLS_XERO,        label="Bills (Xero)")
    all_contacts = zac.fetch_view(token, VIEW_CONTACTS_XERO,     label="Contacts (Xero)")

    # Filter distributions to Zakat + last 30 days + not excluded
    dists = [
        d for d in all_dists
        if (d.get("status") or "").lower().strip() in INCLUDED_STATUSES
        and zac.parse_dt(d.get("created_time") or "") is not None
        and zac.parse_dt(d.get("created_time")) >= cutoff
    ]
    print(f"  Filtered: {len(dists):,} Zakat distributions in last {LOOKBACK_DAYS} days")

    # Build bills lookup: bill_number → best bill row.
    # Multiple Xero bills can share the same bill number (e.g. PAID + VOIDED duplicate).
    # Priority: PAID (has fully_paid_on_date) > any non-VOIDED status > VOIDED.
    # This prevents a voided duplicate overwriting a real payment.
    def _bill_priority(b):
        if b.get("fully_paid_on_date"):
            return 0   # confirmed paid — highest priority
        if (b.get("status") or "").upper() == "VOIDED":
            return 2   # voided — lowest priority
        return 1       # authorised / submitted / etc.

    bills_by_num = {}
    for b in all_bills:
        num = (b.get("bill_number") or "").strip()
        if not num:
            continue
        if num not in bills_by_num or _bill_priority(b) < _bill_priority(bills_by_num[num]):
            bills_by_num[num] = b

    contacts_by_id = {(c.get("contact_id") or "").strip(): c.get("name", "")
                      for c in all_contacts if (c.get("contact_id") or "").strip()}

    # JOIN and build rows
    rows = []
    for d in dists:
        dist_id   = (d.get("distribution_id") or "").strip()
        xero_bill = bills_by_num.get(dist_id)

        xero_payee = ""
        if xero_bill:
            xero_payee = contacts_by_id.get(
                (xero_bill.get("contact_id") or "").strip(), "")

        # SLA clock base: approved_date → created_time fallback
        base_dt = (zac.parse_dt(d.get("approved_date") or "")
                   or zac.parse_dt(d.get("created_time") or ""))

        row = {
            "dist_id":           dist_id,
            "record_id":         (d.get("id") or "").strip(),
            "distribution_type": d.get("distribution_type", ""),
            "subject":           d.get("subject", ""),
            "crm_status":        d.get("status", ""),
            "amount":            parse_amount(d.get("grand_total", "")),  # stored as float
            "transfer_type":     d.get("transfer_type", ""),
            "created_time":      d.get("created_time", ""),
            "approved_date":     d.get("approved_date", ""),
            "crm_paid_date":     d.get("paid_date", ""),
            "crm_payee":         d.get("acc_name", ""),
            "xero_bill_num":     xero_bill.get("bill_number", "") if xero_bill else "",
            "xero_bill_status":  xero_bill.get("status", "")      if xero_bill else "",
            "xero_payment_date": xero_bill.get("fully_paid_on_date", "") if xero_bill else "",
            "xero_amount":       parse_amount(xero_bill.get("total__fcy_", "")) if xero_bill else 0.0,
            "xero_payee":        xero_payee or d.get("acc_name", ""),
        }
        row["_status"] = get_status(xero_bill, base_dt, now_utc)
        row["_hours"]  = get_hours(base_dt, xero_bill, now_utc)
        rows.append(row)

    paid    = [r for r in rows if r["_status"] == "paid"]
    overdue = [r for r in rows if r["_status"] == "overdue"]
    urgent  = [r for r in rows if r["_status"] == "urgent"]
    pending = [r for r in rows if r["_status"] == "pending"]
    nobill  = [r for r in rows if r["_status"] == "nobill"]

    total_amt  = sum(parse_amount(r["amount"]) for r in rows)
    paid_amt   = sum(parse_amount(r["amount"]) for r in paid)
    unpaid_amt = total_amt - paid_amt

    print(f"  Paid: {len(paid)} | Overdue: {len(overdue)} | Urgent: {len(urgent)} "
          f"| Pending: {len(pending)} | No Bill: {len(nobill)}")
    print(f"  AUD → Total: ${total_amt:,.0f} | Paid: ${paid_amt:,.0f} | Unpaid: ${unpaid_amt:,.0f}")

    return {
        "meta": {
            "last_updated":  now_utc.isoformat(),
            "record_count":  len(rows),
            "crm_base_url":  CRM_BASE_URL,
            "lookback_days": LOOKBACK_DAYS,
        },
        "summary": {
            "total": len(rows), "paid": len(paid),
            "overdue": len(overdue), "urgent": len(urgent),
            "pending": len(pending), "nobill": len(nobill),
            "total_aud": round(total_amt, 2),
            "paid_aud":  round(paid_amt, 2),
            "unpaid_aud": round(unpaid_amt, 2),
        },
        "rows": rows,
    }


def main():
    print("=" * 55)
    print("NZF — Distribution Reconciliation | Zoho Analytics")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 55)

    token = zac.get_access_token()
    data  = build_reconciliation_report(token)

    if not data:
        print("ERROR: No data produced")
        sys.exit(1)

    out_path = os.path.join(DATA_DIR, "reconciliation.json")
    with open(out_path, "w") as f:
        json.dump(data, f, default=str)

    print(f"\nDone. {data['summary']['total']} distributions written to reconciliation.json")
    print("=" * 55)


if __name__ == "__main__":
    main()
