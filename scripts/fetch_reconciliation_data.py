"""
fetch_reconciliation_data.py
────────────────────────────
Builds /data/reconciliation.json for the Distribution Reconciliation dashboard.

Source: Zoho Analytics SQL join across:
  - Distributions (CRM Purchase Orders, Zakat only, last 30 days)
  - Bills (Xero)  — joined on Bill Number = Distribution ID
  - Contacts (Xero) — for payee name

Data validation confirmed 24 Apr 2026:
  - Bills (Xero) table exists in workspace 1715382000001002475 ✓
  - Contacts (Xero) table exists ✓
  - Join on Bill Number = Distribution ID is 100% accurate on recent paid records ✓
  - Amounts match exactly between CRM and Xero ✓

PII policy: payee/account name stored only for finance reconciliation purposes.
This dashboard is owned by Salma (finance) and Usman (compliance).
"""

import os, sys, json
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
RULES    = zac.RULES
CRM_BASE_URL = RULES["_meta"].get("zoho_crm_base_url", "")

SQL = """
SELECT
  d."Distribution ID"          AS dist_id,
  d."Subject"                  AS subject,
  d."Status"                   AS crm_status,
  d."Grand Total"              AS amount,
  d."Created Time"             AS created_time,
  d."Approved Date"            AS approved_date,
  d."Paid Date"                AS crm_paid_date,
  d."Acc Name"                 AS crm_payee,
  d."BSB / Biller Code"        AS crm_bsb,
  b."Bill Number"              AS xero_bill_num,
  b."Status"                   AS xero_bill_status,
  b."Fully Paid On Date"       AS xero_payment_date,
  b."Total (FCY)"              AS xero_amount,
  c."Name"                     AS xero_payee
FROM "Distributions" d
LEFT JOIN "Bills (Xero)" b
  ON (b."Bill Number" = d."Distribution ID"
   OR b."Bill Number" LIKE CONCAT('% ', d."Distribution ID"))
LEFT JOIN "Contacts (Xero)" c
  ON b."Contact ID" = c."Contact ID"
WHERE d."Distribution Type" = 'Zakat'
  AND d."Status" NOT IN ('Cancelled','Rejected','Draft','Void')
  AND d."Created Time" >= DATEADD(day, -30, GETDATE())
ORDER BY d."Created Time" DESC
""".strip()


def parse_amount(s):
    if not s:
        return 0.0
    import re
    return float(re.sub(r"[^0-9.]", "", str(s))) or 0.0


def parse_date(s):
    return zac.parse_dt(s)


def get_status(row):
    """
    Xero payment is the source of truth.
    Status logic matches the original standalone dashboard exactly:
      paid    — Xero has a payment date (confirmed paid)
      nobill  — no Xero bill at all (control bypass — investigate)
      overdue — >72h elapsed from approved/created, bill exists but unpaid
      urgent  — 48–72h elapsed
      pending — bill exists, <48h elapsed
    """
    if row.get("xero_payment_date"):
        return "paid"
    base = parse_date(row.get("approved_date")) or parse_date(row.get("created_time"))
    hours = (datetime.now(timezone.utc) - base).total_seconds() / 3600 if base else None
    if not row.get("xero_bill_num"):
        return "nobill"
    if hours is not None and hours > 72:
        return "overdue"
    if hours is not None and hours > 48:
        return "urgent"
    return "pending"


def get_hours(row):
    base = parse_date(row.get("approved_date")) or parse_date(row.get("created_time"))
    if not base:
        return None
    end = parse_date(row.get("xero_payment_date")) or datetime.now(timezone.utc)
    return round((end - base).total_seconds() / 3600)


def build_reconciliation_report(token):
    print("\n=== Distribution Reconciliation ===")
    print(f"  Running SQL query against Zoho Analytics...")

    rows_raw = zac.run_sql_query(token, SQL)
    if not rows_raw:
        print("  WARNING: No data returned from Analytics")
        return None

    print(f"  Raw rows: {len(rows_raw):,}")

    rows = []
    for r in rows_raw:
        row = {
            "dist_id":           r.get("dist_id", ""),
            "subject":           r.get("subject", ""),
            "crm_status":        r.get("crm_status", ""),
            "amount":            r.get("amount", ""),
            "created_time":      r.get("created_time", ""),
            "approved_date":     r.get("approved_date", ""),
            "crm_paid_date":     r.get("crm_paid_date", ""),
            "crm_payee":         r.get("crm_payee", ""),
            "xero_bill_num":     r.get("xero_bill_num", ""),
            "xero_bill_status":  r.get("xero_bill_status", ""),
            "xero_payment_date": r.get("xero_payment_date", ""),
            "xero_amount":       r.get("xero_amount", ""),
            "xero_payee":        r.get("xero_payee", "") or r.get("crm_payee", ""),
        }
        row["_status"] = get_status(row)
        row["_hours"]  = get_hours(row)
        rows.append(row)

    # Compute summary KPIs
    paid    = [r for r in rows if r["_status"] == "paid"]
    overdue = [r for r in rows if r["_status"] == "overdue"]
    urgent  = [r for r in rows if r["_status"] == "urgent"]
    pending = [r for r in rows if r["_status"] == "pending"]
    nobill  = [r for r in rows if r["_status"] == "nobill"]

    total_amt  = sum(parse_amount(r["amount"]) for r in rows)
    paid_amt   = sum(parse_amount(r["amount"]) for r in paid)
    unpaid_amt = total_amt - paid_amt

    print(f"  Total: {len(rows)} | Paid: {len(paid)} | Overdue: {len(overdue)} "
          f"| Urgent: {len(urgent)} | Pending: {len(pending)} | No Bill: {len(nobill)}")
    print(f"  Total AUD: ${total_amt:,.0f} | Paid: ${paid_amt:,.0f} | Unpaid: ${unpaid_amt:,.0f}")

    return {
        "meta": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "record_count": len(rows),
            "crm_base_url": CRM_BASE_URL,
        },
        "summary": {
            "total":        len(rows),
            "paid":         len(paid),
            "overdue":      len(overdue),
            "urgent":       len(urgent),
            "pending":      len(pending),
            "nobill":       len(nobill),
            "total_aud":    round(total_amt, 2),
            "paid_aud":     round(paid_amt, 2),
            "unpaid_aud":   round(unpaid_amt, 2),
        },
        "rows": rows,
    }


def main():
    print("=" * 55)
    print("NZF — Distribution Reconciliation | Zoho Analytics")
    print(f"Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 55)

    token = zac.get_access_token()
    print("✓ Access token obtained")

    data = build_reconciliation_report(token)
    if not data:
        print("ERROR: No data produced")
        sys.exit(1)

    out_path = os.path.join(DATA_DIR, "reconciliation.json")
    with open(out_path, "w") as f:
        json.dump(data, f, default=str)

    print(f"\nDone. reconciliation.json written")
    print(f"  Total distributions:  {data['summary']['total']}")
    print(f"  Paid in Xero:         {data['summary']['paid']}")
    print(f"  Overdue >72h:         {data['summary']['overdue']}")
    print(f"  No Xero bill:         {data['summary']['nobill']}")
    print(f"  Unpaid AUD:           ${data['summary']['unpaid_aud']:,.0f}")
    print("=" * 55)


if __name__ == "__main__":
    main()
