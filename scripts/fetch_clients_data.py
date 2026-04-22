"""
fetch_clients_data.py
─────────────────────
Builds /data/clients.json using Zoho Analytics SQL.

Two queries total (vs 27+ API calls with the CRM approach):

  Query 1 — Cases + new/returning classification + last paid date
    Source: Cases table + Distributions (correlated subquery)
    Determines new vs returning purely from distribution history —
    no reliance on the "New or existing" CRM field which is not
    synced to Analytics.

  Query 2 — Case notes for returning clients (qualitative analysis)
    Source: pre-built "Cases x Distribution x Notes - All" table
    Already has Cases, Distributions and Notes joined.
"""

import os, json, sys
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

ONGOING_STAGES = {
    "Ongoing Funding",
    "Post Funding - Follow Up",
    "Post=Follow-Up",
    "Post- Follow-Up",
    "Phase 4: Monitoring & Impact",
}

ONGOING_SQL = ", ".join(f"'{s}'" for s in ONGOING_STAGES)

BAND_ORDER = [
    "< 1 month", "1–3 months", "3–6 months",
    "6–12 months", "1–2 years", "2+ years", "Unknown",
]

# ── Query 1: Cases with new/returning classification ──────────────
# New      = client has NO paid/extracted distribution before this case
# Returning= client HAS at least one paid/extracted distribution before this case
#
# effective_paid_date = COALESCE(Paid Date, Extracted Date, Created Time)
# This handles old records where date fields were not filled in.

CASES_SQL = f"""
SELECT
    c.`CASE-ID`        AS case_id,
    c.`Created Time`   AS case_created,
    c.`Stage`          AS stage,
    c.`Description`    AS description,
    c.`Client Name`    AS client_id,

    CASE
        WHEN (
            SELECT COUNT(*)
            FROM `Distributions` d
            WHERE d.`Client Name` = c.`Client Name`
              AND d.`Status` IN ('Paid', 'Extracted')
              AND COALESCE(d.`Paid Date`, d.`Extracted Date`, d.`Created Time`)
                  < c.`Created Time`
        ) > 0 THEN 'Returning'
        ELSE 'New'
    END AS client_type,

    (
        SELECT MAX(COALESCE(d2.`Paid Date`, d2.`Extracted Date`, d2.`Created Time`))
        FROM `Distributions` d2
        WHERE d2.`Client Name`  = c.`Client Name`
          AND d2.`Status`       IN ('Paid', 'Extracted')
          AND COALESCE(d2.`Paid Date`, d2.`Extracted Date`, d2.`Created Time`)
              < c.`Created Time`
    ) AS last_paid_date

FROM `Cases` c
WHERE c.`Created Time` >= DATE_SUB(NOW(), INTERVAL 14 MONTH)
  AND c.`Stage` NOT IN ({ONGOING_SQL})
ORDER BY c.`Created Time` DESC
"""

# ── Query 2: Notes for qualitative analysis ───────────────────────
# The pre-built table already has the JOIN done.
# We grab all returning-window cases and aggregate notes in Python.

NOTES_SQL = f"""
SELECT DISTINCT
    case_id,
    description,
    stage,
    case_created_dt,
    notes,
    note_title,
    client_id
FROM `Cases x Distribution x Notes - All`
WHERE case_created_dt >= DATE_SUB(NOW(), INTERVAL 14 MONTH)
  AND stage NOT IN ({ONGOING_SQL})
ORDER BY case_created_dt DESC
"""

# ── Helpers ───────────────────────────────────────────────────────
def last_13_months():
    now = datetime.now(timezone.utc)
    result, seen = [], set()
    for i in range(14):
        m, y = now.month - i, now.year
        while m <= 0: m += 12; y -= 1
        mk = f"{y}-{m:02d}"
        if mk not in seen:
            seen.add(mk); result.append(mk)
    result.reverse()
    return result[-13:]

def days_between(s1, s2):
    d1, d2 = zac.parse_dt(s1), zac.parse_dt(s2)
    return abs((d2 - d1).days) if d1 and d2 else None

def return_gap_band(days):
    if days is None:  return "Unknown"
    if days < 30:     return "< 1 month"
    if days < 90:     return "1–3 months"
    if days < 180:    return "3–6 months"
    if days < 365:    return "6–12 months"
    if days < 730:    return "1–2 years"
    return "2+ years"

# ── Build report ──────────────────────────────────────────────────
def build_clients_report(token):
    # ── Run both queries ──────────────────────────────────────────
    cases_rows = zac.run_query(token, CASES_SQL, label="Cases")
    notes_rows = zac.run_query(token, NOTES_SQL, label="Notes")

    months         = last_13_months()
    current_month  = months[-1]
    previous_month = months[-2]

    new_by_month       = defaultdict(int)
    returning_by_month = defaultdict(int)
    returning_cases    = []
    gap_bands          = defaultdict(int)

    # ── Process case rows ─────────────────────────────────────────
    for row in cases_rows:
        created_dt = zac.parse_dt(row.get("case_created", ""))
        mk         = zac.month_key(created_dt)
        if not mk or mk not in months:
            continue

        client_type = row.get("client_type", "").strip()

        if client_type == "New":
            new_by_month[mk] += 1

        elif client_type == "Returning":
            returning_by_month[mk] += 1

            gap_days = days_between(
                row.get("last_paid_date"),
                row.get("case_created"),
            )
            band = return_gap_band(gap_days)
            gap_bands[band] += 1

            returning_cases.append({
                "case_id":         row.get("case_id", ""),
                "client_id":       row.get("client_id", ""),
                "created":         row.get("case_created", ""),
                "month":           mk,
                "stage":           row.get("stage", ""),
                "description":     (row.get("description") or "")[:500],
                "last_paid_date":  row.get("last_paid_date", ""),
                "return_gap_days": gap_days,
                "return_gap_band": band,
            })

    # ── Attach notes to returning cases ───────────────────────────
    # Build a dict: case_id → list of note strings
    notes_by_case = defaultdict(list)
    for n in notes_rows:
        cid  = n.get("case_id", "")
        note = (n.get("notes") or "").strip()
        if cid and note:
            notes_by_case[cid].append(note)

    for c in returning_cases:
        notes = notes_by_case.get(c["case_id"], [])
        c["notes_summary"] = " | ".join(notes[:3])[:500]  # First 3 notes

    # ── Trend series ──────────────────────────────────────────────
    trend = [
        {
            "month":     m,
            "new":       new_by_month.get(m, 0),
            "returning": returning_by_month.get(m, 0),
            "total":     new_by_month.get(m, 0) + returning_by_month.get(m, 0),
        }
        for m in months
    ]

    def pct(c, p):
        return round(((c - p) / p) * 100, 1) if p else None

    new_curr = new_by_month.get(current_month, 0)
    new_prev = new_by_month.get(previous_month, 0)
    ret_curr = returning_by_month.get(current_month, 0)
    ret_prev = returning_by_month.get(previous_month, 0)

    gap_days_list = [c["return_gap_days"] for c in returning_cases
                     if c["return_gap_days"] is not None]
    avg_gap = round(sum(gap_days_list) / len(gap_days_list)) if gap_days_list else 0

    # Qual sample: most recent 50 returning cases with descriptions or notes
    qual_sample = sorted(
        [c for c in returning_cases if c.get("description") or c.get("notes_summary")],
        key=lambda x: x["created"] or "",
        reverse=True,
    )[:50]

    return {
        "meta": {
            "last_updated":   datetime.now(timezone.utc).isoformat(),
            "record_count":   len(cases_rows),
            "months_covered": months,
            "current_month":  current_month,
            "previous_month": previous_month,
        },
        "summary": {
            "new_clients_current_month":        new_curr,
            "new_clients_previous_month":       new_prev,
            "new_clients_pct_change":           pct(new_curr, new_prev),
            "returning_clients_current_month":  ret_curr,
            "returning_clients_previous_month": ret_prev,
            "returning_clients_pct_change":     pct(ret_curr, ret_prev),
            "total_returning_in_period":        sum(returning_by_month.values()),
            "avg_return_gap_days":              avg_gap,
        },
        "trend":  trend,
        "gap_distribution": [
            {"band": b, "count": gap_bands[b]}
            for b in BAND_ORDER if gap_bands.get(b, 0) > 0
        ],
        "returning_cases": qual_sample,
    }

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("═" * 55)
    print("NZF — Client Report  |  Zoho Analytics")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("═" * 55)

    token = zac.get_access_token()

    print("\n📊 Running Analytics queries...")
    data  = build_clients_report(token)

    out = os.path.join(DATA_DIR, "clients.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    s = data["summary"]
    print(f"\n✅ clients.json written")
    print(f"   Cases in window:        {data['meta']['record_count']:,}")
    print(f"   New this month:         {s['new_clients_current_month']}")
    print(f"   Returning this month:   {s['returning_clients_current_month']}")
    print(f"   Avg return gap:         {s['avg_return_gap_days']} days")
    print("═" * 55)

if __name__ == "__main__":
    main()
