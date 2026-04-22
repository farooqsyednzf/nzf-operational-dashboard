"""
fetch_zoho_data.py
──────────────────
Builds /data/pipeline.json for the Case Pipeline dashboard.
Uses the shared zoho_client layer (COQL with list-API fallback).
"""

import os, json
from datetime import datetime, timezone
from collections import defaultdict

import sys
sys.path.insert(0, os.path.dirname(__file__))
import zoho_client as zc

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

CLOSED_STAGES = {"Closed - Funded", "Closed - Not Funded", "Closed - NO Response"}

# ── Fetch open cases ──────────────────────────────────────────────
def fetch_open_cases(token):
    print("  Fetching open Cases...")
    fields = [
        "id", "Deal_Name", "Contact_Name", "Owner",
        "Stage", "Priority", "Amount", "Closing_Date",
        "Created_Time", "CASE_ID", "Case_Type1",
    ]

    # COQL: exclude closed stages using NOT IN
    closed_list = ", ".join(f"'{s}'" for s in CLOSED_STAGES)
    return zc.fetch(
        token          = token,
        coql_query_str = f"""
            SELECT {', '.join(fields)}
            FROM Deals
            WHERE Stage NOT IN ({closed_list})
        """.strip(),
        fallback_module    = "Deals",
        fallback_fields    = fields,
        fallback_cutoff_dt = None,
        label              = "Open Cases",
        max_records        = 5000,
    )

# ── Build pipeline data ───────────────────────────────────────────
def build_pipeline(token):
    cases = fetch_open_cases(token)

    stage_map = defaultdict(lambda: {"count": 0, "value": 0})
    owner_map = defaultdict(lambda: {"deal_count": 0, "value": 0})
    total_value = 0

    deals = []
    for c in cases:
        owner   = (c.get("Owner") or {}).get("name", "Unassigned")
        stage   = c.get("Stage", "")
        amount  = c.get("Amount") or 0
        contact = (c.get("Contact_Name") or {}).get("name", "")

        total_value += amount
        stage_map[stage]["count"]  += 1
        stage_map[stage]["value"]  += amount
        owner_map[owner]["deal_count"] += 1
        owner_map[owner]["value"]      += amount

        deals.append({
            "name":       c.get("Deal_Name", ""),
            "case_id":    c.get("CASE_ID", ""),
            "account":    contact,
            "owner":      owner,
            "stage":      stage,
            "value":      round(amount, 2),
            "close_date": c.get("Closing_Date", ""),
            "priority":   c.get("Priority", ""),
        })

    return {
        "meta": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "record_count": len(cases),
        },
        "summary": {
            "total_deals":    len(cases),
            "total_value":    round(total_value, 2),
            "avg_deal_size":  round(total_value / len(cases), 2) if cases else 0,
        },
        "by_stage": [
            {"stage": k, "count": v["count"], "value": round(v["value"], 2)}
            for k, v in sorted(stage_map.items(), key=lambda x: -x[1]["count"])
        ],
        "by_owner": [
            {"owner": k, **v}
            for k, v in sorted(owner_map.items(), key=lambda x: -x[1]["deal_count"])
        ],
        "deals": sorted(deals, key=lambda x: x["stage"]),
    }

# ── Main ──────────────────────────────────────────────────────────
def main():
    print("═" * 55)
    print("NZF — Pipeline Data Refresh")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("═" * 55)

    token = zc.get_access_token()
    print("\n📥 Fetching from Zoho CRM...")
    data  = build_pipeline(token)

    out = os.path.join(DATA_DIR, "pipeline.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"\n✅ pipeline.json written")
    print(f"   Open cases: {data['meta']['record_count']}")
    print("═" * 55)

if __name__ == "__main__":
    main()
