"""
fetch_zoho_data.py
──────────────────
Builds /data/pipeline.json using the Zoho Analytics view fetch approach.
"""

import os, json, sys
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

CLOSED_STAGES = {
    "Closed - Funded",
    "Closed - Not Funded",
    "Closed - NO Response",
}

def build_pipeline(token):
    print("\n  Fetching Analytics views...")
    all_cases = zac.fetch_view(token, zac.VIEW_CASES, label="Cases")

    from datetime import timedelta
    n = datetime.now(timezone.utc)
    m, y = n.month - 12, n.year
    while m <= 0: m += 12; y -= 1
    cutoff = datetime(y, m, 1, tzinfo=timezone.utc)

    stage_map = defaultdict(int)
    cw_map    = defaultdict(int)
    cases     = []

    for c in all_cases:
        dt = zac.parse_dt(c.get("created_time", ""))
        if not dt or dt < cutoff:
            continue
        stage = c.get("stage", "").strip()
        if stage in CLOSED_STAGES:
            continue

        cw = c.get("caseworker", "Unassigned") or "Unassigned"
        stage_map[stage] += 1
        cw_map[cw]       += 1

        cases.append({
            "case_id":   c.get("case_id") or c.get("case-id", ""),
            "case_name": c.get("case_name", ""),
            "stage":     stage,
            "priority":  c.get("case_urgency", ""),
            "case_type": c.get("case_type", ""),
            "caseworker":cw,
            "created":   c.get("created_time", ""),
        })

    print(f"  Open cases in window: {len(cases):,}")

    return {
        "meta": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "record_count": len(cases),
        },
        "summary": {"total_open": len(cases)},
        "by_stage": [
            {"stage": k, "count": v}
            for k, v in sorted(stage_map.items(), key=lambda x: -x[1])
        ],
        "by_caseworker": [
            {"caseworker": k, "count": v}
            for k, v in sorted(cw_map.items(), key=lambda x: -x[1])
        ],
        "cases": cases,
    }

def main():
    print("═" * 55)
    print("NZF — Pipeline Report  |  Zoho Analytics")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("═" * 55)

    token = zac.get_access_token()
    data  = build_pipeline(token)

    out = os.path.join(DATA_DIR, "pipeline.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"\n✅ pipeline.json written  ({data['meta']['record_count']:,} open cases)")
    print("═" * 55)

if __name__ == "__main__":
    main()
