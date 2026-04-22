"""
fetch_zoho_data.py
──────────────────
Builds /data/pipeline.json using Zoho Analytics SQL.
One query, no pagination, no limits.
"""

import os, json, sys
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
import zoho_analytics_client as zac

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

CLOSED_STAGES = (
    "'Closed - Funded'",
    "'Closed - Not Funded'",
    "'Closed - NO Response'",
)

PIPELINE_SQL = f"""
SELECT
    c.`CASE-ID`       AS case_id,
    c.`Case Name`     AS case_name,
    c.`Client Name`   AS client_id,
    c.`Stage`         AS stage,
    c.`Case Urgency`  AS priority,
    c.`Case Type`     AS case_type,
    c.`Created Time`  AS created,
    c.`Caseworker`    AS caseworker,
    c.`Internal Case Type` AS deal_type
FROM `Cases` c
WHERE c.`Created Time` >= DATE_SUB(NOW(), INTERVAL 13 MONTH)
  AND c.`Stage` NOT IN ({', '.join(CLOSED_STAGES)})
ORDER BY c.`Created Time` DESC
"""

def build_pipeline(token):
    rows = zac.run_query(token, PIPELINE_SQL, label="Pipeline")

    stage_map = defaultdict(int)
    cw_map    = defaultdict(int)
    cases     = []

    for r in rows:
        stage = r.get("stage", "Unknown")
        cw    = r.get("caseworker", "Unassigned") or "Unassigned"

        stage_map[stage] += 1
        cw_map[cw]       += 1

        cases.append({
            "case_id":   r.get("case_id", ""),
            "case_name": r.get("case_name", ""),
            "stage":     stage,
            "priority":  r.get("priority", ""),
            "case_type": r.get("case_type", ""),
            "caseworker":cw,
            "created":   r.get("created", ""),
        })

    return {
        "meta": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "record_count": len(rows),
        },
        "summary": {
            "total_open":    len(rows),
        },
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

    print("\n📊 Running Analytics query...")
    data  = build_pipeline(token)

    out = os.path.join(DATA_DIR, "pipeline.json")
    with open(out, "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"\n✅ pipeline.json written  ({data['meta']['record_count']:,} open cases)")
    print("═" * 55)

if __name__ == "__main__":
    main()

