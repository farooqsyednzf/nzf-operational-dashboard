"""
fetch_zoho_data.py
──────────────────
Pulls data from Zoho CRM and writes JSON files to /data.
Runs as a GitHub Action on a schedule.

Required GitHub Secrets:
  ZOHO_CLIENT_ID      — From Zoho API Console (Self Client)
  ZOHO_CLIENT_SECRET  — From Zoho API Console
  ZOHO_REFRESH_TOKEN  — Generated once via OAuth flow (see README)
  ZOHO_ACCOUNTS_URL   — e.g. https://accounts.zoho.com (or .com.au)
  ZOHO_API_DOMAIN     — e.g. https://www.zohoapis.com (or .com.au)
"""

import os
import json
import requests
from datetime import datetime, timezone

# ── Config from environment ──────────────────────────────────────────────────
CLIENT_ID      = os.environ["ZOHO_CLIENT_ID"]
CLIENT_SECRET  = os.environ["ZOHO_CLIENT_SECRET"]
REFRESH_TOKEN  = os.environ["ZOHO_REFRESH_TOKEN"]
ACCOUNTS_URL   = os.environ.get("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com")
API_DOMAIN     = os.environ.get("ZOHO_API_DOMAIN",   "https://www.zohoapis.com")

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Auth ─────────────────────────────────────────────────────────────────────
def get_access_token():
    """Exchange refresh token for a fresh access token."""
    res = requests.post(f"{ACCOUNTS_URL}/oauth/v2/token", params={
        "refresh_token": REFRESH_TOKEN,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type":    "refresh_token",
    })
    res.raise_for_status()
    token = res.json().get("access_token")
    if not token:
        raise ValueError(f"Failed to get access token: {res.json()}")
    print("✓ Access token obtained")
    return token

# ── Generic paginated fetch ───────────────────────────────────────────────────
def fetch_all_records(module, token, fields=None, criteria=None, max_records=5000):
    """Fetch all records from a Zoho CRM module with pagination.
    Uses /search endpoint when criteria is supplied, plain list otherwise."""
    headers  = {"Authorization": f"Zoho-oauthtoken {token}"}
    records  = []
    page     = 1
    per_page = 200

    # Choose endpoint based on whether we're filtering
    base_url = (
        f"{API_DOMAIN}/crm/v3/{module}/search"
        if criteria
        else f"{API_DOMAIN}/crm/v3/{module}"
    )

    while len(records) < max_records:
        params = {"page": page, "per_page": per_page}
        if fields:   params["fields"]   = ",".join(fields)
        if criteria: params["criteria"] = criteria

        res = requests.get(base_url, headers=headers, params=params)

        # 204 = no content / no more records
        if res.status_code == 204:
            break

        if res.status_code == 429:
            import time; time.sleep(10)
            continue

        res.raise_for_status()
        data  = res.json()
        batch = data.get("data", [])
        if not batch:
            break

        records.extend(batch)

        if not data.get("info", {}).get("more_records", False):
            break

        page += 1

    print(f"  → {module}: {len(records)} records fetched")
    return records

# ── Pipeline data ─────────────────────────────────────────────────────────────
def build_pipeline(token):
    records = fetch_all_records(
        module="Deals",
        token=token,
        fields=[
            "Deal_Name", "Account_Name", "Owner", "Stage",
            "Amount", "Closing_Date", "Probability"
        ],
        criteria="(Stage:not_equal:Closed Won)and(Stage:not_equal:Closed Lost)"
    )

    deals = []
    for r in records:
        deals.append({
            "name":       r.get("Deal_Name", ""),
            "account":    (r.get("Account_Name") or {}).get("name", ""),
            "owner":      (r.get("Owner") or {}).get("name", ""),
            "stage":      r.get("Stage", ""),
            "value":      r.get("Amount") or 0,
            "close_date": r.get("Closing_Date", ""),
            "probability":r.get("Probability") or 0,
        })

    # Aggregations
    stage_map = {}
    owner_map = {}
    total_value    = 0
    weighted_value = 0

    for d in deals:
        v = d["value"]
        total_value    += v
        weighted_value += v * (d["probability"] / 100)
        stage_map[d["stage"]]  = stage_map.get(d["stage"],  0) + v
        owner_map[d["owner"]] = owner_map.get(d["owner"],  {"deal_count": 0, "value": 0})
        owner_map[d["owner"]]["deal_count"] += 1
        owner_map[d["owner"]]["value"]      += v

    return {
        "meta": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "record_count": len(deals)
        },
        "summary": {
            "total_deals":    len(deals),
            "total_value":    round(total_value, 2),
            "avg_deal_size":  round(total_value / len(deals), 2) if deals else 0,
            "weighted_value": round(weighted_value, 2),
        },
        "by_stage": [{"stage": k, "value": round(v, 2)} for k, v in sorted(stage_map.items(), key=lambda x: -x[1])],
        "by_owner": [{"owner": k, **v} for k, v in sorted(owner_map.items(), key=lambda x: -x[1]["deal_count"])],
        "deals": sorted(deals, key=lambda x: -x["value"]),
    }

# ── Sales Summary ─────────────────────────────────────────────────────────────
def build_sales_summary(token):
    records = fetch_all_records(
        module="Deals",
        token=token,
        fields=["Deal_Name", "Account_Name", "Owner", "Stage", "Amount", "Closing_Date"],
        criteria="(Stage:equals:Closed Won)"
    )

    deals = []
    monthly = {}

    for r in records:
        close_date = r.get("Closing_Date", "")
        amount     = r.get("Amount") or 0
        month_key  = close_date[:7] if close_date else "Unknown"  # "YYYY-MM"

        deals.append({
            "name":       r.get("Deal_Name", ""),
            "account":    (r.get("Account_Name") or {}).get("name", ""),
            "owner":      (r.get("Owner") or {}).get("name", ""),
            "amount":     amount,
            "close_date": close_date,
        })

        if month_key not in monthly:
            monthly[month_key] = {"month": month_key, "revenue": 0, "deal_count": 0}
        monthly[month_key]["revenue"]    += amount
        monthly[month_key]["deal_count"] += 1

    total = sum(d["amount"] for d in deals)

    return {
        "meta": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "record_count": len(deals)
        },
        "summary": {
            "total_won":   len(deals),
            "total_revenue": round(total, 2),
            "avg_deal_size": round(total / len(deals), 2) if deals else 0,
        },
        "by_month": sorted(monthly.values(), key=lambda x: x["month"]),
        "deals":    sorted(deals, key=lambda x: x["close_date"], reverse=True),
    }

# ── Activities ────────────────────────────────────────────────────────────────
def build_activities(token):
    records = fetch_all_records(
        module="Activities",
        token=token,
        fields=["Subject", "Activity_Type", "Status", "Owner", "Due_Date", "Who_Id"]
    )

    activities = []
    type_map   = {}

    for r in records:
        a_type = r.get("Activity_Type", "Other")
        activities.append({
            "subject":    r.get("Subject", ""),
            "type":       a_type,
            "status":     r.get("Status", ""),
            "owner":      (r.get("Owner") or {}).get("name", ""),
            "due_date":   r.get("Due_Date", ""),
            "contact":    (r.get("Who_Id") or {}).get("name", ""),
        })
        type_map[a_type] = type_map.get(a_type, 0) + 1

    return {
        "meta": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "record_count": len(activities)
        },
        "by_type": [{"type": k, "count": v} for k, v in type_map.items()],
        "activities": activities[:500],  # Cap for performance
    }

# ── Leads ─────────────────────────────────────────────────────────────────────
def build_leads(token):
    records = fetch_all_records(
        module="Leads",
        token=token,
        fields=["First_Name", "Last_Name", "Company", "Lead_Source", "Lead_Status", "Owner", "Created_Time"]
    )

    leads      = []
    source_map = {}
    status_map = {}

    for r in records:
        source = r.get("Lead_Source", "Unknown")
        status = r.get("Lead_Status", "Unknown")
        leads.append({
            "name":    f"{r.get('First_Name','')} {r.get('Last_Name','')}".strip(),
            "company": r.get("Company", ""),
            "source":  source,
            "status":  status,
            "owner":   (r.get("Owner") or {}).get("name", ""),
            "created": r.get("Created_Time", ""),
        })
        source_map[source] = source_map.get(source, 0) + 1
        status_map[status] = status_map.get(status, 0) + 1

    return {
        "meta": {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "record_count": len(leads)
        },
        "summary": {
            "total_leads": len(leads),
        },
        "by_source": [{"source": k, "count": v} for k, v in sorted(source_map.items(), key=lambda x: -x[1])],
        "by_status": [{"status": k, "count": v} for k, v in sorted(status_map.items(), key=lambda x: -x[1])],
        "leads": leads[:500],
    }

# ── Meta file ─────────────────────────────────────────────────────────────────
def build_meta(datasets):
    return {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "datasets": datasets,
    }

# ── Write JSON ─────────────────────────────────────────────────────────────────
def write(filename, data):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  ✓ Written: {filename}")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print("═" * 50)
    print("NZF Dashboard — Zoho CRM Data Refresh")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 50)

    token = get_access_token()

    print("\n📥 Fetching data from Zoho CRM...")
    pipeline   = build_pipeline(token)
    sales      = build_sales_summary(token)
    activities = build_activities(token)
    leads      = build_leads(token)

    print("\n💾 Writing JSON data files...")
    write("pipeline.json",   pipeline)
    write("sales.json",      sales)
    write("activities.json", activities)
    write("leads.json",      leads)

    # Write meta file (used by index.html status table)
    meta = build_meta([
        {"name": "Pipeline",   "record_count": pipeline["meta"]["record_count"]},
        {"name": "Sales",      "record_count": sales["meta"]["record_count"]},
        {"name": "Activities", "record_count": activities["meta"]["record_count"]},
        {"name": "Leads",      "record_count": leads["meta"]["record_count"]},
    ])
    write("meta.json", meta)

    print("\n✅ Data refresh complete!")
    print("═" * 50)

if __name__ == "__main__":
    main()
