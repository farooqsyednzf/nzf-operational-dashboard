"""
zoho_crm_client.py
──────────────────
Direct Zoho CRM REST API client for time-sensitive data.

Used ONLY for the Priority Intelligence section of the Cases Report,
where Analytics' up-to-24h sync delay would miss critical P1 cases.

All other data (KPIs, trends, distributions, clients) uses Zoho Analytics
via zoho_analytics_client.py — do not change that.

Scope required: ZohoCRM.modules.READ
  (combined with ZohoAnalytics.fullaccess.all in the same refresh token)

Field normalisation:
  CRM API returns camelCase field names with object values for linked records.
  This module normalises them to the same lowercase_underscore shape used
  throughout the dashboard scripts, so the priority intelligence code works
  identically regardless of data source.
"""

import os, requests
from datetime import datetime, timezone, timedelta

CRM_API_BASE = "https://www.zohoapis.com/crm/v6"

# Fields to request from the Potentials module
# Contact_Name returns {"id": "...", "name": "..."} — we extract id only (PII)
CRM_FIELDS = "id,CASE_ID,Stage,Priority,Description,Created_Time,Contact_Name,CW_Recommendation,Reason_for_Not_Funding"


def _parse_crm_dt(s):
    """Parse CRM API ISO 8601 datetime to UTC. Delegates to zoho_analytics_client."""
    from zoho_analytics_client import parse_dt
    return parse_dt(s)


def _normalise_crm_record(rec):
    """
    Map raw CRM API record → flat dict with the same field names
    used by the existing priority intelligence code.

    CRM API shape example:
      {
        "id": "981539000123456789",
        "CASE_ID": "201730419",
        "Stage": "Intake",
        "Priority": "Priority 1 - (SAME DAY)",
        "Description": "Client is homeless ...",
        "Created_Time": "2026-04-23T09:15:00+10:00",
        "Contact_Name": {"id": "981539000098765432", "name": "Client Name"}
      }
    """
    contact = rec.get("Contact_Name") or {}
    if isinstance(contact, dict):
        client_id = contact.get("id", "")
    else:
        client_id = str(contact)

    return {
        "id":                  rec.get("id", ""),
        "case_id":             rec.get("CASE_ID", ""),
        "stage":               rec.get("Stage", "").strip(),
        "case_urgency":        rec.get("Priority", ""),       # API field is "Priority", UI label is "Case Urgency"
        "description":         (rec.get("Description") or "").strip(),
        "created_time":        rec.get("Created_Time", ""),
        "client_name":         client_id,
        "cw_recommendation":   (rec.get("CW_Recommendation") or "").strip(),
        "reason_not_funded":   (rec.get("Reason_for_Not_Funding") or "").strip(),
        "_source":             "crm_live",
    }


def fetch_recent_cases(token, days=30, max_pages=5):
    """
    Fetch cases created in the last `days` days directly from Zoho CRM.

    Returns a list of normalised case dicts in the same shape as
    Analytics-sourced records, ready to drop into priority intelligence code.

    Paginates up to max_pages * 200 = up to 1000 cases.
    For 30-day windows, NZF typically has ~250-300 cases, so 2 pages max.
    """
    cutoff     = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    headers    = {"Authorization": f"Zoho-oauthtoken {token}"}
    all_cases  = []

    print(f"  [CRM Live] Fetching cases created after {cutoff.strftime('%Y-%m-%d %H:%M UTC')}...")

    for page in range(1, max_pages + 1):
        params = {
            "fields":    CRM_FIELDS,
            "criteria":  f"(Created_Time:greater_than:{cutoff_str})",
            "sort_by":   "Created_Time",
            "sort_order":"desc",
            "per_page":  200,
            "page":      page,
        }

        try:
            res = requests.get(
                f"{CRM_API_BASE}/Potentials",
                headers=headers,
                params=params,
                timeout=30,
            )
        except requests.exceptions.RequestException as e:
            print(f"  [CRM Live] WARNING: Request failed on page {page}: {e}")
            break

        if res.status_code == 204:
            # No content — no more records
            break

        if res.status_code == 401:
            print("  [CRM Live] ERROR: 401 Unauthorised — refresh token may lack "
                  "ZohoCRM.modules.READ scope. Re-run the Setup workflow with the "
                  "combined scope: ZohoAnalytics.fullaccess.all,ZohoCRM.modules.READ")
            return []

        if not res.ok:
            print(f"  [CRM Live] WARNING: HTTP {res.status_code} on page {page} — stopping")
            break

        data    = res.json()
        records = data.get("data", [])
        if not records:
            break

        all_cases.extend(_normalise_crm_record(r) for r in records)

        info = data.get("info", {})
        more = info.get("more_records", False)
        print(f"  [CRM Live] Page {page}: {len(records)} records "
              f"(total so far: {len(all_cases)}, more: {more})")

        if not more:
            break

    print(f"  [CRM Live] Done — {len(all_cases)} cases fetched from live CRM")
    return all_cases


def fetch_all_open_cases_no_priority(token, threshold_hours=24, max_pages=5):
    """
    Fetch ALL currently open cases with no priority assigned,
    where the case is older than threshold_hours.

    Uses two filters: no Stage in closed stages AND Priority is null (API field name for Case Urgency).
    Returns normalised case dicts.
    """
    cutoff     = datetime.now(timezone.utc) - timedelta(hours=threshold_hours)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    headers   = {"Authorization": f"Zoho-oauthtoken {token}"}
    all_cases = []

    closed_stages = [
        "Closed - Funded",
        "Closed - Not Funded",
        "Closed - NO Response",
    ]
    closed_criteria = "".join(
        f"(Stage:not_equal:{s})and" for s in closed_stages
    )
    # Cases with no urgency AND created before the threshold AND not closed
    criteria = (
        f"{closed_criteria}"
        f"(Priority:is_null:true)and"
        f"(Created_Time:less_than:{cutoff_str})"
    )

    print(f"  [CRM Live] Fetching unprioritized open cases (>{threshold_hours}h)...")

    for page in range(1, max_pages + 1):
        params = {
            "fields":     CRM_FIELDS,
            "criteria":   criteria,
            "sort_by":    "Created_Time",
            "sort_order": "desc",       # Newest first
            "per_page":   200,
            "page":       page,
        }

        try:
            res = requests.get(
                f"{CRM_API_BASE}/Potentials",
                headers=headers,
                params=params,
                timeout=30,
            )
        except requests.exceptions.RequestException as e:
            print(f"  [CRM Live] WARNING: Request failed on page {page}: {e}")
            break

        if res.status_code == 204:
            break

        if res.status_code == 401:
            print("  [CRM Live] ERROR: 401 — missing ZohoCRM.modules.READ scope")
            return []

        if not res.ok:
            print(f"  [CRM Live] WARNING: HTTP {res.status_code} on page {page}")
            break

        data    = res.json()
        records = data.get("data", [])
        if not records:
            break

        all_cases.extend(_normalise_crm_record(r) for r in records)

        if not data.get("info", {}).get("more_records", False):
            break

    print(f"  [CRM Live] Done — {len(all_cases)} unprioritized open cases")
    return all_cases


def fetch_notes_for_cases(token, zoho_record_ids, days=30, max_pages_per_chunk=20):
    """
    Fetch notes for a specific set of Potentials (case) record IDs using COQL.

    Why COQL instead of /Notes search:
      The /Notes search endpoint returned cross-module results from the entire org
      and capped at ~1,000 records. With ~2,400 notes/30d across all modules,
      anything older than ~14 days never reached the indexer — cases >2 weeks old
      appeared interaction-less even when they had notes.

      COQL with WHERE Parent_Id IN (...) filters server-side, so we only retrieve
      notes for our cases. No cross-module contamination, no pagination cap.

    Returns dict: zoho_record_id -> list of note dicts, newest first.
    """
    if not zoho_record_ids:
        return {}

    cutoff     = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    headers    = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "Content-Type":  "application/json",
    }
    indexed = {}

    # COQL has a practical limit on the IN-clause size; chunk into safe batches.
    CHUNK_SIZE = 50
    chunks = [zoho_record_ids[i:i+CHUNK_SIZE]
              for i in range(0, len(zoho_record_ids), CHUNK_SIZE)]

    print(f"  [CRM Live] Fetching notes via COQL for {len(zoho_record_ids)} cases "
          f"({len(chunks)} chunks, last {days} days)...")

    total_fetched = 0
    for chunk_idx, chunk in enumerate(chunks, 1):
        ids_clause = ",".join(f"'{i}'" for i in chunk)
        page = 0
        offset = 0
        per_page = 200

        while page < max_pages_per_chunk:
            page += 1
            query = (
                "SELECT id, Note_Title, Note_Content, Created_Time, Parent_Id "
                "FROM Notes "
                f"WHERE Parent_Id in ({ids_clause}) "
                f"AND Created_Time > '{cutoff_str}' "
                "ORDER BY Created_Time DESC "
                f"LIMIT {offset}, {per_page}"
            )
            try:
                res = requests.post(
                    f"{CRM_API_BASE}/coql",
                    headers=headers,
                    json={"select_query": query},
                    timeout=30,
                )
            except requests.exceptions.RequestException as e:
                print(f"  [CRM Live] WARNING: COQL notes failed (chunk {chunk_idx}, page {page}): {e}")
                break

            if res.status_code == 204:
                break  # no records
            if not res.ok:
                print(f"  [CRM Live] WARNING: COQL notes HTTP {res.status_code} "
                      f"(chunk {chunk_idx}, page {page}): {res.text[:200]}")
                break

            data    = res.json()
            records = data.get("data", [])
            if not records:
                break

            for n in records:
                pid_raw = n.get("Parent_Id")
                parent  = pid_raw.get("id", "") if isinstance(pid_raw, dict) else str(pid_raw or "")
                if not parent:
                    continue
                indexed.setdefault(parent, []).append({
                    "title":   (n.get("Note_Title")   or "").strip(),
                    "content": (n.get("Note_Content") or "").strip(),
                    "created": n.get("Created_Time", ""),
                })

            total_fetched += len(records)
            if not data.get("info", {}).get("more_records", False):
                break
            offset += per_page

    print(f"  [CRM Live] {total_fetched} notes fetched, "
          f"indexed for {len(indexed)} of {len(zoho_record_ids)} cases")
    return indexed
