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


def fetch_recent_cases(token, days=30, max_pages=50):
    """
    Fetch cases created in the last `days` days directly from Zoho CRM.

    Returns a list of normalised case dicts in the same shape as
    Analytics-sourced records, ready to drop into priority intelligence code.

    Pagination: stops naturally when more_records=false; max_pages=50 is a
    defensive ceiling (= 10,000 cases) to prevent runaway pagination if the
    cutoff filter ever malfunctions. NZF typically generates 2,000-3,500
    cases in a 30-day window, so 10-18 pages.

    Previous default (max_pages=5) capped the fetch at 1,000 cases — about
    33% of the actual 30-day population — which silently dropped older cases
    from the attention table.
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
    chunks_with_data    = 0
    chunks_empty        = 0
    chunks_http_error   = 0
    chunks_exception    = 0
    first_query_logged  = False

    for chunk_idx, chunk in enumerate(chunks, 1):
        ids_clause = ",".join(f"'{i}'" for i in chunk)
        page = 0
        offset = 0
        per_page = 200
        chunk_records = 0
        chunk_failed = False

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
            # Log the FIRST query verbatim so we can see exactly what's being sent
            if not first_query_logged:
                print(f"  [CRM Live] First COQL query (truncated to 500 chars): {query[:500]}")
                first_query_logged = True

            try:
                res = requests.post(
                    f"{CRM_API_BASE}/coql",
                    headers=headers,
                    json={"select_query": query},
                    timeout=30,
                )
            except requests.exceptions.RequestException as e:
                chunks_exception += 1
                chunk_failed = True
                print(f"  [CRM Live] EXCEPTION on chunk {chunk_idx}/{len(chunks)} page {page}: "
                      f"{type(e).__name__}: {e}")
                break

            # 204 = no content (Zoho's "empty" response for some endpoints)
            if res.status_code == 204:
                if page == 1:
                    chunks_empty += 1
                    print(f"  [CRM Live] Chunk {chunk_idx}/{len(chunks)}: HTTP 204 (no records)")
                break
            # OAuth scope mismatch — fail fast, no point retrying remaining chunks
            if res.status_code == 401 and "OAUTH_SCOPE_MISMATCH" in res.text:
                print("")
                print("  ╔══════════════════════════════════════════════════════════════════════╗")
                print("  ║  CRITICAL: COQL endpoint requires ZohoCRM.coql.READ scope            ║")
                print("  ║  The current refresh token does not have this scope.                 ║")
                print("  ║                                                                      ║")
                print("  ║  Notes cannot be fetched — every case will appear interaction-less.  ║")
                print("  ║                                                                      ║")
                print("  ║  TO FIX: Regenerate refresh token at https://api-console.zoho.com    ║")
                print("  ║  with scopes:                                                        ║")
                print("  ║    ZohoCRM.modules.ALL                                               ║")
                print("  ║    ZohoCRM.coql.READ                                                 ║")
                print("  ║    ZohoCRM.settings.ALL                                              ║")
                print("  ║    ZohoAnalytics.metadata.READ                                       ║")
                print("  ║    ZohoAnalytics.data.READ                                           ║")
                print("  ║  Then update ZOHO_REFRESH_TOKEN GitHub secret.                       ║")
                print("  ╚══════════════════════════════════════════════════════════════════════╝")
                print("")
                # Mark all remaining chunks as failed without making more API calls
                chunks_http_error += (len(chunks) - chunk_idx + 1)
                return {
                    "_error": "OAUTH_SCOPE_MISMATCH",
                    "_error_message": "COQL endpoint requires ZohoCRM.coql.READ scope. Notes fetch disabled until token is regenerated.",
                }
            # Any non-2xx is a real error — log status, body, and url
            if not res.ok:
                chunks_http_error += 1
                chunk_failed = True
                print(f"  [CRM Live] HTTP {res.status_code} on chunk {chunk_idx}/{len(chunks)} "
                      f"page {page}: body={res.text[:400]}")
                break

            # Try to parse JSON — log the raw body if parse fails
            try:
                data = res.json()
            except ValueError as e:
                chunks_http_error += 1
                chunk_failed = True
                print(f"  [CRM Live] JSON parse error on chunk {chunk_idx}/{len(chunks)} "
                      f"page {page}: {e}; body[:300]={res.text[:300]}")
                break

            records = data.get("data", [])
            if not records:
                if page == 1:
                    chunks_empty += 1
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

            chunk_records += len(records)
            total_fetched += len(records)
            if not data.get("info", {}).get("more_records", False):
                break
            offset += per_page

        if chunk_records > 0:
            chunks_with_data += 1
        if chunk_failed:
            print(f"  [CRM Live] Chunk {chunk_idx} failed mid-fetch — partial data may be missing")

    print(f"  [CRM Live] Notes fetch complete: {total_fetched} notes, "
          f"{len(indexed)} of {len(zoho_record_ids)} cases have notes. "
          f"Chunks: {chunks_with_data} with data, {chunks_empty} empty, "
          f"{chunks_http_error} HTTP errors, {chunks_exception} exceptions.")
    return indexed
