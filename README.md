# NZF CRM Dashboards

Live operational dashboards for National Zakat Foundation Australia.
Data sourced from Zoho CRM and Zoho Analytics, hosted on Netlify,
auto-refreshed every 6 hours via GitHub Actions.

---

## Architecture

```
Zoho CRM  ──────────────────────────────────────────────────────► Live CRM API
  (real-time)                                                       └─ Priority Intelligence only
                                                                       (unprioritized alert + AI accuracy)

Zoho CRM  ──► Zoho Analytics (nightly sync) ──► GitHub Actions ──► JSON files ──► Netlify
  (via sync)    up to 24h delay                  (every 6 hours)
                                                  └─ All other metrics
```

### Why two sources?

| Data | Source | Reason |
|---|---|---|
| Cases KPIs, trends, priority charts | Zoho Analytics | Not time-sensitive, high volume — Analytics handles it efficiently |
| Cases Performance (SLA, closure times) | Zoho Analytics | Historical metrics, latency acceptable |
| Client report, distributions | Zoho Analytics | Not time-sensitive |
| **Unprioritized cases alert** | **Live Zoho CRM** | A P1 case is invisible in Analytics for up to 24h |
| **AI priority accuracy (30 days)** | **Live Zoho CRM** | Recent misclassifications must be detected in near real-time |

---

## Dashboards

| Dashboard | URL | Data file | Script |
|---|---|---|---|
| Home | `/dashboards/index.html` | `meta.json` | — |
| Client Report | `/dashboards/clients.html` | `clients.json` | `fetch_clients_data.py` |
| Cases Report | `/dashboards/cases.html` | `cases.json` | `fetch_cases_data.py` |
| Cases Performance | `/dashboards/cases_perf.html` | `cases_perf.json` | `fetch_cases_perf_data.py` |
| Distributions | `/dashboards/distributions.html` | `distributions.json` | `fetch_distributions_data.py` |

---

## Business Rules

All business logic lives in one place:

```
config/nzf_rules.json
```

Single source of truth for: new vs returning client definitions, same-instance
exclusions, priority classification guide (used by AI), SLA targets by priority,
distribution paid status rules, working hours (P1 business-hours SLA),
reporting window sizes, and Zoho org/workspace IDs.

**To change any business rule — edit `nzf_rules.json` and re-run the workflow.**
No Python or HTML changes needed.

**Upload `nzf_rules.json` to the Claude project** so Claude always has full NZF
business definitions when building new dashboards.

---

## GitHub Secrets Required

| Secret | Value |
|---|---|
| `ZOHO_CLIENT_ID` | Zoho API Console → Self Client |
| `ZOHO_CLIENT_SECRET` | Zoho API Console → Self Client |
| `ZOHO_REFRESH_TOKEN` | Generated via Setup workflow (see below) |
| `ZOHO_ACCOUNTS_URL` | `https://accounts.zoho.com` |
| `ANTHROPIC_API_KEY` | console.anthropic.com — enables AI on Cases + Clients |

---

## First-Time Setup

### Step 1 — Get a Refresh Token

The refresh token must include **both** Analytics and CRM scopes:

1. Go to [api-console.zoho.com](https://api-console.zoho.com) → Self Client
2. Click **Generate Code**
3. **Scope** — paste exactly:
   ```
   ZohoAnalytics.fullaccess.all,ZohoCRM.modules.READ
   ```
4. Duration: 10 minutes → **Create** → copy the code
5. GitHub → **Actions** → **Setup — Generate Refresh Token** → **Run workflow** → paste code
6. Copy `refresh_token` from the workflow log
7. GitHub → **Settings** → **Secrets** → **Actions** → save as `ZOHO_REFRESH_TOKEN`

> If you only have the old `ZohoAnalytics.fullaccess.all` token, the dashboard
> still works but the Priority Intelligence section will fail silently and
> fall back to empty data. Re-run Step 1 to get the combined-scope token.

### Step 2 — Connect Netlify

1. Netlify → **Add new site** → **Import from GitHub** → select this repo
2. Build command: *(leave blank)*
3. Publish directory: `.`

### Step 3 — First data refresh

GitHub → **Actions** → **Refresh CRM Data** → **Run workflow** → select `all`

---

## Workflow Options

| Option | What runs |
|---|---|
| `all` | All 4 scripts |
| `clients` | Client report only |
| `cases` | Cases report + Priority Intelligence (live CRM) |
| `cases-perf` | Cases Performance only |
| `distributions` | Distributions only |

---

## Zoho Workspace Reference

| Setting | Value |
|---|---|
| Analytics Org ID | `668395719` |
| Analytics Workspace ID | `1715382000001002475` |
| CRM Org ID | `org30478025` |
| CRM API Base | `https://www.zohoapis.com/crm/v6` |

All view IDs are in `nzf_rules.json → zoho_modules`.

---

## AI Features

| Dashboard | Feature | Model |
|---|---|---|
| Client Report | Qualitative why-returning analysis + per-case summaries | claude-sonnet-4-20250514 |
| Cases Report | Priority accuracy analysis + urgency summaries for unassigned cases | claude-sonnet-4-20250514 |

Model is controlled by `nzf_rules.json → ai.model`. Change it there to apply everywhere.

---

## Enabling Auth (Entra ID / Microsoft SSO)

Auth hooks are built in but currently disabled.

1. Register an app in Azure → Entra ID → App Registrations
2. In `assets/js/auth.js`: set `AUTH_ENABLED: true`, add `ENTRA_CLIENT_ID` + `ENTRA_TENANT_ID`
3. Uncomment the MSAL script tag in each dashboard HTML file
4. Uncomment the redirect blocks in `netlify.toml`

---

## PII Policy

No client names, caseworker names, or personal identifiers are stored in JSON
or displayed in any dashboard. All records are identified by Zoho CRM record IDs
only, displayed as clickable deep links opening directly in CRM.

---

## Disclaimer

All dashboards are in draft stage. Data may not be accurate.
For questions contact Farooq (farooq.syed@nzf.org.au).
