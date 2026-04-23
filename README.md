# NZF CRM Dashboards

Live operational dashboards for National Zakat Foundation Australia, built on
Zoho Analytics data, hosted on Netlify, auto-refreshed via GitHub Actions.

---

## Architecture

```
Zoho CRM  →  Zoho Analytics (auto-sync nightly)
                    ↓  View fetch API (ZohoAnalytics.data.read)
             GitHub Actions (every 6 hours)
                    ↓  Python scripts write JSON to /data/
              GitHub Repository
                    ↓  Netlify auto-deploys on every commit
                  Netlify
```

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

This file is the single source of truth for:
- New vs returning client definitions
- Same-instance case exclusions (ILA keywords, ongoing stages)
- Priority classification guide (used by AI analysis)
- SLA targets (response and resolution, by priority)
- Distribution paid status rules
- Working hours definition (for P1 business-hours SLA)
- Reporting window sizes
- Zoho view IDs and org identifiers

**To change any business rule — edit `nzf_rules.json` and re-run the workflow.**
No Python or HTML changes needed.

**For Claude AI context** — upload `nzf_rules.json` to the Claude project so
Claude always has full NZF business definitions when building new dashboards.

---

## GitHub Secrets Required

| Secret | Value |
|---|---|
| `ZOHO_CLIENT_ID` | From Zoho API Console → Self Client |
| `ZOHO_CLIENT_SECRET` | From Zoho API Console → Self Client |
| `ZOHO_REFRESH_TOKEN` | Generated via the Setup workflow (scope: `ZohoAnalytics.data.read`) |
| `ZOHO_ACCOUNTS_URL` | `https://accounts.zoho.com` |
| `ANTHROPIC_API_KEY` | From console.anthropic.com — enables AI analysis on Cases + Clients |

---

## First-Time Setup

### Step 1 — Get a Refresh Token

1. Go to [api-console.zoho.com](https://api-console.zoho.com) → Self Client
2. Click **Generate Code** → Scope: `ZohoAnalytics.data.read` → Duration: 10 minutes
3. GitHub → Actions → **Setup - Generate Refresh Token** → Run workflow → paste code
4. Copy the `refresh_token` from the logs → save as `ZOHO_REFRESH_TOKEN` secret

### Step 2 — Connect Netlify

1. Netlify → Add new site → Import from GitHub → select this repo
2. Build command: *(leave blank)*
3. Publish directory: `.`

### Step 3 — First data refresh

GitHub → Actions → **Refresh CRM Data** → Run workflow → select `all`

---

## Workflow Options

The refresh workflow can be triggered manually with a specific report:

| Option | What runs |
|---|---|
| `all` | All 4 scripts |
| `clients` | Client report only |
| `cases` | Cases report only |
| `cases-perf` | Cases performance only |
| `distributions` | Distributions only |

---

## Zoho Analytics Workspace

| Setting | Value |
|---|---|
| Org ID | `668395719` |
| Workspace | Zoho CRM Analytics - Marketing |
| Workspace ID | `1715382000001002475` |

Key views used (all IDs in `nzf_rules.json`):

| View | ID |
|---|---|
| Cases | `1715382000001002494` |
| Clients | `1715382000001002492` |
| Distributions | `1715382000001002628` |
| Case Notes | `1715382000012507001` |

---

## AI Features

Two dashboards use the Anthropic API (`ANTHROPIC_API_KEY` GitHub secret):

**Client Report** — qualitative analysis of why returning clients are coming back,
plus per-case summaries in the Returning Cases table. Runs at refresh time.

**Cases Report** — priority accuracy analysis detecting potential misclassifications,
plus alert for cases with no priority assigned for >24 hours. Runs at refresh time.

Both use `claude-haiku-4-5-20251001` for cost efficiency.

---

## Enabling Auth (Entra ID SSO)

Auth hooks are built into every dashboard but currently disabled.
To enable:

1. Register an App in Azure → Entra ID → App Registrations
2. In `assets/js/auth.js` set `AUTH_ENABLED: true` and add `ENTRA_CLIENT_ID` + `ENTRA_TENANT_ID`
3. Uncomment the MSAL script tag in each dashboard HTML
4. Uncomment the redirect blocks in `netlify.toml`

---

## Disclaimer

All dashboards are in draft stage. Data may not be accurate.
For questions contact Farooq (farooq.syed@nzf.org.au).

