# NZF CRM Dashboards

Live reports built from Zoho Analytics, hosted on Netlify, auto-refreshed via GitHub Actions.

---

## Architecture

```
Zoho CRM  →  Zoho Analytics (auto-sync)
                    ↓  SQL export (3 API calls)
             GitHub Actions (every 6 hrs)
                    ↓  commits JSON files
              GitHub Repository
                    ↓  auto-deploy
                  Netlify
```

---

## Why Zoho Analytics (not CRM API directly)

| | CRM API | Zoho Analytics |
|---|---|---|
| API calls per refresh | 27+ | 3 |
| Record limits | 2,000 per query | None |
| JOINs | Not supported | Full MySQL SQL |
| Notes | Separate call per case | Pre-joined tables |

---

## GitHub Secrets Required

Go to: **GitHub repo → Settings → Secrets and variables → Actions**

| Secret | Value |
|---|---|
| `ZOHO_CLIENT_ID` | From Zoho API Console → Self Client |
| `ZOHO_CLIENT_SECRET` | From Zoho API Console → Self Client |
| `ZOHO_REFRESH_TOKEN` | See setup steps below |
| `ZOHO_ACCOUNTS_URL` | `https://accounts.zoho.com` |

> `ZOHO_API_DOMAIN` is no longer needed — you can delete it if it exists.

---

## First-Time Setup

### Step 1 — Get a Refresh Token

1. Go to [api-console.zoho.com](https://api-console.zoho.com)
2. Open your **Self Client**
3. Click **Generate Code** tab
4. Scope: `ZohoAnalytics.data.read`
5. Duration: `10 minutes` → click **Create** → copy the code
6. Go to GitHub → your repo → **Actions** → **🔑 Setup — Generate Refresh Token**
7. Click **Run workflow** → paste the code → click **Run workflow**
8. Open the job logs → copy the `refresh_token` value
9. Save it as the `ZOHO_REFRESH_TOKEN` GitHub secret

### Step 2 — Connect Netlify

1. Netlify → **Add new site → Import from GitHub**
2. Select this repo
3. Build command: *(leave blank)*
4. Publish directory: `.`
5. Deploy

### Step 3 — First data refresh

GitHub → Actions → **Refresh Zoho CRM Data** → **Run workflow**

Netlify auto-deploys within ~30 seconds of each data commit.

---

## Analytics Workspace

| Setting | Value |
|---|---|
| Org ID | `668395719` |
| Workspace | Zoho CRM Analytics - Marketing |
| Workspace ID | `1715382000001002475` |

Key tables used:
- `Cases` — all case records synced from CRM
- `Distributions` — all distribution records
- `Cases x Distribution x Notes - All` — pre-joined view with notes

---

## Client Report — Data Logic

| Metric | Logic |
|---|---|
| **New clients** | Cases where client has NO paid/extracted distribution before this case |
| **Returning clients** | Cases where client HAS at least one prior paid/extracted distribution |
| **Excluded stages** | Ongoing Funding, Post Funding - Follow Up, Post=Follow-Up, etc. |
| **Last assistance date** | MAX(COALESCE(Paid Date, Extracted Date, Created Time)) per client, before this case |
| **Return gap** | Days between last paid distribution and this case's Created Time |

---

## Enabling Entra ID Auth (when ready)

1. Register an App in Azure → Entra ID → App Registrations
2. In `assets/js/auth.js` set `AUTH_ENABLED: true` + add Client ID + Tenant ID
3. Uncomment the MSAL script tag in each dashboard HTML
4. Uncomment the redirect blocks in `netlify.toml`
