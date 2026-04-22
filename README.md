# NZF CRM Dashboards

HTML dashboards sourced from Zoho CRM, hosted on Netlify, auto-refreshed via GitHub Actions.

---

## Project Structure

```
nzf-dashboards/
├── .github/workflows/
│   └── refresh-data.yml          # Scheduled Zoho data fetch (every 6 hours)
├── assets/
│   ├── css/dashboard.css         # Shared brand-compliant styles (NZF brand guide)
│   └── js/
│       ├── auth.js               # Auth module — Entra ID ready, disabled by default
│       └── nav.js                # Shared sidebar navigation
├── dashboards/
│   ├── index.html                # Home / status page
│   ├── clients.html              # ✅ Client Report (new/returning, trends, AI analysis)
│   ├── pipeline.html             # Case pipeline
│   ├── cases.html                # Case summary (coming soon)
│   └── distributions.html       # Distributions (coming soon)
├── data/                         # JSON files written by GitHub Action
│   ├── meta.json                 # Aggregate refresh status
│   ├── clients.json              # Client report data
│   └── pipeline.json             # Pipeline data
├── scripts/
│   ├── fetch_clients_data.py     # Zoho → clients.json
│   └── fetch_zoho_data.py        # Zoho → pipeline.json
└── netlify.toml                  # Hosting + redirect config
```

---

## Zoho CRM Module Mapping

NZF uses custom labels. The API module names are:

| NZF Label      | Zoho API Name    |
|----------------|------------------|
| Clients        | `Contacts`       |
| Cases          | `Deals`          |
| Distributions  | `Purchase_Orders`|

---

## Client Report — Data Logic

| Metric | Logic |
|---|---|
| **New clients** | Cases where `New_or_existing = "New"` |
| **Returning clients** | Cases where `New_or_existing = "Existing"` AND `Stage` NOT IN ongoing stages |
| **Excluded stages** | Ongoing Funding, Post Funding - Follow Up, Post=Follow-Up, Post- Follow-Up, Phase 4: Monitoring & Impact |
| **Last assistance date** | Latest `Paid_Date` (if Status=Paid) or `Extracted_Date` (if Status=Extracted) across all distributions for that client |
| **Return gap** | Days between last paid distribution and new case `Created_Time` |

---

## Setup Guide

### 1. GitHub Repository
Push this code to a new GitHub repo, then add **Repository Secrets** (Settings → Secrets → Actions):

| Secret | Value |
|---|---|
| `ZOHO_CLIENT_ID` | From Zoho API Console → Self Client |
| `ZOHO_CLIENT_SECRET` | From Zoho API Console → Self Client |
| `ZOHO_REFRESH_TOKEN` | See below |
| `ZOHO_ACCOUNTS_URL` | `https://accounts.zoho.com` |
| `ZOHO_API_DOMAIN` | `https://www.zohoapis.com` |

### 2. Getting a Zoho Refresh Token (one-time)
1. Go to [Zoho API Console](https://api-console.zoho.com/)
2. Create a **Self Client**
3. Generate a code with these scopes:
   ```
   ZohoCRM.modules.deals.READ,ZohoCRM.modules.contacts.READ,ZohoCRM.modules.purchase_orders.READ,ZohoCRM.modules.notes.READ,ZohoCRM.coql.READ
   ```
4. Exchange for a refresh token:
   ```bash
   curl -X POST https://accounts.zoho.com/oauth/v2/token \
     -d "code=YOUR_CODE" \
     -d "client_id=YOUR_CLIENT_ID" \
     -d "client_secret=YOUR_CLIENT_SECRET" \
     -d "redirect_uri=https://www.zoho.com/crm" \
     -d "grant_type=authorization_code"
   ```
5. Save the `refresh_token` value as your GitHub secret.

### 3. Netlify
1. Netlify → **Add new site → Import from GitHub**
2. Select this repository
3. Build command: *(leave blank)*
4. Publish directory: `.`
5. Deploy — your site will live at `https://your-site.netlify.app`

### 4. First Data Refresh
Go to **GitHub → Actions → Refresh Zoho CRM Data → Run workflow** to populate data files immediately.

---

## Enabling Entra ID Authentication (when ready)

1. Register an App in **Azure Portal → Entra ID → App Registrations**
   - Redirect URI: `https://your-site.netlify.app`
   - Note the **Client ID** and **Tenant ID**

2. In `assets/js/auth.js`:
   ```js
   AUTH_ENABLED: true,
   ENTRA_CLIENT_ID: "your-client-id",
   ENTRA_TENANT_ID: "your-tenant-id",
   ```

3. In every dashboard HTML, uncomment:
   ```html
   <script src="https://alcdn.msauth.net/browser/2.39.0/js/msal-browser.min.js"></script>
   ```

4. In `netlify.toml`, uncomment the redirect blocks.

---

## Data Refresh Schedule
Configured in `.github/workflows/refresh-data.yml`:
```yaml
cron: '0 */6 * * *'   # Every 6 hours
```
Each data refresh commits updated JSON files → Netlify auto-deploys in ~30 seconds.
