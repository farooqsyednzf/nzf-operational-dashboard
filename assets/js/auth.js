/**
 * auth.js — NZF Dashboard Authentication Module
 *
 * CURRENT MODE: Auth disabled (open access)
 *
 * TO ENABLE ENTRA ID LATER:
 *   1. Register an App in Azure Portal (Entra ID)
 *   2. Set ENTRA_CLIENT_ID and ENTRA_TENANT_ID below
 *   3. Set AUTH_ENABLED = true
 *   4. Add the MSAL script tag to every HTML page (already included, commented out)
 *
 * No other changes needed across the dashboards — all auth logic is here.
 */

const AUTH_CONFIG = {
  AUTH_ENABLED: false,                          // ← Flip to true when ready

  // Azure / Entra ID — fill these in when enabling auth
  ENTRA_CLIENT_ID: "YOUR_AZURE_APP_CLIENT_ID",  // From Azure App Registration
  ENTRA_TENANT_ID: "YOUR_AZURE_TENANT_ID",      // Your NZF Azure tenant ID
  REDIRECT_URI: window.location.origin,         // Auto-detects Netlify URL

  // Optional: restrict to a specific Azure AD group
  // REQUIRED_GROUP_ID: "YOUR_AD_GROUP_OBJECT_ID",
};

// ─────────────────────────────────────────────
//  PUBLIC API — used by every dashboard page
// ─────────────────────────────────────────────

/**
 * Call once on each page load.
 * - Auth disabled: resolves immediately (no-op)
 * - Auth enabled:  redirects to Microsoft login if not authenticated
 */
async function requireAuth() {
  if (!AUTH_CONFIG.AUTH_ENABLED) {
    console.info("[Auth] Auth disabled — open access mode");
    return;
  }

  // ── ENTRA ID PATH (activated when AUTH_ENABLED = true) ──────────────────
  // Uncomment the MSAL script in your HTML pages, then this will run:

  /*
  const msalConfig = {
    auth: {
      clientId: AUTH_CONFIG.ENTRA_CLIENT_ID,
      authority: `https://login.microsoftonline.com/${AUTH_CONFIG.ENTRA_TENANT_ID}`,
      redirectUri: AUTH_CONFIG.REDIRECT_URI,
    },
    cache: { cacheLocation: "sessionStorage" },
  };

  const msalInstance = new msal.PublicClientApplication(msalConfig);
  await msalInstance.initialize();

  // Handle redirect response first
  await msalInstance.handleRedirectPromise();

  const accounts = msalInstance.getAllAccounts();
  if (accounts.length === 0) {
    // Not logged in — redirect to Microsoft login
    await msalInstance.loginRedirect({
      scopes: ["User.Read"],
    });
    return;
  }

  // Store user info for the nav bar
  const account = accounts[0];
  window.__NZF_USER = {
    name: account.name,
    email: account.username,
    initials: account.name.split(" ").map(n => n[0]).join("").toUpperCase(),
  };

  renderUserNav(window.__NZF_USER);
  */
}

/**
 * Returns current user object, or a placeholder in open-access mode.
 */
function getCurrentUser() {
  if (!AUTH_CONFIG.AUTH_ENABLED) {
    return { name: "Guest", email: "", initials: "G" };
  }
  return window.__NZF_USER || null;
}

/**
 * Sign out the current user.
 * No-op in open-access mode.
 */
function signOut() {
  if (!AUTH_CONFIG.AUTH_ENABLED) return;
  // msalInstance.logoutRedirect();
}

// ─────────────────────────────────────────────
//  INTERNAL — render user badge in nav
// ─────────────────────────────────────────────
function renderUserNav(user) {
  const badge = document.getElementById("user-badge");
  if (!badge || !user) return;
  badge.innerHTML = `
    <div class="user-avatar">${user.initials}</div>
    <span class="user-name">${user.name}</span>
    <button class="signout-btn" onclick="signOut()">Sign out</button>
  `;
}
