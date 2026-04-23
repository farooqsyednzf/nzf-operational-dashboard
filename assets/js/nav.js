/**
 * nav.js - Renders the sidebar and topbar, marks the active page.
 * Import this on every dashboard page.
 */

const NAV_ITEMS = [
  {
    section: "Overview",
    items: [
      { label: "Home",             href: "/dashboards/index.html",    icon: "home" },
    ]
  },
  {
    section: "Clients",
    items: [
      { label: "Client Report",    href: "/dashboards/clients.html",  icon: "clients" },
    ]
  },
  {
    section: "Cases",
    items: [
      { label: "Cases Report",       href: "/dashboards/cases.html",      icon: "chart"    },
      { label: "Cases Performance",  href: "/dashboards/cases_perf.html", icon: "pipeline" },
    ]
  },
  {
    section: "Distributions",
    items: [
      { label: "Distributions", href: "/dashboards/distributions.html", icon: "distributions" },
    ]
  },
];

const ICONS = {
  home:          `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>`,
  clients:       `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/></svg>`,
  pipeline:      `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>`,
  chart:         `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>`,
  distributions: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 8 12 12 14 14"/><path d="M16 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10"/><path d="M8 2a15.3 15.3 0 0 0-4 10 15.3 15.3 0 0 0 4 10"/></svg>`,
  activity:      `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>`,
};

function renderNav(pageTitle) {
  const currentPath = window.location.pathname;

  // Build sidebar HTML
  const navHTML = NAV_ITEMS.map(section => `
    <div class="nav-section-label">${section.section}</div>
    ${section.items.map(item => {
      const isActive = currentPath.endsWith(item.href.replace('/dashboards/', ''));
      return `
        <a href="${item.href}" class="nav-item ${isActive ? 'active' : ''}">
          ${ICONS[item.icon] || ''}
          <span>${item.label}</span>
        </a>`;
    }).join('')}
  `).join('');

  const authEnabled = typeof AUTH_CONFIG !== 'undefined' && AUTH_CONFIG.AUTH_ENABLED;

  const sidebar = `
    <aside class="sidebar">
      <div class="sidebar-logo">
        <div class="org-name">NZF</div>
        <div class="org-sub">CRM Dashboards</div>
      </div>
      <nav class="sidebar-nav">
        ${navHTML}
      </nav>
      <div class="sidebar-footer">
        <div id="user-badge">
          ${!authEnabled ? `<div class="auth-status-badge">Auth Disabled</div>` : ''}
        </div>
      </div>
    </aside>`;

  const lastUpdated = getLastUpdated();
  const topbar = `
    <header class="topbar">
      <span class="topbar-title">${pageTitle}</span>
      <div class="topbar-meta">
        <span class="last-updated">Data refreshed: ${lastUpdated}</span>
      </div>
    </header>`;

  // Inject into page
  document.getElementById('sidebar-mount').innerHTML = sidebar;
  document.getElementById('topbar-mount').innerHTML  = topbar;
}

function getLastUpdated() {
  // Will be populated from data JSON files (meta.last_updated)
  // Falls back to a placeholder
  try {
    const meta = window.__NZF_META;
    if (meta && meta.last_updated) {
      return new Date(meta.last_updated).toLocaleString('en-NZ', {
        day: 'numeric', month: 'short', year: 'numeric',
        hour: '2-digit', minute: '2-digit'
      });
    }
  } catch(e) {}
  return 'Not yet available';
}
