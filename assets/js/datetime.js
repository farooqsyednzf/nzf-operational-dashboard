// ─── Melbourne-anchored datetime formatters ──────────────────────
// Every timestamp on these dashboards displays in Melbourne local time
// (AEST/AEDT, auto-switching for daylight saving) regardless of the
// viewer's browser timezone or where the data refresh job ran.
//
// Why: GitHub Actions runners are in UTC. When timestamps render in the
// viewer's local timezone, schedules running in UTC and manual runs
// (which sometimes report in GitHub's UI as +10) produce inconsistent
// "Last Refreshed" displays. Anchoring to Melbourne makes the dashboard
// behaviour deterministic for NZF's audience.
//
// Usage:
//   fmtDateTime(iso)           → "28 Apr 2026, 09:14 AEST"
//   fmtDateTime(iso, {short})  → "28 Apr, 09:14 AEST"   (no year)
//   fmtDateTime(iso, {noTz})   → "28 Apr 2026, 09:14"   (no zone label)
//   fmtDate(iso)               → "28 Apr 2026"
//   fmtDate(iso, {short})      → "28 Apr"
//
// All helpers gracefully handle null/undefined/invalid input → "—".

(function (global) {
  const TZ = 'Australia/Melbourne';

  // Returns a TZ abbreviation like "AEST" or "AEDT" for a given Date.
  // Uses Intl to look up the short timeZoneName so DST transitions are
  // handled automatically by the platform.
  function tzAbbrev(d) {
    try {
      const parts = new Intl.DateTimeFormat('en-AU', {
        timeZone: TZ,
        timeZoneName: 'short',
      }).formatToParts(d);
      const tzPart = parts.find(p => p.type === 'timeZoneName');
      return tzPart ? tzPart.value : 'AEST';
    } catch (e) {
      return 'AEST';
    }
  }

  function fmtDateTime(iso, opts = {}) {
    if (!iso) return '—';
    const d = new Date(iso);
    if (isNaN(d.getTime())) return '—';

    const fmt = new Intl.DateTimeFormat('en-AU', {
      timeZone: TZ,
      day:    'numeric',
      month:  'short',
      year:   opts.short ? undefined : 'numeric',
      hour:   '2-digit',
      minute: '2-digit',
      hour12: false,
    });
    const base = fmt.format(d);
    return opts.noTz ? base : `${base} ${tzAbbrev(d)}`;
  }

  function fmtDate(iso, opts = {}) {
    if (!iso) return '—';
    const d = new Date(iso);
    if (isNaN(d.getTime())) return '—';

    return new Intl.DateTimeFormat('en-AU', {
      timeZone: TZ,
      day:   'numeric',
      month: 'short',
      year:  opts.short ? undefined : 'numeric',
    }).format(d);
  }

  // Expose globally so each dashboard's inline scripts can use them.
  global.fmtDateTime = fmtDateTime;
  global.fmtDate     = fmtDate;
})(window);
