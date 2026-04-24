/**
 * TableFilter — reusable column-level filter component for NZF dashboards
 *
 * Renders a persistent filter row directly under column headers.
 * Each column gets an input appropriate for its data type.
 * Active filters highlight the column header with the NZF red accent.
 *
 * Usage:
 *   const tf = new TableFilter({
 *     tableEl:    document.getElementById('my-table'),
 *     getRows:    () => _rows,
 *     renderRow:  (row) => `<tr>...</tr>`,
 *     tbodyId:    'my-tbody',
 *     columns: [
 *       { key:'status',    type:'select',  label:'Status',    values:['paid','overdue'] },
 *       { key:'dist_id',   type:'text',    label:'Dist. ID'   },
 *       { key:'payee',     type:'text',    label:'Payee'      },
 *       { key:'amount',    type:'amount',  label:'Amount'     },
 *       { key:'date',      type:'date',    label:'Date'       },
 *       { key:'hours',     type:'hours',   label:'Hours'      },
 *       { key:'has_bill',  type:'select',  label:'Xero Bill', values:['Yes','No'] },
 *     ],
 *     externalFilter: () => true,   // optional fn(row) => bool — external tab/search
 *     onUpdate:  (shown, total) => {},  // footer callback
 *     emptyMsg:  'No records match',
 *     colspan:   7,
 *   });
 *
 *   tf.build();          // inject filter row into <thead>
 *   tf.render();         // initial table render
 *   tf.refresh();        // call after data or externalFilter changes
 *   tf.clearAll();       // reset all column filters
 */
class TableFilter {
  constructor(cfg) {
    this.tableEl        = cfg.tableEl;
    this.getRows        = cfg.getRows;
    this.renderRow      = cfg.renderRow;
    this.tbodyId        = cfg.tbodyId;
    this.columns        = cfg.columns;            // [{key, type, label, values?}]
    this.externalFilter = cfg.externalFilter || (() => true);
    this.onUpdate       = cfg.onUpdate || (() => {});
    this.emptyMsg       = cfg.emptyMsg || 'No records match this filter';
    this.colspan        = cfg.colspan || this.columns.length;
    this.clearBtnId     = cfg.clearBtnId || null;  // optional external clear-all button

    this._state = {};   // { [col.key]: value | {min,max} | {from,to} }
    this._filterRow = null;
  }

  // ── Build filter row ──────────────────────────────────────────────
  build() {
    const thead = this.tableEl.querySelector('thead');
    if (!thead) return;

    // Remove any existing filter row
    const existing = thead.querySelector('tr.tf-filter-row');
    if (existing) existing.remove();

    const tr = document.createElement('tr');
    tr.className = 'tf-filter-row';

    this.columns.forEach(col => {
      const th = document.createElement('th');
      th.className = 'tf-filter-cell';
      th.dataset.key = col.key;
      th.innerHTML = this._buildInput(col);
      tr.appendChild(th);
    });

    thead.appendChild(tr);
    this._filterRow = tr;
    this._bindEvents();
  }

  _buildInput(col) {
    const id = 'tf-' + col.key;
    const ph = 'Filter…';

    // Helper: build a custom dropdown (replaces native <select> which has cross-browser
    // rendering issues inside table headers — appears as expanded listbox on Chrome/Safari)
    const dd = (options) => {
      const items = options.map(([val, label]) =>
        '<div class="tf-dd-option' + (val===''?' tf-dd-selected':'') + '" data-val="' + val + '">' + label + '</div>'
      ).join('');
      return '<div class="tf-dd" data-key="' + col.key + '" id="' + id + '">' +
        '<button class="tf-dd-btn" type="button">' +
          '<span class="tf-dd-label">All</span>' +
          '<svg class="tf-dd-arrow" width="8" height="5" viewBox="0 0 8 5" fill="none"><path d="M0 0l4 5 4-5z" fill="currentColor"/></svg>' +
        '</button>' +
        '<div class="tf-dd-menu">' + items + '</div>' +
      '</div>';
    };

    switch (col.type) {
      case 'text':
        return '<input class="tf-input tf-text" id="' + id + '" type="text" ' +
               'placeholder="' + ph + '" data-key="' + col.key + '" autocomplete="off">';

      case 'select': {
        const opts = [['', 'All']].concat(
          (col.values || []).map(v => [v, (col.valueLabels && col.valueLabels[v]) || v])
        );
        return dd(opts);
      }

      case 'amount':
        return '<div class="tf-range-pair">' +
          '<input class="tf-input tf-range-input" type="number" min="0" step="1" ' +
            'placeholder="Min $" data-key="' + col.key + '" data-bound="min">' +
          '<input class="tf-input tf-range-input" type="number" min="0" step="1" ' +
            'placeholder="Max $" data-key="' + col.key + '" data-bound="max">' +
        '</div>';

      case 'date':
        return '<div class="tf-range-pair">' +
          '<input class="tf-input tf-date-input" type="date" ' +
            'title="From" data-key="' + col.key + '" data-bound="from">' +
          '<input class="tf-input tf-date-input" type="date" ' +
            'title="To" data-key="' + col.key + '" data-bound="to">' +
        '</div>';

      case 'hours':
        return dd([['','All'],['lt48','< 48h'],['48to72','48 – 72h'],['gt72','> 72h'],['null','—']]);

      default:
        return '<input class="tf-input tf-text" type="text" ' +
               'placeholder="' + ph + '" data-key="' + col.key + '">';
    }
  }

  // ── Populate select options from live data ────────────────────────
  populateSelects() {
    const rows = this.getRows();
    this.columns.forEach(col => {
      if (col.type !== 'select' || col.values) return;
      const dd = this._filterRow?.querySelector('.tf-dd[data-key="' + col.key + '"]');
      if (!dd) return;
      const menu   = dd.querySelector('.tf-dd-menu');
      const curVal = this._state[col.key] || '';
      const uniq   = [...new Set(rows.map(r => (r[col.key] ?? '').toString()).filter(Boolean))].sort();
      menu.innerHTML = '<div class="tf-dd-option' + (curVal===''?' tf-dd-selected':'') + '" data-val="">All</div>' +
        uniq.map(v => {
          const label = (col.valueLabels && col.valueLabels[v]) || v;
          return '<div class="tf-dd-option' + (v===curVal?' tf-dd-selected':'') + '" data-val="' + v + '">' + label + '</div>';
        }).join('');
      menu.querySelectorAll('.tf-dd-option').forEach(opt => {
        opt.addEventListener('click', e => {
          e.stopPropagation();
          menu.querySelectorAll('.tf-dd-option').forEach(o => o.classList.remove('tf-dd-selected'));
          opt.classList.add('tf-dd-selected');
          const val = opt.dataset.val;
          dd.querySelector('.tf-dd-label').textContent = opt.textContent;
          menu.classList.remove('tf-dd-open');
          this._state[col.key] = val;
          this._updateDDHighlight(dd, val);
          this._updateHeaderHighlight(col.key);
          this.render();
          this._updateClearBtn();
        });
      });
    });
  }

  // ── Bind events ───────────────────────────────────────────────────
  _bindEvents() {
    this._filterRow.querySelectorAll('input').forEach(inp => {
      inp.addEventListener('input',  () => this._onChange(inp));
      inp.addEventListener('change', () => this._onChange(inp));
      inp.addEventListener('click',  e => e.stopPropagation());
    });
    this._filterRow.querySelectorAll('.tf-dd').forEach(dd => this._bindDropdown(dd));
    document.addEventListener('click', () => {
      document.querySelectorAll('.tf-dd-menu').forEach(m => m.classList.remove('tf-dd-open'));
    });
    if (this.clearBtnId) {
      const btn = document.getElementById(this.clearBtnId);
      if (btn) btn.addEventListener('click', () => this.clearAll());
    }
  }

  _bindDropdown(dd) {
    const btn  = dd.querySelector('.tf-dd-btn');
    const menu = dd.querySelector('.tf-dd-menu');
    btn.addEventListener('click', e => {
      e.stopPropagation();
      const isOpen = menu.classList.contains('tf-dd-open');
      document.querySelectorAll('.tf-dd-menu').forEach(m => m.classList.remove('tf-dd-open'));
      if (!isOpen) menu.classList.add('tf-dd-open');
    });
    this._bindDropdownOptions(menu, dd);
  }

  _bindDropdownOptions(menu, dd) {
    menu.querySelectorAll('.tf-dd-option').forEach(opt => {
      opt.addEventListener('click', e => {
        e.stopPropagation();
        menu.querySelectorAll('.tf-dd-option').forEach(o => o.classList.remove('tf-dd-selected'));
        opt.classList.add('tf-dd-selected');
        const val = opt.dataset.val;
        dd.querySelector('.tf-dd-label').textContent = opt.textContent;
        menu.classList.remove('tf-dd-open');
        this._state[dd.dataset.key] = val;
        this._updateDDHighlight(dd, val);
        this._updateHeaderHighlight(dd.dataset.key);
        this.render();
        this._updateClearBtn();
      });
    });
  }

  _updateDDHighlight(dd, val) {
    dd.querySelector('.tf-dd-btn').classList.toggle('tf-dd-active', !!(val && val.trim()));
  }

  _onChange(inp) {
    const key   = inp.dataset.key;
    const bound = inp.dataset.bound;   // 'min'|'max'|'from'|'to' or undefined

    if (bound) {
      if (!this._state[key] || typeof this._state[key] !== 'object') {
        this._state[key] = {};
      }
      const val = inp.value;
      this._state[key][bound] = val;
    } else {
      this._state[key] = inp.value;
    }

    this._updateHeaderHighlight(key);
    this.render();
    this._updateClearBtn();
  }

  _updateHeaderHighlight(key) {
    const cell = this._filterRow?.querySelector(`[data-key="${key}"]`);
    if (!cell) return;
    // Find the matching th in the header row (first tr)
    const headerRow = this.tableEl.querySelector('thead tr:first-child');
    if (!headerRow) return;
    const cells = [...this._filterRow.querySelectorAll('th')];
    const idx   = cells.findIndex(c => c.dataset.key === key);
    const hCell = headerRow.querySelectorAll('th')[idx];
    if (!hCell) return;
    const active = this._isActive(key);
    hCell.classList.toggle('tf-col-active', active);
  }

  _isActive(key) {
    const v = this._state[key];
    if (!v) return false;
    if (typeof v === 'string') return v.trim() !== '';
    if (typeof v === 'object') return Object.values(v).some(x => x && x.trim() !== '');
    return false;
  }

  hasActiveFilters() {
    return this.columns.some(c => this._isActive(c.key));
  }

  _updateClearBtn() {
    if (!this.clearBtnId) return;
    const btn = document.getElementById(this.clearBtnId);
    if (!btn) return;
    btn.style.display = this.hasActiveFilters() ? '' : 'none';
  }

  // ── Apply filters ─────────────────────────────────────────────────
  _applyFilters(rows) {
    return rows.filter(row => {
      // External filter (tabs, search box) first
      if (!this.externalFilter(row)) return false;

      for (const col of this.columns) {
        const v = this._state[col.key];
        if (!v) continue;

        const cellVal = row[col.key];

        switch (col.type) {
          case 'text': {
            if (!v || !v.trim()) break;
            const needle = v.toLowerCase().trim();
            const hay    = (cellVal || '').toString().toLowerCase();
            if (!hay.includes(needle)) return false;
            break;
          }
          case 'select': {
            if (!v || !v.trim()) break;
            // Exact match for selects (dropdowns have known discrete values)
            const selected = v.toLowerCase().trim();
            const actual   = (cellVal ?? '').toString().toLowerCase();
            if (actual !== selected) return false;
            break;
          }
          case 'amount': {
            if (!v || typeof v !== 'object') break;
            const n = parseFloat(cellVal) || 0;
            if (v.min && parseFloat(v.min) > n) return false;
            if (v.max && parseFloat(v.max) < n) return false;
            break;
          }
          case 'date': {
            if (!v || typeof v !== 'object') break;
            if (!cellVal) return false;
            const d = new Date(cellVal);
            if (v.from && new Date(v.from) > d) return false;
            if (v.to   && new Date(v.to)   < d) return false;
            break;
          }
          case 'hours': {
            if (!v || !v.trim()) break;
            const h = cellVal;  // numeric hours or null
            if (v === 'null')   { if (h !== null && h !== undefined) return false; break; }
            if (h === null || h === undefined) return false;
            if (v === 'lt48'   && h >= 48)  return false;
            if (v === '48to72' && (h < 48 || h > 72)) return false;
            if (v === 'gt72'   && h <= 72)  return false;
            break;
          }
        }
      }
      return true;
    });
  }

  // ── Render ────────────────────────────────────────────────────────
  render() {
    const tbody = document.getElementById(this.tbodyId);
    if (!tbody) return;

    const all      = this.getRows();
    const filtered = this._applyFilters(all);

    if (filtered.length === 0) {
      tbody.innerHTML = `<tr><td colspan="${this.colspan}"
        style="padding:40px;text-align:center;font-family:'Cambay';color:var(--text-muted)">
        ${this.emptyMsg}</td></tr>`;
    } else {
      tbody.innerHTML = filtered.map(this.renderRow).join('');
    }

    this.onUpdate(filtered.length, all.length);
  }

  // ── Public API ────────────────────────────────────────────────────
  refresh() {
    this.populateSelects();
    this.render();
    this._updateClearBtn();
  }

  clearAll() {
    this._state = {};
    this._filterRow?.querySelectorAll('input').forEach(inp => { inp.value = ''; });
    this._filterRow?.querySelectorAll('.tf-dd').forEach(dd => {
      dd.querySelector('.tf-dd-label').textContent = 'All';
      dd.querySelector('.tf-dd-menu')?.querySelectorAll('.tf-dd-option').forEach((o,i) => {
        o.classList.toggle('tf-dd-selected', i===0);
      });
      dd.querySelector('.tf-dd-btn')?.classList.remove('tf-dd-active');
    });
    // Remove all header highlights
    const headerRow = this.tableEl.querySelector('thead tr:first-child');
    headerRow?.querySelectorAll('th').forEach(th => th.classList.remove('tf-col-active'));
    this.render();
    this._updateClearBtn();
  }
}
