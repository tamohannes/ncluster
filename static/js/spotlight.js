// ── Spotlight search (Cmd+P) ─────────────────────────────────────────────────

let _spotlightTimer = null;
let _spotlightIdx = -1;
let _spotlightItems = [];

const _quickActions = [
  { icon: '⚡', title: 'Go to Live',      sub: 'View active jobs',         action: () => showTab('live') },
  { icon: '⏱',  title: 'Go to History',   sub: 'View job history',         action: () => showTab('history') },
  { icon: '📓', title: 'Go to Logbook',   sub: 'View project notes',       action: () => showTab('logbook') },
  { icon: '🖥', title: 'Go to Clusters',  sub: 'Availability and advisor', action: () => showTab('clusters') },
  { icon: '⚙',  title: 'Settings',        sub: 'Open settings panel',      action: () => openSettingsModal() },
  { icon: '◧',  title: 'Toggle Sidebar',  sub: '⌘S',                       action: () => toggleSidebar() },
];

function openSpotlight() {
  const ov = document.getElementById('spotlight-overlay');
  if (!ov) return;
  ov.classList.add('open');
  const input = document.getElementById('spotlight-input');
  if (input) { input.value = ''; input.focus(); }
  _spotlightIdx = -1;
  _spotlightItems = [];
  _renderQuickActions();
}

function closeSpotlight() {
  const ov = document.getElementById('spotlight-overlay');
  if (ov) ov.classList.remove('open');
  _spotlightIdx = -1;
  _spotlightItems = [];
}

function _renderQuickActions() {
  _spotlightItems = _quickActions.map((a, i) => ({
    ...a, idx: i,
  }));
  _spotlightIdx = 0;
  _renderSpotlightResults([{ label: 'Quick Actions', items: _spotlightItems }]);
}

function onSpotlightInput() {
  clearTimeout(_spotlightTimer);
  const q = (document.getElementById('spotlight-input') || {}).value || '';
  if (!q.trim()) {
    _renderQuickActions();
    return;
  }
  _spotlightTimer = setTimeout(() => _fetchSpotlight(q.trim()), 180);
}

async function _fetchSpotlight(q) {
  try {
    const res = await fetch(`/api/spotlight?q=${encodeURIComponent(q)}`);
    const data = await res.json();
    const groups = [];
    let idx = 0;

    if (data.projects && data.projects.length) {
      const items = data.projects.map(p => ({
        icon: p.emoji || '📁',
        title: p.project,
        sub: `${p.job_count} jobs`,
        hint: 'project',
        idx: idx++,
        action: () => { closeSpotlight(); openProject(p.project); },
      }));
      groups.push({ label: 'Projects', items });
    }

    if (data.logbook && data.logbook.length) {
      const items = data.logbook.map(e => ({
        icon: e.entry_type === 'plan' ? '📋' : '📝',
        title: e.title,
        sub: `${e.project} · ${e.entry_type}`,
        hint: 'logbook',
        idx: idx++,
        action: () => {
          closeSpotlight();
          if (typeof _lbProject !== 'undefined') _lbProject = e.project;
          showTab('logbook');
          setTimeout(() => {
            const sel = document.getElementById('lb-project-select');
            if (sel) { sel.value = e.project; onLogbookProjectChange(); }
            setTimeout(() => openLogbookEntry(e.id), 200);
          }, 100);
        },
      }));
      groups.push({ label: 'Logbook', items });
    }

    if (data.history && data.history.length) {
      const items = data.history.map(r => {
        const stateIcon = { RUNNING: '🟢', PENDING: '🟡', COMPLETED: '✅', FAILED: '🔴', CANCELLED: '⚪' };
        return {
          icon: stateIcon[r.state] || '⚪',
          title: r.job_name || r.job_id,
          sub: `${r.cluster} · ${r.state}`,
          hint: r.job_id,
          idx: idx++,
          action: () => {
            closeSpotlight();
            openLog(r.cluster, r.job_id, r.job_name || '');
          },
        };
      });
      groups.push({ label: 'Runs', items });
    }

    const ql = q.toLowerCase();
    const actionMatches = _quickActions.filter(a =>
      a.title.toLowerCase().includes(ql) || a.sub.toLowerCase().includes(ql)
    );
    if (actionMatches.length) {
      const items = actionMatches.map(a => ({ ...a, hint: 'action', idx: idx++ }));
      groups.push({ label: 'Actions', items });
    }

    _spotlightItems = groups.flatMap(g => g.items);
    _spotlightIdx = _spotlightItems.length ? 0 : -1;
    _renderSpotlightResults(groups);
  } catch (_) {
    const el = document.getElementById('spotlight-results');
    if (el) el.innerHTML = '<div class="spotlight-empty">Search failed</div>';
  }
}

function _renderSpotlightResults(groups) {
  const el = document.getElementById('spotlight-results');
  if (!el) return;
  if (!groups.length || !groups.some(g => g.items.length)) {
    el.innerHTML = '<div class="spotlight-empty">No results</div>';
    return;
  }
  el.innerHTML = groups.map(g => {
    const header = `<div class="spotlight-group-label">${g.label}</div>`;
    const items = g.items.map(item => {
      const cls = item.idx === _spotlightIdx ? ' active' : '';
      return `<div class="spotlight-item${cls}" data-idx="${item.idx}" onclick="_spotlightSelect(${item.idx})" onmouseenter="_spotlightHover(${item.idx})">
        <span class="spotlight-item-icon">${item.icon}</span>
        <div class="spotlight-item-text">
          <div class="spotlight-item-title">${_escHtml(item.title)}</div>
          ${item.sub ? `<div class="spotlight-item-sub">${_escHtml(item.sub)}</div>` : ''}
        </div>
        ${item.hint ? `<span class="spotlight-item-hint">${_escHtml(item.hint)}</span>` : ''}
      </div>`;
    }).join('');
    return header + items;
  }).join('');
}

function _spotlightHover(idx) {
  _spotlightIdx = idx;
  document.querySelectorAll('.spotlight-item').forEach(el => {
    el.classList.toggle('active', parseInt(el.dataset.idx) === idx);
  });
}

function _spotlightSelect(idx) {
  const item = _spotlightItems.find(i => i.idx === idx);
  if (item && item.action) item.action();
}

function onSpotlightKey(e) {
  if (e.key === 'Escape') {
    e.preventDefault();
    closeSpotlight();
    return;
  }
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    if (!_spotlightItems.length) return;
    _spotlightIdx = (_spotlightIdx + 1) % _spotlightItems.length;
    _highlightSpotlight();
    return;
  }
  if (e.key === 'ArrowUp') {
    e.preventDefault();
    if (!_spotlightItems.length) return;
    _spotlightIdx = (_spotlightIdx - 1 + _spotlightItems.length) % _spotlightItems.length;
    _highlightSpotlight();
    return;
  }
  if (e.key === 'Enter') {
    e.preventDefault();
    if (_spotlightIdx >= 0) _spotlightSelect(_spotlightIdx);
    return;
  }
}

function _highlightSpotlight() {
  const items = document.querySelectorAll('.spotlight-item');
  items.forEach(el => {
    const idx = parseInt(el.dataset.idx);
    el.classList.toggle('active', idx === _spotlightIdx);
    if (idx === _spotlightIdx) el.scrollIntoView({ block: 'nearest' });
  });
}

function _escHtml(s) {
  return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
