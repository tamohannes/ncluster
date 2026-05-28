// ── Logbook v2 — full page ──

let _lbProject = '';
let _lbEditingId = null;
let _lbCampaignFilter = '';
let _lbInitReady = false;
let _lbPendingEntryId = null;
let _lbHistory = [];
let _lbRunNames = [];
let _lbSuggestTarget = null;
let _lbSuggestStart = -1;
let _lbCurrentEntry = null;
let _lbPresentMode = false;
const LB_SIDEBAR_WIDTH_KEY = 'clausius.lbSidebarWidth';
const LB_SIDEBAR_MIN = 200;
const LB_SIDEBAR_MAX = 600;
let _pinnedEntryIds = new Set();

function _cloneLbHistory(history) {
  try {
    return JSON.parse(JSON.stringify(history || []));
  } catch (_) {
    return [];
  }
}

function _captureLogbookTabState() {
  const main = document.getElementById('lb-main');
  const list = document.getElementById('lb-sidebar-list');
  const campaigns = document.getElementById('lb-campaign-filters');
  const select = document.getElementById('lb-project-select');
  const entryId = (_lbCurrentEntry && _lbCurrentEntry.id) || null;
  const entryTitle = (_lbCurrentEntry && _lbCurrentEntry.title) || '';
  return {
    lbProject: _lbProject || null,
    lbEntryId: entryId,
    lbEntryTitle: entryTitle,
    lbState: {
      project: _lbProject || '',
      entryId,
      entryTitle,
      currentEntry: _lbCurrentEntry || null,
      editingId: _lbEditingId || null,
      campaignFilter: _lbCampaignFilter || '',
      history: _cloneLbHistory(_lbHistory),
      mainHtml: main ? main.innerHTML : '',
      mainPlan: main ? main.classList.contains('lb-main-plan') : false,
      mainScrollTop: main ? main.scrollTop : 0,
      sidebarHtml: list ? list.innerHTML : '',
      sidebarScrollTop: list ? list.scrollTop : 0,
      campaignHtml: campaigns ? campaigns.innerHTML : '',
      selectValue: select ? select.value : '',
    },
  };
}

function _restoreLogbookTabState(state, tab) {
  const s = state || (tab && tab.lbState) || null;
  if (!s) return false;

  _lbProject = s.project || (tab && tab.lbProject) || _lbProject || '';
  _lbCurrentEntry = s.currentEntry || null;
  _lbEditingId = s.editingId || null;
  _lbCampaignFilter = s.campaignFilter || '';
  _lbHistory = _cloneLbHistory(s.history);

  const select = document.getElementById('lb-project-select');
  if (select && _lbProject) select.value = _lbProject;

  const main = document.getElementById('lb-main');
  if (main && s.mainHtml) {
    main.innerHTML = s.mainHtml;
    main.classList.toggle('lb-main-plan', !!s.mainPlan);
    main.scrollTop = s.mainScrollTop || 0;
  }

  const list = document.getElementById('lb-sidebar-list');
  if (list && s.sidebarHtml) {
    list.innerHTML = s.sidebarHtml;
    list.scrollTop = s.sidebarScrollTop || 0;
  }

  const campaigns = document.getElementById('lb-campaign-filters');
  if (campaigns && s.campaignHtml) campaigns.innerHTML = s.campaignHtml;

  if (s.entryId) _highlightSidebarItem(s.entryId);
  if (typeof _resolveEntryRefs === 'function') _resolveEntryRefs();
  _syncLogbookPresentMode();
  return !!(s.mainHtml || s.sidebarHtml || s.campaignHtml);
}

function _restoreLogbookSidebarState() {
  try {
    const saved = parseInt(localStorage.getItem(LB_SIDEBAR_WIDTH_KEY) || '', 10);
    if (!isNaN(saved) && saved >= LB_SIDEBAR_MIN && saved <= LB_SIDEBAR_MAX) {
      const sidebar = document.querySelector('.lb-sidebar');
      if (sidebar) sidebar.style.width = saved + 'px';
    }
  } catch (_) {}
}

async function togglePinEntry(entryId, project) {
  const wasPinned = _pinnedEntryIds.has(entryId);
  const newPinned = !wasPinned;
  try {
    await fetch(`/api/logbook/${encodeURIComponent(project || _lbProject)}/entries/${entryId}/pin`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pinned: newPinned }),
    });
    if (newPinned) _pinnedEntryIds.add(entryId);
    else _pinnedEntryIds.delete(entryId);
  } catch (_) {}
  if (_lbProject) _loadEntries(_lbProject);
}

function _isEntryPinned(entryId) {
  return _pinnedEntryIds.has(entryId);
}

(function setupLbSplitter() {
  let dragging = false;
  document.addEventListener('mousedown', e => {
    if (e.target.id === 'lb-splitter') { dragging = true; e.preventDefault(); e.target.classList.add('active'); }
  });
  document.addEventListener('mousemove', e => {
    if (!dragging) return;
    const page = document.querySelector('.lb-page');
    const sidebar = document.querySelector('.lb-sidebar');
    if (!page || !sidebar) return;
    const pageRect = page.getBoundingClientRect();
    let w = e.clientX - pageRect.left;
    w = Math.max(LB_SIDEBAR_MIN, Math.min(LB_SIDEBAR_MAX, w));
    sidebar.style.width = w + 'px';
    try { localStorage.setItem(LB_SIDEBAR_WIDTH_KEY, String(w)); } catch (_) {}
  });
  document.addEventListener('mouseup', () => {
    if (dragging) {
      dragging = false;
      const sp = document.getElementById('lb-splitter');
      if (sp) sp.classList.remove('active');
    }
  });
  _restoreLogbookSidebarState();
})();

// ── Page init ───────────────────────────────────────────────────────────────

function initLogbookPage() {
  const sel = document.getElementById('lb-project-select');
  if (!sel) return;
  _lbInitReady = false;
  _fetchProjectColors();

  Promise.allSettled([
    fetchWithTimeout('/api/projects').then(r => r.json()),
    fetchWithTimeout('/api/logbook_projects').then(r => r.json()),
  ]).then((results) => {
    let jobProjects = results[0].status === 'fulfilled' ? results[0].value : [];
    let lbProjects = results[1].status === 'fulfilled' ? results[1].value : [];
    if (!Array.isArray(jobProjects)) jobProjects = [];
    if (!Array.isArray(lbProjects)) lbProjects = [];

    const jobNames = new Set(jobProjects.map(p => p.project));
    const extraLb = lbProjects.filter(name => !jobNames.has(name));

    let html = '';
    if (jobProjects.length) {
      html += jobProjects.map(p =>
        `<option value="${p.project}">${p.emoji || ''} ${p.project}</option>`
      ).join('');
    }
    if (extraLb.length) {
      html += '<option disabled>──────────</option>';
      html += extraLb.map(name =>
        `<option value="${name}">📒 ${name}</option>`
      ).join('');
    }
    if (!html) html = '<option value="">no projects</option>';
    sel.innerHTML = html;

    const allNames = [...jobProjects.map(p => p.project), ...extraLb];
    if (allNames.length) {
      if (_lbProject && allNames.includes(_lbProject)) {
        sel.value = _lbProject;
      } else {
        _lbProject = allNames[0];
      }
      _loadEntries(_lbProject);
      _loadRunNames(_lbProject);
      _loadCampaignChips();
      if (typeof _updateActiveTabExtra === 'function') {
        _updateActiveTabExtra({ lbProject: _lbProject });
      }
      if (typeof _renderAppTabs === 'function') _renderAppTabs();
    }

    _lbInitReady = true;
    if (_lbPendingEntryId) {
      const eid = _lbPendingEntryId;
      _lbPendingEntryId = null;
      openLogbookEntry(eid);
    }
  }).catch(() => {
    sel.innerHTML = '<option value="">failed to load</option>';
    _lbInitReady = true;
  });
}

function openProjectLogbook() {
  if (_projCurrentName) _lbProject = _projCurrentName;
  showTab('logbook');
}

function onLogbookProjectChange() {
  const sel = document.getElementById('lb-project-select');
  _lbProject = sel.value;
  _lbEditingId = null;
  _lbCampaignFilter = '';
  _lbHistory = [];
  _showMainEmpty();
  _loadEntries(_lbProject);
  _loadRunNames(_lbProject);
  _loadCampaignChips();
  if (typeof _updateActiveTabExtra === 'function') {
    _updateActiveTabExtra({ lbProject: _lbProject, lbEntryId: null, lbEntryTitle: null });
  }
  if (typeof _renderAppTabs === 'function') _renderAppTabs();
  if (typeof _setHash === 'function') _setHash(_hashForView('logbook', { lbProject: _lbProject }));
}


// ── Entry list ──────────────────────────────────────────────────────────────

async function _loadEntries(project, query) {
  const el = document.getElementById('lb-sidebar-list');
  if (!el) return;
  const params = new URLSearchParams({ limit: '200' });
  if (query) params.set('q', query);
  if (_lbCampaignFilter) params.set('campaign', _lbCampaignFilter);
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(project)}/entries?${params}`);
    const entries = await res.json();
    _renderSidebarList(entries);
  } catch (e) {
    el.innerHTML = '<div class="lb-sidebar-empty" style="color:var(--red)">Failed to load</div>';
  }
}

function _renderSidebarList(entries) {
  const el = document.getElementById('lb-sidebar-list');
  if (!el) return;
  if (!entries.length) {
    el.innerHTML = '<div class="lb-sidebar-empty">No entries yet.</div>';
    return;
  }

  _pinnedEntryIds = new Set(entries.filter(e => e.pinned).map(e => e.id));
  const cf = (_lbCampaignFilter || '').trim().toLowerCase();
  let heroEntry = null;
  let heroLabel = '';
  if (cf) {
    heroEntry = entries.find(
      e => e.entry_type === 'mind_map'
        && String(e.campaign || '').trim().toLowerCase() === cf
    ) || null;
    if (heroEntry) {
      heroLabel = 'Mind map';
    } else {
      heroEntry = entries.find(
        e => e.entry_type === 'campaign_board'
          && String(e.campaign || '').trim().toLowerCase() === cf
      ) || null;
      if (heroEntry) heroLabel = 'Campaign board';
    }
  }
  const rest = heroEntry ? entries.filter(e => e.id !== heroEntry.id) : entries;

  const _campaignRoot = (e) => {
    if (e.entry_type === 'mind_map') return 0;
    if (e.entry_type === 'campaign_board') return 1;
    return 2;
  };
  const pinned = rest.filter(e => e.pinned);
  const unpinned = rest.filter(e => !e.pinned);
  const unpinnedRanked = [...unpinned].sort((a, b) => {
    const ra = _campaignRoot(a);
    const rb = _campaignRoot(b);
    if (ra !== rb) return ra - rb;
    return String(b.edited_at || '').localeCompare(String(a.edited_at || ''));
  });
  const sorted = [...pinned, ...unpinnedRanked];

  let html = '';
  if (heroEntry) {
    html += '<div class="lb-sidebar-hero-board">';
    html += `<div class="lb-sidebar-hero-board-label">${heroLabel}</div>`;
    html += _renderSidebarItems([heroEntry], false, { heroBoard: true });
    html += '</div>';
  }
  if (pinned.length && unpinned.length) {
    html += _renderSidebarItems(pinned, true, {});
    html += '<div class="lb-sidebar-pin-sep"></div>';
    html += _renderSidebarItems(unpinnedRanked, false, {});
  } else if (sorted.length) {
    html += _renderSidebarItems(sorted, false, {});
  }
  el.innerHTML = html;
  if (typeof _appTabs !== 'undefined' && typeof _activeTabId !== 'undefined') {
    const at = _appTabs.find(t => t.id === _activeTabId);
    if (at && at.lbEntryId) _highlightSidebarItem(at.lbEntryId);
  }
}

function _renderSnippet(snippet) {
  if (!snippet) return '';
  let s = _cleanSidebarPreview(snippet).replace(/</g, '&lt;');
  s = s.replace(/\x02/g, '<mark class="lb-search-hl">').replace(/\x03/g, '</mark>');
  return s;
}

function _cleanSidebarPreview(text) {
  return String(text || '')
    .replace(/\r?\n/g, ' ')
    .replace(/(^|\s)(\x02)?#{1,6}\s+/g, '$1$2')
    .replace(/(^|\s)(\x02)?(?:[-*+]\s+|>\s+)/g, '$1$2')
    .replace(/\s+/g, ' ')
    .trim();
}

function _renderSidebarItems(items, showPinIcon, opts) {
  const o = opts || {};
  const hero = !!o.heroBoard;
  return items.map(e => {
    const date = _formatDate(e.created_at);
    const title = (e.title || '').replace(/</g, '&lt;');
    const rawPreview = e.snippet || e.body_preview || '';
    const preview = e.snippet ? _renderSnippet(rawPreview) : _cleanSidebarPreview(rawPreview).replace(/</g, '&lt;');
    const isPlan = e.entry_type === 'plan';
    const isBoard = e.entry_type === 'campaign_board';
    const isMindMap = e.entry_type === 'mind_map';
    const heroCls = hero ? ' lb-sidebar-item--hero-board' : '';
    const typeCls = hero
      ? ''
      : (isMindMap ? ' lb-type-mind-map'
         : (isBoard ? ' lb-type-board'
            : (isPlan ? ' lb-type-plan' : '')));
    const boardGlyph = isMindMap
      ? '<span class="lb-sidebar-board-glyph lb-mind-map-glyph" title="Mind map">◈</span>'
      : (isBoard ? '<span class="lb-sidebar-board-glyph" title="Campaign board">▦</span>' : '');
    const pinned = _isEntryPinned(e.id);
    const pinCls = pinned ? ' lb-pinned' : '';
    const pinBtn = `<span class="lb-pin-btn${pinned ? ' active' : ''}" onclick="event.stopPropagation();togglePinEntry(${e.id})" title="${pinned ? 'Unpin' : 'Pin'}">📌</span>`;
    const camp = e.campaign || '';
    const campChip = camp && !_lbCampaignFilter
      ? `<span class="lb-sidebar-item-campaign">${camp}</span>`
      : '';
    return `<div class="lb-sidebar-item${typeCls}${heroCls}${pinCls}" data-id="${e.id}" data-campaign="${camp}" data-entry-meta="#${e.id} · ${date}" onclick="openLogbookEntry(${e.id})">
      <div class="lb-sidebar-item-title">${boardGlyph}<span class="lb-sidebar-item-name">${title}</span>${pinBtn}</div>
      ${campChip}
      <div class="lb-sidebar-item-preview">${preview}</div>
    </div>`;
  }).join('');
}

function _highlightSidebarItem(id) {
  document.querySelectorAll('.lb-sidebar-item').forEach(el => {
    el.classList.toggle('active', el.dataset.id === String(id));
  });
}

function _presentModeButtonLabel() {
  return _lbPresentMode ? 'exit present' : 'present mode';
}

function _syncLogbookPresentMode() {
  const root = document.getElementById('logbook-view');
  if (root) root.classList.toggle('lb-present-mode', _lbPresentMode);
  document.querySelectorAll('.lb-present-toggle').forEach(btn => {
    btn.textContent = _presentModeButtonLabel();
    btn.classList.toggle('active', _lbPresentMode);
  });
}

async function toggleLogbookPresentMode(force) {
  const root = document.getElementById('logbook-view');
  if (!root) return;

  const next = typeof force === 'boolean' ? force : !_lbPresentMode;
  _lbPresentMode = next;
  _syncLogbookPresentMode();

  if (_lbPresentMode) {
    if (typeof root.requestFullscreen === 'function' && document.fullscreenElement !== root) {
      try { await root.requestFullscreen(); } catch (_) {}
    }
    return;
  }

  if (document.fullscreenElement === root && typeof document.exitFullscreen === 'function') {
    try { await document.exitFullscreen(); } catch (_) {}
  }
}

document.addEventListener('fullscreenchange', () => {
  const root = document.getElementById('logbook-view');
  if (!_lbPresentMode || !root) return;
  if (document.fullscreenElement !== root) {
    _lbPresentMode = false;
    _syncLogbookPresentMode();
  }
});


// ── Search & filter ─────────────────────────────────────────────────────────

function onLogbookSearch() {}

async function _loadCampaignChips() {
  const el = document.getElementById('lb-campaign-filters');
  if (!el || !_lbProject) { if (el) el.innerHTML = ''; return; }
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/campaigns`);
    const data = await res.json();
    let html = `<div class="lb-campaign-picker-row">`
      + `<select id="lb-campaign-select" class="lb-campaign-select" onchange="_onCampaignSelectChange(this)">`
      + `<option value="">All campaigns</option>`;
    if (Array.isArray(data) && data.length) {
      for (const c of data) {
        const name = String(c.name || '');
        const val = encodeURIComponent(name);
        const labelEsc = name.replace(/&/g, '&amp;').replace(/</g, '&lt;');
        const sel = _lbCampaignFilter && String(name).trim().toLowerCase() === _lbCampaignFilter ? ' selected' : '';
        const cnt = typeof c.count === 'number' ? c.count : '';
        html += `<option value="${val}"${sel}>${labelEsc}${cnt !== '' ? ` (${cnt})` : ''}</option>`;
      }
    }
    html += `</select></div>`;
    el.innerHTML = html;
  } catch { el.innerHTML = ''; }
}

function _onCampaignSelectChange(sel) {
  if (!sel) return;
  const raw = sel.value || '';
  let decoded = '';
  if (raw) {
    try {
      decoded = decodeURIComponent(raw);
    } catch {
      decoded = raw;
    }
  }
  _lbCampaignFilter = decoded.trim().toLowerCase();
  if (_lbProject) _loadEntries(_lbProject);
}


// ── Main pane — entry detail ────────────────────────────────────────────────

function _showMainEmpty() {
  if (_lbPresentMode) void toggleLogbookPresentMode(false);
  const el = document.getElementById('lb-main');
  if (!el) return;
  el.classList.remove('lb-main-plan', 'lb-main-board');
  el.innerHTML = '<div class="lb-main-empty">Select an entry or create a new one.</div>';
  const topbarEl = document.getElementById('lb-topbar');
  if (topbarEl) {
    topbarEl.innerHTML = `
      <div class="lb-topbar-crumb">
        <span class="lb-topbar-crumb-project">${_lbProject || ''}</span>
      </div>`;
  }
}

function _pushLbHistory(state) {
  const top = _lbHistory[_lbHistory.length - 1];
  if (top && top.type === state.type
      && top.entryId === state.entryId
      && top.project === state.project
      && top.anchor === state.anchor) return;
  _lbHistory.push(state);
}

function _lbGoBack() {
  if (_lbHistory.length <= 1) {
    _lbHistory = [];
    _showMainEmpty();
    return;
  }

  _lbHistory.pop();
  const prev = _lbHistory[_lbHistory.length - 1];

  if (prev.project && prev.project !== _lbProject) {
    _lbProject = prev.project;
    const sel = document.getElementById('lb-project-select');
    if (sel) sel.value = prev.project;
    _loadEntries(prev.project);
    _loadRunNames(prev.project);
  }

  if (prev.entryId) {
    openLogbookEntry(prev.entryId, { pushHistory: false, anchor: prev.anchor });
  } else {
    _showMainEmpty();
  }
}

async function openLogbookEntry(entryId, opts = {}) {
  if (!_lbProject) return;
  const el = document.getElementById('lb-main');
  if (!el) return;
  if (opts.pushHistory !== false) _pushLbHistory({ type: 'entry', entryId, project: _lbProject, anchor: opts.anchor || null });
  _highlightSidebarItem(entryId);
  if (typeof _updateActiveTabExtra === 'function') {
    _updateActiveTabExtra({ lbProject: _lbProject, lbEntryId: entryId });
  }
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/entries/${entryId}`);
    const entry = await res.json();
    if (entry.status === 'error') { toast(entry.error, 'error'); return; }
    _lbCurrentEntry = entry;
    if (typeof _updateActiveTabExtra === 'function') {
      _updateActiveTabExtra({ lbEntryTitle: entry.title || '' });
    }
    if (typeof _renderAppTabs === 'function') _renderAppTabs();
    if (typeof _setHash === 'function') {
      _setHash(_hashForView('logbook', { lbProject: _lbProject, lbEntryId: entry.id }));
    }
    const title = (entry.title || '').replace(/</g, '&lt;');
    const bodyHtml = _renderLogbookMarkdown(entry.body || '');
    const created = _formatDate(entry.created_at);
    const edited = _formatDate(entry.edited_at);
    const isPlan = entry.entry_type === 'plan';
    const isBoard = entry.entry_type === 'campaign_board';
    const isMindMap = entry.entry_type === 'mind_map';
    el.classList.toggle('lb-main-plan', isPlan && !isBoard && !isMindMap);
    el.classList.toggle('lb-main-board', isBoard);
    el.classList.toggle('lb-main-mind-map', isMindMap);
    let typeBadge = '<span class="lb-badge-note">note</span>';
    if (isPlan) typeBadge = '<span class="lb-badge-plan">plan</span>';
    if (isBoard) typeBadge = '<span class="lb-badge-board">campaign board</span>';
    if (isMindMap) typeBadge = '<span class="lb-badge-mind-map">mind map</span>';
    const campChip = entry.campaign ? `<span class="lb-detail-campaign-chip">${String(entry.campaign).replace(/</g, '&lt;')}</span>` : '';
    const goalTxt = ((entry.campaign_goal || '') + '').trim();
    const goalHolder = isBoard || isMindMap;
    const goalBlock = goalHolder && goalTxt
      ? `<div class="lb-board-goal"><h2 class="lb-board-setup-heading">Campaign goal</h2><div class="lb-board-goal-body">${goalTxt.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</div></div>`
      : '';
    let mainContent;
    if (isMindMap) {
      mainContent = `<div class="lb-mind-map-shell">
          <div class="lb-board-ribbon lb-mind-map-ribbon">
            <span class="lb-board-ribbon-label">Mind map</span>
            ${campChip}
          </div>
          ${goalBlock}
          <div class="lb-mind-map-canvas" id="lb-mind-map-canvas"></div>
          ${bodyHtml ? `<div class="lb-board-setup">
            <h2 class="lb-board-setup-heading">Notes &amp; setup</h2>
            <div class="lb-board-setup-body lb-detail-body">${bodyHtml}</div>
          </div>` : ''}
        </div>`;
    } else if (isBoard) {
      mainContent = `<div class="lb-board-shell">
          <div class="lb-board-ribbon">
            <span class="lb-board-ribbon-label">Campaign board</span>
            ${campChip}
          </div>
          ${goalBlock}
          <div class="lb-board-setup">
            <h2 class="lb-board-setup-heading">Setup &amp; conventions</h2>
            <div class="lb-board-setup-body lb-detail-body">${bodyHtml}</div>
          </div>
          <div class="lb-board-grids">${_renderBoardJsonHtml(entry.board_json, false)}</div>
        </div>`;
    } else {
      mainContent = `<div class="lb-detail-body">${bodyHtml}</div>`;
    }
    const topbarEl = document.getElementById('lb-topbar');
    if (topbarEl) {
      const titleSafe = String(entry.title || 'Untitled').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
      topbarEl.innerHTML = `
        <div class="lb-topbar-crumb">
          <span class="lb-topbar-crumb-project">${_lbProject || ''}</span>
          ${entry.campaign ? `<span class="lb-topbar-crumb-sep">/</span><span class="lb-topbar-crumb-campaign">${String(entry.campaign).replace(/</g, '&lt;')}</span>` : ''}
          <span class="lb-topbar-crumb-sep">/</span>
          <span class="lb-topbar-crumb-page">${titleSafe}</span>
        </div>
        <div class="lb-topbar-actions">
          <button class="lb-topbar-btn lb-present-toggle${_lbPresentMode ? ' active' : ''}" onclick="toggleLogbookPresentMode()" title="Open this entry in present mode">${_presentModeButtonLabel()}</button>
          <button class="lb-topbar-btn" onclick="exportEntryDocx()" title="Export as Word document (.docx)">docx</button>
          <button class="lb-topbar-btn" onclick="exportEntryHtml()" title="Export as HTML — then ⌘S to save">html</button>
          <button class="lb-topbar-btn" onclick="editLogbookEntry(${entry.id})" title="Edit entry">edit</button>
          <span class="lb-info-wrapper">
            <button class="lb-topbar-btn lb-info-btn" type="button" tabindex="0" title="Entry info">info</button>
            <div class="lb-info-popup" role="tooltip">
              <div class="lb-info-row"><span class="lb-info-label">Created</span><span class="lb-info-val">${created}</span></div>
              ${entry.created_at !== entry.edited_at ? `<div class="lb-info-row"><span class="lb-info-label">Edited</span><span class="lb-info-val">${edited}</span></div>` : ''}
              <div class="lb-info-row"><span class="lb-info-label">Type</span><span class="lb-info-val">${typeBadge}</span></div>
              ${campChip ? `<div class="lb-info-row"><span class="lb-info-label">Campaign</span><span class="lb-info-val">${campChip}</span></div>` : ''}
            </div>
          </span>
          <button class="lb-topbar-btn lb-topbar-btn-danger" onclick="deleteLogbookEntry(${entry.id})" title="Delete entry">delete</button>
        </div>`;
    }
    el.innerHTML = `
      <div class="lb-detail">
        <button class="lb-present-close" onclick="toggleLogbookPresentMode(false)" title="Exit present mode" aria-label="Exit present mode">×</button>
        <h1 class="lb-detail-title">${title} <span class="lb-detail-id">#${entry.id}</span></h1>
        ${mainContent}
      </div>`;
    if (isMindMap && typeof renderMindMap === 'function') {
      const canvas = document.getElementById('lb-mind-map-canvas');
      if (canvas) renderMindMap(canvas, entry);
    }
    _resolveEntryRefs();
    const scrollContainer = document.getElementById('lb-main');
    if (opts.anchor) {
      requestAnimationFrame(() => {
        const target = document.getElementById(opts.anchor);
        if (target) {
          target.scrollIntoView({ behavior: 'smooth', block: 'center' });
          target.classList.add('lb-anchor-highlight');
          setTimeout(() => target.classList.remove('lb-anchor-highlight'), 2500);
        } else if (scrollContainer) {
          scrollContainer.scrollTop = 0;
        }
      });
    } else if (scrollContainer) {
      scrollContainer.scrollTop = 0;
    }
  } catch (e) {
    toast('Failed to load entry', 'error');
  }
}


// ── Image & HTML lightbox ────────────────────────────────────────────────────

function _dismissLightbox(overlay) {
  overlay.classList.remove('visible');
  const iframe = overlay.querySelector('iframe');
  if (iframe && iframe._htmlEmbedResizeObserver) {
    iframe._htmlEmbedResizeObserver.disconnect();
    iframe._htmlEmbedResizeObserver = null;
  }
  setTimeout(() => overlay.remove(), 200);
}

document.addEventListener('click', e => {
  if (e.target.closest('.lb-html-embed-zoom')) return;
  if (e.target.closest('.lb-html-embed-preview, .lb-html-embed-fallback')) return;
  const img = e.target.closest('.lb-detail-body img, .logbook-entry-content img');
  if (!img) return;
  e.stopPropagation();
  const overlay = document.createElement('div');
  overlay.className = 'lb-lightbox';
  const big = document.createElement('img');
  big.src = img.src;
  overlay.appendChild(big);
  document.body.appendChild(overlay);
  requestAnimationFrame(() => overlay.classList.add('visible'));
  overlay.addEventListener('click', () => _dismissLightbox(overlay));
  document.addEventListener('keydown', function esc(ev) {
    if (ev.key === 'Escape') { _dismissLightbox(overlay); document.removeEventListener('keydown', esc); }
  });
});

function openHtmlLightbox(src) {
  const overlay = document.createElement('div');
  overlay.className = 'lb-lightbox lb-lightbox-html';
  const close = document.createElement('button');
  close.className = 'lb-lightbox-close';
  close.textContent = '✕';
  close.onclick = () => _dismissLightbox(overlay);
  const iframe = document.createElement('iframe');
  iframe.src = src;
  iframe.sandbox = 'allow-scripts allow-same-origin';
  iframe.onload = () => _fitHtmlEmbed(iframe);
  overlay.appendChild(close);
  overlay.appendChild(iframe);
  document.body.appendChild(overlay);
  requestAnimationFrame(() => overlay.classList.add('visible'));
  document.addEventListener('keydown', function esc(ev) {
    if (ev.key === 'Escape') { _dismissLightbox(overlay); document.removeEventListener('keydown', esc); }
  });
}

function openTableLightbox(btn) {
  const wrap = btn && btn.closest ? btn.closest('.md-table-wrap, .lb-board-table-scroll') : null;
  if (!wrap) return;
  const table = wrap.querySelector('table');
  if (!table) return;

  const overlay = document.createElement('div');
  overlay.className = 'lb-lightbox lb-table-lightbox';
  const close = document.createElement('button');
  close.className = 'lb-lightbox-close';
  close.textContent = '✕';
  close.onclick = () => _dismissLightbox(overlay);

  const shell = document.createElement('div');
  shell.className = 'lb-table-lightbox-shell';
  const clone = table.cloneNode(true);
  clone.removeAttribute('id');
  shell.appendChild(clone);
  overlay.appendChild(close);
  overlay.appendChild(shell);
  document.body.appendChild(overlay);
  requestAnimationFrame(() => overlay.classList.add('visible'));
  overlay.addEventListener('click', ev => {
    if (ev.target === overlay) _dismissLightbox(overlay);
  });
  document.addEventListener('keydown', function esc(ev) {
    if (ev.key === 'Escape') { _dismissLightbox(overlay); document.removeEventListener('keydown', esc); }
  });
}

function toggleExpandedTable(btn) {
  openTableLightbox(btn);
}


// ── Export ───────────────────────────────────────────────────────────────────

function _exportShortcutLabel() {
  const s = typeof getShortcut === 'function' ? getShortcut('exportEntry') : null;
  if (!s) return '<kbd>⌘</kbd><kbd>⇧</kbd><kbd>S</kbd>';
  const parts = [];
  if (s.meta) parts.push(navigator.platform.includes('Mac') ? '⌘' : 'Ctrl');
  if (s.shift) parts.push('⇧');
  let k = s.key;
  if (k.length === 1) k = k.toUpperCase();
  parts.push(k);
  return parts.map(p => `<kbd>${p}</kbd>`).join('');
}

function _exportHintText() {
  const s = typeof getShortcut === 'function' ? getShortcut('exportEntry') : null;
  if (!s) return '<b>⌘⇧S</b>';
  const parts = [];
  if (s.meta) parts.push(navigator.platform.includes('Mac') ? '⌘' : 'Ctrl+');
  if (s.shift) parts.push('⇧');
  let k = s.key;
  if (k.length === 1) k = k.toUpperCase();
  parts.push(k);
  return `<b>${parts.join('')}</b>`;
}

function _exportSaveKeyCond() {
  const s = typeof getShortcut === 'function' ? getShortcut('exportEntry') : null;
  const key = s ? s.key.toLowerCase() : 's';
  const meta = s ? !!s.meta : true;
  const shift = s ? !!s.shift : true;
  const parts = [`e.key.toLowerCase()==='${key}'`];
  if (meta) parts.push('(e.metaKey||e.ctrlKey)');
  if (shift) parts.push('e.shiftKey');
  else parts.push('!e.shiftKey');
  return parts.join('&&');
}

function _exportSlug(title) {
  return (title || 'entry').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').substring(0, 40);
}

async function exportEntryHtml() {
  if (!_lbCurrentEntry) { toast('No entry loaded', 'error'); return; }
  const e = _lbCurrentEntry;
  toast('Preparing HTML export…');

  let bodyHtml = _renderLogbookMarkdown(e.body || '');

  const imgRegex = /src="(\/api\/logbook\/[^"]+)"/g;
  const urls = new Set();
  let m;
  while ((m = imgRegex.exec(bodyHtml)) !== null) {
    if (!/\.html?$/i.test(m[1])) urls.add(m[1]);
  }

  const dataUris = {};
  for (const url of urls) {
    try {
      const resp = await fetch(url);
      const blob = await resp.blob();
      const reader = new FileReader();
      const dataUri = await new Promise((resolve) => {
        reader.onloadend = () => resolve(reader.result);
        reader.readAsDataURL(blob);
      });
      dataUris[url] = dataUri;
    } catch (_) {}
  }
  for (const [url, dataUri] of Object.entries(dataUris)) {
    bodyHtml = bodyHtml.split(url).join(dataUri);
  }

  bodyHtml = bodyHtml.replace(/\s*onload="[^"]*"/g, '');
  bodyHtml = bodyHtml.replace(/\s*sandbox="[^"]*"/g, '');
  bodyHtml = bodyHtml.replace(/<button class="lb-html-embed-zoom"[^>]*>[^<]*<\/button>/g, '');
  bodyHtml = bodyHtml.replace(/<button class="md-table-expand"[^>]*>[^<]*<\/button>/g, '');

  const origin = window.location.origin;
  bodyHtml = bodyHtml.replace(
    /<iframe\s+src="(\/api\/logbook\/[^"]+\.html?)"([^>]*)><\/iframe>/gi,
    `<iframe src="${origin}$1" onerror="this.classList.add('embed-failed')"$2></iframe><div class="embed-offline">Interactive figure — open in clausius to view</div>`
  );

  bodyHtml = bodyHtml.replace(/<table\b/g, '<div class="table-wrap"><table')
                     .replace(/<\/table>/g, '</table></div>');

  const created = _formatDate(e.created_at);
  const edited = e.created_at !== e.edited_at ? ` · Edited ${_formatDate(e.edited_at)}` : '';
  let typeBadge = '';
  if (e.entry_type === 'plan') typeBadge = '<span class="badge badge-plan">plan</span>';
  else if (e.entry_type === 'campaign_board') typeBadge = '<span class="badge badge-board">campaign board</span>';

  const exportGoal = (e.entry_type === 'campaign_board')
    ? (((e.campaign_goal || '') + '').trim())
    : '';
  const exportGoalHtml = exportGoal
    ? `<h2>Campaign goal</h2><div class="export-goal">${exportGoal.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</div>`
    : '';
  const mainExport = e.entry_type === 'campaign_board'
    ? `${exportGoalHtml}<h2>Setup &amp; conventions</h2>${bodyHtml}<h2>Structured tables</h2>${_renderBoardJsonHtml(e.board_json, true)}`
    : bodyHtml;

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${(e.title || '').replace(/</g, '&lt;')}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #ffffff; --surface: #f5f5f7; --border: #e4e4ec;
  --text: #18181f; --muted: #9090aa; --accent: #2BA298;
  --code-bg: #f5f5f7;
}
[data-theme="dark"] {
  --bg: #1a1a2e; --surface: #22223a; --border: #33335a;
  --text: #e4e4f0; --muted: #8888aa; --accent: #5ddebe;
  --code-bg: #2a2a44;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: 'Inter', sans-serif; font-size: 14px; line-height: 1.7;
  color: var(--text); background: var(--bg);
  -webkit-font-smoothing: antialiased;
}
.container { max-width: 720px; margin: 0 auto; padding: 48px 24px 80px; }
.meta { font-size: 12px; color: var(--muted); margin-bottom: 32px; }
.badge { font-size: 10px; font-weight: 600; padding: 2px 8px; border-radius: 4px; margin-left: 8px; }
.badge-plan { background: #fef3c7; color: #b45309; }
[data-theme="dark"] .badge-plan { background: #3d2e00; color: #fbbf24; }
.badge-board { background: #e6f5f3; color: #1a7a72; }
[data-theme="dark"] .badge-board { background: #0d3d38; color: #5ddebe; }
h1 { font-size: 28px; font-weight: 700; line-height: 1.3; margin-bottom: 8px; letter-spacing: -0.02em; }
h2 { font-size: 20px; font-weight: 700; margin: 36px 0 12px; padding-bottom: 6px; border-bottom: 1px solid var(--border); }
.export-goal { white-space: pre-wrap; font-size: 14px; line-height: 1.65; margin: 0 0 8px; color: var(--text); }
h3 { font-size: 16px; font-weight: 600; margin: 24px 0 8px; }
p { margin: 10px 0; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
strong { font-weight: 600; }
ul, ol { margin: 10px 0; padding-left: 24px; }
li { margin: 4px 0; }
blockquote {
  border-left: 3px solid var(--accent); padding: 2px 0 2px 16px;
  margin: 12px 0; color: var(--muted); font-style: italic;
}
pre {
  background: var(--code-bg); padding: 14px 18px; border-radius: 8px;
  font-family: 'JetBrains Mono', monospace; font-size: 12px;
  overflow-x: auto; margin: 14px 0; line-height: 1.6;
}
code {
  font-family: 'JetBrains Mono', monospace; font-size: 12px;
  background: var(--code-bg); padding: 2px 5px; border-radius: 4px;
}
pre code { background: none; padding: 0; }
.md-small-caps {
  font-family: 'Inter', sans-serif;
  font-variant-caps: small-caps;
  letter-spacing: 0.045em;
  font-weight: 650;
}
.lb-html-embed {
  margin: 16px 0; border-radius: 8px; overflow: hidden;
  border: 1px solid var(--border); background: #fff;
}
.lb-html-embed iframe {
  width: 100%; height: 600px; border: none; display: block;
}
.lb-html-embed-caption {
  font-size: 12px; color: var(--muted); padding: 6px 12px;
  border-top: 1px solid var(--border); text-align: center;
}
.embed-offline {
  display: none; padding: 32px 16px; text-align: center;
  color: var(--muted); font-size: 13px; font-style: italic;
}
iframe.embed-failed + .embed-offline { display: block; }
iframe.embed-failed { display: none; }
.table-wrap {
  overflow-x: auto; margin: 16px 0; border-radius: 8px;
  border: 1px solid var(--border);
}
table {
  width: 100%; border-collapse: collapse;
  font-size: 13px; white-space: nowrap;
}
th, td { padding: 8px 12px; border: 1px solid var(--border); text-align: left; }
th { background: var(--surface); font-weight: 600; }
tr.md-row-colored td { background: var(--md-row-bg) !important; color: var(--md-row-fg, var(--text)); border-left-color: transparent; border-right-color: transparent; }
tr.md-row-colored td:first-child { border-left: 2px solid color-mix(in srgb, var(--md-row-bg) 100%, transparent); }
td.md-cell-colored, th.md-cell-colored { background: var(--md-cell-bg) !important; color: var(--md-cell-fg, var(--text)); }
tr.md-row-border-thick td, tr.md-row-border-thick th { border-top-width: 2px; border-bottom-width: 2px; }
td.md-cell-border-thick, th.md-cell-border-thick { border-width: 2px; }
.md-table-row-gap td { height: 8px; padding: 0 !important; border: none !important; background: transparent !important; }
figure { margin: 20px 0; text-align: center; }
figure img { max-width: 100%; border-radius: 8px; }
figcaption {
  font-size: 12px; color: var(--muted); margin-top: 8px;
  line-height: 1.5; text-align: left;
}
figcaption strong { color: var(--text); font-weight: 600; }
.save-hint {
  position: fixed; top: 16px; left: 50%; transform: translateX(-50%);
  font-family: 'Inter', sans-serif; font-size: 12px; color: var(--muted);
  background: var(--surface); border: 1px solid var(--border);
  padding: 6px 16px; border-radius: 20px; z-index: 100;
  box-shadow: 0 2px 8px rgba(0,0,0,0.08); opacity: 1;
  transition: opacity 0.5s; cursor: pointer;
}
.save-hint:hover { color: var(--text); border-color: var(--accent); }
.theme-toggle {
  position: fixed; top: 16px; right: 16px; z-index: 100;
  width: 36px; height: 36px; border-radius: 50%;
  border: 1px solid var(--border); background: var(--surface);
  color: var(--text); font-size: 16px; cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  transition: background 0.2s, border-color 0.2s;
  box-shadow: 0 2px 8px rgba(0,0,0,0.08);
}
.theme-toggle:hover { border-color: var(--accent); }
@media print { .theme-toggle, .save-hint { display: none; } }
</style>
</head>
<body>
<div class="save-hint" id="save-hint">${_exportHintText()} to save</div>
<button class="theme-toggle" onclick="toggleTheme()" title="Toggle theme" id="theme-btn">☀️</button>
<div class="container">
  <h1>${(e.title || '').replace(/</g, '&lt;')} ${typeBadge}</h1>
  <div class="meta">${_lbProject} · #${e.id} · ${created}${edited}</div>
  ${mainExport}
</div>
<script>
(function(){
  var pref = window.matchMedia('(prefers-color-scheme:dark)').matches ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', pref);
  updateIcon();
  setTimeout(function(){ var h = document.getElementById('save-hint'); if(h) h.style.opacity='0'; }, 4000);
  setTimeout(function(){ var h = document.getElementById('save-hint'); if(h) h.remove(); }, 4500);
  setTimeout(function(){
    document.querySelectorAll('iframe[src*="/api/logbook/"]').forEach(function(f){
      try { if(!f.contentDocument || !f.contentDocument.body || !f.contentDocument.body.innerHTML) f.classList.add('embed-failed'); }
      catch(e){ f.classList.add('embed-failed'); }
    });
  }, 3000);
})();
function toggleTheme() {
  var t = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', t);
  updateIcon();
}
function updateIcon() {
  var btn = document.getElementById('theme-btn');
  if (btn) btn.textContent = document.documentElement.getAttribute('data-theme') === 'dark' ? '☀️' : '🌙';
}

</script>
</body>
</html>`;

  const pageBlob = new Blob([html], { type: 'text/html' });
  const pageUrl = URL.createObjectURL(pageBlob);
  const win = window.open(pageUrl, '_blank');
  if (win) {
    toast('Opened HTML export in new tab');
  } else {
    toast('Popup blocked — allow popups for this site', 'error');
  }
}


function exportEntryDocx() {
  if (!_lbCurrentEntry) { toast('No entry loaded', 'error'); return; }
  const e = _lbCurrentEntry;
  window.location.href = `/api/logbook/${encodeURIComponent(_lbProject)}/entries/${e.id}/export/docx`;
  toast('Downloading Word document…');
}

// ── Editor ──────────────────────────────────────────────────────────────────

function _lbSyncEditorTypeRow() {
  const sel = document.getElementById('lb-edit-type');
  const row = document.getElementById('lb-edit-board-row');
  const shell = document.getElementById('lb-editor-board-shell');
  const graphRow = document.getElementById('lb-edit-graph-row');
  const graphShell = document.getElementById('lb-editor-graph-shell');
  if (!sel) return;
  const isBoard = sel.value === 'campaign_board';
  const isMindMap = sel.value === 'mind_map';
  if (row) row.style.display = isBoard ? 'block' : 'none';
  if (shell) shell.style.display = isBoard ? 'block' : 'none';
  if (graphRow) graphRow.style.display = isMindMap ? 'block' : 'none';
  if (graphShell) graphShell.style.display = isMindMap ? 'block' : 'none';
}

function _lbFormatGraphJson() {
  const ta = document.getElementById('lb-edit-graph-json');
  if (!ta) return;
  try {
    const o = JSON.parse(ta.value.trim() || '{}');
    ta.value = JSON.stringify(o, null, 2);
    toast('Formatted JSON', 'ok');
  } catch (e) {
    toast('Invalid JSON — fix before formatting', 'error');
  }
}

function _lbFormatBoardJson() {
  const ta = document.getElementById('lb-edit-board-json');
  if (!ta) return;
  try {
    const o = JSON.parse(ta.value.trim() || '{}');
    ta.value = JSON.stringify(o, null, 2);
    toast('Formatted JSON', 'ok');
  } catch (e) {
    toast('Invalid JSON — fix before formatting', 'error');
  }
}

function showLogbookEditor(entryId, title, body, entryType, campaign, boardJson, campaignGoal, graphJson) {
  if (_lbPresentMode) void toggleLogbookPresentMode(false);
  _lbEditingId = entryId || null;
  const el = document.getElementById('lb-main');
  if (!el) return;
  el.classList.remove('lb-main-plan', 'lb-main-board', 'lb-main-mind-map');
  const titleVal = (title || '').replace(/"/g, '&quot;');
  const bodyVal = (body || '').replace(/</g, '&lt;');
  const typeVal = entryType || 'note';
  const campVal = (campaign || '').replace(/"/g, '&quot;');
  let bj = boardJson;
  if (bj == null || bj === '') bj = '{"version":1,"sections":[]}';
  const bjStr = typeof bj === 'string' ? bj : JSON.stringify(bj, null, 2);
  let gj = graphJson;
  if (gj == null || gj === '') gj = '{"version":1,"nodes":[],"edges":[]}';
  const gjStr = typeof gj === 'string' ? gj : JSON.stringify(gj, null, 2);
  const showBoardOption = typeVal === 'campaign_board';
  const boardOptionHtml = showBoardOption
    ? `<option value="campaign_board" selected>Campaign board (legacy)</option>`
    : '';
  el.innerHTML = `
    <div class="lb-editor">
      <div class="lb-editor-type-row">
        <select id="lb-edit-type" class="lb-editor-type-select" onchange="_lbSyncEditorTypeRow()">
          <option value="note" ${typeVal === 'note' ? 'selected' : ''}>Note</option>
          <option value="plan" ${typeVal === 'plan' ? 'selected' : ''}>Plan</option>
          <option value="mind_map" ${typeVal === 'mind_map' ? 'selected' : ''}>Mind map</option>
          ${boardOptionHtml}
        </select>
        <input type="text" class="lb-editor-campaign" id="lb-edit-campaign" list="lb-campaign-suggest" placeholder="campaign (required for mind map / board)" value="${campVal}">
        <datalist id="lb-campaign-suggest"></datalist>
      </div>
      <div id="lb-editor-board-shell" class="lb-editor-board-shell" style="display:${typeVal === 'campaign_board' ? 'block' : 'none'}">
        <div class="lb-editor-board-banner">Campaign board — legacy static surface. New campaign work belongs in mind maps; existing board JSON supports plain table sections only: <code>version</code>, <code>sections</code>, <code>columns</code>, <code>rows</code>, and string <code>cells</code>.</div>
        <label class="lb-editor-board-label" for="lb-edit-campaign-goal">Campaign goal</label>
        <textarea class="lb-editor-campaign-goal" id="lb-edit-campaign-goal" rows="4" placeholder="What this campaign is trying to achieve (plain text, a few sentences)…"></textarea>
      </div>
      <div id="lb-editor-graph-shell" class="lb-editor-board-shell lb-editor-graph-shell" style="display:${typeVal === 'mind_map' ? 'block' : 'none'}">
        <div class="lb-editor-board-banner">Mind map — single source of truth for this campaign. Add tasks, bugs, failures, and successful runs as <strong>nodes</strong>; encode "what happens next" as <strong>edges</strong>. Status enum: <code>planned</code> / <code>active</code> / <code>blocked</code> / <code>done</code> / <code>failed</code> / <code>abandoned</code>. Edge kind enum: <code>default</code> / <code>success</code> / <code>failure</code> / <code>branch</code>. <strong>Prefer editing via the MCP <code>patch_mind_map</code> tool</strong> after discussing with the user — the JSON editor here is a fallback.</div>
        <label class="lb-editor-board-label" for="lb-edit-campaign-goal-mm">Campaign goal</label>
        <textarea class="lb-editor-campaign-goal" id="lb-edit-campaign-goal-mm" rows="4" placeholder="What this campaign is trying to achieve (plain text, a few sentences)…"></textarea>
      </div>
      <input type="text" class="lb-editor-title" id="lb-edit-title" placeholder="Entry title" value="${titleVal}">
      <textarea class="lb-editor-body" id="lb-edit-body" placeholder="Setup &amp; conventions (markdown)…&#10;&#10;Use @run-name to reference jobs.&#10;Drag/drop or paste images to attach." rows="14">${bodyVal}</textarea>
      <div id="lb-edit-board-row" style="display:${typeVal === 'campaign_board' ? 'block' : 'none'}">
        <label class="lb-editor-board-label" for="lb-edit-board-json">board_json</label>
        <textarea class="lb-editor-board-json" id="lb-edit-board-json" rows="14" spellcheck="false"></textarea>
        <div class="lb-editor-board-json-actions">
          <button type="button" class="btn" onclick="_lbFormatBoardJson()">format JSON</button>
        </div>
      </div>
      <div id="lb-edit-graph-row" style="display:${typeVal === 'mind_map' ? 'block' : 'none'}">
        <label class="lb-editor-board-label" for="lb-edit-graph-json">graph_json</label>
        <textarea class="lb-editor-board-json" id="lb-edit-graph-json" rows="18" spellcheck="false"></textarea>
        <div class="lb-editor-board-json-actions">
          <button type="button" class="btn" onclick="_lbFormatGraphJson()">format JSON</button>
        </div>
      </div>
      <div class="lb-editor-hint">drag &amp; drop or paste images into the body field</div>
      <div class="lb-editor-actions">
        <button class="btn" onclick="saveLogbookEntry()">save</button>
        <button class="btn" onclick="_onEditorCancel()">cancel</button>
      </div>
    </div>`;
  const bjEl = document.getElementById('lb-edit-board-json');
  if (bjEl) bjEl.value = bjStr;
  const gjEl = document.getElementById('lb-edit-graph-json');
  if (gjEl) gjEl.value = gjStr;
  const cgVal = (campaignGoal != null && campaignGoal !== '') ? String(campaignGoal) : '';
  const cgEl = document.getElementById('lb-edit-campaign-goal');
  if (cgEl) cgEl.value = cgVal;
  const cgEl2 = document.getElementById('lb-edit-campaign-goal-mm');
  if (cgEl2) cgEl2.value = cgVal;
  _setupImageHandlers();
  _loadCampaignSuggestions();
  _lbSyncEditorTypeRow();
}

async function _loadCampaignSuggestions() {
  if (!_lbProject) return;
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/campaigns`);
    const data = await res.json();
    const dl = document.getElementById('lb-campaign-suggest');
    if (dl && Array.isArray(data)) {
      dl.innerHTML = data.map(c => `<option value="${c.name}">`).join('');
    }
  } catch {}
}

function _setupImageHandlers() {
  const ta = document.getElementById('lb-edit-body');
  if (!ta) return;

  ta.addEventListener('drop', e => {
    const files = e.dataTransfer && e.dataTransfer.files;
    if (files && files.length) {
      const imageFiles = Array.from(files).filter(f => f.type.startsWith('image/'));
      if (imageFiles.length) {
        e.preventDefault();
        imageFiles.forEach(f => _uploadAndInsertImage(ta, f));
      }
    }
  });

  ta.addEventListener('paste', e => {
    const items = e.clipboardData && e.clipboardData.items;
    if (!items) return;
    for (const item of items) {
      if (item.type.startsWith('image/')) {
        e.preventDefault();
        _uploadAndInsertImage(ta, item.getAsFile());
        return;
      }
    }
  });

  ta.addEventListener('dragover', e => {
    if (e.dataTransfer && Array.from(e.dataTransfer.types).includes('Files')) {
      e.preventDefault();
      ta.classList.add('lb-drag-over');
    }
  });
  ta.addEventListener('dragleave', () => ta.classList.remove('lb-drag-over'));
  ta.addEventListener('drop', () => ta.classList.remove('lb-drag-over'));
}

async function _uploadAndInsertImage(textarea, file) {
  if (!_lbProject) return;
  const form = new FormData();
  form.append('file', file);
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/images`, {
      method: 'POST', body: form,
    });
    const d = await res.json();
    if (d.status === 'ok') {
      const md = `![${file.name}](${d.url})`;
      const pos = textarea.selectionStart;
      const before = textarea.value.substring(0, pos);
      const after = textarea.value.substring(pos);
      const insert = (before && !before.endsWith('\n') ? '\n' : '') + md + '\n';
      textarea.value = before + insert + after;
      textarea.selectionStart = textarea.selectionEnd = pos + insert.length;
      textarea.focus();
      toast(`Image uploaded: ${d.filename}`);
    } else {
      toast(d.error || 'Upload failed', 'error');
    }
  } catch (e) {
    toast('Failed to upload image', 'error');
  }
}

function _onEditorCancel() {
  if (_lbEditingId) {
    openLogbookEntry(_lbEditingId);
  } else {
    _showMainEmpty();
  }
  _lbEditingId = null;
}

async function editLogbookEntry(entryId) {
  if (!_lbProject) return;
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/entries/${entryId}`);
    const entry = await res.json();
    if (entry.status === 'error') { toast(entry.error, 'error'); return; }
    showLogbookEditor(entryId, entry.title, entry.body, entry.entry_type, entry.campaign, entry.board_json, entry.campaign_goal, entry.graph_json);
  } catch (e) {
    toast('Failed to load entry for editing', 'error');
  }
}

async function saveLogbookEntry() {
  const titleInput = document.getElementById('lb-edit-title');
  const bodyInput = document.getElementById('lb-edit-body');
  const typeSelect = document.getElementById('lb-edit-type');
  const campInput = document.getElementById('lb-edit-campaign');
  if (!titleInput || !bodyInput || !_lbProject) return;
  const title = titleInput.value.trim();
  const body = bodyInput.value.trim();
  const entry_type = typeSelect ? typeSelect.value : 'note';
  const campaign = campInput ? campInput.value.trim().toLowerCase() : '';
  if (!title) { toast('Title is required', 'error'); return; }
  if (entry_type === 'campaign_board' && !campaign) {
    toast('Campaign is required for a campaign board', 'error');
    return;
  }
  if (entry_type === 'mind_map' && !campaign) {
    toast('Campaign is required for a mind map', 'error');
    return;
  }

  const payload = { title, body, entry_type, campaign };
  if (entry_type === 'campaign_board') {
    const bjta = document.getElementById('lb-edit-board-json');
    const raw = bjta ? bjta.value.trim() : '';
    try {
      payload.board_json = raw ? JSON.parse(raw) : {};
    } catch (e) {
      toast('board_json is not valid JSON', 'error');
      return;
    }
    const cgta = document.getElementById('lb-edit-campaign-goal');
    payload.campaign_goal = cgta ? cgta.value.trim() : '';
  }
  if (entry_type === 'mind_map') {
    const gjta = document.getElementById('lb-edit-graph-json');
    const raw = gjta ? gjta.value.trim() : '';
    try {
      payload.graph_json = raw ? JSON.parse(raw) : {};
    } catch (e) {
      toast('graph_json is not valid JSON', 'error');
      return;
    }
    const cgta = document.getElementById('lb-edit-campaign-goal-mm');
    payload.campaign_goal = cgta ? cgta.value.trim() : '';
  }

  const t = toastLoading(_lbEditingId ? 'Saving entry…' : 'Creating entry…');
  try {
    let res;
    if (_lbEditingId) {
      res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/entries/${_lbEditingId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
    } else {
      res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/entries`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
    }
    const d = await res.json();
    if (d.status === 'ok') {
      t.done(_lbEditingId ? 'Entry updated' : 'Entry created');
      const openId = _lbEditingId || d.id;
      _lbEditingId = null;
      await _loadEntries(_lbProject);
      if (openId) openLogbookEntry(openId);
    } else {
      t.done(d.error || 'Failed', 'error');
    }
  } catch (e) {
    t.done('Failed to save entry', 'error');
  }
}

async function deleteLogbookEntry(entryId) {
  if (!_lbProject) return;
  if (!confirm('Delete this entry? This cannot be undone.')) return;
  const t = toastLoading('Deleting entry…');
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/entries/${entryId}`, { method: 'DELETE' });
    const d = await res.json();
    if (d.status === 'ok') {
      t.done('Entry deleted');
      _showMainEmpty();
      await _loadEntries(_lbProject);
    } else {
      t.done(d.error || 'Failed', 'error');
    }
  } catch (e) {
    t.done('Failed to delete entry', 'error');
  }
}


// ── Helpers ─────────────────────────────────────────────────────────────────

function _shortDate(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso.replace('T', ' '));
    const now = new Date();
    const diffMs = now - d;
    if (diffMs < 86400000 && d.getDate() === now.getDate())
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    if (diffMs < 604800000)
      return d.toLocaleDateString([], { weekday: 'short', hour: '2-digit', minute: '2-digit' });
    return d.toLocaleDateString([], { month: 'short', day: 'numeric', year: 'numeric' });
  } catch (_) { return iso; }
}

function _formatDate(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso.replace('T', ' '));
    return d.toLocaleDateString([], { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' });
  } catch (_) { return iso; }
}

function _lbEscapeAttr(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/'/g, '&#39;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function _renderBoardJsonHtml(boardJsonStr, forExport) {
  if (!boardJsonStr || !String(boardJsonStr).trim()) return '';
  let o;
  try {
    o = JSON.parse(String(boardJsonStr));
  } catch (_) {
    return '<p class="lb-board-json-err">Invalid board JSON</p>';
  }
  const sections = o && Array.isArray(o.sections) ? o.sections : [];
  if (!sections.length) {
    return '<p class="lb-board-empty-hint">No structured tables yet. Edit this board and add JSON sections.</p>';
  }
  return sections.map((sec, si) => {
    const secType = String(sec && sec.type != null ? sec.type : 'table').toLowerCase();
    if (secType !== 'table') {
      return '<p class="lb-board-empty-hint">This legacy table type is no longer supported.</p>';
    }
    const title = (sec && sec.title) ? String(sec.title).replace(/</g, '&lt;') : `Section ${si + 1}`;
    const cols = sec && Array.isArray(sec.columns) ? sec.columns : [];
    const rows = sec && Array.isArray(sec.rows) ? sec.rows : [];
    const head = cols.map(c => `<th>${String(c.label != null ? c.label : c.id || '').replace(/</g, '&lt;')}</th>`).join('');
    const body = rows.map((row) => {
      const cells = cols.map((c) => {
        const id = c.id;
        const v = row && row.cells && id in row.cells ? row.cells[id] : '';
        return `<td>${String(v).replace(/</g, '&lt;')}</td>`;
      }).join('');
      return `<tr>${cells}</tr>`;
    }).join('');
    return `<div class="lb-board-section"><h3 class="lb-board-section-title">${title}</h3>`
      + `<div class="lb-board-table-scroll md-table-wrap"><button class="md-table-expand" onclick="event.stopPropagation();toggleExpandedTable(this)" title="Open table fullscreen">full</button><table class="lb-board-table md-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div></div>`;
  }).join('');
}

// _renderLogbookMarkdown / _resolveEntryRefs / openLogByName / _openEntryRef
// live in static/js/refs.js — the universal reference renderer used by every
// rich-text surface in Clausius (logbook bodies, mind map popovers, ...).
// New code should call renderRichText / renderRefs / hydrateRefs / openRunRef /
// openEntryRef directly; the underscored aliases are kept for inline onclick
// strings that may still reference the legacy names.


// ── @ autocomplete ──────────────────────────────────────────────────────────

async function _loadRunNames(project) {
  try {
    const res = await fetch(`/api/history?project=${encodeURIComponent(project)}&limit=500`);
    const rows = await res.json();
    const names = new Set();
    for (const r of rows) { if (r.job_name) names.add(r.job_name); }
    _lbRunNames = Array.from(names).sort();
  } catch (_) { _lbRunNames = []; }
}

function _getSuggestBox() {
  let box = document.getElementById('lb-suggest-box');
  if (!box) {
    box = document.createElement('div');
    box.id = 'lb-suggest-box';
    box.className = 'lb-suggest-box';
    document.body.appendChild(box);
  }
  return box;
}

function _hideSuggest() {
  const box = document.getElementById('lb-suggest-box');
  if (box) box.style.display = 'none';
  _lbSuggestTarget = null;
  _lbSuggestStart = -1;
}

function _showSuggest(textarea, query) {
  const matches = _lbRunNames.filter(n => n.toLowerCase().includes(query.toLowerCase())).slice(0, 10);
  const box = _getSuggestBox();
  if (!matches.length) { box.style.display = 'none'; return; }
  box.innerHTML = matches.map((name, i) =>
    `<div class="lb-suggest-item${i === 0 ? ' active' : ''}" data-name="${name}">${name}</div>`
  ).join('');
  const rect = textarea.getBoundingClientRect();
  box.style.left = rect.left + 'px';
  box.style.top = (rect.bottom + 2) + 'px';
  box.style.width = Math.max(rect.width, 250) + 'px';
  box.style.display = 'block';
  box.querySelectorAll('.lb-suggest-item').forEach(item => {
    item.addEventListener('mousedown', e => { e.preventDefault(); _insertSuggestion(textarea, item.dataset.name); });
  });
}

function _insertSuggestion(textarea, name) {
  const before = textarea.value.substring(0, _lbSuggestStart);
  const after = textarea.value.substring(textarea.selectionStart);
  textarea.value = before + name + ' ' + after;
  const pos = before.length + name.length + 1;
  textarea.setSelectionRange(pos, pos);
  textarea.focus();
  _hideSuggest();
}

document.addEventListener('input', e => {
  const ta = e.target;
  if (ta.tagName !== 'TEXTAREA') return;
  if (!ta.closest('.lb-editor') && !ta.closest('.logbook-view')) return;
  const val = ta.value;
  const pos = ta.selectionStart;
  const textBefore = val.substring(0, pos);
  const atMatch = textBefore.match(/@([\w_-]*)$/);
  if (atMatch) {
    _lbSuggestTarget = ta;
    _lbSuggestStart = pos - atMatch[1].length;
    _showSuggest(ta, atMatch[1]);
  } else {
    _hideSuggest();
  }
});

document.addEventListener('keydown', e => {
  const box = document.getElementById('lb-suggest-box');
  if (!box || box.style.display === 'none') return;
  const items = box.querySelectorAll('.lb-suggest-item');
  const active = box.querySelector('.lb-suggest-item.active');
  let idx = Array.from(items).indexOf(active);
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    if (active) active.classList.remove('active');
    idx = (idx + 1) % items.length;
    items[idx].classList.add('active');
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    if (active) active.classList.remove('active');
    idx = (idx - 1 + items.length) % items.length;
    items[idx].classList.add('active');
  } else if (e.key === 'Enter' || e.key === 'Tab') {
    if (active && _lbSuggestTarget) { e.preventDefault(); _insertSuggestion(_lbSuggestTarget, active.dataset.name); }
  } else if (e.key === 'Escape') { _hideSuggest(); }
});

document.addEventListener('blur', e => {
  if (e.target.tagName === 'TEXTAREA') setTimeout(_hideSuggest, 150);
}, true);
