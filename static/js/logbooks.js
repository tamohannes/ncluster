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
  let heroBoard = null;
  if (cf) {
    heroBoard = entries.find(
      e => e.entry_type === 'campaign_board'
        && String(e.campaign || '').trim().toLowerCase() === cf
    ) || null;
  }
  const rest = heroBoard ? entries.filter(e => e.id !== heroBoard.id) : entries;

  const pinned = rest.filter(e => e.pinned);
  const unpinned = rest.filter(e => !e.pinned);
  const unpinnedRanked = [...unpinned].sort((a, b) => {
    const ra = a.entry_type === 'campaign_board' ? 0 : 1;
    const rb = b.entry_type === 'campaign_board' ? 0 : 1;
    if (ra !== rb) return ra - rb;
    return String(b.edited_at || '').localeCompare(String(a.edited_at || ''));
  });
  const sorted = [...pinned, ...unpinnedRanked];

  let html = '';
  if (heroBoard) {
    html += '<div class="lb-sidebar-hero-board">';
    html += '<div class="lb-sidebar-hero-board-label">Campaign board</div>';
    html += _renderSidebarItems([heroBoard], false, { heroBoard: true });
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
    const heroCls = hero ? ' lb-sidebar-item--hero-board' : '';
    const typeCls = hero ? '' : (isBoard ? ' lb-type-board' : (isPlan ? ' lb-type-plan' : ''));
    const boardGlyph = isBoard ? '<span class="lb-sidebar-board-glyph" title="Campaign board">▦</span>' : '';
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
    el.classList.toggle('lb-main-plan', isPlan && !isBoard);
    el.classList.toggle('lb-main-board', isBoard);
    let typeBadge = '<span class="lb-badge-note">note</span>';
    if (isPlan) typeBadge = '<span class="lb-badge-plan">plan</span>';
    if (isBoard) typeBadge = '<span class="lb-badge-board">campaign board</span>';
    const campChip = entry.campaign ? `<span class="lb-detail-campaign-chip">${String(entry.campaign).replace(/</g, '&lt;')}</span>` : '';
    const goalTxt = ((entry.campaign_goal || '') + '').trim();
    const goalBlock = isBoard && goalTxt
      ? `<div class="lb-board-goal"><h2 class="lb-board-setup-heading">Campaign goal</h2><div class="lb-board-goal-body">${goalTxt.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')}</div></div>`
      : '';
    const mainContent = isBoard
      ? `<div class="lb-board-shell">
          <div class="lb-board-ribbon">
            <span class="lb-board-ribbon-label">Campaign board</span>
            ${campChip}
          </div>
          ${goalBlock}
          <div class="lb-board-setup">
            <h2 class="lb-board-setup-heading">Setup &amp; conventions</h2>
            <div class="lb-board-setup-body lb-detail-body">${bodyHtml}</div>
          </div>
          <div class="lb-board-grids">${_renderBoardJsonHtml(entry.board_json, false, entry.board_runtime)}</div>
        </div>`
      : `<div class="lb-detail-body">${bodyHtml}</div>`;
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

function _layoutExpandedTable(wrap) {
  const main = wrap && wrap.closest ? wrap.closest('.lb-main') : null;
  if (!wrap || !main) return;

  wrap.style.width = '';
  wrap.style.marginLeft = '';

  const mainStyle = window.getComputedStyle(main);
  const padLeft = parseFloat(mainStyle.paddingLeft) || 0;
  const padRight = parseFloat(mainStyle.paddingRight) || 0;
  const mainRect = main.getBoundingClientRect();
  const wrapRect = wrap.getBoundingClientRect();
  const targetLeft = mainRect.left + padLeft;
  const targetWidth = Math.max(320, mainRect.width - padLeft - padRight);

  wrap.style.width = `${targetWidth}px`;
  wrap.style.marginLeft = `${targetLeft - wrapRect.left}px`;
}

function toggleExpandedTable(btn) {
  const wrap = btn && btn.closest ? btn.closest('.md-table-wrap') : null;
  if (!wrap) return;
  const expanded = !wrap.classList.contains('expanded');
  wrap.classList.toggle('expanded', expanded);
  if (expanded) {
    _layoutExpandedTable(wrap);
    btn.textContent = 'normal';
    btn.title = 'Return table to entry width';
  } else {
    wrap.style.width = '';
    wrap.style.marginLeft = '';
    btn.textContent = 'wide';
    btn.title = 'Fit table to page width';
  }
}

window.addEventListener('resize', () => {
  document.querySelectorAll('.md-table-wrap.expanded').forEach(_layoutExpandedTable);
});


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
    ? `${exportGoalHtml}<h2>Setup &amp; conventions</h2>${bodyHtml}<h2>Structured tables</h2>${_renderBoardJsonHtml(e.board_json, true, e.board_runtime)}`
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
  if (!sel || !row) return;
  const isBoard = sel.value === 'campaign_board';
  row.style.display = isBoard ? 'block' : 'none';
  if (shell) shell.style.display = isBoard ? 'block' : 'none';
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

/** Append a run_metric_grid section: columns declare default metric keys (scalar_latest); cells only need cluster + run_hash unless overriding. */
function _lbInsertSmartMetricGridTemplate() {
  const ta = document.getElementById('lb-edit-board-json');
  if (!ta) return;
  let o;
  try {
    o = JSON.parse(ta.value.trim() || '{}');
  } catch (e) {
    toast('Invalid JSON — fix before inserting a template', 'error');
    return;
  }
  if (typeof o !== 'object' || o === null || Array.isArray(o)) {
    toast('board_json must be a JSON object', 'error');
    return;
  }
  if (o.version == null) o.version = 1;
  if (!Array.isArray(o.sections)) o.sections = [];
  const n = o.sections.filter((s) => s && String(s.type || '').toLowerCase() === 'run_metric_grid').length;
  o.sections.push({
    type: 'run_metric_grid',
    title: `Metrics ${n + 1}`,
    columns: [
      { id: 'metric_a', label: 'Metric A', scalar: 'pass_at_1' },
      { id: 'metric_b', label: 'Metric B', scalar: 'accuracy' },
    ],
    rows: [{ id: 'exp1', label: 'Experiment 1' }],
    cells: {
      'exp1:metric_a': { cluster: 'local', run_hash: '00000000' },
      'exp1:metric_b': { cluster: 'local', run_hash: '00000000' },
    },
  });
  ta.value = JSON.stringify(o, null, 2);
  toast('Inserted run_metric_grid — edit scalar names, cluster, and run_hash for each cell', 'ok');
}

function showLogbookEditor(entryId, title, body, entryType, campaign, boardJson, campaignGoal) {
  if (_lbPresentMode) void toggleLogbookPresentMode(false);
  _lbEditingId = entryId || null;
  const el = document.getElementById('lb-main');
  if (!el) return;
  el.classList.remove('lb-main-plan', 'lb-main-board');
  const titleVal = (title || '').replace(/"/g, '&quot;');
  const bodyVal = (body || '').replace(/</g, '&lt;');
  const typeVal = entryType || 'note';
  const campVal = (campaign || '').replace(/"/g, '&quot;');
  let bj = boardJson;
  if (bj == null || bj === '') bj = '{"version":1,"sections":[]}';
  const bjStr = typeof bj === 'string' ? bj : JSON.stringify(bj, null, 2);
  el.innerHTML = `
    <div class="lb-editor">
      <div class="lb-editor-type-row">
        <select id="lb-edit-type" class="lb-editor-type-select" onchange="_lbSyncEditorTypeRow()">
          <option value="note" ${typeVal === 'note' ? 'selected' : ''}>Note</option>
          <option value="plan" ${typeVal === 'plan' ? 'selected' : ''}>Plan</option>
          <option value="campaign_board" ${typeVal === 'campaign_board' ? 'selected' : ''}>Campaign board</option>
        </select>
        <input type="text" class="lb-editor-campaign" id="lb-edit-campaign" list="lb-campaign-suggest" placeholder="campaign (required for board)" value="${campVal}">
        <datalist id="lb-campaign-suggest"></datalist>
      </div>
      <div id="lb-editor-board-shell" class="lb-editor-board-shell" style="display:${typeVal === 'campaign_board' ? 'block' : 'none'}">
        <div class="lb-editor-board-banner">Campaign board — canonical team surface. <strong>Table</strong> sections: each row needs <code>cluster</code> + <code>run_hash</code>; optional <code>"type":"run_status"</code> column (max one per section) shows live Slurm/SDK aggregate state. <strong>run_metric_grid</strong> (smart metric table): pick which SDK stats appear in each column — optional <code>scalar</code> on a column is the default <code>scalar_latest</code> metric name for that column; each <code>cells["row_id:col_id"]</code> entry sets <code>cluster</code> + <code>run_hash</code> and may override with its own <code>scalar</code>. Every row×column pair is required. On save/read, Clausius fills <code>board_runtime</code> with live status, numeric values, and <code>malfunctioned</code> flags (no external services). Use <strong>insert template</strong> below to scaffold a grid, then replace metric keys and run hashes.</div>
        <label class="lb-editor-board-label" for="lb-edit-campaign-goal">Campaign goal</label>
        <textarea class="lb-editor-campaign-goal" id="lb-edit-campaign-goal" rows="4" placeholder="What this campaign is trying to achieve (plain text, a few sentences)…"></textarea>
      </div>
      <input type="text" class="lb-editor-title" id="lb-edit-title" placeholder="Entry title" value="${titleVal}">
      <textarea class="lb-editor-body" id="lb-edit-body" placeholder="Setup &amp; conventions (markdown)…&#10;&#10;Use @run-name to reference jobs.&#10;Drag/drop or paste images to attach." rows="14">${bodyVal}</textarea>
      <div id="lb-edit-board-row" style="display:${typeVal === 'campaign_board' ? 'block' : 'none'}">
        <label class="lb-editor-board-label" for="lb-edit-board-json">board_json</label>
        <textarea class="lb-editor-board-json" id="lb-edit-board-json" rows="14" spellcheck="false"></textarea>
        <div class="lb-editor-board-json-actions">
          <button type="button" class="btn" onclick="_lbFormatBoardJson()">format JSON</button>
          <button type="button" class="btn" onclick="_lbInsertSmartMetricGridTemplate()" title="Append a run_metric_grid section with column-level scalar keys">insert smart table template</button>
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
  const cgEl = document.getElementById('lb-edit-campaign-goal');
  if (cgEl) cgEl.value = (campaignGoal != null && campaignGoal !== '') ? String(campaignGoal) : '';
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
    showLogbookEditor(entryId, entry.title, entry.body, entry.entry_type, entry.campaign, entry.board_json, entry.campaign_goal);
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

/** CSS class suffix for board_runtime status labels (matches server slugs). */
function _lbBoardStatusSlug(label) {
  const raw = String(label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
  return raw || 'unknown';
}

function _lbBoardStatusDisplay(label) {
  if (label == null || label === '' || label === '—') return '—';
  return String(label).replace(/_/g, ' ');
}

function _lbRunMetricRuntimeKey(sectionIndex, rowId, colId) {
  return `${sectionIndex}|${rowId}|${colId}`;
}

/** Map backend status to a small set of UI tones (colors). */
function _lbRunMetricTone(status, hasValue) {
  if (hasValue) return 'value';
  const s = String(status || '').toLowerCase();
  if (s === 'completed') return 'completed';
  if (s === 'running') return 'running';
  if (s === 'pending' || s === 'submitting') return 'pending';
  if (s === 'failed' || s === 'error' || s === 'cancelled' || s === 'timeout') return 'failed';
  if (s === 'mixed') return 'pending';
  if (s === 'not_found' || s === 'no_jobs') return 'missing';
  return 'unknown';
}

function _lbRunMetricStatusAbbrev(status) {
  const s = String(status || '').toLowerCase();
  const map = {
    completed: '✓',
    running: '●',
    pending: '◔',
    submitting: '◔',
    failed: '✕',
    cancelled: '✕',
    timeout: '✕',
    mixed: '◆',
    not_found: '—',
    no_jobs: '○',
    error: '!',
  };
  return map[s] || '·';
}

function _renderRunMetricGridSection(sec, sectionIndex, boardRuntime, forExport) {
  const title = (sec && sec.title) ? String(sec.title).replace(/</g, '&lt;') : `Section ${sectionIndex + 1}`;
  const cols = sec && Array.isArray(sec.columns) ? sec.columns : [];
  const rows = sec && Array.isArray(sec.rows) ? sec.rows : [];
  const cm = (boardRuntime && boardRuntime.cells) ? boardRuntime.cells : {};
  const head = cols.map((c) => {
    const lab = String(c.label != null ? c.label : c.id || '').replace(/</g, '&lt;');
    const sk = c.scalar ? String(c.scalar).replace(/</g, '&lt;') : '';
    const keySpan = sk ? ` <span class="lb-run-metric-col-key" title="scalar_latest key">${sk}</span>` : '';
    return `<th><span class="lb-run-metric-th-inner">${lab}${keySpan}</span></th>`;
  }).join('');
  const body = rows.map((row) => {
    const rid = row && row.id != null ? String(row.id) : '';
    const tds = cols.map((c) => {
      const cid = c && c.id != null ? String(c.id) : '';
      const rk = _lbRunMetricRuntimeKey(sectionIndex, rid, cid);
      const cell = cm[rk] || null;
      const st = cell && cell.status ? String(cell.status) : 'missing';
      const val = cell && cell.value != null && cell.value !== '' ? String(cell.value) : '';
      const hasVal = !!val;
      const cluster = cell && cell.cluster ? String(cell.cluster).trim() : '';
      const runHash = cell && cell.run_hash ? String(cell.run_hash).trim() : '';
      const scalar = cell && cell.scalar ? String(cell.scalar) : '';
      const malfunctioned = !!(cell && cell.malfunctioned);
      const tone = _lbRunMetricTone(st, hasVal);
      const tipParts = [
        cluster && runHash ? `${cluster}/${runHash}` : '',
        st,
        scalar ? `scalar:${scalar}` : '',
        malfunctioned ? 'malfunctioned' : '',
      ].filter(Boolean);
      const tip = _lbEscapeAttr(tipParts.join(' · '));
      if (forExport) {
        const cellTxt = hasVal ? val : _lbBoardStatusDisplay(st);
        return `<td>${cellTxt.replace(/</g, '&lt;')}</td>`;
      }
      const oc = _lbEscapeAttr(cluster);
      const oh = _lbEscapeAttr(runHash);
      const inner = hasVal
        ? `<span class="lb-run-metric-val">${val.replace(/</g, '&lt;')}</span>`
        : `<span class="lb-run-metric-glyph" aria-hidden="true">${_lbRunMetricStatusAbbrev(st)}</span>`;
      const open = cluster && runHash
        ? `onclick="event.stopPropagation();if(typeof openRunInfoByHash==='function')openRunInfoByHash('${oc}','${oh}','')"`
        : '';
      const mfCls = malfunctioned ? ' lb-run-metric-cell--malfunctioned' : '';
      return `<td class="lb-run-metric-td"><button type="button" class="lb-run-metric-cell lb-run-metric-cell--${tone}${mfCls}" title="${tip}" ${open}>${inner}</button></td>`;
    }).join('');
    const rlab = row && row.label != null ? String(row.label).replace(/</g, '&lt;') : rid;
    return `<tr><th scope="row" class="lb-run-metric-row-label">${rlab}</th>${tds}</tr>`;
  }).join('');
  return `<div class="lb-board-section lb-board-section--metric-grid"><h3 class="lb-board-section-title">${title}</h3>`
    + `<div class="lb-board-table-scroll"><table class="lb-board-table lb-run-metric-grid md-table"><thead><tr><th class="lb-run-metric-corner"></th>${head}</tr></thead><tbody>${body}</tbody></table></div></div>`;
}

function _renderBoardJsonHtml(boardJsonStr, forExport, boardRuntime) {
  if (!boardJsonStr || !String(boardJsonStr).trim()) return '';
  let o;
  try {
    o = JSON.parse(String(boardJsonStr));
  } catch (_) {
    return '<p class="lb-board-json-err">Invalid board JSON</p>';
  }
  const sm = (boardRuntime && boardRuntime.statuses) ? boardRuntime.statuses : {};
  const sections = o && Array.isArray(o.sections) ? o.sections : [];
  if (!sections.length) {
    return '<p class="lb-board-empty-hint">No structured tables yet. Edit this board and add JSON sections.</p>';
  }
  return sections.map((sec, si) => {
    const secType = String(sec && sec.type != null ? sec.type : 'table').toLowerCase();
    if (secType === 'run_metric_grid') {
      return _renderRunMetricGridSection(sec, si, boardRuntime, forExport);
    }
    const title = (sec && sec.title) ? String(sec.title).replace(/</g, '&lt;') : `Section ${si + 1}`;
    const cols = sec && Array.isArray(sec.columns) ? sec.columns : [];
    const rows = sec && Array.isArray(sec.rows) ? sec.rows : [];
    const head = cols.map(c => `<th>${String(c.label != null ? c.label : c.id || '').replace(/</g, '&lt;')}</th>`).join('')
      + '<th class="lb-board-run-col">Run</th>';
    const body = rows.map((row) => {
      const clusterLc = row && row.cluster ? String(row.cluster).trim().toLowerCase() : '';
      const runHashLc = row && row.run_hash ? String(row.run_hash).trim().toLowerCase() : '';
      const statusKey = clusterLc && runHashLc ? `${clusterLc}:${runHashLc}` : '';
      const cells = cols.map((c) => {
        const id = c.id;
        const colType = String(c.type || 'string').toLowerCase();
        if (colType === 'run_status') {
          const stRaw = statusKey && Object.prototype.hasOwnProperty.call(sm, statusKey) ? sm[statusKey] : null;
          const hasVal = stRaw != null && stRaw !== '';
          const display = hasVal ? _lbBoardStatusDisplay(stRaw) : '—';
          const safe = display.replace(/</g, '&lt;');
          if (forExport) {
            return `<td>${safe}</td>`;
          }
          const slug = hasVal ? _lbBoardStatusSlug(stRaw) : 'unknown';
          const titleAttr = _lbEscapeAttr(hasVal ? String(stRaw) : '');
          return `<td><span class="lb-board-status lb-board-status--${slug}" title="${titleAttr}">${safe}</span></td>`;
        }
        const v = row && row.cells && id in row.cells ? row.cells[id] : '';
        return `<td>${String(v).replace(/</g, '&lt;')}</td>`;
      }).join('');
      const cluster = row && row.cluster ? String(row.cluster).trim() : '';
      const runHash = row && row.run_hash ? String(row.run_hash).trim() : '';
      let runCell = '—';
      if (runHash && cluster) {
        if (forExport) {
          runCell = `${cluster.replace(/</g, '&lt;')}/${runHash.replace(/</g, '&lt;')}`;
        } else {
          const oc = _lbEscapeAttr(cluster);
          const oh = _lbEscapeAttr(runHash);
          runCell = `<button type="button" class="lb-board-run-chip" onclick="event.stopPropagation();if(typeof openRunInfoByHash==='function')openRunInfoByHash('${oc}','${oh}','')">${runHash.replace(/</g, '&lt;')}</button>`;
        }
      }
      return `<tr>${cells}<td class="lb-board-run-col">${runCell}</td></tr>`;
    }).join('');
    return `<div class="lb-board-section"><h3 class="lb-board-section-title">${title}</h3>`
      + `<div class="lb-board-table-scroll"><table class="lb-board-table md-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div></div>`;
  }).join('');
}

function _renderLogbookMarkdown(raw) {
  let html = markdownToHtml(raw);
  html = html.replace(/(?<!\w)@([\w_-]+)/g, (match, name) =>
    `<span class="run-ref" onclick="openLogByName('${name}')">${match}</span>`
  );
  const anchorRefs = [];
  html = html.replace(/(?<!\w)#(\d+):(fig|tbl)-(\d+)/g, (match, id, kind, num) => {
    const anchor = `${kind}-${num}`;
    const label = kind === 'fig' ? `Figure ${num}` : `Table ${num}`;
    const placeholder = `\x00ANCHOR${anchorRefs.length}\x00`;
    anchorRefs.push(`<span class="anchor-ref" onclick="openLogbookEntry(${id},{anchor:'${anchor}'})" title="${label} in entry #${id}">${match}</span>`);
    return placeholder;
  });
  html = html.replace(/(?<!\w)#(\d+)/g, (match, id) =>
    `<span class="entry-ref" data-entry-ref="${id}" onclick="_openEntryRef(${id},_lbProject)" title="Open entry #${id}">${match}</span>`
  );
  anchorRefs.forEach((span, i) => {
    html = html.replace(`\x00ANCHOR${i}\x00`, span);
  });
  return html;
}

function _resolveEntryRefs() {
  const refs = document.querySelectorAll('.entry-ref[data-entry-ref]');
  if (!refs.length) return;
  const ids = new Set();
  refs.forEach(el => ids.add(el.dataset.entryRef));
  fetch(`/api/logbook/resolve_refs?ids=${Array.from(ids).join(',')}`)
    .then(r => r.json())
    .then(entries => {
      for (const entry of entries) {
        document.querySelectorAll(`.entry-ref[data-entry-ref="${entry.id}"]`).forEach(el => {
          const escaped = (entry.title || '').replace(/</g, '&lt;').replace(/>/g, '&gt;');
          const crossProject = entry.project && entry.project !== _lbProject;
          const projectBadge = crossProject
            ? `<span class="entry-ref-project">${entry.project}</span>`
            : '';
          el.innerHTML = `<span class="entry-ref-id">#${entry.id}</span>${projectBadge}<span class="entry-ref-title">${escaped}</span>`;
          el.title = entry.title;
          el.dataset.entryProject = entry.project || '';
          el.classList.add('resolved');
          if (crossProject) el.classList.add('cross-project');
          el.setAttribute('onclick', `_openEntryRef(${entry.id},'${(entry.project || '').replace(/'/g, "\\'")}')`);
        });
      }
    })
    .catch(() => {});
}

async function _openEntryRef(entryId, project, anchor) {
  if (!project) {
    try {
      const res = await fetch(`/api/logbook/resolve_refs?ids=${entryId}`);
      const entries = await res.json();
      if (entries.length) project = entries[0].project;
    } catch (_) {}
  }
  if (project && project !== _lbProject) {
    _lbProject = project;
    const sel = document.getElementById('lb-project-select');
    if (sel) sel.value = project;
    _loadEntries(project);
    _loadRunNames(project);
  }
  openLogbookEntry(entryId, anchor ? { anchor } : {});
}

async function openLogByName(runName) {
  if (!_lbProject) return;
  try {
    const res = await fetch(`/api/history?project=${encodeURIComponent(_lbProject)}&limit=500`);
    const rows = await res.json();
    const match = rows.find(r => (r.job_name || '').includes(runName));
    if (match) {
      openLog(match.cluster, match.job_id, match.job_name);
    } else {
      toast(`No job found matching "${runName}"`, 'error');
    }
  } catch (_) {
    toast('Failed to search for run', 'error');
  }
}


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



