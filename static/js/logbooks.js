// ── Logbook v2 — full page ──

let _lbProject = '';
let _lbEditingId = null;
let _lbSearchTimer = null;
let _lbTypeFilter = '';
let _lbHistory = [];
let _lbRunNames = [];
let _lbSuggestTarget = null;
let _lbSuggestStart = -1;
let _lbCurrentEntry = null;
const LB_SIDEBAR_WIDTH_KEY = 'clausius.lbSidebarWidth';
const LB_SIDEBAR_MIN = 200;
const LB_SIDEBAR_MAX = 600;
const LB_MAP_VIEW_KEY = 'clausius.lbMapView';
let _pinnedEntryIds = new Set();

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
    }
  }).catch(() => {
    sel.innerHTML = '<option value="">failed to load</option>';
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
  _mapActive = false;
  _mapFocusEntryId = null;
  _mapNeighborHops = 1;
  _mapEdgeDir = 'both';
  _lbHistory = [];
  _invalidateMapCache();
  const search = document.getElementById('lb-search');
  if (search) search.value = '';
  _showMainEmpty();
  _loadEntries(_lbProject);
  _loadRunNames(_lbProject);
  if (typeof _updateActiveTabExtra === 'function') {
    _updateActiveTabExtra({ lbProject: _lbProject, lbEntryId: null });
  }
}


// ── Entry list ──────────────────────────────────────────────────────────────

async function _loadEntries(project, query) {
  const el = document.getElementById('lb-sidebar-list');
  if (!el) return;
  const params = new URLSearchParams({ limit: '200' });
  if (query) params.set('q', query);
  if (_lbTypeFilter) params.set('type', _lbTypeFilter);
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
  const pinned = entries.filter(e => e.pinned);
  const unpinned = entries.filter(e => !e.pinned);
  const sorted = [...pinned, ...unpinned];

  let html = '';
  if (pinned.length && unpinned.length) {
    html += _renderSidebarItems(pinned, true);
    html += '<div class="lb-sidebar-pin-sep"></div>';
    html += _renderSidebarItems(unpinned, false);
  } else {
    html += _renderSidebarItems(sorted, false);
  }
  el.innerHTML = html;
  if (typeof _appTabs !== 'undefined' && typeof _activeTabId !== 'undefined') {
    const at = _appTabs.find(t => t.id === _activeTabId);
    if (at && at.lbEntryId) _highlightSidebarItem(at.lbEntryId);
  }
}

function _renderSidebarItems(items, showPinIcon) {
  return items.map(e => {
    const date = _formatDate(e.created_at);
    const title = (e.title || '').replace(/</g, '&lt;');
    const preview = (e.body_preview || '').replace(/</g, '&lt;').replace(/\n/g, ' ');
    const isPlan = e.entry_type === 'plan';
    const typeCls = isPlan ? ' lb-type-plan' : '';
    const pinned = _isEntryPinned(e.id);
    const pinCls = pinned ? ' lb-pinned' : '';
    const pinBtn = `<span class="lb-pin-btn${pinned ? ' active' : ''}" onclick="event.stopPropagation();togglePinEntry(${e.id})" title="${pinned ? 'Unpin' : 'Pin'}">📌</span>`;
    return `<div class="lb-sidebar-item${typeCls}${pinCls}" data-id="${e.id}" onclick="openLogbookEntry(${e.id})">
      <div class="lb-sidebar-item-title">${pinBtn}${title} <span class="lb-sidebar-item-id">#${e.id}</span></div>
      <div class="lb-sidebar-item-date">${date}</div>
      <div class="lb-sidebar-item-preview">${preview}</div>
    </div>`;
  }).join('');
}

function _highlightSidebarItem(id) {
  document.querySelectorAll('.lb-sidebar-item').forEach(el => {
    el.classList.toggle('active', el.dataset.id === String(id));
  });
}


// ── Search & filter ─────────────────────────────────────────────────────────

function onLogbookSearch() {
  clearTimeout(_lbSearchTimer);
  _lbSearchTimer = setTimeout(() => {
    const q = (document.getElementById('lb-search') || {}).value || '';
    if (_lbProject) _loadEntries(_lbProject, q.trim() || undefined);
  }, 300);
}

function filterLogbookType(btn) {
  document.querySelectorAll('.lb-type-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  _lbTypeFilter = btn.dataset.type || '';
  const q = (document.getElementById('lb-search') || {}).value || '';
  if (_lbProject) _loadEntries(_lbProject, q.trim() || undefined);
}


// ── Main pane — entry detail ────────────────────────────────────────────────

function _showMainEmpty() {
  const el = document.getElementById('lb-main');
  if (!el) return;
  el.classList.remove('lb-main-plan');
  el.innerHTML = '<div class="lb-main-empty">Select an entry or create a new one.</div>';
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
    _mapActive = false;
    _stopMapGraphSimulation();
    _syncMapBtnActive(false);
    _showMainEmpty();
    return;
  }

  _lbHistory.pop();
  const prev = _lbHistory[_lbHistory.length - 1];

  if (prev.project && prev.project !== _lbProject) {
    _lbProject = prev.project;
    const sel = document.getElementById('lb-project-select');
    if (sel) sel.value = prev.project;
    _invalidateMapCache();
    _loadEntries(prev.project);
    _loadRunNames(prev.project);
  }

  if (prev.type === 'map') {
    _mapActive = true;
    _syncMapBtnActive(true);
    _loadMapData(_lbProject);
    return;
  }

  _mapActive = false;
  _stopMapGraphSimulation();
  _syncMapBtnActive(false);
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
    const title = (entry.title || '').replace(/</g, '&lt;');
    const bodyHtml = _renderLogbookMarkdown(entry.body || '');
    const created = _formatDate(entry.created_at);
    const edited = _formatDate(entry.edited_at);
    const isPlan = entry.entry_type === 'plan';
    el.classList.toggle('lb-main-plan', isPlan);
    const typeBadge = isPlan ? '<span class="lb-badge-plan">plan</span>' : '<span class="lb-badge-note">note</span>';
    el.innerHTML = `
      <div class="lb-detail">
        <div class="lb-detail-actions">
          <button class="btn" onclick="_lbGoBack()" title="Back">← back</button>
          <button class="btn" onclick="openEntryGraph(${entry.id})" title="Graph around this entry">graph</button>
          ${typeBadge}
          <span style="flex:1"></span>
          <span class="lb-export-hint" onclick="exportEntryHtml()" title="Export as HTML — then ⌘S to save">${_exportShortcutLabel()} export</span>
          <button class="btn" onclick="editLogbookEntry(${entry.id})">edit</button>
          <button class="btn" onclick="deleteLogbookEntry(${entry.id})" style="color:var(--red)">delete</button>
        </div>
        <h1 class="lb-detail-title">${title} <span class="lb-detail-id">#${entry.id}</span></h1>
        <div class="lb-detail-meta">
          <span>Created ${created}</span>
          ${entry.created_at !== entry.edited_at ? `<span>· Edited ${edited}</span>` : ''}
        </div>
        <div class="lb-detail-body">${bodyHtml}</div>
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

  const origin = window.location.origin;
  bodyHtml = bodyHtml.replace(
    /<iframe\s+src="(\/api\/logbook\/[^"]+\.html?)"([^>]*)><\/iframe>/gi,
    `<iframe src="${origin}$1" onerror="this.classList.add('embed-failed')"$2></iframe><div class="embed-offline">Interactive figure — open in clausius to view</div>`
  );

  bodyHtml = bodyHtml.replace(/<table\b/g, '<div class="table-wrap"><table')
                     .replace(/<\/table>/g, '</table></div>');

  const created = _formatDate(e.created_at);
  const edited = e.created_at !== e.edited_at ? ` · Edited ${_formatDate(e.edited_at)}` : '';
  const typeBadge = e.entry_type === 'plan' ? '<span class="badge badge-plan">plan</span>' : '';

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
h1 { font-size: 28px; font-weight: 700; line-height: 1.3; margin-bottom: 8px; letter-spacing: -0.02em; }
h2 { font-size: 20px; font-weight: 700; margin: 36px 0 12px; padding-bottom: 6px; border-bottom: 1px solid var(--border); }
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
  ${bodyHtml}
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


// ── Editor ──────────────────────────────────────────────────────────────────

function showLogbookEditor(entryId, title, body, entryType) {
  _lbEditingId = entryId || null;
  const el = document.getElementById('lb-main');
  if (!el) return;
  const titleVal = (title || '').replace(/"/g, '&quot;');
  const bodyVal = (body || '').replace(/</g, '&lt;');
  const typeVal = entryType || 'note';
  el.innerHTML = `
    <div class="lb-editor">
      <div class="lb-editor-type-row">
        <select id="lb-edit-type" class="lb-editor-type-select">
          <option value="note" ${typeVal === 'note' ? 'selected' : ''}>Note</option>
          <option value="plan" ${typeVal === 'plan' ? 'selected' : ''}>Plan</option>
        </select>
      </div>
      <input type="text" class="lb-editor-title" id="lb-edit-title" placeholder="Entry title" value="${titleVal}">
      <textarea class="lb-editor-body" id="lb-edit-body" placeholder="Write your entry in markdown…&#10;&#10;Use @run-name to reference jobs.&#10;Drag/drop or paste images to attach.&#10;Tables, code blocks, and headers are all supported." rows="20">${bodyVal}</textarea>
      <div class="lb-editor-hint">drag &amp; drop or paste images into the editor</div>
      <div class="lb-editor-actions">
        <button class="btn" onclick="saveLogbookEntry()">save</button>
        <button class="btn" onclick="_onEditorCancel()">cancel</button>
      </div>
    </div>`;
  _setupImageHandlers();
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
    showLogbookEditor(entryId, entry.title, entry.body, entry.entry_type);
  } catch (e) {
    toast('Failed to load entry for editing', 'error');
  }
}

async function saveLogbookEntry() {
  const titleInput = document.getElementById('lb-edit-title');
  const bodyInput = document.getElementById('lb-edit-body');
  const typeSelect = document.getElementById('lb-edit-type');
  if (!titleInput || !bodyInput || !_lbProject) return;
  const title = titleInput.value.trim();
  const body = bodyInput.value.trim();
  const entry_type = typeSelect ? typeSelect.value : 'note';
  if (!title) { toast('Title is required', 'error'); return; }

  const t = toastLoading(_lbEditingId ? 'Saving entry…' : 'Creating entry…');
  try {
    let res;
    if (_lbEditingId) {
      res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/entries/${_lbEditingId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title, body, entry_type }),
      });
    } else {
      res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/entries`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title, body, entry_type }),
      });
    }
    const d = await res.json();
    if (d.status === 'ok') {
      t.done(_lbEditingId ? 'Entry updated' : 'Entry created');
      const openId = _lbEditingId || d.id;
      _lbEditingId = null;
      _invalidateMapCache(_lbProject);
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
      _invalidateMapCache(_lbProject);
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
    _invalidateMapCache();
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


// ── Semantic map ─────────────────────────────────────────────────────────────

let _mapActive = false;
let _mapData = null;
let _mapDataProject = '';
let _mapGraphSimulation = null;
let _mapView = 'tree';
let _mapFocusEntryId = null;
let _mapNeighborHops = 1;
let _mapEdgeDir = 'both';

try {
  const savedMapView = localStorage.getItem(LB_MAP_VIEW_KEY);
  if (savedMapView === 'tree' || savedMapView === 'graph') _mapView = savedMapView;
} catch (_) {}

function _syncMapBtnActive(active) {
  const mapBtn = document.querySelector('.lb-map-btn[data-type="map"]');
  if (mapBtn) mapBtn.classList.toggle('active', active);
  if (active) {
    document.querySelectorAll('.lb-type-btn').forEach(b => b.classList.remove('active'));
  } else {
    const allBtn = document.querySelector('.lb-type-btn[data-type=""]');
    if (allBtn && !document.querySelector('.lb-type-btn.active')) allBtn.classList.add('active');
  }
}

function _invalidateMapCache(project) {
  if (!project || project === _mapDataProject) {
    _mapData = null;
    _mapDataProject = '';
  }
}

function _stopMapGraphSimulation() {
  if (_mapGraphSimulation) {
    _mapGraphSimulation.stop();
    _mapGraphSimulation = null;
  }
}

function _mapFocusLabel() {
  return _mapFocusEntryId ? `#${_mapFocusEntryId}` : '';
}

function _renderGraphScopeControls() {
  if (!_mapFocusEntryId && _mapView !== 'graph') return '';

  const hopOpts = ['1', '2', '3', '4', '5', 'all']
    .map(v => `<option value="${v}"${String(_mapNeighborHops) === v ? ' selected' : ''}>${v === 'all' ? 'all' : `${v} hop${v === '1' ? '' : 's'}`}</option>`)
    .join('');

  const dirOpts = [['both', 'Both'], ['outgoing', 'Outgoing'], ['incoming', 'Incoming']]
    .map(([v, label]) => `<option value="${v}"${_mapEdgeDir === v ? ' selected' : ''}>${label}</option>`)
    .join('');

  let html = '<div class="lb-map-scope-controls">';

  if (_mapFocusEntryId) {
    html += `<span class="lb-map-scope-chip">focus ${_mapFocusLabel()}</span>`;
    html += `<label class="lb-map-scope-select-wrap">neighbors <select class="lb-map-scope-select" onchange="_setMapNeighborHops(this.value)">${hopOpts}</select></label>`;
    html += `<button type="button" class="lb-map-scope-clear" onclick="_clearMapFocus()">all entries</button>`;
  }

  if (_mapView === 'graph') {
    html += `<label class="lb-map-scope-select-wrap">edges <select class="lb-map-scope-select" onchange="_setMapEdgeDir(this.value)">${dirOpts}</select></label>`;
  }

  html += '</div>';
  return html;
}

function _renderMapToggle() {
  const treeCls = _mapView === 'tree' ? ' active' : '';
  const graphCls = _mapView === 'graph' ? ' active' : '';
  return `<div class="lb-map-toolbar">
    <div class="lb-map-toggle">
      <button type="button" class="lb-map-toggle-btn${treeCls}" onclick="_setMapView('tree')">Tree</button>
      <button type="button" class="lb-map-toggle-btn${graphCls}" onclick="_setMapView('graph')">Graph</button>
    </div>
    ${_renderGraphScopeControls()}
  </div>`;
}

function _setMapView(view) {
  if (view !== 'tree' && view !== 'graph') return;
  _mapView = view;
  try { localStorage.setItem(LB_MAP_VIEW_KEY, _mapView); } catch (_) {}
  if (!_mapActive) return;
  const el = document.getElementById('lb-main');
  if (!el) return;
  if (_mapData && _mapDataProject === _lbProject) {
    _renderMapView(el, _mapData);
  } else {
    _loadMapData(_lbProject);
  }
}

function _setMapNeighborHops(value) {
  if (value === 'all') _mapNeighborHops = 'all';
  else {
    const n = parseInt(value, 10);
    _mapNeighborHops = Number.isFinite(n) ? Math.max(1, Math.min(6, n)) : 1;
  }
  if (!_mapActive) return;
  const el = document.getElementById('lb-main');
  if (!el) return;
  if (_mapData && _mapDataProject === _lbProject) _renderMapView(el, _mapData);
}

function _setMapEdgeDir(dir) {
  if (dir !== 'both' && dir !== 'outgoing' && dir !== 'incoming') return;
  _mapEdgeDir = dir;
  if (!_mapActive) return;
  const el = document.getElementById('lb-main');
  if (!el) return;
  if (_mapData && _mapDataProject === _lbProject) _renderMapView(el, _mapData);
}

function _clearMapFocus() {
  _mapFocusEntryId = null;
  _mapNeighborHops = 1;
  if (!_mapActive) return;
  const el = document.getElementById('lb-main');
  if (!el) return;
  if (_mapData && _mapDataProject === _lbProject) _renderMapView(el, _mapData);
}

function openEntryGraph(entryId) {
  if (!_lbProject || !entryId) return;
  _mapFocusEntryId = Number(entryId);
  _mapNeighborHops = 1;
  _mapEdgeDir = 'both';
  _mapView = 'graph';
  try { localStorage.setItem(LB_MAP_VIEW_KEY, _mapView); } catch (_) {}
  _mapActive = true;
  _syncMapBtnActive(true);
  _pushLbHistory({ type: 'map', project: _lbProject });
  _loadMapData(_lbProject);
}

function toggleLogbookMap() {
  _mapActive = !_mapActive;
  _syncMapBtnActive(_mapActive);
  if (_mapActive) {
    _mapFocusEntryId = null;
    _mapNeighborHops = 1;
    _mapEdgeDir = 'both';
    _pushLbHistory({ type: 'map', project: _lbProject });
    _loadMapData(_lbProject);
  } else {
    _stopMapGraphSimulation();
    const top = _lbHistory[_lbHistory.length - 1];
    if (top && top.type === 'map') _lbHistory.pop();
    _showMainEmpty();
    filterLogbookType(document.querySelector('.lb-type-btn[data-type=""]'));
  }
}

async function _loadMapData(project, forceRefresh = false) {
  if (!project) return;
  const el = document.getElementById('lb-main');
  if (!el) return;
  if (!forceRefresh && _mapData && _mapDataProject === project) {
    _renderMapView(el, _mapData);
    return;
  }
  el.innerHTML = _renderMapToggle() + '<div class="lb-main-empty">Loading map...</div>';
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(project)}/map`);
    const data = await res.json();
    if (data && !data.error) {
      _mapData = data;
      _mapDataProject = project;
    }
    _renderMapView(el, data && !data.error ? data : (_mapData || data));
  } catch (e) {
    el.innerHTML = _renderMapToggle() + '<div class="lb-main-empty" style="color:var(--red)">Failed to load map</div>';
  }
}

function _renderMapView(el, data) {
  if (_mapView === 'graph') _renderGraph(el, data);
  else _renderMap(el, data);
}

function _renderMap(el, data) {
  _stopMapGraphSimulation();
  const focused = _getFocusedGraphData(data);
  const nodes = focused.nodes;
  const links = focused.links;
  if (!nodes.length) {
    el.innerHTML = _renderMapToggle() + '<div class="lb-main-empty">No entries to map. Use #id in entry bodies to create links.</div>';
    return;
  }

  const byId = {};
  for (const n of nodes) byId[n.id] = n;
  const nodeIds = new Set(nodes.map(n => n.id));

  // Build directed graph: source references target via #id.
  // In the tree view, target is rendered as parent, source as child.
  const childrenOf = {};
  const hasParent = new Set();
  for (const l of links) {
    if (!nodeIds.has(l.source_id) || !nodeIds.has(l.target_id)) continue;
    childrenOf[l.target_id] = childrenOf[l.target_id] || [];
    childrenOf[l.target_id].push(l.source_id);
    hasParent.add(l.source_id);
  }

  const roots = nodes.filter(n => !hasParent.has(n.id));

  roots.sort((a, b) => {
    if (a.entry_type !== b.entry_type) return a.entry_type === 'plan' ? -1 : 1;
    return (b.edited_at || '').localeCompare(a.edited_at || '');
  });

  const rendered = new Set();
  function renderSubtree(nodeId, depth) {
    if (rendered.has(nodeId)) return '';
    rendered.add(nodeId);
    const n = byId[nodeId];
    if (!n) return '';
    const isPlan = n.entry_type === 'plan';
    const kids = (childrenOf[nodeId] || []).filter(id => !rendered.has(id));
    kids.sort((a, b) => (byId[b]?.edited_at || '').localeCompare(byId[a]?.edited_at || ''));

    const nodeCls = isPlan ? 'lb-map-plan' : 'lb-map-note';
    const iconCls = isPlan ? 'is-plan' : 'is-note';
    const pinnedCls = _isEntryPinned(n.id) ? ' lb-map-pinned' : '';
    const idBadge = `<span class="lb-map-id">#${n.id}</span>`;

    let html = '';
    if (depth > 0) html += '<div class="lb-map-child-wrap">';
    html += `<div class="lb-map-node ${nodeCls}${pinnedCls}" onclick="_mapClickEntry(${n.id})">
      <span class="lb-map-icon ${iconCls}" aria-hidden="true"></span>
      <div class="lb-map-node-text">
        <span class="lb-map-node-title">${_escMapHtml(n.title)}</span>
        <span class="lb-map-node-meta">${_fmtMapDate(n.created_at)} ${idBadge}</span>
      </div>
      ${kids.length ? `<span class="lb-map-count">${kids.length}</span>` : ''}
    </div>`;

    if (kids.length) {
      html += '<div class="lb-map-children">';
      for (const kid of kids) html += renderSubtree(kid, depth + 1);
      html += '</div>';
    }
    if (depth > 0) html += '</div>';
    return html;
  }

  let html = _renderMapToggle() + '<div class="lb-map-scroll"><div class="lb-map">';
  for (const root of roots) {
    html += `<div class="lb-map-branch">${renderSubtree(root.id, 0)}</div>`;
  }

  // Render any nodes in cycles (linked but not reachable from roots).
  const unreached = nodes.filter(n => !rendered.has(n.id));
  for (const n of unreached) {
    html += `<div class="lb-map-branch">${renderSubtree(n.id, 0)}</div>`;
  }

  html += '</div></div>';
  el.innerHTML = html;
}

function _buildMapDepths(nodes, links) {
  const indegree = new Map();
  const outgoing = new Map();
  for (const n of nodes) {
    indegree.set(n.id, 0);
    outgoing.set(n.id, []);
  }
  for (const l of links) {
    if (!indegree.has(l.source) || !indegree.has(l.target)) continue;
    indegree.set(l.target, (indegree.get(l.target) || 0) + 1);
    outgoing.get(l.source).push(l.target);
  }

  const roots = nodes
    .filter(n => (indegree.get(n.id) || 0) === 0)
    .map(n => n.id);
  const queue = roots.slice();
  const depths = new Map();
  for (const id of roots) depths.set(id, 0);

  while (queue.length) {
    const id = queue.shift();
    const baseDepth = depths.get(id) || 0;
    for (const child of outgoing.get(id) || []) {
      const nextDepth = baseDepth + 1;
      if (!depths.has(child) || nextDepth < depths.get(child)) {
        depths.set(child, nextDepth);
        queue.push(child);
      }
    }
  }

  // Cyclic or disconnected nodes still get a deterministic lane.
  const unresolved = nodes
    .filter(n => !depths.has(n.id))
    .sort((a, b) => (a.created_at || '').localeCompare(b.created_at || ''));
  let altDepth = 0;
  for (const n of unresolved) {
    depths.set(n.id, altDepth);
    altDepth = (altDepth + 1) % 2;
  }
  return { depths, indegree };
}

function _getFocusedGraphData(data) {
  const nodes = Array.isArray(data?.nodes) ? data.nodes : [];
  const links = Array.isArray(data?.links) ? data.links : [];
  if (!_mapFocusEntryId) return { nodes, links, focusKey: null };

  const focusKey = String(_mapFocusEntryId);
  const nodeById = new Map(nodes.map(n => [String(n.id), n]));
  if (!nodeById.has(focusKey)) {
    _mapFocusEntryId = null;
    _mapNeighborHops = 1;
    return { nodes, links, focusKey: null };
  }

  const adj = new Map();
  for (const n of nodes) adj.set(String(n.id), new Set());
  for (const l of links) {
    const s = String(l.source_id);
    const t = String(l.target_id);
    if (!adj.has(s) || !adj.has(t)) continue;
    // Neighborhood is treated as undirected around focus.
    adj.get(s).add(t);
    adj.get(t).add(s);
  }

  const keep = new Set([focusKey]);
  const q = [[focusKey, 0]];
  const maxHops = _mapNeighborHops === 'all' ? Number.POSITIVE_INFINITY : Number(_mapNeighborHops) || 1;
  while (q.length) {
    const [cur, dist] = q.shift();
    if (dist >= maxHops) continue;
    for (const nxt of adj.get(cur) || []) {
      if (keep.has(nxt)) continue;
      keep.add(nxt);
      q.push([nxt, dist + 1]);
    }
  }

  const filteredNodes = nodes.filter(n => keep.has(String(n.id)));
  const filteredLinks = links.filter(l => keep.has(String(l.source_id)) && keep.has(String(l.target_id)));
  return { nodes: filteredNodes, links: filteredLinks, focusKey };
}

function _renderGraph(el, data) {
  const focused = _getFocusedGraphData(data);
  const nodesIn = focused.nodes;
  const linksIn = focused.links;
  const focusKey = focused.focusKey;
  if (!nodesIn.length) {
    _stopMapGraphSimulation();
    el.innerHTML = _renderMapToggle() + '<div class="lb-main-empty">No entries to map. Use #id in entry bodies to create links.</div>';
    return;
  }
  if (typeof d3 === 'undefined') {
    _mapView = 'tree';
    _renderMap(el, data);
    return;
  }
  _stopMapGraphSimulation();

  const nodeById = new Map();
  for (const n of nodesIn) nodeById.set(n.id, { ...n });
  const allLinks = [];
  for (const l of linksIn) {
    if (!nodeById.has(l.source_id) || !nodeById.has(l.target_id)) continue;
    allLinks.push({ source: l.source_id, target: l.target_id });
  }

  let links = allLinks;
  if (_mapEdgeDir === 'outgoing' && focusKey) {
    links = allLinks.filter(l => String(l.source) === focusKey);
  } else if (_mapEdgeDir === 'incoming' && focusKey) {
    links = allLinks.filter(l => String(l.target) === focusKey);
  }
  const nodes = Array.from(nodeById.values()).map(n => ({ ...n }));

  const { depths } = _buildMapDepths(nodes, links);
  let maxDepth = 0;
  for (const n of nodes) {
    n.depth = depths.get(n.id) || 0;
    maxDepth = Math.max(maxDepth, n.depth);
  }

  // ── Static hierarchical layout ──
  const minNodeW = 260;
  const maxNodeW = 600;
  const nodeH = 54;
  const rowGap = 18;
  const colGap = 60;

  for (const n of nodes) {
    const estimated = 70 + String(n.title || '').length * 7;
    n.w = Math.max(minNodeW, Math.min(maxNodeW, estimated));
  }

  // Group nodes by depth column, sort by date within each column.
  const columns = new Map();
  for (const n of nodes) {
    if (!columns.has(n.depth)) columns.set(n.depth, []);
    columns.get(n.depth).push(n);
  }
  for (const col of columns.values()) {
    col.sort((a, b) => {
      if (a.entry_type !== b.entry_type) return a.entry_type === 'plan' ? -1 : 1;
      return (b.edited_at || '').localeCompare(a.edited_at || '');
    });
  }

  // Compute max width per column for uniform spacing.
  const colWidths = new Map();
  for (const [d, col] of columns) {
    colWidths.set(d, Math.max(...col.map(n => n.w)));
  }

  // Compute x positions (column centers).
  const leftPad = 40;
  const colCenters = new Map();
  let cx = leftPad;
  for (let d = 0; d <= maxDepth; d++) {
    const cw = colWidths.get(d) || minNodeW;
    colCenters.set(d, cx + cw / 2);
    cx += cw + colGap;
  }
  const rightPad = 40;
  const totalWidth = cx - colGap + rightPad;

  // Compute y positions (stack within column).
  const topPad = 30;
  let maxY = 0;
  for (const [d, col] of columns) {
    let y = topPad;
    for (const n of col) {
      n.x = colCenters.get(d);
      n.y = y + nodeH / 2;
      y += nodeH + rowGap;
    }
    maxY = Math.max(maxY, y);
  }
  const totalHeight = Math.max(maxY + topPad, 300);

  el.innerHTML = _renderMapToggle() + `
    <div class="lb-map lb-map--graph">
      <div class="lb-map-graph">
        <svg role="img" aria-label="Logbook graph map"></svg>
      </div>
    </div>`;

  const graphWrap = el.querySelector('.lb-map-graph');
  const svgNode = graphWrap ? graphWrap.querySelector('svg') : null;
  if (!graphWrap || !svgNode) return;

  const svg = d3.select(svgNode)
    .attr('viewBox', `0 0 ${totalWidth} ${totalHeight}`)
    .attr('width', totalWidth)
    .attr('height', totalHeight);

  const markerId = `lb-map-arrow-${Date.now()}`;
  svg.append('defs').append('marker')
    .attr('id', markerId)
    .attr('viewBox', '0 -5 10 10')
    .attr('refX', 9)
    .attr('refY', 0)
    .attr('markerWidth', 7)
    .attr('markerHeight', 7)
    .attr('orient', 'auto')
    .append('path')
    .attr('class', 'lb-map-arrow-head')
    .attr('d', 'M0,-5L10,0L0,5');

  const root = svg.append('g').attr('class', 'lb-map-graph-root');

  // Build a lookup for positioned nodes.
  const posById = new Map();
  for (const n of nodes) posById.set(n.id, n);

  // Draw edges as paths (curved for clarity).
  root.append('g').attr('class', 'lb-map-links')
    .selectAll('path')
    .data(links)
    .enter()
    .append('path')
    .attr('class', 'lb-map-link')
    .attr('fill', 'none')
    .attr('marker-end', `url(#${markerId})`)
    .attr('d', d => {
      const s = posById.get(typeof d.source === 'object' ? d.source.id : d.source);
      const t = posById.get(typeof d.target === 'object' ? d.target.id : d.target);
      if (!s || !t) return '';
      const sx = s.x + s.w / 2;
      const sy = s.y;
      const tx = t.x - t.w / 2 - 12;
      const ty = t.y;
      const mx = (sx + tx) / 2;
      return `M${sx},${sy} C${mx},${sy} ${mx},${ty} ${tx},${ty}`;
    });

  const nodeSel = root.append('g').attr('class', 'lb-map-nodes')
    .selectAll('g')
    .data(nodes, d => d.id)
    .enter()
    .append('g')
    .attr('class', d => {
      const typeCls = d.entry_type === 'plan' ? 'is-plan' : 'is-note';
      const focusCls = focusKey && String(d.id) === focusKey ? ' is-focus' : '';
      const pinCls = _isEntryPinned(d.id) ? ' is-pinned' : '';
      return `lb-map-gnode ${typeCls}${focusCls}${pinCls}`;
    })
    .attr('transform', d => `translate(${d.x},${d.y})`)
    .on('click', (event, d) => {
      event.stopPropagation();
      _mapClickEntry(d.id);
    });

  nodeSel.append('rect')
    .attr('x', d => -d.w / 2)
    .attr('y', -nodeH / 2)
    .attr('width', d => d.w)
    .attr('height', nodeH)
    .attr('rx', 10)
    .attr('ry', 10);

  nodeSel.append('circle')
    .attr('class', 'lb-map-gnode-dot')
    .attr('cx', d => -d.w / 2 + 13)
    .attr('cy', -nodeH / 2 + 13)
    .attr('r', 4.5);

  nodeSel.append('text')
    .attr('class', 'lb-map-gnode-title')
    .attr('x', d => -d.w / 2 + 24)
    .attr('y', 4)
    .text(d => String(d.title || ''));

  nodeSel.append('text')
    .attr('class', 'lb-map-gnode-meta')
    .attr('x', d => -d.w / 2 + 24)
    .attr('y', 18)
    .text(d => {
      const date = _fmtMapDate(d.created_at);
      return date ? `#${d.id} · ${date}` : `#${d.id}`;
    });

  // Drag support (manual repositioning, no physics).
  const drag = d3.drag()
    .on('start', (event, d) => { d._dx = 0; d._dy = 0; })
    .on('drag', (event, d) => {
      d.x = event.x;
      d.y = event.y;
      d3.select(event.sourceEvent.target.closest('.lb-map-gnode'))
        .attr('transform', `translate(${d.x},${d.y})`);
      // Redraw connected edges.
      root.selectAll('.lb-map-link').attr('d', dd => {
        const s = posById.get(typeof dd.source === 'object' ? dd.source.id : dd.source);
        const t = posById.get(typeof dd.target === 'object' ? dd.target.id : dd.target);
        if (!s || !t) return '';
        const sx = s.x + s.w / 2;
        const sy = s.y;
        const tx = t.x - t.w / 2 - 12;
        const ty = t.y;
        const mx = (sx + tx) / 2;
        return `M${sx},${sy} C${mx},${sy} ${mx},${ty} ${tx},${ty}`;
      });
    });
  nodeSel.call(drag);

  // Zoom + pan.
  const zoom = d3.zoom()
    .scaleExtent([0.3, 2.5])
    .on('zoom', event => root.attr('transform', event.transform));
  svg.call(zoom);

  // Compute bounding box of all nodes.
  let bx0 = Infinity, by0 = Infinity, bx1 = -Infinity, by1 = -Infinity;
  for (const n of nodes) {
    bx0 = Math.min(bx0, n.x - n.w / 2);
    by0 = Math.min(by0, n.y - nodeH / 2);
    bx1 = Math.max(bx1, n.x + n.w / 2);
    by1 = Math.max(by1, n.y + nodeH / 2);
  }
  const pad = 30;
  bx0 -= pad; by0 -= pad; bx1 += pad; by1 += pad;
  const bw = bx1 - bx0;
  const bh = by1 - by0;

  const vpW = graphWrap.clientWidth || totalWidth;
  const vpH = graphWrap.clientHeight || totalHeight;

  // If there's a focused entry, center on it at 1x scale.
  // Otherwise fit the full bounding box.
  const focusNode = focusKey ? posById.get(Number(focusKey)) : null;
  if (focusNode) {
    const s = Math.min(1, vpW / (focusNode.w + 400), vpH / (nodeH + 300));
    const tx = vpW / 2 - focusNode.x * s;
    const ty = vpH / 2 - focusNode.y * s;
    svg.call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(s));
  } else {
    const s = Math.min(1, vpW / bw * 0.95, vpH / bh * 0.95);
    const tx = (vpW - bw * s) / 2 - bx0 * s;
    const ty = (vpH - bh * s) / 2 - by0 * s;
    svg.call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(s));
  }
}

function _mapClickEntry(id) {
  _mapActive = false;
  _stopMapGraphSimulation();
  _syncMapBtnActive(false);
  openLogbookEntry(id);
}

function _escMapHtml(s) {
  return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function _fmtMapDate(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso.replace('T', ' '));
    return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
  } catch (_) { return ''; }
}

