// ── Logbook v2 — full page ──

let _lbProject = '';
let _lbEditingId = null;
let _lbSearchTimer = null;
let _lbTypeFilter = '';
let _lbRunNames = [];
let _lbSuggestTarget = null;
let _lbSuggestStart = -1;
const LB_SIDEBAR_WIDTH_KEY = 'ncluster.lbSidebarWidth';
const LB_SIDEBAR_MIN = 200;
const LB_SIDEBAR_MAX = 600;

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
  try {
    const saved = parseInt(localStorage.getItem(LB_SIDEBAR_WIDTH_KEY) || '', 10);
    if (!isNaN(saved) && saved >= LB_SIDEBAR_MIN && saved <= LB_SIDEBAR_MAX) {
      const sidebar = document.querySelector('.lb-sidebar');
      if (sidebar) sidebar.style.width = saved + 'px';
    }
  } catch (_) {}
})();

// ── Page init ───────────────────────────────────────────────────────────────

function initLogbookPage() {
  const sel = document.getElementById('lb-project-select');
  if (!sel) return;

  fetch('/api/projects')
    .then(r => r.json())
    .then(projects => {
      if (!Array.isArray(projects)) projects = [];
      sel.innerHTML = projects.length
        ? projects.map(p => `<option value="${p.project}">${p.emoji || ''} ${p.project}</option>`).join('')
        : '<option value="">no projects</option>';

      if (projects.length) {
        if (_lbProject && projects.some(p => p.project === _lbProject)) {
          sel.value = _lbProject;
        } else {
          _lbProject = projects[0].project;
        }
        _loadEntries(_lbProject);
        _loadRunNames(_lbProject);
      }
    })
    .catch(() => {
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
  el.innerHTML = entries.map(e => {
    const date = _formatDate(e.created_at);
    const title = (e.title || '').replace(/</g, '&lt;');
    const preview = (e.body_preview || '').replace(/</g, '&lt;').replace(/\n/g, ' ');
    const isPlan = e.entry_type === 'plan';
    const typeCls = isPlan ? ' lb-type-plan' : '';
    return `<div class="lb-sidebar-item${typeCls}" data-id="${e.id}" onclick="openLogbookEntry(${e.id})">
      <div class="lb-sidebar-item-title">${title}</div>
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

async function openLogbookEntry(entryId) {
  if (!_lbProject) return;
  const el = document.getElementById('lb-main');
  if (!el) return;
  _highlightSidebarItem(entryId);
  if (typeof _updateActiveTabExtra === 'function') {
    _updateActiveTabExtra({ lbProject: _lbProject, lbEntryId: entryId });
  }
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/entries/${entryId}`);
    const entry = await res.json();
    if (entry.status === 'error') { toast(entry.error, 'error'); return; }
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
          ${typeBadge}
          <button class="btn" onclick="editLogbookEntry(${entry.id})">edit</button>
          <button class="btn" onclick="deleteLogbookEntry(${entry.id})" style="color:var(--red)">delete</button>
        </div>
        <h1 class="lb-detail-title">${title}</h1>
        <div class="lb-detail-meta">
          <span>Created ${created}</span>
          ${entry.created_at !== entry.edited_at ? `<span>· Edited ${edited}</span>` : ''}
        </div>
        <div class="lb-detail-body">${bodyHtml}</div>
      </div>`;
  } catch (e) {
    toast('Failed to load entry', 'error');
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
      toast(_lbEditingId ? 'Entry updated' : 'Entry created');
      const openId = _lbEditingId || d.id;
      _lbEditingId = null;
      await _loadEntries(_lbProject);
      if (openId) openLogbookEntry(openId);
    } else {
      toast(d.error || 'Failed', 'error');
    }
  } catch (e) {
    toast('Failed to save entry', 'error');
  }
}

async function deleteLogbookEntry(entryId) {
  if (!_lbProject) return;
  if (!confirm('Delete this entry? This cannot be undone.')) return;
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_lbProject)}/entries/${entryId}`, { method: 'DELETE' });
    const d = await res.json();
    if (d.status === 'ok') {
      toast('Entry deleted');
      _showMainEmpty();
      await _loadEntries(_lbProject);
    } else {
      toast(d.error || 'Failed', 'error');
    }
  } catch (e) {
    toast('Failed to delete entry', 'error');
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
  html = html.replace(/@([\w_-]+)/g, (match, name) =>
    `<span class="run-ref" onclick="openLogByName('${name}')">${match}</span>`
  );
  return html;
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
