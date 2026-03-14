// ── Logbooks ──

let _lbCurrentLogbook = '';
let _lbEditingIndex = -1;
let _lbResizing = false;
let _lbRunNames = [];
let _lbSuggestTarget = null;
let _lbSuggestStart = -1;

(function setupLogbookResizer() {
  document.addEventListener('mousedown', e => {
    if (e.target.id === 'logbook-resizer') {
      _lbResizing = true;
      e.preventDefault();
    }
  });
  document.addEventListener('mousemove', e => {
    if (!_lbResizing) return;
    const panel = document.getElementById('logbook-panel');
    if (!panel) return;
    const parentRect = panel.parentElement.getBoundingClientRect();
    let w = parentRect.right - e.clientX;
    if (w < 280) w = 280;
    if (w > 800) w = 800;
    panel.style.width = w + 'px';
  });
  document.addEventListener('mouseup', () => { _lbResizing = false; });
})();

async function loadLogbookPanel(project) {
  const sel = document.getElementById('logbook-select');
  const entries = document.getElementById('logbook-entries');
  if (!sel || !entries) return;

  try {
    const res = await fetch(`/api/logbooks/${encodeURIComponent(project)}`);
    const logbooks = await res.json();

    sel.innerHTML = logbooks.length
      ? logbooks.map(lb => `<option value="${lb.name}">${lb.name} (${lb.entry_count})</option>`).join('')
      : '<option value="">no logbooks</option>';

    if (logbooks.length) {
      if (_lbCurrentLogbook && logbooks.some(lb => lb.name === _lbCurrentLogbook)) {
        sel.value = _lbCurrentLogbook;
      } else {
        _lbCurrentLogbook = logbooks[0].name;
      }
      await renderLogbook(project, _lbCurrentLogbook);
    } else {
      _lbCurrentLogbook = '';
      entries.innerHTML = '<div class="logbook-empty">Create a logbook to start taking notes.</div>';
    }
  } catch (e) {
    entries.innerHTML = `<div class="logbook-empty" style="color:var(--red)">Failed to load logbooks</div>`;
  }
}

async function switchLogbook() {
  const sel = document.getElementById('logbook-select');
  _lbCurrentLogbook = sel.value;
  _lbEditingIndex = -1;
  if (_projCurrentName && _lbCurrentLogbook) {
    await renderLogbook(_projCurrentName, _lbCurrentLogbook);
  }
}

async function renderLogbook(project, name) {
  const el = document.getElementById('logbook-entries');
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(project)}/${encodeURIComponent(name)}`);
    const data = await res.json();
    if (data.error) {
      el.innerHTML = `<div class="logbook-empty">${data.error}</div>`;
      return;
    }
    const entries = data.entries || [];
    if (!entries.length) {
      el.innerHTML = '<div class="logbook-empty">No entries yet. Add your first note above.</div>';
      return;
    }
    el.innerHTML = entries.map((entry, i) => {
      const rendered = _renderLogbookMarkdown(entry);
      return `<div class="logbook-entry" data-index="${i}">
        <div class="logbook-entry-content">${rendered}</div>
        <div class="logbook-entry-actions">
          <button class="logbook-entry-btn" onclick="editLogbookEntry(${i})" title="edit">edit</button>
          <button class="logbook-entry-btn" onclick="deleteLogbookEntry(${i})" title="delete" style="color:var(--red)">delete</button>
        </div>
      </div>`;
    }).join('<div class="logbook-separator"></div>');
  } catch (e) {
    el.innerHTML = `<div class="logbook-empty" style="color:var(--red)">Failed: ${e}</div>`;
  }
}

function _renderLogbookMarkdown(raw) {
  let html = markdownToHtml(raw);
  html = html.replace(/@([\w_-]+)/g, (match, name) => {
    return `<span class="run-ref" onclick="openLogByName('${name}')">${match}</span>`;
  });
  return html;
}

async function openLogByName(runName) {
  if (!_projCurrentName) return;
  try {
    const res = await fetch(`/api/history?project=${encodeURIComponent(_projCurrentName)}&limit=500`);
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

async function addLogbookEntry() {
  const textarea = document.getElementById('logbook-new-entry');
  const content = textarea.value.trim();
  if (!content || !_projCurrentName) return;

  let name = _lbCurrentLogbook;
  if (!name) {
    name = 'notes';
    _lbCurrentLogbook = name;
  }

  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}/${encodeURIComponent(name)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      textarea.value = '';
      await loadLogbookPanel(_projCurrentName);
      toast('Entry added');
    } else {
      toast(d.error || 'Failed', 'error');
    }
  } catch (e) {
    toast('Failed to add entry', 'error');
  }
}

function editLogbookEntry(index) {
  const el = document.querySelector(`.logbook-entry[data-index="${index}"]`);
  if (!el) return;
  const contentEl = el.querySelector('.logbook-entry-content');
  const actionsEl = el.querySelector('.logbook-entry-actions');

  // Fetch raw content
  fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}/${encodeURIComponent(_lbCurrentLogbook)}`)
    .then(r => r.json())
    .then(data => {
      const raw = (data.entries || [])[index] || '';
      contentEl.innerHTML = `<textarea class="logbook-edit-area" rows="6">${raw.replace(/</g, '&lt;')}</textarea>`;
      actionsEl.innerHTML = `
        <button class="logbook-entry-btn" onclick="saveLogbookEntry(${index})">save</button>
        <button class="logbook-entry-btn" onclick="renderLogbook('${_projCurrentName}','${_lbCurrentLogbook}')">cancel</button>
      `;
    });
}

async function saveLogbookEntry(index) {
  const textarea = document.querySelector(`.logbook-entry[data-index="${index}"] textarea`);
  if (!textarea) return;
  const content = textarea.value.trim();
  if (!content) return;

  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}/${encodeURIComponent(_lbCurrentLogbook)}/${index}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      await renderLogbook(_projCurrentName, _lbCurrentLogbook);
      toast('Entry updated');
    } else {
      toast(d.error || 'Failed', 'error');
    }
  } catch (e) {
    toast('Failed to save entry', 'error');
  }
}

async function promptNewLogbook() {
  const name = prompt('Logbook name (e.g. experiments, bugs, ideas):');
  if (!name || !name.trim() || !_projCurrentName) return;

  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: name.trim() }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      _lbCurrentLogbook = name.trim();
      await loadLogbookPanel(_projCurrentName);
      toast(`Created logbook "${name.trim()}"`);
    } else {
      toast(d.error || 'Failed', 'error');
    }
  } catch (e) {
    toast('Failed to create logbook', 'error');
  }
}

async function deleteLogbookEntry(index) {
  if (!_projCurrentName || !_lbCurrentLogbook) return;
  if (!confirm(`Delete entry #${index + 1}?`)) return;
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}/${encodeURIComponent(_lbCurrentLogbook)}/${index}`, {
      method: 'DELETE',
    });
    const d = await res.json();
    if (d.status === 'ok') {
      await renderLogbook(_projCurrentName, _lbCurrentLogbook);
      toast('Entry deleted');
    } else {
      toast(d.error || 'Failed', 'error');
    }
  } catch (e) {
    toast('Failed to delete entry', 'error');
  }
}

async function promptRenameLogbook() {
  if (!_projCurrentName || !_lbCurrentLogbook) return;
  const newName = prompt(`Rename "${_lbCurrentLogbook}" to:`, _lbCurrentLogbook);
  if (!newName || !newName.trim() || newName.trim() === _lbCurrentLogbook) return;
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}/${encodeURIComponent(_lbCurrentLogbook)}/rename`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ new_name: newName.trim() }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      _lbCurrentLogbook = d.name || newName.trim();
      await loadLogbookPanel(_projCurrentName);
      toast(`Renamed to "${_lbCurrentLogbook}"`);
    } else {
      toast(d.error || 'Rename failed', 'error');
    }
  } catch (e) {
    toast('Failed to rename logbook', 'error');
  }
}

async function promptDeleteLogbook() {
  if (!_projCurrentName || !_lbCurrentLogbook) return;
  if (!confirm(`Delete logbook "${_lbCurrentLogbook}" and all its entries? This cannot be undone.`)) return;
  try {
    const res = await fetch(`/api/logbook/${encodeURIComponent(_projCurrentName)}/${encodeURIComponent(_lbCurrentLogbook)}`, {
      method: 'DELETE',
    });
    const d = await res.json();
    if (d.status === 'ok') {
      _lbCurrentLogbook = '';
      await loadLogbookPanel(_projCurrentName);
      toast('Logbook deleted');
    } else {
      toast(d.error || 'Delete failed', 'error');
    }
  } catch (e) {
    toast('Failed to delete logbook', 'error');
  }
}

// ── @ autocomplete ──────────────────────────────────────────────────────────

async function _loadRunNames(project) {
  try {
    const res = await fetch(`/api/history?project=${encodeURIComponent(project)}&limit=500`);
    const rows = await res.json();
    const names = new Set();
    for (const r of rows) {
      if (r.job_name) names.add(r.job_name);
    }
    _lbRunNames = Array.from(names).sort();
  } catch (_) {
    _lbRunNames = [];
  }
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

  // Position near the cursor in the textarea
  const rect = textarea.getBoundingClientRect();
  box.style.left = rect.left + 'px';
  box.style.top = (rect.bottom + 2) + 'px';
  box.style.width = Math.max(rect.width, 250) + 'px';
  box.style.display = 'block';

  box.querySelectorAll('.lb-suggest-item').forEach(item => {
    item.addEventListener('mousedown', e => {
      e.preventDefault();
      _insertSuggestion(textarea, item.dataset.name);
    });
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
  if (!ta.closest('.logbook-panel') && !ta.closest('.logbook-entry')) return;

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
    if (active && _lbSuggestTarget) {
      e.preventDefault();
      _insertSuggestion(_lbSuggestTarget, active.dataset.name);
    }
  } else if (e.key === 'Escape') {
    _hideSuggest();
  }
});

document.addEventListener('blur', e => {
  if (e.target.tagName === 'TEXTAREA') {
    setTimeout(_hideSuggest, 150);
  }
}, true);
