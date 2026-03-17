// ── File Explorer ──
let _exCluster = null, _exJobId = null, _currentFilePath = null;
let _currentRemotePath = null, _currentResolvedPath = null, _currentSource = null;
const _treeState = {};   // path -> { open, entries }
const TREE_CACHE_TTL_MS = 30000;

async function openLog(cluster, jobId, jobName, force) {
  _exCluster = cluster;
  _exJobId = jobId;
  _currentFilePath = null;

  document.getElementById('modal-overlay').classList.add('open');
  if (jobName) document.getElementById('modal-title').textContent = jobName;
  document.getElementById('modal-subtitle').textContent = `${cluster} · job ${jobId}`;
  document.getElementById('content-path').textContent = 'discovering files…';
  document.getElementById('content-source').textContent = 'source: —';
  document.getElementById('content-source').className = 'source-pill';
  document.getElementById('modal-content').className = 'log-loading';
  document.getElementById('modal-content').textContent = 'Discovering log directories…';
  document.getElementById('tree-pane').innerHTML = '<div class="tree-loading">loading…</div>';
  for (const k of Object.keys(_treeState)) delete _treeState[k];

  try {
    const qs = force ? '?force=1' : '';
    const res = await fetch(`/api/log_files/${cluster}/${jobId}${qs}`);
    const data = await res.json();

    if (data.files && data.files[0] && data.files[0].error) {
      document.getElementById('modal-content').textContent = `SSH error: ${data.files[0].error}`;
      return;
    }
    if (data.error) {
      document.getElementById('modal-content').className = 'placeholder';
      document.getElementById('modal-content').textContent = data.error;
      document.getElementById('content-path').textContent = 'no logs available';
      return;
    }

    const files = (data.files || []).filter(f => f.path);
    const dirs  = data.dirs || [];

    // Build tree sections
    const tree = document.getElementById('tree-pane');
    tree.innerHTML = '';

    // Section 1: Quick-access log files grouped by type
    if (files.length) {
      tree.appendChild(makeTreeSection('📋 logs', files.map(f => ({
        name: f.label, path: f.path, is_dir: false,
        icon: f.label.includes('error') || f.label.includes('stderr') ? '⚠' : '📄'
      })), true));
    }

    // Section 2: Explorable dirs (eval-logs, eval-results, etc.)
    for (const dir of dirs) {
      tree.appendChild(makeTreeSection('📁 ' + dir.label, [], false, dir.path));
    }

    // Auto-open first file
    if (files.length) {
      await viewFile(files[0].path);
    } else if (dirs.length) {
      await expandDir(dirs[0].path, tree.querySelector('.tree-items'));
    } else {
      document.getElementById('modal-content').className = 'placeholder';
      document.getElementById('modal-content').textContent = 'No log files found for this job. It may not have started yet, or was killed before producing output.';
      document.getElementById('content-path').textContent = 'no logs available';
      document.getElementById('tree-pane').innerHTML = '<div class="tree-loading" style="color:var(--muted)">no files</div>';
    }
  } catch (e) {
    document.getElementById('modal-content').textContent = 'Failed: ' + e;
  }
}

function makeTreeSection(label, items, startOpen, dirPath) {
  const section = document.createElement('div');
  section.className = 'tree-section';

  const head = document.createElement('div');
  head.className = 'tree-section-head';
  const chevron = document.createElement('span');
  chevron.className = 'tree-chevron' + (startOpen ? ' open' : '');
  chevron.textContent = '▶';
  head.appendChild(chevron);
  head.appendChild(document.createTextNode(' ' + label));
  section.appendChild(head);

  const itemsEl = document.createElement('div');
  itemsEl.className = 'tree-items' + (startOpen ? ' open' : '');
  section.appendChild(itemsEl);

  if (items.length) {
    renderTreeItems(itemsEl, items);
  } else if (dirPath) {
    itemsEl.dataset.dirPath = dirPath;
    itemsEl.innerHTML = '<div class="tree-loading">click to load…</div>';
  }

  head.addEventListener('click', async () => {
    const open = itemsEl.classList.toggle('open');
    chevron.classList.toggle('open', open);
    if (open && dirPath) {
      // Always re-fetch on open (clear stale "(empty)" state).
      await expandDir(dirPath, itemsEl);
    }
  });

  return section;
}

function renderTreeItems(container, items, depth) {
  depth = depth || 0;
  container.innerHTML = '';
  for (const item of items) {
    const el = document.createElement('div');
    el.className = 'tree-item' + (item.is_dir ? ' is-dir' : '');
    if (!item.is_dir) {
      const n = (item.name || '').toLowerCase();
      if (n.endsWith('.err') || n.includes('error') || n.includes('traceback') || n.includes('failed')) {
        el.style.color = 'var(--red)';
      } else if (n.includes('warn')) {
        el.style.color = 'var(--amber)';
      }
    }
    el.style.paddingLeft = (22 + depth * 14) + 'px';
    el.title = item.path;

    const icon = document.createElement('span');
    icon.className = 'item-icon';
    icon.textContent = item.icon || (item.is_dir ? '📁' : '📄');
    el.appendChild(icon);

    const name = document.createElement('span');
    name.className = 'item-name';
    name.textContent = item.name;
    el.appendChild(name);
    if (!item.is_dir && item.source_hint) {
      const hint = document.createElement('span');
      hint.className = 'item-size';
      hint.textContent = item.source_hint;
      el.appendChild(hint);
    }

    // Badge for eval-logs context:
    // - current: file name includes selected job id
    // - group: other files under eval-logs
    if (!item.is_dir) {
      const pathL = (item.path || '').toLowerCase();
      const base = (item.name || '').toLowerCase();
      const jobId = String(_exJobId || '').toLowerCase();
      if (pathL.includes('/eval-logs/')) {
        const badge = document.createElement('span');
        badge.className = 'item-badge ' + (jobId && base.includes(jobId) ? 'current' : 'group');
        badge.textContent = jobId && base.includes(jobId) ? 'current' : 'group';
        el.appendChild(badge);
      }
    }

    if (!item.is_dir && item.size != null) {
      const sz = document.createElement('span');
      sz.className = 'item-size';
      sz.textContent = fmtSize(item.size);
      el.appendChild(sz);
    }

    if (item.is_dir) {
      // Sub-tree container
      const subContainer = document.createElement('div');
      subContainer.className = 'tree-items';
      subContainer.dataset.dirPath = item.path;

      el.addEventListener('click', async (e) => {
        e.stopPropagation();
        const isOpen = subContainer.classList.toggle('open');
        icon.textContent = isOpen ? '📂' : '📁';
        if (isOpen) {
          await expandDir(item.path, subContainer, depth + 1);
        }
      });

      // Insert after el
      const wrapper = document.createElement('div');
      wrapper.appendChild(el);
      wrapper.appendChild(subContainer);
      container.appendChild(wrapper);
    } else {
      el.addEventListener('click', async () => {
        document.querySelectorAll('.tree-item.active').forEach(e => e.classList.remove('active'));
        el.classList.add('active');
        await viewFile(item.path);
      });
      container.appendChild(el);
    }
  }
}

async function expandDir(path, container, depth) {
  const cacheKey = `${_exCluster}:${path}`;
  const cached = _treeState[cacheKey];
  if (cached && (Date.now() - cached.ts) < TREE_CACHE_TTL_MS) {
    renderTreeItems(container, cached.items, depth || 0);
    return;
  }

  container.innerHTML = '<div class="tree-loading">loading…</div>';
  try {
    const res = await fetch(`/api/ls/${_exCluster}?path=${encodeURIComponent(path)}&force=1`);
    const data = await res.json();
    if (data.status !== 'ok') {
      container.innerHTML = `<div class="tree-loading">${data.error || '(error)'}</div>`;
      return;
    }
    if (!data.entries || !data.entries.length) {
      container.innerHTML = '<div class="tree-loading">(empty directory)</div>';
      return;
    }
    const items = data.entries.map(e => ({
      name: e.name, path: e.path, is_dir: e.is_dir, size: e.size,
      icon: e.is_dir ? '📁' : guessIcon(e.name)
    }));
    _treeState[cacheKey] = { ts: Date.now(), items };
    renderTreeItems(container, items, depth || 0);
  } catch (e) {
    container.innerHTML = `<div class="tree-loading" style="color:var(--red)">Error: ${e}</div>`;
  }
}

function guessIcon(name) {
  const n = name.toLowerCase();
  if (n.endsWith('.log') || n.endsWith('.out') || n.endsWith('.err')) return '📋';
  if (n.endsWith('.json') || n.endsWith('.jsonl') || n.endsWith('.jsonl-async')) return '{}';
  if (n.endsWith('.md')) return 'Ⓜ';
  if (n.endsWith('.sh'))  return '⚙';
  if (n.endsWith('.txt')) return '📝';
  return '📄';
}

function escapeHtml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function renderLogWithHighlights(raw) {
  const lines = String(raw || '').split('\n');
  return lines.map((line) => {
    const l = line.toLowerCase();
    let cls = 'log-line';
    if (isBenignLogLine(l)) {
      cls += ' warn';
    } else {
      if (l.includes('traceback')) cls += ' trace';
      if (l.includes('error') || l.includes('exception') || l.includes('fatal')) cls += ' error';
      else if (l.includes('warning') || l.includes('warn')) cls += ' warn';
    }
    return `<div class="${cls}">${escapeHtml(line)}</div>`;
  }).join('');
}

function renderJsonWithSyntax(raw) {
  const src = escapeHtml(String(raw || ''));
  const re = /("(?:\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(?:\s*:)?|\btrue\b|\bfalse\b|\bnull\b|-?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?)/g;
  const highlighted = src.replace(re, (m) => {
    if (/^"/.test(m)) {
      if (/:$/.test(m)) return `<span class="json-key">${m}</span>`;
      return `<span class="json-string">${m}</span>`;
    }
    if (/^(true|false)$/.test(m)) return `<span class="json-boolean">${m}</span>`;
    if (m === 'null') return `<span class="json-null">${m}</span>`;
    return `<span class="json-number">${m}</span>`;
  });
  return highlighted.split('\n').map(line => `<div class="log-line">${line}</div>`).join('');
}

function jsonlRecordSummary(obj) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
    return '(non-object record)';
  }
  const preferred = ['id', 'uid', 'task', 'status', 'question', 'problem', 'prompt', 'error'];
  const picks = [];
  for (const k of preferred) {
    if (Object.prototype.hasOwnProperty.call(obj, k)) picks.push(k);
    if (picks.length >= 3) break;
  }
  if (!picks.length) {
    const ks = Object.keys(obj).slice(0, 3);
    picks.push(...ks);
  }
  const parts = picks.map((k) => {
    const v = obj[k];
    const s = typeof v === 'string' ? v : JSON.stringify(v);
    const short = String(s).replace(/\s+/g, ' ').slice(0, 44);
    return `${k}=${short}`;
  });
  return parts.join(' | ') || `(keys: ${Object.keys(obj).length})`;
}

function renderJsonlViewer(raw) {
  const lines = String(raw || '').split('\n').filter((ln) => ln.trim().length > 0);
  if (!lines.length) {
    return `<div class="jsonl-view"><div class="jsonl-meta">No JSONL records in file.</div></div>`;
  }
  let bad = 0;
  const records = lines.map((line, i) => {
    try {
      const obj = JSON.parse(line);
      const summary = escapeHtml(jsonlRecordSummary(obj));
      const pretty = JSON.stringify(obj, null, 2);
      return `<details class="jsonl-rec" data-jsonl-rec>
        <summary><span class="jsonl-sub"><span class="jsonl-idx">#${i + 1}</span><span class="jsonl-sum">${summary}</span></span><button class="jsonl-copy-btn" onclick="copyJsonlRecord(event, this)" title="copy record">⧉ copy</button></summary>
        <div class="jsonl-body json-view">${renderJsonWithSyntax(pretty)}</div>
      </details>`;
    } catch (_) {
      bad += 1;
      return `<details class="jsonl-rec" data-jsonl-rec>
        <summary><span class="jsonl-sub"><span class="jsonl-idx">#${i + 1}</span><span class="jsonl-bad">invalid JSON</span></span><button class="jsonl-copy-btn" onclick="copyJsonlRecord(event, this)" title="copy record">⧉ copy</button></summary>
        <div class="jsonl-body"><div class="log-line error">${escapeHtml(line)}</div></div>
      </details>`;
    }
  }).join('');

  return `<div class="jsonl-view">
    <div class="jsonl-toolbar">
      <div class="jsonl-meta">${lines.length} records${bad ? `, ${bad} invalid` : ''}</div>
      <div class="jsonl-actions">
        <button class="jsonl-btn" onclick="jsonlExpandAll()">expand all</button>
        <button class="jsonl-btn" onclick="jsonlCollapseAll()">collapse all</button>
      </div>
    </div>
    ${records}
  </div>`;
}

function jsonlExpandAll() {
  document.querySelectorAll('#modal-content [data-jsonl-rec]').forEach((d) => { d.open = true; });
}

function jsonlCollapseAll() {
  document.querySelectorAll('#modal-content [data-jsonl-rec]').forEach((d) => { d.open = false; });
}

function renderJsonlLazyViewer(data, filePath) {
  const records = data.records || [];
  const count = data.count || 0;
  if (!count) return '<div class="jsonl-view"><div class="jsonl-meta">No records in file.</div></div>';

  const invalid = records.filter(r => !r.valid).length;
  const items = records.map(r => {
    const summary = escapeHtml(_jsonlPreviewSummary(r.preview, r.valid));
    const sizeStr = r.size > 1024 ? `${(r.size / 1024).toFixed(0)}K` : `${r.size}B`;
    return `<details class="jsonl-rec" data-jsonl-rec data-line="${r.line}" data-path="${escapeHtml(filePath)}">
      <summary><span class="jsonl-sub">
        <span class="jsonl-idx">#${r.line + 1}</span>
        ${!r.valid ? '<span class="jsonl-bad">invalid</span>' : ''}
        <span class="jsonl-sum">${summary}</span>
      </span>
      <span style="font-family:var(--mono);font-size:9px;color:var(--muted);position:absolute;right:8px;top:50%;transform:translateY(-50%)">${sizeStr}</span>
      </summary>
      <div class="jsonl-body" data-lazy="1"><div class="tree-loading">click to load…</div></div>
    </details>`;
  }).join('');

  // Attach click handler via event delegation
  setTimeout(() => {
    const container = document.getElementById('modal-content');
    if (!container) return;
    container.addEventListener('toggle', async (e) => {
      const det = e.target.closest('[data-jsonl-rec]');
      if (!det || !det.open) return;
      const body = det.querySelector('[data-lazy]');
      if (!body || body.dataset.lazy !== '1') return;
      body.dataset.lazy = '0';
      body.innerHTML = '<div class="tree-loading">loading record…</div>';
      const line = det.dataset.line;
      const path = det.dataset.path;
      try {
        const res = await fetch(`/api/jsonl_record/${_exCluster}/${_exJobId}?path=${encodeURIComponent(path)}&line=${line}`);
        const d = await res.json();
        if (d.status === 'ok' && d.content) {
          try {
            const obj = JSON.parse(d.content);
            const pretty = JSON.stringify(obj, null, 2);
            body.className = 'jsonl-body json-view';
            body.innerHTML = renderJsonWithSyntax(pretty);
          } catch (_) {
            body.className = 'jsonl-body';
            body.innerHTML = `<div class="log-line error">${escapeHtml(d.content)}</div>`;
          }
        } else {
          body.innerHTML = `<div class="log-line error">${escapeHtml(d.error || 'Failed')}</div>`;
        }
      } catch (err) {
        body.innerHTML = `<div class="log-line error">Fetch error: ${err}</div>`;
      }
    }, true);
  }, 0);

  const total = data.total;
  const modeLabel = data.mode === 'all' ? 'all' : `${data.mode} ${data.limit}`;
  const metaId = 'jsonl-meta-' + Date.now();
  let showing;
  if (total > 0 && total !== count) {
    showing = `showing ${count} of ${total} (${modeLabel})`;
  } else if (total < 0) {
    showing = `${count} records (${modeLabel}) · counting…`;
  } else {
    showing = `${count} records (${modeLabel})`;
  }

  // Async total count fetch when total is unknown.
  if (total < 0 && filePath) {
    setTimeout(async () => {
      try {
        const res = await fetch(`/api/jsonl_index/${_exCluster}/${_exJobId}?path=${encodeURIComponent(filePath)}&mode=first&limit=0`);
        const d = await res.json();
        const el = document.getElementById(metaId);
        if (el && d.total > 0) {
          el.textContent = `showing ${count} of ${d.total} (${modeLabel})`;
        } else if (el && d.count != null) {
          el.textContent = `showing ${count} of ${d.count} (${modeLabel})`;
        }
      } catch (_) {}
    }, 0);
  }

  return `<div class="jsonl-view">
    <div class="jsonl-toolbar">
      <div class="jsonl-meta" id="${metaId}">${showing}${invalid ? `, ${invalid} invalid` : ''}</div>
    </div>
    ${items}
  </div>`;
}

function _jsonlPreviewSummary(preview, valid) {
  if (!valid) return preview.slice(0, 80);
  try {
    const trimmed = preview.trim();
    // Extract key fields from the preview (may be truncated).
    const preferred = ['id', 'uid', 'task', 'status', 'question', 'problem', 'prompt', 'error'];
    const parts = [];
    for (const k of preferred) {
      const re = new RegExp(`"${k}"\\s*:\\s*("[^"]*"|\\d+|true|false|null)`);
      const m = trimmed.match(re);
      if (m) {
        let v = m[1];
        if (v.startsWith('"')) v = v.slice(1, -1);
        parts.push(`${k}=${v.slice(0, 40)}`);
      }
      if (parts.length >= 3) break;
    }
    if (parts.length) return parts.join(' | ');
    return trimmed.slice(0, 80) + (trimmed.length >= 80 ? '…' : '');
  } catch (_) {
    return preview.slice(0, 80);
  }
}

async function copyJsonlRecord(ev, btn) {
  if (ev) {
    ev.preventDefault();
    ev.stopPropagation();
  }
  const details = btn && btn.closest ? btn.closest('[data-jsonl-rec]') : null;
  const body = details ? details.querySelector('.jsonl-body') : null;
  const text = (body && body.textContent ? body.textContent : '').trim();
  if (!text) {
    toast('No record content to copy', 'error');
    return;
  }
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      toast('Copied record');
      return;
    }
    throw new Error('Clipboard API unavailable');
  } catch (_) {
    try {
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.top = '-10000px';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      const ok = document.execCommand('copy');
      document.body.removeChild(ta);
      if (ok) toast('Copied record');
      else toast('Clipboard copy failed', 'error');
    } catch (_) {
      toast('Clipboard copy failed', 'error');
    }
  }
}

function _mdInline(text) {
  let s = escapeHtml(text);
  s = s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  s = s.replace(/\*(.+?)\*/g, '<em>$1</em>');
  s = s.replace(/`(.+?)`/g, '<code class="md-inline-code">$1</code>');
  s = s.replace(/~~(.+?)~~/g, '<del>$1</del>');
  return s;
}

function _isTableRow(line) {
  const t = line.trim();
  return t.startsWith('|') && t.endsWith('|') && t.includes('|');
}

function _isTableSep(line) {
  return /^\|[\s:|-]+\|$/.test(line.trim());
}

function _renderTableRows(tableLines) {
  if (tableLines.length < 2) return tableLines.map(l => `<p>${_mdInline(l)}</p>`).join('');
  const parseRow = (line, tag) => {
    const cells = line.trim().replace(/^\||\|$/g, '').split('|').map(c => c.trim());
    return `<tr>${cells.map(c => `<${tag}>${_mdInline(c)}</${tag}>`).join('')}</tr>`;
  };
  let html = '<table class="md-table"><thead>' + parseRow(tableLines[0], 'th') + '</thead><tbody>';
  const start = _isTableSep(tableLines[1]) ? 2 : 1;
  for (let i = start; i < tableLines.length; i++) {
    html += parseRow(tableLines[i], 'td');
  }
  html += '</tbody></table>';
  return html;
}

function markdownToHtml(raw) {
  const lines = String(raw || '').split('\n');
  let html = '';
  let inCode = false;
  let inList = false;
  let tableBuffer = [];

  function flushTable() {
    if (tableBuffer.length) {
      html += _renderTableRows(tableBuffer);
      tableBuffer = [];
    }
  }

  for (const line of lines) {
    if (line.trim().startsWith('```')) {
      flushTable();
      if (!inCode) {
        if (inList) { html += '</ul>'; inList = false; }
        html += '<pre><code>';
        inCode = true;
      } else {
        html += '</code></pre>';
        inCode = false;
      }
      continue;
    }
    if (inCode) {
      html += escapeHtml(line) + '\n';
      continue;
    }
    if (_isTableRow(line)) {
      if (inList) { html += '</ul>'; inList = false; }
      tableBuffer.push(line);
      continue;
    }
    flushTable();
    const h = line.match(/^(#{1,3})\s+(.*)$/);
    if (h) {
      if (inList) { html += '</ul>'; inList = false; }
      const lvl = h[1].length;
      html += `<h${lvl}>${_mdInline(h[2])}</h${lvl}>`;
      continue;
    }
    const li = line.match(/^\s*[-*]\s+(.*)$/);
    if (li) {
      if (!inList) { html += '<ul>'; inList = true; }
      html += `<li>${_mdInline(li[1])}</li>`;
      continue;
    }
    if (inList) { html += '</ul>'; inList = false; }
    if (!line.trim()) html += '<p></p>';
    else html += `<p>${_mdInline(line)}</p>`;
  }
  flushTable();
  if (inList) html += '</ul>';
  if (inCode) html += '</code></pre>';
  return html;
}

function renderFileContentByType(path, raw) {
  const p = (path || '').toLowerCase();
  const isJsonlLike = /\.jsonl(?:-async)?(?:$|\?)/.test(p);
  // JSON: pretty print when possible
  if (p.endsWith('.json')) {
    try {
      const obj = JSON.parse(String(raw || ''));
      return { cls: 'log-content json-view', html: renderJsonWithSyntax(JSON.stringify(obj, null, 2)) };
    } catch (_) {
      return { cls: 'log-content json-view', html: renderJsonWithSyntax(String(raw || '')) };
    }
  }
  // JSONL: pretty print line-by-line
  if (isJsonlLike) {
    return { cls: 'log-content', html: renderJsonlViewer(raw) };
  }
  // Markdown: lightweight HTML rendering
  if (p.endsWith('.md')) {
    return { cls: 'log-content md-view', html: markdownToHtml(String(raw || '')) };
  }
  return { cls: 'log-content', html: renderLogWithHighlights(String(raw || '')) };
}

function fmtSize(bytes) {
  if (bytes < 1024) return bytes + 'B';
  if (bytes < 1024*1024) return (bytes/1024).toFixed(0) + 'K';
  return (bytes/1024/1024).toFixed(1) + 'M';
}

async function viewFile(path, force) {
  force = !!force;
  _currentFilePath = path;
  _currentRemotePath = path;
  _currentResolvedPath = path;
  _currentSource = null;
  document.getElementById('content-path').textContent = path;
  document.getElementById('content-source').textContent = 'source: loading';
  document.getElementById('content-source').className = 'source-pill';
  document.getElementById('modal-content').className = 'log-loading';
  document.getElementById('modal-content').textContent = 'Loading…';

  // JSONL files: use lazy index instead of loading full content.
  const isJsonl = /\.jsonl(?:-async)?$/i.test(path);
  if (isJsonl) {
    try {
      const res = await fetch(`/api/jsonl_index/${_exCluster}/${_exJobId}?path=${encodeURIComponent(path)}&mode=${jsonlMode}&limit=${jsonlLimit}`);
      const data = await res.json();
      const el = document.getElementById('modal-content');
      if (data.status !== 'ok') {
        el.className = 'log-content';
        el.textContent = data.error || 'Failed to load index';
      } else {
        el.className = 'log-content';
        el.innerHTML = renderJsonlLazyViewer(data, path);
        _currentSource = data.source || 'ssh';
      }
      const sourceEl = document.getElementById('content-source');
      sourceEl.textContent = `source: ${data.source || 'unknown'}`;
      sourceEl.className = `source-pill ${data.source || ''}`;
    } catch (e) {
      document.getElementById('modal-content').textContent = 'Failed: ' + e;
    }
    return;
  }

  try {
    const res = await fetch(`/api/log/${_exCluster}/${_exJobId}?path=${encodeURIComponent(path)}&lines=300&force=${force ? 1 : 0}`);
    const data = await res.json();
    const el = document.getElementById('modal-content');
    const raw = (data.status === 'ok' ? data.content : data.error) || '(empty)';
    const rendered = renderFileContentByType(path, raw);
    el.className = rendered.cls;
    el.innerHTML = rendered.html;
    _currentSource = data.source || 'ssh';
    _currentResolvedPath = data.resolved_path || path;
    const sourceEl = document.getElementById('content-source');
    sourceEl.textContent = `source: ${_currentSource}`;
    sourceEl.className = `source-pill ${_currentSource}`;
    if (_currentResolvedPath && _currentResolvedPath !== _currentRemotePath) {
      document.getElementById('content-path').textContent = `${path}  ->  ${_currentResolvedPath}`;
    }
    el.parentElement.scrollTop = el.parentElement.scrollHeight;
  } catch (e) {
    const el = document.getElementById('modal-content');
    el.className = 'log-content';
    el.innerHTML = renderLogWithHighlights('Failed to load file.');
    const sourceEl = document.getElementById('content-source');
    sourceEl.textContent = 'source: error';
    sourceEl.className = 'source-pill ssh';
  }
}

async function reloadCurrentFile() {
  if (_currentFilePath) {
    await viewFile(_currentFilePath, true);
  } else if (_exCluster && _exJobId) {
    await openLog(_exCluster, _exJobId, null, true);
  }
}

async function copyCurrentFileWithPath() {
  const path = _currentRemotePath || '';
  const resolvedPath = _currentResolvedPath || path;
  const source = _currentSource || 'unknown';
  const contentEl = document.getElementById('modal-content');
  const content = contentEl ? contentEl.textContent : '';
  if (!_currentFilePath || !path || !content || content.includes('select a file')) {
    toast('No file content to copy', 'error');
    return;
  }
  const payload = `Path: ${path}\nSource: ${source}\nResolvedPath: ${resolvedPath}\n\n${content}`;
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(payload);
      toast('Copied path + content');
      return;
    }
    throw new Error('Clipboard API unavailable');
  } catch (e) {
    // Fallback for browsers/contexts where Clipboard API is blocked.
    try {
      const ta = document.createElement('textarea');
      ta.value = payload;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.top = '-10000px';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      const ok = document.execCommand('copy');
      document.body.removeChild(ta);
      if (ok) {
        toast('Copied path + content');
      } else {
        toast('Clipboard copy failed (browser blocked copy)', 'error');
      }
    } catch (_) {
      toast('Clipboard copy failed', 'error');
    }
  }
}

function closeModal(e) {
  if (e.target === document.getElementById('modal-overlay')) closeModalDirect();
}
function closeModalDirect() {
  document.getElementById('modal-overlay').classList.remove('open');
}
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') {
    closeModalDirect();
    closeStatsDirect();
    closeSettingsModal();
  }
});

// ── Horizontal resize for explorer tree pane ──
function setupTreeResizer() {
  const splitter = document.getElementById('tree-splitter');
  const pane = document.getElementById('tree-pane');
  const modal = document.querySelector('.modal');
  if (!splitter || !pane || !modal) return;

  splitter.addEventListener('mousedown', (e) => {
    _isResizingTree = true;
    e.preventDefault();
  });

  window.addEventListener('mousemove', (e) => {
    if (!_isResizingTree) return;
    const rect = modal.getBoundingClientRect();
    const minW = 180;
    const maxW = Math.max(420, rect.width * 0.65);
    let next = e.clientX - rect.left;
    if (next < minW) next = minW;
    if (next > maxW) next = maxW;
    pane.style.width = `${next}px`;
  });

  window.addEventListener('mouseup', () => {
    _isResizingTree = false;
  });
}

