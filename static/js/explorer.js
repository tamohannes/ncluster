// ── File Explorer ──
let _expCluster = null, _expJobId = null, _expPath = null;
let _expPage = 0, _expTotalPages = 1, _expTotalLines = 0;
let _expView = 'formatted';
let _expRawContent = '';

function openExplorer(cluster, jobId, path, filename) {
  _expCluster = cluster;
  _expJobId = jobId;
  _expPath = path;
  _expPage = 0;
  _expView = 'formatted';
  closeModalDirect();

  document.getElementById('exp-filename').textContent = filename || path.split('/').pop();
  document.getElementById('exp-cluster').textContent = cluster;
  document.getElementById('exp-tab-formatted').classList.add('active');
  document.getElementById('exp-tab-raw').classList.remove('active');

  document.getElementById('live-view').classList.add('hidden');
  document.getElementById('history-view').classList.remove('active');
  document.getElementById('project-view').classList.remove('active');
  document.getElementById('explorer-page').classList.add('open');

  const isJsonl = /\.jsonl(?:-async)?$/i.test(path);
  if (isJsonl && _expView === 'formatted') {
    _loadExplorerJsonl();
  } else {
    _loadExplorerPage(0);
  }
}

function closeExplorer() {
  document.getElementById('explorer-page').classList.remove('open');
  showTab(currentTab);
}

function setExpView(mode) {
  _expView = mode;
  document.getElementById('exp-tab-formatted').classList.toggle('active', mode === 'formatted');
  document.getElementById('exp-tab-raw').classList.toggle('active', mode === 'raw');

  const isJsonl = /\.jsonl(?:-async)?$/i.test(_expPath);
  if (isJsonl && mode === 'formatted') {
    _loadExplorerJsonl();
  } else if (mode === 'raw' && _expRawContent) {
    _renderExpRaw(_expRawContent);
  } else {
    _loadExplorerPage(_expPage);
  }
}

async function _loadExplorerPage(page) {
  const el = document.getElementById('exp-content');
  el.className = 'explorer-content';
  el.innerHTML = '<div class="log-loading">Loading…</div>';

  try {
    const res = await fetch(`/api/log_full/${_expCluster}/${_expJobId}?path=${encodeURIComponent(_expPath)}&page=${page}&page_size=500`);
    const d = await res.json();
    if (d.status !== 'ok') {
      el.innerHTML = `<div class="log-loading" style="color:var(--red)">${d.error || 'Failed'}</div>`;
      return;
    }
    _expPage = d.page;
    _expTotalPages = d.total_pages;
    _expTotalLines = d.total_lines;
    _expRawContent = d.content;

    document.getElementById('exp-source').textContent = `source: ${d.source}`;
    document.getElementById('exp-source').className = `source-pill ${d.source}`;

    if (_expView === 'raw') {
      _renderExpRaw(d.content);
    } else {
      const rendered = renderFileContentByType(_expPath, d.content);
      el.className = 'explorer-content';
      const wrapper = document.createElement('div');
      wrapper.className = rendered.cls;
      wrapper.innerHTML = rendered.html;
      el.innerHTML = '';
      el.appendChild(wrapper);
    }
    _renderExpPagination();
  } catch (e) {
    el.innerHTML = `<div class="log-loading" style="color:var(--red)">Failed: ${e}</div>`;
  }
}

async function _loadExplorerJsonl() {
  const el = document.getElementById('exp-content');
  el.className = 'explorer-content';
  el.innerHTML = '<div class="log-loading">Loading JSONL index…</div>';

  try {
    const res = await fetch(`/api/jsonl_index/${_expCluster}/${_expJobId}?path=${encodeURIComponent(_expPath)}&mode=all&limit=0`);
    const data = await res.json();
    if (data.status !== 'ok') {
      el.innerHTML = `<div class="log-loading" style="color:var(--red)">${data.error || 'Failed'}</div>`;
      return;
    }
    document.getElementById('exp-source').textContent = `source: ${data.source}`;
    document.getElementById('exp-source').className = `source-pill ${data.source}`;
    el.innerHTML = renderJsonlLazyViewer(data, _expPath);
    document.getElementById('exp-pagination').innerHTML =
      `<span>${data.total || data.count} records</span>`;
  } catch (e) {
    el.innerHTML = `<div class="log-loading" style="color:var(--red)">Failed: ${e}</div>`;
  }
}

function _renderExpRaw(content) {
  const el = document.getElementById('exp-content');
  el.className = 'explorer-content';
  const lines = content.split('\n');
  const startLine = _expPage * 500 + 1;
  const gutter = lines.map((_, i) => `<div>${startLine + i}</div>`).join('');
  const code = escapeHtml(content);
  el.innerHTML = `<div class="ide-raw"><div class="ide-gutter">${gutter}</div><div class="ide-code">${code}</div></div>`;
  _renderExpPagination();
}

function _renderExpPagination() {
  const pag = document.getElementById('exp-pagination');
  if (_expTotalPages <= 1) {
    pag.innerHTML = `<span>${_expTotalLines} lines</span>`;
    return;
  }
  pag.innerHTML = `
    <button onclick="_loadExplorerPage(0)" ${_expPage === 0 ? 'disabled' : ''}>first</button>
    <button onclick="_loadExplorerPage(${_expPage - 1})" ${_expPage === 0 ? 'disabled' : ''}>← prev</button>
    <span>${_expPage + 1} / ${_expTotalPages}</span>
    <button onclick="_loadExplorerPage(${_expPage + 1})" ${_expPage >= _expTotalPages - 1 ? 'disabled' : ''}>next →</button>
    <button onclick="_loadExplorerPage(${_expTotalPages - 1})" ${_expPage >= _expTotalPages - 1 ? 'disabled' : ''}>last</button>
    <span style="font-size:10px">${_expTotalLines} lines</span>
  `;
}

function openExplorerForCurrentFile() {
  if (!_currentFilePath || !_exCluster || !_exJobId) {
    toast('No file selected', 'error');
    return;
  }
  const filename = _currentFilePath.split('/').pop();
  openExplorer(_exCluster, _exJobId, _currentFilePath, filename);
}

// Add Escape handler for explorer
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && document.getElementById('explorer-page').classList.contains('open')) {
    closeExplorer();
  }
});

