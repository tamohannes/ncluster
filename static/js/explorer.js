// ── File Explorer ──
let _expCluster = null, _expJobId = null, _expPath = null;
let _expTopPage = 0, _expBottomPage = 0, _expTotalPages = 1, _expTotalLines = 0;
let _expIsLoading = false, _expFullLoaded = false;
let _expView = 'formatted';
let _expRawContent = '';

function openExplorer(cluster, jobId, path, filename) {
  _expCluster = cluster;
  _expJobId = jobId;
  _expPath = path;
  _expTopPage = 0;
  _expBottomPage = 0;
  _expIsLoading = false;
  _expFullLoaded = false;
  _expView = 'formatted';
  _expRawContent = '';
  closeModalDirect();

  document.getElementById('exp-filename').textContent = filename || path.split('/').pop();
  document.getElementById('exp-cluster').textContent = cluster;
  document.getElementById('exp-tab-formatted').classList.add('active');
  document.getElementById('exp-tab-raw').classList.remove('active');

  document.getElementById('live-view').classList.add('hidden');
  document.getElementById('history-view').classList.remove('active');
  document.getElementById('project-view').classList.remove('active');
  document.getElementById('explorer-page').classList.add('open');

  if (typeof _setHash === 'function') _setHash(`#/explorer/${encodeURIComponent(cluster)}/${encodeURIComponent(jobId)}/${encodeURIComponent(path)}`);

  const isJsonl = /\.jsonl(?:-async)?$/i.test(path);
  if (isJsonl && _expView === 'formatted') {
    _loadExplorerJsonl();
  } else {
    _loadExplorerPage(0);
  }

  _loadExplorerTree();
}

async function _loadExplorerTree() {
  const tree = document.getElementById('exp-tree-pane');
  tree.innerHTML = '<div class="tree-loading">loading…</div>';
  
  _exCluster = _expCluster;
  _exJobId = _expJobId;

  try {
    const res = await fetchWithTimeout(`/api/log_files/${_expCluster}/${_expJobId}?include_first=0`);
    const data = await res.json();
    
    if (data.error) {
      tree.innerHTML = `<div class="tree-loading" style="color:var(--muted)">${data.error}</div>`;
      return;
    }
    
    tree.innerHTML = '';
    const files = (data.files || []).filter(f => f.path);
    const dirs  = data.dirs || [];
    
    const onFileClick = (path) => {
      _expPath = path;
      _expPage = 0;
      _expRawContent = '';
      document.getElementById('exp-filename').textContent = path.split('/').pop();
      if (typeof _setHash === 'function') _setHash(`#/explorer/${encodeURIComponent(_expCluster)}/${encodeURIComponent(_expJobId)}/${encodeURIComponent(path)}`);
      setExpView('formatted');
    };
    
    if (files.length) {
      tree.appendChild(makeTreeSection('📋 logs', files.map(f => ({
        name: f.label, path: f.path, is_dir: false,
        icon: f.label.includes('error') || f.label.includes('stderr') ? '⚠' : '📄',
        job_id: _extractJobId(f.path.split('/').pop() || ''),
      })), true, null, onFileClick));
    }
    for (const dir of dirs) {
      tree.appendChild(makeTreeSection('📁 ' + dir.label, [], false, dir.path, onFileClick));
    }
    if (!files.length && dirs.length) {
      await expandDir(dirs[0].path, tree.querySelector('.tree-items'), 0, onFileClick);
    }
  } catch (e) {
    tree.innerHTML = '<div class="tree-loading" style="color:var(--muted)">unavailable</div>';
  }
}

let _isResizingExpTree = false;
function setupExpTreeResizer() {
  const splitter = document.getElementById('exp-tree-splitter');
  const pane = document.getElementById('exp-tree-pane');
  if (!splitter || !pane) return;

  splitter.addEventListener('mousedown', (e) => {
    _isResizingExpTree = true;
    e.preventDefault();
  });

  window.addEventListener('mousemove', (e) => {
    if (!_isResizingExpTree) return;
    const rect = document.getElementById('explorer-page').getBoundingClientRect();
    const minW = 180;
    const maxW = Math.max(420, rect.width * 0.65);
    let next = e.clientX - rect.left;
    if (next < minW) next = minW;
    if (next > maxW) next = maxW;
    pane.style.width = `${next}px`;
  });

  window.addEventListener('mouseup', () => {
    _isResizingExpTree = false;
  });
}
document.addEventListener('DOMContentLoaded', setupExpTreeResizer);

function closeExplorer() {
  document.getElementById('explorer-page').classList.remove('open');
  showTab(currentTab);
}

// Explorer state is now persisted via URL hash (#/explorer/cluster/jobId/path)

function setExpView(mode) {
  _expView = mode;
  document.getElementById('exp-tab-formatted').classList.toggle('active', mode === 'formatted');
  document.getElementById('exp-tab-raw').classList.toggle('active', mode === 'raw');

  const isJsonl = /\.jsonl(?:-async)?$/i.test(_expPath);
  if (isJsonl && mode === 'formatted') {
    _loadExplorerJsonl();
  } else if (_expRawContent) {
    if (mode === 'raw') {
      _renderExpRaw(_expRawContent);
    } else {
      const rendered = renderFileContentByType(_expPath, _expRawContent);
      const el = document.getElementById('exp-content');
      el.className = 'explorer-content';
      el.innerHTML = '';
      const wrapper = document.createElement('div');
      wrapper.className = rendered.cls;
      wrapper.innerHTML = rendered.html;
      el.appendChild(wrapper);
    }
  } else {
    _loadExplorerPage(-1);
  }
}

async function _loadExplorerPage(targetPage, prepend = false, loadFull = false) {
  if (_expIsLoading) return;
  _expIsLoading = true;

  const el = document.getElementById('exp-content');
  if (!prepend) {
    el.className = 'explorer-content';
    el.innerHTML = '<div class="log-loading">Loading…</div>';
  } else {
    const loader = document.createElement('div');
    loader.className = 'log-loading';
    loader.id = 'exp-prepend-loader';
    loader.textContent = 'Loading earlier part…';
    el.insertBefore(loader, el.firstChild);
  }

  let pageSize = loadFull ? 1000000 : 500;
  let reqPage = targetPage < 0 ? 999999 : targetPage;

  try {
    const res = await fetch(`/api/log_full/${_expCluster}/${_expJobId}?path=${encodeURIComponent(_expPath)}&page=${reqPage}&page_size=${pageSize}`);
    const d = await res.json();
    if (d.status !== 'ok') {
      if (!prepend) el.innerHTML = `<div class="log-loading" style="color:var(--red)">${d.error || 'Failed'}</div>`;
      else document.getElementById('exp-prepend-loader')?.remove();
      _expIsLoading = false;
      return;
    }
    
    if (loadFull) {
        _expTopPage = 0;
        _expBottomPage = d.total_pages - 1;
        _expFullLoaded = true;
        _expRawContent = d.content;
    } else if (!prepend) {
        _expTopPage = d.page;
        _expBottomPage = d.page;
        _expFullLoaded = d.total_pages <= 1;
        _expRawContent = d.content;
    } else {
        _expTopPage = d.page;
        _expRawContent = d.content + '\n' + _expRawContent;
        if (_expTopPage === 0) _expFullLoaded = true;
    }

    _expTotalPages = d.total_pages;
    _expTotalLines = d.total_lines;

    document.getElementById('exp-source').textContent = `source: ${d.source}`;
    document.getElementById('exp-source').className = `source-pill ${d.source}`;

    const oldScrollHeight = el.scrollHeight;
    const oldScrollTop = el.scrollTop;

    if (_expView === 'raw') {
      _renderExpRaw(_expRawContent);
    } else {
      if (!prepend || loadFull) {
        const rendered = renderFileContentByType(_expPath, _expRawContent);
        el.className = 'explorer-content';
        el.innerHTML = '';
        const wrapper = document.createElement('div');
        wrapper.className = rendered.cls;
        wrapper.innerHTML = rendered.html;
        el.appendChild(wrapper);
      } else {
        document.getElementById('exp-prepend-loader')?.remove();
        const rendered = renderFileContentByType(_expPath, _expRawContent);
        el.className = 'explorer-content';
        el.innerHTML = '';
        const wrapper = document.createElement('div');
        wrapper.className = rendered.cls;
        wrapper.innerHTML = rendered.html;
        el.appendChild(wrapper);
      }
    }

    if (prepend && !loadFull) {
      el.scrollTop = oldScrollTop + (el.scrollHeight - oldScrollHeight);
    } else if (!prepend && !loadFull && targetPage < 0) {
      // initial load from bottom
      el.scrollTop = el.scrollHeight;
    }

    _renderExpToolbar();
    
    if (!loadFull && !_expFullLoaded) {
        _setupExplorerScroll();
    } else {
        el.onscroll = null;
    }
  } catch (e) {
    if (!prepend) el.innerHTML = `<div class="log-loading" style="color:var(--red)">Failed: ${e}</div>`;
    else document.getElementById('exp-prepend-loader')?.remove();
  }
  
  _expIsLoading = false;
}

function _setupExplorerScroll() {
  const el = document.getElementById('exp-content');
  el.onscroll = () => {
    if (_expIsLoading || _expFullLoaded || _expTopPage <= 0) return;
    if (el.scrollTop < 100) {
      _loadExplorerPage(_expTopPage - 1, true, false);
    }
  };
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
    el.innerHTML = renderJsonlLazyViewer(data, _expPath, {
      containerId: 'exp-content',
      cluster: _expCluster,
      jobId: _expJobId,
    });
    document.getElementById('exp-pagination').innerHTML =
      `<span style="margin-right:8px;font-size:10px">${data.total || data.count} records</span>`;
  } catch (e) {
    el.innerHTML = `<div class="log-loading" style="color:var(--red)">Failed: ${e}</div>`;
  }
}

function _renderExpRaw(content) {
  const el = document.getElementById('exp-content');
  el.className = 'explorer-content';
  const lines = content.split('\n');
  const startLine = _expTopPage * 500 + 1;
  const gutter = lines.map((_, i) => `<div>${startLine + i}</div>`).join('');
  const code = escapeHtml(content);
  el.innerHTML = `<div class="ide-raw"><div class="ide-gutter">${gutter}</div><div class="ide-code">${code}</div></div>`;
  _renderExpToolbar();
}

function _renderExpToolbar() {
  const pag = document.getElementById('exp-pagination');
  if (_expTotalPages <= 1 || _expFullLoaded) {
    pag.innerHTML = `<span>${_expTotalLines} lines</span>`;
    return;
  }
  pag.innerHTML = `
    <span style="margin-right:8px;font-size:10px">${_expTotalLines} lines</span>
    <button onclick="_loadExplorerPage(0, false, true)" title="Load entire file from the beginning" class="btn">load full file</button>
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

