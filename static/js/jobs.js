// ── Error banner ─────────────────────────────────────────────────────────────
let _bannerErrors = {};
let _bannerTimer = null;

function _setErrorBanner(key, msg) {
  _bannerErrors[key] = msg;
  _renderErrorBanner();
}

function _clearErrorBannerKey(key) {
  delete _bannerErrors[key];
  _renderErrorBanner();
}

function _clearErrorBanner() {
  _bannerErrors = {};
  _renderErrorBanner();
}

function _renderErrorBanner() {
  const el = document.getElementById('error-banner');
  const msgEl = document.getElementById('error-banner-msg');
  if (!el || !msgEl) return;
  const msgs = Object.values(_bannerErrors);
  if (!msgs.length) {
    el.classList.add('hidden');
    return;
  }
  msgEl.textContent = msgs.length === 1 ? msgs[0] : msgs.join(' · ');
  el.classList.remove('hidden');
}


function _persistAllData() {
  return;
}


// ── App tabs ─────────────────────────────────────────────────────────────────
const _tabIcons = {
  live:    '⚡', history: '⏱',
  logbook: '📓', project: '📁',
  clusters: '🖥',
};
let _appTabs = [{ id: 1, type: 'live', label: 'Live', project: null }];
let _activeTabId = 1;
let _nextTabId = 2;

// ── Hash-based URL routing ──────────────────────────────────────────
let _hashNavigating = false;

function _setHash(h) {
  _hashNavigating = true;
  location.hash = h;
  setTimeout(() => { _hashNavigating = false; }, 0);
}

function _hashForView(type, extra) {
  if (type === 'live') return '#/live';
  if (type === 'history') return '#/history';
  if (type === 'clusters') return '#/compute';
  if (type === 'logbook') {
    const proj = extra || (typeof _lbProject !== 'undefined' ? _lbProject : '');
    return proj ? `#/logbook/${proj}` : '#/logbook';
  }
  if (type === 'project' && extra) return `#/project/${extra}`;
  return '#/live';
}

function _onHashChange() {
  if (_hashNavigating) return;
  const raw = location.hash.replace(/^#\/?/, '');
  if (!raw) return;
  const parts = raw.split('/');
  const view = parts[0];

  if (view === 'live') showTab('live');
  else if (view === 'history') showTab('history');
  else if (view === 'compute') showTab('clusters');
  else if (view === 'logbook') {
    if (parts[1] && typeof _lbProject !== 'undefined') _lbProject = decodeURIComponent(parts[1]);
    showTab('logbook');
  }
  else if (view === 'project' && parts[1]) openProject(decodeURIComponent(parts[1]));
  else if (view === 'explorer' && parts.length >= 4) {
    const cluster = decodeURIComponent(parts[1]);
    const jobId = decodeURIComponent(parts[2]);
    const path = decodeURIComponent(parts.slice(3).join('/'));
    openExplorer(cluster, jobId, path, path.split('/').pop());
  }
}

window.addEventListener('hashchange', _onHashChange);

function _activateView(tab) {
  currentTab = tab;
  document.getElementById('live-view').classList.toggle('hidden', tab !== 'live');
  document.getElementById('history-view').classList.toggle('active', tab === 'history');
  document.getElementById('project-view').classList.toggle('active', tab === 'project');
  document.getElementById('logbook-view').classList.toggle('active', tab === 'logbook');
  document.getElementById('clusters-view').classList.toggle('active', tab === 'clusters');
  document.getElementById('explorer-page').classList.remove('open');
  document.getElementById('tab-live').classList.toggle('active', tab === 'live');
  document.getElementById('tab-history').classList.toggle('active', tab === 'history');
  document.getElementById('tab-logbook').classList.toggle('active', tab === 'logbook');
  document.getElementById('tab-clusters').classList.toggle('active', tab === 'clusters');
  if (tab !== 'project') {
    document.querySelectorAll('.nav-project-btn').forEach(b => b.classList.remove('active'));
  }
  if (tab === 'history') loadHistory();
  if (tab === 'logbook') initLogbookPage();
  if (tab === 'clusters') {
    if (_partitionData) _renderAvailTable();
    initClustersPage();
  }
}

function showTab(tab) {
  _activateView(tab);
  const at = _appTabs.find(t => t.id === _activeTabId);
  if (at) {
    at.type = tab;
    at.label = { live: 'Live', history: 'History', logbook: 'Logbook', clusters: 'Compute' }[tab] || tab;
    at.project = null;
    if (tab === 'logbook' && typeof _lbProject !== 'undefined' && _lbProject) {
      at.lbProject = _lbProject;
    }
  }
  _renderAppTabs();
  _persistTabs();
  _setHash(_hashForView(tab));
}

function switchAppTab(id) {
  const t = _appTabs.find(t => t.id === id);
  if (!t) return;
  _activeTabId = id;
  if (t.type === 'project' && t.project) {
    _activateView('project');
    openProject(t.project, true);
  } else if (t.type === 'logbook') {
    if (t.lbProject) _lbProject = t.lbProject;
    _activateView('logbook');
    if (t.lbEntryId) setTimeout(() => openLogbookEntry(t.lbEntryId), 300);
  } else {
    _activateView(t.type);
  }
  _renderAppTabs();
  _persistTabs();
  _setHash(_hashForView(t.type, t.project || t.lbProject));
}

function _updateActiveTabExtra(fields) {
  const at = _appTabs.find(t => t.id === _activeTabId);
  if (at) Object.assign(at, fields);
  _persistTabs();
}

function addAppTab(type, label, project) {
  const t = {
    id: _nextTabId++,
    type: type || 'live',
    label: label || 'Live',
    project: project || null,
  };
  _appTabs.push(t);
  _activeTabId = t.id;
  if (t.type === 'project' && t.project) {
    _activateView('project');
    openProject(t.project, true);
  } else {
    _activateView(t.type);
  }
  _renderAppTabs();
  _persistTabs();
}

function closeAppTab(id) {
  if (_appTabs.length <= 1) return;
  const idx = _appTabs.findIndex(t => t.id === id);
  if (idx === -1) return;
  _appTabs.splice(idx, 1);
  if (_activeTabId === id) {
    const next = _appTabs[Math.min(idx, _appTabs.length - 1)];
    _activeTabId = next.id;
    switchAppTab(next.id);
  }
  _renderAppTabs();
  _persistTabs();
}

function cycleAppTab(dir) {
  if (_appTabs.length <= 1) return;
  const idx = _appTabs.findIndex(t => t.id === _activeTabId);
  const next = (idx + dir + _appTabs.length) % _appTabs.length;
  switchAppTab(_appTabs[next].id);
}

function _renderAppTabs() {
  const el = document.getElementById('topbar-tabs');
  if (!el) return;
  el.innerHTML = _appTabs.map(t => {
    const icon = _tabIcons[t.type] || '📄';
    const active = t.id === _activeTabId ? ' active' : '';
    const closable = _appTabs.length > 1
      ? `<button class="topbar-tab-close" onclick="event.stopPropagation();closeAppTab(${t.id})" title="Close tab">×</button>`
      : '';
    return `<div class="topbar-tab${active}" onclick="switchAppTab(${t.id})" title="${t.label}">
      <span class="topbar-tab-icon">${icon}</span>
      <span class="topbar-tab-label">${t.label}</span>
      ${closable}
    </div>`;
  }).join('');
}

function _persistTabs() {
  try {
    localStorage.setItem('clausius.appTabs', JSON.stringify(_appTabs));
    localStorage.setItem('clausius.activeTabId', String(_activeTabId));
    localStorage.setItem('clausius.nextTabId', String(_nextTabId));
  } catch (_) {}
}

function _restoreTabs() {
  const validTypes = new Set(['live', 'history', 'logbook', 'project', 'clusters']);
  try {
    const raw = localStorage.getItem('clausius.appTabs');
    if (raw) {
      const tabs = JSON.parse(raw);
      if (Array.isArray(tabs) && tabs.length) {
        const clean = tabs.filter(t => t && t.id && validTypes.has(t.type));
        if (!clean.length) return false;
        _appTabs = clean;
        _activeTabId = parseInt(localStorage.getItem('clausius.activeTabId') || '1', 10);
        _nextTabId = parseInt(localStorage.getItem('clausius.nextTabId') || '2', 10);
        if (!_appTabs.find(t => t.id === _activeTabId)) _activeTabId = _appTabs[0].id;
        const at = _appTabs.find(t => t.id === _activeTabId) || _appTabs[0];
        _activeTabId = at.id;
        switchAppTab(at.id);
        return true;
      }
    }
  } catch (_) {}
  return false;
}

function applySidebarState() {
  const nav = document.getElementById('side-nav');
  const btn = document.getElementById('nav-toggle');
  const splitter = document.getElementById('nav-splitter');
  if (!nav || !btn) return;
  nav.classList.toggle('collapsed', navCollapsed);
  if (navCollapsed) {
    nav.style.width = '0px';
  } else {
    try {
      const saved = parseInt(localStorage.getItem('clausius.navWidth') || '', 10);
      const minW = 230;
      const maxW = Math.min(640, Math.floor(window.innerWidth * 0.55));
      const w = Number.isNaN(saved) ? 280 : Math.min(maxW, Math.max(minW, saved));
      nav.style.width = `${w}px`;
    } catch (_) {
      nav.style.width = '280px';
    }
  }
  if (splitter) splitter.style.display = navCollapsed ? 'none' : '';
  btn.classList.toggle('sidebar-open', !navCollapsed);
}

function toggleSidebar() {
  navCollapsed = !navCollapsed;
  try { localStorage.setItem('clausius.navCollapsed', navCollapsed ? '1' : '0'); } catch (_) {}
  applySidebarState();
}

function navClick(event, type, label, project) {
  if (event && (event.metaKey || event.ctrlKey)) {
    addAppTab(type, label, project || null);
  } else {
    if (type === 'project' && project) openProject(project);
    else showTab(type);
  }
}

document.addEventListener('keydown', e => {
  if (typeof _recordingShortcutId !== 'undefined' && _recordingShortcutId) return;
  if (matchesShortcut(e, 'refreshLive') && currentTab === 'live') {
    e.preventDefault();
    toast('Refreshing live data…');
    _forceRefreshAll();
    return;
  }
  if (matchesShortcut(e, 'toggleSidebar')) { e.preventDefault(); toggleSidebar(); return; }
  if (matchesShortcut(e, 'openSpotlight')) { e.preventDefault(); if (typeof openSpotlight === 'function') openSpotlight(); return; }
  if (matchesShortcut(e, 'closeTab')) { e.preventDefault(); closeAppTab(_activeTabId); return; }
  if (matchesShortcut(e, 'prevTab')) { e.preventDefault(); cycleAppTab(-1); return; }
  if (matchesShortcut(e, 'nextTab')) { e.preventDefault(); cycleAppTab(1); return; }
  if (matchesShortcut(e, 'exportEntry')) { e.preventDefault(); if (typeof exportEntryHtml === 'function') exportEntryHtml(); return; }
  if (matchesShortcut(e, 'goBack') && currentTab === 'logbook') { e.preventDefault(); if (typeof _lbGoBack === 'function') _lbGoBack(); return; }
});

function setupSidebarResizer() {
  const splitter = document.getElementById('nav-splitter');
  const nav = document.getElementById('side-nav');
  if (!splitter || !nav) return;

  splitter.addEventListener('mousedown', (e) => {
    if (navCollapsed) return;
    _isResizingNav = true;
    nav.style.transition = 'none';
    e.preventDefault();
  });

  window.addEventListener('mousemove', (e) => {
    if (!_isResizingNav || navCollapsed) return;
    const minW = 230;
    const maxW = Math.min(640, Math.floor(window.innerWidth * 0.55));
    let next = e.clientX;
    if (next < minW) next = minW;
    if (next > maxW) next = maxW;
    nav.style.width = `${next}px`;
  });

  window.addEventListener('mouseup', () => {
    if (_isResizingNav) {
      nav.style.transition = '';
      try { localStorage.setItem('clausius.navWidth', nav.style.width.replace('px', '')); } catch (_) {}
    }
    _isResizingNav = false;
  });

  try {
    const saved = parseInt(localStorage.getItem('clausius.navWidth') || '', 10);
    if (!Number.isNaN(saved)) {
      const minW = 230;
      const maxW = Math.min(640, Math.floor(window.innerWidth * 0.55));
      const w = Math.min(maxW, Math.max(minW, saved));
      nav.style.width = `${w}px`;
    }
  } catch (_) {}

  window.addEventListener('resize', () => {
    if (!navCollapsed) {
      try {
        const saved = parseInt(localStorage.getItem('clausius.navWidth') || '', 10);
        const minW = 230;
        const maxW = Math.min(640, Math.floor(window.innerWidth * 0.55));
        const w = Number.isNaN(saved) ? 320 : Math.min(maxW, Math.max(minW, saved));
        nav.style.width = `${w}px`;
      } catch (_) {}
    }
  });
}

// ── Summary ──
function updateSummary(data) {
  let running = 0, pending = 0, failed = 0, completed = 0, reach = 0, totalGpus = 0, mounted = 0;
  for (const [name, d] of Object.entries(data)) {
    if (d.status === 'ok') reach++;
    if (d.mount && d.mount.mounted) mounted++;
    for (const j of d.jobs || []) {
      const s = (j.state || '').toUpperCase();
      if (s === 'RUNNING' || s === 'COMPLETING') {
        running++;
        const gm = (j.gres || '').match(/gpu[^:]*:(?:[^:]+:)?(\d+)/);
        if (gm) totalGpus += (parseInt(gm[1]) || 0) * (parseInt(j.nodes) || 1);
      }
      else if (s === 'PENDING') pending++;
      else if (s.includes('FAIL')) {
        if (isUnneededBackup(j, d.jobs || [])) completed++;
        else failed++;
      }
      else if (s.startsWith('COMPLETED')) completed++;
    }
  }
  document.getElementById('s-running').textContent = running;
  document.getElementById('s-pending').textContent = pending;
  document.getElementById('s-failed').textContent = failed;
  document.getElementById('s-completed').textContent = completed;
  document.getElementById('s-gpus').textContent = totalGpus;
  document.getElementById('s-clusters').textContent = `${reach}/${Object.keys(CLUSTERS).length}`;
  document.getElementById('s-mounted').textContent = `${mounted}/${reach > 0 ? reach - (data.local ? 1 : 0) : 0}`;
  const ts = document.getElementById('topbar-stat');
  if (ts) {
    const parts = [];
    if (running) parts.push(`${running} running`);
    if (pending) parts.push(`${pending} pending`);
    if (failed) parts.push(`${failed} failed`);
    ts.textContent = parts.join(' · ') || '—';
  }
}

function renderMountPanel(data) {
  const panel = document.getElementById('mount-panel');
  if (!panel) return;
  const items = Object.keys(CLUSTERS)
    .filter(name => name !== 'local')
    .sort((a, b) => {
      const aErr = ((data[a] || {}).status === 'error') ? 1 : 0;
      const bErr = ((data[b] || {}).status === 'error') ? 1 : 0;
      if (aErr !== bErr) return aErr - bErr; // unreachable clusters go last
      return a.localeCompare(b);
    })
    .map((name) => {
      const d = data[name] || {};
      const m = d.mount || {};
      const mounted = !!m.mounted;
      const root = m.root || '';
      return `<div class="mount-item">
        <div class="mount-head">
          <span class="mount-name">${name}</span>
          <span class="mount-state ${mounted ? 'ok' : 'off'}" title="${root.replace(/"/g, '&quot;')}">${mounted ? 'mounted' : 'ssh-only'}</span>
        </div>
        <div class="mount-actions">
          <button class="icon-btn" onclick="checkMountStatus('${name}')">check</button>
          ${mounted
            ? `<button class="icon-btn" onclick="unmountCluster('${name}')">unmount</button>`
            : `<button class="icon-btn" onclick="mountCluster('${name}')">mount</button>`}
          <button class="icon-btn" onclick="remountCluster('${name}')">restart</button>
        </div>
      </div>`;
    }).join('');
  panel.innerHTML = items || '<div class="no-jobs" style="padding:8px">no clusters</div>';
}

function computeRefreshIntervalSec(data) {
  for (const [, d] of Object.entries(data || {})) {
    for (const j of d.jobs || []) {
      if (_isActivelyCancelableState(j.state)) return 30;
    }
  }
  return 60;
}

function _isActivelyCancelableState(state) {
  const s = (state || '').toUpperCase();
  return s === 'RUNNING' || s === 'COMPLETING' || s === 'PENDING' || s === 'SUBMITTING';
}

// ── Cluster card rendering ──
function renderCard(name, data) {
  const info = CLUSTERS[name];
  const jobs = data.jobs || [];
  const isErr = data.status === 'error';
  const hasRunning = jobs.some(j => j.state === 'RUNNING');
  const isEmpty = !isErr && jobs.length === 0;

  let cardClass = 'card';
  if (hasRunning) cardClass += ' has-running';
  if (isEmpty) cardClass += ' is-empty';

  const poller = data.poller || {};
  const pollerState = poller.state || 'healthy';
  const failCount = poller.failure_count || 0;
  const staleness = poller.staleness_sec;
  const isStale = staleness != null && staleness > 60;
  const isRetrying = pollerState === 'backoff' || pollerState === 'retrying';
  const statusClass = (isErr && failCount >= 3 && !jobs.length) ? 'error' : isStale ? 'stale' : 'ok';
  const runCount = jobs.filter(j => (j.state || '').toUpperCase() === 'RUNNING' || (j.state || '').toUpperCase() === 'COMPLETING').length;
  const pendCount = jobs.filter(j => (j.state || '').toUpperCase() === 'PENDING').length;
  const jobParts = [];
  if (runCount) jobParts.push(`${runCount} running`);
  if (pendCount) jobParts.push(`${pendCount} pending`);
  if (runCount || pendCount) {
    let cardGpuTotal = 0;
    for (const j of jobs) {
      const s = (j.state || '').toUpperCase();
      if (s === 'RUNNING' || s === 'COMPLETING' || s === 'PENDING' || s === 'SUBMITTING') {
        cardGpuTotal += jobGpuCount(j.nodes, j.gres);
      }
    }
    jobParts.push(`${cardGpuTotal} GPU${cardGpuTotal !== 1 ? 's' : ''}`);
  }
  let jobCountText;
  if (isErr && failCount >= 3 && !jobs.length) {
    jobCountText = 'unreachable';
  } else {
    jobCountText = jobParts.length ? jobParts.join(' · ') : 'no jobs';
    if (isStale) {
      const mins = Math.round(staleness / 60);
      jobCountText += ` · data ${mins}m old`;
    }
    if (isRetrying && failCount > 0) {
      const nextSec = Math.round(poller.next_poll_sec || 0);
      jobCountText += ` · retrying in ${nextSec}s`;
    }
  }

  const updated = data.updated ? new Date(data.updated).toLocaleTimeString() : '';
  const freshBadge = freshnessBadgeHtml(name);
  const mount = data.mount || { mounted: false };

  let body = '';
  if (isErr) {
    body = `<div class="err-msg">⚠ ${data.error}<button class="btn" style="margin-left:10px" onclick="refreshCluster('${name}',true)">retry</button></div>`;
  } else if (jobs.length === 0) {
    body = `<div class="no-jobs">no active jobs</div>`;
  } else {
    const groupEntries = groupJobsByDependency(jobs);
    const allGroupKeys = groupEntries.map(([gk]) => gk);
    const groupKeyCounts = {};
    for (const [gk] of groupEntries) groupKeyCounts[gk] = (groupKeyCounts[gk] || 0) + 1;
    const gkHL = computeNameHighlight(allGroupKeys);

    const rows = groupEntries.map(([gk, groupJobs], gidx) => {
      const _proj = groupJobs[0]?.project || '';
      const _projColor = groupJobs[0]?.project_color || '';
      const _projEmoji = groupJobs[0]?.project_emoji || '';
      const _projBadge = _proj ? `<span class="group-project-badge">${_projEmoji ? _projEmoji + ' ' : ''}${_proj}</span>` : '';
      const rootJob = groupJobs.find(j => !(j.depends_on || []).length) || groupJobs[0];
      const rootJobId = rootJob.jobid;
      const safeGk = gk.replace(/'/g, "\\'");
      const _campaign = groupJobs[0]?.campaign || '';
      const _shadedColor = _projColor && _campaign ? campaignShade(_projColor, _campaign) : _projColor;
      const runBadgeStyle = _shadedColor ? projectBadgeStyle(_shadedColor) : '';
      const highlightedGk = highlightJobName(gk, gkHL.prefix, gkHL.suffix);
      const attemptBadge = groupKeyCounts[gk] > 1 ? runAttemptBadge(rootJob) : '';
      const runDataAttrs = name !== 'local'
        ? ` data-run-cluster="${escAttr(name)}" data-run-root="${escAttr(String(rootJobId))}"`
        : '';
      const runBadge = name !== 'local'
        ? `<span class="run-name-badge${rootJob.starred ? ' run-name-badge--starred' : ''}"${runDataAttrs}${runBadgeStyle} onclick="event.stopPropagation();openRunInfo('${name}','${rootJobId}','${safeGk}')" title="${gk.replace(/"/g, '&quot;')}">${highlightedGk}</span>`
        : highlightedGk;

      // Compute dependency depth for indentation.
      const idSet = new Set(groupJobs.map(j => j.jobid));
      const byId = {};
      for (const j of groupJobs) byId[j.jobid] = j;
      const depthMemo = {};

      // Identify backup jobs and reorder so each parent is followed by its backups.
      const { backupMap, parentOf } = buildBackupInfo(groupJobs, byId);
      const backupSet = new Set(Object.keys(parentOf));
      const ordered = [];
      for (const j of groupJobs) {
        if (backupSet.has(j.jobid)) continue;
        ordered.push(j);
        if (backupMap[j.jobid]) {
          for (const bk of backupMap[j.jobid]) ordered.push(bk);
        }
      }
      const visibleCount = groupJobs.length - backupSet.size;
      const groupId = `${name}:${rootJobId}`;
      const isGroupExpanded = _expandedGroups.has(groupId);
      const chevronCls = isGroupExpanded ? ' expanded' : '';
      const chevronHtml = `<span class="group-chevron${chevronCls}" data-group-chevron="${groupId}">&#9654;</span>`;
      const donutHtml = statusDonut(groupJobs);
      const summaryHtml = statusSummaryHtml(groupJobs, name);
      const groupLabel = `<span>${chevronHtml}${donutHtml}${runBadge}${attemptBadge}${_projBadge} ${summaryHtml}</span>`;

      const jobNames = groupJobs.map(j => j.name || '');
      const jnHL = computeNameHighlight(jobNames);

      const groupRows = ordered.map(j => {
      const gpuStr = parseGpus(j.nodes, j.gres);
      const resourceCell = gpuStr
        ? `<span style="color:var(--text);font-weight:500">${gpuStr}</span>`
        : `<span class="dim">${j.nodes || '—'}n</span>`;
      const isPinned = j._pinned;
      const st = (j.state || '').toUpperCase();
      const isActivelyCancelable = _isActivelyCancelableState(st);
      const pinKind = (isPinned && !isActivelyCancelable)
        ? (isSoftFail(j.state, j.reason) ? 'pinned-softfail-row' : isCompletedState(st) ? 'pinned-completed-row' : 'pinned-failed-row')
        : '';
      const depth = depthInGroup(j, byId, idSet, depthMemo);

      const isBackup = backupSet.has(j.jobid);
      const backupParentId = parentOf[j.jobid] || '';
      const hasBackups = backupMap[j.jobid] && backupMap[j.jobid].length > 0;
      const isExpanded = _expandedBackups.has(j.jobid);
      const backupHidden = isBackup && !_expandedBackups.has(backupParentId);
      const backupRowCls = isBackup ? 'backup-child-row' : '';

      const _isBgJob = typeof isBackgroundRun === 'function' && isBackgroundRun(j.name);
      const bgJobCls = _isBgJob ? ' bg-job' : '';
      const rowClass = `${isPinned ? 'pinned-row' : ''} ${pinKind} ${backupRowCls}${bgJobCls} group-bg-${gidx % 4}`;
      const groupHidden = !isGroupExpanded;
      const rowDisplay = (groupHidden || backupHidden) ? 'display:none;' : '';
      const parentAttr = isBackup ? ` data-backup-parent="${backupParentId}"` : '';
      const groupAttr = ` data-run-group="${groupId}"`;

      const startTime = fmtStartCell(j);
      const endTime   = isPinned ? fmtTime(j.ended_local || j.ended_at) : '—';
      const elapsedCell = fmtElapsedCell(j);
      const safeName = (j.name || '').replace(/'/g, "\\'");
      const isPending = st === 'PENDING';
      const logBtn = isPending ? '' : `<button class="action-btn log-btn" onclick="openLog('${name}','${j.jobid}','${safeName}')">log</button>`;
      const statsBtn = isPending ? '' : (name === 'local'
        ? `<button class="action-btn" title="Stats not available for local process mode" onclick="toast('Stats popup is for Slurm cluster jobs','error')">stats</button>`
        : `<button class="action-btn log-btn" onclick="openStats('${name}','${j.jobid}','${safeName}')">stats</button>`);
      const quickActions = `${logBtn} ${statsBtn}`;
      const tailAction = (isPinned && !isActivelyCancelable)
        ? `<button class="action-btn" title="dismiss" onclick="dismissFailed('${name}','${j.jobid}')">✕</button>`
        : `<button class="action-btn" onclick="cancelJob('${name}','${j.jobid}')">cancel</button>`;

      const depBadge = depBadgeHtml(j, byId);
      let backupKind = isBackup ? classifyBackupJob(j, byId) : null;
      const isUnneeded = !backupKind && isUnneededBackup(j, groupJobs);
      if (isUnneeded) backupKind = 'unneeded';
      const bkBadge = backupBadgeHtml(backupKind);
      const indent = depth > 0 ? `<span class="dep-indent" style="padding-left:${depth * 16}px"></span>` : '';
      const depArrow = depth > 0 ? '<span class="dep-arrow">↳</span> ' : '';
      const hasGpu = !!gpuStr;
      const nameCls = hasGpu ? '' : ' name-cpu';

      let backupBtn = '';
      if (hasBackups) {
        const n = backupMap[j.jobid].length;
        const cls = isExpanded ? 'backups-btn expanded' : 'backups-btn';
        backupBtn = ` <button class="${cls}" data-backups-toggle="${j.jobid}" onclick="event.stopPropagation();toggleBackups('${j.jobid}')">${n} backup${n !== 1 ? 's' : ''}</button>`;
      }
      const nameCell = `${indent}${depArrow}<span class="${nameCls}" title="${j.name}">${highlightJobName(j.name, jnHL.prefix, jnHL.suffix)}</span>${backupBtn}`;

      const _rowShaded = j.project_color && j.campaign ? campaignShade(j.project_color, j.campaign) : (j.project_color || '');
      const _rowBg = _rowShaded ? `background:${lightenColor(_rowShaded)};` : '';
      const _prog = resolveProgress(name, j.jobid, j.progress, j.state, j.progress_source);
      const _jobMeta = { nodes: j.nodes, gres: j.gres, partition: j.partition, timelimit: j.timelimit };
      return `<tr class="${rowClass}"${parentAttr}${groupAttr} style="${_rowBg}${rowDisplay}">
        <td class="dim">${j.jobid}</td>
        <td class="bold">${nameCell}</td>
        <td>${isUnneeded
          ? `<span class="state-chip s-COMPLETED">SKIPPED</span>`
          : stateChip(j.state, _prog.pct, j.reason, j.exit_code, j.crash_detected, j.est_start, _jobMeta, _prog.source)} ${bkBadge}${depBadge}</td>
        <td>${quickActions}</td>
        <td class="dim">${startTime}</td>
        <td class="dim">${endTime}</td>
        <td class="dim">${elapsedCell}</td>
        <td>${resourceCell}</td>
        <td class="dim">${j.partition || '—'}</td>
        <td class="dim acct-cell">${_shortAcct(j.account) || '—'}</td>
        <td>${tailAction}</td>
      </tr>`;
      }).join('');
      const cancelableIds = [...new Set(
        groupJobs
          .filter(j => _isActivelyCancelableState(j.state))
          .map(j => String(j.jobid))
      )];
      const cancelKey = `${name}:${rootJobId}`;
      window._cancelGroupIds = window._cancelGroupIds || {};
      window._cancelGroupIds[cancelKey] = cancelableIds;
      const cancelGroupBtn = cancelableIds.length >= 1 && name !== 'local'
        ? `<button class="action-btn cancel-group-btn" onclick="event.stopPropagation();cancelGroupByKey('${cancelKey}','${gk.replace(/'/g, "\\'")}')">cancel group</button>`
        : '';
      return `<tr class="group-head-row" onclick="toggleRunGroup('${groupId}')"><td colspan="11"><span class="group-head-content">${groupLabel}${cancelGroupBtn}</span></td></tr>${groupRows}`;
    }).join('');

    body = `<div class="card-body">
      <table>
        <thead><tr><th>ID</th><th>Name</th><th>State</th><th>Logs/Stats</th><th>Start</th><th>End</th><th>Elapsed</th><th>GPUs</th><th>Partition</th><th>Account</th><th></th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
  }

  const pinnedCount = jobs.filter(j => j._pinned).length;
  const pinnedFailedCount = jobs.filter(j => j._pinned && _isFailedNotCancelled(j.state)).length;
  const pinnedCancelledCount = jobs.filter(j => j._pinned && _isCancelledState(j.state)).length;
  const pinnedCompletedCount = jobs.filter(j => j._pinned && isCompletedState(j.state)).length;
  const liveCount   = jobs.filter(j => !j._pinned).length;

  const clearFailedBtn = pinnedFailedCount > 0
    ? `<button class="icon-btn" style="border-color:#fecaca;color:var(--red)" onclick="clearFailed('${name}')">clear ${pinnedFailedCount} failed</button>`
    : '';
  const clearCancelledBtn = pinnedCancelledCount > 0
    ? `<button class="icon-btn" style="border-color:var(--gray-bd);color:var(--muted)" onclick="clearCancelled('${name}')">clear ${pinnedCancelledCount} canc/comp</button>`
    : '';
  const clearCompletedBtn = pinnedCompletedCount > 0
    ? `<button class="icon-btn" style="border-color:#bbf7d0;color:var(--green)" onclick="clearCompleted('${name}')">clear ${pinnedCompletedCount} done</button>`
    : '';
  const mountBadge = name !== 'local'
    ? `<span class="mount-badge ${mount.mounted ? 'ok' : 'off'}" title="${(mount.root || '').replace(/"/g, '&quot;')}">${mount.mounted ? 'mounted' : 'ssh-only'}</span>`
    : '';
  const mountBtn = (name !== 'local' && !mount.mounted)
    ? `<button class="icon-btn" onclick="mountCluster('${name}')">mount</button>`
    : '';

  return `<div class="${cardClass}" id="card-${name}">
    <div class="card-head">
      <div class="card-info-row">
        <span class="card-name">${name}</span>
        <span class="badge">${clusterGpuBadge(name)}</span>
        ${quotaBadgesHtml(name)}
        <span class="status-indicator ${statusClass}"></span>
        <span class="job-count-text">${jobCountText}</span>
        ${mountBadge}
        <span class="card-freshness-group">${freshBadge}<button class="icon-btn" onclick="refreshCluster('${name}',true)" title="Refresh">↻</button></span>
      </div>
      <div class="card-actions-row">
        ${mountBtn}
        <button class="icon-btn" onclick="showClusterHistory('${name}')">history</button>
        ${clearCompletedBtn}
        ${clearCancelledBtn}
        ${clearFailedBtn}
      </div>
    </div>
    ${body}
  </div>`;
}

// ── Grouping ──

let _lastActiveSet = new Set();
try {
  const saved = JSON.parse(localStorage.getItem('clausius.activeClusters') || '[]');
  if (Array.isArray(saved)) _lastActiveSet = new Set(saved);
} catch (_) {}

function _persistActiveSet() {
  try { localStorage.setItem('clausius.activeClusters', JSON.stringify([..._lastActiveSet])); } catch (_) {}
}

let _prevSectionMap = {};
let _transitioned = {};

function groupClusters(data) {
  const local   = [];
  const active  = [];
  const idle    = [];
  const failed  = [];
  const sectionMap = {};

  for (const name of Object.keys(CLUSTERS)) {
    const d = data[name];
    if (!d) continue;
    if (name === 'local') { local.push(name); sectionMap[name] = 'local'; continue; }
    const poller = d.poller || {};
    const failCount = poller.failure_count || 0;
    if (d.status === 'error' && failCount >= 3 && !(d.jobs && d.jobs.length)) {
      failed.push(name); sectionMap[name] = 'unreachable'; continue;
    }
    const allJobs = d.jobs || [];
    const liveJobs = allJobs.filter(j => !j._pinned);
    const hasActiveJobs = liveJobs.length > 0
      || allJobs.some(j => {
        const s = (j.state || '').toUpperCase();
        return s === 'RUNNING' || s === 'COMPLETING' || s === 'PENDING' || s === 'SUBMITTING';
      });
    const hasFreshData = !!d.updated;
    if (hasActiveJobs) {
      active.push(name);
      _lastActiveSet.add(name);
      sectionMap[name] = 'active';
    } else if (!hasFreshData && _lastActiveSet.has(name)) {
      active.push(name);
      sectionMap[name] = 'active';
    } else {
      idle.push(name);
      _lastActiveSet.delete(name);
      sectionMap[name] = 'idle';
    }
  }

  _transitioned = {};
  for (const name of Object.keys(sectionMap)) {
    const prev = _prevSectionMap[name];
    if (prev && prev !== sectionMap[name]) {
      _transitioned[name] = { from: prev, to: sectionMap[name] };
    }
  }
  _prevSectionMap = sectionMap;

  _persistActiveSet();
  return { local, active, idle, failed };
}

function toggleSection(name) {
  sectionCollapsed[name] = !sectionCollapsed[name];
  localStorage.setItem('clausius.sectionCollapsed', JSON.stringify(sectionCollapsed));
  renderGrid(allData);
}

function renderGroupLabel(dot, color, text, count, sectionName, toggleable) {
  const countStr = count > 0 ? ` (${count})` : '';
  const isCollapsed = sectionName ? !!sectionCollapsed[sectionName] : false;
  const toggleCls = toggleable ? 'toggleable' : '';
  const click = toggleable ? `onclick="toggleSection('${sectionName}')"` : '';
  const chevron = toggleable
    ? `<span class="section-chevron ${isCollapsed ? 'collapsed' : ''}">▼</span>`
    : '';
  return `<div class="section-label ${toggleCls}" ${click}>
    ${chevron}
    <span class="section-dot" style="background:${color}"></span>
    ${text}${countStr}
  </div>`;
}

function renderGrid(data) {
  const grid = document.getElementById('grid');
  const { local, active, idle, failed } = groupClusters(data);

  const hasTransitions = Object.keys(_transitioned).length > 0;
  if (hasTransitions) {
    for (const t of Object.values(_transitioned)) {
      if (sectionCollapsed[t.to]) {
        sectionCollapsed[t.to] = false;
        localStorage.setItem('clausius.sectionCollapsed', JSON.stringify(sectionCollapsed));
      }
    }
  }

  const sectionGrid = (names, onlyIfHasRuns) => {
    const hasRuns = names.length === 1 && !!(data[names[0]] && (data[names[0]].jobs || []).length);
    const single = names.length === 1 && (!onlyIfHasRuns || hasRuns);
    return `<div class="grid${single ? ' single-card' : ''}">${names.map(n => {
      const t = _transitioned[n];
      const cls = t ? ' card-transition' : '';
      const tag = t ? ` data-from="${t.from}" data-to="${t.to}"` : '';
      const cardHtml = renderCard(n, data[n] || {status:'error',error:'No response',jobs:[]});
      return cls ? cardHtml.replace(/class="card/, `class="card${cls}"${tag} `) : cardHtml;
    }).join('')}</div>`;
  };

  let html = '';

  // Local — always first, no label
  if (local.length) {
    const localBody = sectionCollapsed.local
      ? ''
      : sectionGrid(local, true);
    html += `<div class="section">
      ${renderGroupLabel('', 'var(--muted)', 'local', local.length, 'local', true)}
      ${localBody}
    </div>`;
  }

  // Active
  html += `<div class="section">
    ${renderGroupLabel('', 'var(--green)', 'active', active.length, 'active', false)}
    ${sectionGrid(active, true)}
  </div>`;

  // Idle
  if (idle.length) {
    const idleBody = sectionCollapsed.idle
      ? ''
      : sectionGrid(idle, true);
    html += `<div class="section">
      ${renderGroupLabel('', 'var(--muted)', 'idle', idle.length, 'idle', true)}
      ${idleBody}
    </div>`;
  }

  // Failed auth
  if (failed.length) {
    const failedBody = sectionCollapsed.unreachable
      ? ''
      : sectionGrid(failed, true);
    html += `<div class="section">
      ${renderGroupLabel('', 'var(--red)', 'unreachable', failed.length, 'unreachable', true)}
      ${failedBody}
    </div>`;
  }

  grid.innerHTML = html;
}

function _collectVisibleJobs(data) {
  const jobs = [];
  for (const [cluster, d] of Object.entries(data || {})) {
    if (!d || d.status !== 'ok') continue;
    for (const j of (d.jobs || [])) {
      const s = (j.state || '').toUpperCase();
      if (_isActivelyCancelableState(s)) {
        jobs.push({ cluster, job_id: j.jobid, state: s });
      }
    }
  }
  return jobs.slice(0, 60);
}

async function prefetchAndUpdateProgress(data) {
  if (document.hidden) return;
  const batch = _collectVisibleJobs(data);
  if (!batch.length) { _saveProgressCache(); return; }
  const hasPending = batch.some(j => (j.state || '').toUpperCase() === 'PENDING');
  try {
    if (hasPending && !_waitCalibration) {
      await fetchWaitCalibration();
    }
  } catch (_) {}
  setTimeout(() => _fetchProgressUpdate(batch), 4000);
}

async function _fetchProgressUpdate(batch) {
  if (document.hidden) return;
  try {
    const res = await fetchWithTimeout('/api/progress', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jobs: batch }),
    });
    const result = await res.json();
    const overlayVersion = result.board_version != null ? String(result.board_version) : null;
    if (_lastBoardVersion && overlayVersion && overlayVersion !== _lastBoardVersion) {
      return;
    }
    const progressMap = result.progress || result;
    const progressSources = result.progress_sources || {};
    const estStarts = result.est_starts || {};
    if (result.team_usage) {
      for (const [c, tu] of Object.entries(result.team_usage)) {
        _teamUsageData[c] = tu;
      }
    }
    if (result.team_gpu_allocations) {
      _teamGpuAlloc = result.team_gpu_allocations;
    }
    let changed = false;
    for (const [key, pct] of Object.entries(progressMap)) {
      const [cluster, jobid] = key.split(':');
      if (allData[cluster]) {
        for (const j of (allData[cluster].jobs || [])) {
          if (String(j.jobid) === jobid) {
            if (j.progress !== pct) { j.progress = pct; changed = true; }
            if (progressSources[key]) j.progress_source = progressSources[key];
          }
        }
      }
    }
    for (const [key, est] of Object.entries(estStarts)) {
      const [cluster, jobid] = key.split(':');
      if (allData[cluster]) {
        for (const j of (allData[cluster].jobs || [])) {
          if (String(j.jobid) === jobid && j.est_start !== est) {
            j.est_start = est;
            changed = true;
          }
        }
      }
    }
    if (changed) _scheduleRender();
  } catch (_) {}
}

// ── Live fetch ──
function _showLoadingSkeleton() {
  const grid = document.getElementById('grid');
  grid.innerHTML = `<div class="section">
    ${renderGroupLabel('', 'var(--accent)', 'loading…', 0)}
    <div class="grid">${Object.keys(CLUSTERS).map(name => `
      <div class="card" id="card-${name}">
        <div class="card-head">
          <div class="card-info-row"><span class="card-name">${name}</span><span class="badge">${clusterGpuBadge(name)}</span><span class="status-indicator loading"></span><span class="job-count-text">loading…</span></div>
        </div>
        <div class="no-jobs" style="color:#bbb">waiting…</div>
      </div>`).join('')}
    </div>
  </div>`;
}

function _isCacheFresh(data) {
  const now = Date.now();
  const maxAge = 60 * 1000;
  for (const d of Object.values(data)) {
    if (d.updated && (now - new Date(d.updated).getTime()) < maxAge) return true;
  }
  return false;
}

let _fetchAllRunning = false;
let _lastEtag = null;
let _lastBoardVersion = null;

async function fetchAll() {
  if (_fetchAllRunning || document.hidden) {
    const debugRunId = `jobs-skip-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    // #region agent log
    fetch('http://localhost:7812/ingest/20aa2267-7a00-4d7a-a8e0-160ef713eecd',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'41bcda'},body:JSON.stringify({sessionId:'41bcda',runId:debugRunId,hypothesisId:'H5',location:'static/js/jobs.js:fetchAll:skip',message:'fetchAll skipped',data:{fetchAllRunning:_fetchAllRunning,hidden:document.hidden,gridChildren:document.getElementById('grid')?.children?.length||0},timestamp:Date.now()})}).catch(()=>{});
    // #endregion
    return;
  }
  _fetchAllRunning = true;
  try { await _doFetchAll(); } finally { _fetchAllRunning = false; }
}
async function _doFetchAll() {
  const grid = document.getElementById('grid');
  const debugRunId = `jobs-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  if (!grid.children.length) _showLoadingSkeleton();

  fetchClusterUtilization().then(() => {
    if (_clusterUtil && Object.keys(allData).length) _scheduleRender();
  });
  fetchPartitions().then(() => {
    if (_partitionData && Object.keys(allData).length) _scheduleRender();
  });
  if (!Object.keys(_storageQuota).length) {
    fetchStorageQuotas().then(() => {
      if (Object.keys(allData).length) _scheduleRender();
    });
  }

  // Single conditional fetch — replaces bulk + N per-cluster requests
  try {
    const headers = {};
    if (_lastEtag) headers['If-None-Match'] = _lastEtag;
    headers['X-Debug-Run-Id'] = debugRunId;
    // #region agent log
    fetch('http://localhost:7812/ingest/20aa2267-7a00-4d7a-a8e0-160ef713eecd',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'41bcda'},body:JSON.stringify({sessionId:'41bcda',runId:debugRunId,hypothesisId:'H3',location:'static/js/jobs.js:_doFetchAll:start',message:'jobs fetch start',data:{lastEtag:_lastEtag,allDataKeys:Object.keys(allData).length,gridChildren:grid.children.length},timestamp:Date.now()})}).catch(()=>{});
    // #endregion
    const res = await fetchWithTimeout('/api/jobs', { headers });
    // #region agent log
    fetch('http://localhost:7812/ingest/20aa2267-7a00-4d7a-a8e0-160ef713eecd',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'41bcda'},body:JSON.stringify({sessionId:'41bcda',runId:debugRunId,hypothesisId:'H3',location:'static/js/jobs.js:_doFetchAll:response',message:'jobs fetch response',data:{status:res.status,ok:res.ok,etag:res.headers.get('ETag')},timestamp:Date.now()})}).catch(()=>{});
    // #endregion

    if (res.status === 304) {
      _clearErrorBannerKey('jobs');
      _clearErrorBannerKey('clusters');
      grid.classList.remove('grid-loading');
      return;
    }

    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    _lastEtag = res.headers.get('ETag');
    _lastBoardVersion = (_lastEtag || '').replace(/"/g, '');
    const fresh = await res.json();
    const hasData = Object.values(fresh).some(d => d.updated);
    // #region agent log
    fetch('http://localhost:7812/ingest/20aa2267-7a00-4d7a-a8e0-160ef713eecd',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'41bcda'},body:JSON.stringify({sessionId:'41bcda',runId:debugRunId,hypothesisId:'H4',location:'static/js/jobs.js:_doFetchAll:parsed',message:'jobs payload parsed',data:{clusterKeys:Object.keys(fresh||{}).length,updatedClusters:Object.values(fresh||{}).filter(d=>d&&d.updated).length,hasData},timestamp:Date.now()})}).catch(()=>{});
    // #endregion
    if (hasData) {
      allData = fresh;
      _fillMissing();
      _renderAll();
      // #region agent log
      fetch('http://localhost:7812/ingest/20aa2267-7a00-4d7a-a8e0-160ef713eecd',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'41bcda'},body:JSON.stringify({sessionId:'41bcda',runId:debugRunId,hypothesisId:'H4',location:'static/js/jobs.js:_doFetchAll:rendered',message:'jobs render complete',data:{allDataKeys:Object.keys(allData).length,gridChildren:grid.children.length},timestamp:Date.now()})}).catch(()=>{});
      // #endregion
      _clearErrorBannerKey('jobs');
      _clearErrorBannerKey('clusters');
    }
  } catch (e) {
    console.warn('Job fetch failed:', e);
    // #region agent log
    fetch('http://localhost:7812/ingest/20aa2267-7a00-4d7a-a8e0-160ef713eecd',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'41bcda'},body:JSON.stringify({sessionId:'41bcda',runId:debugRunId,hypothesisId:'H3',location:'static/js/jobs.js:_doFetchAll:catch',message:'jobs fetch failed',data:{name:e?.name||null,message:e?.message||String(e),allDataKeys:Object.keys(allData).length,gridChildren:grid.children.length},timestamp:Date.now()})}).catch(()=>{});
    // #endregion
    if (!Object.keys(allData).length) {
      _setErrorBanner('jobs', `Server unreachable — retrying (${e.message})`);
    }
  }

  if (!Object.keys(allData).length && !grid.children.length) _showLoadingSkeleton();

  _saveProgressCache();
  prefetchAndUpdateProgress(allData);
}

async function _forceRefreshAll() {
  const grid = document.getElementById('grid');
  const debugRunId = `force-all-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  grid.classList.add('grid-loading');
  const promises = Object.keys(CLUSTERS).map(name =>
    fetch(`/api/force_poll/${name}`, { method: 'POST', headers: { 'X-Debug-Run-Id': debugRunId } }).catch(() => {})
  );
  await Promise.allSettled(promises);
  _lastEtag = null;
  await _doFetchAll();
  grid.classList.remove('grid-loading');
}

function _fillMissing() {
  for (const n of Object.keys(CLUSTERS)) {
    if (!allData[n]) allData[n] = {status:'error', error:'No response', jobs:[]};
  }
}

function _renderAll() {
  renderGrid(allData);
  updateSummary(allData);
  _attachPendingTooltips();
}

let _renderScheduled = false;
function _scheduleRender() {
  if (_renderScheduled) return;
  _renderScheduled = true;
  requestAnimationFrame(() => {
    _renderScheduled = false;
    _renderAll();
  });
}

function _attachPendingTooltips() {
  if (!_clusterUtil && !_partitionData) return;
  document.querySelectorAll('.pending-util-chip').forEach(chip => {
    if (chip._utilBound) return;
    const card = chip.closest('.card');
    if (!card) return;
    const clusterName = (card.id || '').replace('card-', '');
    if (!clusterName) return;
    const jobInfo = {
      reason: chip.dataset.reason || '',
      nodes: chip.dataset.nodes || '',
      gres: chip.dataset.gres || '',
      partition: chip.dataset.partition || '',
      timelimit: chip.dataset.timelimit || '',
      estStart: chip.dataset.estStart || '',
    };
    attachPendingTooltip(chip, clusterName, jobInfo);
    chip._utilBound = true;
  });
}

async function refreshCluster(name, force) {
  const grid = document.getElementById('grid');
  const debugRunId = `refresh-${name}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const card = document.getElementById(`card-${name}`);
  if (card) {
    const si = card.querySelector('.status-indicator');
    if (si) si.className = 'status-indicator loading';
  }
  grid.classList.add('grid-loading');
  try {
    if (force) {
      await fetch(`/api/force_poll/${name}`, { method: 'POST', headers: { 'X-Debug-Run-Id': debugRunId } });
    }
    _lastEtag = null;
    const res = await fetchWithTimeout('/api/jobs', { headers: { 'X-Debug-Run-Id': debugRunId } });
    if (res.ok) {
      _lastEtag = res.headers.get('ETag');
      _lastBoardVersion = (_lastEtag || '').replace(/"/g, '');
      const fresh = await res.json();
      if (Object.values(fresh).some(d => d.updated)) {
        allData = fresh;
        _fillMissing();
      }
    }
    _scheduleRender();
  } catch (e) {
    toast(`Failed to refresh ${name}`, 'error');
  } finally {
    grid.classList.remove('grid-loading');
  }
}

