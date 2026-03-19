function showTab(tab) {
  currentTab = tab;
  document.getElementById('live-view').classList.toggle('hidden', tab !== 'live');
  document.getElementById('history-view').classList.toggle('active', tab === 'history');
  document.getElementById('project-view').classList.toggle('active', tab === 'project');
  document.getElementById('explorer-page').classList.remove('open');
  document.getElementById('tab-live').classList.toggle('active', tab === 'live');
  document.getElementById('tab-history').classList.toggle('active', tab === 'history');
  if (tab === 'history') loadHistory();
  try { sessionStorage.setItem('ncluster.activeTab', tab); } catch (_) {}
}

function applySidebarState() {
  const nav = document.getElementById('side-nav');
  const btn = document.getElementById('nav-toggle');
  const splitter = document.getElementById('nav-splitter');
  if (!nav || !btn) return;
  nav.classList.toggle('collapsed', navCollapsed);
  if (navCollapsed) {
    // Force collapse even when a resizer-set inline width exists.
    nav.style.width = '0px';
  } else {
    // Restore persisted width when opening.
    try {
      const saved = parseInt(localStorage.getItem('ncluster.navWidth') || '', 10);
      const minW = 230;
      const maxW = Math.min(640, Math.floor(window.innerWidth * 0.55));
      const w = Number.isNaN(saved) ? 320 : Math.min(maxW, Math.max(minW, saved));
      nav.style.width = `${w}px`;
    } catch (_) {
      nav.style.width = '320px';
    }
  }
  if (splitter) splitter.style.display = navCollapsed ? 'none' : '';
  btn.textContent = navCollapsed ? '☰' : '✕';
  btn.title = navCollapsed ? 'open controls' : 'hide controls';
  updateNavTogglePosition();
}

function toggleSidebar() {
  navCollapsed = !navCollapsed;
  try { localStorage.setItem('ncluster.navCollapsed', navCollapsed ? '1' : '0'); } catch (_) {}
  applySidebarState();
}

function updateNavTogglePosition() {
  const btn = document.getElementById('nav-toggle');
  if (!btn) return;
  // Keep toggle fixed on the left for predictable interaction.
  btn.style.left = '10px';
}

function setupSidebarResizer() {
  const splitter = document.getElementById('nav-splitter');
  const nav = document.getElementById('side-nav');
  if (!splitter || !nav) return;

  splitter.addEventListener('mousedown', (e) => {
    if (navCollapsed) return;
    _isResizingNav = true;
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
    try { localStorage.setItem('ncluster.navWidth', String(next)); } catch (_) {}
    updateNavTogglePosition();
  });

  window.addEventListener('mouseup', () => {
    _isResizingNav = false;
  });

  try {
    const saved = parseInt(localStorage.getItem('ncluster.navWidth') || '', 10);
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
        const saved = parseInt(localStorage.getItem('ncluster.navWidth') || '', 10);
        const minW = 230;
        const maxW = Math.min(640, Math.floor(window.innerWidth * 0.55));
        const w = Number.isNaN(saved) ? 320 : Math.min(maxW, Math.max(minW, saved));
        nav.style.width = `${w}px`;
      } catch (_) {}
    }
    updateNavTogglePosition();
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
      else if (s.includes('FAIL')) failed++;
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
        </div>
      </div>`;
    }).join('');
  panel.innerHTML = items || '<div class="no-jobs" style="padding:8px">no clusters</div>';
}

function computeRefreshIntervalSec(data) {
  for (const [, d] of Object.entries(data || {})) {
    for (const j of d.jobs || []) {
      const s = (j.state || '').toUpperCase();
      if (s === 'RUNNING' || s === 'PENDING' || s === 'COMPLETING') return 30;
    }
  }
  return 60;
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

  const statusClass = isErr ? 'error' : 'ok';
  const jobCountText = isErr ? 'unreachable' : `${jobs.length} job${jobs.length !== 1 ? 's' : ''}`;

  const updated = data.updated ? new Date(data.updated).toLocaleTimeString() : '';
  const mount = data.mount || { mounted: false };

  let body = '';
  if (isErr) {
    body = `<div class="err-msg">⚠ ${data.error}<button class="btn" style="margin-left:10px" onclick="refreshCluster('${name}')">retry</button></div>`;
  } else if (jobs.length === 0) {
    body = `<div class="no-jobs">no active jobs</div>`;
  } else {
    const groupEntries = groupJobsByDependency(jobs);

    const rows = groupEntries.map(([gk, groupJobs], gidx) => {
      const _proj = groupJobs[0]?.project || '';
      const _projColor = groupJobs[0]?.project_color || '';
      const _projEmoji = groupJobs[0]?.project_emoji || '';
      const _projBadge = _proj ? `<span class="group-project-badge">${_projEmoji ? _projEmoji + ' ' : ''}${_proj}</span>` : '';
      const rootJob = groupJobs.find(j => !(j.depends_on || []).length) || groupJobs[0];
      const rootJobId = rootJob.jobid;
      const safeGk = gk.replace(/'/g, "\\'");
      const runBadgeStyle = _projColor ? ` style="background:${_projColor};border-color:${_projColor};color:${contrastTextColor(_projColor)}"` : '';
      const runBadge = name !== 'local'
        ? `<span class="run-name-badge"${runBadgeStyle} onclick="event.stopPropagation();openRunInfo('${name}','${rootJobId}','${safeGk}')" title="View run details">${gk}</span>`
        : gk;

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
      const groupLabel = `<span>${runBadge}${_projBadge} <span class="group-count">· ${visibleCount} job${visibleCount !== 1 ? 's' : ''}</span></span>`;

      const groupRows = ordered.map(j => {
      const gpuStr = parseGpus(j.nodes, j.gres);
      const resourceCell = gpuStr
        ? `<span style="color:var(--text);font-weight:500">${gpuStr}</span>`
        : `<span class="dim">${j.nodes || '—'}n</span>`;
      const isPinned = j._pinned;
      const st = (j.state || '').toUpperCase();
      const pinKind = isPinned ? (isCompletedState(st) ? 'pinned-completed-row' : 'pinned-failed-row') : '';
      const depth = depthInGroup(j, byId, idSet, depthMemo);

      const isBackup = backupSet.has(j.jobid);
      const backupParentId = parentOf[j.jobid] || '';
      const hasBackups = backupMap[j.jobid] && backupMap[j.jobid].length > 0;
      const isExpanded = _expandedBackups.has(j.jobid);
      const backupHidden = isBackup && !_expandedBackups.has(backupParentId);
      const backupRowCls = isBackup ? 'backup-child-row' : '';

      const rowClass = `${isPinned ? 'pinned-row' : ''} ${pinKind} ${backupRowCls} group-bg-${gidx % 4}`;
      const rowDisplay = backupHidden ? 'display:none;' : '';
      const parentAttr = isBackup ? ` data-backup-parent="${backupParentId}"` : '';

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
      const tailAction = isPinned
        ? `<button class="action-btn" title="dismiss" onclick="dismissFailed('${name}','${j.jobid}')">✕</button>`
        : `<button class="action-btn" onclick="cancelJob('${name}','${j.jobid}')">cancel</button>`;

      const depBadge = depBadgeHtml(j, byId);
      const backupKind = isBackup ? classifyBackupJob(j, byId) : null;
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
      const nameCell = `${indent}${depArrow}<span class="${nameCls}" title="${j.name}">${j.name}</span>${backupBtn}`;

      const _rowBg = j.project_color ? `background:${lightenColor(j.project_color)}` : '';
      const _pct = resolveProgress(name, j.jobid, j.progress, j.state);
      return `<tr class="${rowClass}"${parentAttr} style="${_rowBg}${rowDisplay}">
        <td class="dim">${j.jobid}</td>
        <td class="bold">${nameCell}</td>
        <td>${stateChip(j.state, _pct, j.reason, j.exit_code, j.crash_detected, j.est_start)} ${bkBadge}${depBadge}</td>
        <td>${quickActions}</td>
        <td class="dim">${startTime}</td>
        <td class="dim">${endTime}</td>
        <td class="dim">${elapsedCell}</td>
        <td>${resourceCell}</td>
        <td class="dim">${j.partition || '—'}</td>
        <td>${tailAction}</td>
      </tr>`;
      }).join('');
      const cancelableIds = groupJobs.filter(j => !j._pinned).map(j => j.jobid);
      const idsAttr = JSON.stringify(cancelableIds).replace(/"/g, '&quot;');
      const cancelGroupBtn = cancelableIds.length > 1 && name !== 'local'
        ? `<button class="action-btn cancel-group-btn" onclick="event.stopPropagation();cancelGroup('${name}','${idsAttr}','${gk.replace(/'/g, "\\'")}')">cancel group</button>`
        : '';
      return `<tr class="group-head-row"><td colspan="10"><span class="group-head-content">${groupLabel}${cancelGroupBtn}</span></td></tr>${groupRows}`;
    }).join('');

    body = `<div class="card-body">
      <table>
        <thead><tr><th>ID</th><th>Name</th><th>State</th><th>Logs/Stats</th><th>Start</th><th>End</th><th>Elapsed</th><th>GPUs</th><th>Partition</th><th></th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
  }

  const pinnedCount = jobs.filter(j => j._pinned).length;
  const pinnedFailedCount = jobs.filter(j => j._pinned && isFailedLikeState(j.state)).length;
  const pinnedCompletedCount = jobs.filter(j => j._pinned && isCompletedState(j.state)).length;
  const liveCount   = jobs.filter(j => !j._pinned).length;

  const cancelAllBtn = (!isErr && liveCount > 0 && name !== 'local')
    ? `<button class="icon-btn danger" onclick="cancelAll('${name}')">cancel all</button>`
    : '';
  const clearFailedBtn = pinnedFailedCount > 0
    ? `<button class="icon-btn" style="border-color:#fecaca;color:var(--red)" onclick="clearFailed('${name}')">clear ${pinnedFailedCount} failed</button>`
    : '';
  const clearCompletedBtn = pinnedCompletedCount > 0
    ? `<button class="icon-btn" style="border-color:#bbf7d0;color:var(--green)" onclick="clearCompleted('${name}')">clear ${pinnedCompletedCount} completed</button>`
    : '';
  const mountBtn = name !== 'local'
    ? (mount.mounted
      ? `<button class="icon-btn" onclick="unmountCluster('${name}')">unmount</button>`
      : `<button class="icon-btn" onclick="mountCluster('${name}')">mount</button>`)
    : '';
  const mountBadge = name !== 'local'
    ? `<span class="mount-badge ${mount.mounted ? 'ok' : 'off'}" title="${(mount.root || '').replace(/"/g, '&quot;')}">${mount.mounted ? 'mounted' : 'ssh-only'}</span>`
    : '';

  return `<div class="${cardClass}" id="card-${name}">
    <div class="card-head">
      <div class="card-title">
        <span class="card-name">${name}</span>
        <span class="badge">${info.gpu_type}</span>
        ${hasRunning ? '<span class="badge badge-accent">● active</span>' : ''}
      </div>
      <div class="card-meta">
        <span class="status-indicator ${statusClass}"></span>
        <span class="job-count-text">${jobCountText}</span>
        ${mountBadge}
        <div class="card-actions">
          <button class="icon-btn" onclick="refreshCluster('${name}')">↻</button>
          ${mountBtn}
          <button class="icon-btn" onclick="showClusterHistory('${name}')">history</button>
          ${clearCompletedBtn}
          ${clearFailedBtn}
          ${cancelAllBtn}
        </div>
      </div>
    </div>
    ${body}
  </div>`;
}

// ── Grouping ──
function groupClusters(data) {
  const local   = [];
  const active  = [];  // reachable + has any live jobs (incl. pending)
  const idle    = [];  // reachable + no live jobs at all
  const failed  = [];  // unreachable

  for (const name of Object.keys(CLUSTERS)) {
    const d = data[name];
    if (!d) continue;
    if (name === 'local') { local.push(name); continue; }
    if (d.status === 'error') { failed.push(name); continue; }
    const liveJobs = (d.jobs || []).filter(j => !j._pinned);
    if (liveJobs.length > 0) active.push(name);
    else idle.push(name);
  }

  return { local, active, idle, failed };
}

function toggleSection(name) {
  sectionCollapsed[name] = !sectionCollapsed[name];
  localStorage.setItem('ncluster.sectionCollapsed', JSON.stringify(sectionCollapsed));
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
  const sectionGrid = (names, onlyIfHasRuns) => {
    const hasRuns = names.length === 1 && !!(data[names[0]] && (data[names[0]].jobs || []).length);
    const single = names.length === 1 && (!onlyIfHasRuns || hasRuns);
    return `<div class="grid${single ? ' single-card' : ''}">${names.map(n => renderCard(n, data[n] || {status:'error',error:'No response',jobs:[]})).join('')}</div>`;
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
      if (j._pinned) continue;
      if (s === 'RUNNING' || s === 'COMPLETING' || s === 'PENDING') {
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
  try {
    await fetch('/api/prefetch_visible', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jobs: batch }),
    });
  } catch (_) {}
  setTimeout(() => _fetchProgressUpdate(batch), 4000);
}

async function _fetchProgressUpdate(batch) {
  if (document.hidden) return;
  try {
    const res = await fetch('/api/progress', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jobs: batch }),
    });
    const result = await res.json();
    const progressMap = result.progress || result;
    const estStarts = result.est_starts || {};
    let changed = false;
    for (const [key, pct] of Object.entries(progressMap)) {
      const [cluster, jobid] = key.split(':');
      _progressCache[key] = pct;
      if (allData[cluster]) {
        for (const j of (allData[cluster].jobs || [])) {
          if (String(j.jobid) === jobid && j.progress !== pct) {
            j.progress = pct;
            changed = true;
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
    _saveProgressCache();
    if (changed) _renderAll();
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
          <div class="card-title"><span class="card-name">${name}</span><span class="badge">${CLUSTERS[name].gpu_type}</span></div>
          <div class="card-meta"><span class="status-indicator loading"></span><span class="job-count-text">loading…</span></div>
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

async function fetchAll() {
  if (document.hidden) return;
  const grid = document.getElementById('grid');
  if (!grid.children.length) _showLoadingSkeleton();

  // 1) Try cached data — only render if it's reasonably fresh.
  let usedCache = false;
  try {
    const res = await fetch('/api/jobs');
    const cached = await res.json();
    if (_isCacheFresh(cached)) {
      allData = cached;
      _fillMissing();
      _renderAll();
      usedCache = true;
    }
  } catch (e) {
    toast('Failed to fetch jobs', 'error');
  }

  if (!usedCache && !grid.children.length) _showLoadingSkeleton();

  // 2) Refresh each cluster in parallel; update cards as responses arrive.
  grid.classList.add('grid-loading');
  await _refreshAllClusters();
  grid.classList.remove('grid-loading');

  // 3) Prefetch logs for visible running jobs, then async-fetch progress.
  _saveProgressCache();
  prefetchAndUpdateProgress(allData);
}

async function _refreshAllClusters() {
  return _refreshClusters(Object.keys(CLUSTERS));
}

async function _refreshClusters(names) {
  const promises = names.map(name =>
    fetch(`/api/jobs/${name}`)
      .then(r => r.json())
      .then(data => {
        allData[name] = data;
        _fillMissing();
        _renderAll();
      })
      .catch(err => { console.warn('Cluster refresh failed:', name, err); })
  );
  await Promise.allSettled(promises);
}

function _fillMissing() {
  for (const n of Object.keys(CLUSTERS)) {
    if (!allData[n]) allData[n] = {status:'error', error:'No response', jobs:[]};
  }
}

function _renderAll() {
  renderGrid(allData);
  updateSummary(allData);
}

async function refreshCluster(name) {
  const grid = document.getElementById('grid');
  const card = document.getElementById(`card-${name}`);
  if (card) {
    const si = card.querySelector('.status-indicator');
    if (si) si.className = 'status-indicator loading';
  }
  grid.classList.add('grid-loading');
  try {
    const res = await fetch(`/api/jobs/${name}`);
    const data = await res.json();
    allData[name] = data;
    _renderAll();
    prefetchAndUpdateProgress({ [name]: data });
  } catch (e) {
    toast(`Failed to refresh ${name}`, 'error');
  } finally {
    grid.classList.remove('grid-loading');
  }
}

