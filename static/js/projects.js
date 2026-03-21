// ── Projects ──

let _projData = [];
let _projGroups = [];
let _projLiveJobs = [];
let _projPage = 0;
let _projCurrentName = '';
let _projRefreshTimer = null;
const PROJ_GROUPS_PER_PAGE = 50;

function _projSearchStorageKey(projectName) {
  return `ncluster.projectSearch.${projectName || ''}`;
}

function _restoreProjectSearch(projectName) {
  try {
    return sessionStorage.getItem(_projSearchStorageKey(projectName)) || '';
  } catch (_) {
    return '';
  }
}

function _saveProjectSearch(projectName, value) {
  try {
    sessionStorage.setItem(_projSearchStorageKey(projectName), value || '');
  } catch (_) {}
}

async function loadProjectButtons() {
  const el = document.getElementById('nav-project-grid');
  if (!el) return;
  try {
    const res = await fetch('/api/projects');
    const projects = await res.json();
    if (!projects.length) {
      el.innerHTML = '<div style="font-family:var(--mono);font-size:10px;color:var(--muted);padding:4px 0">no projects</div>';
      return;
    }
    el.innerHTML = projects.map(p => {
      const emoji = p.emoji || '';
      const rawColor = p.color || '';
      const color = rawColor ? (_isDarkTheme() ? darkenColor(rawColor, 0.6) : rawColor) : 'var(--surface)';
      return `<button class="nav-project-btn" style="border-color:${color}" onclick="openProject('${p.project}')">${emoji ? emoji + ' ' : ''}${p.project}</button>`;
    }).join('');
  } catch (_) {}
}

async function openProject(projectName) {
  _projCurrentName = projectName;
  try { sessionStorage.setItem('ncluster.activeProject', projectName); } catch (_) {}
  showTab('project');
  const projCfg = await fetch('/api/settings').then(r => r.json()).then(c => (c.projects || {})[projectName] || {}).catch(() => ({}));
  const emoji = projCfg.emoji || '';
  document.getElementById('project-detail-title').textContent = `${emoji ? emoji + ' ' : ''}${projectName}`;
  document.getElementById('proj-search').value = _restoreProjectSearch(projectName);
  document.querySelectorAll('#proj-state-filters .hist-state-btn').forEach(b => b.classList.add('active'));

  document.getElementById('proj-stats-bar').innerHTML = '<span class="proj-stat-lbl">loading…</span>';
  document.getElementById('proj-live-section').innerHTML = '';
  document.getElementById('project-hist-body').innerHTML = '<tr><td colspan="11" style="padding:20px;text-align:center;color:var(--muted)">loading…</td></tr>';

  await _fetchProjectData();
  _restoreLogbookState();
  loadLogbookPanel(projectName);
  _loadRunNames(projectName);

  if (_projRefreshTimer) clearInterval(_projRefreshTimer);
  _projRefreshTimer = setInterval(() => {
    if (document.hidden) return;
    if (currentTab === 'project') _fetchProjectData();
  }, 30000);
}

async function refreshProjectPage() {
  if (_projCurrentName) await _fetchProjectData();
}

async function _fetchProjectData() {
  const name = _projCurrentName;
  if (!name) return;

  // Fetch live jobs and history in parallel
  const [liveRes, histRes] = await Promise.all([
    fetch('/api/jobs').then(r => r.json()).catch(() => ({})),
    fetch(`/api/history?project=${encodeURIComponent(name)}&limit=500`).then(r => r.json()).catch(() => []),
  ]);

  // Extract live + pinned jobs for this project from the board.
  // Pinned jobs (recently completed/failed) are included so runs stay together.
  _projLiveJobs = [];
  const clusterActivity = {};
  if (typeof liveRes === 'object' && !Array.isArray(liveRes)) {
    for (const [cname, cdata] of Object.entries(liveRes)) {
      if (!cdata || cdata.status !== 'ok') continue;
      for (const j of (cdata.jobs || [])) {
        if (j.project === name) {
          _projLiveJobs.push({ ...j, _cluster: cname });
          const st = (j.state || '').toUpperCase();
          if (!j._pinned) {
            if (!clusterActivity[cname]) clusterActivity[cname] = { running: 0, pending: 0 };
            if (st === 'RUNNING' || st === 'COMPLETING') clusterActivity[cname].running++;
            else if (st === 'PENDING') clusterActivity[cname].pending++;
          }
        }
      }
    }
  }

  const activeLiveIds = new Set(_projLiveJobs.filter(j => !j._pinned).map(j => String(j.jobid)));
  _projData = (Array.isArray(histRes) ? histRes : []).filter(r => !activeLiveIds.has(String(r.job_id)));
  _projPage = 0;

  _renderProjStats(clusterActivity);
  _renderProjLive();
  filterProjectRuns();
  _saveProgressCache();
}

function _renderProjStats(clusterActivity) {
  const all = [..._projLiveJobs.map(j => j.state), ..._projData.map(r => r.state || '')];
  const running = all.filter(s => s === 'RUNNING' || s === 'COMPLETING').length;
  const pending = all.filter(s => s === 'PENDING').length;
  const failed = all.filter(s => (s || '').toUpperCase().includes('FAIL')).length;
  const completed = all.filter(s => (s || '').toUpperCase().startsWith('COMPLETED')).length;
  const cancelled = all.filter(s => (s || '').toUpperCase().startsWith('CANCEL')).length;
  const totalGpus = _projLiveJobs.reduce((sum, j) => {
    const g = parseGpus(j.nodes, j.gres);
    if (!g) return sum;
    const m = g.match(/(\d+)\s*GPU/);
    return sum + (m ? parseInt(m[1]) : 0);
  }, 0);

  const clusters = Object.entries(clusterActivity).map(([c, a]) => {
    const hasActive = a.running > 0 || a.pending > 0;
    const label = hasActive ? `${c} (${a.running}r/${a.pending}p)` : c;
    return `<span class="proj-cluster-tag${hasActive ? ' has-active' : ''}">${label}</span>`;
  }).join(' ');

  document.getElementById('proj-stats-bar').innerHTML = `
    <span class="proj-stat"><span class="proj-stat-val" style="color:var(--green)">${running}</span><span class="proj-stat-lbl">running</span></span>
    <span class="proj-stat"><span class="proj-stat-val" style="color:var(--amber)">${pending}</span><span class="proj-stat-lbl">pending</span></span>
    <span class="proj-stat"><span class="proj-stat-val" style="color:var(--red)">${failed}</span><span class="proj-stat-lbl">failed</span></span>
    <span class="proj-stat"><span class="proj-stat-val">${completed}</span><span class="proj-stat-lbl">completed</span></span>
    <span class="proj-stat"><span class="proj-stat-val">${cancelled}</span><span class="proj-stat-lbl">cancelled</span></span>
    ${totalGpus ? `<span class="proj-stat"><span class="proj-stat-val" style="color:var(--accent)">${totalGpus}</span><span class="proj-stat-lbl">GPUs</span></span>` : ''}
    <span class="proj-stat"><span class="proj-stat-val">${_projData.length + _projLiveJobs.length}</span><span class="proj-stat-lbl">total</span></span>
    ${clusters ? `<span style="margin-left:4px">${clusters}</span>` : ''}
  `;
}

function _renderProjLive() {
  const el = document.getElementById('proj-live-section');
  // Only show this section if there are any non-pinned (truly live) jobs.
  const hasLive = _projLiveJobs.some(j => !j._pinned);
  if (!hasLive) {
    el.innerHTML = '';
    return;
  }

  const byCluster = {};
  for (const j of _projLiveJobs) {
    if (!byCluster[j._cluster]) byCluster[j._cluster] = [];
    byCluster[j._cluster].push(j);
  }

  // Pre-collect all live group keys for name highlighting
  const _liveGroupEntries = [];
  for (const [cluster, jobs] of Object.entries(byCluster)) {
    if (!jobs.some(j => !j._pinned)) continue;
    for (const [gk, groupJobs] of groupJobsByDependency(jobs)) {
      if (groupJobs.some(j => !j._pinned)) _liveGroupEntries.push(gk);
    }
  }
  const _liveGkHL = computeNameHighlight(_liveGroupEntries);

  let html = '<div class="proj-live-label">● live jobs</div>';
  for (const [cluster, jobs] of Object.entries(byCluster)) {
    // Only render clusters that have at least one non-pinned job.
    if (!jobs.some(j => !j._pinned)) continue;
    const groups = groupJobsByDependency(jobs);
    for (const [gk, groupJobs] of groups) {
      // Only render groups that contain at least one active job.
      if (!groupJobs.some(j => !j._pinned)) continue;
      const idSet = new Set(groupJobs.map(j => j.jobid));
      const byId = {};
      for (const j of groupJobs) byId[j.jobid] = j;
      const depthMemo = {};

      const rootJob = groupJobs.find(j => !(j.depends_on || []).length) || groupJobs[0];
      const rootJobId = rootJob.jobid;
      const safeGk = gk.replace(/'/g, "\\'");
      const _projColor = groupJobs[0]?.project_color || '';
      const runBadgeStyle = _projColor ? projectBadgeStyle(_projColor) : '';
      const highlightedGk = highlightJobName(gk, _liveGkHL.prefix, _liveGkHL.suffix);
      const runBadge = cluster !== 'local'
        ? `<span class="run-name-badge"${runBadgeStyle} onclick="event.stopPropagation();openRunInfo('${cluster}','${rootJobId}','${safeGk}')" title="${gk.replace(/"/g, '&quot;')}">${highlightedGk}</span>`
        : highlightedGk;
      const groupLabel = `${runBadge} ${cluster} <span class="group-count">· ${groupJobs.length} run${groupJobs.length !== 1 ? 's' : ''}</span>`;
      let rows = `<tr class="group-head-row"><td colspan="11">${groupLabel}</td></tr>`;

      const _liveJobNames = groupJobs.map(j => j.name).filter(Boolean);
      const _liveJnHL = computeNameHighlight(_liveJobNames);

      for (const j of groupJobs) {
        const st = (j.state || '').toUpperCase();
        const isPinned = j._pinned;
        const depth = depthInGroup(j, byId, idSet, depthMemo);
        const gpuStr = parseGpus(j.nodes, j.gres);
        const resourceCell = gpuStr
          ? `<span style="color:var(--text);font-weight:500">${gpuStr}</span>`
          : `<span class="dim">${j.nodes || '—'}n</span>`;
        const startTime = fmtStartCell(j);
        const endTime = isPinned ? fmtTime(j.ended_local || j.ended_at) : '—';
        const safeName = (j.name || '').replace(/'/g, "\\'");
        const isPending = st === 'PENDING';
        const logBtn = isPending ? '' : `<button class="action-btn log-btn" onclick="openLog('${cluster}','${j.jobid}','${safeName}')">log</button>`;
        const statsBtn = isPending ? '' : (cluster === 'local'
          ? ''
          : `<button class="action-btn log-btn" onclick="openStats('${cluster}','${j.jobid}','${safeName}')">stats</button>`);
        const tailAction = isPinned
          ? `<button class="action-btn" title="dismiss" onclick="dismissFailed('${cluster}','${j.jobid}')">✕</button>`
          : `<button class="action-btn" onclick="cancelJob('${cluster}','${j.jobid}')">cancel</button>`;
        const depBadge = depBadgeHtml(j, byId);
        const indent = depth > 0 ? `<span class="dep-indent" style="padding-left:${depth * 16}px"></span>` : '';
        const depArrow = depth > 0 ? '<span class="dep-arrow">↳</span> ' : '';
        const hasGpu = !!gpuStr;
        const nameCls = hasGpu ? '' : ' name-cpu';
        const pinKind = isPinned ? (isCompletedState(st) ? 'pinned-completed-row' : 'pinned-failed-row') : '';

        const _pct = resolveProgress(cluster, j.jobid, j.progress, j.state);
        const _rowBg = j.project_color ? `background:${lightenColor(j.project_color)}` : '';
        rows += `<tr class="${isPinned ? 'pinned-row' : ''} ${pinKind}" style="${_rowBg}">
          <td class="dim">${j.jobid}</td>
          <td class="bold">${indent}${depArrow}<span class="${nameCls}" title="${j.name}">${j.name ? highlightJobName(j.name, _liveJnHL.prefix, _liveJnHL.suffix) : '—'}</span></td>
          <td>${stateChip(j.state, _pct, j.reason, j.exit_code, j.crash_detected, j.est_start)} ${depBadge}</td>
          <td>${logBtn} ${statsBtn}</td>
          <td class="dim">${startTime}</td>
          <td class="dim">${endTime}</td>
          <td class="dim">${j.elapsed || '—'}</td>
          <td>${resourceCell}</td>
          <td class="dim">${j.partition || '—'}</td>
          <td>${tailAction}</td>
        </tr>`;
      }
      html += `<div class="proj-live-card"><table><thead><tr><th>ID</th><th>Name</th><th>State</th><th>Logs/Stats</th><th>Start</th><th>End</th><th>Elapsed</th><th>GPUs</th><th>Partition</th><th></th></tr></thead><tbody>${rows}</tbody></table></div>`;
    }
  }
  el.innerHTML = html;
}

function toggleProjStateFilter(btn) {
  btn.classList.toggle('active');
  filterProjectRuns();
}

function _getProjCheckedStates() {
  const btns = document.querySelectorAll('#proj-state-filters .hist-state-btn.active');
  return Array.from(btns).map(b => b.dataset.state);
}

function _projectSearchMatches(row, query) {
  if (!query) return true;
  const jobName = (row.job_name || row.name || '').toLowerCase();
  const jobId = String(row.job_id || row.jobid || '').toLowerCase();
  const runName = groupKeyForJob(row.job_name || row.name || '').toLowerCase();
  return jobName.includes(query) || jobId.includes(query) || runName.includes(query);
}

function filterProjectRuns() {
  const q = document.getElementById('proj-search').value.toLowerCase();
  _saveProjectSearch(_projCurrentName, q);
  const allowedStates = _getProjCheckedStates();
  const filtered = _projData.filter(r => {
    const st = (r.state || '').toUpperCase().split(' ')[0];
    if (!allowedStates.some(s => st.startsWith(s))) return false;
    if (!_projectSearchMatches(r, q)) return false;
    return true;
  });
  _projPage = 0;
  _buildProjGroups(filtered);
  _renderProjPage();
}

function _buildProjGroups(rows) {
  const normalized = rows.map(r => ({
    jobid: r.job_id, name: r.job_name || '', state: r.state || '',
    elapsed: r.elapsed || '', nodes: r.nodes || '', gres: r.gres || '',
    partition: r.partition || '', submitted: r.submitted || '',
    started: r.started || '', started_local: r.started_local || '',
    ended_local: r.ended_local || '', ended_at: r.ended_at || '',
    depends_on: r.depends_on || [], dependents: r.dependents || [],
    dep_details: r.dep_details || [], project: r.project || '',
    project_color: r.project_color || '', project_emoji: r.project_emoji || '',
    reason: r.reason || '', exit_code: r.exit_code || '',
    _cluster: r.cluster, _pinned: true,
  }));

  const byCluster = {};
  for (const j of normalized) {
    if (!byCluster[j._cluster]) byCluster[j._cluster] = [];
    byCluster[j._cluster].push(j);
  }

  _projGroups = [];
  for (const [cluster, clusterJobs] of Object.entries(byCluster)) {
    for (const [label, jobs] of groupJobsByDependency(clusterJobs)) {
      _projGroups.push({ label, cluster, jobs });
    }
  }
  _projGroups.sort((a, b) => {
    const tsA = a.jobs.reduce((best, j) => { const t = j.submitted || j.started || ''; return t > best ? t : best; }, '');
    const tsB = b.jobs.reduce((best, j) => { const t = j.submitted || j.started || ''; return t > best ? t : best; }, '');
    if (tsA !== tsB) return tsA > tsB ? -1 : 1;
    return b.jobs.length - a.jobs.length;
  });
}

function _renderProjPage() {
  const tbody = document.getElementById('project-hist-body');
  const totalGroups = _projGroups.length;
  const totalPages = Math.max(1, Math.ceil(totalGroups / PROJ_GROUPS_PER_PAGE));
  if (_projPage >= totalPages) _projPage = totalPages - 1;
  if (_projPage < 0) _projPage = 0;

  if (!totalGroups) {
    tbody.innerHTML = '<tr><td colspan="11" style="padding:20px;text-align:center;color:var(--muted)">no runs match filters</td></tr>';
    document.getElementById('proj-pagination').innerHTML = '';
    return;
  }

  const start = _projPage * PROJ_GROUPS_PER_PAGE;
  const pageGroups = _projGroups.slice(start, start + PROJ_GROUPS_PER_PAGE);
  const _projGkHL = computeNameHighlight(pageGroups.map(g => g.label));

  let html = '';
  pageGroups.forEach((g, gidx) => {
    const groupJobs = g.jobs;
    const rootJob = groupJobs.find(j => !(j.depends_on || []).length) || groupJobs[0];
    const rootJobId = rootJob.jobid;
    const safeLabel = g.label.replace(/'/g, "\\'");
    const _projColor = groupJobs[0]?.project_color || '';
    const runBadgeStyle = _projColor ? projectBadgeStyle(_projColor) : '';
    const highlightedLabel = highlightJobName(g.label, _projGkHL.prefix, _projGkHL.suffix);
    const runBadge = `<span class="run-name-badge"${runBadgeStyle} onclick="event.stopPropagation();openRunInfo('${g.cluster}','${rootJobId}','${safeLabel}')" title="${g.label.replace(/"/g, '&quot;')}">${highlightedLabel}</span>`;
    const groupLabel = `${runBadge} ${g.cluster} <span class="group-count">· ${groupJobs.length} run${groupJobs.length !== 1 ? 's' : ''}</span>`;
    if (groupJobs.length > 1) {
      html += `<tr class="group-head-row"><td colspan="11" style="padding:4px 16px">${groupLabel}</td></tr>`;
    }
    const idSet = new Set(groupJobs.map(j => j.jobid));
    const byId = {};
    for (const j of groupJobs) byId[j.jobid] = j;
    const depthMemo = {};
    const _projJobNames = groupJobs.map(j => j.name).filter(Boolean);
    const _projJnHL = computeNameHighlight(_projJobNames);

    groupJobs.forEach(j => {
      const st = (j.state || '').toUpperCase();
      const depth = depthInGroup(j, byId, idSet, depthMemo);
      const gpuStr = parseGpus(j.nodes, j.gres) || '—';
      const safeName = (j.name || '').replace(/'/g, "\\'");
      const logBtn = `<button class="action-btn log-btn" onclick="openLog('${g.cluster}','${j.jobid}','${safeName}')">log</button>`;
      const statsBtn = `<button class="action-btn log-btn" onclick="openStats('${g.cluster}','${j.jobid}','${safeName}')">stats</button>`;
      const depBadge = depBadgeHtml(j, byId);
      const indent = depth > 0 ? `<span class="dep-indent" style="padding-left:${depth * 16}px"></span>` : '';
      const depArrow = depth > 0 ? '<span class="dep-arrow">↳</span> ' : '';
      const pinKind = isCompletedState(st) ? 'pinned-completed-row' : (isFailedLikeState(st) ? 'pinned-failed-row' : '');
      const bgClass = groupJobs.length > 1 ? ` group-bg-${(start + gidx) % 4}` : '';
      const started = fmtTime(j.started_local || j.started);
      const ended = fmtTime(j.ended_local || j.ended_at);
      const hasGpu = parseGpus(j.nodes, j.gres) !== null;
      const nameCls = hasGpu ? '' : ' name-cpu';
      const _rowBg = j.project_color ? `background:${lightenColor(j.project_color)}` : '';

      html += `<tr class="hist-compact ${pinKind}${bgClass}" style="${_rowBg}">
        <td><span class="badge">${g.cluster}</span></td>
        <td class="dim">${j.jobid}</td>
        <td class="bold">${indent}${depArrow}<span class="${nameCls}" title="${j.name}">${j.name ? highlightJobName(j.name, _projJnHL.prefix, _projJnHL.suffix) : '—'}</span></td>
        <td>${stateChip(j.state, null, j.reason, j.exit_code)} ${depBadge}</td>
        <td>${logBtn} ${statsBtn}</td>
        <td class="dim">${started}</td>
        <td class="dim">${ended}</td>
        <td class="dim">${j.elapsed || '—'}</td>
        <td class="dim">${gpuStr}</td>
        <td class="dim">${j.partition || '—'}</td>
        <td></td>
      </tr>`;
    });
  });
  tbody.innerHTML = html;

  const pag = document.getElementById('proj-pagination');
  pag.innerHTML = `
    <button onclick="projPrev()" ${_projPage === 0 ? 'disabled' : ''}>← prev</button>
    <span class="page-info">${_projPage + 1} / ${totalPages}</span>
    <button onclick="projNext()" ${_projPage >= totalPages - 1 ? 'disabled' : ''}>next →</button>
    <span style="margin-left:8px;font-size:10px">${totalGroups} groups</span>
  `;
}

function projPrev() { _projPage--; _renderProjPage(); }
function projNext() { _projPage++; _renderProjPage(); }
