// ── Projects ──

let _projData = [];
let _projGroups = [];
let _projLiveJobs = [];
let _projPage = 0;
let _projCurrentName = '';
let _projRefreshTimer = null;
const PROJ_GROUPS_PER_PAGE = 50;

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
      const color = p.color || 'var(--surface)';
      return `<button class="nav-project-btn" style="border-color:${color}" onclick="openProject('${p.project}')">${emoji ? emoji + ' ' : ''}${p.project}</button>`;
    }).join('');
  } catch (_) {}
}

async function openProject(projectName) {
  _projCurrentName = projectName;
  showTab('project');
  const projCfg = await fetch('/api/settings').then(r => r.json()).then(c => (c.projects || {})[projectName] || {}).catch(() => ({}));
  const emoji = projCfg.emoji || '';
  document.getElementById('project-detail-title').textContent = `${emoji ? emoji + ' ' : ''}${projectName}`;
  document.getElementById('proj-search').value = '';
  document.querySelectorAll('#proj-state-filters .hist-state-btn').forEach(b => b.classList.add('active'));

  document.getElementById('proj-stats-bar').innerHTML = '<span class="proj-stat-lbl">loading…</span>';
  document.getElementById('proj-live-section').innerHTML = '';
  document.getElementById('project-hist-body').innerHTML = '<tr><td colspan="11" style="padding:20px;text-align:center;color:var(--muted)">loading…</td></tr>';

  await _fetchProjectData();

  if (_projRefreshTimer) clearInterval(_projRefreshTimer);
  _projRefreshTimer = setInterval(() => {
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

  // Extract live jobs for this project
  _projLiveJobs = [];
  const clusterActivity = {};
  if (typeof liveRes === 'object' && !Array.isArray(liveRes)) {
    for (const [cname, cdata] of Object.entries(liveRes)) {
      if (!cdata || cdata.status !== 'ok') continue;
      for (const j of (cdata.jobs || [])) {
        if (j.project === name && !j._pinned) {
          _projLiveJobs.push({ ...j, _cluster: cname });
          const st = (j.state || '').toUpperCase();
          if (!clusterActivity[cname]) clusterActivity[cname] = { running: 0, pending: 0 };
          if (st === 'RUNNING' || st === 'COMPLETING') clusterActivity[cname].running++;
          else if (st === 'PENDING') clusterActivity[cname].pending++;
        }
      }
    }
  }

  _projData = Array.isArray(histRes) ? histRes : [];
  _projPage = 0;

  _renderProjStats(clusterActivity);
  _renderProjLive();
  _buildProjGroups(_projData);
  _renderProjPage();
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
  if (!_projLiveJobs.length) {
    el.innerHTML = '';
    return;
  }

  const byCluster = {};
  for (const j of _projLiveJobs) {
    if (!byCluster[j._cluster]) byCluster[j._cluster] = [];
    byCluster[j._cluster].push(j);
  }

  let html = '<div class="proj-live-label">● live jobs</div>';
  for (const [cluster, jobs] of Object.entries(byCluster)) {
    const groups = groupJobsByDependency(jobs);
    for (const [gk, groupJobs] of groups) {
      const idSet = new Set(groupJobs.map(j => j.jobid));
      const byId = {};
      for (const j of groupJobs) byId[j.jobid] = j;
      const depthMemo = {};

      const groupLabel = `${cluster} · ${gk} <span class="group-count">· ${groupJobs.length} run${groupJobs.length !== 1 ? 's' : ''}</span>`;
      let rows = `<tr class="group-head-row"><td colspan="11">${groupLabel}</td></tr>`;

      for (const j of groupJobs) {
        const st = (j.state || '').toUpperCase();
        const depth = depthInGroup(j, byId, idSet, depthMemo);
        const gpuStr = parseGpus(j.nodes, j.gres);
        const resourceCell = gpuStr
          ? `<span style="color:var(--text);font-weight:500">${gpuStr}</span>`
          : `<span class="dim">${j.nodes || '—'}n</span>`;
        const startTime = fmtTime(j.started_local || j.started || j.start);
        const safeName = (j.name || '').replace(/'/g, "\\'");
        const isPending = st === 'PENDING';
        const logBtn = isPending ? '' : `<button class="action-btn log-btn" onclick="openLog('${cluster}','${j.jobid}','${safeName}')">log</button>`;
        const statsBtn = isPending ? '' : (cluster === 'local'
          ? ''
          : `<button class="action-btn log-btn" onclick="openStats('${cluster}','${j.jobid}','${safeName}')">stats</button>`);
        const cancelBtn = `<button class="action-btn" onclick="cancelJob('${cluster}','${j.jobid}')">cancel</button>`;
        const depBadge = depBadgeHtml(j, byId);
        const indent = depth > 0 ? `<span class="dep-indent" style="padding-left:${depth * 16}px"></span>` : '';
        const depArrow = depth > 0 ? '<span class="dep-arrow">↳</span> ' : '';
        const hasGpu = !!gpuStr;
        const nameCls = hasGpu ? '' : ' name-cpu';

        rows += `<tr>
          <td class="dim">${j.jobid}</td>
          <td class="bold">${indent}${depArrow}<span class="${nameCls}" title="${j.name}">${j.name}</span></td>
          <td>${stateChip(j.state, j.progress)} ${depBadge}</td>
          <td>${logBtn} ${statsBtn}</td>
          <td class="dim">${startTime}</td>
          <td class="dim">—</td>
          <td class="dim">${j.elapsed || '—'}</td>
          <td>${resourceCell}</td>
          <td class="dim">${j.partition || '—'}</td>
          <td>${cancelBtn}</td>
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

function filterProjectRuns() {
  const q = document.getElementById('proj-search').value.toLowerCase();
  const allowedStates = _getProjCheckedStates();
  const filtered = _projData.filter(r => {
    const st = (r.state || '').toUpperCase().split(' ')[0];
    if (!allowedStates.some(s => st.startsWith(s))) return false;
    if (q && !(r.job_name||'').toLowerCase().includes(q) && !(r.job_id||'').includes(q)) return false;
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

  let html = '';
  pageGroups.forEach((g, gidx) => {
    const groupJobs = g.jobs;
    const groupLabel = `${g.cluster} · ${g.label} <span class="group-count">· ${groupJobs.length} run${groupJobs.length !== 1 ? 's' : ''}</span>`;
    if (groupJobs.length > 1) {
      html += `<tr class="group-head-row"><td colspan="11" style="padding:4px 16px">${groupLabel}</td></tr>`;
    }
    const idSet = new Set(groupJobs.map(j => j.jobid));
    const byId = {};
    for (const j of groupJobs) byId[j.jobid] = j;
    const depthMemo = {};

    groupJobs.forEach(j => {
      const st = (j.state || '').toUpperCase();
      const depth = depthInGroup(j, byId, idSet, depthMemo);
      const gpuStr = parseGpus(j.nodes, j.gres) || '—';
      const safeName = (j.name || '').replace(/'/g, "\\'");
      const logBtn = `<button class="action-btn log-btn" onclick="openLog('${g.cluster}','${j.jobid}','${safeName}')">log</button>`;
      const depBadge = depBadgeHtml(j, byId);
      const indent = depth > 0 ? `<span class="dep-indent" style="padding-left:${depth * 16}px"></span>` : '';
      const depArrow = depth > 0 ? '<span class="dep-arrow">↳</span> ' : '';
      const pinKind = isCompletedState(st) ? 'pinned-completed-row' : (isFailedLikeState(st) ? 'pinned-failed-row' : '');
      const bgClass = groupJobs.length > 1 ? ` group-bg-${(start + gidx) % 4}` : '';
      const started = fmtTime(j.started_local || j.started);
      const ended = fmtTime(j.ended_local || j.ended_at);
      const hasGpu = parseGpus(j.nodes, j.gres) !== null;
      const nameCls = hasGpu ? '' : ' name-cpu';

      html += `<tr class="hist-compact ${pinKind}${bgClass}">
        <td><span class="badge">${g.cluster}</span></td>
        <td class="dim">${j.jobid}</td>
        <td class="bold">${indent}${depArrow}<span class="${nameCls}" title="${j.name}">${j.name || '—'}</span></td>
        <td>${stateChip(j.state)} ${depBadge}</td>
        <td>${logBtn}</td>
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
