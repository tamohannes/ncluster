// ── Projects ──

let _projData = [];
let _projGroups = [];
let _projPage = 0;
let _projCurrentName = '';
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
  // Reset state filter buttons to all active
  document.querySelectorAll('#proj-state-filters .hist-state-btn').forEach(b => b.classList.add('active'));

  const tbody = document.getElementById('project-hist-body');
  tbody.innerHTML = '<tr><td colspan="10" style="padding:20px;text-align:center;color:var(--muted)">loading…</td></tr>';

  try {
    const res = await fetch(`/api/history?project=${encodeURIComponent(projectName)}&limit=500`);
    _projData = await res.json();
    _projPage = 0;
    _buildProjGroups(_projData);
    _renderProjPage();
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="10" style="padding:20px;color:var(--red)">Failed: ${e}</td></tr>`;
  }
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
    project_color: r.project_color || '', project_emoji: r.project_emoji || '',
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
    tbody.innerHTML = '<tr><td colspan="10" style="padding:20px;text-align:center;color:var(--muted)">no runs match filters</td></tr>';
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
      html += `<tr class="group-head-row"><td colspan="10" style="padding:4px 16px">${groupLabel}</td></tr>`;
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
