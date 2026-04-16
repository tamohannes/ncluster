// ── Projects ──

let _projData = [];
let _projGroups = [];
let _projLiveJobs = [];
let _projPage = 0;
let _projCurrentName = '';
let _projRefreshTimer = null;
let _projSearchTimer = null;
let _projHistLoadSeq = 0;
const PROJ_GROUPS_PER_PAGE = 50;

function _projSearchStorageKey(projectName) {
  return `clausius.projectSearch.${projectName || ''}`;
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

function _projHasSearchQuery() {
  const el = document.getElementById('proj-search');
  return !!(el && el.value.trim());
}

function projectSearchChanged() {
  const value = document.getElementById('proj-search')?.value || '';
  _saveProjectSearch(_projCurrentName, value);
  if (_projSearchTimer) clearTimeout(_projSearchTimer);
  _projSearchTimer = setTimeout(() => {
    _projSearchTimer = null;
    if (_archivedVisible || _projHistLoaded) {
      _fetchProjectHistory(false);
    } else {
      filterProjectRuns();
    }
  }, 180);
}

function _highlightProjectBtn(projectName) {
  document.querySelectorAll('.nav-project-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.project === projectName);
  });
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
      const color = rawColor ? (_isDarkTheme() ? darkenColor(rawColor, 0.6) : lightenColor(rawColor, 0.5)) : 'var(--surface)';
      return `<button class="nav-project-btn" data-project="${p.project}" style="--proj-color:${color}" onclick="navClick(event,'project','${p.project}','${p.project}')">${emoji ? emoji + ' ' : ''}${p.project}</button>`;
    }).join('');
  } catch (_) {}
}

async function openProject(projectName, fromTab) {
  _projCurrentName = projectName;
  _projHistLoaded = false;
  _archivedVisible = false;
  const archWrap = document.getElementById('proj-archive-wrap');
  if (archWrap) archWrap.style.display = 'none';
  _projData = [];
  _projGroups = [];
  try { sessionStorage.setItem('clausius.activeProject', projectName); } catch (_) {}
  if (!fromTab) {
    const at = _appTabs.find(t => t.id === _activeTabId);
    if (at) {
      at.type = 'project';
      at.label = projectName;
      at.project = projectName;
    }
    _activateView('project');
    _renderAppTabs();
    _persistTabs();
  }
  _highlightProjectBtn(projectName);
  const projCfg = await fetch('/api/settings').then(r => r.json()).then(c => (c.projects || {})[projectName] || {}).catch(() => ({}));
  const emoji = projCfg.emoji || '';
  document.getElementById('project-detail-title').textContent = `${emoji ? emoji + ' ' : ''}${projectName}`;
  document.getElementById('proj-search').value = _restoreProjectSearch(projectName);
  document.querySelectorAll('#proj-state-filters .hist-state-btn').forEach(b => b.classList.add('active'));

  const hasCachedData = _projCurrentName === projectName && (_projLiveJobs.length > 0 || _projGroups.length > 0);
  if (!hasCachedData) {
    document.getElementById('proj-stats-bar').innerHTML = '<span class="proj-stat-lbl">loading…</span>';
    document.getElementById('proj-live-section').innerHTML = '';
    const histCards = document.getElementById('proj-hist-cards');
    if (histCards) histCards.innerHTML = '<div class="no-jobs">loading…</div>';
  }

  _fetchProjectData(!hasCachedData);

  if (_projRefreshTimer) clearInterval(_projRefreshTimer);
  _projRefreshTimer = setInterval(() => {
    if (document.hidden) return;
    if (currentTab === 'project') _fetchProjectData(false);
  }, 30000);
}

async function refreshProjectPage() {
  if (_projCurrentName) await _fetchProjectData(true);
}

let _projHistLoaded = false;

async function _fetchProjectData(showToast) {
  const name = _projCurrentName;
  if (!name) return;

  const t = showToast ? toastLoading(`Loading ${name}…`) : null;

  try {
    const cachedLive = (typeof allData !== 'undefined' && Object.keys(allData).length) ? allData : null;
    const liveRes = cachedLive || await fetch('/api/jobs').then(r => r.json()).catch(() => ({}));

    const newJobs = [];
    const clusterActivity = {};
    if (typeof liveRes === 'object' && !Array.isArray(liveRes)) {
      for (const [cname, cdata] of Object.entries(liveRes)) {
        if (!cdata || cdata.status !== 'ok') continue;
        for (const j of (cdata.jobs || [])) {
          if (j.project === name) {
            newJobs.push({ ...j, _cluster: cname });
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
    if (newJobs.length || !_projLiveJobs.length) _projLiveJobs = newJobs;

    _renderProjStats(clusterActivity);
    _renderProjLive();

    const liveCount = _projLiveJobs.filter(j => !j._pinned).length;
    if (t) t.done(`${name}: ${liveCount} live jobs`);
  } catch (e) {
    if (t) t.done(`Failed to load ${name}`, 'error');
  }
}

async function _fetchProjectHistory(showToast = true) {
  const name = _projCurrentName;
  if (!name) return;
  const seq = ++_projHistLoadSeq;
  const q = (document.getElementById('proj-search')?.value || '').trim();
  _saveProjectSearch(name, q);
  const t = showToast ? toastLoading(`Loading archived runs…`) : null;
  try {
    const params = buildHistoryQueryParams({ project: name, q, limit: 10000 });
    const histRes = await fetch(`/api/history?${params.toString()}`).then(r => r.json()).catch(() => []);
    if (seq !== _projHistLoadSeq) return;
    const activeLiveIds = new Set(_projLiveJobs.filter(j => !j._pinned).map(j => String(j.jobid)));
    _projData = (Array.isArray(histRes) ? histRes : []).filter(r => !activeLiveIds.has(String(r.job_id)));
    _projPage = 0;
    _projHistLoaded = true;
    filterProjectRuns();
    if (t) t.done(`${_projData.length} archived runs loaded`);
  } catch (e) {
    if (seq !== _projHistLoadSeq) return;
    if (t) t.done('Failed to load archived runs', 'error');
  }
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
    <span class="proj-stat"><span class="proj-stat-val" style="color:var(--yellow)">${pending}</span><span class="proj-stat-lbl">pending</span></span>
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
  const name = _projCurrentName;

  // Build filtered data: for each cluster, only include this project's jobs
  const filteredData = {};
  const source = (typeof allData !== 'undefined' && Object.keys(allData).length) ? allData : {};
  for (const [cluster, cdata] of Object.entries(source)) {
    if (!cdata || cdata.status !== 'ok') continue;
    const projJobs = (cdata.jobs || []).filter(j => j.project === name);
    if (projJobs.length) {
      filteredData[cluster] = { ...cdata, jobs: projJobs };
    }
  }

  const hasLive = Object.values(filteredData).some(d => d.jobs.some(j => !j._pinned));

  if (!Object.keys(filteredData).length) {
    el.innerHTML = '';
  } else {
    // Reuse the Live dashboard's renderCard for each cluster
    const clusterNames = Object.keys(filteredData).sort();
    el.innerHTML = `<div class="proj-live-label">● live jobs</div><div class="grid${clusterNames.length === 1 ? ' single-card' : ''}">${clusterNames.map(c => renderCard(c, filteredData[c])).join('')}</div>`;
  }

  const sep = document.getElementById('proj-archive-sep');
  if (sep) {
    const chevron = _archivedVisible ? '▾' : '▸';
    sep.innerHTML = `${chevron} archived runs`;
  }
}

let _archivedVisible = false;

function toggleArchivedRuns() {
  _archivedVisible = !_archivedVisible;
  const wrap = document.getElementById('proj-archive-wrap');
  const sep = document.getElementById('proj-archive-sep');
  if (wrap) wrap.style.display = _archivedVisible ? '' : 'none';
  if (sep) {
    const chevron = _archivedVisible ? '▾' : '▸';
    sep.innerHTML = `${chevron} archived runs`;
  }
  if (_archivedVisible && !_projHistLoaded) {
    _fetchProjectHistory();
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

function _projectSearchMatches(row, query) {
  return historySearchMatchesRow(row, query);
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
  const normalized = rows.map(normalizeHistoryJobRow);

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
  const container = document.getElementById('proj-hist-cards');
  if (!container) return;
  const totalGroups = _projGroups.length;
  const searchOnlyRuns = _projHasSearchQuery();
  const totalPages = Math.max(1, Math.ceil(totalGroups / PROJ_GROUPS_PER_PAGE));
  if (_projPage >= totalPages) _projPage = totalPages - 1;
  if (_projPage < 0) _projPage = 0;

  if (!totalGroups) {
    container.innerHTML = '<div class="no-jobs" style="padding:20px;text-align:center">no runs match filters</div>';
    document.getElementById('proj-pagination').innerHTML = '';
    return;
  }

  const start = _projPage * PROJ_GROUPS_PER_PAGE;
  const pageGroups = _projGroups.slice(start, start + PROJ_GROUPS_PER_PAGE);

  // Group page groups by cluster
  const byCluster = {};
  for (const g of pageGroups) {
    if (!byCluster[g.cluster]) byCluster[g.cluster] = [];
    byCluster[g.cluster].push(g);
  }

  let html = '';
  for (const [cluster, groups] of Object.entries(byCluster)) {
    const gpuLabel = clusterGpuBadge(cluster);
    const gpuBadge = gpuLabel ? `<span class="avail-gpu-badge">${gpuLabel}</span>` : '';
    const totalJobs = groups.reduce((s, g) => s + g.jobs.length, 0);

    html += `<div class="proj-hist-card"><div class="proj-hist-card-head">${cluster} ${gpuBadge} <span class="group-count">${groups.length} runs · ${totalJobs} jobs</span></div>`;
    html += `<table class="proj-compact-table"><thead><tr><th>ID</th><th>Name</th><th>State</th><th>Logs/Stats</th><th>Start</th><th>End</th><th>Elapsed</th><th>GPUs</th><th>Partition</th><th>Account</th></tr></thead><tbody>`;

    const _clusterGkHL = computeNameHighlight(groups.map(g => g.label));

    groups.forEach((g, gidx) => {
      const groupJobs = g.jobs;
      const rootJob = groupJobs.find(j => !(j.depends_on || []).length) || groupJobs[0];
      const rootJobId = rootJob.jobid;
      const safeLabel = g.label.replace(/'/g, "\\'");
      const _projColor = groupJobs[0]?.project_color || '';
      const runBadgeStyle = _projColor ? projectBadgeStyle(_projColor) : '';
      const highlightedLabel = highlightJobName(g.label, _clusterGkHL.prefix, _clusterGkHL.suffix);
      const runDataAttrs = ` data-run-cluster="${escAttr(cluster)}" data-run-root="${escAttr(String(rootJobId))}"`;
      const runBadge = `<span class="run-name-badge${rootJob.starred ? ' run-name-badge--starred' : ''}"${runDataAttrs}${runBadgeStyle} onclick="event.stopPropagation();openRunInfo('${cluster}','${rootJobId}','${safeLabel}')" title="${g.label.replace(/"/g, '&quot;')}">${highlightedLabel}</span>`;
      const hasMultiple = groupJobs.length > 1;
      const groupId = `${cluster}:${rootJobId}`;
      const isGroupExpanded = _expandedGroups.has(groupId);

      const showChevron = hasMultiple;
      const chevronCls = showChevron && isGroupExpanded ? ' expanded' : '';
      const chevronHtml = showChevron ? `<span class="group-chevron${chevronCls}" data-group-chevron="${groupId}">&#9654;</span>` : '';
      const donutHtml = statusDonut(groupJobs);
      const summaryHtml = statusSummaryHtml(groupJobs, cluster);
      const groupLabel = `<span>${chevronHtml}${donutHtml}${runBadge} ${summaryHtml} <span class="group-count">· ${groupJobs.length} job${groupJobs.length > 1 ? 's' : ''}</span></span>`;
      const rowAction = hasMultiple ? `toggleRunGroup('${groupId}')` : `openRunInfo('${cluster}','${rootJobId}','${safeLabel}')`;
      html += `<tr class="group-head-row${searchOnlyRuns ? ' search-only' : ''}" onclick="${rowAction}"><td colspan="10"><span class="group-head-content">${groupLabel}</span></td></tr>`;

      if (searchOnlyRuns && !hasMultiple) {
        return;
      }

      const idSet = new Set(groupJobs.map(j => j.jobid));
      const byId = {};
      for (const j of groupJobs) byId[j.jobid] = j;
      const depthMemo = {};
      const _jnHL = computeNameHighlight(groupJobs.map(j => j.name).filter(Boolean));

      for (const j of groupJobs) {
        const st = (j.state || '').toUpperCase();
        const depth = depthInGroup(j, byId, idSet, depthMemo);
        const gpuStr = parseGpus(j.nodes, j.gres) || '—';
        const safeName = (j.name || '').replace(/'/g, "\\'");
        const logBtn = `<button class="action-btn log-btn" onclick="openLog('${cluster}','${j.jobid}','${safeName}')">log</button>`;
        const statsBtn = `<button class="action-btn log-btn" onclick="openStats('${cluster}','${j.jobid}','${safeName}')">stats</button>`;
        const depBadge = depBadgeHtml(j, byId);
        const indent = depth > 0 ? `<span class="dep-indent" style="padding-left:${depth * 16}px"></span>` : '';
        const depArrow = depth > 0 ? '<span class="dep-arrow">↳</span> ' : '';
        const pinKind = isSoftFail(j.state, j.reason) ? 'pinned-softfail-row' : isCompletedState(st) ? 'pinned-completed-row' : (isFailedLikeState(st) ? 'pinned-failed-row' : '');
        const started = fmtTime(j.started_local || j.started);
        const ended = fmtTime(j.ended_local || j.ended_at);
        const nameCls = parseGpus(j.nodes, j.gres) !== null ? '' : ' name-cpu';
        const _grpHidden = hasMultiple && !isGroupExpanded;
        const _rowDisp = _grpHidden ? 'display:none' : '';
        const _grpAttr = ` data-run-group="${groupId}"`;

        html += `<tr class="hist-compact ${pinKind}"${_grpAttr} style="${_rowDisp}">
          <td class="dim">${j.jobid}</td>
          <td class="bold">${indent}${depArrow}<span class="${nameCls}" title="${j.name}">${j.name ? highlightJobName(j.name, _jnHL.prefix, _jnHL.suffix) : '—'}</span></td>
          <td>${stateChip(j.state, null, j.reason, j.exit_code)} ${depBadge}</td>
          <td>${logBtn} ${statsBtn}</td>
          <td class="dim">${started}</td>
          <td class="dim">${ended}</td>
          <td class="dim">${j.elapsed || '—'}</td>
          <td class="dim">${gpuStr}</td>
          <td class="dim">${j.partition || '—'}</td>
          <td class="dim acct-cell">${_shortAcct(j.account || '') || '—'}</td>
        </tr>`;
      }
    });

    html += '</tbody></table></div>';
  }
  container.innerHTML = html;

  const pag = document.getElementById('proj-pagination');
  pag.innerHTML = `
    <button onclick="projPrev()" ${_projPage === 0 ? 'disabled' : ''}>← prev</button>
    <span class="page-info">${_projPage + 1} / ${totalPages}</span>
    <button onclick="projNext()" ${_projPage >= totalPages - 1 ? 'disabled' : ''}>next →</button>
    <span style="margin-left:8px;font-size:10px">${totalGroups} runs</span>
  `;
}

function projPrev() { _projPage--; _renderProjPage(); }
function projNext() { _projPage++; _renderProjPage(); }
