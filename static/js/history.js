// ── History ──
let HIST_GROUPS_PER_PAGE = 50;
let histPage = 0;
let histGroups = [];

function _histEsc(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function _histValue(id) {
  const el = document.getElementById(id);
  return el ? el.value : '';
}

function _historyUniqueValues(rows, getValue) {
  const seen = new Set();
  for (const row of rows) {
    const value = getValue(row);
    if (value) seen.add(value);
  }
  return Array.from(seen).sort((a, b) => a.localeCompare(b));
}

function _historyOptionLabel(id, value) {
  if (id === 'hist-account') return _shortAcct(value);
  return value;
}

function _historyHasSearchQuery() {
  return !!_histValue('hist-search').trim();
}

function _historyGroupCountLabel(count) {
  return `${count} ${count === 1 ? 'job' : 'jobs'}`;
}

function _syncHistorySelect(id, values, allLabel) {
  const el = document.getElementById(id);
  if (!el) return;
  const current = el.value;
  el.innerHTML = `<option value="">${allLabel}</option>` + values.map(value => (
    `<option value="${_histEsc(value)}">${_histEsc(_historyOptionLabel(id, value))}</option>`
  )).join('');
  if (current && values.includes(current)) el.value = current;
}

function _historyMatches(row, ignore = new Set()) {
  const allowedStates = _getCheckedStates();
  const rowState = (row.state || '').toUpperCase().split(' ')[0];
  if (!ignore.has('state')) {
    if (!allowedStates.length) return false;
    if (!allowedStates.some(state => rowState.startsWith(state))) return false;
  }

  const project = _histValue('hist-project');
  if (!ignore.has('project') && project && (row.project || '') !== project) return false;

  const campaign = _histValue('hist-campaign');
  if (!ignore.has('campaign') && campaign && (row.campaign || '') !== campaign) return false;

  const partition = _histValue('hist-partition');
  if (!ignore.has('partition') && partition && (row.partition || '') !== partition) return false;

  const account = _histValue('hist-account');
  if (!ignore.has('account') && account && (row.account || '') !== account) return false;

  const q = _histValue('hist-search').trim().toLowerCase();
  if (!ignore.has('search') && q) {
    const values = [
      row.job_name || row.name || '',
      String(row.job_id || row.jobid || ''),
      groupKeyForJob(row.job_name || row.name || ''),
      row.project || '',
      row.campaign || '',
      row.partition || '',
      row.account || '',
      row.cluster || row._cluster || '',
    ].map(v => String(v).toLowerCase());
    if (!values.some(value => value.includes(q))) return false;
  }

  return true;
}

function _syncHistoryFacetOptions() {
  _syncHistorySelect(
    'hist-project',
    _historyUniqueValues(historyData.filter(row => _historyMatches(row, new Set(['project']))), row => row.project || ''),
    'All projects',
  );
  _syncHistorySelect(
    'hist-campaign',
    _historyUniqueValues(historyData.filter(row => _historyMatches(row, new Set(['campaign']))), row => row.campaign || ''),
    'All campaigns',
  );
  _syncHistorySelect(
    'hist-partition',
    _historyUniqueValues(historyData.filter(row => _historyMatches(row, new Set(['partition']))), row => row.partition || ''),
    'All partitions',
  );
  _syncHistorySelect(
    'hist-account',
    _historyUniqueValues(historyData.filter(row => _historyMatches(row, new Set(['account']))), row => row.account || ''),
    'All accounts',
  );
}

async function loadHistory() {
  const cluster = _histValue('hist-cluster') || 'all';
  const days = _histValue('hist-days');
  const params = new URLSearchParams({ cluster, limit: '10000' });
  if (days && days !== 'all') params.set('days', days);
  try {
    const res = await fetch(`/api/history?${params.toString()}`);
    const rows = await res.json();
    historyData = Array.isArray(rows) ? rows : [];
    histPage = 0;
    filterHistory();
  } catch (e) {
    toast('Failed to load history', 'error');
  }
}

function historyGroupKey(r) {
  return `${r.cluster}:${groupKeyForJob(r.job_name || r.name || '')}`;
}

function _buildHistGroups(rows) {
  const normalized = rows.map(r => ({
    jobid: r.job_id,
    name: r.job_name || '',
    state: r.state || '',
    elapsed: r.elapsed || '',
    nodes: r.nodes || '',
    gres: r.gres || '',
    partition: r.partition || '',
    account: r.account || '',
    campaign: r.campaign || '',
    submitted: r.submitted || '',
    started: r.started || '',
    started_local: r.started_local || '',
    ended_local: r.ended_local || '',
    ended_at: r.ended_at || '',
    depends_on: r.depends_on || [],
    dependents: r.dependents || [],
    dep_details: r.dep_details || [],
    project: r.project || '',
    project_color: r.project_color || '',
    project_emoji: r.project_emoji || '',
    reason: r.reason || '',
    exit_code: r.exit_code || '',
    _cluster: r.cluster,
    _pinned: true,
  }));

  const byCluster = {};
  for (const j of normalized) {
    if (!byCluster[j._cluster]) byCluster[j._cluster] = [];
    byCluster[j._cluster].push(j);
  }

  histGroups = [];
  for (const [cluster, clusterJobs] of Object.entries(byCluster)) {
    const groups = groupJobsByDependency(clusterJobs);
    for (const [label, jobs] of groups) {
      histGroups.push({ label, cluster, jobs });
    }
  }

  histGroups.sort((a, b) => {
    const tsA = a.jobs.reduce((best, j) => { const t = j.submitted || j.started || ''; return t > best ? t : best; }, '');
    const tsB = b.jobs.reduce((best, j) => { const t = j.submitted || j.started || ''; return t > best ? t : best; }, '');
    if (tsA !== tsB) return tsA > tsB ? -1 : 1;
    return b.jobs.length - a.jobs.length;
  });
}

function _renderHistPage() {
  const tbody = document.getElementById('hist-body');
  const totalGroups = histGroups.length;
  const searchOnlyRuns = _historyHasSearchQuery();
  const totalPages = Math.max(1, Math.ceil(totalGroups / HIST_GROUPS_PER_PAGE));
  if (histPage >= totalPages) histPage = totalPages - 1;
  if (histPage < 0) histPage = 0;

  if (!totalGroups) {
    tbody.innerHTML = `<tr><td colspan="11" style="padding:20px;text-align:center;font-family:var(--mono);font-size:11px;color:var(--muted)">no history yet</td></tr>`;
    document.getElementById('hist-pagination').innerHTML = '';
    return;
  }

  const start = histPage * HIST_GROUPS_PER_PAGE;
  const pageGroups = histGroups.slice(start, start + HIST_GROUPS_PER_PAGE);
  const _histGkHL = computeNameHighlight(pageGroups.map(g => g.label));

  let html = '';
  pageGroups.forEach((g, gidx) => {
    const groupJobs = g.jobs;
    const _proj = groupJobs[0]?.project || '';
    const _projColor = groupJobs[0]?.project_color || '';
    const _projEmoji = groupJobs[0]?.project_emoji || '';
    const _campaign = groupJobs[0]?.campaign || '';
    const _projBadge = _proj ? `<span class="group-project-badge">${_projEmoji ? _projEmoji + ' ' : ''}${_proj}</span>` : '';
    const rootJob = groupJobs.find(j => !(j.depends_on || []).length) || groupJobs[0];
    const rootJobId = rootJob.jobid;
    const safeLabel = g.label.replace(/'/g, "\\'");
    const _shadedColor = _projColor && _campaign ? campaignShade(_projColor, _campaign) : _projColor;
    const runBadgeStyle = _shadedColor ? projectBadgeStyle(_shadedColor) : '';
    const highlightedLabel = highlightJobName(g.label, _histGkHL.prefix, _histGkHL.suffix);
    const runBadge = `<span class="run-name-badge"${runBadgeStyle} onclick="event.stopPropagation();openRunInfo('${g.cluster}','${rootJobId}','${safeLabel}')" title="${g.label.replace(/"/g, '&quot;')}">${highlightedLabel}</span>`;
    const hasMultiple = groupJobs.length > 1;
    const groupId = `${g.cluster}:${rootJobId}`;
    const isGroupExpanded = _expandedGroups.has(groupId);
    if (searchOnlyRuns || hasMultiple) {
      const showChevron = hasMultiple && !searchOnlyRuns;
      const chevronCls = showChevron && isGroupExpanded ? ' expanded' : '';
      const chevronHtml = showChevron ? `<span class="group-chevron${chevronCls}" data-group-chevron="${groupId}">&#9654;</span>` : '';
      const donutHtml = statusDonut(groupJobs);
      const summaryHtml = statusSummaryHtml(groupJobs, g.cluster);
      const rowAction = searchOnlyRuns ? `openRunInfo('${g.cluster}','${rootJobId}','${safeLabel}')` : `toggleRunGroup('${groupId}')`;
      const groupLabel = `<span>${chevronHtml}${donutHtml}${runBadge}${_projBadge} ${g.cluster} ${summaryHtml} <span class="group-count">· ${_historyGroupCountLabel(groupJobs.length)}</span></span>`;
      html += `<tr class="group-head-row${searchOnlyRuns ? ' search-only' : ''}" onclick="${rowAction}"><td colspan="11" style="padding:4px 16px"><span class="group-head-content">${groupLabel}</span></td></tr>`;
    }

    if (searchOnlyRuns) {
      return;
    }

    const idSet = new Set(groupJobs.map(j => j.jobid));
    const byId = {};
    for (const j of groupJobs) byId[j.jobid] = j;
    const depthMemo = {};
    const _histJobNames = groupJobs.map(j => j.name).filter(Boolean);
    const _histJnHL = computeNameHighlight(_histJobNames);

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
      const pinKind = isSoftFail(j.state, j.reason) ? 'pinned-softfail-row' : isCompletedState(st) ? 'pinned-completed-row' : (isFailedLikeState(st) ? 'pinned-failed-row' : '');
      const bgClass = groupJobs.length > 1 ? ` group-bg-${(start + gidx) % 4}` : '';
      const started = fmtTime(j.started_local || j.started);
      const ended = fmtTime(j.ended_local || j.ended_at);

      const hasGpu = parseGpus(j.nodes, j.gres) !== null;
      const nameCls = hasGpu ? '' : ' name-cpu';
      const _rowShaded = j.project_color && j.campaign ? campaignShade(j.project_color, j.campaign) : (j.project_color || '');
      const _rowBg = _rowShaded ? `background:${lightenColor(_rowShaded)}` : '';
      const _grpHidden = hasMultiple && !isGroupExpanded;
      const _rowDisp = _grpHidden ? 'display:none' : '';
      const _rowStyle = [_rowBg, _rowDisp].filter(Boolean).join(';');
      const _grpAttr = hasMultiple ? ` data-run-group="${groupId}"` : '';
      html += `<tr class="hist-compact ${pinKind}${bgClass}"${_grpAttr} style="${_rowStyle}">
        <td><span class="badge">${g.cluster}</span></td>
        <td class="dim">${j.jobid}</td>
        <td class="bold">${indent}${depArrow}<span class="${nameCls}" title="${j.name}">${j.name ? highlightJobName(j.name, _histJnHL.prefix, _histJnHL.suffix) : '—'}</span></td>
        <td>${stateChip(j.state, null, j.reason, j.exit_code)} ${depBadge}</td>
        <td>${logBtn} ${statsBtn}</td>
        <td class="dim">${started}</td>
        <td class="dim">${ended}</td>
        <td class="dim">${j.elapsed || '—'}</td>
        <td class="dim">${gpuStr}</td>
        <td class="dim">${j.partition || '—'}</td>
        <td class="dim acct-cell">${_shortAcct(j.account || '') || '—'}</td>
      </tr>`;
    });
  });
  tbody.innerHTML = html;

  const pag = document.getElementById('hist-pagination');
  pag.innerHTML = `
    <button onclick="histPrev()" ${histPage === 0 ? 'disabled' : ''}>← prev</button>
    <span class="page-info">${histPage + 1} / ${totalPages}</span>
    <button onclick="histNext()" ${histPage >= totalPages - 1 ? 'disabled' : ''}>next →</button>
    <span style="margin-left:8px;font-size:10px">${totalGroups} groups</span>
  `;
}

function histPrev() { histPage--; _renderHistPage(); }
function histNext() { histPage++; _renderHistPage(); }

function toggleStateFilter(btn) {
  btn.classList.toggle('active');
  filterHistory();
}

function _getCheckedStates() {
  const btns = document.querySelectorAll('#hist-state-filters .hist-state-btn.active');
  return Array.from(btns).map(b => b.dataset.state);
}

function filterHistory() {
  _syncHistoryFacetOptions();
  const filtered = historyData.filter(row => _historyMatches(row));
  histPage = 0;
  _buildHistGroups(filtered);
  _renderHistPage();
}

function resetHistoryFilters() {
  document.getElementById('hist-cluster').value = 'all';
  document.getElementById('hist-days').value = 'all';
  document.getElementById('hist-project').value = '';
  document.getElementById('hist-campaign').value = '';
  document.getElementById('hist-partition').value = '';
  document.getElementById('hist-account').value = '';
  document.getElementById('hist-search').value = '';
  document.querySelectorAll('#hist-state-filters .hist-state-btn').forEach(btn => btn.classList.add('active'));
  loadHistory();
}

async function cleanupHistory() {
  const days = parseInt(document.getElementById('cleanup-days').value) || 30;
  if (!confirm(`Delete all history records older than ${days} days and remove their local log files?\n\nThis cannot be undone.`)) return;
  try {
    const res = await fetch('/api/cleanup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ days }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      toast(`Cleaned ${d.deleted_records} records, ${d.cleaned_dirs} files`);
      loadHistory();
    } else {
      toast(d.error || 'Cleanup failed', 'error');
    }
  } catch (e) {
    toast('Cleanup failed', 'error');
  }
}

function showClusterHistory(name) {
  document.getElementById('hist-cluster').value = name;
  showTab('history');
}

