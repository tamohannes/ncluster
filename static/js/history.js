// ── History ──
let HIST_GROUPS_PER_PAGE = 50;
let histPage = 0;
let histGroups = [];

async function loadHistory() {
  const cluster = document.getElementById('hist-cluster').value;
  try {
    const res = await fetch(`/api/history?cluster=${cluster}&limit=500`);
    historyData = await res.json();
    histPage = 0;
    _buildHistGroups(historyData);
    _renderHistPage();
  } catch (e) {
    toast('Failed to load history', 'error');
  }
}

function historyGroupKey(r) {
  const n = (r.job_name || '').trim();
  if (!n) return `${r.cluster}:misc`;
  const evalMatch = n.match(/^(eval-[a-z0-9_]+)/i);
  const base = evalMatch ? evalMatch[1].toLowerCase() : n.replace(/(?:-|_)rs\d+\b/i, '').replace(/(?:-|_)(?:judge|summarize[-_]results?).*$/i, '').toLowerCase();
  return `${r.cluster}:${base}`;
}

function _buildHistGroups(rows) {
  // Normalize history rows to look like live job dicts.
  const normalized = rows.map(r => ({
    jobid: r.job_id,
    name: r.job_name || '',
    state: r.state || '',
    elapsed: r.elapsed || '',
    nodes: r.nodes || '',
    gres: r.gres || '',
    partition: r.partition || '',
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
    _cluster: r.cluster,
    _pinned: true,
  }));

  // Group per-cluster, then use the same groupJobsByDependency as the live view
  // (name prefix + dependency chains + topo sort).
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

  // Sort groups by newest job first.
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
  const totalPages = Math.max(1, Math.ceil(totalGroups / HIST_GROUPS_PER_PAGE));
  if (histPage >= totalPages) histPage = totalPages - 1;
  if (histPage < 0) histPage = 0;

  if (!totalGroups) {
    tbody.innerHTML = `<tr><td colspan="10" style="padding:20px;text-align:center;font-family:var(--mono);font-size:11px;color:var(--muted)">no history yet</td></tr>`;
    document.getElementById('hist-pagination').innerHTML = '';
    return;
  }

  const start = histPage * HIST_GROUPS_PER_PAGE;
  const pageGroups = histGroups.slice(start, start + HIST_GROUPS_PER_PAGE);

  let html = '';
  pageGroups.forEach((g, gidx) => {
    const groupJobs = g.jobs;
    const _proj = groupJobs[0]?.project || '';
    const _projColor = groupJobs[0]?.project_color || '';
    const _projEmoji = groupJobs[0]?.project_emoji || '';
    const _projBadge = _proj ? `<span class="group-project-badge" style="background:${_projColor || 'var(--surface)'}">${_projEmoji ? _projEmoji + ' ' : ''}${_proj}</span> ` : '';
    const groupLabel = `${_projBadge}${g.cluster} · ${g.label} <span class="group-count">· ${groupJobs.length} run${groupJobs.length !== 1 ? 's' : ''}</span>`;

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
      const _rowBg = j.project_color ? `background:${lightenColor(j.project_color)}` : '';
      html += `<tr class="hist-compact ${pinKind}${bgClass}" style="${_rowBg}">
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

function _getCheckedStates() {
  const cbs = document.querySelectorAll('#hist-state-filters input[type="checkbox"]');
  const states = [];
  for (const cb of cbs) {
    if (cb.checked) states.push(cb.value);
  }
  return states;
}

function filterHistory() {
  const q = document.getElementById('hist-search').value.toLowerCase();
  const allowedStates = _getCheckedStates();
  const filtered = historyData.filter(r => {
    const st = (r.state || '').toUpperCase().split(' ')[0];
    if (!allowedStates.some(s => st.startsWith(s))) return false;
    if (q && !(r.job_name||'').toLowerCase().includes(q) && !(r.job_id||'').includes(q)) return false;
    return true;
  });
  histPage = 0;
  _buildHistGroups(filtered);
  _renderHistPage();
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

