const CLUSTERS = JSON.parse(document.getElementById('cluster-data').textContent || '{}');
const USERNAME = (document.getElementById('username-data')?.textContent || '').trim();
const TEAM = (document.getElementById('team-data')?.textContent || '').trim();
let allData = {};
let historyData = [];
let countdown = 20;
let refreshIntervalSec = 20;
let cdTimer;
let currentTab = 'live';
let _isResizingTree = false;
let _isResizingNav = false;
let navCollapsed = false;

/* ── Cluster utilization data (from external dashboard) ── */
let _clusterUtil = null;
let _clusterUtilFetching = false;
const sectionCollapsed = {
  local: false,
  idle: false,
  unreachable: false,
};
try {
  const saved = JSON.parse(localStorage.getItem('ncluster.sectionCollapsed') || '{}');
  Object.assign(sectionCollapsed, saved);
} catch (_) {}
try {
  navCollapsed = localStorage.getItem('ncluster.navCollapsed') === '1';
} catch (_) {}

const STATE_ORDER = { RUNNING: 0, COMPLETING: 1, PENDING: 2, FAILED: 3, CANCELLED: 4 };
const _expandedBackups = new Set();

function toggleBackups(parentJobId) {
  if (_expandedBackups.has(parentJobId)) {
    _expandedBackups.delete(parentJobId);
  } else {
    _expandedBackups.add(parentJobId);
  }
  const show = _expandedBackups.has(parentJobId);
  document.querySelectorAll(`tr[data-backup-parent="${parentJobId}"]`).forEach(r => {
    r.style.display = show ? '' : 'none';
  });
  const btn = document.querySelector(`[data-backups-toggle="${parentJobId}"]`);
  if (btn) btn.classList.toggle('expanded', show);
}

const _progressCache = (() => {
  try {
    return JSON.parse(sessionStorage.getItem('ncluster.progress') || '{}');
  } catch (_) { return {}; }
})();

function _saveProgressCache() {
  try { sessionStorage.setItem('ncluster.progress', JSON.stringify(_progressCache)); } catch (_) {}
}

function resolveProgress(cluster, jobid, apiProgress, state) {
  const key = `${cluster}:${jobid}`;
  const st = (state || '').toUpperCase();
  if (st !== 'RUNNING' && st !== 'COMPLETING') {
    delete _progressCache[key];
    return apiProgress;
  }
  if (apiProgress != null) {
    _progressCache[key] = apiProgress;
    return apiProgress;
  }
  return _progressCache[key] ?? null;
}

function stateClass(s) {
  s = (s || '').toUpperCase().split(' ')[0];
  if (s === 'RUNNING')    return 's-RUNNING';
  if (s === 'PENDING')    return 's-PENDING';
  if (s.includes('FAIL')) return 's-FAILED';
  if (s.includes('CANCEL')) return 's-CANCELLED';
  if (s === 'COMPLETED')  return 's-COMPLETED';
  if (s === 'COMPLETING') return 's-COMPLETING';
  return 's-OTHER';
}

function progressRing(pct) {
  const r = 5, c = 2 * Math.PI * r;
  const dash = (pct / 100) * c;
  return `<svg class="progress-ring" width="14" height="14" viewBox="0 0 14 14" role="img">
    <title>${pct}%</title>
    <circle class="ring-bg" cx="7" cy="7" r="${r}" fill="none" stroke-width="1.8"/>
    <circle class="ring-fg" cx="7" cy="7" r="${r}" fill="none" stroke-width="1.8"
      stroke-dasharray="${dash.toFixed(1)} ${c.toFixed(1)}" transform="rotate(-90 7 7)"/>
  </svg>`;
}

function stateChip(s, progress, reason, exitCode, crashDetected, estStart) {
  const cls = stateClass(s);
  const st = (s || '').toUpperCase();
  if (crashDetected && (st === 'RUNNING' || st === 'COMPLETING')) {
    const short = crashDetected.length > 40 ? crashDetected.slice(0, 38) + '…' : crashDetected;
    return `<span class="state-chip crash-warning" title="${crashDetected}">RUNNING</span><span class="crash-badge">crashed</span><span class="fail-reason">${short}</span>`;
  }
  if (progress != null && st === 'RUNNING') {
    return `<span class="state-chip ${cls}">${s}${progressRing(progress)}<span class="progress-pct">${progress}%</span></span>`;
  }
  const hasUtil = st === 'PENDING' && _clusterUtil;
  const utilCls = hasUtil ? ' has-util' : '';
  if (st === 'PENDING' && estStart) {
    const d = new Date(estStart.replace('T', ' '));
    if (!isNaN(d)) {
      const now = new Date();
      const diffH = Math.round((d - now) / 3600000);
      const when = diffH >= 24 ? `~${Math.round(diffH / 24)}d` : diffH > 0 ? `~${diffH}h` : 'soon';
      return `<span class="state-chip ${cls}${utilCls} pending-util-chip" title="Est. start: ${d.toLocaleString()}">PENDING <span class="est-inline">(${when})</span></span>`;
    }
  }
  if (st === 'PENDING') {
    const tip = reason && reason !== 'None' && reason !== 'Priority' ? ` title="${reason}"` : '';
    return `<span class="state-chip ${cls}${utilCls} pending-util-chip"${tip}>PENDING</span>`;
  }
  const tip = reason && reason !== 'None' && reason !== 'Priority' ? ` title="${reason}"` : '';
  let extra = '';
  if (exitCode && exitCode !== '0:0' && (st.includes('FAIL') || st.includes('CANCEL') || st.includes('TIMEOUT'))) {
    extra = `<span class="exit-code">exit ${exitCode}</span>`;
  }
  if (reason && reason !== 'None' && reason !== 'Priority' && !reason.includes('Dependency') && (st.includes('FAIL') || st.includes('CANCEL') || st.includes('TIMEOUT'))) {
    const short = reason.length > 30 ? reason.slice(0, 28) + '…' : reason;
    extra += `<span class="fail-reason">${short}</span>`;
  }
  return `<span class="state-chip ${cls}"${tip}>${s || '—'}</span>${extra}`;
}

function isFailedLikeState(s) {
  s = (s || '').toUpperCase();
  return s.includes('FAIL') || s.startsWith('CANCEL') || s.startsWith('TIMEOUT') || s.includes('OUT_OF_MEMORY') || s.includes('NODE_FAIL') || s.includes('BOOT_FAIL');
}

function isCompletedState(s) {
  return (s || '').toUpperCase().startsWith('COMPLETED');
}

const DEP_TYPE_DESC = {
  afterany:   'runs after parent finishes (any exit status)',
  afterok:    'runs only if parent succeeds (exit 0)',
  afternotok: 'runs only if parent fails (non-zero exit)',
  aftercorr:  'runs after corresponding parent task completes',
  after:      'runs after parent starts',
  afterburstbuffer: 'runs after parent burst buffer completes',
};

function classifyBackupJob(job, byId) {
  const st = (job.state || '').toUpperCase();
  if (st !== 'PENDING') return null;
  if (!byId) return null;

  const reason = (job.reason || '');
  const deps = job.dep_details || [];

  if (reason === 'DependencyNeverSatisfied') return 'dormant';

  // A backup/continuation job has afterany or afternotok dep on a
  // parent with the SAME NAME (checkpoint-restart chain pattern).
  const backupDeps = deps.filter(d => {
    if (d.type !== 'afterany' && d.type !== 'afternotok') return false;
    const parent = byId[d.job_id];
    return parent && parent.name === job.name;
  });

  if (backupDeps.length) {
    let anyRunning = false;
    let allDone = true;

    for (const d of backupDeps) {
      const pst = (byId[d.job_id].state || '').toUpperCase();
      if (pst === 'RUNNING' || pst === 'COMPLETING') {
        anyRunning = true;
        allDone = false;
      } else if (!pst.startsWith('COMPLETED') && !isFailedLikeState(pst)) {
        allDone = false;
      }
    }

    if (allDone && !anyRunning) return 'dormant';
    if (anyRunning) return 'standby';
    return 'standby';
  }

  // Heuristic fallback: no dep_details but same-name completed siblings.
  const siblings = Object.values(byId).filter(
    s => s.name === job.name && s.jobid !== job.jobid
  );
  if (!siblings.length) return null;

  const hasCompleted = siblings.some(
    s => (s.state || '').toUpperCase().startsWith('COMPLETED')
  );
  const hasRunning = siblings.some(s => {
    const ss = (s.state || '').toUpperCase();
    return ss === 'RUNNING' || ss === 'COMPLETING';
  });

  if (hasCompleted && !hasRunning) return 'dormant';
  if (hasRunning) return 'standby';
  return null;
}

function backupBadgeHtml(kind) {
  if (!kind) return '';
  if (kind === 'dormant') {
    return '<span class="backup-badge dormant" title="Backup job — parent completed successfully, this will not run">backup · won\'t run</span>';
  }
  return '<span class="backup-badge standby" title="Standby backup — will start if parent fails or hits time limit">standby</span>';
}

function buildBackupInfo(groupJobs, byId) {
  const backupMap = {};
  const parentOf = {};

  for (const j of groupJobs) {
    if ((j.state || '').toUpperCase() !== 'PENDING') continue;
    const deps = j.dep_details || [];
    for (const d of deps) {
      if (d.type !== 'afterany' && d.type !== 'afternotok') continue;
      const parent = byId[d.job_id];
      if (parent && parent.name === j.name) {
        if (!backupMap[d.job_id]) backupMap[d.job_id] = [];
        backupMap[d.job_id].push(j);
        parentOf[j.jobid] = d.job_id;
        break;
      }
    }
    if (parentOf[j.jobid]) continue;

    // Heuristic: same-name completed/running sibling with closest lower ID.
    if (classifyBackupJob(j, byId)) {
      const jid = parseInt(j.jobid, 10);
      let best = null, bestDist = Infinity;
      for (const s of groupJobs) {
        if (s.jobid === j.jobid || s.name !== j.name) continue;
        const sid = parseInt(s.jobid, 10);
        if (sid < jid && (jid - sid) < bestDist) {
          const ss = (s.state || '').toUpperCase();
          if (ss.startsWith('COMPLETED') || ss === 'RUNNING' || ss === 'COMPLETING') {
            best = s.jobid;
            bestDist = jid - sid;
          }
        }
      }
      if (best) {
        if (!backupMap[best]) backupMap[best] = [];
        backupMap[best].push(j);
        parentOf[j.jobid] = best;
      }
    }
  }

  return { backupMap, parentOf };
}

function depBadgeHtml(job, byId) {
  const deps = job.dep_details || [];
  if (!deps.length) return '';
  const parts = deps.map(d => {
    const parentName = byId && byId[d.job_id] ? byId[d.job_id].name : '';
    const shortType = d.type.replace('after', '');
    const typeDesc = DEP_TYPE_DESC[d.type] || d.type;
    const namePart = parentName ? ` (${parentName})` : '';
    const tip = `${d.type}:${d.job_id}${namePart} — ${typeDesc}`;
    return `<span class="dep-badge" title="${tip}">${shortType}:${d.job_id}</span>`;
  });
  return parts.join(' ');
}

function fmtTime(s) {
  if (!s || s === 'N/A' || s === 'Unknown') return '—';
  const d = new Date(s.replace('T', ' '));
  if (isNaN(d)) return s;
  const now = new Date();
  const sameDay = d.toDateString() === now.toDateString();
  if (sameDay) {
    return d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
  }
  return d.toLocaleDateString([], {month: 'short', day: 'numeric'})
       + ' ' + d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
}

function fmtStartCell(job) {
  const st = (job.state || '').toUpperCase();
  if (st === 'PENDING' && job.est_start) {
    return fmtTime(job.est_start);
  }
  return fmtTime(job.started_local || job.started || job.start);
}

function fmtElapsedCell(job) {
  const st = (job.state || '').toUpperCase();
  if (st === 'PENDING') {
    const tl = job.timelimit;
    if (tl && tl !== '—' && tl !== 'N/A') {
      return `<span class="dim" title="Time limit">${tl} limit</span>`;
    }
    return '—';
  }
  return job.elapsed || '—';
}

function parseGpus(nodes, gres) {
  if (!gres || gres === 'cpu' || gres === '(null)' || gres === 'N/A' || gres === 'local') return null;
  const m = gres.match(/gpu[^:]*:(?:[^:]+:)?(\d+)/);
  if (!m) return null;
  const perNode = parseInt(m[1], 10);
  if (perNode === 0) return null;
  const n = parseInt(nodes, 10) || 1;
  const total = perNode * n;
  return total === perNode ? `${total} GPU${total !== 1 ? 's' : ''}` : `${total} GPUs (${n}×${perNode})`;
}

function groupKeyForJob(name) {
  const n = (name || '').trim();
  if (!n) return 'misc';
  const evalMatch = n.match(/^(eval-[a-z0-9_]+)/i);
  if (evalMatch) return evalMatch[1].toLowerCase();
  return n
    .replace(/(?:-|_)(?:judge|summarize[-_]results?)(?:-rs\d+)?$/i, '')
    .replace(/(?:-|_)rs\d+$/i, '')
    .toLowerCase();
}

function groupJobsByDependency(jobs) {
  // Build dependency-aware groups: if A -> B -> C form a chain,
  // they belong in one group regardless of name prefix.
  const byId = {};
  for (const j of jobs) byId[j.jobid] = j;

  // Union-Find to merge dependency-connected jobs into one group.
  const parent = {};
  function find(x) {
    if (!(x in parent)) parent[x] = x;
    while (parent[x] !== x) { parent[x] = parent[parent[x]]; x = parent[x]; }
    return x;
  }
  function union(a, b) { parent[find(a)] = find(b); }

  for (const j of jobs) {
    for (const pid of (j.depends_on || [])) {
      if (byId[pid]) union(j.jobid, pid);
    }
  }

  // Also union by name-prefix for jobs without explicit deps.
  const nameGroups = {};
  for (const j of jobs) {
    const key = groupKeyForJob(j.name);
    if (!nameGroups[key]) nameGroups[key] = [];
    nameGroups[key].push(j.jobid);
  }
  for (const ids of Object.values(nameGroups)) {
    for (let i = 1; i < ids.length; i++) union(ids[0], ids[i]);
  }

  // Collect groups.
  const groups = {};
  for (const j of jobs) {
    const root = find(j.jobid);
    if (!groups[root]) groups[root] = [];
    groups[root].push(j);
  }

  // Topological sort within each group (parents before children).
  for (const [root, grp] of Object.entries(groups)) {
    groups[root] = topoSortJobs(grp);
  }

  // Derive a readable label for each group.
  const result = [];
  for (const grp of Object.values(groups)) {
    const rootJob = grp.find(j => !(j.depends_on || []).length) || grp[0];
    const label = groupKeyForJob(rootJob.name);
    result.push([label, grp]);
  }
  result.sort((a, b) => {
    // Newest group first (by earliest submitted timestamp in the group).
    const tsA = a[1].reduce((best, j) => {
      const t = j.submitted || j.started || '';
      return t > best ? t : best;
    }, '');
    const tsB = b[1].reduce((best, j) => {
      const t = j.submitted || j.started || '';
      return t > best ? t : best;
    }, '');
    if (tsA !== tsB) return tsA > tsB ? -1 : 1;
    return b[1].length - a[1].length;
  });
  return result;
}

function topoSortJobs(jobs) {
  if (jobs.length <= 1) return jobs;
  const idSet = new Set(jobs.map(j => j.jobid));
  const byId = {};
  for (const j of jobs) byId[j.jobid] = j;

  // In-degree within this group.
  const inDeg = {};
  for (const j of jobs) {
    inDeg[j.jobid] = 0;
  }
  for (const j of jobs) {
    for (const pid of (j.depends_on || [])) {
      if (idSet.has(pid)) inDeg[j.jobid]++;
    }
  }

  // Kahn's algorithm.
  const queue = jobs.filter(j => inDeg[j.jobid] === 0)
    .sort((a, b) => STATE_ORDER[a.state] - STATE_ORDER[b.state] || a.jobid.localeCompare(b.jobid));
  const sorted = [];
  while (queue.length) {
    const j = queue.shift();
    sorted.push(j);
    for (const cid of (j.dependents || [])) {
      if (!idSet.has(cid)) continue;
      inDeg[cid]--;
      if (inDeg[cid] === 0) queue.push(byId[cid]);
    }
  }
  // Append any remaining (cycle protection).
  for (const j of jobs) {
    if (!sorted.includes(j)) sorted.push(j);
  }
  return sorted;
}

function depthInGroup(job, byId, idSet, memo) {
  if (memo[job.jobid] !== undefined) return memo[job.jobid];
  let d = 0;
  for (const pid of (job.depends_on || [])) {
    if (idSet.has(pid) && byId[pid]) d = Math.max(d, depthInGroup(byId[pid], byId, idSet, memo) + 1);
  }
  memo[job.jobid] = d;
  return d;
}

function _isDarkTheme() {
  return document.documentElement.getAttribute('data-theme') === 'dark';
}

function lightenColor(hex, lightness) {
  if (!hex || !hex.startsWith('#')) return '';
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  if (_isDarkTheme()) {
    const mix = lightness || 0.15;
    const br = 0x1c, bg = 0x1c, bb = 0x28;
    const lr = Math.round(br + (r - br) * mix);
    const lg = Math.round(bg + (g - bg) * mix);
    const lb = Math.round(bb + (b - bb) * mix);
    return `rgb(${lr},${lg},${lb})`;
  }
  const t = lightness || 0.92;
  const lr = Math.round(r + (255 - r) * t);
  const lg = Math.round(g + (255 - g) * t);
  const lb = Math.round(b + (255 - b) * t);
  return `rgb(${lr},${lg},${lb})`;
}

function darkenColor(hex, factor) {
  if (!hex || !hex.startsWith('#')) return hex;
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  const f = factor || 0.45;
  const dr = Math.round(r * f);
  const dg = Math.round(g * f);
  const db = Math.round(b * f);
  return `#${dr.toString(16).padStart(2,'0')}${dg.toString(16).padStart(2,'0')}${db.toString(16).padStart(2,'0')}`;
}

function projectBadgeBg(hex) {
  if (!hex) return '';
  return _isDarkTheme() ? darkenColor(hex, 0.45) : hex;
}

function contrastTextColor(hex) {
  if (!hex || !hex.startsWith('#')) return '#000';
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  const lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
  return lum > 0.55 ? '#000' : '#fff';
}

function projectBadgeStyle(hex) {
  if (!hex) return '';
  const bg = projectBadgeBg(hex);
  const text = contrastTextColor(bg);
  return ` style="background:${bg};border-color:${bg};color:${text}"`;
}

/* ── Job name highlighting (dim common prefix/suffix, bold unique part) ── */

function computeNameHighlight(names) {
  const unique = [...new Set(names)];
  if (unique.length <= 1) return { prefix: '', suffix: '' };

  let prefix = unique[0];
  for (let i = 1; i < unique.length; i++) {
    while (prefix && !unique[i].startsWith(prefix)) {
      prefix = prefix.slice(0, -1);
    }
  }

  const rems = unique.map(n => n.slice(prefix.length));
  let suffix = rems[0];
  for (let i = 1; i < rems.length; i++) {
    while (suffix && !rems[i].endsWith(suffix)) {
      suffix = suffix.slice(1);
    }
  }

  for (const n of unique) {
    if (n.length - prefix.length - suffix.length <= 0) {
      return { prefix: '', suffix: '' };
    }
  }

  if (prefix.length + suffix.length < 3) return { prefix: '', suffix: '' };

  return { prefix, suffix };
}

function highlightJobName(name, prefix, suffix) {
  if (!prefix && !suffix) return name;
  const end = suffix ? name.length - suffix.length : name.length;
  const mid = name.slice(prefix.length, end);
  let html = '';
  if (prefix) html += '<span class="jn-dim">' + prefix + '</span>';
  html += '<span class="jn-hi">' + mid + '</span>';
  if (suffix) html += '<span class="jn-dim">' + suffix + '</span>';
  return html;
}

/* ── Cluster Utilization & Quota ────────────────────────── */

let _storageQuota = {};

async function fetchClusterUtilization() {
  if (_clusterUtilFetching) return;
  _clusterUtilFetching = true;
  try {
    const res = await fetch('/api/cluster_utilization');
    const data = await res.json();
    if (data.status === 'ok') {
      _clusterUtil = data;
    }
  } catch (_) {}
  _clusterUtilFetching = false;
}

async function fetchStorageQuotas() {
  const clusters = Object.keys(CLUSTERS).filter(c => c !== 'local');
  const promises = clusters.map(async c => {
    try {
      const res = await fetch(`/api/storage_quota/${c}`);
      const data = await res.json();
      if (data.status === 'ok') _storageQuota[c] = data;
    } catch (_) {}
  });
  await Promise.allSettled(promises);
}

function getClusterUtil(clusterName) {
  if (!_clusterUtil || !_clusterUtil.clusters) return null;
  return _clusterUtil.clusters[clusterName] || null;
}

function _teamStats(u) {
  const gpuPer = u.gpus_per_node || 8;
  const alloc = TEAM ? ((u.team_alloc_gpus || {})[TEAM] || 0) : 0;
  const members = TEAM ? u.users.filter(x => x.team === TEAM) : [];
  const runGpus = members.reduce((s, x) => s + x.running, 0) * gpuPer;
  const pendGpus = members.reduce((s, x) => s + x.pending, 0) * gpuPer;
  const pct = alloc > 0 ? Math.round(runGpus / alloc * 100) : 0;
  return { alloc, runGpus, pendGpus, pct, gpuPer };
}

function utilBarHtml(clusterName) {
  const u = getClusterUtil(clusterName);
  if (!u) return '';
  const t = _teamStats(u);
  if (!t.alloc) return '';
  const level = t.pct >= 90 ? 'high' : t.pct >= 60 ? 'medium' : 'low';
  return `<span class="util-bar-wrap" title="Science team: ${t.runGpus} / ${t.alloc} GPUs allocated">
    <span class="util-bar"><span class="util-bar-fill ${level}" style="width:${Math.min(t.pct, 100)}%"></span></span>
    <span>${t.pct}%</span>
  </span>`;
}

function quotaBadgesHtml(clusterName) {
  const q = _storageQuota[clusterName];
  if (!q || !q.project_quotas) return '';
  const badges = [];
  for (const [name, pq] of Object.entries(q.project_quotas)) {
    const short = name.replace('llmservice_nemo_', '');
    const spacePct = pq.space_used_pct || 0;
    const inodePct = pq.files_used_pct || 0;
    const worst = Math.max(spacePct, inodePct);
    const level = worst >= 95 ? 'crit' : worst >= 85 ? 'warn' : 'ok';
    const detail = `${short}: ${pq.space_used_human} / ${pq.space_quota_human} (${spacePct}% space, ${inodePct}% inodes)`;
    badges.push(`<span class="quota-pill ${level}" title="${detail}">${short} ${Math.round(worst)}%</span>`);
  }
  return badges.join('');
}

/* ── Tooltip for pending jobs ── */
const _tooltip = (() => {
  const el = document.createElement('div');
  el.className = 'cluster-tooltip';
  document.body.appendChild(el);

  let _hideTimer = null;

  function _pctColor(pct) { return pct >= 90 ? 'red' : pct >= 60 ? 'amber' : 'green'; }
  function _barLevel(pct) { return pct >= 90 ? 'high' : pct >= 60 ? 'medium' : 'low'; }
  function _statusLabel(pct) {
    if (pct >= 90) return '<span class="tt-head-status busy">saturated</span>';
    if (pct >= 60) return '<span class="tt-head-status loaded">busy</span>';
    return '<span class="tt-head-status normal">available</span>';
  }

  function show(anchorEl, clusterName) {
    const u = getClusterUtil(clusterName);
    if (!u) return;
    clearTimeout(_hideTimer);

    const t = _teamStats(u);
    const gpuPer = t.gpuPer;

    // Cluster fullness — total_nodes = nodes currently occupied cluster-wide
    const allBusy = u.total_nodes > 0 && u.pending_nodes > 0;
    const clusterFull = u.total_nodes > 0 && (u.pending_nodes >= u.total_nodes * 0.3);

    // Header status: combine team quota position + cluster pressure
    let headerStatus = '';
    if (t.alloc > 0) {
      if (clusterFull && t.pct < 100) {
        headerStatus = '<span class="tt-head-status busy">cluster full</span>';
      } else if (t.pct >= 100) {
        headerStatus = '<span class="tt-head-status busy">over quota</span>';
      } else if (t.pct >= 70) {
        headerStatus = '<span class="tt-head-status loaded">near quota</span>';
      } else {
        headerStatus = '<span class="tt-head-status normal">has priority</span>';
      }
    }

    let html = `<div class="tt-head"><span class="tt-head-name">${clusterName}</span>${headerStatus}</div>`;

    // Team allocation — primary info
    if (t.alloc > 0) {
      html += `<div class="tt-util-bar"><div class="tt-util-bar-fill ${_barLevel(t.pct)}" style="width:${Math.min(t.pct, 100)}%"></div></div>`;
      html += `<div class="tt-row"><span class="tt-label">Team GPUs</span><span class="tt-val ${_pctColor(t.pct)}">${t.runGpus} / ${t.alloc} allocated</span></div>`;
      if (t.pendGpus > 0) {
        html += `<div class="tt-row"><span class="tt-label">Team pending</span><span class="tt-val amber">${t.pendGpus} GPUs</span></div>`;
      }
    }

    // Cluster-wide context: show nodes occupied + queue depth
    html += `<div class="tt-sep"></div>`;
    html += `<div class="tt-row"><span class="tt-label">Nodes occupied</span><span class="tt-val">${u.total_nodes}</span></div>`;
    if (u.pending_nodes > 0) {
      html += `<div class="tt-row"><span class="tt-label">Nodes queued</span><span class="tt-val amber">${u.pending_nodes}</span></div>`;
    }

    // Storage quota health
    const sq = _storageQuota[clusterName];
    if (sq && sq.project_quotas && Object.keys(sq.project_quotas).length) {
      html += `<div class="tt-sep"></div>`;
      for (const [pname, pq] of Object.entries(sq.project_quotas)) {
        const short = pname.replace('llmservice_nemo_', '');
        const sp = pq.space_used_pct || 0;
        const ip = pq.files_used_pct || 0;
        const worst = Math.max(sp, ip);
        const cls = _pctColor(worst);
        const barCls = _barLevel(worst);
        html += `<div class="tt-quota-row">`;
        html += `<span class="tt-quota-name">${short}</span>`;
        html += `<span class="tt-quota-bar"><span class="tt-util-bar-fill ${barCls}" style="width:${Math.min(worst, 100)}%"></span></span>`;
        html += `<span class="tt-quota-pct ${cls}">${Math.round(worst)}%</span>`;
        html += `</div>`;
      }
    }

    el.innerHTML = html;

    const rect = anchorEl.getBoundingClientRect();
    const ttW = el.offsetWidth || 260;
    let left = rect.left + rect.width / 2 - ttW / 2;
    let top = rect.bottom + 6;
    if (left < 8) left = 8;
    if (left + ttW > window.innerWidth - 8) left = window.innerWidth - ttW - 8;
    if (top + el.offsetHeight > window.innerHeight - 8) {
      top = rect.top - 6;
      el.style.transform = 'translateY(-100%)';
    } else {
      el.style.transform = '';
    }
    el.style.left = left + 'px';
    el.style.top = top + 'px';
    el.classList.add('visible');
  }

  function hide() {
    _hideTimer = setTimeout(() => { el.classList.remove('visible'); }, 100);
  }

  return { show, hide, el };
})();

function attachPendingTooltip(chipEl, clusterName) {
  if (!chipEl) return;
  chipEl.addEventListener('mouseenter', () => _tooltip.show(chipEl, clusterName));
  chipEl.addEventListener('mouseleave', () => _tooltip.hide());
}

/* ── Tab Visibility Guard ───────────────────────────────── */
document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    if (typeof stopCountdown === 'function') stopCountdown();
    if (typeof _projRefreshTimer !== 'undefined' && _projRefreshTimer) {
      clearInterval(_projRefreshTimer);
    }
  } else {
    if (typeof fetchAll === 'function') fetchAll();
    if (typeof startCountdown === 'function' && typeof refreshIntervalSec !== 'undefined' && refreshIntervalSec > 0) {
      startCountdown();
    }
    if (typeof currentTab !== 'undefined' && currentTab === 'project' && typeof _fetchProjectData === 'function') {
      _fetchProjectData();
    }
  }
});

