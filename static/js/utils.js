const CLUSTERS = JSON.parse(document.getElementById('cluster-data').textContent || '{}');
let allData = {};
let historyData = [];
let countdown = 20;
let refreshIntervalSec = 20;
let cdTimer;
let currentTab = 'live';
let _isResizingTree = false;
let _isResizingNav = false;
let navCollapsed = false;
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

function stateChip(s, progress, reason, exitCode, crashDetected) {
  const cls = stateClass(s);
  const st = (s || '').toUpperCase();
  if (crashDetected && (st === 'RUNNING' || st === 'COMPLETING')) {
    const short = crashDetected.length > 40 ? crashDetected.slice(0, 38) + '…' : crashDetected;
    return `<span class="state-chip crash-warning" title="${crashDetected}">RUNNING</span><span class="crash-badge">crashed</span><span class="fail-reason">${short}</span>`;
  }
  if (progress != null && st === 'RUNNING') {
    return `<span class="state-chip ${cls}">${s}${progressRing(progress)}<span class="progress-pct">${progress}%</span></span>`;
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
  // Slurm gives "2026-03-09T13:22:55" or "2026-03-09 13:22:55"
  const d = new Date(s.replace('T', ' '));
  if (isNaN(d)) return s;
  const now = new Date();
  const diffMs = now - d;
  // If today, just show time; otherwise show date+time
  const sameDay = d.toDateString() === now.toDateString();
  if (sameDay) {
    return d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
  }
  return d.toLocaleDateString([], {month: 'short', day: 'numeric'})
       + ' ' + d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
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

function lightenColor(hex, lightness) {
  if (!hex || !hex.startsWith('#')) return '';
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  const t = lightness || 0.92;
  const lr = Math.round(r + (255 - r) * t);
  const lg = Math.round(g + (255 - g) * t);
  const lb = Math.round(b + (255 - b) * t);
  return `rgb(${lr},${lg},${lb})`;
}

function contrastTextColor(hex) {
  if (!hex || !hex.startsWith('#')) return '#000';
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  const lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
  return lum > 0.55 ? '#000' : '#fff';
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

