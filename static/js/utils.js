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
  const saved = JSON.parse(localStorage.getItem('clausius.sectionCollapsed') || '{}');
  Object.assign(sectionCollapsed, saved);
} catch (_) {}
try {
  navCollapsed = localStorage.getItem('clausius.navCollapsed') === '1';
} catch (_) {}

const STATE_ORDER = { RUNNING: 0, COMPLETING: 1, PENDING: 2, FAILED: 3, CANCELLED: 4 };
const _expandedBackups = new Set();
const _expandedGroups = new Set();

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

function toggleRunGroup(groupId) {
  const wasExpanded = _expandedGroups.has(groupId);
  if (wasExpanded) _expandedGroups.delete(groupId);
  else _expandedGroups.add(groupId);
  const show = !wasExpanded;
  document.querySelectorAll(`tr[data-run-group="${groupId}"]`).forEach(r => {
    if (!show) { r.style.display = 'none'; return; }
    const bp = r.dataset.backupParent;
    r.style.display = (bp && !_expandedBackups.has(bp)) ? 'none' : '';
  });
  const chev = document.querySelector(`[data-group-chevron="${groupId}"]`);
  if (chev) chev.classList.toggle('expanded', show);
}

const _progressCache = (() => {
  try {
    return JSON.parse(sessionStorage.getItem('clausius.progress') || '{}');
  } catch (_) { return {}; }
})();
const _progressSourceCache = (() => {
  try {
    return JSON.parse(sessionStorage.getItem('clausius.progressSrc') || '{}');
  } catch (_) { return {}; }
})();

function _saveProgressCache() {
  try { sessionStorage.setItem('clausius.progress', JSON.stringify(_progressCache)); } catch (_) {}
  try { sessionStorage.setItem('clausius.progressSrc', JSON.stringify(_progressSourceCache)); } catch (_) {}
}

function resolveProgress(cluster, jobid, apiProgress, state, apiSource) {
  const key = `${cluster}:${jobid}`;
  const st = (state || '').toUpperCase();
  if (st !== 'RUNNING' && st !== 'COMPLETING') {
    delete _progressCache[key];
    delete _progressSourceCache[key];
    return { pct: apiProgress, source: apiSource || '' };
  }
  if (apiProgress != null) {
    _progressCache[key] = apiProgress;
    if (apiSource) _progressSourceCache[key] = apiSource;
    return { pct: apiProgress, source: apiSource || _progressSourceCache[key] || '' };
  }
  return { pct: _progressCache[key] ?? null, source: _progressSourceCache[key] || '' };
}

function isSoftFail(state, reason) {
  return (state || '').toUpperCase() === 'COMPLETED' && (reason || '').startsWith('soft-fail:');
}

function isUnneededBackup(job, groupJobs) {
  const st = (job.state || '').toUpperCase();
  if (!st.includes('FAIL')) return false;
  const deps = job.dep_details || [];
  const hasBackupDep = deps.some(d => d.type === 'afterany' || d.type === 'afternotok');
  if (!hasBackupDep) {
    const siblings = groupJobs.filter(s => s.name === job.name && s.jobid !== job.jobid);
    if (!siblings.length) return false;
    const hasCompleted = siblings.some(s => (s.state || '').toUpperCase().startsWith('COMPLETED'));
    const jid = parseInt(job.jobid, 10);
    const hasLowerCompleted = siblings.some(s =>
      (s.state || '').toUpperCase().startsWith('COMPLETED') && parseInt(s.jobid, 10) < jid
    );
    return hasLowerCompleted;
  }
  const byId = {};
  for (const j of groupJobs) byId[j.jobid] = j;
  for (const d of deps) {
    if (d.type !== 'afterany' && d.type !== 'afternotok') continue;
    const parent = byId[d.job_id];
    if (parent && (parent.state || '').toUpperCase().startsWith('COMPLETED')) return true;
  }
  return false;
}

function stateClass(s, reason) {
  if (isSoftFail(s, reason)) return 's-SOFT_FAIL';
  s = (s || '').toUpperCase().split(' ')[0];
  if (s === 'RUNNING')    return 's-RUNNING';
  if (s === 'PENDING')    return 's-PENDING';
  if (s.includes('FAIL')) return 's-FAILED';
  if (s.includes('CANCEL')) return 's-CANCELLED';
  if (s === 'COMPLETED')  return 's-COMPLETED';
  if (s === 'COMPLETING') return 's-COMPLETING';
  return 's-OTHER';
}

function _isServerSource(source) {
  return (source || '').toLowerCase().includes('server');
}

function progressRing(pct, isServer) {
  const r = 5, c = 2 * Math.PI * r;
  const dash = (pct / 100) * c;
  const svgCls = isServer ? 'progress-ring ring-server' : 'progress-ring';
  return `<svg class="${svgCls}" width="14" height="14" viewBox="0 0 14 14" role="img">
    <title>${pct}%${isServer ? ' (loading)' : ''}</title>
    <circle class="ring-bg" cx="7" cy="7" r="${r}" fill="none" stroke-width="1.8"/>
    <circle class="ring-fg" cx="7" cy="7" r="${r}" fill="none" stroke-width="1.8"
      stroke-dasharray="${dash.toFixed(1)} ${c.toFixed(1)}" transform="rotate(-90 7 7)"/>
  </svg>`;
}

function stateChip(s, progress, reason, exitCode, crashDetected, estStart, jobMeta, progressSource) {
  const cls = stateClass(s, reason);
  const st = (s || '').toUpperCase();
  if (crashDetected && (st === 'RUNNING' || st === 'COMPLETING')) {
    const short = crashDetected.length > 40 ? crashDetected.slice(0, 38) + '…' : crashDetected;
    return `<span class="state-chip crash-warning" title="${crashDetected}">RUNNING</span><span class="crash-badge">crashed</span><span class="fail-reason">${short}</span>`;
  }
  if (progress != null && st === 'RUNNING') {
    const isSrv = _isServerSource(progressSource);
    if (isSrv && progress >= 100) {
      return `<span class="state-chip ${cls}">${s}</span>`;
    }
    const label = isSrv ? 'loading' : `${progress}%`;
    const pctCls = isSrv ? 'progress-pct progress-pct-server' : 'progress-pct';
    return `<span class="state-chip ${cls}">${s}${progressRing(progress, isSrv)}<span class="${pctCls}">${label}</span></span>`;
  }
  const hasUtil = st === 'PENDING' && (_clusterUtil || _partitionData);
  const utilCls = hasUtil ? ' has-util' : '';
  if (st === 'PENDING') {
    const m = jobMeta || {};
    const esc = v => (v || '').replace(/"/g, '&quot;');
    const dataAttrs = ` data-reason="${esc(reason)}" data-nodes="${esc(m.nodes)}" data-gres="${esc(m.gres)}" data-partition="${esc(m.partition)}" data-timelimit="${esc(m.timelimit)}" data-est-start="${esc(estStart)}"`;
    if (estStart) {
      const d = new Date(estStart.replace('T', ' '));
      if (!isNaN(d)) {
        const now = new Date();
        const diffH = Math.round((d - now) / 3600000);
        const when = diffH >= 24 ? `~${Math.round(diffH / 24)}d` : diffH > 0 ? `~${diffH}h` : 'soon';
        return `<span class="state-chip ${cls}${utilCls} pending-util-chip"${dataAttrs} title="Est. start: ${d.toLocaleString()}">PENDING <span class="est-inline">(${when})</span></span>`;
      }
    }
    const tip = reason && reason !== 'None' && reason !== 'Priority' ? ` title="${esc(reason)}"` : '';
    return `<span class="state-chip ${cls}${utilCls} pending-util-chip"${dataAttrs}${tip}>PENDING</span>`;
  }
  if (isSoftFail(s, reason)) {
    const detail = reason.replace(/^soft-fail:\s*/i, '');
    const short = detail.length > 30 ? detail.slice(0, 28) + '…' : detail;
    return `<span class="state-chip ${cls}" title="${reason}">SOFT FAIL</span><span class="softfail-reason">${short}</span>`;
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

function _isFailedNotCancelled(s) {
  s = (s || '').toUpperCase();
  return (s.includes('FAIL') || s.startsWith('TIMEOUT') || s.includes('OUT_OF_MEMORY') || s.includes('NODE_FAIL') || s.includes('BOOT_FAIL')) && !s.startsWith('CANCEL');
}

function _isCancelledState(s) {
  return (s || '').toUpperCase().startsWith('CANCEL');
}

function isCompletedState(s) {
  return (s || '').toUpperCase().startsWith('COMPLETED');
}

function _countJobStates(jobs) {
  const cnt = { run: 0, comp: 0, pend: 0, fail: 0, canc: 0, done: 0 };
  for (const j of jobs) {
    const st = (j.state || '').toUpperCase();
    if (st === 'RUNNING') cnt.run++;
    else if (st === 'COMPLETING') cnt.comp++;
    else if (st === 'PENDING') cnt.pend++;
    else if (st.startsWith('COMPLETED')) cnt.done++;
    else if (st.startsWith('CANCEL')) cnt.canc++;
    else if (isUnneededBackup(j, jobs)) cnt.done++;
    else cnt.fail++;
  }
  return cnt;
}

function statusDonut(jobs) {
  const cnt = _countJobStates(jobs);
  const total = jobs.length;
  if (!total) return '';
  const sz = 18, r = 6, sw = 3.5;
  const C = 2 * Math.PI * r;
  const cx = sz / 2, cy = sz / 2;
  const segs = [
    [cnt.run,  'var(--green)'],
    [cnt.comp, 'var(--cyan)'],
    [cnt.pend, 'var(--yellow)'],
    [cnt.fail, 'var(--red)'],
    [cnt.canc, 'var(--muted)'],
    [cnt.done, 'var(--done)'],
  ];
  let off = 0;
  const arcs = segs.filter(([n]) => n > 0).map(([n, color]) => {
    const d = (n / total) * C;
    const el = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${color}" stroke-width="${sw}" stroke-dasharray="${d.toFixed(1)} ${(C - d).toFixed(1)}" stroke-dashoffset="${(-off).toFixed(1)}" transform="rotate(-90 ${cx} ${cy})"/>`;
    off += d;
    return el;
  }).join('');
  const tip = [];
  if (cnt.run)  tip.push(`${cnt.run} running`);
  if (cnt.comp) tip.push(`${cnt.comp} completing`);
  if (cnt.pend) tip.push(`${cnt.pend} pending`);
  if (cnt.fail) tip.push(`${cnt.fail} failed`);
  if (cnt.canc) tip.push(`${cnt.canc} cancelled`);
  if (cnt.done) tip.push(`${cnt.done} completed`);
  return `<svg class="status-donut" width="${sz}" height="${sz}" viewBox="0 0 ${sz} ${sz}"><title>${tip.join(', ')}</title><circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="var(--border)" stroke-width="${sw}"/>${arcs}</svg>`;
}

function statusSummaryHtml(jobs) {
  const cnt = _countJobStates(jobs);
  const parts = [];
  if (cnt.run)  parts.push(`<span class="ss-run">${cnt.run} running</span>`);
  if (cnt.comp) parts.push(`<span class="ss-comp">${cnt.comp} completing</span>`);
  if (cnt.pend) parts.push(`<span class="ss-pend">${cnt.pend} pending</span>`);
  if (cnt.fail) parts.push(`<span class="ss-fail">${cnt.fail} failed</span>`);
  if (cnt.canc) parts.push(`<span class="ss-canc">${cnt.canc} cancelled</span>`);
  if (cnt.done) parts.push(`<span class="ss-done">${cnt.done} done</span>`);
  return `<span class="status-summary">${parts.join('<span class="ss-sep">\u00b7</span>')}</span>`;
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
  if (kind === 'unneeded') {
    return '<span class="backup-badge dormant" title="Backup job — parent completed successfully, backup was not needed">backup · not needed</span>';
  }
  return '<span class="backup-badge standby" title="Standby backup — will start if parent fails or hits time limit">standby</span>';
}

function buildBackupInfo(groupJobs, byId) {
  const backupMap = {};
  const parentOf = {};

  for (const j of groupJobs) {
    const jst = (j.state || '').toUpperCase();
    if (jst !== 'PENDING' && !isUnneededBackup(j, groupJobs)) continue;
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

  // Union by run_id so all jobs from the same run stay grouped.
  const runGroups = {};
  for (const j of jobs) {
    if (j.run_id) {
      if (!runGroups[j.run_id]) runGroups[j.run_id] = [];
      runGroups[j.run_id].push(j.jobid);
    }
  }
  for (const ids of Object.values(runGroups)) {
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
let _partitionData = null;
let _teamUsageData = {};
let _teamGpuAlloc = {};

function _clusterStatsFromPartitions(clusterName) {
  const ps = _partitionData && _partitionData[clusterName];
  if (!ps) return null;
  const parts = Array.isArray(ps) ? ps : (Array.isArray(ps.partitions) ? ps.partitions : []);
  if (!parts.length) return null;
  const gpu = parts.filter(p => (p.gpus_per_node || 0) > 0);
  if (!gpu.length) return null;
  const biggest = gpu.slice().sort((a, b) => (b.total_nodes || 0) - (a.total_nodes || 0))[0];
  return {
    total_nodes: biggest.total_nodes || 0,
    alloc_nodes: biggest.alloc_nodes || 0,
    idle_nodes: biggest.idle_nodes || 0,
    other_nodes: biggest.other_nodes || 0,
    pending_jobs: gpu.reduce((s, p) => s + (p.pending_jobs || 0), 0),
    gpus_per_node: biggest.gpus_per_node || 0,
  };
}

function _resolveGpuAlloc(clusterName) {
  const raw = _teamGpuAlloc[clusterName];
  if (raw === 'any' || raw === -1) {
    const ps = _partitionData && _partitionData[clusterName];
    if (ps && ps.partitions) {
      let total = 0;
      for (const p of ps.partitions) {
        const gpus = (p.total_nodes || 0) * (p.gpus_per_node || 0);
        if (gpus > total) total = gpus;
      }
      return { gpus: total || 0, isAny: true };
    }
    return { gpus: 0, isAny: true };
  }
  return { gpus: raw || 0, isAny: false };
}
let _partitionFetching = false;

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

async function fetchPartitions() {
  if (_partitionFetching) return;
  _partitionFetching = true;
  try {
    const res = await fetch('/api/partition_summary');
    const data = await res.json();
    if (data.status === 'ok') {
      _partitionData = data.clusters;
    }
  } catch (_) {}
  _partitionFetching = false;
}

function _fmtTime(timeStr) {
  if (!timeStr || timeStr === 'UNLIMITED') return '∞';
  const s = timeStr.trim();
  if (s.includes('-')) {
    const d = parseInt(s.split('-')[0], 10);
    return d + 'd';
  }
  const parts = s.split(':');
  if (parts.length >= 2) {
    const h = parseInt(parts[0], 10);
    const m = parseInt(parts[1], 10);
    if (h > 0) return m > 0 ? h + 'h' + m + 'm' : h + 'h';
    return m + 'm';
  }
  return s;
}

function _bestWaitCls(partitions) {
  const order = { fast: 0, moderate: 1, slow: 2, long: 3 };
  let best = 'long';
  for (const p of partitions) {
    const cls = p.est_wait_cls || 'long';
    if ((order[cls] ?? 3) < (order[best] ?? 3)) best = cls;
  }
  return best;
}

async function openClusterAvail() {
  showTab('clusters');
}

function closeClusterAvail() {}

const _TEAM_CACHE_KEY = 'clausius.teamUsageCache';

function _saveTeamCache() {
  try {
    localStorage.setItem(_TEAM_CACHE_KEY, JSON.stringify({
      ts: Date.now(),
      usage: _teamUsageData,
      alloc: _teamGpuAlloc,
    }));
  } catch (_) {}
}

function _loadTeamCache() {
  try {
    const raw = localStorage.getItem(_TEAM_CACHE_KEY);
    if (!raw) return false;
    const c = JSON.parse(raw);
    if (c.usage && Object.keys(c.usage).length) {
      Object.assign(_teamUsageData, c.usage);
      if (c.alloc && Object.keys(c.alloc).length) _teamGpuAlloc = c.alloc;
      return true;
    }
  } catch (_) {}
  return false;
}

function showTeamGpuCached() {
  _loadTeamCache();
}

async function refreshTeamGpuStatus(silent) {}

async function refreshClusterAvailability() {
  const t = toastLoading('Fetching partition data…');
  try {
    await fetchPartitions();
    _renderAvailTable();
    t.done('Partition data refreshed');
  } catch (e) {
    t.done('Failed to fetch partition data', 'error');
  }
}

/* ── PPP Allocation Dashboard ── */

let _pppAllocData = null;

function _shortAcct(acct) {
  const parts = acct.split('_');
  return parts.length >= 3 ? parts.slice(2).join('-') : acct;
}

const _PPP_COLORS = ['ppp-c0', 'ppp-c1', 'ppp-c2'];

const _COMPUTE_CACHE_KEY = 'clausius.computeCache';

function _saveComputeCache() {
  try {
    localStorage.setItem(_COMPUTE_CACHE_KEY, JSON.stringify({
      ts: Date.now(),
      alloc: _pppAllocData,
      overlay: _pppOverlayData,
      myFs: _myFairshareData,
      teamJobs: _teamJobsData,
      partitions: _partitionData,
    }));
  } catch (_) {}
}

function _loadComputeCache() {
  try {
    const raw = localStorage.getItem(_COMPUTE_CACHE_KEY);
    if (!raw) return false;
    const c = JSON.parse(raw);
    if (!c.alloc || Date.now() - c.ts > 3600000) return false;
    _pppAllocData = c.alloc;
    if (c.overlay) _pppOverlayData = c.overlay;
    if (c.myFs) _myFairshareData = c.myFs;
    if (c.teamJobs) _teamJobsData = c.teamJobs;
    if (c.partitions) _partitionData = c.partitions;
    return true;
  } catch (_) {}
  return false;
}

function _showComputeLoadBar(show) {
  let bar = document.getElementById('compute-load-bar');
  if (!bar) {
    bar = document.createElement('div');
    bar.id = 'compute-load-bar';
    bar.className = 'compute-load-bar';
    const body = document.getElementById('ppp-alloc-body');
    if (body) body.parentElement.insertBefore(bar, body);
  }
  bar.classList.toggle('active', show);
}

async function refreshPppAllocations() {
  const el = document.getElementById('ppp-alloc-body');
  if (!el) return;

  if (_loadComputeCache() && _pppAllocData) {
    _renderPppAllocations(_pppAllocData);
  }

  _showComputeLoadBar(true);
  try {
    const [allocRes] = await Promise.all([
      fetch('/api/aihub/allocations'),
      _ensureOverlayData(),
      fetchPartitions(),
      _fetchMyFairshare(),
      _fetchTeamJobs(),
      _fetchProjectColors(),
    ]);
    const data = await allocRes.json();
    if (data.status === 'ok') {
      _pppAllocData = data;
      _renderPppAllocations(data);
      _saveComputeCache();
    } else if (!_pppAllocData) {
      el.innerHTML = `<div class="no-jobs" style="color:var(--red)">${data.error || 'Failed to load'}</div>`;
    }
  } catch (e) {
    if (!_pppAllocData) {
      el.innerHTML = '<div class="no-jobs" style="color:var(--red)">Failed to connect to AI Hub</div>';
    }
  }
  _showComputeLoadBar(false);
}

let _teamJobsData = null;

function _getTeamRunningOnCluster(cn) {
  if (!_pppOverlayData?.clusters?.[cn]) return 0;
  const teamMembers = _pppOverlayData.team_members || [];
  let total = 0;
  for (const acctUsers of Object.values(_pppOverlayData.clusters[cn])) {
    for (const m of teamMembers) total += (acctUsers[m] || 0);
  }
  return total;
}

function _getJobStats(cn) {
  const cd = _teamJobsData?.clusters?.[cn];
  if (!cd) return { myRunning: 0, myPending: 0, teamRunning: 0, teamPending: 0 };
  const s = cd.summary || {};
  const byUser = s.by_user || {};
  const me = byUser[USERNAME] || {};
  return {
    myRunning: me.running || 0,
    myPending: (me.pending || 0) + (me.dependent || 0),
    teamRunning: s.total_running || 0,
    teamPending: (s.total_pending || 0) + (s.total_dependent || 0),
  };
}

function _getWdsInputs() {
  const nodes = parseInt(document.getElementById('wds-nodes')?.value || '1') || 1;
  const gpn = parseInt(document.getElementById('wds-gpn')?.value || '8') || 8;
  return { reqNodes: nodes, reqGpn: gpn, reqGpus: nodes * gpn };
}

function computeWds(cn, acct, ad, curGpuType) {
  const { reqNodes, reqGpus } = _getWdsInputs();
  const ps = _partitionData?.[cn];
  const idleNodes = ps?.idle_nodes || 0;
  const pendingQueue = ps?.pending_jobs || 0;
  const pppHeadroom = ad?.headroom || 0;
  const s = _clusterSubmitScore(_pppAllocData?.clusters?.[cn] || {}, cn);
  const freeForTeam = s.freeForTeam || 0;
  const teamNum = s.teamNum;

  const myFs = _myFairshareData?.clusters?.[cn]?.[acct];
  const myLevelFs = myFs?.level_fs || 0;
  const pppLevelFs = ad?.level_fs || 0;

  const clusterGpu = (CLUSTERS[cn]?.gpu_type || '').toLowerCase();
  const prefGpu = (curGpuType || '').toLowerCase();
  const machineScore = (!prefGpu || clusterGpu === prefGpu) ? 1.0 : 0.85;

  const cd = _pppAllocData?.clusters?.[cn] || {};
  const clOcc = cd.cluster_occupied_gpus || 0;
  const clTot = cd.cluster_total_gpus || 0;
  const occPct = clTot > 0 ? Math.round(clOcc / clTot * 100) : 100;

  const hardCapacity = Math.max(pppHeadroom, freeForTeam);
  const resourceGate = Math.min(
    1,
    hardCapacity / Math.max(reqGpus, 1),
    idleNodes / Math.max(reqNodes, 1)
  );

  const teamPenalty = (teamNum && teamNum > 0 && freeForTeam <= 0) ? 0.7 : 1.0;

  const effectiveMyFs = myLevelFs > 0 ? myLevelFs : pppLevelFs;
  const myFsScore = Math.min(effectiveMyFs / 1.5, 1);
  const pppFsScore = Math.min(pppLevelFs / 1.5, 1);
  const queueScore = 1 - Math.min(Math.log1p(pendingQueue / Math.max(idleNodes, 1)) / Math.log1p(50), 1);
  const occupancyFactor = 1.15 - 0.30 * Math.min(occPct / 100, 1);

  const priorityBlend = 0.55 * myFsScore + 0.20 * pppFsScore + 0.25 * queueScore;
  const wds = Math.round(100 * resourceGate * priorityBlend * machineScore * teamPenalty * occupancyFactor);

  return {
    wds: Math.max(0, Math.min(100, wds)),
    resourceGate: Math.round(resourceGate * 100) / 100,
    myLevelFs, pppLevelFs,
    queueScore: Math.round(queueScore * 100) / 100,
    machineScore, occupancyFactor: Math.round(occupancyFactor * 1000) / 1000,
    idleNodes, pendingQueue, freeForTeam, pppHeadroom,
  };
}

function onWdsSizeChange() {
  if (_pppAllocData) _renderPppAllocations(_pppAllocData);
}

function _clusterSubmitScore(cd, cn) {
  const bc = cd.best_capacity || {};
  const bp = cd.best_priority || {};
  const pppHeadroom = bc.headroom || 0;
  const levelFs = bp.level_fs || 0;

  const teamAlloc = cd.team_gpu_alloc;
  const teamNum = teamAlloc === 'any' ? null : (typeof teamAlloc === 'number' ? teamAlloc : null);
  const teamRunning = _getTeamRunningOnCluster(cn);

  let freeForTeam;
  if (teamNum && teamNum > 0) {
    freeForTeam = Math.min(pppHeadroom, Math.max(0, teamNum - teamRunning));
  } else {
    freeForTeam = pppHeadroom;
  }

  const jobs = _getJobStats(cn);

  return { freeForTeam, pppHeadroom, teamRunning, teamNum, levelFs,
           myRunning: jobs.myRunning, myPending: jobs.myPending,
           teamPending: jobs.teamPending,
           score: freeForTeam * Math.min(levelFs, 3) };
}

function _renderSubmitSummary(clusters) {
  const curGpuType = '';
  const ranked = Object.entries(clusters)
    .map(([cn, cd]) => {
      const bc = cd.best_capacity || {};
      const bp = cd.best_priority || {};
      const bestAcct = (bc.headroom || 0) > 50 ? (bc.account || '') : (bp.account || '');
      const gpuType = cd.gpu_type || '';
      const s = _clusterSubmitScore(cd, cn);
      const bestAd = cd.accounts?.[bestAcct] || {};
      const wdsResult = computeWds(cn, bestAcct, bestAd, curGpuType);
      return { cn, gpuType, bestAcct, wds: wdsResult.wds, ...s };
    })
    .filter(c => c.freeForTeam > 0 || c.levelFs > 0.5)
    .sort((a, b) => b.wds - a.wds);

  if (!ranked.length) return '<div class="ws-strip"><span class="ws-none">No clusters with available headroom</span></div>';

  return '<div class="ws-strip">' + ranked.map((c, i) => {
    const cls = i === 0 ? 'ws-best' : c.wds >= 50 ? 'ws-good' : 'ws-ok';
    const acctShort = c.bestAcct ? _shortAcct(c.bestAcct) : '';
    const teamLabel = c.teamNum ? ` / ${c.teamNum}` : '';
    const tooltip = `Team free: ${c.freeForTeam} GPUs (using ${c.teamRunning}${teamLabel}), PPP headroom: ${c.pppHeadroom}, FS: ${c.levelFs.toFixed(1)}`;

    const myR = c.myRunning || 0;
    const myP = c.myPending || 0;
    const teamR = c.teamRunning || 0;
    const teamP = c.teamPending || 0;

    const wdsCls = c.wds >= 75 ? 'wds-high' : c.wds >= 50 ? 'wds-med' : 'wds-low';
    return `<div class="ws-chip ${cls}" title="${tooltip}">
      <span class="ws-cluster">${c.cn}</span>
      ${c.gpuType ? `<span class="ws-gpu">${c.gpuType}</span>` : ''}
      <span class="wds-badge ${wdsCls}">${c.wds}</span>
      <span class="ws-headroom">${c.freeForTeam} free</span>
      ${(() => {
        const cps = _partitionData?.[c.cn];
        return cps ? `<span class="ws-idle">${cps.idle_nodes || 0} idle</span>` : '';
      })()}
      ${acctShort ? `<span class="ws-acct">via ${acctShort}</span>` : ''}
      ${(() => {
        const mf = _myFairshareData?.clusters?.[c.cn]?.[c.bestAcct];
        if (!mf) return '';
        const cls = mf.level_fs >= 1.2 ? 'ws-fs-good' : mf.level_fs >= 0.8 ? 'ws-fs-neutral' : 'ws-fs-low';
        return `<span class="ws-my-fs ${cls}">you ${mf.level_fs.toFixed(1)}</span>`;
      })()}
    </div>`;
  }).join('') + '</div>';
}

function _renderPppAllocations(data) {
  const el = document.getElementById('ppp-alloc-body');
  if (!el) return;
  const clusters = data.clusters || {};
  const configOrder = Object.keys(CLUSTERS).filter(c => c !== 'local');
  const names = Object.keys(clusters).sort((a, b) => {
    const aAlloc = clusters[a].team_gpu_alloc;
    const bAlloc = clusters[b].team_gpu_alloc;
    const aHas = aAlloc === 'any' || (typeof aAlloc === 'number' && aAlloc > 0) ? 0 : 1;
    const bHas = bAlloc === 'any' || (typeof bAlloc === 'number' && bAlloc > 0) ? 0 : 1;
    if (aHas !== bHas) return aHas - bHas;
    const ai = configOrder.indexOf(a);
    const bi = configOrder.indexOf(b);
    return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
  });
  if (!names.length) {
    el.innerHTML = '<div class="no-jobs">No allocation data available</div>';
    return;
  }

  let html = _renderSubmitSummary(clusters);
  html += '<div class="ppp-grid">';
  for (const cn of names) {
    const cd = clusters[cn];
    const accts = Object.entries(cd.accounts || {}).sort((a, b) => (b[1].gpus_allocated || 0) - (a[1].gpus_allocated || 0));
    if (!accts.length) continue;
    const rawMaxAlloc = Math.max(...accts.map(([, d]) => d.gpus_allocated || 1));

    const teamAlloc = cd.team_gpu_alloc;
    const teamNum = teamAlloc === 'any' ? null : (typeof teamAlloc === 'number' ? teamAlloc : null);
    const showTeamAlloc = true;
    const showMe = document.getElementById('ppp-my-toggle')?.checked ?? false;
    const showTeamUsage = document.getElementById('ppp-team-usage-toggle')?.checked ?? false;
    const teamScale = document.getElementById('ppp-scale-toggle')?.checked ?? false;


    const maxAlloc = (teamScale && teamNum && teamNum > 0) ? teamNum * 1.2 : rawMaxAlloc;

    const hasTeamQuota = teamAlloc === 'any' || (teamNum && teamNum > 0);
    const ps = _partitionData?.[cn];
    const idleNodes = ps?.idle_nodes || 0;
    const pendingJobs = ps?.pending_jobs || 0;
    const idleCls = idleNodes > 0 ? 'ppp-idle-ok' : 'ppp-idle-none';

    html += `<div class="ppp-card${hasTeamQuota ? '' : ' ppp-card-dim'}">
      <div class="ppp-card-head">
        <span class="ppp-card-cluster">${cn}</span>
        ${cd.gpu_type ? `<span class="ppp-card-gpu">${cd.gpu_type}</span>` : ''}
        ${teamScale && teamNum ? `<span class="ppp-card-scale-label">scaled to ${teamNum}</span>` : ''}
      </div>
      ${ps ? `<div class="ppp-card-live"><span class="${idleCls}">${idleNodes} idle</span> · ${pendingJobs} queued</div>` : ''}`;

    const overlayCluster = _pppOverlayData?.clusters?.[cn] || {};
    const currentUser = _pppOverlayData?.current_user || USERNAME;
    const teamMembers = _pppOverlayData?.team_members || [];

    const tjCluster = _teamJobsData?.clusters?.[cn]?.summary?.by_user || null;
    const hasJobSplit = !!tjCluster;

    accts.forEach(([acct, ad], i) => {
      const allocPct = Math.min(100, maxAlloc > 0 ? Math.round(ad.gpus_allocated / maxAlloc * 100) : 0);
      const fsCls = ad.level_fs >= 1.2 ? 'ppp-fs-good' : ad.level_fs >= 0.8 ? 'ppp-fs-neutral' : 'ppp-fs-low';
      const consumed = ad.gpus_consumed || 0;
      const toPct = (gpus) => Math.round(gpus / maxAlloc * 100);

      let teamAllocMarker = '';
      if (showTeamAlloc && teamNum) {
        const markerPct = Math.min(98, Math.round(teamNum / maxAlloc * 100));
        teamAllocMarker = `<div class="ppp-team-marker" style="left:${markerPct}%"></div>`;
      }

      const acctJobs = hasJobSplit ? (_teamJobsData?.clusters?.[cn]?.jobs || []).filter(j => j.account === acct) : [];
      const acctUsers = overlayCluster[acct] || {};
      const acctShortName = _shortAcct(acct);
      let myTotalAihub = acctUsers[currentUser] || 0;
      let myTotalSqueue = 0;
      let teamOthersTotal = 0;
      if (teamMembers.length) {
        for (const m of teamMembers) {
          if (m !== currentUser) teamOthersTotal += (acctUsers[m] || 0);
        }
      }
      for (const j of acctJobs) {
        if (j.user === currentUser) myTotalSqueue += (j.gpus || 0);
      }
      let myTotal = Math.max(myTotalAihub, myTotalSqueue);
      myTotal = Math.min(myTotal, consumed || myTotal);
      teamOthersTotal = Math.min(teamOthersTotal, Math.max(0, (consumed || 0) - myTotal));
      const pppNonTeam = Math.max(0, consumed - myTotal - teamOthersTotal);

      const clusterOccupied = cd.cluster_occupied_gpus || 0;
      const allPppsConsumed = accts.reduce((s, [, a]) => s + (a.gpus_consumed || 0), 0);
      const clusterOthers = Math.max(0, clusterOccupied - allPppsConsumed);
      let segments = '';
      if (hasJobSplit) {
        let myRunning = 0, myPending = 0, teamRunGpus = 0, teamPendGpus = 0;
        for (const j of acctJobs) {
          const g = j.gpus || 0;
          if (j.user === currentUser) {
            if (j.state === 'RUNNING') myRunning += g; else myPending += g;
          } else if (teamMembers.includes(j.user)) {
            if (j.state === 'RUNNING') teamRunGpus += g; else teamPendGpus += g;
          }
        }
        const myRunGpus = Math.min(myRunning, myTotal);
        const myPendGpus = Math.min(myPending, myTotal * 0.3);

        const teamRunW = Math.min(teamRunGpus, teamOthersTotal);
        const teamPendW = Math.min(teamPendGpus, teamOthersTotal * 0.3);

        const showProjects = document.getElementById('ppp-project-toggle')?.checked && _projectColors;
        if (showMe && showProjects) {
          const projMap = {};
          for (const j of acctJobs) {
            if (j.user !== currentUser) continue;
            const proj = _getProjectFromJobName(j.job_name) || '_other';
            if (!projMap[proj]) projMap[proj] = { run: 0, pend: 0 };
            if (j.state === 'RUNNING') projMap[proj].run += (j.gpus || 0);
            else projMap[proj].pend += (j.gpus || 0);
          }
          for (const [proj, pd] of Object.entries(projMap).sort((a, b) => (b[1].run + b[1].pend) - (a[1].run + a[1].pend))) {
            const color = _getProjectColor(proj) || 'var(--accent)';
            const runW = Math.min(pd.run, myTotal);
            const pendW = Math.min(pd.pend, myTotal * 0.3);
            if (runW > 0)
              segments += `<div class="ppp-seg ppp-seg-proj" style="width:${toPct(runW)}%;background:${color}"></div>`;
            if (pendW > 0)
              segments += `<div class="ppp-seg ppp-seg-proj-pend" style="width:${toPct(pendW)}%;--proj-color:${color}"></div>`;
          }
        } else {
          if (showMe && myRunGpus > 0)
            segments += `<div class="ppp-seg ppp-seg-me-run" style="width:${toPct(myRunGpus)}%"></div>`;
          if (showMe && myPendGpus > 0)
            segments += `<div class="ppp-seg ppp-seg-me-pend" style="width:${toPct(myPendGpus)}%"></div>`;
        }
        if (showTeamUsage && teamRunW > 0)
          segments += `<div class="ppp-seg ppp-seg-team-run" style="width:${toPct(teamRunW)}%"></div>`;
        if (showTeamUsage && teamPendW > 0)
          segments += `<div class="ppp-seg ppp-seg-team-pend" style="width:${toPct(teamPendW)}%"></div>`;
      } else {
        if (showMe && myTotal > 0)
          segments += `<div class="ppp-seg ppp-seg-me-run" style="width:${toPct(myTotal)}%"></div>`;
        if (showTeamUsage && teamOthersTotal > 0)
          segments += `<div class="ppp-seg ppp-seg-team-run" style="width:${toPct(teamOthersTotal)}%"></div>`;
      }
      if (pppNonTeam > 0)
        segments += `<div class="ppp-seg ppp-seg-ppp-rest" style="width:${toPct(pppNonTeam)}%"></div>`;

      const clusterOcc = cd.cluster_occupied_gpus || 0;
      const clusterTot = cd.cluster_total_gpus || 0;
      const allPpps = accts.reduce((s, [, a]) => s + (a.gpus_consumed || 0), 0);
      const nonPpps = Math.max(0, clusterOcc - allPpps);

      const teamAllocLabel = teamNum ? `${teamNum} GPUs` : (teamAlloc === 'any' ? 'unlimited' : '');

      let myPopLabel, teamPopLabel;
      if (hasJobSplit) {
        const myR = acctJobs.filter(j => j.user === currentUser && j.state === 'RUNNING').reduce((s, j) => s + (j.gpus || 0), 0);
        const myP = acctJobs.filter(j => j.user === currentUser && j.state !== 'RUNNING').reduce((s, j) => s + (j.gpus || 0), 0);
        myPopLabel = myR > 0 || myP > 0 ? `${myR} run` : `${myTotal}`;
        if (myP > 0) myPopLabel += ` · ${myP} pend`;
        let tR = 0, tP = 0;
        for (const j of acctJobs) {
          if (j.user === currentUser) continue;
          if (!teamMembers.includes(j.user)) continue;
          if (j.state === 'RUNNING') tR += (j.gpus || 0); else tP += (j.gpus || 0);
        }
        teamPopLabel = tR > 0 || tP > 0 ? `${tR} run` : `${teamOthersTotal}`;
        if (tP > 0) teamPopLabel += ` · ${tP} pend`;
      } else {
        myPopLabel = `${myTotal}`;
        teamPopLabel = `${teamOthersTotal}`;
      }

      const myFsAcct = _myFairshareData?.clusters?.[cn]?.[acct];
      const myFsLabel = myFsAcct ? `FS ${myFsAcct.level_fs.toFixed(2)}` : '';
      const myFsCls = myFsAcct ? (myFsAcct.level_fs >= 1.2 ? 'pop-fs-good' : myFsAcct.level_fs >= 0.8 ? 'pop-fs-neutral' : 'pop-fs-low') : '';

      const popupRows = [
        { label: currentUser, value: myPopLabel, detail: myFsLabel, cls: `pop-me ${myFsCls}` },
        { label: 'team', value: teamPopLabel, detail: '', cls: 'pop-team' },
        ...(teamAllocLabel ? [{ label: 'team alloc', value: teamAllocLabel, detail: 'informal', cls: 'pop-team-alloc' }] : []),
        { label: `${acctShortName} non-team`, value: `${pppNonTeam}`, detail: '', cls: 'pop-ppp' },
        { label: 'other PPPs', value: `${nonPpps}`, detail: '', cls: 'pop-other' },
        { label: 'cluster total', value: `${clusterOcc} / ${clusterTot}`, detail: clusterTot > 0 ? `${Math.round(clusterOcc/clusterTot*100)}%` : '', cls: 'pop-cluster' },
      ];
      const popupHtml = popupRows.map(r =>
        `<div class="ppp-pop-row ${r.cls}"><span class="ppp-pop-label">${r.label}</span><span class="ppp-pop-val">${r.value}</span>${r.detail ? `<span class="ppp-pop-detail">${r.detail}</span>` : ''}</div>`
      ).join('');

      html += `<div class="ppp-acct-row">
        <span class="ppp-acct-name" title="${acct}">${_shortAcct(acct)}</span>
        <div class="ppp-bar-outer ppp-bar-hoverable" onclick="openUserBreakdown('${cn}','${acct}')">
          <div class="ppp-bar-wrap">
            ${segments}
          </div>
          ${teamAllocMarker}
          <div class="ppp-popup">${popupHtml}</div>
        </div>
        <span class="ppp-acct-nums"><strong>${consumed}</strong> / ${ad.gpus_allocated}</span>
        ${(() => {
          const curGpu = CLUSTERS[cn]?.gpu_type || '';
          const w = computeWds(cn, acct, ad, curGpu);
          const wCls = w.wds >= 75 ? 'wds-high' : w.wds >= 50 ? 'wds-med' : 'wds-low';
          const myFsCls = w.myLevelFs >= 1.2 ? 'green' : w.myLevelFs >= 0.8 ? 'amber' : 'red';
          const pppFsCls = ad.level_fs >= 1.2 ? 'green' : ad.level_fs >= 0.8 ? 'amber' : 'red';
          return `<span class="wds-badge-wrap">
            <span class="wds-badge ${wCls}">${w.wds}</span>
            <div class="wds-popup">
              <div class="wds-pop-row wds-pop-head"><span>WDS</span><span class="${wCls}">${w.wds}</span></div>
              <div class="wds-pop-row"><span>resource fit</span><span class="${w.resourceGate >= 1 ? 'green' : w.resourceGate >= 0.5 ? 'amber' : 'red'}">${w.resourceGate >= 1 ? 'fits' : w.resourceGate.toFixed(2)}</span></div>
              <div class="wds-pop-row"><span>your FS</span><span class="${myFsCls}">${w.myLevelFs > 0 ? w.myLevelFs.toFixed(2) : 'no data'}</span></div>
              <div class="wds-pop-row"><span>PPP FS</span><span class="${pppFsCls}">${ad.level_fs.toFixed(2)}</span></div>
              <div class="wds-pop-row"><span>queue</span><span class="${w.idleNodes > 0 ? (w.queueScore >= 0.5 ? 'green' : 'amber') : 'red'}">${w.idleNodes} idle / ${w.pendingQueue}q</span></div>
              <div class="wds-pop-row"><span>team quota</span><span class="${w.freeForTeam > 0 ? 'green' : w.pppHeadroom > 0 ? 'amber' : 'red'}">${w.freeForTeam > 0 ? `${w.freeForTeam} free` : w.pppHeadroom > 0 ? `over (PPP ${w.pppHeadroom})` : 'full'}</span></div>
            </div>
          </span>`;
        })()}
      </div>`;
    });

    const hasAnySplit = !!_teamJobsData;
    const showProjectsLegend = document.getElementById('ppp-project-toggle')?.checked && _projectColors;
    html += '<div class="ppp-overlay-legend">';
    if (showMe && showProjectsLegend) {
      const seenProjs = new Set();
      const allJobs = _teamJobsData?.clusters?.[cn]?.jobs || [];
      for (const j of allJobs) {
        if (j.user !== currentUser) continue;
        const p = _getProjectFromJobName(j.job_name);
        if (p) seenProjs.add(p);
      }
      for (const p of seenProjs) {
        const color = _getProjectColor(p) || 'var(--accent)';
        html += `<span><span class="ppp-legend-swatch" style="background:${color}"></span>${p}</span>`;
      }
      if (!seenProjs.size) html += `<span><span class="ppp-legend-swatch swatch-me"></span>${currentUser}</span>`;
    } else if (showMe) {
      html += `<span><span class="ppp-legend-swatch swatch-me"></span>${currentUser}${hasAnySplit ? ' run' : ''}</span>`;
      if (hasAnySplit) html += `<span><span class="ppp-legend-swatch swatch-me-pend"></span>${currentUser} pend</span>`;
    }
    if (showTeamUsage) {
      const tn = _pppOverlayData?.team_name || 'team';
      html += `<span><span class="ppp-legend-swatch swatch-team"></span>${tn}${hasAnySplit ? ' run' : ''}</span>`;
      if (hasAnySplit) html += `<span><span class="ppp-legend-swatch swatch-team-pend"></span>${tn} pend</span>`;
    }
    html += `<span><span class="ppp-legend-swatch swatch-ppp-rest"></span>PPP non-team</span>`;
    html += '</div>';


    const clusterOccupied = cd.cluster_occupied_gpus || 0;
    const clusterTotal = cd.cluster_total_gpus || 0;
    const allPppsConsumed = accts.reduce((s, [, a]) => s + (a.gpus_consumed || 0), 0);
    const clusterOthers = Math.max(0, clusterOccupied - allPppsConsumed);
    const clusterPct = clusterTotal > 0 ? Math.round(clusterOccupied / clusterTotal * 100) : 0;

    html += `<div class="ppp-card-footer">
      ${clusterTotal > 0 ? `<span class="ppp-cluster-occ" title="Cluster-wide: ${clusterOccupied} / ${clusterTotal} GPUs (our PPPs: ${allPppsConsumed}, others: ${clusterOthers})">${clusterPct}% cluster load</span>` : ''}
    </div></div>`;
  }
  html += '</div>';
  el.innerHTML = html;
}

/* ── GPU Usage History Chart ── */

let _historyChart = null;
let _historyDays = 14;

function setHistoryDays(d) {
  _historyDays = d;
  document.querySelectorAll('.history-range-btns .btn-sm').forEach(b => {
    b.classList.toggle('active', parseInt(b.dataset.days) === d);
  });
  refreshUsageHistory();
}

async function refreshUsageHistory() {
  const cluster = document.getElementById('history-cluster')?.value || '';
  const params = new URLSearchParams({ days: _historyDays });
  if (cluster) params.set('cluster', cluster);

  try {
    const res = await fetch('/api/aihub/history?' + params);
    const data = await res.json();
    if (data.status === 'ok') _renderHistoryChart(data);
  } catch (_) {}
}

const _CHART_COLORS = {
  'reasoning': { line: '#558B2F', fill: 'rgba(85,139,47,0.12)', dash: '#558B2F' },
  'robustness': { line: '#A4D65E', fill: 'rgba(164,214,94,0.12)', dash: '#A4D65E' },
  'long-context': { line: '#7CB342', fill: 'rgba(124,179,66,0.12)', dash: '#7CB342' },
};

function _chartColor(acct) {
  const short = _shortAcct(acct);
  return _CHART_COLORS[short] || { line: '#888', fill: 'rgba(136,136,136,0.1)', dash: '#888' };
}

function _renderHistoryChart(data) {
  const canvas = document.getElementById('history-chart');
  if (!canvas) return;

  if (_historyChart) {
    _historyChart.destroy();
    _historyChart = null;
  }

  const allClusters = data.clusters || {};
  const clusterNames = Object.keys(allClusters);
  if (!clusterNames.length) return;

  const merged = {};
  for (const [cn, series] of Object.entries(allClusters)) {
    for (const [acct, points] of Object.entries(series)) {
      const label = clusterNames.length > 1 ? `${_shortAcct(acct)} (${cn})` : _shortAcct(acct);
      merged[label] = { acct, points };
    }
  }

  const allDates = new Set();
  for (const { points } of Object.values(merged)) {
    for (const p of points) allDates.add(p.date);
  }
  const labels = [...allDates].sort();

  const datasets = [];
  for (const [label, { acct, points }] of Object.entries(merged)) {
    const colors = _chartColor(acct);
    const dateMap = {};
    for (const p of points) dateMap[p.date] = p;

    datasets.push({
      label: label + ' (consumed)',
      data: labels.map(d => dateMap[d]?.gpus_consumed ?? null),
      borderColor: colors.line,
      backgroundColor: colors.fill,
      fill: true,
      tension: 0.3,
      pointRadius: 2,
      pointHoverRadius: 5,
      borderWidth: 2,
    });

    datasets.push({
      label: label + ' (allocated)',
      data: labels.map(d => dateMap[d]?.gpus_allocated ?? null),
      borderColor: colors.dash,
      borderDash: [6, 3],
      fill: false,
      tension: 0,
      pointRadius: 0,
      borderWidth: 1.5,
    });
  }

  const ctx = canvas.getContext('2d');
  _historyChart = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: {
          position: 'bottom',
          labels: {
            font: { family: "'JetBrains Mono', monospace", size: 10 },
            boxWidth: 16,
            padding: 12,
            filter: item => item.text.includes('consumed'),
          },
        },
        tooltip: {
          titleFont: { family: "'JetBrains Mono', monospace", size: 11 },
          bodyFont: { family: "'JetBrains Mono', monospace", size: 10 },
          callbacks: {
            label: ctx => `${ctx.dataset.label}: ${ctx.parsed.y ?? '—'} GPUs`,
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          ticks: {
            font: { family: "'JetBrains Mono', monospace", size: 9 },
            maxRotation: 0,
            callback: (val, i) => {
              const d = labels[i];
              return d ? d.slice(5) : '';
            },
          },
        },
        y: {
          beginAtZero: true,
          grid: { color: 'rgba(128,128,128,0.1)' },
          ticks: {
            font: { family: "'JetBrains Mono', monospace", size: 9 },
          },
        },
      },
    },
  });
}

let _pppOverlayData = null;
let _myFairshareData = null;
let _projectColors = null;

async function _fetchProjectColors() {
  if (_projectColors) return;
  try {
    const res = await fetch('/api/settings');
    const data = await res.json();
    if (data.projects) _projectColors = data.projects;
  } catch (_) {}
}

function _getProjectFromJobName(name) {
  if (!name) return '';
  const m = name.match(/^([a-zA-Z][a-zA-Z0-9-]*)_/);
  return m ? m[1].toLowerCase() : '';
}

function _getProjectColor(proj) {
  if (!proj || !_projectColors) return null;
  const cfg = _projectColors[proj];
  return cfg?.color || null;
}
let _pppOverlayFetching = false;

async function _ensureOverlayData() {
  if (_pppOverlayData || _pppOverlayFetching) return _pppOverlayData;
  _pppOverlayFetching = true;
  try {
    const res = await fetch('/api/aihub/team_overlay');
    const data = await res.json();
    if (data.status === 'ok') _pppOverlayData = data;
  } catch (_) {}
  _pppOverlayFetching = false;
  return _pppOverlayData;
}

async function _fetchMyFairshare() {
  try {
    const res = await fetch('/api/aihub/my_fairshare');
    const data = await res.json();
    if (data.status === 'ok') _myFairshareData = data;
  } catch (_) {}
}

async function togglePppOverlays() {
  const showMe = document.getElementById('ppp-my-toggle')?.checked;
  const showTeamUsage = document.getElementById('ppp-team-usage-toggle')?.checked;
  if ((showMe || showTeamUsage) && !_pppOverlayData) {
    await _ensureOverlayData();
  }
  if (_pppAllocData) _renderPppAllocations(_pppAllocData);
}

function toggleTeamOverlay() {
  if (_pppAllocData) _renderPppAllocations(_pppAllocData);
}

async function openUserBreakdown(cluster, account) {
  const acctShort = _shortAcct(account);
  const gpuType = CLUSTERS[cluster]?.gpu_type || '';
  const teamMembers = new Set(_pppOverlayData?.team_members || []);

  let overlay = document.getElementById('user-breakdown-overlay');
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.id = 'user-breakdown-overlay';
    overlay.className = 'ub-overlay';
    overlay.onclick = (e) => { if (e.target === overlay) overlay.classList.remove('open'); };
    document.body.appendChild(overlay);
  }

  overlay.innerHTML = `<div class="ub-modal">
    <div class="ub-head">
      <div>
        <div class="ub-title">${cluster} / ${acctShort}</div>
        <div class="ub-sub">${gpuType ? gpuType + ' — ' : ''}Per-user GPU breakdown</div>
      </div>
      <button class="btn" onclick="document.getElementById('user-breakdown-overlay').classList.remove('open')">✕</button>
    </div>
    <div class="ub-body"><div class="no-jobs">Loading user data...</div></div>
  </div>`;
  overlay.classList.add('open');

  try {
    const res = await fetch(`/api/aihub/users?account=${encodeURIComponent(account)}&cluster=${cluster}&days=3`);
    const data = await res.json();
    if (data.status !== 'ok' || !data.users?.length) {
      overlay.querySelector('.ub-body').innerHTML = '<div class="no-jobs">No user data available</div>';
      return;
    }

    const allJobs = _teamJobsData?.clusters?.[cluster]?.jobs || [];
    const acctJobsByUser = {};
    for (const j of allJobs) {
      if (j.account !== account) continue;
      if (!acctJobsByUser[j.user]) acctJobsByUser[j.user] = { running: 0, pending: 0, cpu_running: 0, cpu_pending: 0 };
      const d = acctJobsByUser[j.user];
      if (j.is_gpu === false) {
        if (j.state === 'RUNNING') d.cpu_running += (j.nodes || 1);
        else d.cpu_pending += (j.nodes || 1);
      } else {
        if (j.state === 'RUNNING') d.running += (j.gpus || 0);
        else d.pending += (j.gpus || 0);
      }
    }
    const allocData = _pppAllocData?.clusters?.[cluster]?.accounts?.[account] || {};
    const totalConsumed = allocData.gpus_consumed || 0;
    const totalAllocated = allocData.gpus_allocated || 0;
    const maxUser = data.users[0]?.avg_gpus_consumed || 1;

    const activeUsers = data.users.filter(u => {
      const tj = acctJobsByUser[u.user] || {};
      return (tj.running || 0) > 0 || (tj.pending || 0) > 0 || (tj.cpu_running || 0) > 0 || (tj.cpu_pending || 0) > 0;
    });
    const inactiveUsers = data.users.filter(u => {
      const tj = acctJobsByUser[u.user] || {};
      return !((tj.running || 0) > 0 || (tj.pending || 0) > 0 || (tj.cpu_running || 0) > 0 || (tj.cpu_pending || 0) > 0);
    });

    const renderUser = (u) => {
      const isMe = u.user === USERNAME;
      const isTeam = teamMembers.has(u.user);
      const tj = acctJobsByUser[u.user] || {};
      const running = tj.running || 0;
      const pending = tj.pending || 0;
      const cpuR = tj.cpu_running || 0;
      const cpuP = tj.cpu_pending || 0;
      const hasLive = running > 0 || pending > 0 || cpuR > 0 || cpuP > 0;
      const barPct = Math.round(u.avg_gpus_consumed / maxUser * 100);

      let statParts = [];
      if (hasLive) {
        if (running > 0) statParts.push(`<span class="ub-live-run">${running} run</span>`);
        if (pending > 0) statParts.push(`<span class="ub-live-pend">${pending} pend</span>`);
        if (cpuR > 0 || cpuP > 0) statParts.push(`<span class="ub-live-cpu">${cpuR + cpuP} cpu</span>`);
      } else {
        statParts.push(`<span class="ub-live-avg">${Math.round(u.avg_gpus_consumed)} avg</span>`);
      }

      return `<div class="ub-user-row${isMe ? ' ub-me' : ''}">
        <span class="ub-user-name${isMe ? ' ub-name-me' : isTeam ? ' ub-name-team' : ''}">${u.user}</span>
        <span class="ub-bar-wrap"><span class="ub-bar ${isMe ? 'ub-bar-me' : isTeam ? 'ub-bar-team' : 'ub-bar-other'}" style="width:${barPct}%"></span></span>
        <span class="ub-stats">${statParts.join('<span class="ub-sep">·</span>')}</span>
      </div>`;
    };

    let rows = activeUsers.map(renderUser).join('');
    if (inactiveUsers.length) {
      rows += `<div class="ub-divider"><span>recent (no active jobs)</span></div>`;
      rows += inactiveUsers.map(renderUser).join('');
    }

    const header = `<div class="ub-summary">
      <span>Total: <strong>${totalConsumed}</strong> / ${totalAllocated} GPUs</span>
      <span>${data.users.length} users</span>
    </div>`;

    overlay.querySelector('.ub-body').innerHTML = header + '<div class="ub-users">' + rows + '</div>';
  } catch (e) {
    overlay.querySelector('.ub-body').innerHTML = '<div class="no-jobs" style="color:var(--red)">Failed to load</div>';
  }
}

/* ── Clusters page init ── */

let _clustersPageInited = false;

function _populateAccountSelect() {
  const sel = document.getElementById('adv-account');
  if (!sel) return;
  if (sel.options.length > 1) return;
  try {
    const settingsEl = document.getElementById('cluster-data');
    const pppEl = document.querySelector('script[id="cluster-data"]');
  } catch (_) {}
  if (_pppAllocData && _pppAllocData.clusters) {
    const seen = new Set();
    for (const cd of Object.values(_pppAllocData.clusters)) {
      for (const acct of Object.keys(cd.accounts || {})) seen.add(acct);
    }
    for (const acct of seen) {
      const opt = document.createElement('option');
      opt.value = acct;
      opt.textContent = _shortAcct(acct);
      sel.appendChild(opt);
    }
  }
}

async function initClustersPage() {
  refreshPppAllocations().then(() => _populateAccountSelect());
  refreshUsageHistory();
}

/* ── Team Jobs ── */

async function _fetchTeamJobs() {
  try {
    const res = await fetch('/api/team_jobs');
    const data = await res.json();
    if (data.status === 'ok') _teamJobsData = data;
  } catch (_) {}
}

async function refreshTeamJobs() {
  await _fetchTeamJobs();
  if (_pppAllocData) _renderPppAllocations(_pppAllocData);
}

function _renderTeamJobs(clusters) {
  const el = document.getElementById('team-jobs-body');
  if (!el) return;

  const clusterNames = Object.keys(clusters)
    .filter(cn => {
      const s = clusters[cn].summary || {};
      return (s.total_running || 0) + (s.total_pending || 0) + (s.total_dependent || 0) > 0;
    })
    .sort((a, b) => {
      const sa = clusters[a].summary || {}, sb = clusters[b].summary || {};
      return (sb.total_running || 0) - (sa.total_running || 0);
    });

  if (!clusterNames.length) {
    el.innerHTML = '<div class="no-jobs">No team jobs found</div>';
    return;
  }

  let html = '<div class="tj-grid">';
  for (const cn of clusterNames) {
    const cd = clusters[cn];
    const summary = cd.summary || {};
    const byUser = summary.by_user || {};
    const gpuType = CLUSTERS[cn]?.gpu_type || '';

    const users = Object.entries(byUser)
      .map(([u, d]) => ({ user: u, ...d }))
      .filter(u => u.running > 0 || u.pending > 0 || u.dependent > 0)
      .sort((a, b) => b.running - a.running || b.pending - a.pending);

    if (!users.length) continue;

    const totalR = summary.total_running || 0;
    const totalP = summary.total_pending || 0;
    const totalD = summary.total_dependent || 0;
    const maxGpus = Math.max(...users.map(u => u.running + u.pending + u.dependent));

    html += `<div class="tj-card">
      <div class="tj-card-head">
        <span class="tj-card-cluster">${cn}</span>
        ${gpuType ? `<span class="ppp-card-gpu">${gpuType}</span>` : ''}
        <span class="tj-card-totals">
          ${totalR ? `<span class="tj-badge tj-running">${totalR}</span>` : ''}
          ${totalP ? `<span class="tj-badge tj-pending">${totalP}</span>` : ''}
          ${totalD ? `<span class="tj-badge tj-dependent">${totalD}</span>` : ''}
        </span>
      </div>`;

    for (const u of users) {
      const isMe = u.user === USERNAME;
      const total = u.running + u.pending + u.dependent;
      const barMax = maxGpus || 1;
      const rW = Math.round(u.running / barMax * 100);
      const pW = Math.round(u.pending / barMax * 100);
      const dW = Math.round(u.dependent / barMax * 100);

      html += `<div class="tj-user-row${isMe ? ' tj-me' : ''}">
        <span class="tj-user-name${isMe ? ' tj-user-me' : ''}">${u.user}</span>
        <span class="tj-bar-wrap">
          ${rW > 0 ? `<span class="tj-bar-seg tj-seg-running" style="width:${rW}%" title="Running: ${u.running}"></span>` : ''}
          ${pW > 0 ? `<span class="tj-bar-seg tj-seg-pending" style="width:${pW}%" title="Pending: ${u.pending}"></span>` : ''}
          ${dW > 0 ? `<span class="tj-bar-seg tj-seg-dependent" style="width:${dW}%" title="Dependent: ${u.dependent}"></span>` : ''}
        </span>
        <span class="tj-user-total">${total}</span>
      </div>`;
    }

    html += '</div>';
  }
  html += '</div>';
  el.innerHTML = html;
}

function _renderAvailTable() {
  const el = document.getElementById('avail-body');
  if (!el || !_partitionData) {
    if (el) el.innerHTML = '<div class="no-jobs">No partition data available</div>';
    return;
  }

  const entries = Object.entries(_partitionData)
    .map(([name, ps]) => ({
      name,
      gpu: ps.gpu_type || '',
      parts: (ps.partitions || []).filter(p => p.total_nodes > 0),
      bestWait: _bestWaitCls(ps.partitions || []),
    }))
    .filter(e => e.parts.length > 0)
    .sort((a, b) => {
      const order = { fast: 0, moderate: 1, slow: 2, long: 3 };
      return (order[a.bestWait] ?? 3) - (order[b.bestWait] ?? 3);
    });

  if (!entries.length) {
    el.innerHTML = '<div class="no-jobs">No partition data available</div>';
    return;
  }

  let rows = '';
  for (const e of entries) {
    const gpuBadge = e.gpu ? `<span class="avail-gpu-badge">${e.gpu}</span>` : '';
    rows += `<tr class="avail-cluster-row"><td colspan="6">${e.name}${gpuBadge}</td></tr>`;
    for (const p of e.parts) {
      const gpuPer = p.gpus_per_node || 0;
      const idleGpus = p.idle_nodes * gpuPer;
      const totalGpus = p.total_nodes * gpuPer;
      const idleStr = gpuPer > 0 ? idleGpus.toLocaleString() : `${p.idle_nodes}n`;
      const totalStr = gpuPer > 0 ? totalGpus.toLocaleString() : `${p.total_nodes}n`;
      const waitCls = `avail-wait-${p.est_wait_cls || 'long'}`;
      rows += `<tr>
        <td>${p.name}</td>
        <td>${_fmtTime(p.max_time)}</td>
        <td>${p.preemptable ? 'yes' : ''}</td>
        <td>${idleStr}</td>
        <td>${totalStr}</td>
        <td class="${waitCls}">${p.est_wait || '—'}</td>
      </tr>`;
    }
  }

  el.innerHTML = `<table class="avail-table">
    <thead><tr>
      <th>Partition</th>
      <th>Max Time</th>
      <th>Preempt</th>
      <th>Idle GPUs</th>
      <th>Total GPUs</th>
      <th>Est. Wait</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

function getPartitionSummary(clusterName) {
  if (!_partitionData) return null;
  return _partitionData[clusterName] || null;
}

function partitionChipHtml(clusterName) {
  const ps = getPartitionSummary(clusterName);
  if (!ps) return '';
  const idle = ps.idle_nodes || 0;
  const pending = ps.pending_jobs || 0;
  const nParts = ps.gpu_partitions || 0;
  const cls = idle > 0 ? 'part-has-idle' : (pending > 0 ? 'part-busy' : '');
  return `<span class="partition-chip ${cls}" title="${nParts} GPU partitions, ${idle} idle nodes, ${pending} pending jobs" onclick="event.stopPropagation();openAdvisor('${clusterName}')">
    <span class="part-count">${nParts}p</span>
    ${idle > 0 ? `<span class="part-idle">${idle} idle</span>` : ''}
    ${pending > 0 ? `<span class="part-pending">${pending}q</span>` : ''}
  </span>`;
}

async function openAdvisor(preselectedCluster) {
  showTab('clusters');
  const clusterInput = document.getElementById('adv-cluster');
  if (clusterInput && preselectedCluster) {
    clusterInput.value = preselectedCluster;
  }
}

function closeAdvisor() {}

async function runAdvisor() {
  const resultsEl = document.getElementById('adv-results');
  if (!resultsEl) return;
  resultsEl.innerHTML = '<div class="no-jobs">Analyzing partitions across clusters...</div>';

  const nodes = parseInt(document.getElementById('adv-nodes')?.value || '1') || 1;
  const timeLimit = document.getElementById('adv-time')?.value || '4:00:00';
  const canPreempt = document.getElementById('adv-preempt')?.checked || false;
  const cluster = document.getElementById('adv-cluster')?.value || '';
  const account = document.getElementById('adv-account')?.value || '';

  const body = { nodes, time_limit: timeLimit, can_preempt: canPreempt };
  if (cluster) body.clusters = [cluster];
  if (account) body.account = account;

  try {
    const res = await fetch('/api/recommend', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (data.status === 'ok') {
      _renderAdvisorResults(data.recommendations || []);
    } else {
      resultsEl.innerHTML = `<div class="err-msg">${data.error || 'Unknown error'}</div>`;
    }
  } catch (e) {
    resultsEl.innerHTML = `<div class="err-msg">Failed to fetch recommendations</div>`;
  }
}

function _fsGaugeHtml(levelFs) {
  if (!levelFs) return '<span class="dim">—</span>';
  const cls = levelFs >= 1.2 ? 'fs-good' : levelFs >= 0.8 ? 'fs-neutral' : 'fs-low';
  const pct = Math.min(100, Math.round(Math.min(levelFs, 2.5) / 2.5 * 100));
  return `<span class="adv-fs-gauge">
    <span class="adv-fs-bar"><span class="adv-fs-fill ${cls}" style="width:${pct}%"></span></span>
    <span class="adv-fs-val">${levelFs.toFixed(2)}</span>
  </span>`;
}

function _renderAdvisorResults(recs) {
  const el = document.getElementById('adv-results');
  if (!el) return;
  if (!recs.length) {
    el.innerHTML = '<div class="no-jobs">No eligible partitions found for these requirements</div>';
    return;
  }

  const hasAcct = recs.some(r => r.recommended_account);
  const rows = recs.slice(0, 20).map((r, i) => {
    const d = r.details || {};
    const isBest = i === 0;
    const preemptBadge = d.preemptable ? '<span class="adv-preempt-tag">preemptable</span>' : '';
    const defaultBadge = d.is_default ? '<span class="adv-default-tag">default</span>' : '';
    const occLevel = d.occupancy_pct >= 90 ? 'high' : d.occupancy_pct >= 60 ? 'medium' : 'low';
    const acctCell = hasAcct ? `<td class="adv-acct"><span class="adv-acct-short">${r.recommended_account ? _shortAcct(r.recommended_account) : '—'}</span></td>` : '';
    const fsCell = hasAcct ? `<td>${_fsGaugeHtml(r.level_fs)}</td>` : '';
    return `<tr class="${isBest ? 'adv-best' : ''}">
      <td class="adv-rank">${isBest ? '★' : r.rank}</td>
      <td><span class="adv-cluster">${r.cluster}</span></td>
      ${acctCell}
      <td><span class="adv-part">${r.partition}</span> ${defaultBadge}${preemptBadge}</td>
      <td><span class="tt-wait-badge ${r.est_wait_cls}">${r.est_wait}</span></td>
      ${fsCell}
      <td class="dim">${d.idle_nodes}</td>
      <td class="dim">${d.pending_jobs}</td>
      <td>
        <span class="adv-occ-bar"><span class="adv-occ-fill ${occLevel}" style="width:${Math.min(d.occupancy_pct, 100)}%"></span></span>
        <span class="dim">${d.occupancy_pct}%</span>
      </td>
      <td class="dim">T${d.priority_tier}</td>
      <td class="dim">${d.max_time || ''}</td>
      <td class="adv-tip">${r.tip || ''}</td>
    </tr>`;
  }).join('');

  const acctHdr = hasAcct ? '<th>Account</th>' : '';
  const fsHdr = hasAcct ? '<th>Fairshare</th>' : '';
  el.innerHTML = `<table class="adv-table">
    <thead><tr>
      <th></th><th>Cluster</th>${acctHdr}<th>Partition</th><th>Est. Wait</th>
      ${fsHdr}<th>Idle</th><th>Queue</th><th>Occupancy</th><th>Tier</th><th>Limit</th><th>Notes</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
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
    const parts = name.split('_');
    const short = parts.length > 2 ? parts.slice(-1)[0] : name;
    const spacePct = pq.space_used_pct || 0;
    const inodePct = pq.files_used_pct || 0;
    const worst = Math.max(spacePct, inodePct);
    const level = worst >= 95 ? 'crit' : worst >= 85 ? 'warn' : 'ok';
    const detail = `${short}: ${pq.space_used_human} / ${pq.space_quota_human} (${spacePct}% space, ${inodePct}% inodes)`;
    badges.push(`<span class="quota-pill ${level}" title="${detail}">${short} ${Math.round(worst)}%</span>`);
  }
  return badges.join('');
}

/* ── Pending-reason translator ── */

function _translateReason(reason, jobNodes, jobGpuStr, freeNodes, freeGpus) {
  const r = (reason || '').trim();
  if (!r || r === 'None') return null;

  const needNodes = parseInt(jobNodes, 10) || 0;

  if (r === 'Priority') {
    return {
      short: 'Waiting for priority',
      detail: 'Other pending jobs have higher fair-share priority. Your job moves up as they start or finish.',
      blocker: 'priority',
    };
  }
  if (r === 'Resources') {
    if (needNodes > 0 && freeNodes !== null && needNodes > freeNodes) {
      return {
        short: `Needs ${needNodes} nodes, only ${freeNodes} free`,
        detail: `Job requires ${needNodes} node${needNodes > 1 ? 's' : ''} but only ${freeNodes} are available. Waiting for ${needNodes - freeNodes} more to free up.`,
        blocker: 'resources',
      };
    }
    return {
      short: 'Waiting for resources',
      detail: 'Required nodes or GPUs are not yet available. Waiting for running jobs to complete.',
      blocker: 'resources',
    };
  }
  if (r === 'DependencyNeverSatisfied') {
    return {
      short: 'Dependency failed',
      detail: 'A parent job failed or was cancelled. This job will not run unless manually released.',
      blocker: 'blocked',
    };
  }
  if (r.includes('Dependency')) {
    return {
      short: 'Waiting for dependency',
      detail: 'This job depends on another job finishing first. It will start automatically when the parent completes.',
      blocker: 'dependency',
    };
  }
  if (r.includes('QOS') || r.includes('Assoc') || r.includes('MaxGres') || r.includes('GrpGRES') || r.includes('GrpCpu')) {
    return {
      short: 'QOS / quota limit',
      detail: 'You\u2019ve hit a scheduler limit (max GPUs per user, group resource cap, or QOS constraint). Waiting for your other jobs to finish.',
      blocker: 'qos',
    };
  }
  if (r.includes('NodeNotAvail') || r === 'ReqNodeNotAvail') {
    return {
      short: 'Node unavailable',
      detail: 'Requested node is down, reserved for maintenance, or otherwise unavailable.',
      blocker: 'resources',
    };
  }
  if (r === 'BeginTime') {
    return {
      short: 'Scheduled start time',
      detail: 'Job is configured to start at a specific future time.',
      blocker: 'time',
    };
  }
  if (r.includes('requeued') || r.includes('held') || r.includes('Held')) {
    return {
      short: 'Held / requeued',
      detail: 'Job failed to launch and was requeued in held state. May need manual release.',
      blocker: 'blocked',
    };
  }
  return {
    short: r.length > 30 ? r.slice(0, 28) + '\u2026' : r,
    detail: `Slurm reason: ${r}`,
    blocker: 'unknown',
  };
}

/* ── Wait-time estimation for pending jobs ── */

function _estimateWait(occupancyPct, pendingNodes, totalNodes, t, reason, clusterName) {
  const queueRatio = totalNodes > 0 ? pendingNodes / totalNodes : 0;
  const hasPriority = t.alloc > 0;
  const r = (reason || '').trim();
  const tu = clusterName ? (_teamUsageData[clusterName] || null) : null;

  const aihub = _pppAllocData?.clusters?.[clusterName];
  const bestFs = aihub?.best_priority?.level_fs || 0;
  const clOcc = aihub?.cluster_occupied_gpus || 0;
  const clTot = aihub?.cluster_total_gpus || 1;
  const clPct = aihub ? Math.round(clOcc / clTot * 100) : occupancyPct;
  if (aihub) occupancyPct = clPct;

  if (r === 'DependencyNeverSatisfied') {
    return { label: 'won\u2019t run', cls: 'long',
      reason: 'Parent job failed. This job is stuck unless manually released.' };
  }
  if (r.includes('Dependency') && r !== 'None') {
    return { label: 'blocked', cls: 'moderate',
      reason: 'Waiting for a parent job to finish. Start time depends on the parent.' };
  }
  if (r.includes('QOS') || r.includes('Assoc') || r.includes('MaxGres') || r.includes('GrpGRES') || r.includes('GrpCpu')) {
    return { label: 'quota-limited', cls: 'slow',
      reason: 'Blocked by a scheduler quota. Will start when your other jobs finish and free up your allowance.' };
  }
  if (r === 'BeginTime') {
    return { label: 'scheduled', cls: 'moderate',
      reason: 'Job has a deferred start time. Will begin at the scheduled time if resources are available.' };
  }
  if (r.includes('requeued') || r.includes('held') || r.includes('Held')) {
    return { label: 'held', cls: 'long',
      reason: 'Job is held after a failed launch. Needs manual intervention to release.' };
  }

  if (r === 'Priority') {
    if (bestFs >= 1.5 && occupancyPct < 80)
      return { label: '~5 \u2013 15 min', cls: 'fast',
        reason: `Strong fairshare (FS ${bestFs.toFixed(1)}). Your PPP has scheduling credit \u2014 should start soon.` };
    if (bestFs >= 1.2 && occupancyPct < 90)
      return { label: '~15 \u2013 45 min', cls: 'fast',
        reason: `Good fairshare (FS ${bestFs.toFixed(1)}). PPP is underutilizing its allocation \u2014 moderate wait.` };
    if (bestFs >= 0.8)
      return { label: '~30 min \u2013 2h', cls: 'moderate',
        reason: `Neutral fairshare (FS ${bestFs.toFixed(1)}). PPP near its allocation \u2014 normal scheduling priority.` };
    if (bestFs > 0)
      return { label: '~1 \u2013 4h', cls: 'slow',
        reason: `Low fairshare (FS ${bestFs.toFixed(1)}). PPP is overdrawn \u2014 other teams get priority.` };
    return { label: '~1 \u2013 4h', cls: 'slow',
      reason: 'Waiting for priority. Other pending jobs have higher fair-share priority.' };
  }

  if (pendingNodes === 0 && occupancyPct < 90) {
    return { label: 'starts immediately', cls: 'fast',
      reason: 'Cluster has free capacity and no queue.' };
  }
  if (occupancyPct < 70) {
    if (hasPriority && t.pct < 50)
      return { label: 'minutes', cls: 'fast',
        reason: 'Cluster has significant free capacity and your team has high priority.' };
    return { label: '< 30 min', cls: 'fast',
      reason: 'Cluster has significant free capacity.' };
  }

  if (hasPriority && t.pct < 50) {
    if (occupancyPct < 90)
      return { label: '< 30 min', cls: 'fast',
        reason: 'Team well under quota \u2014 scheduler gives you top priority.' };
    if (queueRatio < 0.5)
      return { label: '~30 min \u2013 2h', cls: 'moderate',
        reason: 'Cluster saturated but team has strong priority (well under quota).' };
    return { label: '~1 \u2013 3h', cls: 'moderate',
      reason: 'Deep queue, but team priority should pull you ahead.' };
  }

  if (hasPriority && t.pct < 90) {
    if (occupancyPct < 90)
      return { label: '~30 min \u2013 1h', cls: 'moderate',
        reason: 'Some capacity available, team near quota \u2014 normal priority.' };
    if (queueRatio < 0.5)
      return { label: '~1 \u2013 4h', cls: 'slow',
        reason: 'Cluster saturated, team near quota \u2014 moderate priority.' };
    return { label: '~2 \u2013 6h', cls: 'slow',
      reason: 'Saturated cluster with deep queue and diminishing fair-share.' };
  }

  if (hasPriority) {
    if (queueRatio < 0.3)
      return { label: '~2 \u2013 6h', cls: 'slow',
        reason: 'Team over quota \u2014 scheduler deprioritizes. Wait for capacity to free up.' };
    if (queueRatio < 0.7)
      return { label: '~4 \u2013 12h', cls: 'long',
        reason: 'Over quota with deep queue. Consider a less loaded cluster.' };
    return { label: '12h+', cls: 'long',
      reason: 'Severely oversubscribed and over quota. Try a different cluster or off-peak hours.' };
  }

  if (occupancyPct < 80)
    return { label: '< 1h', cls: 'moderate',
      reason: 'Cluster has available capacity.' };
  if (queueRatio < 0.5)
    return { label: '~1 \u2013 4h', cls: 'slow',
      reason: 'Cluster busy with moderate queue pressure.' };
  return { label: '4h+', cls: 'long',
    reason: 'High demand. Consider a less loaded cluster.' };
}

/* ── Tooltip for pending jobs ── */
const _tooltip = (() => {
  const el = document.createElement('div');
  el.className = 'cluster-tooltip';
  document.body.appendChild(el);

  let _hideTimer = null;

  function _pctColor(pct) { return pct >= 90 ? 'red' : pct >= 60 ? 'amber' : 'green'; }
  function _barLevel(pct) { return pct >= 90 ? 'high' : pct >= 60 ? 'medium' : 'low'; }

  function show(anchorEl, clusterName, jobInfo) {
    const pStats = _clusterStatsFromPartitions(clusterName);
    const u = getClusterUtil(clusterName);
    if (!pStats && !u) return;
    clearTimeout(_hideTimer);

    const t = u ? _teamStats(u) : { alloc: 0, runGpus: 0, pendGpus: 0, pct: 0, gpuPer: 8 };
    const gpuPer = pStats ? pStats.gpus_per_node : (t.gpuPer || 8);

    const totalNodes = pStats ? pStats.total_nodes : (u?.total_nodes || 0);
    const allocNodes = pStats ? pStats.alloc_nodes : (u?.running_nodes || 0);
    const idleNodes = pStats ? pStats.idle_nodes : Math.max(0, totalNodes - allocNodes);
    const pendingJobs = pStats ? pStats.pending_jobs : (u?.pending_nodes || 0);
    const occupancyPct = totalNodes > 0 ? Math.round(allocNodes / totalNodes * 100) : 0;
    const freeNodes = idleNodes;
    const freeGpus = freeNodes * gpuPer;
    const pendingGpus = pendingJobs * gpuPer;
    const dataSource = pStats ? 'slurm' : 'dashboard';

    const resolved = _resolveGpuAlloc(clusterName);
    if (resolved.gpus > 0 && t.alloc === 0) t.alloc = resolved.gpus;

    const ji = jobInfo || {};
    const jobReason = ji.reason || '';
    const jobGpuStr = parseGpus(ji.nodes, ji.gres);
    const reasonInfo = _translateReason(jobReason, ji.nodes, ji.gres, freeNodes, freeGpus);

    const est = _estimateWait(occupancyPct, pendingJobs, totalNodes, t, jobReason, clusterName);

    // ── Header: cluster name + wait estimate badge ──
    let html = `<div class="tt-head">
      <span class="tt-head-name">${clusterName}</span>
      <span class="tt-wait-badge ${est.cls}">${est.label}</span>
    </div>`;

    // ── Why this job is pending (the key section) ──
    if (reasonInfo) {
      html += `<div class="tt-section-lbl">Why this job is pending</div>`;
      html += `<div class="tt-reason-box ${reasonInfo.blocker}">`;
      html += `<div class="tt-reason-short">${reasonInfo.short}</div>`;
      html += `<div class="tt-reason-detail">${reasonInfo.detail}</div>`;
      html += `</div>`;
    }

    // Job resource requirements
    const resParts = [];
    if (jobGpuStr) resParts.push(jobGpuStr);
    else if (ji.nodes) resParts.push(`${ji.nodes} node${ji.nodes !== '1' ? 's' : ''}`);
    if (ji.partition) resParts.push(`${ji.partition}`);
    if (ji.timelimit && ji.timelimit !== '\u2014' && ji.timelimit !== 'N/A')
      resParts.push(`${ji.timelimit} limit`);
    if (resParts.length) {
      html += `<div class="tt-job-resources">Needs: ${resParts.join(' \u00b7 ')}</div>`;
    }

    if (ji.estStart) {
      const d = new Date(ji.estStart.replace('T', ' '));
      if (!isNaN(d)) {
        html += `<div class="tt-job-resources">Slurm estimate: ${d.toLocaleString()}</div>`;
      }
    }

    // ── Cluster load (from AI Hub) ──
    const aihubCluster = _pppAllocData?.clusters?.[clusterName];
    if (aihubCluster) {
      html += `<div class="tt-sep"></div>`;
      const clOcc = aihubCluster.cluster_occupied_gpus || 0;
      const clTot = aihubCluster.cluster_total_gpus || 0;
      const clPct = clTot > 0 ? Math.round(clOcc / clTot * 100) : 0;
      html += `<div class="tt-section-lbl">Cluster load</div>`;
      html += `<div class="tt-gauge-row">
        <div class="tt-gauge-track"><div class="tt-gauge-fill ${_barLevel(clPct)}" style="width:${Math.min(clPct, 100)}%"></div></div>
        <span class="tt-gauge-pct ${_pctColor(clPct)}">${clPct}%</span>
      </div>`;
      html += `<div class="tt-detail">${clOcc.toLocaleString()} / ${clTot.toLocaleString()} GPUs occupied</div>`;

      const bestP = aihubCluster.best_priority;
      const bestC = aihubCluster.best_capacity;
      if (bestP) {
        const fsCls = bestP.level_fs >= 1.2 ? 'green' : bestP.level_fs >= 0.8 ? 'amber' : 'red';
        html += `<div class="tt-detail">Best account: <b>${_shortAcct(bestP.account)}</b> (FS <span class="${fsCls}">${bestP.level_fs.toFixed(1)}</span>)`;
        if (bestC && bestC.account !== bestP.account)
          html += ` · Most headroom: <b>${_shortAcct(bestC.account)}</b> (${bestC.headroom} free)`;
        html += `</div>`;
      }
    } else {
      html += `<div class="tt-sep"></div>`;
      html += `<div class="tt-section-lbl">Cluster load</div>`;
      const occLevel = _barLevel(occupancyPct);
      html += `<div class="tt-gauge-row">
        <div class="tt-gauge-track"><div class="tt-gauge-fill ${occLevel}" style="width:${Math.min(occupancyPct, 100)}%"></div></div>
        <span class="tt-gauge-pct ${_pctColor(occupancyPct)}">${occupancyPct}%</span>
      </div>`;
      html += `<div class="tt-detail">${allocNodes} of ${totalNodes} nodes in use</div>`;
    }

    // ── Actionable insight ──
    html += `<div class="tt-sep"></div>`;
    html += `<div class="tt-insight">${est.reason}</div>`;

    // ── Where to submit instead ──
    if (_pppAllocData?.clusters) {
      const curGpuType = (CLUSTERS[clusterName]?.gpu_type || '').toLowerCase();
      const jobNodes = parseInt(ji.nodes) || 1;
      const gm = (ji.gres || '').match(/gpu[^:]*:(?:[^:]+:)?(\d+)/);
      const jobGpusPerNode = gm ? parseInt(gm[1]) : (gpuPer || 8);
      const jobTotalGpus = jobNodes * jobGpusPerNode;

      const alts = Object.entries(_pppAllocData.clusters)
        .filter(([c]) => c !== clusterName)
        .map(([c, cd]) => {
          const s = _clusterSubmitScore(cd, c);
          const gpu = (cd.gpu_type || '').toLowerCase();
          const sameGpu = gpu === curGpuType;
          return { cluster: c, gpu: cd.gpu_type || '', sameGpu, ...s };
        })
        .filter(a => a.freeForTeam >= jobTotalGpus)
        .sort((a, b) => {
          if (a.sameGpu !== b.sameGpu) return a.sameGpu ? -1 : 1;
          return b.score - a.score;
        })
        .slice(0, 2);

      if (alts.length) {
        html += `<div class="tt-sep"></div>`;
        html += `<div class="tt-section-lbl">Would start faster on</div>`;
        for (const a of alts) {
          const acctShort = a.bestAcct ? _shortAcct(a.bestAcct) : '';
          const fsCls = a.levelFs >= 1.2 ? 'green' : a.levelFs >= 0.8 ? 'amber' : 'red';
          html += `<div class="tt-insight green"><b>${a.cluster}</b>`;
          html += ` <span class="dim">${a.gpu}${a.sameGpu ? '' : ' ⚠'}</span>`;
          html += ` · ${a.freeForTeam} free`;
          if (acctShort) html += ` · via <b>${acctShort}</b>`;
          html += ` · FS <span class="${fsCls}">${a.levelFs.toFixed(1)}</span>`;
          html += `</div>`;
        }
      }
    }

    el.innerHTML = html;

    const rect = anchorEl.getBoundingClientRect();
    const ttW = el.offsetWidth || 280;
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

function attachPendingTooltip(chipEl, clusterName, jobInfo) {
  if (!chipEl) return;
  chipEl.addEventListener('mouseenter', () => _tooltip.show(chipEl, clusterName, jobInfo));
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

