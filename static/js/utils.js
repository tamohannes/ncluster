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
    return JSON.parse(sessionStorage.getItem('ncluster.progress') || '{}');
  } catch (_) { return {}; }
})();
const _progressSourceCache = (() => {
  try {
    return JSON.parse(sessionStorage.getItem('ncluster.progressSrc') || '{}');
  } catch (_) { return {}; }
})();

function _saveProgressCache() {
  try { sessionStorage.setItem('ncluster.progress', JSON.stringify(_progressCache)); } catch (_) {}
  try { sessionStorage.setItem('ncluster.progressSrc', JSON.stringify(_progressSourceCache)); } catch (_) {}
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
  const hasUtil = st === 'PENDING' && _clusterUtil;
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

const _TEAM_CACHE_KEY = 'ncluster.teamUsageCache';

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
  const el = document.getElementById('team-gpu-body');
  if (!el) return;
  const hasCached = Object.keys(_teamUsageData).length > 0 || _loadTeamCache();
  if (hasCached) {
    _renderTeamGpuStatus(_teamUsageData, _teamGpuAlloc);
  } else {
    el.innerHTML = '<div class="no-jobs">Loading team GPU data...</div>';
  }
}

async function refreshTeamGpuStatus(silent) {
  const el = document.getElementById('team-gpu-body');
  if (!el) return;
  const t = silent ? null : toastLoading('Refreshing cluster data…');

  const clusters = Object.keys(CLUSTERS).filter(c => c !== 'local');
  const fetches = [
    fetch('/api/team_usage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clusters }),
    }).then(r => r.json()),
  ];
  if (!silent) {
    fetches.push(fetchPartitions().then(() => _renderAvailTable()));
  }
  try {
    const [teamRes] = await Promise.all(fetches);
    const tu = teamRes.team_usage || {};
    const allocs = teamRes.team_gpu_allocations || {};
    for (const [c, d] of Object.entries(tu)) _teamUsageData[c] = d;
    if (Object.keys(allocs).length) _teamGpuAlloc = allocs;
    _renderTeamGpuStatus(tu, allocs);
    _saveTeamCache();
    if (t) t.done(`Cluster data refreshed (${Object.keys(tu).length} clusters)`);
  } catch (e) {
    if (!Object.keys(_teamUsageData).length) {
      el.innerHTML = '<div class="no-jobs" style="color:var(--red)">Failed to load team GPU data</div>';
    }
    if (t) t.done('Failed to fetch cluster data', 'error');
  }
}

function _renderTeamGpuStatus(teamUsage, allocs) {
  const el = document.getElementById('team-gpu-body');
  if (!el) return;

  const clusters = Object.keys(CLUSTERS).filter(c => c !== 'local').sort();
  const hasData = clusters.some(c => teamUsage[c] || allocs[c]);

  if (!hasData) {
    el.innerHTML = '<div class="no-jobs">No team data yet. Set weekly GPU allocations in Settings > Profile, and wait for team usage to be fetched.</div>';
    return;
  }

  let html = '<div class="team-gpu-grid">';
  for (const c of clusters) {
    const tu = teamUsage[c] || {};
    const rawAlloc = allocs[c];
    let weekly = 0;
    let isAny = false;
    if (rawAlloc === 'any' || rawAlloc === -1) {
      isAny = true;
      const ps = _partitionData && _partitionData[c];
      if (ps && ps.partitions) {
        for (const p of ps.partitions) {
          const g = (p.total_nodes || 0) * (p.gpus_per_node || 0);
          if (g > weekly) weekly = g;
        }
      }
    } else {
      weekly = rawAlloc || 0;
    }
    const running = tu.total_running_gpus || 0;
    const pending = tu.total_pending_gpus || 0;
    const users = tu.users || {};
    const gpuType = CLUSTERS[c]?.gpu_type || '';

    if (!weekly && !running && !pending && !Object.keys(users).length) continue;

    const myDataCard = users[USERNAME] || {};
    const myRunCard = myDataCard.running_gpus || 0;
    const myUsagePct = weekly > 0 ? Math.min(100, Math.round(myRunCard / weekly * 100)) : 0;
    const free = weekly > 0 ? Math.max(0, weekly - myRunCard) : null;
    const level = myUsagePct >= 90 ? 'high' : myUsagePct >= 60 ? 'medium' : 'low';
    const statusLabel = myUsagePct >= 100 ? 'over quota' : myUsagePct >= 70 ? 'near quota' : 'under quota';
    const statusCls = myUsagePct >= 100 ? 'red' : myUsagePct >= 70 ? 'amber' : 'green';

    html += `<div class="team-gpu-card">
      <div class="team-gpu-card-head">
        <span class="team-gpu-cluster">${c}</span>
        ${gpuType ? `<span class="avail-gpu-badge">${gpuType}</span>` : ''}
        ${weekly ? `<span class="team-gpu-status ${statusCls}">${statusLabel}</span>` : ''}
      </div>`;

    const myData = users[USERNAME] || {};
    const myRun = myData.running_gpus || 0;
    const myPend = myData.pending_gpus || 0;
    const teamRest = Math.max(0, running - myRun);
    const teamPend = Math.max(0, pending - myPend);
    const cap = weekly || running || 1;
    const myPct = Math.min(100, Math.round(myRun / cap * 100));
    const teamPct = Math.min(100 - myPct, Math.round(teamRest / cap * 100));

    html += `<div class="team-stacked-bar" style="margin:8px 0 6px">
      <div class="team-stacked-track">
        ${myPct > 0 ? `<div class="team-stacked-seg you" style="width:${myPct}%" title="You: ${myRun} GPUs"></div>` : ''}
        ${teamPct > 0 ? `<div class="team-stacked-seg team" style="width:${teamPct}%" title="Others: ${teamRest} GPUs"></div>` : ''}
      </div>
    </div>
    <div class="team-gpu-labels">
      <div class="team-gpu-label-line"><span class="team-gpu-label-you">you: ${myRun}</span>${myPend > 0 ? ` <span class="team-pend-badge ${weekly > 0 && (myRun + myPend) > weekly ? 'over' : 'ok'}">+${myPend} pending</span>` : ''}</div>
      <div class="team-gpu-label-line"><span class="team-gpu-label-team">others: ${teamRest}${teamPend > 0 ? ` (+${teamPend} pending)` : ''}</span></div>
      ${free !== null && free > 0 ? `<div class="team-gpu-label-line"><span class="team-gpu-label-free">free: ${free}</span></div>` : ''}
    </div>
    <div class="team-gpu-summary">your usage: ${myRunCard} / ${weekly} GPUs${isAny ? ' (any)' : ''}</div>`;

    const sorted = Object.entries(users)
      .map(([u, d]) => ({ user: u, run: d.running_gpus || 0, pend: d.pending_gpus || 0 }))
      .filter(x => x.run > 0 || x.pend > 0)
      .sort((a, b) => b.run - a.run || b.pend - a.pend);

    if (sorted.length) {
      html += '<div class="team-gpu-users">';
      for (const m of sorted.slice(0, 8)) {
        const barPct = weekly > 0 ? Math.min(100, Math.round(m.run / weekly * 100)) : 0;
        const barLevel = barPct >= 50 ? 'high' : barPct >= 20 ? 'medium' : 'low';
        const isMe = m.user === USERNAME;
        const parts = [];
        if (m.run > 0) parts.push(`${m.run} running`);
        if (m.pend > 0) parts.push(`${m.pend} pending`);
        html += `<div class="team-gpu-user-row">
          <span class="team-gpu-user-name${isMe ? ' me' : ''}">${m.user}</span>
          <span class="tt-team-bar-wrap"><span class="tt-team-bar ${barLevel}" style="width:${barPct}%"></span></span>
          <span class="team-gpu-user-val">${parts.join(', ')}</span>
        </div>`;
      }
      if (sorted.length > 8) {
        html += `<div class="team-gpu-user-val" style="text-align:center">+${sorted.length - 8} more</div>`;
      }
      html += '</div>';
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

  const body = { nodes, time_limit: timeLimit, can_preempt: canPreempt };
  if (cluster) body.clusters = [cluster];

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

function _renderAdvisorResults(recs) {
  const el = document.getElementById('adv-results');
  if (!el) return;
  if (!recs.length) {
    el.innerHTML = '<div class="no-jobs">No eligible partitions found for these requirements</div>';
    return;
  }

  const rows = recs.slice(0, 20).map((r, i) => {
    const d = r.details || {};
    const isBest = i === 0;
    const preemptBadge = d.preemptable ? '<span class="adv-preempt-tag">preemptable</span>' : '';
    const defaultBadge = d.is_default ? '<span class="adv-default-tag">default</span>' : '';
    const occLevel = d.occupancy_pct >= 90 ? 'high' : d.occupancy_pct >= 60 ? 'medium' : 'low';
    return `<tr class="${isBest ? 'adv-best' : ''}">
      <td class="adv-rank">${isBest ? '★' : r.rank}</td>
      <td><span class="adv-cluster">${r.cluster}</span></td>
      <td><span class="adv-part">${r.partition}</span> ${defaultBadge}${preemptBadge}</td>
      <td><span class="tt-wait-badge ${r.est_wait_cls}">${r.est_wait}</span></td>
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

  el.innerHTML = `<table class="adv-table">
    <thead><tr>
      <th></th><th>Cluster</th><th>Partition</th><th>Est. Wait</th>
      <th>Idle</th><th>Queue</th><th>Occupancy</th><th>Tier</th><th>Limit</th><th>Notes</th>
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
    if (tu && tu.total_running_gpus > 0 && hasPriority && t.alloc > 0 && tu.total_running_gpus >= t.alloc) {
      const topUser = Object.entries(tu.users || {})
        .map(([u, d]) => ({ user: u, gpus: d.running_gpus || 0 }))
        .sort((a, b) => b.gpus - a.gpus)[0];
      const who = topUser && topUser.gpus > t.alloc * 0.5
        ? `${topUser.user} is using ${topUser.gpus} GPUs. `
        : '';
      return { label: 'team at quota', cls: 'slow',
        reason: `${who}Team is at/over quota (${tu.total_running_gpus}/${t.alloc} GPUs). Jobs wait until teammates\u2019 runs finish.` };
    }
    if (hasPriority && t.pct < 50 && occupancyPct < 70)
      return { label: '~10 \u2013 30 min', cls: 'fast',
        reason: 'Free capacity exists, but other jobs have higher fair-share priority. Your high team priority should help.' };
    if (hasPriority && t.pct < 50)
      return { label: '~30 min \u2013 2h', cls: 'moderate',
        reason: 'Scheduler is processing higher-priority jobs first. Your team is under quota, so priority is strong.' };
    if (hasPriority && t.pct < 90)
      return { label: '~1 \u2013 4h', cls: 'slow',
        reason: 'Fair-share priority is moderate (team near quota). Other teams\u2019 jobs may schedule first.' };
    if (hasPriority)
      return { label: '~2 \u2013 8h', cls: 'slow',
        reason: 'Team is over quota \u2014 low fair-share priority. Jobs from under-quota teams go first.' };
    return { label: '~1 \u2013 4h', cls: 'slow',
      reason: 'Scheduler has higher-priority jobs ahead in the queue.' };
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

    // ── Cluster load gauge ──
    html += `<div class="tt-sep"></div>`;
    html += `<div class="tt-section-lbl">Cluster load <span class="tt-source">${dataSource}</span></div>`;
    const occLevel = _barLevel(occupancyPct);
    html += `<div class="tt-gauge-row">
      <div class="tt-gauge-track"><div class="tt-gauge-fill ${occLevel}" style="width:${Math.min(occupancyPct, 100)}%"></div></div>
      <span class="tt-gauge-pct ${_pctColor(occupancyPct)}">${occupancyPct}%</span>
    </div>`;
    html += `<div class="tt-detail">${allocNodes} of ${totalNodes} nodes in use`;
    if (freeNodes > 0) html += ` \u00b7 <span class="green">${freeNodes} free</span>`;
    else html += ` \u00b7 <span class="red">none free</span>`;
    html += `</div>`;

    if (pendingJobs > 0) {
      const oversubRatio = freeNodes > 0
        ? (pendingJobs / freeNodes).toFixed(1)
        : null;
      html += `<div class="tt-detail">${pendingGpus} GPUs queued cluster-wide`;
      if (oversubRatio !== null) html += ` \u00b7 ${oversubRatio}\u00d7 oversubscribed`;
      else html += ` \u00b7 no free capacity`;
      html += `</div>`;
    } else {
      html += `<div class="tt-detail green">No queue \u2014 jobs start immediately</div>`;
    }

    // ── Team priority ──
    if (t.alloc > 0) {
      html += `<div class="tt-sep"></div>`;
      html += `<div class="tt-section-lbl">Your team\u2019s priority</div>`;
      html += `<div class="tt-gauge-row">
        <div class="tt-gauge-track"><div class="tt-gauge-fill ${_barLevel(t.pct)}" style="width:${Math.min(t.pct, 100)}%"></div></div>
        <span class="tt-gauge-pct ${_pctColor(t.pct)}">${t.pct}%</span>
      </div>`;

      let priorityHtml;
      if (t.pct >= 100)
        priorityHtml = `<span class="tt-priority-tag low">Low priority</span> over quota`;
      else if (t.pct >= 70)
        priorityHtml = `<span class="tt-priority-tag med">Normal</span> near quota`;
      else
        priorityHtml = `<span class="tt-priority-tag high">High priority</span> under quota`;
      html += `<div class="tt-row"><span class="tt-label">${t.runGpus} / ${t.alloc} GPUs</span><span class="tt-val">${priorityHtml}</span></div>`;

      if (t.pendGpus > 0) {
        html += `<div class="tt-detail">${t.pendGpus} GPUs pending from your team</div>`;
      }
    }

    // ── Team member usage (from Slurm account query) ──
    const tu = _teamUsageData[clusterName];
    if (tu && tu.users && Object.keys(tu.users).length > 0) {
      html += `<div class="tt-sep"></div>`;
      const resolved = _resolveGpuAlloc(clusterName);
      const weeklyAlloc = resolved.gpus;
      const isAnyAlloc = resolved.isAny;
      const totalRun = tu.total_running_gpus || 0;
      const quota = weeklyAlloc || t.alloc || totalRun || 1;
      const anyTag = isAnyAlloc ? ' (any)' : '';
      const allocLabel = weeklyAlloc ? `${totalRun} / ${weeklyAlloc} GPUs${anyTag}` : '';
      if (weeklyAlloc) {
        const allocPct = Math.min(100, Math.round(totalRun / weeklyAlloc * 100));
        html += `<div class="tt-section-lbl">Team usage · ${allocLabel}</div>`;
        html += `<div class="tt-gauge-row">
          <div class="tt-gauge-track"><div class="tt-gauge-fill ${_barLevel(allocPct)}" style="width:${Math.min(allocPct, 100)}%"></div></div>
          <span class="tt-gauge-pct ${_pctColor(allocPct)}">${allocPct}%</span>
        </div>`;
      } else {
        html += `<div class="tt-section-lbl">Team usage by member</div>`;
      }
      const sorted = Object.entries(tu.users)
        .map(([u, d]) => ({ user: u, run: d.running_gpus || 0, pend: d.pending_gpus || 0 }))
        .filter(x => x.run > 0 || x.pend > 0)
        .sort((a, b) => b.run - a.run || b.pend - a.pend);
      if (sorted.length) {
        for (const m of sorted) {
          const barPct = Math.min(100, Math.round(m.run / quota * 100));
          const isMe = m.user === (typeof USERNAME !== 'undefined' ? USERNAME : '');
          const nameCls = isMe ? 'tt-team-me' : '';
          const parts = [];
          if (m.run > 0) parts.push(`${m.run} running`);
          if (m.pend > 0) parts.push(`${m.pend} pending`);
          html += `<div class="tt-team-row">
            <span class="tt-team-name ${nameCls}">${m.user}</span>
            <span class="tt-team-bar-wrap"><span class="tt-team-bar ${_barLevel(barPct)}" style="width:${barPct}%"></span></span>
            <span class="tt-team-gpus">${parts.join(', ')}</span>
          </div>`;
        }
        if (sorted.length > 0 && sorted[0].run > quota * 0.7 && sorted[0].user !== (typeof USERNAME !== 'undefined' ? USERNAME : '')) {
          html += `<div class="tt-insight amber"><b>${sorted[0].user}</b> is using ${sorted[0].run}/${quota} team GPUs</div>`;
        }
      }
    }

    // ── Partition info ──
    if (ji.partition && _partitionData) {
      const cps = _partitionData[clusterName];
      if (cps && cps.partitions) {
        const curPart = cps.partitions.find(p => p.name === ji.partition);
        if (curPart) {
          html += `<div class="tt-sep"></div>`;
          html += `<div class="tt-section-lbl">Partition: ${ji.partition}</div>`;
          html += `<div class="tt-detail">Priority tier ${curPart.priority_tier} · ${curPart.idle_nodes} idle nodes · ${curPart.pending_jobs} pending</div>`;
          const betterParts = cps.partitions.filter(p =>
            p.name !== ji.partition &&
            p.priority_tier > curPart.priority_tier &&
            !p.preemptable &&
            p.idle_nodes > 0
          );
          if (betterParts.length) {
            const best = betterParts[0];
            html += `<div class="tt-insight green">Try <b>${best.name}</b> (tier ${best.priority_tier}, ${best.idle_nodes} idle)</div>`;
          }
        }
      }
    }

    // ── Cross-cluster suggestion ──
    if (_partitionData) {
      const otherClusters = Object.entries(_partitionData)
        .filter(([c]) => c !== clusterName)
        .map(([c, ps]) => ({ cluster: c, idle: ps.idle_nodes || 0, pending: ps.pending_jobs || 0, total: ps.total_nodes || 0 }))
        .filter(c => c.idle > 5)
        .sort((a, b) => (b.idle / Math.max(b.total, 1)) - (a.idle / Math.max(a.total, 1)));
      if (otherClusters.length && pendingNodes > 0) {
        const best = otherClusters[0];
        const bestIdlePct = Math.round(best.idle / Math.max(best.total, 1) * 100);
        if (bestIdlePct > 5) {
          html += `<div class="tt-insight green">Consider <b>${best.cluster}</b>: ${best.idle} idle nodes (${bestIdlePct}% free)</div>`;
        }
      }
    }

    // ── Actionable insight ──
    html += `<div class="tt-sep"></div>`;
    html += `<div class="tt-insight">${est.reason}</div>`;

    // ── Storage quota (compact) ──
    const sq = _storageQuota[clusterName];
    if (sq && sq.project_quotas && Object.keys(sq.project_quotas).length) {
      html += `<div class="tt-sep"></div>`;
      const pills = [];
      for (const [pname, pq] of Object.entries(sq.project_quotas)) {
        const pparts = pname.split('_');
        const short = pparts.length > 2 ? pparts.slice(-1)[0] : pname;
        const sp = pq.space_used_pct || 0;
        const ip = pq.files_used_pct || 0;
        const worst = Math.max(sp, ip);
        const cls = _pctColor(worst);
        pills.push(`<span class="tt-storage-pill ${cls}" title="${short}: ${pq.space_used_human} / ${pq.space_quota_human}">${short} ${Math.round(worst)}%</span>`);
      }
      html += `<div class="tt-storage-row">${pills.join('')}</div>`;
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

