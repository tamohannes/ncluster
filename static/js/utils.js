const CLUSTERS = JSON.parse(document.getElementById('cluster-data').textContent || '{}');
const USERNAME = (document.getElementById('username-data')?.textContent || '').trim();
const TEAM = (document.getElementById('team-data')?.textContent || '').trim();

// ── In-flight connection registry ──────────────────────────────────────────
// Every wrapped fetch registers its AbortController here. When the server
// wedges (gunicorn worker watchdog SIGTERMs itself on 20+ stuck threads) we
// can call `dropAllInFlight()` to abort every pending request at once. This
// stops the client from piling more stuck requests onto a restarting worker
// and lets the UI reconnect cleanly once /api/health is reachable again.
const _inFlightControllers = new Set();

function _trackController(controller) {
  _inFlightControllers.add(controller);
  return () => _inFlightControllers.delete(controller);
}

function _makeTimeoutError() {
  if (typeof DOMException === 'function') {
    return new DOMException('signal timed out', 'TimeoutError');
  }
  const err = new Error('signal timed out');
  err.name = 'TimeoutError';
  return err;
}

function dropAllInFlight(reason = 'client-reconnect') {
  const n = _inFlightControllers.size;
  if (!n) return 0;
  const reasonErr = (typeof DOMException === 'function')
    ? new DOMException(`dropped: ${reason}`, 'AbortError')
    : reason;
  for (const c of Array.from(_inFlightControllers)) {
    try { c.abort(reasonErr); } catch (_) {}
  }
  _inFlightControllers.clear();
  try { console.info(`[clausius] dropped ${n} in-flight fetches (${reason})`); } catch (_) {}
  return n;
}

// Keep the unwrapped browser fetch on a window property so tests (and any
// debug tooling) can swap it out without touching the wrapper.
window._clausiusNativeFetch = window.fetch.bind(window);

function _wrapFetchTracked(input, init, timeoutMs) {
  const opts = init ? { ...init } : {};
  const userSignal = opts.signal || null;
  const controller = new AbortController();
  const release = _trackController(controller);

  if (userSignal) {
    if (userSignal.aborted) {
      try { controller.abort(userSignal.reason); } catch (_) {}
    } else {
      userSignal.addEventListener('abort', () => {
        try { controller.abort(userSignal.reason); } catch (_) {}
      }, { once: true });
    }
  }

  let tid = null;
  if (timeoutMs && timeoutMs > 0) {
    tid = setTimeout(() => {
      try { controller.abort(_makeTimeoutError()); } catch (_) {}
    }, timeoutMs);
  }

  opts.signal = controller.signal;
  return window._clausiusNativeFetch(input, opts).finally(() => {
    if (tid !== null) clearTimeout(tid);
    release();
  });
}

window.fetch = function(input, init) {
  const hasUserSignal = !!(init && init.signal);
  const ms = hasUserSignal ? 0 : 20000;
  return _wrapFetchTracked(input, init, ms);
};

function fetchWithTimeout(url, opts = {}, ms = 15000) {
  return _wrapFetchTracked(url, opts, ms);
}

// Expose drop helper for debugging and for the reconnect state machine.
window.dropAllInFlight = dropAllInFlight;
window._inFlightCount = () => _inFlightControllers.size;

/** Escape a string for use in an HTML double-quoted attribute value */
function escAttr(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;');
}

/** Update board/history/project run name badges when mark state changes (no full refresh). */
function syncRunMarkedBorders(cluster, rootJobId, marked) {
  if (cluster == null || rootJobId == null) return;
  const c = String(cluster);
  const r = String(rootJobId);
  document.querySelectorAll('.run-name-badge[data-run-cluster][data-run-root]').forEach((el) => {
    if (el.getAttribute('data-run-cluster') === c && el.getAttribute('data-run-root') === r) {
      el.classList.toggle('run-name-badge--starred', !!marked);
    }
  });
}
let allData = {};
let historyData = [];
let countdown = 20;
let refreshIntervalSec = 20;
let cdTimer;
let currentTab = 'live';
let _isResizingTree = false;
let _isResizingNav = false;
let navCollapsed = false;

function freshnessBadgeHtml(clusterName) {
  const d = allData[clusterName];
  let staleness = d?.poller?.staleness_sec;
  if (staleness == null && d?.updated) {
    staleness = (Date.now() - new Date(d.updated).getTime()) / 1000;
  }
  if (staleness == null) return '';
  let label;
  if (staleness < 60) label = `${Math.round(staleness)}s`;
  else if (staleness < 3600) label = `${Math.round(staleness / 60)}m`;
  else label = `${Math.round(staleness / 3600)}h`;
  const cls = staleness <= 20 ? 'fresh' : staleness <= 60 ? 'warm' : 'old';
  return `<span class="freshness-badge ${cls}" title="Data age: ${label} ago">${label}</span>`;
}

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

const STATE_ORDER = { SUBMITTING: -1, RUNNING: 0, COMPLETING: 1, PENDING: 2, FAILED: 3, CANCELLED: 4 };
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

function _saveProgressCache() {
  return;
}

function resolveProgress(cluster, jobid, apiProgress, state, apiSource) {
  return { pct: apiProgress ?? null, source: apiSource || '' };
}

function isSoftFail(state, reason) {
  return (state || '').toUpperCase() === 'COMPLETED' && (reason || '').startsWith('soft-fail:');
}

function isUnneededBackup(job, groupJobs) {
  const st = (job.state || '').toUpperCase();
  if (!st.includes('FAIL')) return false;
  const deps = job.dep_details || [];
  const hasBackupDep = deps.some(d => d.type === 'afterany' || d.type === 'afternotok');
  if (!hasBackupDep) return false;
  
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
  if (s === 'SUBMITTING') return 's-SUBMITTING';
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
  s = (s || '').toUpperCase();
  return s.startsWith('CANCEL') || s.startsWith('COMPLETING');
}

function isCompletedState(s) {
  return (s || '').toUpperCase().startsWith('COMPLETED');
}

function _isDependentJob(j) {
  const st = (j.state || '').toUpperCase();
  if (st === 'DEPENDENT' || st === 'BACKUP') return true;
  if (st !== 'PENDING' && st !== 'SUBMITTING') return false;
  const r = (j.reason || '').toLowerCase();
  if (r.includes('depend')) return true;
  if ((j.depends_on || []).length > 0) return true;
  return false;
}

/** Backup dependent: only runs if parent fails (afternotok / same-name afterany). */
function _isBackupDep(j, byId) {
  const st = (j.state || '').toUpperCase();
  if (st === 'BACKUP') return true;
  if (!_isDependentJob(j)) return false;
  const deps = j.dep_details || [];
  if (!deps.length) return false;
  return deps.every(d => {
    if (d.type === 'afternotok') return true;
    if (d.type === 'afterany' && byId) {
      const parent = byId[d.job_id];
      if (parent && parent.name === j.name) return true;
    }
    return false;
  });
}

function _countJobStates(jobs) {
  const cnt = { run: 0, comp: 0, pend: 0, dep: 0, bkp: 0, fail: 0, canc: 0, done: 0 };
  const byId = {};
  for (const j of jobs) { if (j.jobid) byId[j.jobid] = j; }
  for (const j of jobs) {
    const st = (j.state || '').toUpperCase();
    if (st === 'RUNNING') cnt.run++;
    else if (st === 'COMPLETING') cnt.comp++;
    else if (st === 'PENDING' || st === 'SUBMITTING' || st === 'DEPENDENT') {
      if (_isBackupDep(j, byId)) cnt.bkp++;
      else if (_isDependentJob(j)) cnt.dep++;
      else cnt.pend++;
    }
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
    [cnt.dep,  'var(--yellow-muted, rgba(255,193,7,0.35))'],
    [cnt.bkp,  'var(--border)'],
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
  if (cnt.dep)  tip.push(`${cnt.dep} dependent`);
  if (cnt.bkp)  tip.push(`${cnt.bkp} backup`);
  if (cnt.fail) tip.push(`${cnt.fail} failed`);
  if (cnt.canc) tip.push(`${cnt.canc} cancelled`);
  if (cnt.done) tip.push(`${cnt.done} completed`);
  return `<svg class="status-donut" width="${sz}" height="${sz}" viewBox="0 0 ${sz} ${sz}"><title>${tip.join(', ')}</title><circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="var(--border)" stroke-width="${sw}"/>${arcs}</svg>`;
}

function statusSummaryHtml(jobs, clusterName) {
  const cnt = _countJobStates(jobs);
  const byId = {};
  for (const j of jobs) { if (j.jobid) byId[j.jobid] = j; }
  let runGpus = 0, pendGpus = 0, depGpus = 0, bkpGpus = 0;
  for (const j of jobs) {
    const g = jobGpuCount(j.nodes, j.gres);
    const st = (j.state || '').toUpperCase();
    if (st === 'RUNNING' || st === 'COMPLETING') runGpus += g;
    else if (st === 'PENDING' || st === 'SUBMITTING' || st === 'DEPENDENT') {
      if (_isBackupDep(j, byId)) bkpGpus += g;
      else if (_isDependentJob(j)) depGpus += g;
      else pendGpus += g;
    }
  }
  const parts = [];
  if (cnt.run) {
    const gpuLabel = runGpus > 0 ? ` (<span class="ss-gpu-count">${runGpus}</span>)` : '';
    parts.push(`<span class="ss-run">${cnt.run} running${gpuLabel}</span>`);
  }
  if (cnt.comp) parts.push(`<span class="ss-comp">${cnt.comp} completing</span>`);
  if (cnt.pend) {
    let waitHint = '';
    if (clusterName) {
      const badge = _wdsWaitBadge(clusterName);
      if (badge) waitHint = ` <span class="ss-wait ${badge.cls}">${badge.label}</span>`;
    }
    const gpuLabel = pendGpus > 0 ? ` (<span class="ss-gpu-count">${pendGpus}</span>)` : '';
    parts.push(`<span class="ss-pend">${cnt.pend} pending${gpuLabel}${waitHint}</span>`);
  }
  if (cnt.dep) {
    const gpuLabel = depGpus > 0 ? ` (<span class="ss-gpu-count">${depGpus}</span>)` : '';
    parts.push(`<span class="ss-dep">${cnt.dep} dep${gpuLabel}</span>`);
  }
  if (cnt.bkp) {
    const gpuLabel = bkpGpus > 0 ? ` (<span class="ss-gpu-count">${bkpGpus}</span>)` : '';
    parts.push(`<span class="ss-bkp">${cnt.bkp} backup${gpuLabel}</span>`);
  }
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

function runAttemptBadge(job) {
  const when = fmtTime(job?.submitted || job?.started || job?.ended_at || '');
  const jid = String(job?.jobid || job?.job_id || '').trim();
  if (when === '—' && !jid) return '';
  const label = when !== '—' ? when : `#${jid}`;
  const title = jid ? `Run root ${jid}` : 'Run attempt';
  return `<span class="group-project-badge" title="${title}">${label}</span>`;
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

function clusterGpuBadge(clusterName) {
  const info = CLUSTERS[clusterName] || {};
  const gpuType = info.gpu_type || '';
  if (!gpuType) return '';
  const mem = info.gpu_mem_gb;
  const namePart = mem ? `${gpuType}(${mem}GB)` : gpuType;
  const gpn = info.gpus_per_node;
  return gpn ? `${namePart}×${gpn}` : namePart;
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

/** Numeric GPU count (nodes × per-node gres) for aggregations; 0 if none. */
function jobGpuCount(nodes, gres) {
  if (!gres || gres === 'cpu' || gres === '(null)' || gres === 'N/A' || gres === 'local') return 0;
  const m = String(gres).match(/gpu[^:]*:(?:[^:]+:)?(\d+)/);
  if (!m) return 0;
  const perNode = parseInt(m[1], 10) || 0;
  if (perNode === 0) return 0;
  const n = parseInt(nodes, 10) || 1;
  return perNode * n;
}

const _STAGE_SUFFIX_RE = new RegExp(
  '(?:-|_)(?:' +
  '(?:probes?|sep)[-_](?:server|l\\d+)' +
  '|(?:paths?|server)[-_](?:probes?|paths?)' +
  '|path[-_](?:analytical|computational|knowledge)(?:-c\\d+)?' +
  '|paths?[-_]server' +
  '|merge[-_](?:analytical|computational|knowledge)' +
  '|(?:eval[-_])?judge[-_](?:server|client|eval)' +
  '|gate(?:[-_](?:classify|prep))?' +
  '|chunk\\d+' +
  '|server' +
  '|summarize(?:[-_]results?)?' +
  '|judge(?:[-_]rs\\d+)?' +
  '|rs\\d+(?:[-_]c\\d+)?' +
  ')$', 'i'
);

function groupKeyForJob(name) {
  let n = (name || '').trim();
  if (!n) return 'misc';
  const evalMatch = n.match(/^(eval-[a-z0-9_]+)/i);
  if (evalMatch) return evalMatch[1].toLowerCase();
  let prev = null;
  while (prev !== n) {
    prev = n;
    n = n.replace(_STAGE_SUFFIX_RE, '');
  }
  return n.toLowerCase();
}

function historySearchValues(row) {
  const baseName = row.run_name || row.job_name || row.name || '';
  return [
    row.job_name || row.name || '',
    row.run_name || '',
    String(row.job_id || row.jobid || ''),
    groupKeyForJob(baseName),
    row.project || '',
    row.campaign || '',
    row.partition || '',
    row.account || '',
    row.cluster || row._cluster || '',
  ].map(v => String(v).toLowerCase());
}

function historySearchMatchesRow(row, query) {
  const q = String(query || '').trim().toLowerCase();
  if (!q) return true;
  return historySearchValues(row).some(value => value.includes(q));
}

function normalizeHistoryJobRow(row) {
  return {
    jobid: row.job_id,
    name: row.job_name || '',
    state: row.state || '',
    elapsed: row.elapsed || '',
    nodes: row.nodes || '',
    gres: row.gres || '',
    partition: row.partition || '',
    account: row.account || '',
    campaign: row.campaign || '',
    submitted: row.submitted || '',
    started: row.started || '',
    started_local: row.started_local || '',
    ended_local: row.ended_local || '',
    ended_at: row.ended_at || '',
    depends_on: row.depends_on || [],
    dependents: row.dependents || [],
    dep_details: row.dep_details || [],
    project: row.project || '',
    project_color: row.project_color || '',
    project_emoji: row.project_emoji || '',
    reason: row.reason || '',
    exit_code: row.exit_code || '',
    run_id: row.run_id || null,
    run_name: row.run_name || '',
    output_dir: row.output_dir || '',
    _cluster: row.cluster,
    _pinned: true,
  };
}

function buildHistoryQueryParams(options = {}) {
  const params = new URLSearchParams();
  const cluster = options.cluster;
  const project = options.project;
  const campaign = options.campaign;
  const partition = options.partition;
  const account = options.account;
  const state = options.state;
  const days = options.days;
  const q = options.q;
  const limit = options.limit;

  if (cluster) params.set('cluster', cluster);
  if (project) params.set('project', project);
  if (campaign) params.set('campaign', campaign);
  if (partition) params.set('partition', partition);
  if (account) params.set('account', account);
  if (days && days !== 'all') params.set('days', days);
  if (q) params.set('q', q);
  if (limit != null && limit !== '') params.set('limit', String(limit));

  const stateValues = Array.isArray(state) ? state : (state ? [state] : []);
  for (const value of stateValues) {
    if (value) params.append('state', value);
  }
  return params;
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

  // Union by run_id so all jobs from the same detected run stay grouped.
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

  // Union by output_dir so continuation runs (same experiment restarted) are
  // displayed as a single entity rather than separate groups.
  const outputDirGroups = {};
  for (const j of jobs) {
    if (j.output_dir) {
      if (!outputDirGroups[j.output_dir]) outputDirGroups[j.output_dir] = [];
      outputDirGroups[j.output_dir].push(j.jobid);
    }
  }
  for (const ids of Object.values(outputDirGroups)) {
    for (let i = 1; i < ids.length; i++) union(ids[0], ids[i]);
  }

  // Merge resubmissions: if jobs share the same group key and one set has
  // a run_id while the other doesn't, they're a skip_filled retry — merge.
  const nameGroups = {};
  for (const j of jobs) {
    const gk = groupKeyForJob(j.name);
    if (!nameGroups[gk]) nameGroups[gk] = [];
    nameGroups[gk].push(j);
  }
  for (const sameNameJobs of Object.values(nameGroups)) {
    const withRun = sameNameJobs.filter(j => j.run_id);
    const withoutRun = sameNameJobs.filter(j => !j.run_id);
    if (withRun.length && withoutRun.length) {
      for (const j of withoutRun) union(j.jobid, withRun[0].jobid);
    }
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
  const lum = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
  if (_isDarkTheme()) {
    const mix = lightness || (0.12 + lum * 0.08);
    const br = 0x1c, bg = 0x1c, bb = 0x28;
    const lr = Math.round(br + (r - br) * mix);
    const lg = Math.round(bg + (g - bg) * mix);
    const lb = Math.round(bb + (b - bb) * mix);
    return `rgb(${lr},${lg},${lb})`;
  }
  const t = lightness || (lum > 0.75 ? 0.82 : 0.92);
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
  return ` style="background-color:${bg};border-color:${bg};color:${text}"`;
}

/* ── Campaign shade (sub-project color differentiation) ── */

function _hexToHSL(hex) {
  const r = parseInt(hex.slice(1, 3), 16) / 255;
  const g = parseInt(hex.slice(3, 5), 16) / 255;
  const b = parseInt(hex.slice(5, 7), 16) / 255;
  const max = Math.max(r, g, b), min = Math.min(r, g, b);
  const l = (max + min) / 2;
  if (max === min) return [0, 0, l];
  const d = max - min;
  const s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
  let h;
  if (max === r) h = ((g - b) / d + (g < b ? 6 : 0)) / 6;
  else if (max === g) h = ((b - r) / d + 2) / 6;
  else h = ((r - g) / d + 4) / 6;
  return [h * 360, s, l];
}

function _hslToHex(h, s, l) {
  h = ((h % 360) + 360) % 360;
  s = Math.max(0, Math.min(1, s));
  l = Math.max(0, Math.min(1, l));
  const c = (1 - Math.abs(2 * l - 1)) * s;
  const x = c * (1 - Math.abs((h / 60) % 2 - 1));
  const m = l - c / 2;
  let r, g, b;
  if (h < 60)       { r = c; g = x; b = 0; }
  else if (h < 120) { r = x; g = c; b = 0; }
  else if (h < 180) { r = 0; g = c; b = x; }
  else if (h < 240) { r = 0; g = x; b = c; }
  else if (h < 300) { r = x; g = 0; b = c; }
  else              { r = c; g = 0; b = x; }
  const toHex = v => Math.round((v + m) * 255).toString(16).padStart(2, '0');
  return `#${toHex(r)}${toHex(g)}${toHex(b)}`;
}

function _campaignHash(str) {
  let h = 0;
  for (let i = 0; i < str.length; i++) {
    h = ((h << 5) - h + str.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

function campaignShade(hexColor, campaign) {
  if (!hexColor || !campaign || !hexColor.startsWith('#')) return hexColor;
  const [h, s, l] = _hexToHSL(hexColor);
  const idx = _campaignHash(campaign) % 7;
  const hueShift = (idx - 3) * 5;
  const lightShift = ((idx % 3) - 1) * 0.02;
  return _hslToHex(h + hueShift, s, l + lightShift);
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

function highlightJobName(name, prefix, suffix, campaign, shadedColor) {
  if (campaign) {
    const first_ = name.indexOf('_');
    if (first_ >= 0) {
      const second_ = name.indexOf('_', first_ + 1);
      if (second_ > first_ && name.slice(first_ + 1, second_) === campaign) {
        const projPart = name.slice(0, first_ + 1);
        const campPart = name.slice(first_ + 1, second_);
        const rest = name.slice(second_);
        return '<span class="jn-dim">' + projPart + '</span><span class="jn-campaign">' + campPart + '</span>' + rest;
      }
    }
  }

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

let _clusterUtilLastFetched = 0;
let _partitionLastFetched = 0;
const _AUX_TTL_MS = 90000;

// Soft-fail counter for the partition fetch (mirrors the per-cluster
// fanout's _consecutiveFanoutFails). A single failed tick is almost
// always transient (worker restart, slow tick) — only show the banner
// after sustained failure so we stop crying wolf.
let _consecutivePartitionFails = 0;
const _PARTITION_FAIL_BANNER_THRESHOLD = 2;

async function fetchClusterUtilization(force) {
  if (_clusterUtilFetching) return;
  if (!force && _clusterUtil && Date.now() - _clusterUtilLastFetched < _AUX_TTL_MS) return;
  _clusterUtilFetching = true;
  try {
    const res = await fetchWithTimeout('/api/cluster_utilization');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (data.status === 'ok') {
      _clusterUtil = data;
      _clusterUtilLastFetched = Date.now();
    }
  } catch (e) {
    console.warn('Cluster utilization fetch failed:', e);
  }
  _clusterUtilFetching = false;
}

async function fetchPartitions(force, clusterName) {
  const isSingleCluster = !!clusterName;
  if (!isSingleCluster) {
    if (_partitionFetching) return;
    if (!force && _partitionData && Date.now() - _partitionLastFetched < _AUX_TTL_MS) return;
    _partitionFetching = true;
  }
  try {
    const q = _computeClusterQuery(clusterName, force);
    const res = await fetchWithTimeout('/api/partition_summary' + q);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (data.status === 'ok') {
      const hasClusters = data.clusters && Object.keys(data.clusters).length > 0;
      if (hasClusters || !_partitionData) {
        if (isSingleCluster) {
          _partitionData = _mergeClusterMap(_partitionData, data.clusters || {}, clusterName);
        } else {
          _partitionData = data.clusters;
        }
      }
      _partitionLastFetched = Date.now();
      _consecutivePartitionFails = 0;
      if (typeof _clearErrorBannerKey === 'function') _clearErrorBannerKey('partitions');
    }
  } catch (e) {
    console.warn('Partition fetch failed:', e);
    _consecutivePartitionFails++;
    // Soft-fail: most single failures are transient (worker restart,
    // slow tick). Only alarm the user when it's been failing repeatedly.
    if (_consecutivePartitionFails >= _PARTITION_FAIL_BANNER_THRESHOLD &&
        typeof _setErrorBanner === 'function') {
      _setErrorBanner('partitions', 'Partition data unavailable');
    }
  } finally {
    if (!isSingleCluster) _partitionFetching = false;
  }
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

function _bestIdleRatio(partitions) {
  let best = 0;
  for (const p of partitions) {
    const total = p.total_nodes || 1;
    const idle = p.idle_nodes || 0;
    best = Math.max(best, idle / total);
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
// Sticky cache of cluster compute sizes (total GPUs) keyed by cluster name.
// Used to keep the cluster card order stable: once we observe a size, we never
// let it shrink between renders, so cards stay in place even if data is briefly
// incomplete during a refresh.
const _clusterSizeCache = {};

function _shortAcct(acct) {
  const parts = acct.split('_');
  return parts.length >= 3 ? parts.slice(2).join('-') : acct;
}

const _PPP_COLORS = ['ppp-c0', 'ppp-c1', 'ppp-c2'];

const _COMPUTE_CACHE_KEY = 'clausius.computeCache';
const _COMPUTE_CACHE_MAX_AGE_MS = 10 * 60 * 1000;

function _saveComputeCache() {
  try {
    const payload = {
      ts: Date.now(),
      pppAlloc: _pppAllocData,
      partitions: _partitionData,
      teamJobs: _teamJobsData,
      overlay: _pppOverlayData,
      fairshare: _myFairshareData,
    };
    localStorage.setItem(_COMPUTE_CACHE_KEY, JSON.stringify(payload));
  } catch (_) {}
}

function _loadComputeCache() {
  try {
    const raw = localStorage.getItem(_COMPUTE_CACHE_KEY);
    if (!raw) return false;
    const c = JSON.parse(raw);
    if (!c.ts || Date.now() - c.ts > _COMPUTE_CACHE_MAX_AGE_MS) {
      localStorage.removeItem(_COMPUTE_CACHE_KEY);
      return false;
    }
    if (c.pppAlloc) _pppAllocData = c.pppAlloc;
    if (c.partitions) _partitionData = c.partitions;
    if (c.teamJobs) _teamJobsData = c.teamJobs;
    if (c.overlay) _pppOverlayData = c.overlay;
    if (c.fairshare) _myFairshareData = c.fairshare;
    return true;
  } catch (_) {
    try { localStorage.removeItem(_COMPUTE_CACHE_KEY); } catch (_) {}
    return false;
  }
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

let _computeRefreshing = false;
const _computeRefreshingClusters = new Set();

function _mergeClusterPayload(target, payload, clusterName) {
  const merged = { ...(target || {}), ...(payload || {}) };
  const nextClusters = { ...(target?.clusters || {}) };
  if (Object.prototype.hasOwnProperty.call(payload || {}, 'clusters')) {
    if (Object.prototype.hasOwnProperty.call(payload.clusters || {}, clusterName)) {
      nextClusters[clusterName] = payload.clusters[clusterName];
    } else {
      delete nextClusters[clusterName];
    }
    merged.clusters = nextClusters;
  }
  return merged;
}

function _mergeClusterMap(target, payload, clusterName) {
  const next = { ...(target || {}) };
  if (Object.prototype.hasOwnProperty.call(payload || {}, clusterName)) {
    next[clusterName] = payload[clusterName];
  } else {
    delete next[clusterName];
  }
  return next;
}

function _computeClusterQuery(clusterName, force) {
  const params = new URLSearchParams();
  if (clusterName) params.set('cluster', clusterName);
  if (force) params.set('force', '1');
  const query = params.toString();
  return query ? `?${query}` : '';
}

function _renderCurrentPppAllocations() {
  if (_pppAllocData || _partitionData) {
    _renderPppAllocations(_pppAllocData || { clusters: {} });
  }
}

function pppCardFreshnessHtml(clusterName) {
  const disabled = _computeRefreshing || _computeRefreshingClusters.has(clusterName);
  return `<span class="card-freshness-group">${freshnessBadgeHtml(clusterName)}<button class="icon-btn ppp-refresh-btn${disabled ? ' is-loading' : ''}" onclick="refreshPppAllocations(true,'${clusterName}')" title="Refresh"${disabled ? ' disabled' : ''}>↻</button></span>`;
}

async function refreshPppAllocations(force, clusterName) {
  if (clusterName) {
    if (_computeRefreshing || _computeRefreshingClusters.has(clusterName)) return;
    _computeRefreshingClusters.add(clusterName);
    _renderCurrentPppAllocations();
    try {
      await _doRefreshPppAllocations(force, clusterName);
    } finally {
      _computeRefreshingClusters.delete(clusterName);
      _renderCurrentPppAllocations();
    }
    return;
  }
  if (_computeRefreshing) return;
  _computeRefreshing = true;
  _renderCurrentPppAllocations();
  try {
    await _doRefreshPppAllocations(force);
  } finally {
    _computeRefreshing = false;
    _renderCurrentPppAllocations();
  }
}

async function _refreshLiveClusterData(clusterName, force) {
  if (!clusterName) {
    await _ensureLiveJobData(force);
    return;
  }
  try {
    const q = force ? '?force=1' : '';
    const res = await fetchWithTimeout(`/api/jobs/${encodeURIComponent(clusterName)}${q}`);
    const data = await res.json();
    if (data.updated) allData[clusterName] = data;
  } catch (_) {}
}

async function _doRefreshPppAllocations(force, clusterName) {
  const el = document.getElementById('ppp-alloc-body');
  if (!el) return;
  const isSingleCluster = !!clusterName;

  const hadCache = !isSingleCluster && _loadComputeCache();
  if (hadCache) {
    _renderPppAllocations(_pppAllocData || { clusters: {} });
  }

  // Always force-fetch live data so we never show stale backend cache
  const fetchForce = force || hadCache;

  if (!isSingleCluster) _showComputeLoadBar(true);
  try {
    const q = _computeClusterQuery(clusterName, fetchForce);
    const results = await Promise.allSettled([
      fetchWithTimeout('/api/aihub/allocations' + q, {}, 20000),
      _ensureOverlayData(true, fetchForce, clusterName),
      fetchPartitions(fetchForce, clusterName),
      _fetchMyFairshare(fetchForce, clusterName),
      _fetchTeamJobs(fetchForce, clusterName),
      _fetchProjectColors(),
      _refreshLiveClusterData(clusterName, fetchForce),
      ...(isSingleCluster ? [] : [fetchWaitCalibration()]),
    ]);
    const allocResult = results[0];
    if (allocResult.status === 'fulfilled') {
      const data = await allocResult.value.json();
      if (data.status === 'ok') {
        _pppAllocData = isSingleCluster
          ? _mergeClusterPayload(_pppAllocData, data, clusterName)
          : data;
      } else if (!_pppAllocData) {
        el.innerHTML = `<div class="no-jobs" style="color:var(--red)">${data.error || 'Failed to load'}</div>`;
      }
    }
    _renderCurrentPppAllocations();
    _saveComputeCache();
  } catch (e) {
    if (!_pppAllocData && !_partitionData) {
      el.innerHTML = '<div class="no-jobs" style="color:var(--red)">Failed to load compute data</div>';
    }
  } finally {
    if (!isSingleCluster) _showComputeLoadBar(false);
  }
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

function toggleWdsInfo(e) {
  e.stopPropagation();
  const pop = document.getElementById('wds-info-popover');
  if (!pop) return;
  const show = !pop.classList.contains('visible');
  pop.classList.toggle('visible', show);
  if (show) {
    if (!pop._katexDone && typeof katex !== 'undefined') {
      pop.querySelectorAll('[data-katex]').forEach(el => {
        katex.render(el.dataset.katex, el, { displayMode: true, throwOnError: false });
      });
      pop._katexDone = true;
    }
    const close = (ev) => {
      if (!pop.contains(ev.target) && !ev.target.classList.contains('wds-info-btn')) {
        pop.classList.remove('visible');
        document.removeEventListener('click', close);
      }
    };
    setTimeout(() => document.addEventListener('click', close), 0);
  }
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
    const wdsCls = c.wds >= 75 ? 'wds-high' : c.wds >= 50 ? 'wds-med' : 'wds-low';
    const acctShort = c.bestAcct ? _shortAcct(c.bestAcct) : '';
    const cps = _partitionData?.[c.cn];
    const gpn = cps?.partitions?.[0]?.gpus_per_node || CLUSTERS[c.cn]?.gpus_per_node || 8;
    const idleGpus = (cps?.idle_nodes || 0) * gpn;
    const mf = _myFairshareData?.clusters?.[c.cn]?.[c.bestAcct];
    const myFs = mf ? mf.level_fs.toFixed(1) : '';
    const myFsCls = mf ? (mf.level_fs >= 1.2 ? 'ws-fs-good' : mf.level_fs >= 0.8 ? 'ws-fs-neutral' : 'ws-fs-low') : '';

    const popRows = [
      `<div class="ws-pop-row"><span>Free for team</span><span class="${c.freeForTeam > 0 ? 'green' : 'red'}">${c.freeForTeam} GPUs</span></div>`,
      `<div class="ws-pop-row"><span>Idle GPUs</span><span>${idleGpus}</span></div>`,
      `<div class="ws-pop-row"><span>PPP headroom</span><span>${c.pppHeadroom}</span></div>`,
      `<div class="ws-pop-row"><span>PPP FS</span><span>${c.levelFs.toFixed(2)}</span></div>`,
      myFs ? `<div class="ws-pop-row"><span>Your FS</span><span class="${myFsCls}">${myFs}</span></div>` : '',
      acctShort ? `<div class="ws-pop-row"><span>Account</span><span>${acctShort}</span></div>` : '',
      c.teamNum ? `<div class="ws-pop-row"><span>Team alloc</span><span>${c.teamNum}</span></div>` : '',
      `<div class="ws-pop-row"><span>Team running</span><span>${c.teamRunning || 0}</span></div>`,
    ].filter(Boolean).join('');

    return `<div class="ws-chip ${cls}">
      <span class="ws-cluster">${c.cn}</span>
      ${c.gpuType ? `<span class="ws-gpu">${c.gpuType}</span>` : ''}
      <span class="wds-badge ${wdsCls}">${c.wds}</span>
      <span class="ws-headroom">${c.freeForTeam} free</span>
      ${myFs ? `<span class="ws-my-fs ${myFsCls}">you ${myFs}</span>` : ''}
      <div class="ws-popup">${popRows}</div>
    </div>`;
  }).join('') + '</div>';
}

function _renderPppAllocations(data) {
  const el = document.getElementById('ppp-alloc-body');
  if (!el) return;
  const clusters = data.clusters || {};
  const partOnlySet = new Set();
  if (_partitionData) {
    for (const cn of Object.keys(_partitionData)) {
      if (cn !== 'local' && !clusters[cn]) partOnlySet.add(cn);
    }
  }

  const allClusterNames = [...new Set([...Object.keys(clusters), ...partOnlySet])];
  // Stable sort: GPU memory rank (highest-memory GPU first), then total GPU
  // count descending within the same tier, then cluster name alphabetically.
  // Only static properties are used so cards never reorder on refresh.
  const _gpuMemRank = { 'GB200': 0, 'B200': 1, 'H200': 2, 'H100': 3, 'A100': 4, 'L40S': 5 };
  const _gpuRank = (cn) => {
    const gt = (clusters[cn]?.gpu_type || CLUSTERS[cn]?.gpu_type || '').toUpperCase();
    return _gpuMemRank[gt] ?? 99;
  };
  const _clusterSize = (cn) => {
    let total = 0;
    const ppp = clusters[cn]?.cluster_total_gpus;
    if (typeof ppp === 'number' && ppp > 0) total = ppp;
    if (total === 0) {
      const ps = _partitionData?.[cn];
      const parts = ps?.partitions || [];
      for (const p of parts) {
        total += (p.total_nodes || 0) * (p.gpus_per_node || 0);
      }
    }
    if (total > 0 && total > (_clusterSizeCache[cn] || 0)) {
      _clusterSizeCache[cn] = total;
    }
    return _clusterSizeCache[cn] || 0;
  };
  const names = allClusterNames.sort((a, b) => {
    const aGpu = _gpuRank(a);
    const bGpu = _gpuRank(b);
    if (aGpu !== bGpu) return aGpu - bGpu;
    const aSize = _clusterSize(a);
    const bSize = _clusterSize(b);
    if (aSize !== bSize) return bSize - aSize;
    return a.localeCompare(b);
  });
  if (!names.length) {
    el.innerHTML = '<div class="no-jobs">No allocation data available</div>';
    return;
  }

  let html = _renderSubmitSummary(clusters);
  html += '<div class="ppp-grid">';
  for (const cn of names) {
    const cd = clusters[cn];
    if (!cd) {
      const ps = _partitionData?.[cn];
      if (ps) {
        const parts = (ps.partitions || []).filter(p => p.total_nodes > 0);
        if (parts.length) {
          const gpuType = ps.gpu_type || '';
          const idleNodes = ps.idle_nodes || 0;
          const pendingJobs = ps.pending_jobs || 0;
          const totalNodes = ps.total_nodes || 0;
          const gpuPer = parts[0]?.gpus_per_node || 8;
          const totalGpus = totalNodes * gpuPer;
          const usedGpus = (totalNodes - idleNodes) * gpuPer;
          const idleGpusP = idleNodes * gpuPer;
          const idleCls = idleGpusP > 0 ? 'ppp-idle-ok' : 'ppp-idle-none';

          const teamAlloc = _teamGpuAlloc[cn];
          const teamNum = teamAlloc === 'any' ? null : (typeof teamAlloc === 'number' && teamAlloc > 0 ? teamAlloc : null);
          const teamScale = document.getElementById('ppp-scale-toggle')?.checked ?? false;
          const showMe = document.getElementById('ppp-my-toggle')?.checked ?? false;
          const showTeamUsage = document.getElementById('ppp-team-usage-toggle')?.checked ?? false;
          const hasTeamQuota = teamAlloc === 'any' || (teamNum && teamNum > 0);

          const tjCluster = _teamJobsData?.clusters?.[cn];
          let tjJobs = tjCluster?.jobs || [];
          const tjSummary = tjCluster?.summary || {};
          const currentUser = USERNAME;

          if (!tjJobs.length && typeof allData !== 'undefined' && allData[cn]) {
            const cJobs = allData[cn].jobs || [];
            const live = cJobs.filter(j => !j._pinned);
            const src = live.length > 0 ? live : cJobs;
            tjJobs = src.filter(j => {
              const s = (j.state || '').toUpperCase();
              return s === 'RUNNING' || s === 'COMPLETING' || s === 'PENDING' || s === 'SUBMITTING';
            }).map(j => {
              const gm = (j.gres || '').match(/gpu[^:]*:(?:[a-zA-Z]\w*:)?(\d+)/);
              const gpn = gm ? parseInt(gm[1], 10) : 8;
              return {
                user: currentUser,
                account: j.account || '',
                job_name: j.name || '',
                state: (j.state || '').toUpperCase(),
                gpus: (parseInt(j.nodes, 10) || 1) * gpn,
              };
            });
          }

          const acctSet = new Set();
          for (const j of tjJobs) { if (j.account) acctSet.add(j.account); }
          if (!acctSet.size) {
            const cfgAcct = CLUSTERS[cn]?.account || '';
            for (const a of cfgAcct.split(',')) { if (a.trim()) acctSet.add(a.trim()); }
          }
          const acctList = [...acctSet].sort();

          let teamAllocMarker = '';
          if (teamNum) {
            const mPct = Math.min(98, Math.round(teamNum / (teamScale && teamNum > 0 ? teamNum * 1.2 : totalGpus) * 100));
            teamAllocMarker = `<div class="ppp-team-marker" style="left:${mPct}%"></div>`;
          }

          html += `<div class="ppp-card${hasTeamQuota ? '' : ' ppp-card-dim'} ppp-card-partonly">
            <div class="ppp-card-head">
              <span class="ppp-card-cluster">${cn}</span>
              ${gpuType ? `<span class="ppp-card-gpu">${gpuType}</span>` : ''}
              <span class="ppp-card-gpu" style="opacity:0.5">no PPP data</span>
              ${teamScale && teamNum ? `<span class="ppp-card-scale-label">scaled to ${teamNum}</span>` : ''}
              ${pppCardFreshnessHtml(cn)}
            </div>
            <div class="ppp-card-live"><span class="${idleCls}">${idleGpusP} idle</span> · ${pendingJobs} queued</div>`;

          if (acctList.length > 0) {
            for (const acct of acctList) {
              const acctJobs = tjJobs.filter(j => j.account === acct);
              let myRun = 0, myPend = 0, teamRun = 0, teamPend = 0, totalAcctGpus = 0;
              let myAllGpus = 0;
              for (const j of acctJobs) {
                const g = j.gpus || 0;
                totalAcctGpus += g;
                if (j.user === currentUser) {
                  myAllGpus += g;
                  if (j.state === 'RUNNING') myRun += g; else myPend += g;
                } else {
                  if (j.state === 'RUNNING') teamRun += g; else teamPend += g;
                }
              }
              const barMax = Math.max(
                (teamScale && teamNum && teamNum > 0) ? teamNum * 1.2 : totalGpus,
                myAllGpus * 1.05
              );
              const toPct = (v) => Math.min(100, Math.round(v / barMax * 100));
              let segments = '';
              if (showMe && myRun > 0) segments += `<div class="ppp-seg ppp-seg-me-run" style="width:${toPct(myRun)}%"></div>`;
              if (showMe && myPend > 0) segments += `<div class="ppp-seg ppp-seg-me-pend" style="width:${toPct(myPend)}%"></div>`;
              if (showTeamUsage && teamRun > 0) segments += `<div class="ppp-seg ppp-seg-team-run" style="width:${toPct(teamRun)}%"></div>`;
              if (showTeamUsage && teamPend > 0) segments += `<div class="ppp-seg ppp-seg-team-pend" style="width:${toPct(teamPend)}%"></div>`;
              const myPopL = myRun > 0 || myPend > 0 ? `${myRun} run${myPend > 0 ? ` · ${myPend} pend` : ''}` : '0';
              const teamPopL = teamRun > 0 || teamPend > 0 ? `${teamRun} run${teamPend > 0 ? ` · ${teamPend} pend` : ''}` : '0';
              const teamAllocL = teamNum ? `${teamNum} GPUs` : (teamAlloc === 'any' ? 'unlimited' : '');
              const popRows = [
                { label: currentUser, value: myPopL, cls: 'pop-me' },
                { label: 'team', value: teamPopL, cls: 'pop-team' },
                ...(teamAllocL ? [{ label: 'team alloc', value: teamAllocL, detail: 'informal', cls: 'pop-team-alloc' }] : []),
                { label: 'cluster total', value: `${usedGpus} / ${totalGpus}`, detail: totalGpus > 0 ? `${Math.round(usedGpus/totalGpus*100)}%` : '', cls: 'pop-cluster' },
              ];
              const popHtml = popRows.map(r =>
                `<div class="ppp-pop-row ${r.cls}"><span class="ppp-pop-label">${r.label}</span><span class="ppp-pop-val">${r.value}</span>${r.detail ? `<span class="ppp-pop-detail">${r.detail}</span>` : ''}</div>`
              ).join('');

              html += `<div class="ppp-acct-row">
                <span class="ppp-acct-name" title="${acct}">${_shortAcct(acct)}</span>
                <div class="ppp-bar-outer ppp-bar-hoverable" onclick="openUserBreakdown('${cn}','${acct}')">
                  <div class="ppp-bar-wrap">${segments}</div>
                  ${teamAllocMarker}
                  <div class="ppp-popup">${popHtml}</div>
                </div>
                <span class="ppp-acct-nums"><strong>${totalAcctGpus}</strong> GPUs</span>
              </div>`;
            }
          } else {
            const barMax = (teamScale && teamNum && teamNum > 0) ? teamNum * 1.2 : totalGpus;
            const toPct = (v) => Math.min(100, Math.round(v / barMax * 100));
            html += `<div class="ppp-acct-row">
              <span class="ppp-acct-name">cluster</span>
              <div class="ppp-bar-outer">
                <div class="ppp-bar-wrap">
                  <div class="ppp-seg ppp-seg-ppp-rest" style="width:${toPct(usedGpus)}%"></div>
                </div>
                ${teamAllocMarker}
              </div>
              <span class="ppp-acct-nums"><strong>${usedGpus}</strong> / ${totalGpus} GPUs</span>
            </div>`;
          }

          const hasAnyJobs = tjJobs.length > 0;
          if (hasAnyJobs) {
            const legendParts = [];
            if (showMe) legendParts.push(`<span><span class="ppp-legend-swatch swatch-me"></span>${currentUser} run</span>`);
            if (showMe) legendParts.push(`<span><span class="ppp-legend-swatch swatch-me-pend"></span>${currentUser} pend</span>`);
            if (showTeamUsage) legendParts.push(`<span><span class="ppp-legend-swatch swatch-team"></span>team run</span>`);
            if (showTeamUsage) legendParts.push(`<span><span class="ppp-legend-swatch swatch-team-pend"></span>team pend</span>`);
            if (legendParts.length) html += `<div class="ppp-overlay-legend">${legendParts.join('')}</div>`;
          } else {
            html += `<div class="ppp-overlay-legend" style="opacity:0.5">no active jobs on these accounts</div>`;
          }

          html += `<div class="ppp-card-footer">
              <span class="ppp-cluster-occ">${parts.length} partition${parts.length !== 1 ? 's' : ''} · ${totalGpus} total GPUs${teamNum ? ` · team alloc: ${teamNum}` : ''}</span>
            </div>
          </div>`;
        }
      }
      continue;
    }
    const accts = Object.entries(cd.accounts || {}).sort((a, b) => (b[1].gpus_allocated || 0) - (a[1].gpus_allocated || 0));
    if (!accts.length) continue;
    const rawMaxAlloc = Math.max(...accts.map(([, d]) => d.gpus_allocated || 1));

    const teamAlloc = cd.team_gpu_alloc;
    const teamNum = teamAlloc === 'any' ? null : (typeof teamAlloc === 'number' ? teamAlloc : null);
    const showTeamAlloc = true;
    const showMe = document.getElementById('ppp-my-toggle')?.checked ?? false;
    const showTeamUsage = document.getElementById('ppp-team-usage-toggle')?.checked ?? false;
    const teamScale = document.getElementById('ppp-scale-toggle')?.checked ?? false;


    let maxAlloc = (teamScale && teamNum && teamNum > 0) ? teamNum * 1.2 : rawMaxAlloc;

    const hasTeamQuota = teamAlloc === 'any' || (teamNum && teamNum > 0);
    const ps = _partitionData?.[cn];
    const idleNodes = ps?.idle_nodes || 0;
    const pendingJobs = ps?.pending_jobs || 0;
    const psGpuPer = ps?.partitions?.[0]?.gpus_per_node || CLUSTERS[cn]?.gpus_per_node || 8;
    const idleGpus = idleNodes * psGpuPer;
    const idleCls = idleGpus > 0 ? 'ppp-idle-ok' : 'ppp-idle-none';

    html += `<div class="ppp-card${hasTeamQuota ? '' : ' ppp-card-dim'}">
      <div class="ppp-card-head">
        <span class="ppp-card-cluster">${cn}</span>
        ${cd.gpu_type ? `<span class="ppp-card-gpu">${clusterGpuBadge(cn)}</span>` : ''}
        ${teamScale && teamNum ? `<span class="ppp-card-scale-label">scaled to ${teamNum}</span>` : ''}
        ${pppCardFreshnessHtml(cn)}
      </div>
      ${ps ? `<div class="ppp-card-live"><span class="${idleCls}">${idleGpus} idle</span> · ${pendingJobs} queued</div>` : ''}`;

    const overlayCluster = _pppOverlayData?.clusters?.[cn] || {};
    const currentUser = _pppOverlayData?.current_user || USERNAME;
    const teamMembers = _pppOverlayData?.team_members || [];

    const tjCluster = _teamJobsData?.clusters?.[cn]?.summary?.by_user || null;
    const hasJobSplit = !!tjCluster;

    if (hasJobSplit) {
      const allTjJobs = _teamJobsData?.clusters?.[cn]?.jobs || [];
      const currentUser = _pppOverlayData?.current_user || USERNAME;
      for (const [acct] of accts) {
        let myAllGpus = 0;
        for (const j of allTjJobs) {
          if (j.account !== acct || j.user !== currentUser) continue;
          myAllGpus += (j.gpus || 0);
        }
        if (myAllGpus > maxAlloc) maxAlloc = myAllGpus * 1.05;
      }
    }

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
      if (!teamOthersTotal && hasJobSplit && teamMembers.length) {
        for (const j of acctJobs) {
          if (teamMembers.includes(j.user) && j.user !== currentUser && j.state === 'RUNNING') teamOthersTotal += (j.gpus || 0);
        }
      }
      for (const j of acctJobs) {
        if (j.user === currentUser) myTotalSqueue += (j.gpus || 0);
      }
      if (typeof allData !== 'undefined' && allData[cn]) {
        let liveGpus = 0;
        const cJobs = allData[cn].jobs || [];
        const liveJobs = cJobs.filter(j => !j._pinned);
        const fallbackJobs = liveJobs.length > 0 ? liveJobs : cJobs;
        for (const j of fallbackJobs) {
          const s = (j.state || '').toUpperCase();
          if (s !== 'RUNNING' && s !== 'COMPLETING') continue;
          const ja = j.account || '';
          if (ja && ja !== acct) continue;
          const gm = (j.gres || '').match(/gpu[^:]*:(?:[a-zA-Z]\w*:)?(\d+)/);
          const n = parseInt(j.nodes, 10) || 0;
          const gpn = gm ? parseInt(gm[1], 10) : 8;
          liveGpus += n * gpn;
        }
        myTotalSqueue = Math.max(myTotalSqueue, liveGpus);
      }
      let myTotal = myTotalSqueue;
      myTotal = Math.min(myTotal, consumed || myTotal);
      teamOthersTotal = Math.min(teamOthersTotal, Math.max(0, (consumed || 0) - myTotal));
      const pppNonTeam = Math.max(0, consumed - myTotal - teamOthersTotal);

      const clusterOccupied = cd.cluster_occupied_gpus || 0;
      const allPppsConsumed = accts.reduce((s, [, a]) => s + (a.gpus_consumed || 0), 0);
      const clusterOthers = Math.max(0, clusterOccupied - allPppsConsumed);
      let segments = '';
      if (hasJobSplit) {
        let myRunning = 0, myPending = 0, myDep = 0, myBackup = 0, teamRunGpus = 0, teamPendGpus = 0;
        for (const j of acctJobs) {
          const g = j.gpus || 0;
          const st = (j.state || '').toUpperCase();
          if (j.user === currentUser) {
            if (st === 'RUNNING') myRunning += g;
            else if (st === 'BACKUP') myBackup += g;
            else if (st === 'DEPENDENT') myDep += g;
            else myPending += g;
          } else if (teamMembers.length && teamMembers.includes(j.user)) {
            if (st === 'RUNNING') teamRunGpus += g; else teamPendGpus += g;
          }
        }
        const myRunGpus = myRunning;
        const myPendGpus = myPending;
        const myDepGpus = myDep;
        const myBkpGpus = myBackup;

        const myAllGpus = myRunning + myPending + myDep + myBackup;
        const barRemaining = Math.max(0, maxAlloc - myAllGpus);
        const teamTotal = teamRunGpus + teamPendGpus;
        const teamFactor = teamTotal > 0 && teamTotal > barRemaining ? barRemaining / teamTotal : 1;
        const teamRunW = Math.round(teamRunGpus * teamFactor);
        const teamPendW = Math.round(teamPendGpus * teamFactor);

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
            const runW = pd.run;
            const pendW = pd.pend;
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
          if (showMe && myDepGpus > 0)
            segments += `<div class="ppp-seg ppp-seg-me-dep" style="width:${toPct(myDepGpus)}%"></div>`;
          if (showMe && myBkpGpus > 0)
            segments += `<div class="ppp-seg ppp-seg-me-bkp" style="width:${toPct(myBkpGpus)}%"></div>`;
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
              <div class="wds-pop-row"><span>queue</span><span class="${w.idleNodes > 0 ? (w.queueScore >= 0.5 ? 'green' : 'amber') : 'red'}">${w.idleNodes * (ps?.partitions?.[0]?.gpus_per_node || CLUSTERS[cn]?.gpus_per_node || 8)} idle / ${w.pendingQueue}q</span></div>
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

let _pppOverlayData = null;
let _myFairshareData = null;
let _projectColors = null;

async function _fetchProjectColors() {
  if (_projectColors) return;
  try {
    const res = await fetchWithTimeout('/api/settings');
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

async function _ensureOverlayData(refetch, force, clusterName) {
  const isSingleCluster = !!clusterName;
  if (!isSingleCluster && !refetch && (_pppOverlayData || _pppOverlayFetching)) return _pppOverlayData;
  if (!isSingleCluster) _pppOverlayFetching = true;
  try {
    const url = '/api/aihub/team_overlay' + _computeClusterQuery(clusterName, force);
    const res = await fetchWithTimeout(url, {}, 20000);
    const data = await res.json();
    if (data.status === 'ok') {
      const hasClusters = data.clusters && Object.keys(data.clusters).length > 0;
      if (hasClusters || !_pppOverlayData) {
        _pppOverlayData = isSingleCluster
          ? _mergeClusterPayload(_pppOverlayData, data, clusterName)
          : data;
      }
    }
  } catch (_) {}
  if (!isSingleCluster) _pppOverlayFetching = false;
  return _pppOverlayData;
}

async function _fetchMyFairshare(force, clusterName) {
  const isSingleCluster = !!clusterName;
  try {
    const url = '/api/aihub/my_fairshare' + _computeClusterQuery(clusterName, force);
    const res = await fetchWithTimeout(url, {}, 20000);
    const data = await res.json();
    if (data.status === 'ok') {
      _myFairshareData = isSingleCluster
        ? _mergeClusterPayload(_myFairshareData, data, clusterName)
        : data;
    }
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
    const allJobs = _teamJobsData?.clusters?.[cluster]?.jobs || [];
    const acctJobsByUser = {};
    for (const j of allJobs) {
      if (j.account !== account) continue;
      if (!acctJobsByUser[j.user]) acctJobsByUser[j.user] = { running: 0, pending: 0, dependent: 0, backup: 0, cpu_running: 0, cpu_pending: 0 };
      const d = acctJobsByUser[j.user];
      const st = (j.state || '').toUpperCase();
      if (j.is_gpu === false) {
        if (st === 'RUNNING') d.cpu_running += (j.nodes || 1);
        else d.cpu_pending += (j.nodes || 1);
      } else if (st === 'RUNNING') {
        d.running += (j.gpus || 0);
      } else if (st === 'BACKUP') {
        d.backup += (j.gpus || 0);
      } else if (st === 'DEPENDENT') {
        d.dependent += (j.gpus || 0);
      } else {
        d.pending += (j.gpus || 0);
      }
    }

    let aihubUsers = null;
    try {
      const res = await fetch(`/api/aihub/users?account=${encodeURIComponent(account)}&cluster=${cluster}&days=3`);
      const data = await res.json();
      if (data.status === 'ok' && data.users?.length) aihubUsers = data.users;
    } catch (_) { /* AI Hub unavailable — use squeue only */ }

    const allocData = _pppAllocData?.clusters?.[cluster]?.accounts?.[account] || {};
    const totalConsumed = allocData.gpus_consumed || 0;
    const totalAllocated = allocData.gpus_allocated || 0;

    const renderUserRow = (user, isMe, isTeam, barPct, tj) => {
      const running = tj.running || 0;
      const pending = tj.pending || 0;
      const dep = tj.dependent || 0;
      const bkp = tj.backup || 0;
      const cpuR = tj.cpu_running || 0;
      const cpuP = tj.cpu_pending || 0;
      const hasLive = running > 0 || pending > 0 || dep > 0 || bkp > 0 || cpuR > 0 || cpuP > 0;
      let statParts = [];
      if (hasLive) {
        if (running > 0) statParts.push(`<span class="ub-live-run">run (<span class="gpu-num">${running}</span>)</span>`);
        if (pending > 0) statParts.push(`<span class="ub-live-pend">pend (<span class="gpu-num">${pending}</span>)</span>`);
        if (dep > 0) statParts.push(`<span class="ub-live-dep">dep (<span class="gpu-num">${dep}</span>)</span>`);
        if (bkp > 0) statParts.push(`<span class="ub-live-bkp">backup (<span class="gpu-num">${bkp}</span>)</span>`);
        if (cpuR > 0 || cpuP > 0) statParts.push(`<span class="ub-live-cpu">${cpuR + cpuP} cpu</span>`);
      } else {
        statParts.push(`<span class="ub-live-avg">${Math.round(barPct > 0 ? barPct : 0)} avg</span>`);
      }
      return `<div class="ub-user-row${isMe ? ' ub-me' : ''}">
        <span class="ub-user-name${isMe ? ' ub-name-me' : isTeam ? ' ub-name-team' : ''}">${user}</span>
        <span class="ub-bar-wrap"><span class="ub-bar ${isMe ? 'ub-bar-me' : isTeam ? 'ub-bar-team' : 'ub-bar-other'}" style="width:${Math.max(2, barPct)}%"></span></span>
        <span class="ub-stats">${statParts.join('<span class="ub-sep">·</span>')}</span>
      </div>`;
    };

    let rows = '';
    let userCount = 0;
    let totalGpusLive = 0;

    if (aihubUsers) {
      const maxUser = aihubUsers[0]?.avg_gpus_consumed || 1;
      const activeUsers = aihubUsers.filter(u => {
        const tj = acctJobsByUser[u.user] || {};
        return (tj.running || 0) > 0 || (tj.pending || 0) > 0 || (tj.cpu_running || 0) > 0 || (tj.cpu_pending || 0) > 0;
      });
      const inactiveUsers = aihubUsers.filter(u => {
        const tj = acctJobsByUser[u.user] || {};
        return !((tj.running || 0) > 0 || (tj.pending || 0) > 0 || (tj.cpu_running || 0) > 0 || (tj.cpu_pending || 0) > 0);
      });
      rows = activeUsers.map(u => renderUserRow(u.user, u.user === USERNAME, teamMembers.has(u.user),
        Math.round(u.avg_gpus_consumed / maxUser * 100), acctJobsByUser[u.user] || {})).join('');
      if (inactiveUsers.length) {
        rows += `<div class="ub-divider"><span>recent (no active jobs)</span></div>`;
        rows += inactiveUsers.map(u => renderUserRow(u.user, u.user === USERNAME, teamMembers.has(u.user),
          Math.round(u.avg_gpus_consumed / maxUser * 100), acctJobsByUser[u.user] || {})).join('');
      }
      userCount = aihubUsers.length;
    } else {
      const squeueUsers = Object.keys(acctJobsByUser).sort((a, b) => {
        if (a === USERNAME) return -1;
        if (b === USERNAME) return 1;
        return ((acctJobsByUser[b].running || 0) + (acctJobsByUser[b].pending || 0)) -
               ((acctJobsByUser[a].running || 0) + (acctJobsByUser[a].pending || 0));
      });
      if (!squeueUsers.length) {
        overlay.querySelector('.ub-body').innerHTML = '<div class="no-jobs">No active jobs on this account</div>';
        return;
      }
      const maxGpus = Math.max(1, ...squeueUsers.map(u => (acctJobsByUser[u].running || 0) + (acctJobsByUser[u].pending || 0)));
      rows = squeueUsers.map(u => {
        const tj = acctJobsByUser[u];
        const total = (tj.running || 0) + (tj.pending || 0);
        totalGpusLive += total;
        return renderUserRow(u, u === USERNAME, teamMembers.has(u),
          Math.round(total / maxGpus * 100), tj);
      }).join('');
      userCount = squeueUsers.length;
    }

    const summaryConsumed = totalConsumed || totalGpusLive;
    const summaryAlloc = totalAllocated ? ` / ${totalAllocated}` : '';
    const sourceLabel = aihubUsers ? '' : ' <span style="opacity:0.5">(live squeue data)</span>';
    const header = `<div class="ub-summary">
      <span>Total: <strong class="gpu-num">${summaryConsumed}</strong>${summaryAlloc} GPUs${sourceLabel}</span>
      <span>${userCount} user${userCount !== 1 ? 's' : ''}</span>
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

async function _ensureLiveJobData(force) {
  if (!force && typeof allData !== 'undefined' && Object.values(allData).some(d => d.updated)) return;
  try {
    const res = await fetchWithTimeout('/api/jobs');
    const data = await res.json();
    if (typeof allData !== 'undefined') {
      for (const [name, d] of Object.entries(data)) {
        if (d.updated) allData[name] = d;
      }
    }
  } catch (_) {}
}

async function initClustersPage() {
  refreshPppAllocations().then(() => _populateAccountSelect());
}

/* ── Team Jobs ── */

async function _fetchTeamJobs(force, clusterName) {
  const isSingleCluster = !!clusterName;
  try {
    const url = '/api/team_jobs' + _computeClusterQuery(clusterName, force);
    const res = await fetchWithTimeout(url);
    const data = await res.json();
    if (data.status === 'ok') {
      const hasData = Object.values(data.clusters || {}).some(c => c && c.jobs && c.jobs.length > 0);
      if (hasData || !_teamJobsData) {
        _teamJobsData = isSingleCluster
          ? _mergeClusterPayload(_teamJobsData, data, clusterName)
          : data;
      }
    }
  } catch (_) {}
}

async function refreshTeamJobs() {
  await _fetchTeamJobs(true);
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
      bestIdle: _bestIdleRatio(ps.partitions || []),
    }))
    .filter(e => e.parts.length > 0)
    .sort((a, b) => b.bestIdle - a.bestIdle);

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
      const pendStr = (p.pending_jobs || 0).toLocaleString();
      rows += `<tr>
        <td>${p.name}</td>
        <td>${_fmtTime(p.max_time)}</td>
        <td>${p.preemptable ? 'yes' : ''}</td>
        <td>${idleStr}</td>
        <td>${totalStr}</td>
        <td class="dim">${pendStr}</td>
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
      <th>Queue</th>
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
  const chipGpn = ps.partitions?.[0]?.gpus_per_node || CLUSTERS[clusterName]?.gpus_per_node || 8;
  const idleGpusChip = idle * chipGpn;
  const cls = idleGpusChip > 0 ? 'part-has-idle' : (pending > 0 ? 'part-busy' : '');
  return `<span class="partition-chip ${cls}" title="${nParts} GPU partitions, ${idleGpusChip} idle GPUs, ${pending} pending jobs" onclick="event.stopPropagation();openAdvisor('${clusterName}')">
    <span class="part-count">${nParts}p</span>
    ${idleGpusChip > 0 ? `<span class="part-idle">${idleGpusChip} idle</span>` : ''}
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
      ${fsCell}
      <td class="dim">${d.idle_nodes * (d.gpus_per_node || 8)}</td>
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
      <th></th><th>Cluster</th>${acctHdr}<th>Partition</th>
      ${fsHdr}<th>Idle GPUs</th><th>Queue</th><th>Occupancy</th><th>Tier</th><th>Limit</th><th>Notes</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

async function fetchStorageQuotas() {
  const clusters = Object.keys(CLUSTERS).filter(c => c !== 'local');
  const promises = clusters.map(async c => {
    try {
      const res = await fetchWithTimeout(`/api/storage_quota/${c}`, {}, 20000);
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

/* ── Wait-time estimation: Slurm est_start > WDS-calibrated > fallback ── */

let _waitCalibration = null;

async function fetchWaitCalibration() {
  try {
    const res = await fetchWithTimeout('/api/wait_calibration');
    const data = await res.json();
    if (!data.error) _waitCalibration = data;
  } catch (_) {}
}

function _fmtWaitSec(sec) {
  if (sec < 60) return '<\u20091m';
  if (sec < 3600) return `~${Math.round(sec / 60)}m`;
  const h = sec / 3600;
  if (h < 24) return `~${h < 10 ? h.toFixed(1) : Math.round(h)}h`;
  return `~${Math.round(h / 24)}d`;
}

function _clsFromSec(sec) {
  if (sec < 600) return 'fast';
  if (sec < 3600) return 'moderate';
  if (sec < 14400) return 'slow';
  return 'long';
}

function _wdsWaitBadge(clusterName) {
  const cal = _waitCalibration?.[clusterName];
  if (!cal || !cal.length) return null;

  const cd = _pppAllocData?.clusters?.[clusterName];
  if (!cd) return null;

  const bc = cd.best_capacity || {};
  const bp = cd.best_priority || {};
  const pppHeadroom = bc.headroom || 0;
  const bestAcct = pppHeadroom > 50 ? (bc.account || '') : (bp.account || '');
  const bestAd = cd.accounts?.[bestAcct] || {};
  const wdsResult = computeWds(clusterName, bestAcct, bestAd, '');
  const wds = wdsResult.wds;

  let bucket = cal[0];
  for (const b of cal) {
    if (wds >= b.wds_min) bucket = b;
  }

  const p50 = bucket.p50_s;
  const p75 = bucket.p75_s;
  const label = _fmtWaitSec(p50);
  return { label, cls: _clsFromSec(p50), p50, p75, wds, n: bucket.n };
}

function _pendingWaitBadge(estStart, reason, clusterName) {
  const r = (reason || '').trim();

  if (r === 'DependencyNeverSatisfied')
    return { label: 'won\u2019t run', cls: 'long' };
  if (r.includes('Dependency') && r !== 'None')
    return { label: 'blocked', cls: 'moderate' };
  if (r.includes('QOS') || r.includes('Assoc') || r.includes('MaxGres') || r.includes('GrpGRES') || r.includes('GrpCpu'))
    return { label: 'quota-limited', cls: 'slow' };
  if (r === 'BeginTime')
    return { label: 'scheduled', cls: 'moderate' };
  if (r.includes('requeued') || r.includes('held') || r.includes('Held'))
    return { label: 'held', cls: 'long' };

  if (estStart) {
    const d = new Date(estStart.replace('T', ' '));
    if (!isNaN(d)) {
      const sec = Math.round((d - new Date()) / 1000);
      if (sec <= 0) return { label: 'soon', cls: 'fast' };
      return { label: _fmtWaitSec(sec), cls: _clsFromSec(sec), source: 'slurm' };
    }
  }

  if (r === 'Priority' || r === 'Resources' || !r || r === 'None') {
    const wdsBadge = _wdsWaitBadge(clusterName);
    if (wdsBadge) return { ...wdsBadge, source: 'calibrated' };
  }

  return { label: 'queued', cls: 'moderate' };
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

    const est = _pendingWaitBadge(ji.estStart, jobReason, clusterName);

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
        html += `<div class="tt-job-resources">Slurm start: ${d.toLocaleString()}</div>`;
      }
    }
    if (est.source === 'calibrated' && est.p75 != null) {
      html += `<div class="tt-job-resources dim">p50 ${_fmtWaitSec(est.p50)} · p75 ${_fmtWaitSec(est.p75)} <span class="dim">(${est.n} jobs, WDS ${est.wds})</span></div>`;
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
    if (typeof currentTab !== 'undefined' && currentTab === 'clusters') {
      refreshPppAllocations();
    } else if (typeof currentTab !== 'undefined' && currentTab === 'project' && typeof _fetchProjectData === 'function') {
      _fetchProjectData();
    } else if (typeof fetchAll === 'function') {
      fetchAll();
    }
    if (typeof startCountdown === 'function' && typeof refreshIntervalSec !== 'undefined' && refreshIntervalSec > 0) {
      startCountdown();
    }
  }
});

