// ── Theme ──
const _themeMediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

function getThemePreference() {
  return localStorage.getItem('clausius.theme') || 'system';
}

function resolveTheme(pref) {
  if (pref === 'dark') return 'dark';
  if (pref === 'light') return 'light';
  return _themeMediaQuery.matches ? 'dark' : 'light';
}

function applyTheme(pref) {
  const resolved = resolveTheme(pref || getThemePreference());
  document.documentElement.setAttribute('data-theme', resolved);
  updateThemeUI(pref || getThemePreference());
  _updateFavicon(resolved);
  if (typeof _renderAll === 'function' && Object.keys(allData || {}).length) _renderAll();
  if (typeof loadProjectButtons === 'function') loadProjectButtons();
}

function _updateFavicon(resolved) {
  const fg = resolved === 'dark' ? '%23E95378' : '%232BA298';
  const bg = resolved === 'dark' ? '%230d1117' : '%23ffffff';
  const svg = `data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 128 128'><rect width='128' height='128' rx='28' fill='${bg}'/><path d='M28 88 L44 48 L60 88 Z M38 82 L44 58 L50 82 Z' fill='${fg}' fill-rule='evenodd'/><text x='78' y='88' text-anchor='middle' font-family='Helvetica Neue,Arial,sans-serif' font-weight='800' font-size='62' fill='${fg}'>S</text><text x='108' y='106' text-anchor='middle' font-family='Helvetica Neue,Arial,sans-serif' font-weight='700' font-size='24' fill='${fg}'>&lt;0</text></svg>`;
  const el = document.getElementById('favicon');
  if (el) el.href = svg;
}

function updateThemeUI(pref) {
  document.querySelectorAll('.theme-option').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.theme === pref);
  });
}

function setTheme(mode) {
  localStorage.setItem('clausius.theme', mode);
  applyTheme(mode);
}

function cycleTheme() {
  const order = ['system', 'light', 'dark'];
  const cur = getThemePreference();
  const next = order[(order.indexOf(cur) + 1) % order.length];
  setTheme(next);
}

_themeMediaQuery.addEventListener('change', () => {
  if (getThemePreference() === 'system') applyTheme('system');
});

applyTheme();

// ── Keyboard shortcuts config ──
const SHORTCUT_DEFAULTS = {
  toggleSidebar: { label: 'Toggle sidebar',   key: 's',   code: 'KeyS',        meta: true,  ctrl: false, shift: false },
  openSpotlight: { label: 'Spotlight search',  key: 'p',   code: 'KeyP',        meta: true,  ctrl: false, shift: false },
  closeTab:      { label: 'Close tab',         key: 'w',   code: 'KeyW',        meta: true,  ctrl: false, shift: false },
  nextTab:       { label: 'Next tab',          key: ']',   code: 'BracketRight', meta: true,  ctrl: false, shift: true  },
  prevTab:       { label: 'Previous tab',      key: '[',   code: 'BracketLeft',  meta: true,  ctrl: false, shift: true  },
  refreshLive:   { label: 'Refresh live data', key: 'r',   code: 'KeyR',        meta: false, ctrl: false, shift: true, alt: true },
  exportEntry:   { label: 'Export entry',      key: 's',   code: 'KeyS',        meta: true,  ctrl: false, shift: true  },
  goBack:        { label: 'Go back',           key: 'ArrowLeft', code: 'ArrowLeft', meta: false, ctrl: false, shift: false, alt: true },
};

let _shortcuts = {};

function loadShortcuts() {
  try {
    const raw = localStorage.getItem('clausius.shortcuts');
    if (raw) {
      const saved = JSON.parse(raw);
      _shortcuts = {};
      for (const [id, def] of Object.entries(SHORTCUT_DEFAULTS)) {
        if (saved[id]) {
          const s = saved[id];
          const d = def;
          const isOldDefault = !s.code && s.key === d.key && !!s.meta === !!d.meta && !s.shift && !!d.shift;
          _shortcuts[id] = isOldDefault ? { ...d } : { ...d, ...s };
        } else {
          _shortcuts[id] = { ...def };
        }
      }
      return;
    }
  } catch (_) {}
  _shortcuts = {};
  for (const [id, def] of Object.entries(SHORTCUT_DEFAULTS)) {
    _shortcuts[id] = { ...def };
  }
}

function saveShortcuts() {
  try { localStorage.setItem('clausius.shortcuts', JSON.stringify(_shortcuts)); } catch (_) {}
}

function getShortcut(id) {
  return _shortcuts[id] || SHORTCUT_DEFAULTS[id];
}

function matchesShortcut(e, id) {
  const s = getShortcut(id);
  if (!s) return false;
  const keyMatch = s.code
    ? e.code === s.code
    : (e.key === s.key || e.key.toLowerCase() === s.key.toLowerCase());
  if (!keyMatch) return false;
  const needMeta = !!s.meta;
  const needCtrl = !!s.ctrl;
  const needShift = !!s.shift;
  const needAlt = !!s.alt;
  if (needMeta && !(e.metaKey || e.ctrlKey)) return false;
  if (!needMeta && (e.metaKey || e.ctrlKey) && !needCtrl) return false;
  if (needCtrl && !e.ctrlKey) return false;
  if (needShift !== e.shiftKey) return false;
  if (needAlt !== e.altKey) return false;
  return true;
}

function _codeToLabel(code) {
  if (!code) return null;
  const map = {
    BracketLeft: '[', BracketRight: ']', Backslash: '\\', Semicolon: ';',
    Quote: "'", Comma: ',', Period: '.', Slash: '/', Minus: '-', Equal: '=',
    Backquote: '`', Space: 'Space', Tab: '\u21E5', Enter: '\u21A9',
    Backspace: '\u232B', Delete: 'Del', Escape: 'Esc',
    ArrowLeft: '\u2190', ArrowRight: '\u2192', ArrowUp: '\u2191', ArrowDown: '\u2193',
  };
  if (map[code]) return map[code];
  if (code.startsWith('Key')) return code.slice(3);
  if (code.startsWith('Digit')) return code.slice(5);
  return null;
}

function _formatShortcutKeys(s) {
  const parts = [];
  if (s.meta) parts.push(navigator.platform.includes('Mac') ? '\u2318' : 'Ctrl');
  if (s.ctrl && !s.meta) parts.push('Ctrl');
  if (s.alt) parts.push(navigator.platform.includes('Mac') ? '\u2325' : 'Alt');
  if (s.shift) parts.push('\u21E7');
  let k = _codeToLabel(s.code) || s.key;
  if (k.length === 1) k = k.toUpperCase();
  parts.push(k);
  return parts;
}

let _recordingShortcutId = null;

function renderShortcutsEditor() {
  const el = document.getElementById('shortcuts-editor');
  if (!el) return;
  const rows = Object.entries(_shortcuts).map(([id, s]) => {
    const keys = _formatShortcutKeys(s).map(k => `<kbd>${k}</kbd>`).join('');
    const isRecording = _recordingShortcutId === id;
    const btnLabel = isRecording ? 'press keys…' : 'edit';
    const btnCls = isRecording ? 'shortcut-edit-btn recording' : 'shortcut-edit-btn';
    return `<div class="shortcut-row" data-shortcut-id="${id}">
      <span class="shortcut-label">${s.label}</span>
      <span class="shortcut-keys">${keys}</span>
      <button class="${btnCls}" onclick="startRecordingShortcut('${id}')">${btnLabel}</button>
    </div>`;
  }).join('');
  el.innerHTML = rows + '<button class="shortcut-reset-btn" onclick="resetShortcuts()">Reset all to defaults</button>';
}

function startRecordingShortcut(id) {
  _recordingShortcutId = id;
  renderShortcutsEditor();

  function onKey(e) {
    if (e.key === 'Escape') {
      _recordingShortcutId = null;
      document.removeEventListener('keydown', onKey, true);
      renderShortcutsEditor();
      return;
    }
    if (['Shift', 'Control', 'Alt', 'Meta'].includes(e.key)) return;
    e.preventDefault();
    e.stopPropagation();

    _shortcuts[id] = {
      ..._shortcuts[id],
      key: e.key,
      code: e.code,
      meta: e.metaKey,
      ctrl: e.ctrlKey,
      shift: e.shiftKey,
      alt: e.altKey,
    };
    saveShortcuts();
    _recordingShortcutId = null;
    document.removeEventListener('keydown', onKey, true);
    renderShortcutsEditor();
    toast(`Shortcut updated: ${_shortcuts[id].label}`);
  }
  document.addEventListener('keydown', onKey, true);
}

function resetShortcuts() {
  for (const [id, def] of Object.entries(SHORTCUT_DEFAULTS)) {
    _shortcuts[id] = { ...def };
  }
  saveShortcuts();
  renderShortcutsEditor();
  toast('Shortcuts reset to defaults');
}

loadShortcuts();

// ── Stats popup ──
const _gpuColorsLight = [
  '#0d6e3f', '#16a34a', '#22c55e', '#4ade80', '#6ee7a0', '#86efac', '#a7f3c0', '#bbf7d0',
  '#15803d', '#059669', '#10b981', '#34d399', '#5eead4', '#2dd4bf', '#14b8a6', '#0f766e',
];
const _gpuColorsDark = [
  '#E95378', '#f472b6', '#fb7185', '#f87171', '#fca5a5', '#fdba74', '#fbbf24', '#f59e0b',
  '#ff6b9d', '#ef4444', '#e879a0', '#f0abfc', '#d946ef', '#c084fc', '#a78bfa', '#818cf8',
];

function _getGpuColors() {
  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  return isDark ? _gpuColorsDark : _gpuColorsLight;
}

async function openStats(cluster, jobId, jobName) {
  document.getElementById('stats-overlay').classList.add('open');
  document.getElementById('stats-title').textContent = jobName || `job ${jobId}`;
  document.getElementById('stats-sub').textContent = `${cluster} · ${jobId}`;
  document.getElementById('stats-body').innerHTML = '<div class="log-loading">Loading stats…</div>';
  try {
    const res = await fetch(`/api/stats/${cluster}/${jobId}`);
    const d = await res.json();
    if (d.status !== 'ok') {
      document.getElementById('stats-body').innerHTML = `<div class="log-loading" style="color:var(--red)">${d.error || 'Could not load stats.'}</div>`;
      return;
    }

    const snapshots = d.snapshots || [];
    const liveGpus = d.gpus || [];
    const hasPerGpu = snapshots.some(s => s.per_gpu && s.per_gpu.length > 0);
    const hasGpuData = hasPerGpu || snapshots.some(s => s.gpu_util != null) || liveGpus.length > 0;
    const hasRssData = snapshots.some(s => s.rss_used != null);
    const hasCpuData = snapshots.some(s => s.cpu_util && s.cpu_util !== '00:00:00');

    let chartsHtml = '';
    if (hasGpuData) chartsHtml += '<div class="stats-chart-wrap"><canvas id="chart-gpu-util"></canvas></div>';
    if (hasGpuData) chartsHtml += '<div class="stats-chart-wrap"><canvas id="chart-gpu-mem"></canvas></div>';
    if (hasCpuData) chartsHtml += '<div class="stats-chart-wrap"><canvas id="chart-cpu"></canvas></div>';
    if (hasRssData) chartsHtml += '<div class="stats-chart-wrap"><canvas id="chart-rss"></canvas></div>';
    if (chartsHtml) chartsHtml = `<div class="stats-charts">${chartsHtml}</div>`;

    const kvs = [
      ['State', d.state], ['Elapsed', d.elapsed],
      ['Nodes', d.nodes], ['GPUs', d.gres],
      ['CPU', d.cpus], ['RSS', `${d.ave_rss || '—'} / ${d.max_rss || '—'}`],
    ].filter(([, v]) => v && v !== '—' && v !== 'N/A' && v !== '— / —')
     .map(([k, v]) => `<div class="stats-kv"><div class="stats-k">${k}</div><div class="stats-v">${v}</div></div>`)
     .join('');

    document.getElementById('stats-body').innerHTML = `
      <div class="stats-grid">${kvs}</div>
      ${chartsHtml}
    `;

    _renderStatsCharts(snapshots, liveGpus);
  } catch (e) {
    document.getElementById('stats-body').innerHTML = `<div class="log-loading" style="color:var(--red)">Failed to load stats.</div>`;
  }
}

let _statsChartInstances = [];

function _parseGpuUtil(g) {
  if (!g || !g.util) return null;
  try { return parseFloat(String(g.util).replace('%', '')); } catch (_) { return null; }
}

function _parseGpuMemUsed(g) {
  if (!g || !g.mem) return null;
  try { return parseFloat(g.mem.split('/')[0].replace('MiB', '').trim()); } catch (_) { return null; }
}

function _parseGpuMemTotal(g) {
  if (!g || !g.mem || !g.mem.includes('/')) return null;
  try { return parseFloat(g.mem.split('/')[1].replace('MiB', '').trim()); } catch (_) { return null; }
}

function _parseCpuTimeToSec(str) {
  if (!str) return null;
  const parts = str.split(':');
  if (parts.length === 3) {
    const h = parseInt(parts[0]) || 0;
    const m = parseInt(parts[1]) || 0;
    const s = parseInt(parts[2]) || 0;
    return h * 3600 + m * 60 + s;
  }
  if (parts.length === 2) {
    return (parseInt(parts[0]) || 0) * 60 + (parseInt(parts[1]) || 0);
  }
  return null;
}

function _renderStatsCharts(snapshots, liveGpus) {
  _statsChartInstances.forEach(c => c.destroy());
  _statsChartInstances = [];

  const colors = _getGpuColors();
  const cs = getComputedStyle(document.documentElement);
  const textColor = cs.getPropertyValue('--text').trim();
  const mutedColor = cs.getPropertyValue('--muted').trim();
  const gridColor = cs.getPropertyValue('--border').trim();
  const amber = cs.getPropertyValue('--amber').trim() || '#f59e0b';

  const allSnaps = [...snapshots];
  if (liveGpus && liveGpus.length > 0) {
    allSnaps.push({ ts: new Date().toISOString(), per_gpu: liveGpus, rss_used: null, gpu_util: null, gpu_mem_used: null, gpu_mem_total: null });
  }

  const labels = allSnaps.map(s => {
    try { return new Date(s.ts.replace('T', ' ')).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
    catch (_) { return s.ts; }
  });

  const chartOpts = (title, yLabel, yMax) => ({
    responsive: true, maintainAspectRatio: false,
    plugins: {
      title: { display: true, text: title, font: { family: 'monospace', size: 11, weight: 'bold' }, color: textColor },
      legend: { display: true, position: 'bottom', labels: { font: { family: 'monospace', size: 9 }, boxWidth: 10, padding: 6, color: mutedColor, usePointStyle: true, pointStyle: 'line' } },
      tooltip: { mode: 'index', intersect: false, titleFont: { family: 'monospace', size: 10 }, bodyFont: { family: 'monospace', size: 10 } },
    },
    scales: {
      x: { ticks: { font: { family: 'monospace', size: 9 }, color: mutedColor, maxTicksLimit: 10 }, grid: { color: gridColor } },
      y: { min: 0, max: yMax || undefined, ticks: { font: { family: 'monospace', size: 9 }, color: mutedColor }, grid: { color: gridColor }, title: { display: true, text: yLabel, font: { family: 'monospace', size: 9 }, color: mutedColor } },
    },
    interaction: { mode: 'index', intersect: false },
  });

  const hasPerGpu = allSnaps.some(s => s.per_gpu && s.per_gpu.length > 0);
  const gpuCount = hasPerGpu ? Math.max(...allSnaps.map(s => (s.per_gpu || []).length)) : 0;

  if (gpuCount > 0) {
    const utilDatasets = [];
    const memDatasets = [];
    for (let gi = 0; gi < gpuCount; gi++) {
      const color = colors[gi % colors.length];
      const lbl = `GPU ${gi}`;
      utilDatasets.push({
        label: lbl, borderColor: color, backgroundColor: color + '18',
        data: allSnaps.map(s => _parseGpuUtil((s.per_gpu || [])[gi])),
        fill: false, tension: 0.3, pointRadius: allSnaps.length < 4 ? 3 : 1, borderWidth: 2,
      });
      memDatasets.push({
        label: lbl, borderColor: color, backgroundColor: color + '18',
        data: allSnaps.map(s => _parseGpuMemUsed((s.per_gpu || [])[gi])),
        fill: false, tension: 0.3, pointRadius: allSnaps.length < 4 ? 3 : 1, borderWidth: 2,
      });
    }

    const ctxUtil = document.getElementById('chart-gpu-util');
    if (ctxUtil && utilDatasets.some(ds => ds.data.some(v => v != null))) {
      _statsChartInstances.push(new Chart(ctxUtil, {
        type: 'line', data: { labels, datasets: utilDatasets },
        options: chartOpts('GPU Utilization', '%', 100),
      }));
    }

    const ctxMem = document.getElementById('chart-gpu-mem');
    if (ctxMem && memDatasets.some(ds => ds.data.some(v => v != null))) {
      const totalVal = _parseGpuMemTotal((allSnaps[allSnaps.length - 1].per_gpu || [])[0]);
      if (totalVal) {
        memDatasets.push({
          label: 'Total', borderColor: mutedColor, borderDash: [5, 3],
          data: allSnaps.map(() => totalVal),
          fill: false, tension: 0, pointRadius: 0, borderWidth: 1,
        });
      }
      _statsChartInstances.push(new Chart(ctxMem, {
        type: 'line', data: { labels, datasets: memDatasets },
        options: chartOpts('GPU Memory', 'MiB'),
      }));
    }
  } else if (allSnaps.some(s => s.gpu_util != null)) {
    const ctxUtil = document.getElementById('chart-gpu-util');
    if (ctxUtil) {
      _statsChartInstances.push(new Chart(ctxUtil, {
        type: 'line',
        data: { labels, datasets: [{ label: 'Avg', data: allSnaps.map(s => s.gpu_util), borderColor: colors[0], backgroundColor: colors[0] + '33', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 }] },
        options: chartOpts('GPU Utilization (avg)', '%', 100),
      }));
    }
    const ctxMem = document.getElementById('chart-gpu-mem');
    if (ctxMem && allSnaps.some(s => s.gpu_mem_used != null)) {
      const ds = [{ label: 'Used', data: allSnaps.map(s => s.gpu_mem_used), borderColor: colors[1], backgroundColor: colors[1] + '33', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 }];
      if (allSnaps.some(s => s.gpu_mem_total != null)) ds.push({ label: 'Total', data: allSnaps.map(s => s.gpu_mem_total), borderColor: mutedColor, borderDash: [5, 3], fill: false, tension: 0, pointRadius: 0, borderWidth: 1 });
      _statsChartInstances.push(new Chart(ctxMem, { type: 'line', data: { labels, datasets: ds }, options: chartOpts('GPU Memory (avg)', 'MiB') }));
    }
  }

  const cpuSnaps = snapshots.filter(s => s.cpu_util && s.cpu_util !== '00:00:00');
  if (cpuSnaps.length >= 2) {
    const cpuLabels = cpuSnaps.map(s => {
      try { return new Date(s.ts.replace('T', ' ')).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
      catch (_) { return s.ts; }
    });
    const cpuUtilPct = [];
    for (let i = 0; i < cpuSnaps.length; i++) {
      if (i === 0) { cpuUtilPct.push(null); continue; }
      const cpuSec0 = _parseCpuTimeToSec(cpuSnaps[i - 1].cpu_util);
      const cpuSec1 = _parseCpuTimeToSec(cpuSnaps[i].cpu_util);
      const ts0 = new Date(cpuSnaps[i - 1].ts.replace('T', ' ')).getTime() / 1000;
      const ts1 = new Date(cpuSnaps[i].ts.replace('T', ' ')).getTime() / 1000;
      if (cpuSec0 != null && cpuSec1 != null && ts1 > ts0) {
        const pct = Math.min(100, Math.max(0, ((cpuSec1 - cpuSec0) / (ts1 - ts0)) * 100));
        cpuUtilPct.push(Math.round(pct * 10) / 10);
      } else {
        cpuUtilPct.push(null);
      }
    }
    const cpuColor = colors[Math.min(2, colors.length - 1)];
    const ctxCpu = document.getElementById('chart-cpu');
    if (ctxCpu && cpuUtilPct.some(v => v != null)) {
      _statsChartInstances.push(new Chart(ctxCpu, {
        type: 'line',
        data: { labels: cpuLabels, datasets: [{ label: 'CPU', data: cpuUtilPct, borderColor: cpuColor, backgroundColor: cpuColor + '33', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 }] },
        options: chartOpts('CPU Utilization', '%', 100),
      }));
    }
  }

  const rssSnaps = snapshots.filter(s => s.rss_used != null);
  if (rssSnaps.length > 0) {
    const rssLabels = rssSnaps.map(s => {
      try { return new Date(s.ts.replace('T', ' ')).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
      catch (_) { return s.ts; }
    });
    const rssColor = colors[0];
    const ctx = document.getElementById('chart-rss');
    if (ctx) _statsChartInstances.push(new Chart(ctx, {
      type: 'line',
      data: { labels: rssLabels, datasets: [{ label: 'RSS', data: rssSnaps.map(s => s.rss_used), borderColor: rssColor, backgroundColor: rssColor + '33', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 }] },
      options: chartOpts('RSS Memory', 'MB'),
    }));
  }
}

function closeStats(e) {
  if (e.target === document.getElementById('stats-overlay')) closeStatsDirect();
}
function closeStatsDirect() {
  document.getElementById('stats-overlay').classList.remove('open');
}

// ── Failed/completed pins ──
function _removeJobsFromUI(cluster, predicate) {
  if (allData[cluster] && allData[cluster].jobs) {
    allData[cluster].jobs = allData[cluster].jobs.filter(j => !predicate(j));
    _renderAll();
  }
}

async function dismissFailed(cluster, jobId) {
  _removeJobsFromUI(cluster, j => j._pinned && String(j.jobid) === String(jobId));
  fetch(`/api/clear_failed_job/${cluster}/${jobId}`, { method: 'POST' });
}

async function clearFailed(cluster) {
  _removeJobsFromUI(cluster, j => j._pinned && isFailedLikeState(j.state));
  toast(`Cleared failed jobs on ${cluster}`);
  fetch(`/api/clear_failed/${cluster}`, { method: 'POST' });
}

async function clearCancelled(cluster) {
  _removeJobsFromUI(cluster, j => j._pinned && _isCancelledState(j.state));
  toast(`Cleared cancelled jobs on ${cluster}`);
  fetch(`/api/clear_cancelled/${cluster}`, { method: 'POST' });
}

async function clearCompleted(cluster) {
  _removeJobsFromUI(cluster, j => j._pinned && isCompletedState(j.state));
  toast(`Cleared completed jobs on ${cluster}`);
  fetch(`/api/clear_completed/${cluster}`, { method: 'POST' });
}

// ── Cancel ──
async function cancelJob(cluster, jobId) {
  if (!confirm(`Cancel job ${jobId} on ${cluster}?`)) return;
  const t = toastLoading(`Cancelling ${jobId}…`);
  try {
    const res = await fetchWithTimeout(`/api/cancel/${cluster}/${jobId}`, { method: 'POST' }, 8000);
    const d = await res.json();
    if (d.status === 'ok') { t.done(`Cancelled ${jobId}`); }
    else { t.done(d.error, 'error'); return; }
  } catch { t.done('Cancel failed', 'error'); return; }
  setTimeout(() => refreshCluster(cluster, true), 1500);
}

async function cancelGroup(cluster, jobIdsJson, groupName) {
  const jobIds = JSON.parse(jobIdsJson);
  if (!confirm(`Cancel ${jobIds.length} job${jobIds.length !== 1 ? 's' : ''} in "${groupName}" on ${cluster}?`)) return;
  const t = toastLoading(`Cancelling ${jobIds.length} jobs…`);
  try {
    const res = await fetchWithTimeout(`/api/cancel_jobs/${cluster}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_ids: jobIds }),
    }, 8000);
    const d = await res.json();
    if (d.status === 'ok') { t.done(`Cancelled ${d.cancelled} jobs in ${groupName}`); }
    else if (d.status === 'partial') { t.done(`Cancelled ${d.cancelled} jobs, ${d.errors.length} failed`, 'error'); }
    else { t.done(d.error, 'error'); return; }
  } catch { t.done('Cancel group failed', 'error'); return; }
  setTimeout(() => refreshCluster(cluster, true), 1500);
}


// ── Fetch with timeout ──
const MOUNT_TIMEOUT_MS = 60000;

function _fetchTimeout(url, opts = {}, timeoutMs = MOUNT_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  return fetch(url, { ...opts, signal: ctrl.signal })
    .finally(() => clearTimeout(timer));
}

// ── Mount controls ──
async function mountCluster(cluster) {
  const t = toastLoading(`Mounting ${cluster}…`);
  try {
    const res = await _fetchTimeout(`/api/mount/mount/${cluster}`, { method: 'POST' });
    const d = await res.json();
    if (d.status === 'ok') {
      t.done(`Mounted ${cluster}`);
      refreshCluster(cluster, true);
    } else {
      t.done(d.error || `Mount failed: ${cluster}`, 'error');
    }
  } catch (e) {
    t.done(e.name === 'AbortError' ? `Mount timed out: ${cluster}` : `Mount failed: ${cluster}`, 'error');
  }
  renderMountPanel(allData);
}

async function unmountCluster(cluster) {
  const t = toastLoading(`Unmounting ${cluster}…`);
  try {
    const res = await _fetchTimeout(`/api/mount/unmount/${cluster}`, { method: 'POST' });
    const d = await res.json();
    if (d.status === 'ok') {
      t.done(`Unmounted ${cluster}`);
      refreshCluster(cluster, true);
    } else {
      t.done(d.error || `Unmount failed: ${cluster}`, 'error');
    }
  } catch (e) {
    t.done(e.name === 'AbortError' ? `Unmount timed out: ${cluster}` : `Unmount failed: ${cluster}`, 'error');
  }
  renderMountPanel(allData);
}

async function remountCluster(cluster) {
  const t = toastLoading(`Restarting mount: ${cluster}…`);
  try { await _fetchTimeout(`/api/mount/unmount/${cluster}`, { method: 'POST' }, 15000); } catch {}
  try {
    const res = await _fetchTimeout(`/api/mount/mount/${cluster}`, { method: 'POST' });
    const d = await res.json();
    if (d.status === 'ok') {
      t.done(`Remounted ${cluster}`);
      refreshCluster(cluster, true);
    } else {
      t.done(d.error || `Remount failed: ${cluster}`, 'error');
    }
  } catch (e) {
    t.done(e.name === 'AbortError' ? `Remount timed out: ${cluster}` : `Remount failed: ${cluster}`, 'error');
  }
  renderMountPanel(allData);
}

async function mountAll() {
  const clusters = Object.keys(CLUSTERS).filter(c => c !== 'local');
  const t = toastLoading(`Mounting ${clusters.length} clusters…`);
  let ok = 0, fail = 0;
  const failed = [];
  await Promise.allSettled(clusters.map(async c => {
    try {
      const res = await _fetchTimeout(`/api/mount/mount/${c}`, { method: 'POST' });
      const d = await res.json();
      if (d.status === 'ok') { ok++; } else { fail++; failed.push(c); }
    } catch (e) {
      fail++;
      failed.push(e.name === 'AbortError' ? `${c} (timeout)` : c);
    }
    t.update(`Mounting… ${ok + fail}/${clusters.length}`);
  }));
  if (fail === 0) t.done(`Mounted all ${ok} clusters`);
  else t.done(`Mounted ${ok}/${clusters.length} — failed: ${failed.join(', ')}`, fail === clusters.length ? 'error' : 'ok');
  fetchAll();
  renderMountPanel(allData);
}

async function unmountAll() {
  const clusters = Object.keys(CLUSTERS).filter(c => c !== 'local');
  const t = toastLoading(`Unmounting ${clusters.length} clusters…`);
  let ok = 0, fail = 0;
  const failed = [];
  await Promise.allSettled(clusters.map(async c => {
    try {
      const res = await _fetchTimeout(`/api/mount/unmount/${c}`, { method: 'POST' });
      const d = await res.json();
      if (d.status === 'ok') { ok++; } else { fail++; failed.push(c); }
    } catch (e) {
      fail++;
      failed.push(e.name === 'AbortError' ? `${c} (timeout)` : c);
    }
    t.update(`Unmounting… ${ok + fail}/${clusters.length}`);
  }));
  if (fail === 0) t.done(`Unmounted all ${ok} clusters`);
  else t.done(`Unmounted ${ok}/${clusters.length} — failed: ${failed.join(', ')}`, fail === clusters.length ? 'error' : 'ok');
  fetchAll();
  renderMountPanel(allData);
}

async function checkMountStatus(cluster) {
  try {
    const res = await fetch(`/api/mounts?cluster=${cluster}`);
    const d = await res.json();
    if (d.status !== 'ok') {
      toast(d.error || `Status check failed for ${cluster}`, 'error');
      return;
    }
    const item = (d.mounts || {})[cluster] || {};
    allData[cluster] = allData[cluster] || { status: 'error', jobs: [] };
    allData[cluster].mount = item;
    renderMountPanel(allData);
    toast(`${cluster}: ${item.mounted ? 'mounted' : 'ssh-only'}`);
  } catch {
    toast(`Status check failed for ${cluster}`, 'error');
  }
}

// ── Toast ──
function toast(msg, type='ok') {
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = msg;
  document.getElementById('toasts').appendChild(el);
  setTimeout(() => el.remove(), 3000);
}

function toastLoading(msg) {
  const el = document.createElement('div');
  el.className = 'toast loading';
  el.innerHTML = `<span class="toast-msg">${msg.replace(/</g,'&lt;')}</span><div class="toast-bar"><div class="toast-bar-fill"></div></div>`;
  document.getElementById('toasts').appendChild(el);
  return {
    update(newMsg) { const s = el.querySelector('.toast-msg'); if (s) s.textContent = newMsg; },
    done(finalMsg, type='ok') {
      el.className = `toast ${type}`;
      el.innerHTML = finalMsg.replace(/</g,'&lt;');
      setTimeout(() => el.remove(), type === 'error' ? 6000 : 3000);
    },
    remove() { el.remove(); },
  };
}

// ── Countdown ──
function refreshNow() {
  clearInterval(cdTimer);
  if (typeof _forceRefreshAll === 'function') _forceRefreshAll();
  else fetchAll();
  if (refreshIntervalSec > 0) {
    countdown = refreshIntervalSec;
    startCountdown();
  }
}

function stopCountdown() {
  clearInterval(cdTimer);
}

function _isModalOpen() {
  const overlay = document.getElementById('modal-overlay');
  return overlay && overlay.classList.contains('open');
}

function startCountdown() {
  document.getElementById('cd').textContent = countdown;
  cdTimer = setInterval(() => {
    if (document.hidden) return;
    if (_isModalOpen()) return;
    countdown--;
    document.getElementById('cd').textContent = countdown;
    if (countdown <= 0) {
      countdown = refreshIntervalSec;
      if (typeof currentTab !== 'undefined' && currentTab === 'clusters') {
        refreshPppAllocations();
      } else {
        fetchAll();
      }
    }
  }, 1000);
}

// ── Settings modal ──

function openSettingsModal() {
  document.getElementById('settings-overlay').classList.add('open');
  loadSettingsPanel();
  renderMountPanel(allData);
  updateThemeUI(getThemePreference());
  renderShortcutsEditor();
}

function closeSettingsModal() {
  document.getElementById('settings-overlay').classList.remove('open');
}

function showSettingsSection(el) {
  document.querySelectorAll('.settings-nav-item').forEach(n => n.classList.remove('active'));
  document.querySelectorAll('.settings-section').forEach(s => s.classList.remove('active'));
  el.classList.add('active');
  document.getElementById(el.dataset.section).classList.add('active');
}

async function loadSettingsPanel() {
  try {
    const res = await fetch('/api/settings');
    const cfg = await res.json();
    document.getElementById('set-ssh-timeout').value = cfg.ssh_timeout || 8;
    document.getElementById('set-cache-fresh').value = cfg.cache_fresh_sec || 30;
    document.getElementById('set-stats-interval').value = cfg.stats_interval_sec || 1800;
    document.getElementById('set-backup-interval').value = cfg.backup_interval_hours || 24;
    document.getElementById('set-backup-max').value = cfg.backup_max_keep || 7;

    const inc = (cfg.local_process_filters || {}).include || [];
    const exc = (cfg.local_process_filters || {}).exclude || [];
    document.getElementById('set-proc-include').value = inc.join(', ');
    document.getElementById('set-proc-exclude').value = exc.join(', ');

    document.getElementById('set-team').value = cfg.team || '';
    renderGpuAllocEditor(cfg.team_gpu_allocations || {});
    renderPppEditor(cfg.ppps || {});

    renderClusterEditor(cfg.clusters || {});
    renderProjectEditor(cfg.projects || {});
  } catch (e) {
    toast('Failed to load settings', 'error');
  }
}

function renderPppEditor(ppps) {
  const el = document.getElementById('ppp-editor');
  el.innerHTML = Object.entries(ppps).map(([name, pid]) => `
    <div class="cluster-edit-card" style="margin-bottom:4px">
      <div class="ce-head">
        <span class="ce-name" style="font-size:10px">${name}</span>
        <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove()" title="remove">✕</button>
      </div>
      <div class="ce-fields">
        <div class="ce-field"><span>PPP Name</span><input data-f="ppp-name" value="${name}" style="font-size:10px"></div>
        <div class="ce-field"><span>Project ID</span><input data-f="ppp-id" type="number" value="${pid}"></div>
      </div>
    </div>
  `).join('');
}

function renderGpuAllocEditor(allocs) {
  const el = document.getElementById('gpu-alloc-editor');
  const clusterNames = Object.keys(CLUSTERS).filter(c => c !== 'local').sort();
  if (!clusterNames.length) {
    el.innerHTML = '<div style="font-family:var(--mono);font-size:10px;color:var(--muted)">No clusters configured</div>';
    return;
  }
  let html = '<table class="gpu-alloc-table"><thead><tr><th>Cluster</th><th>GPUs</th><th></th></tr></thead><tbody>';
  for (const c of clusterNames) {
    const raw = allocs[c];
    const isAny = raw === 'any' || raw === -1;
    const val = isAny ? '' : (raw || '');
    const checked = isAny ? ' checked' : '';
    const disabled = isAny ? ' disabled' : '';
    html += `<tr data-alloc-row="${c}">
      <td>${c}</td>
      <td><input data-alloc-cluster="${c}" type="number" value="${val}" min="0" max="99999" class="gpu-alloc-input" placeholder="0"${disabled}></td>
      <td><label class="gpu-alloc-any-label"><input type="checkbox" data-alloc-any="${c}" onchange="_toggleAllocAny('${c}', this.checked)"${checked}> <span>any</span></label></td>
    </tr>`;
  }
  html += '</tbody></table>';
  el.innerHTML = html;
}

function _toggleAllocAny(cluster, isAny) {
  const inp = document.querySelector(`input[data-alloc-cluster="${cluster}"]`);
  if (!inp) return;
  inp.disabled = isAny;
  if (isAny) inp.value = '';
}

function _readGpuAllocations() {
  const allocInputs = document.querySelectorAll('#gpu-alloc-editor input[data-alloc-cluster]');
  const team_gpu_allocations = {};
  for (const inp of allocInputs) {
    const cluster = inp.dataset.allocCluster;
    const anyBox = document.querySelector(`input[data-alloc-any="${cluster}"]`);
    if (anyBox && anyBox.checked) {
      team_gpu_allocations[cluster] = 'any';
    } else {
      const gpus = parseInt(inp.value) || 0;
      if (gpus > 0) team_gpu_allocations[cluster] = gpus;
    }
  }
  return team_gpu_allocations;
}

async function saveGpuAllocations() {
  const team_gpu_allocations = _readGpuAllocations();
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ team_gpu_allocations }),
    });
    const d = await res.json();
    if (d.status === 'ok') toast('GPU allocations saved');
    else toast(d.error || 'Save failed', 'error');
  } catch { toast('Save failed', 'error'); }
}

function addPppRow() {
  const el = document.getElementById('ppp-editor');
  const div = document.createElement('div');
  div.className = 'cluster-edit-card';
  div.style.marginBottom = '4px';
  div.innerHTML = `
    <div class="ce-head">
      <span class="ce-name" style="font-size:10px">new PPP</span>
      <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove()" title="remove">✕</button>
    </div>
    <div class="ce-fields">
      <div class="ce-field"><span>PPP Name</span><input data-f="ppp-name" value="" placeholder="team_project_..." style="font-size:10px"></div>
      <div class="ce-field"><span>Project ID</span><input data-f="ppp-id" type="number" value="" placeholder="12345"></div>
    </div>
  `;
  el.appendChild(div);
}

async function saveProfile() {
  const team = document.getElementById('set-team').value.trim();
  const team_gpu_allocations = _readGpuAllocations();
  const cards = document.querySelectorAll('#ppp-editor .cluster-edit-card');
  const ppps = {};
  for (const card of cards) {
    const name = (card.querySelector('[data-f="ppp-name"]').value || '').trim();
    const pid = parseInt(card.querySelector('[data-f="ppp-id"]').value) || 0;
    if (name && pid > 0) ppps[name] = pid;
  }
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ team, team_gpu_allocations, ppps }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      toast('Profile saved');
      _storageQuota = {};
      fetchStorageQuotas().then(() => { if (Object.keys(allData).length) _renderAll(); });
      fetchClusterUtilization().then(() => { if (Object.keys(allData).length) _renderAll(); });
    } else {
      toast(d.error || 'Save failed', 'error');
    }
  } catch (e) {
    toast('Save failed', 'error');
  }
}

function renderClusterEditor(clusters) {
  const el = document.getElementById('cluster-editor');
  el.innerHTML = Object.entries(clusters).map(([name, c]) => `
    <div class="cluster-edit-card" data-cluster="${name}">
      <div class="ce-head">
        <span class="ce-name">${name}</span>
        <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove()" title="remove">✕</button>
      </div>
      <div class="ce-fields">
        <div class="ce-field"><span>Name</span><input data-f="name" value="${name}"></div>
        <div class="ce-field"><span>Host</span><input data-f="host" value="${c.host || ''}"></div>
        <div class="ce-field"><span>Port</span><input data-f="port" type="number" value="${c.port || 22}"></div>
        <div class="ce-field"><span>GPU Type</span><input data-f="gpu_type" value="${c.gpu_type || ''}"></div>
        <div class="ce-field" style="grid-column:1/-1"><span>Mount Paths</span><textarea data-f="mount_paths" rows="3" placeholder="/lustre/fsw/.../users/$USER">${(c.mount_paths || []).join('\n')}</textarea></div>
      </div>
    </div>
  `).join('');
}

function addClusterRow() {
  const el = document.getElementById('cluster-editor');
  const div = document.createElement('div');
  div.className = 'cluster-edit-card';
  div.innerHTML = `
    <div class="ce-head">
      <span class="ce-name">new cluster</span>
      <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove()" title="remove">✕</button>
    </div>
    <div class="ce-fields">
      <div class="ce-field"><span>Name</span><input data-f="name" value="" placeholder="cluster-name"></div>
      <div class="ce-field"><span>Host</span><input data-f="host" value="" placeholder="login-node.example.com"></div>
      <div class="ce-field"><span>Port</span><input data-f="port" type="number" value="22"></div>
      <div class="ce-field"><span>GPU Type</span><input data-f="gpu_type" value="" placeholder="H100"></div>
      <div class="ce-field" style="grid-column:1/-1"><span>Mount Paths</span><textarea data-f="mount_paths" rows="3" placeholder="/lustre/fsw/.../users/$USER"></textarea></div>
    </div>
  `;
  el.appendChild(div);
}

async function saveClusters() {
  const cards = document.querySelectorAll('#cluster-editor .cluster-edit-card');
  const clusters = {};
  for (const card of cards) {
    const name = (card.querySelector('[data-f="name"]').value || '').trim();
    if (!name) continue;
    const mpRaw = (card.querySelector('[data-f="mount_paths"]').value || '').trim();
    const mountPaths = mpRaw ? mpRaw.split('\n').map(s => s.trim()).filter(Boolean) : [];
    clusters[name] = {
      host: card.querySelector('[data-f="host"]').value.trim(),
      port: parseInt(card.querySelector('[data-f="port"]').value) || 22,
      gpu_type: card.querySelector('[data-f="gpu_type"]').value.trim(),
      mount_paths: mountPaths,
    };
  }
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clusters }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      toast('Clusters saved');
      fetchAll();
    } else {
      toast(d.error || 'Save failed', 'error');
    }
  } catch (e) {
    toast('Save failed', 'error');
  }
}

function renderProjectEditor(projects) {
  const el = document.getElementById('project-editor');
  el.innerHTML = Object.entries(projects).map(([name, p]) => `
    <div class="cluster-edit-card" data-project="${name}">
      <div class="ce-head">
        <span class="ce-name" style="display:flex;align-items:center;gap:6px">
          <span style="font-size:16px">${p.emoji || '📁'}</span>
          <span class="project-color-dot" style="background:${p.color || '#ddd'}"></span>${name}
        </span>
        <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove()" title="remove">✕</button>
      </div>
      <div class="ce-fields">
        <div class="ce-field"><span>Name</span><input data-f="name" value="${name}"></div>
        <div class="ce-field"><span>Prefix</span><input data-f="prefix" value="${p.prefix || ''}" placeholder="name_"></div>
        <div class="ce-field"><span>Emoji</span><input data-f="emoji" value="${p.emoji || ''}" placeholder="🔬" style="width:40px;text-align:center"></div>
        <div class="ce-field"><span>Color</span><span class="color-pair"><input data-f="color" type="color" value="${p.color || '#e8f4fd'}" style="width:28px;height:28px;padding:0;border:none;cursor:pointer" oninput="this.nextElementSibling.value=this.value"><input data-f="color-hex" type="text" value="${p.color || '#e8f4fd'}" style="width:70px" placeholder="#e8f4fd" oninput="const c=this.previousElementSibling;if(/^#[0-9a-fA-F]{6}$/.test(this.value))c.value=this.value"></span></div>
      </div>
    </div>
  `).join('');
}

function addProjectRow() {
  const el = document.getElementById('project-editor');
  const div = document.createElement('div');
  div.className = 'cluster-edit-card';
  div.innerHTML = `
    <div class="ce-head">
      <span class="ce-name">new project</span>
      <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove()" title="remove">✕</button>
    </div>
    <div class="ce-fields">
      <div class="ce-field"><span>Name</span><input data-f="name" value="" placeholder="my-project"></div>
      <div class="ce-field"><span>Prefix</span><input data-f="prefix" value="" placeholder="my-project_"></div>
      <div class="ce-field"><span>Emoji</span><input data-f="emoji" value="" placeholder="🔬" style="width:40px;text-align:center"></div>
      <div class="ce-field"><span>Color</span><span class="color-pair"><input data-f="color" type="color" value="#e8f4fd" style="width:28px;height:28px;padding:0;border:none;cursor:pointer" oninput="this.nextElementSibling.value=this.value"><input data-f="color-hex" type="text" value="#e8f4fd" style="width:70px" placeholder="#e8f4fd" oninput="const c=this.previousElementSibling;if(/^#[0-9a-fA-F]{6}$/.test(this.value))c.value=this.value"></span></div>
    </div>
  `;
  el.appendChild(div);
}

async function saveProjects() {
  const cards = document.querySelectorAll('#project-editor .cluster-edit-card');
  const projects = {};
  for (const card of cards) {
    const name = (card.querySelector('[data-f="name"]').value || '').trim();
    if (!name) continue;
    const hexInput = card.querySelector('[data-f="color-hex"]');
    const pickerInput = card.querySelector('[data-f="color"]');
    const color = (hexInput && /^#[0-9a-fA-F]{6}$/.test(hexInput.value.trim()))
      ? hexInput.value.trim()
      : (pickerInput ? pickerInput.value.trim() : '#e8f4fd');
    projects[name] = {
      prefix: card.querySelector('[data-f="prefix"]').value.trim(),
      emoji: card.querySelector('[data-f="emoji"]').value.trim(),
      color,
    };
  }
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ projects }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      toast('Projects saved');
      if (d.settings && d.settings.projects) {
        renderProjectEditor(d.settings.projects);
      }
      _projectColors = null;
      loadProjectButtons();
      fetchAll();
    } else {
      toast(d.error || 'Save failed', 'error');
    }
  } catch (e) {
    toast('Save failed', 'error');
  }
}

async function saveAdvancedSettings() {
  const sshTimeout = parseInt(document.getElementById('set-ssh-timeout').value) || 8;
  const cacheFresh = parseInt(document.getElementById('set-cache-fresh').value) || 30;
  const statsInterval = parseInt(document.getElementById('set-stats-interval').value) || 1800;
  const backupInterval = parseInt(document.getElementById('set-backup-interval').value) || 24;
  const backupMax = parseInt(document.getElementById('set-backup-max').value) || 7;
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ssh_timeout: sshTimeout,
        cache_fresh_sec: cacheFresh,
        stats_interval_sec: statsInterval,
        backup_interval_hours: backupInterval,
        backup_max_keep: backupMax,
      }),
    });
    const d = await res.json();
    if (d.status === 'ok') toast('Advanced settings saved');
    else toast(d.error || 'Save failed', 'error');
  } catch (e) {
    toast('Save failed', 'error');
  }
}

async function saveProcessFilters() {
  const inc = document.getElementById('set-proc-include').value.split(',').map(s => s.trim()).filter(Boolean);
  const exc = document.getElementById('set-proc-exclude').value.split(',').map(s => s.trim()).filter(Boolean);
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ local_process_filters: { include: inc, exclude: exc } }),
    });
    const d = await res.json();
    if (d.status === 'ok') toast('Process filters saved');
    else toast(d.error || 'Save failed', 'error');
  } catch (e) {
    toast('Save failed', 'error');
  }
}

// ── Background run suffixes ──
const BG_SUFFIXES_DEFAULT = ['_server'];
let bgSuffixes = [...BG_SUFFIXES_DEFAULT];

function loadBgSuffixes() {
  try {
    const raw = localStorage.getItem('clausius.bgSuffixes');
    if (raw) bgSuffixes = JSON.parse(raw);
    else bgSuffixes = [...BG_SUFFIXES_DEFAULT];
  } catch (_) {
    bgSuffixes = [...BG_SUFFIXES_DEFAULT];
  }
  const el = document.getElementById('set-bg-suffixes');
  if (el) el.value = bgSuffixes.join(', ');
}

function saveBgSuffixes() {
  const el = document.getElementById('set-bg-suffixes');
  if (!el) return;
  bgSuffixes = el.value.split(',').map(s => s.trim()).filter(Boolean);
  try { localStorage.setItem('clausius.bgSuffixes', JSON.stringify(bgSuffixes)); } catch (_) {}
  if (typeof _renderAll === 'function' && Object.keys(allData || {}).length) _renderAll();
  toast('Background suffixes saved');
}

function isBackgroundRun(name) {
  if (!name || !bgSuffixes.length) return false;
  const lower = name.toLowerCase();
  return bgSuffixes.some(s => lower.endsWith(s.toLowerCase()));
}

// ── Local settings (localStorage) ──
let jsonlLimit = 50;
let jsonlMode = 'first';

function loadLocalSettings() {
  try {
    const autoRefresh = localStorage.getItem('clausius.autoRefresh') === '1';
    const interval = parseInt(localStorage.getItem('clausius.refreshInterval') || '30');
    const pageSize = parseInt(localStorage.getItem('clausius.histPageSize') || '50');
    jsonlLimit = parseInt(localStorage.getItem('clausius.jsonlLimit') || '50');
    jsonlMode = localStorage.getItem('clausius.jsonlMode') || 'first';
    document.getElementById('set-autorefresh').checked = autoRefresh;
    document.getElementById('set-refresh-interval').value = interval;
    document.getElementById('set-hist-pagesize').value = pageSize;
    document.getElementById('set-jsonl-limit').value = jsonlLimit;
    document.getElementById('set-jsonl-mode').value = jsonlMode;
    return { autoRefresh, interval, pageSize };
  } catch (_) {
    return { autoRefresh: false, interval: 30, pageSize: 50 };
  }
}

function saveLocalSettings() {
  const autoRefresh = document.getElementById('set-autorefresh').checked;
  const interval = parseInt(document.getElementById('set-refresh-interval').value) || 30;
  const pageSize = parseInt(document.getElementById('set-hist-pagesize').value) || 50;
  jsonlLimit = parseInt(document.getElementById('set-jsonl-limit').value) || 100;
  jsonlMode = document.getElementById('set-jsonl-mode').value || 'last';
  try {
    localStorage.setItem('clausius.autoRefresh', autoRefresh ? '1' : '0');
    localStorage.setItem('clausius.refreshInterval', String(interval));
    localStorage.setItem('clausius.histPageSize', String(pageSize));
    localStorage.setItem('clausius.jsonlLimit', String(jsonlLimit));
    localStorage.setItem('clausius.jsonlMode', jsonlMode);
  } catch (_) {}
  applyLocalSettings();
}

function applyLocalSettings() {
  const s = loadLocalSettings();
  HIST_GROUPS_PER_PAGE = s.pageSize;
  refreshIntervalSec = s.autoRefresh ? Math.max(5, s.interval) : 0;
  clearInterval(cdTimer);
  if (refreshIntervalSec > 0) {
    countdown = refreshIntervalSec;
    startCountdown();
    document.getElementById('cd').parentElement.style.display = '';
  } else {
    document.getElementById('cd').parentElement.style.display = 'none';
  }
}

// Init
setupTreeResizer();
setupSidebarResizer();
applySidebarState();
loadLocalSettings();
applyLocalSettings();
loadBgSuffixes();
fetchAll();
loadProjectButtons();

// Restore tabs across refreshes
(function restoreTab() {
  if (!_restoreTabs()) {
    _renderAppTabs();
    showTab('live');
  }
})();

if (refreshIntervalSec > 0) startCountdown();
