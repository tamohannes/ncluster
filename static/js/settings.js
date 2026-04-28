// ── Theme ──
const _themeMediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

function getThemePreference() {
  return localStorage.getItem('clausius.theme') || 'system';
}

function getAppearancePreference() {
  return localStorage.getItem('clausius.appearance') || 'classic';
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
  document.querySelectorAll('.theme-option[data-theme]').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.theme === pref);
  });
}

function applyAppearance(pref) {
  const mode = pref || getAppearancePreference();
  document.documentElement.setAttribute('data-appearance', mode === 'glass' ? 'glass' : 'classic');
  updateAppearanceUI(mode);
}

function updateAppearanceUI(pref) {
  document.querySelectorAll('.appearance-option').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.appearance === pref);
  });
}

function setTheme(mode) {
  localStorage.setItem('clausius.theme', mode);
  applyTheme(mode);
}

function setAppearance(mode) {
  const pref = mode === 'glass' ? 'glass' : 'classic';
  localStorage.setItem('clausius.appearance', pref);
  applyAppearance(pref);
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
applyAppearance();

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
  _statsChartInstances.forEach(c => c.destroy());
  _statsChartInstances = [];
  let slurmHtml = '';
  let snapshots = [];
  let liveGpus = [];
  let shouldRenderCharts = false;
  try {
    const res = await fetch(`/api/stats/${cluster}/${jobId}`);
    const d = await res.json();
    if (d.status !== 'ok') {
      slurmHtml = `<div class="log-loading" style="color:var(--red)">${d.error || 'Could not load stats.'}</div>`;
    } else {
      snapshots = d.snapshots || [];
      liveGpus = d.gpus || [];
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

      slurmHtml = `
        <div class="stats-grid">${kvs}</div>
        ${chartsHtml}
      `;
      shouldRenderCharts = true;
    }
  } catch (e) {
    slurmHtml = `<div class="log-loading" style="color:var(--red)">Failed to load stats.</div>`;
  }
  document.getElementById('stats-body').innerHTML = slurmHtml + '<div id="custom-metrics-section"></div>';
  if (shouldRenderCharts) _renderStatsCharts(snapshots, liveGpus);
  _loadCustomMetricsForStats(cluster, jobId);
}

let _statsChartInstances = [];

async function _loadCustomMetricsForStats(cluster, jobId) {
  const el = document.getElementById('custom-metrics-section');
  if (!el) return;

  const refreshBtn = `<button onclick="_loadCustomMetricsForStats('${cluster}','${jobId}')" style="border:none;background:none;cursor:pointer;color:var(--muted);font-size:13px;padding:0 4px;vertical-align:middle" title="refresh">↻</button>`;
  el.innerHTML = `<div style="margin-top:14px;padding-top:10px;border-top:1px solid var(--border)">
    <div style="font-family:var(--mono);font-size:11px;font-weight:600;margin-bottom:6px">Custom Metrics ${refreshBtn}
      <span id="custom-metrics-loading" style="font-weight:400;color:var(--muted);font-size:10px;margin-left:6px">loading…</span>
    </div>
    <div id="custom-metrics-grid"></div></div>`;

  try {
    const res = await fetch(`/api/custom_metrics/${cluster}/${jobId}`);
    const d = await res.json();
    document.getElementById('custom-metrics-loading')?.remove();
    const grid = document.getElementById('custom-metrics-grid');
    if (!grid) return;

    if (d.unconfigured) {
      grid.innerHTML = `<div class="stats-kv">
        <div class="stats-v" style="color:var(--muted);font-style:italic">
          Not configured. Set regex extractors in the log viewer modal.</div></div>`;
      return;
    }
    if (d.status !== 'ok') {
      grid.innerHTML = `<div class="stats-kv">
        <div class="stats-v" style="color:var(--red)">${d.error || 'Error'}</div></div>`;
      return;
    }
    if (!d.metrics || !d.metrics.length) {
      grid.innerHTML = `<div class="stats-kv">
        <div class="stats-v" style="color:var(--muted)">No extractors defined.</div></div>`;
      return;
    }
    grid.className = 'stats-grid';
    grid.innerHTML = d.metrics.map((metric) => `
      <div class="stats-kv">
        <div class="stats-k">${_statsEsc(metric.name)}</div>
        <div class="stats-v">${metric.value !== null && metric.value !== undefined ? _statsEsc(String(metric.value)) : '—'}
          <span style="color:var(--muted);font-size:9px;margin-left:6px">(${metric.match_count} matches)</span>
        </div>
      </div>`).join('');
  } catch (e) {
    const loadingEl = document.getElementById('custom-metrics-loading');
    if (loadingEl) loadingEl.textContent = 'failed';
  }
}

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

function _statsEsc(s) {
  return (s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
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
    const res = await fetchWithTimeout(`/api/cancel/${cluster}/${jobId}`, { method: 'POST' }, 15000);
    const d = await res.json();
    if (d.status === 'ok') { t.done(`Cancelled ${jobId}`); }
    else { t.done(d.error, 'error'); return; }
  } catch { t.done('Cancel failed', 'error'); return; }
  refreshCluster(cluster, true);
}

async function cancelGroup(cluster, jobIdsJson, groupName) {
  const jobIds = JSON.parse(jobIdsJson);
  return _doCancelGroup(cluster, jobIds, groupName);
}

async function cancelGroupByKey(cancelKey, groupName) {
  const ids = (window._cancelGroupIds || {})[cancelKey];
  if (!ids || !ids.length) { alert('No jobs to cancel'); return; }
  const cluster = cancelKey.split(':')[0];
  return _doCancelGroup(cluster, ids, groupName);
}

async function _doCancelGroup(cluster, jobIds, groupName) {
  if (!confirm(`Cancel ${jobIds.length} job${jobIds.length !== 1 ? 's' : ''} in "${groupName}" on ${cluster}?`)) return;
  const t = toastLoading(`Cancelling ${jobIds.length} job${jobIds.length !== 1 ? 's' : ''}…`);
  try {
    const res = await fetchWithTimeout(`/api/cancel_jobs/${cluster}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ job_ids: jobIds }),
    }, 30000);
    const d = await res.json();
    if (d.status === 'ok') { t.done(`Cancelling ${d.cancelled} job${d.cancelled !== 1 ? 's' : ''} in ${groupName}`); }
    else if (d.status === 'partial') { t.done(`Cancelled ${d.cancelled} jobs, ${d.errors.length} failed`, 'error'); }
    else { t.done(d.error, 'error'); return; }
  } catch { t.done('Cancel group failed', 'error'); return; }
  refreshCluster(cluster, true);
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
      const backoff = typeof _fetchFailCount !== 'undefined' && _fetchFailCount > 0
        ? Math.min(_fetchFailCount * 2, 10)
        : 1;
      countdown = Math.round(refreshIntervalSec * backoff);
      fetchAll();
      if (typeof currentTab !== 'undefined' && currentTab === 'clusters') {
        refreshPppAllocations();
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
  _setupSettingsAutoSave();
}

// ── Autosave ──────────────────────────────────────────────────────────────────
// Settings are persisted on input blur (text/number) and value change
// (checkbox/select). One delegated listener pair is installed on the modal;
// it dispatches to the right save function based on the input's section and
// id. Per-section debouncing coalesces rapid edits into a single POST.

const _autoSaveTimers = {};
const _LOCAL_SETTINGS_IDS = new Set([
  'set-autorefresh', 'set-refresh-interval',
  'set-hist-pagesize', 'set-jsonl-limit', 'set-jsonl-mode',
]);

function _autoSaveDebounce(key, fn, ms = 500) {
  clearTimeout(_autoSaveTimers[key]);
  _autoSaveTimers[key] = setTimeout(fn, ms);
}

function _getInputValue(el) {
  if (!el) return '';
  if (el.type === 'checkbox') return el.checked ? '1' : '0';
  return el.value || '';
}

function _setupSettingsAutoSave() {
  const root = document.getElementById('settings-overlay');
  if (!root || root.dataset.autosaveBound === '1') return;
  root.dataset.autosaveBound = '1';

  let _focusValue = null;
  root.addEventListener('focusin', (ev) => {
    const t = ev.target;
    if (t.matches && t.matches('input, select, textarea')) {
      _focusValue = _getInputValue(t);
    }
  }, true);
  root.addEventListener('focusout', (ev) => {
    const t = ev.target;
    if (!(t.matches && t.matches('input, select, textarea'))) return;
    if (t.type === 'checkbox' || t.tagName === 'SELECT') return;
    const after = _getInputValue(t);
    if (after === _focusValue) return;
    _focusValue = null;
    _routeAutoSave(t);
  }, true);
  root.addEventListener('change', (ev) => {
    const t = ev.target;
    if (t.matches && t.matches('input[type="checkbox"], select')) {
      _routeAutoSave(t);
    }
  }, true);
}

function _routeAutoSave(input) {
  if (input.id && _LOCAL_SETTINGS_IDS.has(input.id)) {
    _autoSaveDebounce('local', () => { saveLocalSettings(); toast('Saved'); }, 200);
    return;
  }
  const section = input.closest('.settings-section');
  if (!section) return;
  switch (section.id) {
    case 'sec-profile':
      _autoSaveDebounce('profile', saveProfile);
      break;
    case 'sec-clusters':
      if (input.closest('#gpu-alloc-editor')) {
        _autoSaveDebounce('alloc', saveGpuAllocations);
      } else if (input.closest('#cluster-editor')) {
        _autoSaveDebounce('clusters', saveClusters);
      }
      break;
    case 'sec-projects': {
      const card = input.closest('.cluster-edit-card');
      if (!card) return;
      const key = 'proj-' + (card.dataset.originalName || card.dataset.cardId || (card.dataset.cardId = String(Date.now() + Math.random())));
      _autoSaveDebounce(key, () => _saveProjectCard(card));
      break;
    }
    case 'sec-advanced':
      if (['set-ssh-timeout', 'set-cache-fresh', 'set-stats-interval',
           'set-backup-interval', 'set-backup-max'].includes(input.id)) {
        _autoSaveDebounce('advanced', saveAdvancedSettings);
      } else if (input.id === 'set-bg-suffixes') {
        _autoSaveDebounce('bg-suffixes', saveBgSuffixes);
      } else if (['set-proc-include', 'set-proc-exclude'].includes(input.id)) {
        _autoSaveDebounce('proc-filters', saveProcessFilters);
      }
      break;
  }
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
    await loadProjectEditor();
  } catch (e) {
    toast('Failed to load settings', 'error');
  }
}

async function loadProjectEditor() {
  try {
    const res = await fetch('/api/projects/all');
    const projects = await res.json();
    renderProjectEditor(Array.isArray(projects) ? projects : []);
  } catch (e) {
    toast('Failed to load projects', 'error');
  }
}

function renderPppEditor(ppps) {
  const el = document.getElementById('ppp-editor');
  el.innerHTML = Object.entries(ppps).map(([name, pid]) => `
    <div class="cluster-edit-card">
      <div class="ce-head">
        <span class="ce-name">${name}</span>
        <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove(); saveProfile();" title="remove">✕</button>
      </div>
      <div class="ce-fields">
        <div class="ce-field"><span>PPP name</span><input data-f="ppp-name" value="${name}"></div>
        <div class="ce-field"><span>Project ID</span><input data-f="ppp-id" type="number" value="${pid}"></div>
      </div>
    </div>
  `).join('');
}

function renderGpuAllocEditor(allocs) {
  const el = document.getElementById('gpu-alloc-editor');
  const clusterNames = Object.keys(CLUSTERS).filter(c => c !== 'local').sort();
  if (!clusterNames.length) {
    el.innerHTML = '<div class="set-help">No clusters configured</div>';
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
  // v4: each cluster owns its team_gpu_alloc field on the cluster row.
  // Iterate the inputs, PUT each cluster individually with its new value.
  const team_gpu_allocations = _readGpuAllocations();
  let failed = 0;
  for (const [cluster, alloc] of Object.entries(team_gpu_allocations)) {
    try {
      const res = await fetch(`/api/clusters/${encodeURIComponent(cluster)}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ team_gpu_alloc: String(alloc) }),
      });
      const d = await res.json();
      if (d.status !== 'ok') failed++;
    } catch { failed++; }
  }
  // Clusters that no longer have a row in the inputs need their alloc cleared.
  // The inputs always cover every active cluster so this branch is rare.
  if (failed === 0) {
    toast('GPU allocations saved');
    _teamGpuAlloc = team_gpu_allocations;
    if (typeof currentTab !== 'undefined' && currentTab === 'clusters') {
      refreshPppAllocations(true);
    }
  } else {
    toast(`Save failed for ${failed} cluster(s)`, 'error');
  }
}

function addPppRow() {
  const el = document.getElementById('ppp-editor');
  const div = document.createElement('div');
  div.className = 'cluster-edit-card';
  div.innerHTML = `
    <div class="ce-head">
      <span class="ce-name">new PPP</span>
      <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove(); saveProfile();" title="remove">✕</button>
    </div>
    <div class="ce-fields">
      <div class="ce-field"><span>PPP name</span><input data-f="ppp-name" value="" placeholder="team_project_..."></div>
      <div class="ce-field"><span>Project ID</span><input data-f="ppp-id" type="number" value="" placeholder="12345"></div>
    </div>
  `;
  el.appendChild(div);
  setTimeout(() => div.querySelector('[data-f="ppp-name"]').focus(), 30);
}

async function saveProfile() {
  // v4: team_name is an app_setting; PPP accounts have their own table.
  const team = document.getElementById('set-team').value.trim();
  const cards = document.querySelectorAll('#ppp-editor .cluster-edit-card');
  const wantedPpps = new Map();  // name -> ppp_id (string)
  for (const card of cards) {
    const name = (card.querySelector('[data-f="ppp-name"]').value || '').trim();
    const pid = (card.querySelector('[data-f="ppp-id"]').value || '').trim();
    if (name) wantedPpps.set(name, pid);
  }
  try {
    // 1) Update team name.
    await fetch('/api/settings/team_name', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ value: team }),
    });

    // 2) Reconcile PPP accounts. Fetch current, drop missing, upsert wanted.
    const existing = await fetch('/api/team/ppps').then(r => r.json());
    const existingNames = new Set((existing || []).map(a => a.name));
    for (const oldName of existingNames) {
      if (!wantedPpps.has(oldName)) {
        await fetch(`/api/team/ppps/${encodeURIComponent(oldName)}`, { method: 'DELETE' });
      }
    }
    for (const [name, pid] of wantedPpps) {
      if (existingNames.has(name)) {
        await fetch(`/api/team/ppps/${encodeURIComponent(name)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ppp_id: pid }),
        });
      } else {
        await fetch('/api/team/ppps', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name, ppp_id: pid }),
        });
      }
    }

    toast('Profile saved');
    if (typeof currentTab !== 'undefined' && currentTab === 'clusters') {
      refreshPppAllocations(true);
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
        <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove(); saveClusters();" title="remove">✕</button>
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
      <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove(); saveClusters();" title="remove">✕</button>
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
  setTimeout(() => div.querySelector('[data-f="name"]').focus(), 30);
}

async function saveClusters() {
  // v4: each cluster is a row in the clusters table. Reconcile by
  // diffing the form against the existing rows and POST/PUT/DELETE as
  // needed so partial failures only affect their own cluster.
  const cards = document.querySelectorAll('#cluster-editor .cluster-edit-card');
  const wanted = new Map();  // name -> {host, port, gpu_type, mount_paths}
  for (const card of cards) {
    const name = (card.querySelector('[data-f="name"]').value || '').trim();
    if (!name) continue;
    const mpRaw = (card.querySelector('[data-f="mount_paths"]').value || '').trim();
    const mountPaths = mpRaw ? mpRaw.split('\n').map(s => s.trim()).filter(Boolean) : [];
    wanted.set(name, {
      host: card.querySelector('[data-f="host"]').value.trim(),
      port: parseInt(card.querySelector('[data-f="port"]').value) || 22,
      gpu_type: card.querySelector('[data-f="gpu_type"]').value.trim(),
      mount_paths: mountPaths,
    });
  }

  let failed = 0;
  try {
    const existing = await fetch('/api/clusters').then(r => r.json());
    const existingNames = new Set((existing || []).map(c => c.name));

    for (const oldName of existingNames) {
      if (!wanted.has(oldName)) {
        const res = await fetch(`/api/clusters/${encodeURIComponent(oldName)}`, { method: 'DELETE' });
        if (!res.ok) failed++;
      }
    }
    for (const [name, body] of wanted) {
      if (existingNames.has(name)) {
        const res = await fetch(`/api/clusters/${encodeURIComponent(name)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        if (!res.ok) failed++;
      } else {
        const res = await fetch('/api/clusters', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ name, ...body }),
        });
        if (!res.ok) failed++;
      }
    }
    if (failed === 0) {
      toast('Clusters saved');
      fetchAll();
    } else {
      toast(`Save failed for ${failed} cluster(s)`, 'error');
    }
  } catch (e) {
    toast('Save failed', 'error');
  }
}

function _prefixesToString(prefixes) {
  if (!Array.isArray(prefixes)) return '';
  return prefixes.map(p => (p && p.prefix) || '').filter(Boolean).join(', ');
}

function _stringToPrefixes(text, originalPrefixes) {
  const wanted = (text || '').split(',').map(s => s.trim()).filter(Boolean);
  const origByPrefix = {};
  for (const p of (originalPrefixes || [])) {
    if (p && p.prefix) origByPrefix[p.prefix] = p;
  }
  return wanted.map(prefix => {
    const orig = origByPrefix[prefix];
    if (orig && orig.default_campaign) {
      return { prefix, default_campaign: orig.default_campaign };
    }
    return { prefix };
  });
}

function renderProjectEditor(projects) {
  const el = document.getElementById('project-editor');
  el.innerHTML = projects.map(p => {
    const name = p.name || '';
    const color = p.color || '#e8f4fd';
    const emoji = p.emoji || '📁';
    const prefixesText = _prefixesToString(p.prefixes);
    const description = p.description || '';
    const originalJson = JSON.stringify({
      color, emoji, prefixes: p.prefixes || [], description,
    }).replace(/"/g, '&quot;');
    return `
    <div class="cluster-edit-card" data-project="${name}" data-original-name="${name}" data-original="${originalJson}">
      <div class="ce-head">
        <span class="ce-name" style="display:flex;align-items:center;gap:6px">
          <span style="font-size:16px">${emoji}</span>
          <span class="project-color-dot" style="background:${color}"></span>${name}
        </span>
        <button class="ce-remove" data-action="delete-project" title="delete">✕</button>
      </div>
      <div class="ce-fields">
        <div class="ce-field"><span>Name</span><input data-f="name" value="${name}"></div>
        <div class="ce-field"><span>Prefixes</span><input data-f="prefixes" value="${prefixesText}" placeholder="name_, alias_"></div>
        <div class="ce-field"><span>Emoji</span><input data-f="emoji" value="${emoji}" placeholder="🔬" style="width:40px;text-align:center"></div>
        <div class="ce-field"><span>Color</span><span class="color-pair"><input data-f="color" type="color" value="${color}" style="width:28px;height:28px;padding:0;border:none;cursor:pointer" oninput="this.nextElementSibling.value=this.value"><input data-f="color-hex" type="text" value="${color}" style="width:70px" placeholder="#e8f4fd" oninput="const c=this.previousElementSibling;if(/^#[0-9a-fA-F]{6}$/.test(this.value))c.value=this.value"></span></div>
        <div class="ce-field"><span>Description</span><input data-f="description" value="${description.replace(/"/g, '&quot;')}" placeholder="optional"></div>
      </div>
    </div>
  `;
  }).join('');

  el.querySelectorAll('[data-action="delete-project"]').forEach(btn => {
    btn.addEventListener('click', async (ev) => {
      const card = ev.target.closest('.cluster-edit-card');
      const origName = card.dataset.originalName || '';
      if (origName) {
        if (!confirm(`Delete project "${origName}"? Job history is preserved but the project will disappear from the sidebar.`)) return;
        try {
          const res = await fetch(`/api/projects/${encodeURIComponent(origName)}`, { method: 'DELETE' });
          const d = await res.json();
          if (d.status !== 'ok') {
            toast(d.error || 'Delete failed', 'error');
            return;
          }
          toast(`Deleted ${origName}`);
        } catch (e) {
          toast('Delete failed', 'error');
          return;
        }
      }
      card.remove();
      _projectColors = null;
      if (typeof loadProjectButtons === 'function') loadProjectButtons();
    });
  });
}

function addProjectRow() {
  const el = document.getElementById('project-editor');
  const div = document.createElement('div');
  div.className = 'cluster-edit-card';
  div.dataset.originalName = '';
  div.dataset.original = '';
  div.innerHTML = `
    <div class="ce-head">
      <span class="ce-name">new project</span>
      <button class="ce-remove" data-action="delete-project" title="remove">✕</button>
    </div>
    <div class="ce-fields">
      <div class="ce-field"><span>Name</span><input data-f="name" value="" placeholder="my-project"></div>
      <div class="ce-field"><span>Prefixes</span><input data-f="prefixes" value="" placeholder="my-project_"></div>
      <div class="ce-field"><span>Emoji</span><input data-f="emoji" value="" placeholder="🔬" style="width:40px;text-align:center"></div>
      <div class="ce-field"><span>Color</span><span class="color-pair"><input data-f="color" type="color" value="#e8f4fd" style="width:28px;height:28px;padding:0;border:none;cursor:pointer" oninput="this.nextElementSibling.value=this.value"><input data-f="color-hex" type="text" value="#e8f4fd" style="width:70px" placeholder="#e8f4fd" oninput="const c=this.previousElementSibling;if(/^#[0-9a-fA-F]{6}$/.test(this.value))c.value=this.value"></span></div>
      <div class="ce-field"><span>Description</span><input data-f="description" value="" placeholder="optional"></div>
    </div>
  `;
  div.querySelector('[data-action="delete-project"]').addEventListener('click', () => div.remove());
  el.appendChild(div);
  div.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  const nameInput = div.querySelector('[data-f="name"]');
  if (nameInput) {
    setTimeout(() => nameInput.focus(), 50);
    nameInput.addEventListener('input', () => {
      const prefixInput = div.querySelector('[data-f="prefixes"]');
      if (prefixInput && !prefixInput.value) {
        const v = nameInput.value.trim().toLowerCase();
        if (v) prefixInput.value = `${v}_`;
      }
    });
  }
}

function _readProjectCard(card) {
  const name = (card.querySelector('[data-f="name"]').value || '').trim().toLowerCase();
  const hexInput = card.querySelector('[data-f="color-hex"]');
  const pickerInput = card.querySelector('[data-f="color"]');
  const color = (hexInput && /^#[0-9a-fA-F]{6}$/.test(hexInput.value.trim()))
    ? hexInput.value.trim()
    : (pickerInput ? pickerInput.value.trim() : '#e8f4fd');
  const emoji = card.querySelector('[data-f="emoji"]').value.trim();
  const prefixesText = card.querySelector('[data-f="prefixes"]').value.trim();
  const description = card.querySelector('[data-f="description"]').value.trim();
  let original = {};
  try { original = JSON.parse(card.dataset.original || '{}'); } catch (e) { original = {}; }
  const prefixes = _stringToPrefixes(prefixesText, original.prefixes || []);
  return { name, color, emoji, prefixesText, prefixes, description, original };
}

async function _saveProjectCard(card) {
  const originalName = card.dataset.originalName || '';
  const { name, color, emoji, prefixesText, prefixes, description, original } = _readProjectCard(card);
  if (!name) return;

  try {
    let res;
    if (!originalName) {
      // Brand-new card: POST only when both name and at least one prefix are filled.
      if (!prefixesText) return;
      res = await fetch('/api/projects', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, color, emoji, prefixes, description }),
      });
      const d = await res.json();
      if (d.status === 'ok') {
        toast(`Created ${name}`);
        await loadProjectEditor();
        _projectColors = null;
        if (typeof loadProjectButtons === 'function') loadProjectButtons();
        if (typeof fetchAll === 'function') fetchAll();
      } else {
        toast(`${name}: ${d.error || 'create failed'}`, 'error');
      }
    } else {
      if (name !== originalName) {
        toast(`Renaming projects isn't supported — delete and re-create`, 'error');
        card.querySelector('[data-f="name"]').value = originalName;
        return;
      }
      const patch = {};
      if (color !== original.color) patch.color = color;
      if (emoji !== original.emoji) patch.emoji = emoji;
      if (description !== (original.description || '')) patch.description = description;
      if (prefixesText !== _prefixesToString(original.prefixes)) patch.prefixes = prefixes;
      if (Object.keys(patch).length === 0) return;
      res = await fetch(`/api/projects/${encodeURIComponent(originalName)}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(patch),
      });
      const d = await res.json();
      if (d.status === 'ok') {
        toast(`Saved ${name}`);
        card.dataset.original = JSON.stringify({
          color: d.project.color, emoji: d.project.emoji,
          prefixes: d.project.prefixes, description: d.project.description,
        });
        _projectColors = null;
        if (typeof loadProjectButtons === 'function') loadProjectButtons();
        if (typeof fetchAll === 'function') fetchAll();
      } else {
        toast(`${name}: ${d.error || 'save failed'}`, 'error');
      }
    }
  } catch (e) {
    toast(`${name}: save failed`, 'error');
  }
}

async function saveAdvancedSettings() {
  // v4: each app_setting key has its own /api/settings/<key> endpoint.
  const updates = {
    ssh_timeout: parseInt(document.getElementById('set-ssh-timeout').value) || 8,
    cache_fresh_sec: parseInt(document.getElementById('set-cache-fresh').value) || 30,
    stats_interval_sec: parseInt(document.getElementById('set-stats-interval').value) || 1800,
    backup_interval_hours: parseInt(document.getElementById('set-backup-interval').value) || 24,
    backup_max_keep: parseInt(document.getElementById('set-backup-max').value) || 7,
  };
  let failed = 0;
  for (const [key, value] of Object.entries(updates)) {
    try {
      const res = await fetch(`/api/settings/${key}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value }),
      });
      if (!res.ok) failed++;
    } catch { failed++; }
  }
  if (failed === 0) toast('Advanced settings saved');
  else toast(`Save failed for ${failed} setting(s)`, 'error');
}

async function saveProcessFilters() {
  // v4: process filters live in their own table with one row per pattern.
  // Reconcile by diffing the form against the stored patterns.
  const inc = document.getElementById('set-proc-include').value.split(',').map(s => s.trim()).filter(Boolean);
  const exc = document.getElementById('set-proc-exclude').value.split(',').map(s => s.trim()).filter(Boolean);

  async function reconcile(mode, wanted) {
    const existing = await fetch(`/api/process_filters/${mode}`).then(r => r.json());
    const existingPatterns = new Set((existing || []).map(f => f.pattern));
    const wantedSet = new Set(wanted);
    for (const p of existingPatterns) {
      if (!wantedSet.has(p)) {
        await fetch(`/api/process_filters/${mode}`, {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ pattern: p }),
        });
      }
    }
    for (const p of wanted) {
      if (!existingPatterns.has(p)) {
        await fetch(`/api/process_filters/${mode}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ pattern: p }),
        });
      }
    }
  }

  try {
    await reconcile('include', inc);
    await reconcile('exclude', exc);
    toast('Process filters saved');
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

// Restore tabs across refreshes, prefer hash over localStorage
(function restoreTab() {
  const hash = location.hash.replace(/^#\/?/, '');
  if (hash) {
    if (!_restoreTabs()) _renderAppTabs();
    _hashNavigating = false;
    _onHashChange();
  } else if (!_restoreTabs()) {
    _renderAppTabs();
    showTab('live');
  }
})();

if (refreshIntervalSec > 0) startCountdown();
