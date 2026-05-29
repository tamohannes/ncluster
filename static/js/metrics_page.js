// =====================================================================
// Metrics Explorer — Aimstack-style metric explorer for clausius
// ---------------------------------------------------------------------
// Sections:
//   1.  State + palette
//   2.  URL / saved-view encoding
//   3.  Run loading + record normalization
//   4.  AimQL parser + evaluator
//   5.  Grouping engine
//   6.  Top-level render
//   7.  Top bar / saved views / runs select
//   8.  Select form (metric chooser + query) and grouping row
//   9.  Chart area (subplots + zoom + highlight)
//  10.  Right controls rail
//  11.  Context table + splitter
//  12.  Saved views API
// =====================================================================

// ─── 1. State + palette ───────────────────────────────────────────────

// Sophisticated 20-color trace palette — based on Tableau 10 + 10 lighter
// companion shades. Slightly desaturated relative to Aim's primary palette
// so it reads as professional/calm on both light and dark backgrounds while
// still keeping enough hue separation between adjacent groups.
const METRICS_PALETTE = [
  // Tableau-10 base hues, lightly polished.
  '#4E79A7', // dusky blue
  '#F28E2B', // warm orange
  '#59A14F', // forest green
  '#E15759', // coral red
  '#B07AA1', // muted plum
  '#76B7B2', // sage teal
  '#EDC948', // soft amber
  '#FF9DA7', // dusty rose
  '#9C755F', // earthen brown
  '#79706E', // graphite
  // Lighter companions — used after the first 10 distinct groups.
  '#A0CBE8', // pale blue
  '#FFBE7D', // pale orange
  '#8CD17D', // pale green
  '#FF9D9A', // pale red
  '#D4A6C8', // pale plum
  '#86BCB6', // pale teal
  '#F1CE63', // pale amber
  '#FABFD2', // pale pink
  '#D7B5A6', // pale brown
  '#BAB0AC', // pale gray
];

// Field paths the grouping/QL UI offers. Each entry is { id, label, kind }.
// `kind`: 'metric' (per-record), 'run' (per-run), 'context' (per-record context key)
const METRICS_GROUPING_FIELDS_BASE = [
  { id: 'run.hash', label: 'Run hash', kind: 'run' },
  { id: 'run.name', label: 'Run name', kind: 'run' },
  { id: 'run.cluster', label: 'Cluster', kind: 'run' },
  { id: 'metric.name', label: 'Metric name', kind: 'metric' },
  { id: 'metric.kind', label: 'Metric kind', kind: 'metric' },
];

let _mpState = {
  // Selected runs
  runs: [],
  runData: {},
  runInput: '',

  // Records derived from runData (one per run × metric)
  records: [],

  // Selected metrics
  selectedMetrics: [],

  // Query
  query: '',

  // Grouping. Default `chart: ['metric.name']` matches Aim's metric explorer
  // default — each selected metric gets its own subplot. Users can clear it
  // via Group By → Chart → uncheck. ``pattern`` overlays a distinct fill
  // pattern (diagonal stripes, crosshatch, dots, …) on each group value,
  // visible on top of the trace color so groups stay distinguishable even
  // when colors collide on small palettes.
  grouping: {
    color: ['run.hash'],
    chart: ['metric.name'],
    pattern: [],
  },

  // Per-trace label fields. Controls what's shown for each run on the chart
  // y-axis, in tooltips, and in the right-rail Runs legend. Default
  // `['run.name']` reproduces the prior behavior; setting `['run.hparams.model']`
  // shows the model name instead.
  traceLabelFields: ['run.name'],

  // Axes
  align: 'step',
  yScale: 'linear',
  xRange: null,
  yRange: null,

  // Smoothing
  smoothing: 0.25,
  showRaw: false,

  // Outliers
  ignoreOutliers: false,

  // Highlight mode: 'off' | 'metric' | 'run'
  highlightMode: 'metric',

  // Hidden trace ids
  hiddenTraces: {},

  // Splitter ratio (charts fraction; rest is table)
  splitRatio: 0.65,

  // Context table visibility toggle
  tableOpen: true,

  // Pivot "Table view" modal (rows = runs, columns = metric × context)
  tableViewOpen: false,
  // Active column sort for the table view: { col: <key|null>, dir: 'desc'|'asc' }
  tableViewSort: { col: null, dir: 'desc' },

  // User-defined ordering of chart subplots (array of chart-group keys).
  // Empty array means "natural" first-seen order. Persisted in URL + saved
  // views so a layout the user has dialed in stays put.
  chartOrder: [],

  // Right-rail width (px). Resizable via drag-handle on its left edge.
  rightRailWidth: 300,

  // Saved views
  savedViews: [],
  activeViewId: null,

  // Run suggestion state
  suggestions: [],
  suggestionTarget: '',
  suggestionIndex: 0,

  // Metric-select dropdown
  metricsOpen: false,
  metricsFilter: '',

  // Chart.js instances
  charts: [],

  // Hover highlight state
  hoveredTrace: null,
  hoveredChartId: '',

  // Load sequence (cancellation)
  loadSeq: 0,
};

let _mpChartTimer = null;
let _mpDragZoom = null;
let _mpSplitDrag = null;

// AimQL autocomplete state. The dropdown lives below the search input and
// surfaces field paths, methods, operators, live run names, metric names, and
// context values based on the cursor position.
let _mpAC = {
  open: false,
  items: [],
  index: 0,
  replaceStart: 0,
  replaceEnd: 0,
  cache: { runNames: { /* query -> [results] */ }, metricNames: [], contextValues: {} },
  loadingToken: 0,
};

// ─── 2. URL / saved-view encoding ─────────────────────────────────────

function openMetricsPage(fromTab = false) {
  if (typeof _activateView === 'function') _activateView('metrics');
  _mpReadUrl();
  // If the URL has no metrics state (e.g. user clicked the Metrics nav tab
  // after working in another tab), fall back to the locally-persisted state
  // so the last query / runs / grouping / saved-view are restored.
  const urlIsBare = !_mpState.runs.length
    && !_mpState.query
    && !_mpState.activeViewId
    && (!_mpState.selectedMetrics || !_mpState.selectedMetrics.length);
  const restoredFromLs = urlIsBare && _mpRestoreFromLocalStorage();
  if (restoredFromLs) {
    // Reflect the restored state in the URL so reloads / shares work too.
    const next = `/metrics${_mpCurrentQuery()}`;
    if (`${location.pathname}${location.search}` !== next) {
      history.replaceState(null, '', next);
    }
  }
  if (!fromTab && typeof _appTabs !== 'undefined') {
    const at = _appTabs.find(t => t.id === _activeTabId);
    if (at) {
      at.type = 'metrics';
      at.label = 'Metrics';
      at.project = null;
    }
    if (typeof _renderAppTabs === 'function') _renderAppTabs();
    if (typeof _persistTabs === 'function') _persistTabs();
    if (typeof _setHash === 'function') _setHash(`/metrics${_mpCurrentQuery()}`);
  }
  _mpRender();
  _mpLoadSavedViews();
  // If the URL pointed at a saved view (?view=N) we re-fetch it from the
  // server so the freshest server-side state wins. Conversely, when we
  // restored from localStorage we already have the user's last in-memory
  // state — skip the network round-trip so they don't lose unsaved tweaks.
  if (_mpState.activeViewId && !restoredFromLs) {
    _mpOpenSavedView(_mpState.activeViewId);
    return;
  }
  if (_mpState.runs.length) _mpLoadRuns();
}

function _mpCurrentQuery() {
  const p = new URLSearchParams();
  if (_mpState.runs.length) p.set('runs', _mpState.runs.map(r => `${r.cluster}/${r.runHash}`).join(','));
  if (_mpState.selectedMetrics.length) p.set('metrics', _mpState.selectedMetrics.map(encodeURIComponent).join(','));
  if (_mpState.query) p.set('q', _mpState.query);
  if (_mpState.grouping.color.length && !(_mpState.grouping.color.length === 1 && _mpState.grouping.color[0] === 'run.hash')) {
    p.set('gc', _mpState.grouping.color.join(','));
  }
  // For chart grouping, serialize only when it differs from the default
  // (`['metric.name']`). Empty array (user cleared) is encoded as `gp=__none`.
  const defaultChart = JSON.stringify(['metric.name']);
  const curChart = JSON.stringify(_mpState.grouping.chart);
  if (curChart !== defaultChart) {
    p.set('gp', _mpState.grouping.chart.length ? _mpState.grouping.chart.join(',') : '__none');
  }
  if (_mpState.grouping.pattern && _mpState.grouping.pattern.length) {
    p.set('gpat', _mpState.grouping.pattern.join(','));
  }
  if (_mpState.chartOrder && _mpState.chartOrder.length) {
    // Chart-group keys can contain `=`, `,`, etc. Base64-encode the JSON
    // payload so the URL stays well-formed regardless of the user's
    // grouping field values.
    try {
      p.set('order', btoa(unescape(encodeURIComponent(JSON.stringify(_mpState.chartOrder)))));
    } catch (_) { /* ignore */ }
  }
  // Label-by fields: serialize when not the default `['run.name']`.
  const defaultLabel = JSON.stringify(['run.name']);
  const curLabel = JSON.stringify(_mpState.traceLabelFields);
  if (curLabel !== defaultLabel) {
    p.set('lf', _mpState.traceLabelFields.length ? _mpState.traceLabelFields.join(',') : '__none');
  }
  if (_mpState.align !== 'step') p.set('align', _mpState.align);
  if (_mpState.yScale !== 'linear') p.set('yscale', _mpState.yScale);
  if (_mpState.smoothing !== 0.25) p.set('smooth', String(_mpState.smoothing));
  if (_mpState.showRaw) p.set('raw', '1');
  if (_mpState.ignoreOutliers) p.set('outliers', '1');
  if (_mpState.highlightMode !== 'metric') p.set('hl', _mpState.highlightMode);
  if (_mpState.splitRatio !== 0.65) p.set('split', _mpState.splitRatio.toFixed(2));
  if (_mpState.tableOpen === false) p.set('table', '0');
  if (_mpState.rightRailWidth !== 300) p.set('rrw', String(Math.round(_mpState.rightRailWidth)));
  if (_mpState.xRange) p.set('xr', _mpState.xRange.join(':'));
  if (_mpState.yRange) p.set('yr', _mpState.yRange.join(':'));
  if (_mpState.activeViewId) p.set('view', String(_mpState.activeViewId));
  const text = p.toString();
  return text ? `?${text}` : '';
}

function _mpReplaceUrl() {
  const next = `/metrics${_mpCurrentQuery()}`;
  if (`${location.pathname}${location.search}` !== next) history.replaceState(null, '', next);
  _mpPersistToLocalStorage();
}

// Persist + restore the page state to localStorage so the explorer survives
// tab switches and page reloads even when the URL is bare `/metrics`.
const _MP_LS_KEY = 'clausius.metrics_page.state';

function _mpPersistToLocalStorage() {
  try {
    const payload = { ..._mpSerializeState(), activeViewId: _mpState.activeViewId };
    localStorage.setItem(_MP_LS_KEY, JSON.stringify(payload));
  } catch (_) {}
}

function _mpRestoreFromLocalStorage() {
  try {
    const raw = localStorage.getItem(_MP_LS_KEY);
    if (!raw) return false;
    const state = JSON.parse(raw);
    if (!state || typeof state !== 'object') return false;
    _mpApplySerialized(state);
    if (Number.isFinite(state.activeViewId)) _mpState.activeViewId = state.activeViewId;
    return true;
  } catch (_) { return false; }
}

function _mpReadUrl() {
  const raw = location.hash.startsWith('#/')
    ? (location.hash.split('?')[1] || '')
    : location.search.replace(/^\?/, '');
  const p = new URLSearchParams(raw);
  _mpState.runs = parseMetricsRunRefs(p.get('runs') || '');
  _mpState.selectedMetrics = (p.get('metrics') || '')
    .split(',').map(decodeURIComponent).map(s => s.trim()).filter(Boolean);
  _mpState.query = p.get('q') || '';
  const gc = (p.get('gc') || '').split(',').map(s => s.trim()).filter(Boolean);
  _mpState.grouping.color = gc.length ? gc : ['run.hash'];
  // Chart grouping defaults to `['metric.name']`. `gp=__none` means user
  // explicitly cleared it.
  const gpRaw = p.get('gp');
  if (gpRaw === null) _mpState.grouping.chart = ['metric.name'];
  else if (gpRaw === '__none' || gpRaw === '') _mpState.grouping.chart = [];
  else _mpState.grouping.chart = gpRaw.split(',').map(s => s.trim()).filter(Boolean);
  _mpState.grouping.pattern = (p.get('gpat') || '').split(',').map(s => s.trim()).filter(Boolean);
  const orderRaw = p.get('order');
  if (orderRaw) {
    try {
      const decoded = JSON.parse(decodeURIComponent(escape(atob(orderRaw))));
      _mpState.chartOrder = Array.isArray(decoded) ? decoded.filter(k => typeof k === 'string') : [];
    } catch (_) { _mpState.chartOrder = []; }
  } else {
    _mpState.chartOrder = [];
  }
  const lfRaw = p.get('lf');
  if (lfRaw === null) _mpState.traceLabelFields = ['run.name'];
  else if (lfRaw === '__none' || lfRaw === '') _mpState.traceLabelFields = [];
  else _mpState.traceLabelFields = lfRaw.split(',').map(s => s.trim()).filter(Boolean);
  _mpState.align = p.get('align') || 'step';
  _mpState.yScale = p.get('yscale') || 'linear';
  _mpState.smoothing = Math.max(0, Math.min(0.95, parseFloat(p.get('smooth') || '0.25') || 0));
  _mpState.showRaw = p.get('raw') === '1';
  _mpState.ignoreOutliers = p.get('outliers') === '1';
  const hl = (p.get('hl') || 'metric').toLowerCase();
  _mpState.highlightMode = ['off', 'metric', 'run'].includes(hl) ? hl : 'metric';
  const split = parseFloat(p.get('split') || '0.65');
  _mpState.splitRatio = Number.isFinite(split) ? Math.max(0.2, Math.min(0.9, split)) : 0.65;
  _mpState.tableOpen = p.get('table') !== '0';
  const rrw = parseInt(p.get('rrw') || '300', 10);
  _mpState.rightRailWidth = Number.isFinite(rrw) ? Math.max(220, Math.min(640, rrw)) : 300;
  _mpState.xRange = _mpParseRange(p.get('xr'));
  _mpState.yRange = _mpParseRange(p.get('yr'));
  _mpState.activeViewId = p.get('view') ? parseInt(p.get('view'), 10) : null;
}

function _mpParseRange(text) {
  if (!text) return null;
  const parts = String(text).split(':').map(s => parseFloat(s));
  if (parts.length !== 2 || !Number.isFinite(parts[0]) || !Number.isFinite(parts[1])) return null;
  return [parts[0], parts[1]];
}

// Only accept refs that look like an actual cluster/run_hash or a bare hex
// run hash. This protects against the failure mode where someone pastes an
// AimQL query into the runs textarea — `metric.name == "..."` etc. — and
// each whitespace token gets ingested as a bogus run.
const _MP_HASH_RE = /^[0-9a-fA-F]{4,16}$/;
const _MP_CLUSTER_HASH_RE = /^[A-Za-z0-9_-]+\/[0-9a-fA-F]{4,16}$/;

function parseMetricsRunRefs(text) {
  const seen = new Set();
  return String(text || '')
    .split(/[,\n\s]+/).map(s => s.trim()).filter(Boolean)
    .map(ref => {
      if (!_MP_HASH_RE.test(ref) && !_MP_CLUSTER_HASH_RE.test(ref)) return null;
      const parts = ref.split(/[/:]+/).filter(Boolean);
      if (parts.length < 1) return null;
      const runHash = parts.pop();
      const cluster = parts.join(':');
      if (!runHash) return null;
      const key = `${cluster || '*'}/${runHash}`.toLowerCase();
      if (seen.has(key)) return null;
      seen.add(key);
      return { cluster, runHash };
    })
    .filter(Boolean);
}

function _mpRunKey(run) { return `${run.cluster}/${run.runHash}`; }
const _MP_DEFAULT_EXCLUDED_RUN_TAGS = ['smoke'];

function _mpQueryOptsIntoExcludedTags() {
  return String(_mpState.query || '').includes('run.tags');
}

function _mpRunHasDefaultExcludedTag(runInfo) {
  if (_mpQueryOptsIntoExcludedTags()) return false;
  const tags = typeof runTagsFromRun === 'function'
    ? runTagsFromRun(runInfo || {})
    : ((runInfo && runInfo.tags) || []);
  return tags.some(tag => _MP_DEFAULT_EXCLUDED_RUN_TAGS.includes(tag));
}

// ─── 3. Run loading + record normalization ────────────────────────────

async function _mpLoadRuns() {
  if (!_mpState.runs.length || document.hidden) return;
  const seq = ++_mpState.loadSeq;
  const body = document.getElementById('mp-body');
  if (body) body.innerHTML = '<div class="mp-empty">Loading selected runs…</div>';
  try {
    if (typeof loadRunTagDefinitions === 'function') {
      await loadRunTagDefinitions();
    }
    await _mpResolveRuns();
    // Each run loads independently. A 404 (run deleted, dedupe, stale saved
    // view, etc.) drops that one run from the list instead of taking down
    // the entire page — the user just sees the remaining runs rendered.
    const results = await Promise.all(_mpState.runs.map(async (run) => {
      try {
        const [infoRes, metricsRes] = await Promise.all([
          fetch(`/api/run_info_by_hash/${encodeURIComponent(run.cluster)}/${encodeURIComponent(run.runHash)}`),
          fetch(`/api/run_metrics_by_hash/${encodeURIComponent(run.cluster)}/${encodeURIComponent(run.runHash)}`),
        ]);
        const info = await infoRes.json();
        const metrics = await metricsRes.json();
        if (info.status !== 'ok' || !info.run) return { run, missing: true, reason: info.error || 'run not found' };
        if (_mpRunHasDefaultExcludedTag(info.run)) return { run, missing: true, reason: 'tagged smoke' };
        if (metrics.status !== 'ok') return { run, missing: true, reason: metrics.error || 'metrics not found' };
        return {
          ok: true,
          run: { cluster: run.cluster, runHash: info.run.run_hash || run.runHash },
          info: info.run,
          metrics: _mpNormalizeMetrics(metrics),
        };
      } catch (err) {
        return { run, missing: true, reason: String(err && err.message || err) };
      }
    }));
    if (seq !== _mpState.loadSeq) return;
    const payloads = results.filter(r => r.ok);
    const dropped = results.filter(r => r.missing);
    _mpState.runData = {};
    _mpState.runs = payloads.map(p => p.run);
    payloads.forEach(p => { _mpState.runData[_mpRunKey(p.run)] = p; });
    if (dropped.length) {
      console.warn(
        `metrics: dropped ${dropped.length} unresolved run(s) from the selection`,
        dropped.map(d => `${d.run.cluster}/${d.run.runHash}: ${d.reason}`),
      );
      // Persist the cleaned-up list so the URL no longer references the
      // missing hashes after this load.
      _mpReplaceUrl();
    }
    if (!payloads.length) {
      if (body) body.innerHTML = `<div class="mp-empty">None of the selected runs could be loaded${dropped.length ? ` (${dropped.length} dropped)` : ''}. Use AimQL or the runs picker to add runs.</div>`;
      return;
    }
    _mpBuildRecords();
    _mpApplyDefaultSelection();
    _mpRender();
  } catch (e) {
    if (seq !== _mpState.loadSeq) return;
    if (body) body.innerHTML = `<div class="mp-empty">${_escHtml(e.message || e)}</div>`;
  }
}

async function _mpResolveRuns() {
  const resolved = [];
  for (const run of _mpState.runs) {
    if (run.cluster) { resolved.push(run); continue; }
    const res = await fetch(`/api/resolve_run_hash/${encodeURIComponent(run.runHash)}`);
    const data = await res.json();
    if (data.status !== 'ok') throw new Error(data.error || `Could not resolve ${run.runHash}`);
    resolved.push({ cluster: data.cluster, runHash: data.run_hash || run.runHash });
  }
  const seen = new Set();
  _mpState.runs = resolved.filter(run => {
    const k = _mpRunKey(run).toLowerCase();
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
  _mpReplaceUrl();
}

function _mpNormalizeMetrics(payload) {
  return {
    metadata: payload.metadata && typeof payload.metadata === 'object' ? payload.metadata : {},
    series: payload.series && typeof payload.series === 'object' ? payload.series : {},
    scalars: payload.scalars && typeof payload.scalars === 'object' ? payload.scalars : {},
    scalar_latest: payload.scalar_latest && typeof payload.scalar_latest === 'object' ? payload.scalar_latest : {},
  };
}

function _mpBuildRecords() {
  // Per run × metric × context-signature: one record. We split a single
  // metric key into multiple records if its points carry different contexts,
  // so AimQL queries on `metric.context.subset == "train"` work correctly.
  const records = [];
  Object.values(_mpState.runData).forEach(payload => {
    const runHash = payload.run.runHash;
    const cluster = payload.run.cluster;
    const runName = (payload.info && (payload.info.run_name || payload.info.name)) || `Run ${runHash}`;
    const metadata = (payload.metrics && payload.metrics.metadata) || {};
    const project = payload.info && payload.info.project;
    const projectColor = payload.info && payload.info.project_color;
    const campaign = payload.info && payload.info.campaign;
    const params = (payload.info && payload.info.params) || {};
    const tags = typeof runTagsFromRun === 'function'
      ? runTagsFromRun(payload.info || {})
      : ((payload.info && payload.info.tags) || []);
    Object.entries(payload.metrics.series || {}).forEach(([key, points]) => {
      _mpSplitByContext(points || []).forEach(group => {
        records.push(_mpRecord({
          cluster, runHash, runName, project, projectColor, campaign, metadata, params, tags,
          key, kind: 'series', points: group.points, context: group.context,
        }));
      });
    });
    Object.entries(payload.metrics.scalars || {}).forEach(([key, points]) => {
      _mpSplitByContext(points || []).forEach(group => {
        records.push(_mpRecord({
          cluster, runHash, runName, project, projectColor, campaign, metadata, params, tags,
          key, kind: 'scalars', points: group.points, context: group.context,
        }));
      });
    });
  });
  _mpState.records = records;
}

function _mpSplitByContext(points) {
  // Group points by their context signature. If all points share the same
  // context, returns a single group. Otherwise splits.
  const groups = new Map();
  (points || []).forEach(p => {
    const ctx = p.context || {};
    const sig = JSON.stringify(_mpSortObj(ctx));
    if (!groups.has(sig)) groups.set(sig, { context: { ...ctx }, points: [] });
    groups.get(sig).points.push(p);
  });
  if (!groups.size) return [{ context: {}, points: [] }];
  return Array.from(groups.values());
}

function _mpSortObj(obj) {
  if (!obj || typeof obj !== 'object') return obj;
  const out = {};
  Object.keys(obj).sort().forEach(k => { out[k] = obj[k]; });
  return out;
}

function _mpRecord({ cluster, runHash, runName, project, projectColor, campaign, metadata, params, tags, key, kind, points, context }) {
  const stats = _mpStats(points || [], kind === 'series');
  return {
    cluster, runHash, runName, project, projectColor, campaign, metadata, params, tags: tags || [],
    key, kind, points: points || [],
    context: context || {},
    stats,
    contextSig: JSON.stringify(_mpSortObj(context || {})),
    numeric: kind === 'series'
      ? stats.numericCount >= 1
      : Number.isFinite((points || []).slice(-1)[0]?.value_num),
  };
}

function _mpStats(points, stepped) {
  const nums = (points || []).map(p => p.value_num).filter(Number.isFinite);
  const first = (points || [])[0] || {};
  const last = (points || []).slice(-1)[0] || {};
  return {
    latest: last.value,
    latestNum: Number.isFinite(last.value_num) ? last.value_num : null,
    firstNum: Number.isFinite(first.value_num) ? first.value_num : null,
    firstStep: stepped ? first.step : null,
    lastStep: stepped ? last.step : null,
    numericCount: nums.length,
    min: nums.length ? Math.min(...nums) : null,
    max: nums.length ? Math.max(...nums) : null,
  };
}

function _mpApplyDefaultSelection() {
  const numeric = _mpState.records.filter(r => r.numeric).map(r => r.key);
  const available = new Set(numeric);
  _mpState.selectedMetrics = _mpState.selectedMetrics.filter(k => available.has(k));
  if (!_mpState.selectedMetrics.length) {
    _mpState.selectedMetrics = Array.from(new Set(numeric)).slice(0, 4);
  }
}

// ─── 4. AimQL parser + evaluator ──────────────────────────────────────
// Recursive descent. Supported:
//   - operators: == != < <= > >= and or not in "not in" parens
//   - methods: .startswith(x) .endswith(x) .contains(x)
//   - literals: numbers, "..."/'...' strings, True/False/None, [a, b, c]
//   - fields:
//       metric.name        metric.kind        metric.last
//       metric.first_step  metric.last_step   metric.context.<k>
//       run.hash           run.name           run.cluster
//       run.project        run.campaign       run.hparams.<k>
//       run.<metadata-key>
// Returns an AST that _mpQLEval can evaluate against a record context.

function _mpQLParse(text) {
  const src = String(text || '').trim();
  if (!src) return null;
  const tokens = _mpQLTokenize(src);
  const parser = { tokens, pos: 0 };
  const ast = _mpQLParseOr(parser);
  if (parser.pos < parser.tokens.length) {
    throw new Error(`unexpected token "${parser.tokens[parser.pos].value}"`);
  }
  return ast;
}

function _mpQLTokenize(src) {
  const out = [];
  let i = 0;
  const len = src.length;
  while (i < len) {
    const c = src[i];
    if (c <= ' ') { i++; continue; }
    if (c === '(' || c === ')' || c === '[' || c === ']' || c === ',') {
      out.push({ type: 'punct', value: c }); i++; continue;
    }
    if (c === '"' || c === "'") {
      let j = i + 1; let val = '';
      while (j < len && src[j] !== c) {
        if (src[j] === '\\' && j + 1 < len) { val += src[j + 1]; j += 2; continue; }
        val += src[j]; j++;
      }
      if (j >= len) throw new Error('unterminated string literal');
      out.push({ type: 'str', value: val });
      i = j + 1; continue;
    }
    if (c === '=' || c === '!' || c === '<' || c === '>') {
      if (src[i + 1] === '=') { out.push({ type: 'op', value: c + '=' }); i += 2; continue; }
      if (c === '<' || c === '>') { out.push({ type: 'op', value: c }); i++; continue; }
      throw new Error(`unexpected character "${c}"`);
    }
    if ((c >= '0' && c <= '9') || (c === '-' && /[0-9]/.test(src[i + 1] || ''))) {
      let j = i + (c === '-' ? 1 : 0);
      while (j < len && /[0-9.eE+\-]/.test(src[j])) j++;
      const text = src.slice(i, j);
      const num = Number(text);
      if (!Number.isFinite(num)) throw new Error(`invalid number "${text}"`);
      out.push({ type: 'num', value: num });
      i = j; continue;
    }
    if (/[A-Za-z_]/.test(c)) {
      let j = i;
      while (j < len && /[A-Za-z0-9_.]/.test(src[j])) j++;
      const word = src.slice(i, j);
      const lower = word.toLowerCase();
      if (lower === 'and' || lower === 'or' || lower === 'not' || lower === 'in') {
        out.push({ type: 'kw', value: lower });
      } else if (lower === 'true' || lower === 'false' || lower === 'none') {
        out.push({ type: 'const', value: lower === 'true' ? true : (lower === 'false' ? false : null) });
      } else {
        out.push({ type: 'ident', value: word });
      }
      i = j; continue;
    }
    throw new Error(`unexpected character "${c}"`);
  }
  return out;
}

function _mpQLPeek(parser, offset = 0) { return parser.tokens[parser.pos + offset]; }
function _mpQLEat(parser) { return parser.tokens[parser.pos++]; }
function _mpQLExpect(parser, type, value) {
  const t = parser.tokens[parser.pos];
  if (!t || t.type !== type || (value != null && t.value !== value)) {
    throw new Error(`expected ${value || type}, got "${t ? t.value : 'EOF'}"`);
  }
  parser.pos++;
  return t;
}

function _mpQLParseOr(parser) {
  let left = _mpQLParseAnd(parser);
  while (true) {
    const t = _mpQLPeek(parser);
    if (!t || t.type !== 'kw' || t.value !== 'or') break;
    _mpQLEat(parser);
    const right = _mpQLParseAnd(parser);
    left = { type: 'or', left, right };
  }
  return left;
}

function _mpQLParseAnd(parser) {
  let left = _mpQLParseNot(parser);
  while (true) {
    const t = _mpQLPeek(parser);
    if (!t || t.type !== 'kw' || t.value !== 'and') break;
    _mpQLEat(parser);
    const right = _mpQLParseNot(parser);
    left = { type: 'and', left, right };
  }
  return left;
}

function _mpQLParseNot(parser) {
  const t = _mpQLPeek(parser);
  if (t && t.type === 'kw' && t.value === 'not') {
    _mpQLEat(parser);
    return { type: 'not', child: _mpQLParseNot(parser) };
  }
  return _mpQLParseCmp(parser);
}

function _mpQLParseCmp(parser) {
  const left = _mpQLParsePrimary(parser);
  const t = _mpQLPeek(parser);
  if (t && t.type === 'op') {
    _mpQLEat(parser);
    const right = _mpQLParsePrimary(parser);
    return { type: 'cmp', op: t.value, left, right };
  }
  if (t && t.type === 'kw' && t.value === 'in') {
    _mpQLEat(parser);
    const right = _mpQLParsePrimary(parser);
    return { type: 'in', left, right };
  }
  if (t && t.type === 'kw' && t.value === 'not'
      && _mpQLPeek(parser, 1) && _mpQLPeek(parser, 1).type === 'kw' && _mpQLPeek(parser, 1).value === 'in') {
    _mpQLEat(parser); _mpQLEat(parser);
    const right = _mpQLParsePrimary(parser);
    return { type: 'not', child: { type: 'in', left, right } };
  }
  return left;
}

function _mpQLParsePrimary(parser) {
  const t = _mpQLPeek(parser);
  if (!t) throw new Error('unexpected end of expression');
  if (t.type === 'punct' && t.value === '(') {
    _mpQLEat(parser);
    const expr = _mpQLParseOr(parser);
    _mpQLExpect(parser, 'punct', ')');
    return expr;
  }
  if (t.type === 'punct' && t.value === '[') {
    _mpQLEat(parser);
    const items = [];
    while (_mpQLPeek(parser) && !(_mpQLPeek(parser).type === 'punct' && _mpQLPeek(parser).value === ']')) {
      items.push(_mpQLParsePrimary(parser));
      if (_mpQLPeek(parser) && _mpQLPeek(parser).type === 'punct' && _mpQLPeek(parser).value === ',') _mpQLEat(parser);
    }
    _mpQLExpect(parser, 'punct', ']');
    return { type: 'list', items };
  }
  if (t.type === 'num') { _mpQLEat(parser); return { type: 'lit', value: t.value }; }
  if (t.type === 'str') { _mpQLEat(parser); return { type: 'lit', value: t.value }; }
  if (t.type === 'const') { _mpQLEat(parser); return { type: 'lit', value: t.value }; }
  if (t.type === 'ident') {
    _mpQLEat(parser);
    // Method call: ident.method(arg)
    if (_mpQLPeek(parser) && _mpQLPeek(parser).type === 'punct' && _mpQLPeek(parser).value === '(') {
      // The method is the suffix of the ident path after the final '.'
      const dot = t.value.lastIndexOf('.');
      if (dot < 0) throw new Error(`expected method on field, got "${t.value}"`);
      const fieldPath = t.value.slice(0, dot);
      const method = t.value.slice(dot + 1);
      _mpQLEat(parser);
      const arg = _mpQLParsePrimary(parser);
      _mpQLExpect(parser, 'punct', ')');
      return { type: 'method', field: fieldPath, method, arg };
    }
    return { type: 'field', path: t.value };
  }
  throw new Error(`unexpected token "${t.value}"`);
}

function _mpQLEval(ast, ctx) {
  if (!ast) return true;
  switch (ast.type) {
    case 'lit': return ast.value;
    case 'list': return ast.items.map(item => _mpQLEval(item, ctx));
    case 'field': return _mpQLResolve(ctx, ast.path);
    case 'not': return !_mpQLEval(ast.child, ctx);
    case 'and': return !!_mpQLEval(ast.left, ctx) && !!_mpQLEval(ast.right, ctx);
    case 'or': return !!_mpQLEval(ast.left, ctx) || !!_mpQLEval(ast.right, ctx);
    case 'in': {
      const left = _mpQLEval(ast.left, ctx);
      const right = _mpQLEval(ast.right, ctx);
      if (Array.isArray(left) && Array.isArray(right)) return left.some(v => right.some(r => _mpQLEq(v, r)));
      if (Array.isArray(right)) return right.some(v => _mpQLEq(left, v));
      if (Array.isArray(left)) return left.some(v => _mpQLEq(v, right));
      if (typeof right === 'string') return right.includes(String(left));
      return false;
    }
    case 'method': {
      const raw = _mpQLResolve(ctx, ast.field);
      const arg = String(_mpQLEval(ast.arg, ctx) ?? '').toLowerCase();
      if (Array.isArray(raw)) {
        return raw.some(item => {
          const v = String(item ?? '').toLowerCase();
          if (ast.method === 'contains') return v.includes(arg);
          if (ast.method === 'startswith') return v.startsWith(arg);
          if (ast.method === 'endswith') return v.endsWith(arg);
          return false;
        });
      }
      const v = String(raw ?? '').toLowerCase();
      if (ast.method === 'contains') return v.includes(arg);
      if (ast.method === 'startswith') return v.startsWith(arg);
      if (ast.method === 'endswith') return v.endsWith(arg);
      return false;
    }
    case 'cmp': {
      const a = _mpQLEval(ast.left, ctx);
      const b = _mpQLEval(ast.right, ctx);
      switch (ast.op) {
        case '==': return _mpQLEq(a, b);
        case '!=': return !_mpQLEq(a, b);
        case '<':  return _mpQLNum(a) < _mpQLNum(b);
        case '<=': return _mpQLNum(a) <= _mpQLNum(b);
        case '>':  return _mpQLNum(a) > _mpQLNum(b);
        case '>=': return _mpQLNum(a) >= _mpQLNum(b);
      }
      return false;
    }
  }
  return false;
}

function _mpQLEq(a, b) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  if (Array.isArray(a)) return a.some(v => _mpQLEq(v, b));
  if (Array.isArray(b)) return b.some(v => _mpQLEq(a, v));
  if (typeof a === 'number' || typeof b === 'number') {
    const an = Number(a); const bn = Number(b);
    if (Number.isFinite(an) && Number.isFinite(bn)) return an === bn;
  }
  return String(a) === String(b);
}

function _mpQLNum(v) { const n = Number(v); return Number.isFinite(n) ? n : NaN; }

function _mpQLResolve(ctx, path) {
  // ctx = { metric, run } virtual object built from a record
  const parts = String(path || '').split('.');
  let cur = ctx;
  for (const part of parts) {
    if (cur == null) return undefined;
    cur = cur[part];
  }
  return cur;
}

function _mpQLBuildContext(record) {
  const params = record.params || {};
  const metadata = record.metadata || {};
  // hparams is an alias for the params block when present, or falls back to
  // metadata when projects log directly to top-level metadata.
  const hparams = (params && typeof params === 'object' && Object.keys(params).length)
    ? params
    : metadata;
  const ctx = {
    metric: {
      name: record.key,
      key: record.key,
      kind: record.kind === 'scalars' ? 'scalar' : 'series',
      last: record.stats.latestNum,
      first: record.stats.firstNum,
      first_step: record.stats.firstStep,
      last_step: record.stats.lastStep,
      min: record.stats.min,
      max: record.stats.max,
      context: { ...(record.context || {}) },
    },
    run: {
      hash: record.runHash,
      name: record.runName,
      cluster: record.cluster,
      project: record.project,
      campaign: record.campaign,
      tags: record.tags || [],
      hparams,
      params,
      ...metadata,
    },
  };
  return ctx;
}

function _mpRecordMatches(record, ast) {
  if (!ast) return true;
  try { return !!_mpQLEval(ast, _mpQLBuildContext(record)); }
  catch (_) { return false; }
}

function _mpCompileQuery(text) {
  if (!text) return { ast: null, error: '' };
  try { return { ast: _mpQLParse(text), error: '' }; }
  catch (e) { return { ast: null, error: String(e.message || e) }; }
}

// ─── 5. Grouping engine ───────────────────────────────────────────────

function _mpGroupingFields() {
  // Base fields + dynamic context.* keys + dynamic run.hparams.* keys + run
  // metadata top-level keys (e.g. model, benchmark logged directly).
  const ctxKeys = new Set();
  const paramKeys = new Set();
  const metaKeys = new Set();
  const RESERVED_META = new Set(['name', 'hash', 'cluster', 'project', 'campaign', 'hparams', 'params']);
  _mpState.records.forEach(r => {
    Object.keys(r.context || {}).forEach(k => ctxKeys.add(k));
    Object.keys(r.params || {}).forEach(k => paramKeys.add(k));
    Object.keys(r.metadata || {}).forEach(k => { if (!RESERVED_META.has(k)) metaKeys.add(k); });
  });
  const fields = [...METRICS_GROUPING_FIELDS_BASE];
  Array.from(ctxKeys).sort().forEach(k => {
    fields.push({ id: `metric.context.${k}`, label: `ctx.${k}`, kind: 'context' });
  });
  Array.from(paramKeys).sort().forEach(k => {
    fields.push({ id: `run.hparams.${k}`, label: `hparams.${k}`, kind: 'run' });
  });
  Array.from(metaKeys).sort().forEach(k => {
    fields.push({ id: `run.${k}`, label: `meta.${k}`, kind: 'run' });
  });
  return fields;
}

// Build the per-trace SHORT label using the configured trace-label fields.
// This is what shows on the y-axis / legend / inline label.
//
// - When `traceLabelFields` is the default `['run.name']`, we auto-append the
//   metric name + remaining context so traces from the same run are
//   distinguishable.
// - When the user has explicitly picked OTHER fields (e.g. `run.cluster`,
//   `run.hparams.model`), we trust the choice and emit ONLY those fields. No
//   metric / context auto-append. The full identifier still shows on hover
//   via `_mpFormatTraceLabelVerbose`.
function _mpFormatTraceLabel(record) {
  const ctx = _mpQLBuildContext(record);
  const chartFields = new Set(_mpState.grouping.chart || []);
  const labelFields = (_mpState.traceLabelFields && _mpState.traceLabelFields.length)
    ? _mpState.traceLabelFields
    : ['run.name'];
  const isDefault = labelFields.length === 1 && labelFields[0] === 'run.name';
  const parts = [];
  for (const f of labelFields) {
    if (chartFields.has(f)) continue; // already in chart title
    const v = _mpQLResolve(ctx, f);
    if (v != null && v !== '') parts.push(String(v));
  }
  if (isDefault) {
    if (!chartFields.has('metric.name')) parts.push(record.key);
    const skipCtx = new Set();
    chartFields.forEach(f => { if (f.startsWith('metric.context.')) skipCtx.add(f.slice('metric.context.'.length)); });
    const ctxText = Object.entries(record.context || {})
      .filter(([k]) => !skipCtx.has(k))
      .map(([k, v]) => `${k}=${_stringValueOr(v, '∅')}`)
      .join(',');
    if (ctxText) parts.push(ctxText);
  }
  return parts.length ? parts.join(' · ') : (record.runName || record.runHash);
}

// The verbose form of a trace's label — always includes run name, run hash,
// cluster, metric name and full context. Used for hover tooltips so the user
// always sees the full identifier even when the short y-axis label is
// something terse like just "aws-cmh".
function _mpFormatTraceLabelVerbose(record) {
  const parts = [];
  parts.push(record.runName || record.runHash);
  const hashShort = String(record.runHash || '').slice(0, 8);
  if (hashShort) parts.push(hashShort);
  if (record.cluster) parts.push(record.cluster);
  parts.push(record.key);
  const ctxText = Object.entries(record.context || {})
    .map(([k, v]) => `${k}=${_stringValueOr(v, '∅')}`)
    .join(',');
  if (ctxText) parts.push(ctxText);
  return parts.join(' · ');
}

// Run identifier used in the right-rail Runs legend. Mirrors the label
// configuration so the legend always matches the chart.
function _mpFormatRunPrimary(run) {
  const payload = _mpState.runData[_mpRunKey(run)];
  const info = (payload && payload.info) || {};
  const labelFields = (_mpState.traceLabelFields && _mpState.traceLabelFields.length)
    ? _mpState.traceLabelFields
    : ['run.name'];
  const parts = [];
  for (const f of labelFields) {
    // Resolve manually since we don't have a record context here.
    const v = _mpResolveRunField(run, info, f);
    if (v != null && v !== '') parts.push(String(v));
  }
  return parts.join(' · ') || info.run_name || info.name || run.runHash;
}

function _mpResolveRunField(run, info, path) {
  const parts = String(path || '').split('.');
  if (parts[0] !== 'run') return undefined;
  const tail = parts.slice(1);
  // Top-level run.* short-circuits
  if (tail.length === 1) {
    switch (tail[0]) {
      case 'name': return info.run_name || info.name;
      case 'hash': return run.runHash;
      case 'cluster': return run.cluster;
      case 'project': return info.project;
      case 'campaign': return info.campaign;
      default: return (info[tail[0]] != null) ? info[tail[0]] : ((info.params || {})[tail[0]]);
    }
  }
  if (tail[0] === 'hparams') {
    const params = info.params || {};
    let cur = params;
    for (const p of tail.slice(1)) { if (cur == null) return undefined; cur = cur[p]; }
    return cur;
  }
  // Generic dotted: traverse info
  let cur = info;
  for (const p of tail) { if (cur == null) return undefined; cur = cur[p]; }
  return cur;
}

function _mpGroupKey(record, fields) {
  // Returns a stable string key from the resolved values of `fields`.
  const ctx = _mpQLBuildContext(record);
  return (fields || []).map(f => `${f}=${_stringValueOr(_mpQLResolve(ctx, f), '∅')}`).join('|');
}

function _stringValueOr(v, fallback) {
  if (v == null) return fallback;
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

// Stable fallback hash for run identifiers when there's no enumerated color
// assignment available (e.g. early renders, isolated views). Distinct group
// keys can still collide here — prefer the enumerated assignment built by
// _mpAssignColorOrder() for chart traces.
function _mpColorIndexForKey(key) {
  let h = 0x811c9dc5;
  const s = String(key || '');
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = (h * 0x01000193) >>> 0;
  }
  return h % METRICS_PALETTE.length;
}

// Walk the matching records in first-seen order and assign each distinct
// `grouping.color` key a sequential palette index. Up to 24 distinct keys
// get unique colors with NO collisions; beyond that, the palette wraps.
// Cached on _mpState so the runs legend can read the same assignment.
function _mpAssignColorOrder(matchingRecords) {
  const order = new Map();
  const colorFields = _mpState.grouping.color || [];
  (matchingRecords || []).forEach(r => {
    const key = _mpGroupKey(r, colorFields) || r.runHash;
    if (!order.has(key)) order.set(key, order.size);
  });
  _mpState.colorOrder = order;
  return order;
}

// Look up the palette index for a given record using the cached enumerated
// assignment, falling back to the stable hash when no order is available.
function _mpRecordColorIdx(record) {
  const colorFields = _mpState.grouping.color || [];
  const key = _mpGroupKey(record, colorFields) || record.runHash;
  const order = _mpState.colorOrder;
  if (order && order.has(key)) return order.get(key) % METRICS_PALETTE.length;
  return _mpColorIndexForKey(key);
}

// ─── Pattern grouping (overlays bar fills / line dashes) ─────────────

// Distinct line-dash patterns for series traces. Index 0 = solid (no
// pattern grouping applied). Order is chosen so adjacent groups look
// maximally different at small line widths.
const METRICS_LINE_DASHES = [
  [],               // solid
  [10, 4],          // long dash
  [4, 4],           // medium dash
  [2, 3],           // dotted
  [10, 3, 2, 3],    // dash-dot
  [12, 3, 2, 3, 2, 3], // dash-dot-dot
  [16, 4],          // very long dash
  [6, 2, 2, 2],     // short dash-dot
];

// Bar pattern factories. Each function paints an 8×8 tile in `color` and
// returns a CanvasPattern. Index 0 = solid (the trace's color, no pattern).
const _mpPatternFactories = [
  null, // 0: solid (use color directly)
  (ctx, color) => _mpPaintTile(ctx, color, (g) => {
    g.beginPath();
    for (let i = -8; i <= 8; i += 4) { g.moveTo(i, 8); g.lineTo(i + 8, 0); }
    g.stroke();
  }),
  (ctx, color) => _mpPaintTile(ctx, color, (g) => {
    g.beginPath();
    for (let i = -8; i <= 8; i += 4) { g.moveTo(i, 0); g.lineTo(i + 8, 8); }
    g.stroke();
  }),
  (ctx, color) => _mpPaintTile(ctx, color, (g) => {
    g.beginPath();
    for (let i = -8; i <= 8; i += 4) { g.moveTo(i, 8); g.lineTo(i + 8, 0); }
    for (let i = -8; i <= 8; i += 4) { g.moveTo(i, 0); g.lineTo(i + 8, 8); }
    g.stroke();
  }),
  (ctx, color) => _mpPaintTile(ctx, color, (g) => {
    g.beginPath();
    g.moveTo(0, 2); g.lineTo(8, 2);
    g.moveTo(0, 6); g.lineTo(8, 6);
    g.stroke();
  }),
  (ctx, color) => _mpPaintTile(ctx, color, (g) => {
    g.beginPath();
    g.moveTo(2, 0); g.lineTo(2, 8);
    g.moveTo(6, 0); g.lineTo(6, 8);
    g.stroke();
  }),
  (ctx, color) => _mpPaintTile(ctx, color, (g, fg) => {
    g.fillStyle = fg;
    g.beginPath();
    g.arc(2, 2, 1.2, 0, Math.PI * 2);
    g.arc(6, 6, 1.2, 0, Math.PI * 2);
    g.fill();
  }),
  (ctx, color) => _mpPaintTile(ctx, color, (g) => {
    g.beginPath();
    g.moveTo(4, 1); g.lineTo(4, 7);
    g.moveTo(1, 4); g.lineTo(7, 4);
    g.stroke();
  }),
];

// Paint one tile and return a repeating CanvasPattern. The base fill is a
// 25% alpha tint of the trace color so the pattern lines stay legible at
// full saturation.
function _mpPaintTile(ctx, color, draw) {
  const off = document.createElement('canvas');
  off.width = 8; off.height = 8;
  const g = off.getContext('2d');
  if (!g) return color;
  g.fillStyle = _mpAlpha(color, 0.28);
  g.fillRect(0, 0, 8, 8);
  g.strokeStyle = color;
  g.lineWidth = 1.5;
  g.lineCap = 'round';
  draw(g, color);
  return ctx.createPattern(off, 'repeat');
}

// Enumerated pattern assignment, mirroring _mpAssignColorOrder. Empty
// grouping.pattern means every trace uses pattern 0 (solid).
function _mpAssignPatternOrder(matchingRecords) {
  const order = new Map();
  const fields = _mpState.grouping.pattern || [];
  if (!fields.length) {
    _mpState.patternOrder = order;
    return order;
  }
  (matchingRecords || []).forEach(r => {
    const key = _mpGroupKey(r, fields);
    if (!order.has(key)) order.set(key, order.size);
  });
  _mpState.patternOrder = order;
  return order;
}

function _mpRecordPatternIdx(record) {
  const fields = _mpState.grouping.pattern || [];
  if (!fields.length) return 0;
  const key = _mpGroupKey(record, fields);
  const order = _mpState.patternOrder;
  if (order && order.has(key)) return order.get(key) % METRICS_LINE_DASHES.length;
  return 0;
}

// ─── 6. Top-level render ──────────────────────────────────────────────

// Selectors for scrollable containers we want to preserve scroll position
// across renders, since _mpRender() replaces the entire innerHTML and would
// otherwise reset every scroll bar to 0.
const _MP_SCROLL_SELECTORS = [
  '.mp-controls',
  '.mp-chart-pane',
  '.mp-table-scroll',
  '.mp-runs-legend-list',
  '.mp-control-metrics .mp-metric-chips',
  '.mp-metric-list',
];

function _mpCaptureScrollPositions(root) {
  const snap = {};
  _MP_SCROLL_SELECTORS.forEach(sel => {
    const node = root.querySelector(sel);
    if (node && node.scrollTop) snap[sel] = node.scrollTop;
  });
  return snap;
}

function _mpRestoreScrollPositions(root, snap) {
  if (!snap) return;
  Object.entries(snap).forEach(([sel, top]) => {
    const node = root.querySelector(sel);
    if (node) node.scrollTop = top;
  });
}

function _mpRender() {
  const el = document.getElementById('metrics-page');
  if (!el) return;
  const scrollSnap = _mpCaptureScrollPositions(el);
  _mpDestroyCharts();
  const { ast, error } = _mpCompileQuery(_mpState.query);
  // When the query has a parse error, return zero records so the user sees
  // the error banner + empty chart instead of unfiltered noise.
  const matchingRecords = error
    ? []
    : _mpState.records.filter((rec) => _mpRecordMatches(rec, ast));
  // Enumerate distinct color keys → palette index so two different group
  // values can never share a color (subject to palette size). Same trick
  // for pattern grouping, so different group values get distinct bar fills
  // and line dashes.
  _mpAssignColorOrder(matchingRecords);
  _mpAssignPatternOrder(matchingRecords);
  const hasRuns = _mpState.runs.length > 0;
  const hasRecords = _mpState.records.length > 0;
  // Main layout always renders so the right rail (with the Runs legend + AimQL
  // hint) is visible even before any runs are loaded.
  const tableOpen = !!_mpState.tableOpen;
  const showTable = hasRecords && tableOpen;
  el.innerHTML = `${_mpTopBarHtml()}
    ${_mpSearchBarHtml(error)}
    <div class="mp-layout" style="grid-template-columns: minmax(0, 1fr) 5px ${_mpState.rightRailWidth}px;">
      <div class="mp-main">
        <div class="mp-chart-pane" id="mp-chart-pane" style="flex: ${showTable ? _mpState.splitRatio : 1} 1 0;">
          ${hasRecords
              ? _mpChartAreaHtml(matchingRecords)
              : (hasRuns
                  ? '<div class="mp-empty">Loading metrics…</div>'
                  : `<div class="mp-empty mp-empty-onboard">
                      <div class="mp-empty-title">Search runs &amp; metrics with AimQL</div>
                      <div class="mp-empty-sub">Type a query above. Examples:</div>
                      <ul class="mp-empty-examples">
                        <li><code>run.name.contains("mcp_mcpv2lt")</code> — find runs by name substring</li>
                        <li><code>run.name.startswith("hle_") and metric.name == "best_of_3_judge_correct"</code> — narrow to a metric</li>
                        <li><code>run.cluster == "aws-cmh" and metric.last &gt; 0.8</code> — combine run and metric filters</li>
                        <li><code>run.hash == "86398daa"</code> — pull in a single run by hash</li>
                      </ul>
                    </div>`)}
        </div>
        ${showTable ? '<div class="mp-splitter" id="mp-splitter" title="Drag to resize"></div>' : ''}
        ${showTable ? `<div class="mp-table-pane" style="flex: ${1 - _mpState.splitRatio} 1 0;">
          ${_mpContextTableHtml(matchingRecords)}
        </div>` : ''}
      </div>
      <div class="mp-sidebar-splitter" id="mp-sidebar-splitter" title="Drag to resize the sidebar"></div>
      <aside class="mp-controls">${_mpControlsHtml(matchingRecords)}</aside>
    </div>
    ${_mpState.tableViewOpen ? _mpTableViewHtml(matchingRecords) : ''}`;
  _mpAttachShellHandlers();
  _mpAttachTableViewEsc();
  if (hasRecords) {
    if (showTable) _mpAttachSplitterHandlers();
    _mpRenderCharts(matchingRecords);
    _mpAttachChartDragHandlers();
  }
  _mpAttachSidebarSplitterHandlers();
  _mpRenderAutocomplete();
  // Restore scroll positions so clicking sidebar controls doesn't jump back
  // to the top of any pane.
  _mpRestoreScrollPositions(el, scrollSnap);
  // Position any open rail dropdowns. They use position:fixed so they can
  // escape `.mp-controls` overflow clipping; we have to set coordinates
  // explicitly relative to their picker button.
  _mpPositionRailDropdowns();
}

// Place a `position:fixed` dropdown right under its anchor button, flipping
// to "above the button" or "anchored to left edge" when it would otherwise
// overflow the viewport.
function _mpPlaceDropdown(anchor, dropdown) {
  if (!anchor || !dropdown) return;
  const ar = anchor.getBoundingClientRect();
  // Force layout so we can read accurate dimensions.
  dropdown.style.left = '-9999px';
  dropdown.style.top  = '0px';
  const dw = dropdown.offsetWidth;
  const dh = dropdown.offsetHeight;
  const pad = 8;
  // Anchor the dropdown's RIGHT edge under the button's right edge so it
  // grows leftward (into the chart area) instead of past the viewport edge.
  let left = ar.right - dw;
  if (left < pad) left = pad;
  if (left + dw > window.innerWidth - pad) left = window.innerWidth - dw - pad;
  let top = ar.bottom + 6;
  if (top + dh > window.innerHeight - pad) {
    // Flip above the button when there's not enough room below.
    top = Math.max(pad, ar.top - dh - 6);
  }
  dropdown.style.left = `${left}px`;
  dropdown.style.top  = `${top}px`;
}

function _mpPositionRailDropdowns() {
  // Metric chooser
  if (_mpState.metricsOpen) {
    const picker = document.querySelector('.mp-control-metrics .mp-metric-picker');
    const dropdown = picker && picker.querySelector('.mp-metric-dropdown.open');
    if (dropdown) _mpPlaceDropdown(picker.querySelector('.mp-btn') || picker, dropdown);
  }
  // Group/Label dropdowns (any that happen to be open)
  document.querySelectorAll('.mp-group-dropdown.open').forEach(dropdown => {
    const picker = dropdown.closest('.mp-group-picker');
    if (picker) _mpPlaceDropdown(picker.querySelector('.mp-group-btn') || picker, dropdown);
  });
}

// Close any open rail dropdown when the rail itself scrolls — the dropdown
// is position:fixed and would otherwise stay visually detached from its
// scrolled-away button.
function _mpAttachRailScrollHandler() {
  const rail = document.querySelector('.mp-controls');
  if (!rail) return;
  rail.addEventListener('scroll', () => {
    let changed = false;
    if (_mpState.metricsOpen) { _mpState.metricsOpen = false; changed = true; }
    document.querySelectorAll('.mp-group-dropdown.open').forEach(d => { d.classList.remove('open'); changed = true; });
    if (changed) _mpRender();
  });
}

// Close the table-view modal on Esc. Listener bound once per session.
let _mpTableViewEscBound = false;
function _mpAttachTableViewEsc() {
  if (_mpTableViewEscBound) return;
  _mpTableViewEscBound = true;
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && _mpState.tableViewOpen) {
      _mpState.tableViewOpen = false;
      _mpRender();
    }
  });
}

function _mpAttachShellHandlers() {
  _mpAttachRailScrollHandler();
  const queryInput = document.getElementById('mp-query-input');
  if (queryInput) {
    queryInput.addEventListener('input', _mpQueryInputChanged);
    queryInput.addEventListener('keydown', _mpQueryKeyDown);
    queryInput.addEventListener('focus', _mpQueryInputChanged);
    queryInput.addEventListener('click', _mpQueryInputChanged);
    queryInput.addEventListener('blur', () => {
      // Delay close so click on a suggestion item registers first
      setTimeout(() => { _mpAC.open = false; _mpRenderAutocomplete(); }, 120);
    });
  }
  document.addEventListener('click', _mpCloseDropdownsOnOutside);
}

function _mpCloseDropdownsOnOutside(e) {
  const inAuto = e.target.closest('#mp-ac-list') || e.target.closest('.mp-search-wrap');
  if (!inAuto && _mpAC.open) { _mpAC.open = false; _mpRenderAutocomplete(); }
  const inViews = e.target.closest('.mp-views-picker') || e.target.closest('.mp-views-menu');
  if (!inViews && _mpState.viewsMenuOpen) {
    _mpCloseViewsMenu();
    return;
  }
  const inMetric = e.target.closest('.mp-metric-picker') || e.target.closest('.mp-metric-dropdown');
  if (!inMetric && _mpState.metricsOpen) { _mpState.metricsOpen = false; _mpRender(); return; }
  const inGroup = e.target.closest('.mp-group-picker') || e.target.closest('.mp-group-dropdown');
  if (!inGroup) {
    document.querySelectorAll('.mp-group-dropdown.open').forEach(el => el.classList.remove('open'));
  }
}

// ─── 7. Top bar / saved views / runs select ───────────────────────────

function _mpTopBarHtml() {
  return `<div class="mp-topbar">
    <div class="mp-topbar-left">
      <div class="mp-kicker">multi-run metrics explorer</div>
      <div class="mp-title">Metrics${_mpActiveViewLabelHtml()}</div>
    </div>
    <div class="mp-topbar-actions">
      <button class="mp-btn" onclick="_mpRefresh()" title="Reload metrics for current runs">↻ refresh</button>
      <button class="mp-btn${_mpState.tableOpen ? ' mp-btn-toggled' : ''}"
              onclick="_mpToggleTable()"
              title="${_mpState.tableOpen ? 'Hide context table' : 'Show context table'}">
        ${_mpState.tableOpen ? '▣' : '▢'} context table
      </button>
      ${_mpViewsMenuHtml()}
      <button class="mp-btn" onclick="_mpResetZoom()" title="Reset zoom on every subplot">⟲ reset zoom</button>
      <button class="mp-btn" onclick="_mpExportAll()" title="Download PNG for every visible subplot">⤓ export png</button>
    </div>
  </div>`;
}

function _mpActiveViewLabelHtml() {
  if (!_mpState.activeViewId) return '';
  const view = (_mpState.savedViews || []).find(v => v.id === _mpState.activeViewId);
  if (!view) return '';
  return ` <span class="mp-active-view-pill" title="Active saved view"> · ${_escHtml(view.title || 'Untitled')}</span>`;
}

function _mpViewsMenuHtml() {
  const views = _mpState.savedViews || [];
  const open = !!_mpState.viewsMenuOpen;
  const renaming = _mpState.renamingViewId || null;
  const activeView = views.find(v => v.id === _mpState.activeViewId);
  const listHtml = views.length
    ? views.map(v => {
        const isActive = _mpState.activeViewId === v.id;
        const isRenaming = renaming === v.id;
        const ts = (v.updated_at || '').replace('T', ' ').slice(0, 16);
        return `<div class="mp-views-row${isActive ? ' active' : ''}">
          ${isRenaming
            ? `<input class="mp-views-rename" data-view-id="${v.id}" value="${escAttr(v.title || '')}"
                      onkeydown="_mpRenameViewKey(event, ${v.id})"
                      onblur="_mpCommitRenameView(${v.id}, this.value)">`
            : `<button class="mp-views-pick" onclick="_mpOpenSavedView(${v.id})" title="Load this view">
                 <span class="mp-views-pick-name">${_escHtml(v.title || 'Untitled')}</span>
                 <span class="mp-views-pick-ts">${_escHtml(ts)}</span>
               </button>`}
          <button class="mp-views-icon" onclick="_mpStartRenameView(${v.id})" title="Rename">✎</button>
          <button class="mp-views-icon" onclick="_mpDeleteSavedView(${v.id})" title="Delete">×</button>
        </div>`;
      }).join('')
    : '<div class="mp-views-empty">No saved views yet.</div>';
  return `<div class="mp-views-picker">
    <button class="mp-btn" onclick="_mpToggleViewsMenu(event)" title="Saved views">
      <span class="mp-views-btn-label">Views</span>
      ${views.length ? `<span class="mp-views-btn-count">${views.length}</span>` : ''}
      <span class="mp-views-btn-caret">▾</span>
    </button>
    <div class="mp-views-menu${open ? ' open' : ''}" id="mp-views-menu">
      <div class="mp-views-menu-head">Saved views</div>
      <div class="mp-views-list">${listHtml}</div>
      <div class="mp-views-actions">
        <button class="mp-views-action" onclick="_mpSaveAsNewView()" title="Create a new view from the current workspace">+ Save as new view…</button>
        ${activeView
          ? `<button class="mp-views-action" onclick="_mpUpdateCurrentView()" title="Overwrite ${escAttr(activeView.title || 'Untitled')}">⟳ Update '${_escHtml(activeView.title || 'Untitled')}'</button>`
          : '<button class="mp-views-action" disabled title="Load a saved view first to update it">⟳ Update current view</button>'}
      </div>
    </div>
  </div>`;
}

function _mpToggleViewsMenu(event) {
  if (event) event.stopPropagation();
  _mpState.viewsMenuOpen = !_mpState.viewsMenuOpen;
  _mpState.renamingViewId = null;
  _mpRender();
}

function _mpCloseViewsMenu() {
  if (!_mpState.viewsMenuOpen && !_mpState.renamingViewId) return;
  _mpState.viewsMenuOpen = false;
  _mpState.renamingViewId = null;
  _mpRender();
}

function _mpStartRenameView(id) {
  _mpState.renamingViewId = id;
  _mpState.viewsMenuOpen = true;
  _mpRender();
  // Focus the rename input after the re-render commits.
  setTimeout(() => {
    const inp = document.querySelector(`.mp-views-rename[data-view-id="${id}"]`);
    if (inp) { inp.focus(); inp.select(); }
  }, 0);
}

function _mpRenameViewKey(event, id) {
  if (event.key === 'Enter') {
    event.preventDefault();
    _mpCommitRenameView(id, event.target.value);
  } else if (event.key === 'Escape') {
    event.preventDefault();
    _mpState.renamingViewId = null;
    _mpRender();
  }
}

async function _mpCommitRenameView(id, title) {
  const trimmed = String(title || '').trim();
  _mpState.renamingViewId = null;
  if (!trimmed) { _mpRender(); return; }
  const view = (_mpState.savedViews || []).find(v => v.id === id);
  if (view && view.title === trimmed) { _mpRender(); return; }
  try {
    const res = await fetch(`/api/metrics_views/${id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: trimmed }),
    });
    const data = await res.json();
    if (data.status === 'ok') {
      toast('View renamed');
      await _mpLoadSavedViews();
    } else {
      toast(data.error || 'Failed to rename view', 'error');
      _mpRender();
    }
  } catch (_) {
    toast('Network error while renaming view', 'error');
    _mpRender();
  }
}

function _mpRemoveRun(key) {
  _mpState.runs = _mpState.runs.filter(r => _mpRunKey(r) !== key);
  delete _mpState.runData[key];
  _mpBuildRecords();
  _mpApplyDefaultSelection();
  _mpReplaceUrl();
  _mpRender();
}

function _mpRefresh() {
  _mpState.loadSeq++;
  _mpLoadRuns();
}

function _mpToggleTable() {
  _mpState.tableOpen = !_mpState.tableOpen;
  _mpReplaceUrl();
  _mpRender();
}

// Drag the left edge of the right rail to resize. Clamped to 220..640 px so
// the layout stays usable in both extremes.
function _mpAttachSidebarSplitterHandlers() {
  const splitter = document.getElementById('mp-sidebar-splitter');
  if (!splitter) return;
  splitter.addEventListener('mousedown', (e) => {
    e.preventDefault();
    const layout = splitter.parentElement;
    if (!layout) return;
    const rect = layout.getBoundingClientRect();
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    const onMove = (mv) => {
      const fromRight = Math.max(220, Math.min(640, rect.right - mv.clientX));
      _mpState.rightRailWidth = fromRight;
      layout.style.gridTemplateColumns = `minmax(0, 1fr) 5px ${fromRight}px`;
      _mpState.charts.forEach(c => { try { c.resize(); } catch (_) {} });
    };
    const onUp = () => {
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
      _mpReplaceUrl();
    };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  });
}

// ─── 8. AimQL search bar (with strong autocomplete) ──────────────────

function _mpSearchBarHtml(queryError) {
  const discovering = _mpState.discovering ? `<span class="mp-search-spinner" title="Discovering runs…">·</span>` : '';
  const invalidCls = queryError ? ' mp-query-invalid' : '';
  return `<div class="mp-search">
    <div class="mp-search-label">AimQL</div>
    <div class="mp-search-wrap">
      <input id="mp-query-input" class="mp-query-input${invalidCls}"
             value="${escAttr(_mpState.query)}"
             placeholder='run.name.contains("…") and metric.name == "…"'
             autocomplete="off" spellcheck="false">
      ${discovering}
      <div class="mp-ac-list" id="mp-ac-list"></div>
    </div>
    <button class="mp-btn primary" onclick="_mpRunQuery(document.getElementById('mp-query-input').value)"
            title="Apply AimQL filter and auto-discover matching runs from run.name / run.hash patterns">Search</button>
    <button class="mp-btn ghost" onclick="_mpClearQuery()" title="Clear search">clear</button>
    ${queryError ? `<div class="mp-query-error">${_escHtml(queryError)} <em>(no traces shown until the query parses; try replacing <code>=</code> with <code>==</code>)</em></div>` : ''}
  </div>`;
}

function _mpMetricsDropdownHtml(metrics, selected) {
  if (!_mpState.metricsOpen) return '';
  const filter = (_mpState.metricsFilter || '').toLowerCase();
  const filtered = filter ? metrics.filter(k => k.toLowerCase().includes(filter)) : metrics;
  return `<div class="mp-metric-dropdown open">
    <input class="mp-metric-search" placeholder="Search metrics…"
           value="${escAttr(_mpState.metricsFilter)}"
           oninput="_mpSetMetricsFilter(this.value)">
    <div class="mp-metric-actions">
      <button class="mp-link" onclick="_mpSelectAllMetrics()">select all</button>
      <button class="mp-link" onclick="_mpClearMetrics()">clear</button>
    </div>
    <div class="mp-metric-list">
      ${filtered.length ? filtered.map(key => `
        <label class="mp-metric-option${selected.has(key) ? ' selected' : ''}">
          <input type="checkbox" ${selected.has(key) ? 'checked' : ''} onchange="_mpToggleMetric('${escAttr(key)}')">
          <span class="mp-metric-option-name">${_escHtml(key)}</span>
          <span class="mp-metric-option-meta">${_mpMetricSummary(key)}</span>
        </label>`).join('') : '<div class="mp-muted" style="padding:8px">No matching metrics.</div>'}
    </div>
  </div>`;
}

function _mpMetricSummary(key) {
  const recs = _mpState.records.filter(r => r.key === key);
  const runHashes = new Set(recs.map(r => r.runHash));
  const ctxs = new Set(recs.map(r => r.contextSig));
  return `${recs.length} rec · ${runHashes.size} run${runHashes.size === 1 ? '' : 's'} · ${ctxs.size} ctx`;
}

function _mpToggleMetricsDropdown(force) {
  _mpState.metricsOpen = force == null ? !_mpState.metricsOpen : !!force;
  _mpRender();
}

// ─── AimQL autocomplete ──────────────────────────────────────────────

// Static field catalog. Dynamic fields (`metric.context.<k>`, `run.hparams.<k>`)
// are appended from loaded records.
const _MP_FIELD_CATALOG = [
  { id: 'run.name',        kind: 'string', hint: 'Run name (use .contains/.startswith)' },
  { id: 'run.hash',        kind: 'string', hint: 'Run hash (8-12 hex chars)' },
  { id: 'run.cluster',     kind: 'string', hint: 'Cluster (aws-cmh, eos, …)' },
  { id: 'run.project',     kind: 'string', hint: 'Project tag' },
  { id: 'run.campaign',    kind: 'string', hint: 'Campaign tag' },
  { id: 'run.tags',        kind: 'string[]', hint: 'Run tags; smoke excluded by default' },
  { id: 'metric.name',     kind: 'string', hint: 'Metric key' },
  { id: 'metric.kind',     kind: 'string', hint: '"series" or "scalar"' },
  { id: 'metric.last',     kind: 'number', hint: 'Last numeric value' },
  { id: 'metric.first',    kind: 'number', hint: 'First numeric value' },
  { id: 'metric.min',      kind: 'number', hint: 'Min observed' },
  { id: 'metric.max',      kind: 'number', hint: 'Max observed' },
  { id: 'metric.last_step',kind: 'number', hint: 'Last step (series only)' },
  { id: 'metric.first_step',kind:'number', hint: 'First step (series only)' },
];

const _MP_KEYWORDS = [
  { id: 'and',     hint: 'Logical AND' },
  { id: 'or',      hint: 'Logical OR' },
  { id: 'not',     hint: 'Negation' },
  { id: 'in',      hint: 'Set membership: x in [a, b]' },
  { id: 'not in',  hint: 'Set non-membership' },
];

const _MP_METHODS = [
  { id: 'contains',   hint: 'Substring match' },
  { id: 'startswith', hint: 'Prefix match' },
  { id: 'endswith',   hint: 'Suffix match' },
];

function _mpDynamicFields() {
  const out = [];
  const ctxKeys = new Set();
  const paramKeys = new Set();
  _mpState.records.forEach(r => {
    Object.keys(r.context || {}).forEach(k => ctxKeys.add(k));
    Object.keys(r.params || {}).forEach(k => paramKeys.add(k));
  });
  Array.from(ctxKeys).sort().forEach(k => out.push({ id: `metric.context.${k}`, kind: 'string', hint: 'Metric context key' }));
  Array.from(paramKeys).sort().forEach(k => out.push({ id: `run.hparams.${k}`, kind: 'number', hint: 'Hyperparam' }));
  return out;
}

function _mpAllFields() { return [..._MP_FIELD_CATALOG, ..._mpDynamicFields()]; }

// Inspect the text before the cursor and decide what to suggest.
//   { kind: 'field' | 'method' | 'operator' | 'value_string' | 'value_number' | 'fresh',
//     partial: '...',
//     replaceStart, replaceEnd,
//     field?: 'run.name' | ..., method?: 'contains' | 'startswith' | 'endswith' | 'equals' }
function _mpAcContext(text, cursor) {
  const before = String(text || '').slice(0, cursor);

  // Inside an unterminated string literal: emit a value suggestion for the
  // preceding field.
  let inStr = null;
  let strStart = -1;
  for (let i = 0; i < before.length; i++) {
    const c = before[i];
    if (inStr) {
      if (c === '\\') { i++; continue; }
      if (c === inStr) { inStr = null; strStart = -1; }
    } else if (c === '"' || c === "'") { inStr = c; strStart = i; }
  }
  if (inStr) {
    const partial = before.slice(strStart + 1);
    // Find what's immediately to the left of the open quote
    const lhs = before.slice(0, strStart).trimEnd();
    const method = lhs.match(/([\w.]+)\.(contains|startswith|endswith)\s*\(\s*$/);
    if (method) {
      return { kind: 'value_string', partial, field: method[1], method: method[2],
               replaceStart: strStart + 1, replaceEnd: cursor };
    }
    const eq = lhs.match(/([\w.]+)\s*(==|!=)\s*$/);
    if (eq) {
      return { kind: 'value_string', partial, field: eq[1], method: 'equals',
               replaceStart: strStart + 1, replaceEnd: cursor };
    }
    return { kind: 'value_string', partial, field: '', method: 'equals',
             replaceStart: strStart + 1, replaceEnd: cursor };
  }

  // Trailing identifier (possibly dotted)
  const idMatch = before.match(/([A-Za-z_][\w.]*)$/);
  if (idMatch) {
    const partial = idMatch[1];
    return { kind: 'field', partial,
             replaceStart: cursor - partial.length, replaceEnd: cursor };
  }

  // Trailing operator or after operator
  const opAfter = before.match(/(==|!=|<=|>=|<|>)\s*$/);
  if (opAfter) {
    // Suggest values for whatever field precedes the op
    const lhs = before.slice(0, before.length - opAfter[0].length).trimEnd();
    const fm = lhs.match(/([\w.]+)$/);
    return { kind: 'value_after_op', partial: '',
             field: fm ? fm[1] : '', method: 'equals',
             replaceStart: cursor, replaceEnd: cursor };
  }

  // Fresh / between expressions
  const trimEnd = before.replace(/[ \t]+$/, '');
  if (!trimEnd || /(\(|,|\b(?:and|or|not)\b)$/i.test(trimEnd)) {
    return { kind: 'fresh', partial: '', replaceStart: cursor, replaceEnd: cursor };
  }

  // After a complete value/identifier — suggest operators / keywords
  return { kind: 'after_term', partial: '', replaceStart: cursor, replaceEnd: cursor };
}

function _mpAcSnippets() {
  // Quick-start templates
  return [
    { kind: 'snippet', id: 'run.name.contains("…")', label: 'run.name.contains("…")', hint: 'find runs by name substring',
      insert: 'run.name.contains("")', cursorBack: 2 },
    { kind: 'snippet', id: 'run.name.startswith("…")', label: 'run.name.startswith("…")', hint: 'find runs by name prefix',
      insert: 'run.name.startswith("")', cursorBack: 2 },
    { kind: 'snippet', id: 'metric.name == "…"', label: 'metric.name == "…"', hint: 'filter to one metric',
      insert: 'metric.name == ""', cursorBack: 1 },
    { kind: 'snippet', id: 'run.hash == "…"', label: 'run.hash == "…"', hint: 'pull in a single run by hash',
      insert: 'run.hash == ""', cursorBack: 1 },
    { kind: 'snippet', id: 'run.cluster == "…"', label: 'run.cluster == "aws-cmh"', hint: 'restrict to a cluster',
      insert: 'run.cluster == ""', cursorBack: 1 },
    { kind: 'snippet', id: 'metric.context.…', label: 'metric.context.<key> == "…"', hint: 'filter by metric context',
      insert: 'metric.context.', cursorBack: 0 },
    { kind: 'snippet', id: 'metric.last > …', label: 'metric.last > 0.5', hint: 'numeric threshold',
      insert: 'metric.last > ', cursorBack: 0 },
  ];
}

function _mpAcFieldItems(partial) {
  const fields = _mpAllFields();
  const p = String(partial || '').toLowerCase();
  return fields
    .filter(f => f.id.toLowerCase().startsWith(p) || f.id.toLowerCase().includes(p))
    .map(f => ({
      kind: 'field', id: f.id, label: f.id, hint: f.hint,
      insert: f.id, cursorBack: 0,
    }))
    .slice(0, 12);
}

function _mpAcOperatorItems(field) {
  const f = _mpAllFields().find(x => x.id === field);
  const isString = !f || f.kind === 'string';
  const ops = isString
    ? [
        { id: '.contains("…")',   insert: `${field}.contains("")`,   cursorBack: 2, hint: 'substring' },
        { id: '.startswith("…")', insert: `${field}.startswith("")`, cursorBack: 2, hint: 'prefix' },
        { id: '.endswith("…")',   insert: `${field}.endswith("")`,   cursorBack: 2, hint: 'suffix' },
        { id: '== "…"',           insert: `${field} == ""`,          cursorBack: 1, hint: 'equals' },
        { id: '!= "…"',           insert: `${field} != ""`,          cursorBack: 1, hint: 'not equals' },
        { id: 'in [...]',         insert: `${field} in [""]`,        cursorBack: 2, hint: 'one of' },
      ]
    : [
        { id: '== …',  insert: `${field} == `,  cursorBack: 0, hint: 'equals' },
        { id: '!= …',  insert: `${field} != `,  cursorBack: 0, hint: 'not equals' },
        { id: '> …',   insert: `${field} > `,   cursorBack: 0, hint: 'greater' },
        { id: '< …',   insert: `${field} < `,   cursorBack: 0, hint: 'less' },
        { id: '>= …',  insert: `${field} >= `,  cursorBack: 0, hint: 'gte' },
        { id: '<= …',  insert: `${field} <= `,  cursorBack: 0, hint: 'lte' },
        { id: 'in […]',insert: `${field} in [`, cursorBack: 0, hint: 'one of' },
      ];
  return ops.map(o => ({ kind: 'method', id: o.id, label: `${field}${o.id}`, hint: o.hint, insert: o.insert, cursorBack: o.cursorBack }));
}

function _mpAcKeywordItems() {
  return _MP_KEYWORDS.map(k => ({ kind: 'keyword', id: k.id, label: k.id, hint: k.hint,
                                  insert: `${k.id} `, cursorBack: 0 }));
}

function _mpAcValueItemsLocal(field, partial) {
  const p = String(partial || '').toLowerCase();
  if (field === 'metric.name') {
    const names = Array.from(new Set(_mpState.records.map(r => r.key))).sort();
    return names.filter(n => !p || n.toLowerCase().includes(p))
      .slice(0, 20)
      .map(n => ({ kind: 'value', id: n, label: n, hint: 'metric', insert: n, cursorBack: 0 }));
  }
  if (field === 'run.cluster') {
    const clusters = Array.from(new Set(_mpState.records.map(r => r.cluster).filter(Boolean))).sort();
    return clusters.filter(c => !p || c.toLowerCase().includes(p))
      .map(c => ({ kind: 'value', id: c, label: c, hint: 'cluster', insert: c, cursorBack: 0 }));
  }
  if (field === 'run.project') {
    const projects = Array.from(new Set(_mpState.records.map(r => r.project).filter(Boolean))).sort();
    return projects.filter(c => !p || c.toLowerCase().includes(p))
      .map(c => ({ kind: 'value', id: c, label: c, hint: 'project', insert: c, cursorBack: 0 }));
  }
  if (field === 'run.tags') {
    const tags = new Set();
    _mpState.records.forEach(r => (r.tags || []).forEach(tag => tags.add(tag)));
    return Array.from(tags).sort()
      .filter(c => !p || c.toLowerCase().includes(p))
      .map(c => ({ kind: 'value', id: c, label: c, hint: 'run tag', insert: c, cursorBack: 0 }));
  }
  if (field === 'metric.kind') {
    return ['series', 'scalar']
      .filter(c => !p || c.includes(p))
      .map(c => ({ kind: 'value', id: c, label: c, hint: 'metric kind', insert: c, cursorBack: 0 }));
  }
  if (field && field.startsWith('metric.context.')) {
    const k = field.slice('metric.context.'.length);
    const vals = new Set();
    _mpState.records.forEach(r => { if (r.context && r.context[k] != null) vals.add(String(r.context[k])); });
    return Array.from(vals).sort()
      .filter(v => !p || v.toLowerCase().includes(p))
      .map(v => ({ kind: 'value', id: v, label: v, hint: `ctx.${k}`, insert: v, cursorBack: 0 }));
  }
  if (field && field.startsWith('run.hparams.')) {
    const k = field.slice('run.hparams.'.length);
    const vals = new Set();
    _mpState.records.forEach(r => { if (r.params && r.params[k] != null) vals.add(String(r.params[k])); });
    return Array.from(vals).sort()
      .filter(v => !p || v.toLowerCase().includes(p))
      .map(v => ({ kind: 'value', id: v, label: v, hint: `hparam.${k}`, insert: v, cursorBack: 0 }));
  }
  return [];
}

// Catalog of fields the server can suggest values for via
// /api/metric_field_values. For others we fall back to local records or no
// remote lookup.
const _MP_CATALOG_FIELDS = new Set([
  'metric.name', 'metric.key', 'metric.kind',
  'run.cluster', 'run.project', 'run.tags',
]);
function _mpIsCatalogField(field) {
  if (!field) return false;
  if (_MP_CATALOG_FIELDS.has(field)) return true;
  if (field.startsWith('metric.context.')) return true;
  if (field.startsWith('run.hparams.')) return true;
  // Top-level run.<metadata-key> (single dot after "run.")
  if (field.startsWith('run.') && !field.slice(4).includes('.')) {
    const reserved = new Set(['name', 'hash', 'cluster', 'project', 'campaign', 'tags', 'hparams', 'params']);
    if (!reserved.has(field.slice(4))) return true;
  }
  return false;
}

// Fetch and cache distinct values for a catalog field (e.g.
// metric.context.benchmark). Returns the cached array so client-side
// filtering on `partial` can update results without hitting the network on
// every keystroke.
async function _mpAcFetchFieldValues(field) {
  if (!_mpAC.cache.fieldValues) _mpAC.cache.fieldValues = {};
  if (_mpAC.cache.fieldValues[field]) return _mpAC.cache.fieldValues[field];
  if (_mpAC.cache.fieldValuesPending && _mpAC.cache.fieldValuesPending[field]) {
    return _mpAC.cache.fieldValuesPending[field];
  }
  if (!_mpAC.cache.fieldValuesPending) _mpAC.cache.fieldValuesPending = {};
  const pending = (async () => {
    try {
      const params = new URLSearchParams({ path: field, limit: '200' });
      const res = await fetch(`/api/metric_field_values?${params.toString()}`);
      const data = await res.json();
      const values = (data && data.values) || [];
      _mpAC.cache.fieldValues[field] = values;
      return values;
    } catch (_) {
      _mpAC.cache.fieldValues[field] = [];
      return [];
    } finally {
      delete _mpAC.cache.fieldValuesPending[field];
    }
  })();
  _mpAC.cache.fieldValuesPending[field] = pending;
  return pending;
}

async function _mpAcValueItemsRemote(field, method, partial) {
  // Run-name / run-hash use the dedicated search endpoint so we can do
  // mode-aware matching (contains/startswith/endswith) and get nice hints.
  if (field === 'run.name' || field === 'run.hash' || field === '') {
    if (!partial || partial.length < 2) return [];
    const mode = (method === 'startswith' || method === 'endswith') ? method : 'contains';
    try {
      const params = new URLSearchParams({ q: partial, mode, limit: '12' });
      if (!_mpQueryOptsIntoExcludedTags()) params.set('exclude_tags', _MP_DEFAULT_EXCLUDED_RUN_TAGS.join(','));
      const res = await fetch(`/api/runs_by_name?${params.toString()}`);
      const data = await res.json();
      return (data.runs || []).map(r => ({
        kind: 'value',
        id: r.run_name,
        label: r.run_name,
        hint: `${r.run_hash.slice(0, 8)} · ${r.cluster}${r.project ? ' · ' + r.project : ''}`,
        insert: r.run_name,
        cursorBack: 0,
      }));
    } catch (_) { return []; }
  }

  // Other catalog fields → fetch all distinct values once, then filter
  // client-side. This makes typing fast and works even when no runs are
  // loaded on the page yet.
  if (!_mpIsCatalogField(field)) return [];
  const values = await _mpAcFetchFieldValues(field);
  const p = String(partial || '').toLowerCase();
  const filtered = p ? values.filter(v => v.toLowerCase().includes(p)) : values;
  return filtered.slice(0, 20).map(v => ({
    kind: 'value', id: v, label: v,
    hint: field, insert: v, cursorBack: 0,
  }));
}

async function _mpAcBuildItems(value, cursor) {
  const ctx = _mpAcContext(value, cursor);
  if (ctx.kind === 'fresh') {
    const fields = _mpAcFieldItems('');
    const snippets = _mpAcSnippets();
    return { items: [...snippets, ...fields.slice(0, 6)], replaceStart: cursor, replaceEnd: cursor };
  }
  if (ctx.kind === 'field') {
    const fields = _mpAcFieldItems(ctx.partial);
    // If the partial is an exact field, also surface method/operator templates
    const exact = _mpAllFields().find(f => f.id === ctx.partial);
    const opItems = exact ? _mpAcOperatorItems(exact.id) : [];
    return { items: [...opItems, ...fields], replaceStart: ctx.replaceStart, replaceEnd: ctx.replaceEnd };
  }
  if (ctx.kind === 'after_term') {
    return { items: _mpAcKeywordItems(), replaceStart: ctx.replaceStart, replaceEnd: ctx.replaceEnd };
  }
  if (ctx.kind === 'value_string' || ctx.kind === 'value_after_op') {
    const local = _mpAcValueItemsLocal(ctx.field, ctx.partial);
    const remote = await _mpAcValueItemsRemote(ctx.field, ctx.method, ctx.partial);
    const seen = new Set(local.map(i => i.id));
    const merged = [...local, ...remote.filter(r => !seen.has(r.id))].slice(0, 20);
    return { items: merged, replaceStart: ctx.replaceStart, replaceEnd: ctx.replaceEnd, field: ctx.field };
  }
  return { items: [], replaceStart: cursor, replaceEnd: cursor };
}

async function _mpQueryInputChanged(event) {
  const input = event && event.target ? event.target : document.getElementById('mp-query-input');
  if (!input) return;
  _mpState.query = input.value;
  const cursor = input.selectionStart || input.value.length;
  const token = ++_mpAC.loadingToken;
  const { items, replaceStart, replaceEnd } = await _mpAcBuildItems(input.value, cursor);
  if (token !== _mpAC.loadingToken) return;
  _mpAC.items = items;
  _mpAC.open = items.length > 0;
  _mpAC.index = 0;
  _mpAC.replaceStart = replaceStart;
  _mpAC.replaceEnd = replaceEnd;
  _mpRenderAutocomplete();
}

function _mpQueryKeyDown(event) {
  if (_mpAC.open && _mpAC.items.length) {
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      _mpAC.index = Math.min(_mpAC.items.length - 1, _mpAC.index + 1);
      _mpRenderAutocomplete(); return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      _mpAC.index = Math.max(0, _mpAC.index - 1);
      _mpRenderAutocomplete(); return;
    }
    if (event.key === 'Tab' || (event.key === 'Enter' && !event.shiftKey)) {
      event.preventDefault();
      _mpAcApply(_mpAC.index);
      return;
    }
    if (event.key === 'Escape') {
      _mpAC.open = false; _mpRenderAutocomplete();
      event.preventDefault();
      return;
    }
  }
  if (event.key === 'Enter') {
    event.preventDefault();
    const input = event.target;
    _mpRunQuery(input.value);
  }
}

function _mpAcApply(idx) {
  const item = _mpAC.items[idx];
  if (!item) return;
  const input = document.getElementById('mp-query-input');
  if (!input) return;
  const v = input.value;
  const before = v.slice(0, _mpAC.replaceStart);
  const after = v.slice(_mpAC.replaceEnd);
  const next = before + item.insert + after;
  input.value = next;
  let pos = before.length + item.insert.length - (item.cursorBack || 0);
  try { input.setSelectionRange(pos, pos); } catch (_) {}
  input.focus();
  _mpState.query = next;
  // Refresh suggestions at new cursor position so chain-typing keeps working.
  _mpQueryInputChanged({ target: input });
}

function _mpRenderAutocomplete() {
  const el = document.getElementById('mp-ac-list');
  if (!el) return;
  if (!_mpAC.open || !_mpAC.items.length) {
    el.classList.remove('open'); el.innerHTML = '';
    return;
  }
  el.classList.add('open');
  el.innerHTML = _mpAC.items.map((item, idx) => `
    <button type="button" class="mp-ac-item${idx === _mpAC.index ? ' active' : ''} mp-ac-kind-${escAttr(item.kind)}"
            onmousedown="event.preventDefault();_mpAcApply(${idx})">
      <span class="mp-ac-kind">${_escHtml(item.kind)}</span>
      <span class="mp-ac-label" title="${escAttr(item.label)}">${_escHtml(item.label)}</span>
      <span class="mp-ac-hint">${_escHtml(item.hint || '')}</span>
    </button>`).join('');
}

function _mpSetMetricsFilter(value) {
  _mpState.metricsFilter = value || '';
  _mpRender();
}

function _mpToggleMetric(key) {
  if (_mpState.selectedMetrics.includes(key)) _mpRemoveMetric(key);
  else _mpAddMetric(key);
}

function _mpAddMetric(key) {
  if (!key || _mpState.selectedMetrics.includes(key)) return;
  _mpState.selectedMetrics = [..._mpState.selectedMetrics, key];
  _mpReplaceUrl();
  _mpRender();
}

function _mpRemoveMetric(key) {
  _mpState.selectedMetrics = _mpState.selectedMetrics.filter(k => k !== key);
  _mpReplaceUrl();
  _mpRender();
}

function _mpSelectAllMetrics() {
  const all = Array.from(new Set(_mpState.records.filter(r => r.numeric).map(r => r.key))).sort();
  _mpState.selectedMetrics = all;
  _mpReplaceUrl();
  _mpRender();
}

function _mpClearMetrics() {
  _mpState.selectedMetrics = [];
  _mpReplaceUrl();
  _mpRender();
}

function _mpOnQueryInput(value) {
  _mpState.query = value || '';
}

function _mpSetQuery(value) {
  _mpState.query = value || '';
  _mpReplaceUrl();
  _mpRender();
}

// "Search" button handler: applies the AimQL filter AND discovers any runs
// referenced by run.name / run.hash patterns server-side, so users don't have
// to add each run by hash explicitly.
async function _mpRunQuery(value) {
  _mpState.query = value || '';
  _mpReplaceUrl();
  const { ast, error } = _mpCompileQuery(_mpState.query);
  if (error) { _mpRender(); return; }
  const terms = _mpExtractRunSearchTerms(ast);
  if (!terms.length) { _mpRender(); return; }
  // Show "Discovering…" briefly
  _mpState.discovering = true;
  _mpRender();
  try {
    const seen = new Set(_mpState.runs.map(r => _mpRunKey(r).toLowerCase()));
    const found = [];
    for (const term of terms) {
      const params = new URLSearchParams({ q: term.value, mode: term.mode, limit: '100' });
      if (!_mpQueryOptsIntoExcludedTags()) params.set('exclude_tags', _MP_DEFAULT_EXCLUDED_RUN_TAGS.join(','));
      try {
        const res = await fetch(`/api/runs_by_name?${params.toString()}`);
        const data = await res.json();
        for (const r of (data.runs || [])) {
          if (!r.run_hash) continue;
          const key = `${r.cluster}/${r.run_hash}`.toLowerCase();
          if (seen.has(key)) continue;
          seen.add(key);
          found.push({ cluster: r.cluster, runHash: r.run_hash });
        }
      } catch (_) {}
    }
    if (found.length) {
      _mpState.runs = [..._mpState.runs, ...found];
      _mpState.discovering = false;
      _mpReplaceUrl();
      _mpRender();
      _mpLoadRuns();
      toast(`Discovered ${found.length} run${found.length === 1 ? '' : 's'} from query`);
    } else {
      _mpState.discovering = false;
      _mpRender();
      if (!_mpState.runs.length) {
        toast(`No runs match: ${terms.map(t => `${t.mode}("${t.value}")`).join(', ')}`, 'error');
      }
    }
  } catch (e) {
    _mpState.discovering = false;
    _mpRender();
    toast('Discovery failed: ' + (e.message || e), 'error');
  }
}

// Walk an AimQL AST and return literal terms attached to `run.name` /
// `run.hash` / `run.run_name` patterns. Each entry is `{ value, mode }`
// where `mode` matches the /api/runs_by_name `mode` param.
function _mpExtractRunSearchTerms(ast) {
  const out = [];
  const seen = new Set();
  const push = (value, mode) => {
    if (typeof value !== 'string' || !value || value.length < 2) return;
    const key = `${mode}::${value.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ value, mode });
  };
  const isRunNameField = (path) => path === 'run.name' || path === 'run.run_name';
  function walk(node) {
    if (!node || typeof node !== 'object') return;
    if (node.type === 'method' && isRunNameField(node.field) && node.arg && node.arg.type === 'lit') {
      const mode = node.method === 'startswith' ? 'startswith'
                 : node.method === 'endswith' ? 'endswith'
                 : 'contains';
      push(node.arg.value, mode);
    }
    if (node.type === 'cmp' && node.op === '==' && node.left && node.left.type === 'field'
        && isRunNameField(node.left.path) && node.right && node.right.type === 'lit') {
      push(node.right.value, 'equals');
    }
    walk(node.left); walk(node.right); walk(node.child);
    if (Array.isArray(node.items)) node.items.forEach(walk);
  }
  walk(ast);
  return out;
}

function _mpClearQuery() {
  _mpState.query = '';
  _mpReplaceUrl();
  _mpRender();
}

function _mpLabelPickerHtml() {
  const selected = _mpState.traceLabelFields || [];
  const fields = _mpGroupingFields();
  const selectedSet = new Set(selected);
  const chipText = selected.length ? selected.map(f => _mpFieldLabel(f)).join(' · ') : 'default';
  return `<div class="mp-group-picker">
    <button type="button" class="mp-group-btn${selected.length && !(selected.length === 1 && selected[0] === 'run.name') ? ' active' : ''}"
            onclick="_mpToggleLabelDropdown(event)">
      <span class="mp-group-btn-label">Run</span>
      <span class="mp-group-btn-text">${_escHtml(chipText)}</span>
      <span class="mp-group-btn-caret">▾</span>
    </button>
    <div class="mp-group-dropdown" id="mp-group-dd-label">
      <div class="mp-group-dd-head">Trace label fields</div>
      <div class="mp-group-dd-list">
        ${fields.map(f => `
          <label class="mp-group-option${selectedSet.has(f.id) ? ' selected' : ''}">
            <input type="checkbox" ${selectedSet.has(f.id) ? 'checked' : ''}
                   onchange="_mpToggleLabelField('${escAttr(f.id)}')">
            <span class="mp-group-option-id">${_escHtml(f.id)}</span>
            <span class="mp-group-option-kind">${_escHtml(f.kind)}</span>
          </label>`).join('')}
      </div>
      <div class="mp-group-dd-foot">
        <button class="mp-link" onclick="_mpResetLabelFields()">reset</button>
        <button class="mp-link" onclick="_mpToggleLabelDropdown()">done</button>
      </div>
    </div>
  </div>`;
}

function _mpToggleLabelDropdown(event) {
  if (event) event.stopPropagation();
  document.querySelectorAll('.mp-group-dropdown').forEach(el => {
    if (el.id === 'mp-group-dd-label') el.classList.toggle('open');
    else el.classList.remove('open');
  });
  _mpPositionRailDropdowns();
}

function _mpToggleLabelField(field) {
  const cur = _mpState.traceLabelFields || [];
  if (cur.includes(field)) {
    _mpState.traceLabelFields = cur.filter(f => f !== field);
  } else {
    _mpState.traceLabelFields = [...cur, field];
  }
  _mpReplaceUrl();
  _mpRender();
}

function _mpResetLabelFields() {
  _mpState.traceLabelFields = ['run.name'];
  _mpReplaceUrl();
  _mpRender();
}

function _mpGroupPickerHtml(group, label, selected) {
  const fields = _mpGroupingFields();
  const selectedSet = new Set(selected);
  const chipText = selected.length ? selected.map(f => _mpFieldLabel(f)).join(' · ') : 'none';
  return `<div class="mp-group-picker">
    <button type="button" class="mp-group-btn${selected.length ? ' active' : ''}" onclick="_mpToggleGroupDropdown('${group}', event)">
      <span class="mp-group-btn-label">${label}</span>
      <span class="mp-group-btn-text">${_escHtml(chipText)}</span>
      <span class="mp-group-btn-caret">▾</span>
    </button>
    <div class="mp-group-dropdown" id="mp-group-dd-${group}">
      <div class="mp-group-dd-head">${label} grouping</div>
      <div class="mp-group-dd-list">
        ${fields.map(f => `
          <label class="mp-group-option${selectedSet.has(f.id) ? ' selected' : ''}">
            <input type="checkbox" ${selectedSet.has(f.id) ? 'checked' : ''}
                   onchange="_mpToggleGroupField('${group}','${escAttr(f.id)}')">
            <span class="mp-group-option-id">${_escHtml(f.id)}</span>
            <span class="mp-group-option-kind">${_escHtml(f.kind)}</span>
          </label>`).join('')}
      </div>
      <div class="mp-group-dd-foot">
        <button class="mp-link" onclick="_mpClearGroupField('${group}')">clear</button>
        <button class="mp-link" onclick="_mpToggleGroupDropdown('${group}')">done</button>
      </div>
    </div>
  </div>`;
}

function _mpFieldLabel(id) {
  const f = _mpGroupingFields().find(x => x.id === id);
  return f ? f.id : id;
}

function _mpToggleGroupDropdown(group, event) {
  if (event) event.stopPropagation();
  document.querySelectorAll('.mp-group-dropdown').forEach(el => {
    if (el.id === `mp-group-dd-${group}`) el.classList.toggle('open');
    else el.classList.remove('open');
  });
  _mpPositionRailDropdowns();
}

function _mpToggleGroupField(group, field) {
  const current = _mpState.grouping[group] || [];
  if (current.includes(field)) {
    _mpState.grouping[group] = current.filter(f => f !== field);
  } else {
    _mpState.grouping[group] = [...current, field];
  }
  _mpReplaceUrl();
  _mpRender();
}

function _mpClearGroupField(group) {
  _mpState.grouping[group] = [];
  _mpReplaceUrl();
  _mpRender();
}

// ─── 9. Chart area (subplots + zoom + highlight) ─────────────────────

function _mpChartAreaHtml(matchingRecords) {
  const groups = _mpComputeChartGroups(matchingRecords);
  if (!groups.length) {
    return '<div class="mp-empty">Pick at least one metric using <b>+ Metrics</b> above.</div>';
  }
  return `<div class="mp-chart-grid" id="mp-chart-grid">
    ${groups.map((g, idx) => _mpChartCardHtml(g, idx)).join('')}
  </div>`;
}

// ─── Chart drag-and-drop reordering ──────────────────────────────────

let _mpDragChartKey = null;

function _mpAttachChartDragHandlers() {
  document.querySelectorAll('.mp-chart-card').forEach(card => {
    const head = card.querySelector('.mp-chart-head');
    if (!head) return;
    // Only the header is `draggable=true`. We want the visual ghost to be
    // the whole card, so we override the drag image on dragstart.
    head.addEventListener('dragstart', (e) => {
      _mpDragChartKey = card.getAttribute('data-chart-key');
      if (e.dataTransfer) {
        try {
          e.dataTransfer.effectAllowed = 'move';
          e.dataTransfer.setData('text/plain', _mpDragChartKey || '');
          e.dataTransfer.setDragImage(card, 20, 20);
        } catch (_) {}
      }
      requestAnimationFrame(() => card.classList.add('mp-chart-dragging'));
    });
    head.addEventListener('dragend', () => {
      card.classList.remove('mp-chart-dragging');
      document.querySelectorAll('.mp-chart-card.mp-chart-drag-over').forEach(c => {
        c.classList.remove('mp-chart-drag-over', 'mp-chart-drag-over-after');
      });
      _mpDragChartKey = null;
    });
    card.addEventListener('dragover', (e) => {
      if (!_mpDragChartKey || _mpDragChartKey === card.getAttribute('data-chart-key')) return;
      e.preventDefault();
      if (e.dataTransfer) e.dataTransfer.dropEffect = 'move';
      // Decide whether the drop will land BEFORE or AFTER this card based on
      // cursor position relative to the card centerline.
      const r = card.getBoundingClientRect();
      const before = (e.clientX - r.left) < r.width / 2;
      document.querySelectorAll('.mp-chart-card.mp-chart-drag-over').forEach(c => {
        if (c !== card) c.classList.remove('mp-chart-drag-over', 'mp-chart-drag-over-after');
      });
      card.classList.add('mp-chart-drag-over');
      card.classList.toggle('mp-chart-drag-over-after', !before);
    });
    card.addEventListener('dragleave', (e) => {
      // Only clear when leaving the whole card (relatedTarget outside).
      if (!card.contains(e.relatedTarget)) {
        card.classList.remove('mp-chart-drag-over', 'mp-chart-drag-over-after');
      }
    });
    card.addEventListener('drop', (e) => {
      e.preventDefault();
      const targetKey = card.getAttribute('data-chart-key');
      const dragKey = _mpDragChartKey;
      const after = card.classList.contains('mp-chart-drag-over-after');
      card.classList.remove('mp-chart-drag-over', 'mp-chart-drag-over-after');
      if (!dragKey || !targetKey || dragKey === targetKey) return;
      _mpReorderChart(dragKey, targetKey, after);
    });
  });
}

// Move `dragKey` so it lands just before or after `targetKey` in the chart
// order. Updates _mpState.chartOrder + persists + re-renders.
function _mpReorderChart(dragKey, targetKey, after) {
  // Start from the current DOM ordering so any keys not yet pinned in
  // _mpState.chartOrder also get captured.
  const currentKeys = Array.from(document.querySelectorAll('.mp-chart-card'))
    .map(c => c.getAttribute('data-chart-key'));
  const filtered = currentKeys.filter(k => k !== dragKey);
  let pos = filtered.indexOf(targetKey);
  if (pos < 0) {
    filtered.push(dragKey);
  } else {
    filtered.splice(after ? pos + 1 : pos, 0, dragKey);
  }
  _mpState.chartOrder = filtered;
  _mpReplaceUrl();
  _mpRender();
}

// Build the bucketed (key → records) chart groups and apply any user
// reordering. Used by both the HTML renderer and the Chart.js instance
// builder so the DOM cards line up 1:1 with the Chart instances.
function _mpComputeChartGroups(matchingRecords) {
  const selected = new Set(_mpState.selectedMetrics);
  const records = matchingRecords.filter(r => selected.has(r.key) && r.numeric);
  if (!records.length) return [];
  const chartGroups = new Map();
  records.forEach(r => {
    const key = _mpGroupKey(r, _mpState.grouping.chart) || 'all';
    if (!chartGroups.has(key)) chartGroups.set(key, { key, records: [], titleParts: _mpGroupKeyParts(r, _mpState.grouping.chart) });
    chartGroups.get(key).records.push(r);
  });
  const groups = Array.from(chartGroups.values());
  const order = _mpState.chartOrder || [];
  if (order.length) {
    const idx = new Map(order.map((k, i) => [k, i]));
    groups.sort((a, b) => {
      const ia = idx.has(a.key) ? idx.get(a.key) : Number.MAX_SAFE_INTEGER;
      const ib = idx.has(b.key) ? idx.get(b.key) : Number.MAX_SAFE_INTEGER;
      if (ia !== ib) return ia - ib;
      return a.key.localeCompare(b.key);
    });
  }
  return groups;
}

function _mpGroupKeyParts(record, fields) {
  const ctx = _mpQLBuildContext(record);
  return (fields || []).map(f => ({ id: f, value: _stringValueOr(_mpQLResolve(ctx, f), '∅') }));
}

function _mpChartCardHtml(group, idx) {
  const id = `mp-chart-${idx}`;
  const hasSeries = (group.records || []).some(r => r.kind === 'series');
  // Pretty-print grouping fields in the title:
  //  - metric.name           → just the value, bold (e.g. "accuracy")
  //  - metric.context.<k>    → "<k>=<v>" compact
  //  - everything else       → "id=value"
  const titleParts = group.titleParts && group.titleParts.length
    ? group.titleParts.map(p => {
        if (p.id === 'metric.name' || p.id === 'metric.kind') {
          return `<b>${_escHtml(p.value)}</b>`;
        }
        if (p.id.startsWith('metric.context.')) {
          const k = p.id.slice('metric.context.'.length);
          return `<span>${_escHtml(k)}=${_escHtml(p.value)}</span>`;
        }
        return `<span>${_escHtml(p.id)}=${_escHtml(p.value)}</span>`;
      }).join(' · ')
    : 'all selected metrics';

  // Trace-count breakdown: N runs × M context combos. Helps explain why a
  // metric like best_of_3_judge_correct (9 contexts/run) gives many bars.
  const traceCount = group.records.length;
  const runs = new Set(group.records.map(r => r.runHash)).size;
  const ctxSigs = new Set(group.records.map(r => r.contextSig)).size;
  const metrics = new Set(group.records.map(r => r.key)).size;
  let meta = `${traceCount} trace${traceCount === 1 ? '' : 's'}`;
  if (ctxSigs > 1 || runs > 1) {
    const parts = [];
    if (runs > 1) parts.push(`${runs} runs`);
    if (metrics > 1) parts.push(`${metrics} metrics`);
    if (ctxSigs > 1) parts.push(`${ctxSigs} contexts`);
    if (parts.length) meta = `${traceCount} traces · ${parts.join(' × ')}`;
  }

  // Surface useful context dimensions as one-click "split into subplots"
  // shortcuts. e.g. when there are 9 benchmark contexts → "split by
  // benchmark" creates 9 subplots with 1 run/metric each.
  const ctxKeyVariance = _mpContextKeyVariance(group.records);
  const splittable = Object.entries(ctxKeyVariance)
    .filter(([k, n]) => n > 1)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 4);
  const shortcuts = splittable.length && !_mpState.grouping.chart.length
    ? `<div class="mp-chart-shortcut">
         split into subplots by ${splittable.map(([k, n]) => `
           <button class="mp-link" onclick="_mpAddChartGrouping('metric.context.${escAttr(k)}')"
                   title="One subplot per metric.context.${escAttr(k)} value">
             ${_escHtml(k)} <span>(${n})</span>
           </button>`).join(' · ')}
       </div>`
    : '';

  return `<div class="mp-chart-card" data-chart-id="${escAttr(id)}" data-chart-key="${escAttr(group.key)}">
    <div class="mp-chart-head" draggable="true">
      <span class="mp-chart-drag-grip" title="Drag to reorder charts" aria-hidden="true">⋮⋮</span>
      <div class="mp-chart-title">${titleParts}</div>
      <div class="mp-chart-meta">${meta}</div>
      <div class="mp-chart-tools">
        ${hasSeries ? `<button class="mp-icon-btn" onclick="_mpResetZoom('${escAttr(id)}')" title="Reset zoom">⟲</button>` : ''}
        <button class="mp-icon-btn" onclick="_mpExportChart('${escAttr(id)}')" title="Export PNG">⤓</button>
      </div>
    </div>
    ${shortcuts}
    <div class="mp-chart-canvas-wrap">
      <canvas id="${escAttr(id)}"></canvas>
      <div class="mp-chart-zoom-rect" id="${escAttr(id)}-rect"></div>
    </div>
  </div>`;
}

// For each context key across the given records, count how many distinct
// values are present. Used to surface "split by X" suggestions.
function _mpContextKeyVariance(records) {
  const buckets = {};
  records.forEach(r => {
    Object.entries(r.context || {}).forEach(([k, v]) => {
      if (!buckets[k]) buckets[k] = new Set();
      buckets[k].add(String(v));
    });
  });
  const out = {};
  Object.entries(buckets).forEach(([k, set]) => { out[k] = set.size; });
  return out;
}

function _mpAddChartGrouping(field) {
  if (!_mpState.grouping.chart.includes(field)) {
    _mpState.grouping.chart = [..._mpState.grouping.chart, field];
  }
  _mpReplaceUrl();
  _mpRender();
}

// Stable label-first comparator used to keep bars/lines in the same y-axis
// (or legend) order across every chart subplot. Falls back to run hash so
// duplicate labels still order deterministically.
function _mpCompareTracesByLabel(a, b) {
  const la = String(a && a.label || '');
  const lb = String(b && b.label || '');
  const cmp = la.localeCompare(lb, undefined, { numeric: true, sensitivity: 'base' });
  if (cmp !== 0) return cmp;
  const ha = String(a && a.record && a.record.runHash || '');
  const hb = String(b && b.record && b.record.runHash || '');
  return ha.localeCompare(hb);
}

function _mpBuildTraces(records) {
  // Returns an array of traces. Each trace = one drawable line/point on a chart.
  const traces = [];
  const colorFields = _mpState.grouping.color;
  records.forEach(record => {
    const colorKey = _mpGroupKey(record, colorFields);
    const colorIdx = _mpRecordColorIdx(record);
    const color = METRICS_PALETTE[colorIdx];
    const label = _mpFormatTraceLabel(record);
    const fullLabel = _mpFormatTraceLabelVerbose(record);
    const patternIdx = _mpRecordPatternIdx(record);
    traces.push({
      id: `${record.cluster}/${record.runHash}/${record.key}/${record.contextSig}`,
      record,
      label,
      fullLabel,
      color,
      colorKey,
      patternIdx,
      chartKey: _mpGroupKey(record, _mpState.grouping.chart) || 'all',
    });
  });
  return traces;
}

function _mpContextLabel(ctx) {
  const keys = Object.keys(ctx || {});
  if (!keys.length) return '';
  return keys.sort().map(k => `${k}=${_stringValueOr(ctx[k], '∅')}`).join(',');
}

function _mpRenderCharts(matchingRecords) {
  if (typeof Chart === 'undefined') {
    if (_mpChartTimer) clearTimeout(_mpChartTimer);
    _mpChartTimer = setTimeout(() => { _mpChartTimer = null; _mpRender(); }, 80);
    return;
  }
  // Walk the same ordered groups the HTML renderer used so each Chart.js
  // instance lands on the right `mp-chart-<idx>` canvas after user drags.
  const orderedGroups = _mpComputeChartGroups(matchingRecords);
  const traces = _mpBuildTraces(matchingRecords.filter(r => new Set(_mpState.selectedMetrics).has(r.key) && r.numeric));
  // Bucket traces by chartKey for fast lookup
  const byChart = new Map();
  traces.forEach(t => {
    if (!byChart.has(t.chartKey)) byChart.set(t.chartKey, []);
    byChart.get(t.chartKey).push(t);
  });
  let idx = 0;
  for (const group of orderedGroups) {
    // Sort traces by their LABEL so the same label always lands at the
    // same y-position (or trace order on line charts) across every chart
    // subplot. Run-hash break-tie keeps duplicates deterministic.
    const tracesInChart = (byChart.get(group.key) || []).slice().sort(_mpCompareTracesByLabel);
    const id = `mp-chart-${idx}`;
    const canvas = document.getElementById(id);
    if (!canvas) { idx++; continue; }

    // Scalar-only chart: render as a clean horizontal bar chart so each
    // (run × context) gets a labeled row instead of being squashed into
    // overlapping scatter dots at x=1, 2, 3, …
    const allScalar = tracesInChart.length > 0 && tracesInChart.every(t => t.record.kind === 'scalars');
    if (allScalar) {
      _mpRenderScalarBarChart(canvas, tracesInChart, id);
    } else {
      const datasets = _mpDatasetsForTraces(tracesInChart, id);
      const chart = new Chart(canvas, {
        type: 'line',
        data: { datasets },
        options: _mpChartOptions(id),
        plugins: [_mpHighlightPlugin(), _mpCrosshairPlugin()],
      });
      chart._mpId = id;
      chart._mpTraces = tracesInChart;
      _mpState.charts.push(chart);
      _mpAttachZoomHandlers(canvas, chart, id);
      _mpAttachCrosshair(canvas, chart);
      _mpAttachHoverExit(canvas, chart);
    }
    idx++;
  }
}

// Compute optimal y-axis font size, per-label truncation, axis width and
// right-side padding for a horizontal bar chart so that:
//  * the full y-axis labels are shown when they fit (no left-clipping by the
//    canvas edge);
//  * font size scales down from 11px to 8px as labels get longer;
//  * labels get an end-ellipsis at the smallest font size when still too
//    wide (instead of being clipped at the left by the canvas);
//  * the right padding is just enough for the inline value labels (no
//    leftover whitespace).
function _mpFitBarLabels(canvas, labels, values) {
  const cw = canvas.offsetWidth || canvas.clientWidth || canvas.parentElement.offsetWidth || 800;
  // Cap the y-axis area at 55% of canvas width so the bars always have at
  // least 45% to live in. No artificial minimum — short labels (like a bare
  // "accuracy" with LABEL BY=metric.name) only get just-enough width.
  const yAxisTargetMax = Math.min(560, Math.round(cw * 0.55));
  const ctx = (canvas.getContext && canvas.getContext('2d')) || null;
  const fontFamily = 'Inter, system-ui, sans-serif';
  const candidates = [11, 10.5, 10, 9.5, 9, 8.5, 8];
  // Fallback ~0.55*fontSize per char if no canvas 2d context (e.g. headless).
  function widthAt(size, label) {
    if (!ctx) return label.length * size * 0.55;
    ctx.font = `${size}px ${fontFamily}`;
    return ctx.measureText(label).width;
  }
  function measure(label, size) {
    if (!ctx) return label.length * size * 0.55;
    ctx.font = `${size}px ${fontFamily}`;
    return ctx.measureText(label).width;
  }
  const longest = labels.reduce((a, b) => (a.length >= b.length ? a : b), '');
  let chosenFont = 8;
  for (const size of candidates) {
    if (widthAt(size, longest) + 24 <= yAxisTargetMax) { chosenFont = size; break; }
  }
  // Compute final axis width based on the labels we actually have at the
  // chosen font — just-enough, capped by the upper bound.
  let axisW = Math.min(
    yAxisTargetMax,
    Math.ceil(measure(longest, chosenFont) + 24),
  );
  // Per-label end-ellipsis when even chosenFont can't make a particular
  // label fit. Binary-search the cut.
  const fittedLabels = labels.map(l => {
    if (measure(l, chosenFont) + 24 <= axisW) return l;
    let lo = 1, hi = l.length;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      const candidate = l.slice(0, mid) + '…';
      if (measure(candidate, chosenFont) + 24 <= axisW) lo = mid;
      else hi = mid - 1;
    }
    return l.slice(0, lo) + '…';
  });
  // Right-side padding: just enough for the widest value label + a 12px gap.
  const maxValueText = values.reduce((max, v) => {
    const t = _mpFormatBarValue(v);
    return t.length > max.length ? t : max;
  }, '');
  const rightPad = Math.max(24, Math.ceil(measure(maxValueText, 10) + 14));
  return { labels: fittedLabels, fontSize: chosenFont, axisWidth: axisW, rightPad };
}

// Render a horizontal bar chart for a chart group that contains only scalar
// metrics. Each (run × metric × context) gets one labeled row. We also
// dynamically size the canvas so bars stay readable when there are many.
function _mpRenderScalarBarChart(canvas, traces, chartId) {
  // The trace list is already sorted by label upstream in _mpRenderCharts
  // (via _mpCompareTracesByLabel) so the same y-axis label appears at the
  // same row across every chart subplot. Preserve that order here.
  const items = traces
    .map(t => {
      const r = t.record;
      const p = (r.points || []).slice(-1)[0] || {};
      return {
        trace: t,
        value: Number.isFinite(p.value_num) ? p.value_num : null,
        label: t.label,
      };
    })
    .filter(i => i.value != null);
  if (!items.length) return;

  // Stretch the canvas wrap so each row has comfortable vertical breathing
  // room. Scales down for very many bars but never gets squashed; the chart
  // pane scrolls vertically.
  const wrap = canvas.parentElement;
  if (wrap) {
    const rowH = items.length > 400 ? 20
              : items.length > 200 ? 24
              : items.length > 80  ? 28
              :                       32;
    const desired = Math.max(420, items.length * rowH + 80);
    wrap.style.height = `${desired}px`;
  }

  const rawLabels = items.map(i => i.label);
  const fullLabels = items.map(i => i.trace.fullLabel || i.label);
  const values = items.map(i => i.value);
  const colors = items.map(i => i.trace.color);
  const patternIdxs = items.map(i => i.trace.patternIdx || 0);
  const traceIds = items.map(i => i.trace.id);
  const runHashes = items.map(i => i.trace.record.runHash);
  const metricKeys = items.map(i => i.trace.record.key);
  const hidden = items.map(i => !!_mpState.hiddenTraces[i.trace.id]);

  // Build per-bar fills: either the solid color (pattern idx 0) or a
  // repeating CanvasPattern painted in the trace color. The off-screen
  // tiles are tiny and freshly allocated per render — cheap enough.
  const canvasCtx = canvas.getContext('2d');
  const barFills = colors.map((c, i) => {
    const pIdx = patternIdxs[i] % _mpPatternFactories.length;
    const factory = _mpPatternFactories[pIdx];
    if (!factory || !canvasCtx) return c;
    try { return factory(canvasCtx, c) || c; } catch (_) { return c; }
  });

  // Dynamically size the y-axis font + truncate per-label so the FULL label
  // is shown when it fits, and ellipsized at the END (not clipped at the
  // canvas edge) otherwise. Also compute exactly how much right-side padding
  // the value labels need so we don't waste space.
  const fit = _mpFitBarLabels(canvas, rawLabels, values);

  const chart = new Chart(canvas, {
    type: 'bar',
    data: {
      labels: fit.labels,
      datasets: [{
        label: 'value',
        data: values.map((v, i) => hidden[i] ? null : v),
        backgroundColor: barFills,
        borderColor: colors,
        borderWidth: 1,
        // Roomier bar geometry: bar = 55% of its category slot, ~45% gap.
        barPercentage: 0.55,
        categoryPercentage: 0.9,
        borderRadius: 2,
        _traceIds: traceIds,
        _runHashes: runHashes,
        _metricKeys: metricKeys,
        _origColors: colors.slice(),
        _origPatterns: barFills.slice(),
        _patternIdxs: patternIdxs.slice(),
        // Keep the verbose label around so the tooltip / external HTML
        // tooltip can show the full identifier even when the y-axis short
        // label is something terse like just "aws-cmh".
        _fullLabels: fullLabels,
      }],
    },
    options: _mpBarChartOptions(chartId, items.length, fit),
    plugins: [_mpHighlightPlugin(), _mpBarValueLabelPlugin()],
  });
  chart._mpId = chartId;
  chart._mpTraces = traces;
  chart._mpKind = 'bar';
  _mpState.charts.push(chart);
  _mpAttachHoverExit(canvas, chart);

  // Double-click a bar → scroll to its context-table row.
  canvas.addEventListener('dblclick', (event) => {
    const els = chart.getElementsAtEventForMode(event, 'nearest', { intersect: true }, false);
    const el = els && els[0];
    if (!el) return;
    const ds = chart.data.datasets[el.datasetIndex];
    const tid = ds && Array.isArray(ds._traceIds) ? ds._traceIds[el.index] : null;
    if (tid) _mpScrollToTableRow(tid);
  });
}

function _mpDatasetsForTraces(traces, chartId) {
  const out = [];
  traces.forEach(t => {
    const r = t.record;
    const traceId = t.id;
    const hidden = !!_mpState.hiddenTraces[traceId];
    // Scalars in a mixed chart: single scatter dot per trace. (Pure-scalar
    // charts are handled by _mpRenderScalarBarChart and never reach here.)
    if (r.kind === 'scalars') {
      const p = (r.points || []).slice(-1)[0] || {};
      if (!Number.isFinite(p.value_num)) return;
      out.push({
        type: 'scatter', label: t.label,
        data: [{ x: out.length + 1, y: p.value_num }],
        borderColor: t.color, backgroundColor: t.color + 'cc',
        pointRadius: 5, pointHoverRadius: 8,
        showLine: false, hidden,
        _traceId: traceId, _color: t.color, _runHash: r.runHash, _metricKey: r.key, _chartId: chartId,
      });
      return;
    }
    const rawPoints = _mpState.ignoreOutliers ? _mpFilterOutliers(r.points) : r.points;
    const axisPoints = _mpPointsForAxis(rawPoints);
    if (!axisPoints.length) return;
    const downsampled = _mpDownsample(axisPoints, 2200);
    if (_mpState.showRaw && _mpState.smoothing > 0) {
      out.push({
        label: `${t.label} (raw)`,
        data: downsampled,
        borderColor: t.color + '55', backgroundColor: 'transparent',
        borderWidth: 1, pointRadius: 0, tension: 0, hidden,
        _traceId: `${traceId}::raw`, _color: t.color, _muted: true,
        _runHash: r.runHash, _metricKey: r.key, _chartId: chartId,
      });
    }
    const dashIdx = (t.patternIdx || 0) % METRICS_LINE_DASHES.length;
    out.push({
      label: t.label,
      _fullLabel: t.fullLabel,
      data: _mpSmooth(downsampled, _mpState.smoothing),
      borderColor: t.color,
      backgroundColor: 'transparent',
      // Aim uses 1.5px lines with 2.8px on highlight and 3.0px on active.
      borderWidth: 1.5,
      borderDash: METRICS_LINE_DASHES[dashIdx],
      pointRadius: 0,
      pointHoverRadius: 4.5,
      pointHitRadius: 10,
      pointHoverBackgroundColor: '#ffffff',
      pointHoverBorderColor: t.color,
      pointHoverBorderWidth: 2.4,
      tension: 0,
      hidden,
      _traceId: traceId, _color: t.color, _patternIdx: dashIdx,
      _runHash: r.runHash, _metricKey: r.key, _chartId: chartId,
    });
  });
  return out;
}

function _mpFilterOutliers(points) {
  if (typeof _runPageFilterOutlierPoints === 'function') return _runPageFilterOutlierPoints(points);
  return points || [];
}

function _mpDownsample(points, max) {
  if (typeof _runPageDownsample === 'function') return _runPageDownsample(points, max);
  return points || [];
}

function _mpSmooth(points, amount) {
  if (typeof _runPageSmoothPoints === 'function') return _runPageSmoothPoints(points, amount);
  return points || [];
}

function _mpPointsForAxis(points) {
  const firstTs = (points || []).find(p => Number.isFinite(p.ts))?.ts || 0;
  const out = [];
  (points || []).forEach((p, idx) => {
    if (!Number.isFinite(p.value_num)) return;
    let x = idx + 1;
    if (_mpState.align === 'step') x = p.step == null ? idx + 1 : p.step;
    else if (_mpState.align === 'wall_time') x = Number.isFinite(p.ts) && firstTs ? (p.ts - firstTs) / 60 : idx + 1;
    if (_mpState.yScale === 'logarithmic' && p.value_num <= 0) return;
    out.push({ x, y: p.value_num, raw: p });
  });
  return out;
}

// External tooltip — renders the Chart.js tooltip as an HTML div appended to
// the body so it can extend past the canvas / chart card edges (the default
// canvas-drawn tooltip gets clipped). Clamped to the viewport.
function _mpExternalTooltip(context) {
  const { chart, tooltip } = context;
  let el = document.getElementById('mp-tooltip');
  if (!el) {
    el = document.createElement('div');
    el.id = 'mp-tooltip';
    el.className = 'mp-tooltip';
    document.body.appendChild(el);
  }
  if (!tooltip || tooltip.opacity === 0) {
    el.style.opacity = '0';
    el.style.pointerEvents = 'none';
    return;
  }
  const titleLines = tooltip.title || [];
  const bodyEntries = tooltip.body || [];
  const colorEntries = tooltip.labelColors || [];
  const titleHtml = titleLines
    .filter(Boolean)
    .map(t => `<div class="mp-tooltip-title">${_escHtml(t)}</div>`).join('');
  const bodyHtml = bodyEntries.map((b, i) => {
    const lines = (b.lines || []).map(line => _escHtml(line)).join('<br>');
    const c = colorEntries[i];
    const swatch = c
      ? `<span class="mp-tooltip-swatch" style="background:${c.borderColor || c.backgroundColor || '#888'}"></span>`
      : '';
    return `<div class="mp-tooltip-row">${swatch}<span class="mp-tooltip-text">${lines}</span></div>`;
  }).join('');
  el.innerHTML = `${titleHtml}${bodyHtml}`;
  // Position. caretX/Y are relative to the canvas; the canvas itself can be
  // anywhere on screen. We want the tooltip just to the right of the cursor,
  // flipped to the left if it would overflow the viewport.
  const canvasRect = chart.canvas.getBoundingClientRect();
  // Force a layout pass to get accurate offsetWidth/Height after innerHTML.
  el.style.left = '-9999px';
  el.style.top = '0px';
  el.style.opacity = '0';
  const tw = el.offsetWidth;
  const th = el.offsetHeight;
  const cursorX = canvasRect.left + tooltip.caretX;
  const cursorY = canvasRect.top + tooltip.caretY;
  let left = cursorX + 12;
  let top = cursorY + 12;
  if (left + tw + 8 > window.innerWidth) left = cursorX - tw - 12;
  if (left < 8) left = 8;
  if (top + th + 8 > window.innerHeight) top = cursorY - th - 12;
  if (top < 8) top = 8;
  el.style.left = `${left}px`;
  el.style.top = `${top}px`;
  el.style.opacity = '1';
}

function _mpHideExternalTooltip() {
  const el = document.getElementById('mp-tooltip');
  if (el) { el.style.opacity = '0'; el.innerHTML = ''; }
}

// Aim-style chart colors. Resolved once per render. Matches the navy text /
// thin axes Aim uses; gracefully degrades for dark theme via CSS variables.
function _mpChartColors() {
  const cs = getComputedStyle(document.documentElement);
  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  return {
    text:    isDark ? (cs.getPropertyValue('--text').trim() || '#e0e3ee')   : '#1c2852',
    muted:   isDark ? (cs.getPropertyValue('--muted').trim() || '#a4adc3')  : '#414b6d',
    axisDom: isDark ? '#3d465e' : '#414b6d',
    axisTick:isDark ? '#586386' : '#8E9BAE',
    grid:    isDark ? 'rgba(255,255,255,0.05)' : 'rgba(28,40,82,0.06)',
    bg:      isDark ? (cs.getPropertyValue('--surface').trim() || '#1b1f2b') : '#ffffff',
    tipBg:   isDark ? '#1b1f2b' : '#ffffff',
    tipBorder: isDark ? '#3d465e' : '#dee6f3',
    tipText: isDark ? '#e0e3ee' : '#1c2852',
    crosshair: isDark ? 'rgba(160,170,200,0.45)' : 'rgba(28,40,82,0.32)',
    valuePillBg: isDark ? '#243969' : '#f7faff',
    valuePillBorder: isDark ? '#5b6789' : '#7a94cc',
    valuePillText: isDark ? '#e0e3ee' : '#243969',
  };
}

function _mpChartOptions(chartId) {
  const C = _mpChartColors();
  const xTitle = _mpState.align === 'wall_time' ? 'minutes since first point'
                : _mpState.align === 'index' ? 'point index' : 'step';
  return {
    responsive: true, maintainAspectRatio: false,
    animation: false, normalized: true, parsing: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        enabled: false,
        external: _mpExternalTooltip,
        mode: 'nearest', intersect: false,
        callbacks: {
          title(items) {
            const first = items && items[0];
            if (!first) return '';
            const x = first.parsed && first.parsed.x != null ? first.parsed.x : first.label;
            return `${xTitle}: ${_formatMetricValue(x)}`;
          },
          label(ctx) {
            const y = ctx.parsed && ctx.parsed.y != null ? _formatMetricValue(ctx.parsed.y) : '';
            const lbl = (ctx.dataset && ctx.dataset._fullLabel) || ctx.dataset.label;
            return `${lbl}: ${y}`;
          },
        },
      },
    },
    transitions: {
      active: { animation: { duration: 0 } },
      resize: { animation: { duration: 0 } },
    },
    elements: {
      line: { tension: 0, borderJoinStyle: 'round' },
      point: {
        radius: 0,
        hoverRadius: 4.5,
        hitRadius: 10,
        hoverBorderWidth: 2.4,
        hoverBackgroundColor: '#ffffff',
      },
    },
    scales: {
      x: {
        type: 'linear',
        min: _mpState.xRange ? _mpState.xRange[0] : undefined,
        max: _mpState.xRange ? _mpState.xRange[1] : undefined,
        title: { display: true, text: xTitle, color: C.muted,
                 font: { family: 'Inter', size: 10, weight: '500' } },
        ticks: { color: C.axisDom, font: { family: 'Inter', size: 10 },
                 maxTicksLimit: 9, autoSkip: true, padding: 6 },
        grid: { color: C.grid, drawTicks: false, lineWidth: 1 },
        border: { color: C.axisDom, width: 0.5 },
      },
      y: {
        type: _mpState.yScale || 'linear',
        min: _mpState.yRange ? _mpState.yRange[0] : undefined,
        max: _mpState.yRange ? _mpState.yRange[1] : undefined,
        ticks: { color: C.axisDom, font: { family: 'Inter', size: 10 },
                 maxTicksLimit: 7, padding: 6 },
        grid: { color: C.grid, drawTicks: false, lineWidth: 1 },
        border: { color: C.axisDom, width: 0.5 },
      },
    },
    interaction: { mode: 'nearest', intersect: false, axis: 'xy' },
    color: C.text,
    onHover: (event, elements, chart) => _mpOnChartHover(chart, elements, event),
  };
}

function _mpBarChartOptions(chartId, n, fit) {
  const C = _mpChartColors();
  const fontSize = (fit && fit.fontSize) || 10;
  const rightPad = (fit && fit.rightPad) || 72;
  const axisWidth = (fit && fit.axisWidth) || null;
  return {
    indexAxis: 'y',
    responsive: true, maintainAspectRatio: false,
    animation: false,
    // Right padding just covers the widest inline value label so we don't
    // leave large empty space on the right.
    layout: { padding: { right: rightPad } },
    plugins: {
      legend: { display: false },
      tooltip: {
        enabled: false,
        external: _mpExternalTooltip,
        mode: 'nearest', intersect: true,
        callbacks: {
          title(items) {
            const first = items && items[0];
            if (!first) return '';
            // Prefer the full pre-truncation label stored on the dataset.
            const ds = first.chart && first.chart.data.datasets[first.datasetIndex];
            const full = ds && ds._fullLabels && ds._fullLabels[first.dataIndex];
            return String(full || first.label || '');
          },
          label(ctx) {
            const v = ctx.parsed && ctx.parsed.x != null ? ctx.parsed.x : ctx.raw;
            return `value: ${_formatMetricValue(v)}`;
          },
        },
      },
    },
    transitions: { active: { animation: { duration: 0 } }, resize: { animation: { duration: 0 } } },
    scales: {
      x: {
        type: _mpState.yScale === 'logarithmic' ? 'logarithmic' : 'linear',
        beginAtZero: true,
        ticks: { color: C.axisDom, font: { family: 'Inter', size: 10 },
                 maxTicksLimit: 6, padding: 6 },
        grid: { color: C.grid, drawTicks: false, lineWidth: 1 },
        border: { color: C.axisDom, width: 0.5 },
      },
      y: {
        type: 'category',
        afterFit(scale) {
          // Force a minimum width so long labels aren't clipped by Chart.js's
          // auto layout. The width was pre-computed in _mpFitBarLabels using
          // the chosen font size.
          if (axisWidth && scale.width < axisWidth) scale.width = axisWidth;
        },
        ticks: {
          color: C.axisDom,
          autoSkip: false,
          font: { family: 'Inter', size: fontSize },
          padding: 6,
          callback(value) {
            // Labels are pre-truncated in _mpFitBarLabels using a per-row
            // binary search, so we can just return them as-is here.
            return String(this.getLabelForValue(value) || '');
          },
        },
        grid: { display: false },
        border: { color: C.axisDom, width: 0.5 },
      },
    },
    interaction: { mode: 'nearest', intersect: true, axis: 'y' },
    color: C.text,
    onHover: (event, elements, chart) => _mpOnBarHover(chart, elements),
  };
}

function _mpOnBarHover(chart, elements) {
  if (_mpState.highlightMode === 'off') {
    if (_mpState.hoveredTrace !== null) { _mpState.hoveredTrace = null; chart.update('none'); }
    return;
  }
  const el = (elements || [])[0];
  if (!el) {
    if (_mpState.hoveredTrace !== null) { _mpState.hoveredTrace = null; chart.update('none'); }
    return;
  }
  const ds = chart.data.datasets[el.datasetIndex];
  const idx = el.index;
  const next = {
    traceId: ds._traceIds[idx],
    runHash: ds._runHashes[idx],
    metricKey: ds._metricKeys[idx],
  };
  if (!_mpHoverEq(_mpState.hoveredTrace, next)) { _mpState.hoveredTrace = next; chart.update('none'); }
}


function _mpOnChartHover(chart, elements, event) {
  // Capture cursor position so the crosshair plugin can draw guide lines.
  if (event && Number.isFinite(event.x) && Number.isFinite(event.y)) {
    chart._mpHover = { x: event.x, y: event.y };
  }
  if (_mpState.highlightMode === 'off') {
    if (_mpState.hoveredTrace !== null) {
      _mpState.hoveredTrace = null; chart.update('none');
    } else { chart.draw(); }
    return;
  }
  const el = (elements || [])[0];
  if (!el) {
    if (_mpState.hoveredTrace !== null) {
      _mpState.hoveredTrace = null; chart.update('none');
    } else { chart.draw(); }
    return;
  }
  const ds = chart.data.datasets[el.datasetIndex];
  const next = ds && ds._traceId ? { traceId: ds._traceId, runHash: ds._runHash, metricKey: ds._metricKey } : null;
  const prev = _mpState.hoveredTrace;
  if (!_mpHoverEq(prev, next)) {
    _mpState.hoveredTrace = next; chart.update('none');
  } else { chart.draw(); }
}

function _mpClearHoveredTrace(chart) {
  let changed = false;
  if (_mpState.hoveredTrace !== null) {
    _mpState.hoveredTrace = null;
    changed = true;
  }
  if (chart && chart._mpHover) {
    chart._mpHover = null;
    changed = true;
  }
  if (changed && chart) chart.update('none');
}

// Listen for raw pointer exit so highlight dimming clears when the cursor
// leaves the chart canvas. Chart.js `onHover` doesn't reliably fire for
// canvas exit, especially on scalar bar charts.
function _mpAttachHoverExit(canvas, chart) {
  const clear = () => _mpClearHoveredTrace(chart);
  canvas.addEventListener('mouseleave', clear);
  canvas.addEventListener('pointerleave', clear);
  canvas.addEventListener('pointercancel', clear);
}

// Listen for raw mouseleave so the crosshair clears when the cursor leaves
// the canvas. Chart.js `onHover` doesn't fire for mouseleave.
function _mpAttachCrosshair(canvas, chart) {
  canvas.addEventListener('mouseleave', () => {
    if (chart._mpHover) _mpClearHoveredTrace(chart);
  });
}

function _mpHoverEq(a, b) {
  if (!a && !b) return true;
  if (!a || !b) return false;
  return a.traceId === b.traceId;
}

// Highlight plugin: dims datasets that don't match the hovered trace by run
// hash (run mode) or by trace id (metric mode). Matches Aim's behavior —
// non-hovered lines drop to 1.5px @ 0.2 opacity; the hovered line bumps to
// 2.8px @ full opacity. Handles both line/scatter datasets (one trace per
// dataset) and bar datasets (many traces per dataset, color per index).
function _mpHighlightPlugin() {
  return {
    id: 'mpHighlight',
    beforeDatasetsDraw(chart) {
      const ht = _mpState.hoveredTrace;
      const mode = _mpState.highlightMode;
      const ds = chart.data.datasets || [];
      ds.forEach(d => {
        if (Array.isArray(d._traceIds)) {
          // Bar dataset with per-index trace ids.
          const colorOrig = d._origColors || (Array.isArray(d.backgroundColor) ? d.backgroundColor.slice() : []);
          const patternOrig = d._origPatterns || colorOrig;
          if (!d._origColors) d._origColors = colorOrig;
          if (!d._origPatterns) d._origPatterns = patternOrig;
          d.backgroundColor = d._traceIds.map((tid, i) => {
            const c = d._origColors[i];
            let dim = false;
            if (ht && mode !== 'off') {
              if (mode === 'run' && d._runHashes[i] !== ht.runHash) dim = true;
              else if (mode === 'metric' && tid !== ht.traceId) dim = true;
            }
            // When un-dimmed, prefer the pattern fill; when dimmed, drop
            // to a flat low-alpha colour so dim bars stay visually quiet.
            return dim ? _mpAlpha(c, 0.18) : d._origPatterns[i];
          });
          d.borderColor = d._traceIds.map((tid, i) => {
            const c = d._origColors[i];
            let dim = false;
            if (ht && mode !== 'off') {
              if (mode === 'run' && d._runHashes[i] !== ht.runHash) dim = true;
              else if (mode === 'metric' && tid !== ht.traceId) dim = true;
            }
            return dim ? _mpAlpha(c, 0.3) : c;
          });
          return;
        }
        const c = d._color;
        if (!c) return;
        let dim = false;
        let active = false;
        if (ht && mode !== 'off') {
          if (mode === 'run' && d._runHash !== ht.runHash) dim = true;
          else if (mode === 'metric' && d._traceId !== ht.traceId && d._traceId !== `${ht.traceId}::raw`) dim = true;
          else active = true;
        } else if (d._muted) {
          dim = true;
        }
        if (dim) {
          d.borderColor = _mpAlpha(c, 0.2);
          d.borderWidth = 1.5;
        } else if (active) {
          d.borderColor = c;
          d.borderWidth = 2.8;
        } else {
          d.borderColor = c;
          d.borderWidth = 1.5;
        }
        d.backgroundColor = 'transparent';
      });
    },
  };
}

// Compact numeric formatting for the inline value labels: at most 2 digits
// after the decimal point, with trailing zeros stripped (so "0.00" → "0",
// "18.44000" → "18.44", "18.40" → "18.4").
function _mpFormatBarValue(v) {
  if (!Number.isFinite(v)) return '';
  return Number(v.toFixed(2)).toString();
}

// Bar-chart value labels — draws each bar's value as a small inline label
// at the right edge of the bar so you can scan values without hovering.
function _mpBarValueLabelPlugin() {
  return {
    id: 'mpBarValueLabel',
    afterDatasetsDraw(chart) {
      if (chart.config && chart.config.type !== 'bar') return;
      const ds = (chart.data.datasets || [])[0];
      if (!ds || !Array.isArray(ds.data)) return;
      const meta = chart.getDatasetMeta(0);
      if (!meta || !meta.data) return;
      const ht = _mpState.hoveredTrace;
      const mode = _mpState.highlightMode;
      const C = _mpChartColors();
      const ctx = chart.ctx;
      ctx.save();
      ctx.font = '600 10px Inter, system-ui, sans-serif';
      ctx.textBaseline = 'middle';
      ctx.textAlign = 'left';
      meta.data.forEach((bar, i) => {
        const v = ds.data[i];
        if (v == null || !Number.isFinite(v)) return;
        // Honor highlight dimming: matching bars stay bold, others fade.
        let alpha = 1;
        if (ht && mode !== 'off' && Array.isArray(ds._traceIds)) {
          const tid = ds._traceIds[i];
          const rh = ds._runHashes && ds._runHashes[i];
          if (mode === 'run' && rh !== ht.runHash) alpha = 0.35;
          else if (mode === 'metric' && tid !== ht.traceId) alpha = 0.35;
        }
        ctx.fillStyle = C.text;
        ctx.globalAlpha = alpha;
        const text = _mpFormatBarValue(v);
        ctx.fillText(text, bar.x + 6, bar.y);
      });
      ctx.restore();
    },
  };
}

// Convert "#RRGGBB" to an rgba() with the requested alpha so dimmed lines
// blend properly on white. Falls back to the hex+suffix trick if not hex.
function _mpAlpha(color, alpha) {
  if (typeof color !== 'string') return color;
  if (/^#[0-9a-f]{6}$/i.test(color)) {
    const r = parseInt(color.slice(1, 3), 16);
    const g = parseInt(color.slice(3, 5), 16);
    const b = parseInt(color.slice(5, 7), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }
  return color;
}

// Crosshair plugin — Aim's signature feature: a dashed vertical line that
// follows the cursor on the x-axis, plus little value-pill badges floating
// at the bottom (x value) and left (y value) edges. Cleaner than a giant
// tooltip overlay because each axis only shows the value at the cursor.
function _mpCrosshairPlugin() {
  return {
    id: 'mpCrosshair',
    afterDatasetsDraw(chart) {
      const ev = chart._mpHover || null;
      if (!ev || chart._mpKind === 'bar') return;
      const x = ev.x;
      const y = ev.y;
      const area = chart.chartArea;
      if (!area || x < area.left || x > area.right || y < area.top || y > area.bottom) return;

      const C = _mpChartColors();
      const ctx = chart.ctx;
      ctx.save();
      ctx.strokeStyle = C.crosshair;
      ctx.lineWidth = 1;
      ctx.setLineDash([3, 3]);
      // Vertical line
      ctx.beginPath();
      ctx.moveTo(Math.round(x) + 0.5, area.top);
      ctx.lineTo(Math.round(x) + 0.5, area.bottom);
      ctx.stroke();
      // Horizontal line
      ctx.beginPath();
      ctx.moveTo(area.left,  Math.round(y) + 0.5);
      ctx.lineTo(area.right, Math.round(y) + 0.5);
      ctx.stroke();
      ctx.setLineDash([]);
      // X-axis value pill (centered under the cursor at chartArea.bottom)
      const xv = chart.scales.x ? chart.scales.x.getValueForPixel(x) : null;
      if (Number.isFinite(xv)) {
        const text = _formatMetricValue(xv);
        _mpDrawPill(ctx, text, x, area.bottom + 4, 'x', C);
      }
      const yv = chart.scales.y ? chart.scales.y.getValueForPixel(y) : null;
      if (Number.isFinite(yv)) {
        const text = _formatMetricValue(yv);
        _mpDrawPill(ctx, text, area.left - 4, y, 'y', C);
      }
      ctx.restore();
    },
  };
}

function _mpDrawPill(ctx, text, x, y, axis, C) {
  ctx.font = '600 10px Inter, system-ui, sans-serif';
  const padX = 6, padY = 3;
  const m = ctx.measureText(text);
  const w = Math.ceil(m.width) + padX * 2;
  const h = 18;
  let left, top;
  if (axis === 'x') {
    left = x - w / 2;
    top  = y;
  } else {
    left = x - w;
    top  = y - h / 2;
  }
  ctx.fillStyle = C.valuePillBg;
  ctx.strokeStyle = C.valuePillBorder;
  ctx.lineWidth = 1;
  const r = axis === 'x' ? 4 : 4;
  ctx.beginPath();
  ctx.roundRect ? ctx.roundRect(left, top, w, h, r) : ctx.rect(left, top, w, h);
  ctx.fill();
  ctx.stroke();
  ctx.fillStyle = C.valuePillText;
  ctx.textBaseline = 'middle';
  ctx.fillText(text, left + padX, top + h / 2);
}

// ─── Drag-to-zoom ──────────────────────────────────────────────────────

function _mpAttachZoomHandlers(canvas, chart, chartId) {
  const wrap = canvas.parentElement;
  if (!wrap) return;
  const rect = document.getElementById(`${chartId}-rect`);
  canvas.addEventListener('mousedown', (e) => {
    if (e.button !== 0) return;
    const b = canvas.getBoundingClientRect();
    _mpDragZoom = { chartId, chart, startX: e.clientX - b.left, startY: e.clientY - b.top, currentX: 0, currentY: 0 };
  });
  canvas.addEventListener('mousemove', (e) => {
    if (!_mpDragZoom || _mpDragZoom.chartId !== chartId) return;
    const b = canvas.getBoundingClientRect();
    _mpDragZoom.currentX = e.clientX - b.left;
    _mpDragZoom.currentY = e.clientY - b.top;
    if (rect) {
      const x = Math.min(_mpDragZoom.startX, _mpDragZoom.currentX);
      const y = Math.min(_mpDragZoom.startY, _mpDragZoom.currentY);
      const w = Math.abs(_mpDragZoom.currentX - _mpDragZoom.startX);
      const h = Math.abs(_mpDragZoom.currentY - _mpDragZoom.startY);
      rect.style.display = (w > 4 || h > 4) ? 'block' : 'none';
      rect.style.left = `${x}px`; rect.style.top = `${y}px`;
      rect.style.width = `${w}px`; rect.style.height = `${h}px`;
    }
  });
  const finishDrag = (e) => {
    if (!_mpDragZoom || _mpDragZoom.chartId !== chartId) return;
    if (rect) rect.style.display = 'none';
    const dx = Math.abs((_mpDragZoom.currentX || 0) - _mpDragZoom.startX);
    const dy = Math.abs((_mpDragZoom.currentY || 0) - _mpDragZoom.startY);
    if (dx < 6 && dy < 6) { _mpDragZoom = null; return; }
    const x0 = chart.scales.x.getValueForPixel(Math.min(_mpDragZoom.startX, _mpDragZoom.currentX));
    const x1 = chart.scales.x.getValueForPixel(Math.max(_mpDragZoom.startX, _mpDragZoom.currentX));
    const y0 = chart.scales.y.getValueForPixel(Math.max(_mpDragZoom.startY, _mpDragZoom.currentY));
    const y1 = chart.scales.y.getValueForPixel(Math.min(_mpDragZoom.startY, _mpDragZoom.currentY));
    if (Number.isFinite(x0) && Number.isFinite(x1) && x1 > x0) _mpState.xRange = [x0, x1];
    if (Number.isFinite(y0) && Number.isFinite(y1) && y1 > y0) _mpState.yRange = [y0, y1];
    _mpDragZoom = null;
    _mpReplaceUrl();
    _mpRender();
  };
  canvas.addEventListener('mouseup', finishDrag);
  canvas.addEventListener('mouseleave', finishDrag);
  // Double-click on a line/point → jump to its context-table row. If the
  // click is on empty chart area (no line under the cursor), fall back to
  // the existing "reset zoom" behavior.
  canvas.addEventListener('dblclick', (event) => {
    try {
      const els = chart.getElementsAtEventForMode(event, 'nearest', { intersect: true }, false);
      const el = els && els[0];
      if (el) {
        const ds = chart.data.datasets[el.datasetIndex];
        const tid = ds && ds._traceId;
        if (tid) { _mpScrollToTableRow(tid); return; }
      }
    } catch (_) {}
    _mpResetZoom(chartId);
  });
}

function _mpResetZoom(_chartId) {
  _mpState.xRange = null;
  _mpState.yRange = null;
  _mpReplaceUrl();
  _mpRender();
}

function _mpExportChart(chartId) {
  const canvas = document.getElementById(chartId);
  if (!canvas) return;
  const a = document.createElement('a');
  a.download = `clausius-metrics-${chartId}.png`;
  a.href = canvas.toDataURL('image/png');
  a.click();
}

function _mpExportAll() {
  let idx = 0;
  _mpState.charts.forEach(chart => {
    if (chart && chart._mpId) _mpExportChart(chart._mpId);
    idx++;
  });
  if (!idx) toast('No charts to export', 'error');
}

function _mpDestroyCharts() {
  if (_mpChartTimer) { clearTimeout(_mpChartTimer); _mpChartTimer = null; }
  (_mpState.charts || []).forEach(c => { try { c.destroy(); } catch (_) {} });
  _mpState.charts = [];
  _mpHideExternalTooltip();
}

// ─── 10. Right controls rail ─────────────────────────────────────────

// Metrics chooser at the top of the right rail. Replaces the old horizontal
// metric-chips row.
function _mpMetricsRailSectionHtml(matchingRecords) {
  const numericMetrics = Array.from(new Set(matchingRecords.filter(r => r.numeric).map(r => r.key))).sort();
  const selected = new Set(_mpState.selectedMetrics);
  const visibleSelected = _mpState.selectedMetrics.filter(k => numericMetrics.includes(k));
  const chips = visibleSelected.length
    ? visibleSelected.map(key => `<button type="button" class="mp-metric-chip" onclick="_mpRemoveMetric('${escAttr(key)}')">
        <span>${_escHtml(key)}</span><span class="mp-metric-chip-x">×</span>
      </button>`).join('')
    : '<span class="mp-muted">Click <b>+ Metrics</b> to pick.</span>';
  return `<div class="mp-control-section mp-control-metrics">
    <div class="mp-control-title-row">
      <div class="mp-control-title">Metrics</div>
      <div class="mp-metric-picker">
        <button type="button" class="mp-btn primary mp-btn-sm" onclick="_mpToggleMetricsDropdown()">+ Metrics</button>
        ${_mpMetricsDropdownHtml(numericMetrics, selected)}
      </div>
    </div>
    <div class="mp-metric-chips">${chips}</div>
  </div>`;
}

// Group-by / Label-by pickers in the right rail.
function _mpGroupingRailSectionHtml() {
  return `<div class="mp-control-section mp-control-grouping">
    <div class="mp-control-title">Group by</div>
    <div class="mp-rail-pickers">
      ${_mpGroupPickerHtml('color', 'Color', _mpState.grouping.color)}
      ${_mpGroupPickerHtml('pattern', 'Pattern', _mpState.grouping.pattern || [])}
      ${_mpGroupPickerHtml('chart', 'Chart', _mpState.grouping.chart)}
    </div>
    <div class="mp-control-title" style="margin-top:8px">Label by</div>
    <div class="mp-rail-pickers">
      ${_mpLabelPickerHtml()}
    </div>
  </div>`;
}

function _mpRunsLegendHtml() {
  if (!_mpState.runs.length) {
    return `<div class="mp-control-section mp-runs-legend mp-runs-legend-empty">
      <div class="mp-control-title">Runs</div>
      <div class="mp-muted">No runs loaded. Type an AimQL query above and press <b>Search</b> to discover runs.</div>
    </div>`;
  }
  const rows = _mpState.runs.map(run => _mpRunsLegendRowHtml(run)).join('');
  return `<div class="mp-control-section mp-runs-legend">
    <div class="mp-runs-legend-head">
      <div class="mp-control-title">Runs <span class="mp-runs-legend-count">${_mpState.runs.length}</span></div>
      <button class="mp-link" onclick="_mpClearAllRuns()" title="Remove every loaded run">clear all</button>
    </div>
    <div class="mp-runs-legend-list">${rows}</div>
  </div>`;
}

function _mpRunsLegendRowHtml(run) {
  const key = _mpRunKey(run);
  const payload = _mpState.runData[key];
  const info = (payload && payload.info) || {};
  const projectColor = info.project_color;
  const primary = _mpFormatRunPrimary(run);
  const runName = info.run_name || info.name || run.runHash;
  // Only show the original name as a secondary line when LABEL BY actually
  // changed it. Default LABEL BY=run.name already puts the run name on the
  // primary line, so a second line would be a duplicate.
  const showOriginal = primary !== runName;
  // Match the chart trace color (enumerated assignment), falling back to
  // project tint when no record from this run is loaded yet.
  const sample = _mpState.records.find(r => r.cluster === run.cluster && r.runHash === run.runHash);
  const tags = sample ? (sample.tags || []) : (typeof runTagsFromRun === 'function' ? runTagsFromRun(info) : (info.tags || []));
  const tagPills = tags.length && typeof runTagsPillsHtml === 'function'
    ? `<div class="mp-runs-legend-tags">${runTagsPillsHtml(tags)}</div>`
    : '';
  let color;
  if (sample) color = METRICS_PALETTE[_mpRecordColorIdx(sample)];
  else color = projectColor || METRICS_PALETTE[_mpColorIndexForKey(run.runHash)];
  const hidden = _mpRunHidden(key);
  return `<div class="mp-runs-legend-row${hidden ? ' hidden' : ''}" data-run-key="${escAttr(key)}">
    <button class="mp-runs-legend-toggle" onclick="_mpToggleRunVisibility('${escAttr(key)}')" title="${hidden ? 'Show this run' : 'Hide this run'}">
      <span class="mp-runs-legend-dot" style="background:${color}"></span>
    </button>
    <a class="mp-runs-legend-text mp-runs-legend-link" href="javascript:void(0)"
       onclick="_mpOpenRunFromLegend('${escAttr(run.cluster)}','${escAttr(run.runHash)}')"
       title="Open run popup for ${escAttr(runName)}">
      <div class="mp-runs-legend-name" title="${escAttr(primary)}">${_escHtml(primary)}</div>
      ${showOriginal ? `<div class="mp-runs-legend-original" title="${escAttr(runName)}">${_escHtml(runName)}</div>` : ''}
      ${tagPills}
    </a>
    <button class="mp-runs-legend-remove" onclick="_mpRemoveRun('${escAttr(key)}')" title="Remove">×</button>
  </div>`;
}

function _mpOpenRunFromLegend(cluster, runHash) {
  const payload = _mpState.runData[`${cluster}/${runHash}`];
  const name = payload && payload.info && (payload.info.run_name || payload.info.name);
  _mpOpenRunPopup(cluster, runHash, name || '');
}

function _mpRunHidden(key) {
  // A run is "hidden" if every trace from it is in hiddenTraces. Cheap check:
  // store a per-run hidden flag.
  return !!(_mpState.hiddenRuns && _mpState.hiddenRuns[key]);
}

function _mpRunRecordCount(run) {
  const payload = _mpState.runData[_mpRunKey(run)];
  if (!payload || !payload.metrics) return 0;
  const series = Object.keys(payload.metrics.series || {}).length;
  const scalars = Object.keys(payload.metrics.scalars || {}).length;
  return series + scalars;
}

function _mpToggleRunVisibility(key) {
  if (!_mpState.hiddenRuns) _mpState.hiddenRuns = {};
  _mpState.hiddenRuns[key] = !_mpState.hiddenRuns[key];
  // Sync to hiddenTraces — find all trace ids for this run and toggle them.
  const [cluster, runHash] = key.split('/');
  const hidden = _mpState.hiddenRuns[key];
  Object.keys(_mpState.hiddenTraces).forEach(traceId => {
    if (traceId.startsWith(`${cluster}/${runHash}/`)) {
      if (hidden) _mpState.hiddenTraces[traceId] = true;
      else delete _mpState.hiddenTraces[traceId];
    }
  });
  if (hidden) {
    _mpState.records.forEach(r => {
      if (r.cluster === cluster && r.runHash === runHash) {
        const tid = `${r.cluster}/${r.runHash}/${r.key}/${r.contextSig}`;
        _mpState.hiddenTraces[tid] = true;
      }
    });
  }
  _mpRender();
}

function _mpClearAllRuns() {
  if (!_mpState.runs.length) return;
  if (!confirm(`Remove all ${_mpState.runs.length} loaded runs?`)) return;
  _mpState.runs = [];
  _mpState.runData = {};
  _mpState.records = [];
  _mpState.hiddenRuns = {};
  _mpState.hiddenTraces = {};
  _mpApplyDefaultSelection();
  _mpReplaceUrl();
  _mpRender();
}

function _mpControlsHtml(matchingRecords) {
  const hasRecords = _mpState.records.length > 0;
  const hasVisibleSeries = _mpHasVisibleSeries(matchingRecords || []);
  return `<div class="mp-controls-inner">
    ${hasRecords ? _mpMetricsRailSectionHtml(matchingRecords || []) : ''}
    ${hasRecords ? _mpGroupingRailSectionHtml() : ''}
    ${_mpRunsLegendHtml()}
    ${hasVisibleSeries ? _mpControlSection('Axes Alignment', `
      <label class="mp-radio${_mpState.align === 'step' ? ' active' : ''}">
        <input type="radio" name="mp-align" ${_mpState.align === 'step' ? 'checked' : ''} onchange="_mpSetAlign('step')"> Step
      </label>
      <label class="mp-radio${_mpState.align === 'wall_time' ? ' active' : ''}">
        <input type="radio" name="mp-align" ${_mpState.align === 'wall_time' ? 'checked' : ''} onchange="_mpSetAlign('wall_time')"> Relative time
      </label>
      <label class="mp-radio${_mpState.align === 'index' ? ' active' : ''}">
        <input type="radio" name="mp-align" ${_mpState.align === 'index' ? 'checked' : ''} onchange="_mpSetAlign('index')"> Index
      </label>
    `) : ''}
    ${_mpControlSection('Axes Scale', `
      <label class="mp-radio${_mpState.yScale === 'linear' ? ' active' : ''}">
        <input type="radio" name="mp-yscale" ${_mpState.yScale === 'linear' ? 'checked' : ''} onchange="_mpSetYScale('linear')"> Linear
      </label>
      <label class="mp-radio${_mpState.yScale === 'logarithmic' ? ' active' : ''}">
        <input type="radio" name="mp-yscale" ${_mpState.yScale === 'logarithmic' ? 'checked' : ''} onchange="_mpSetYScale('logarithmic')"> Logarithmic
      </label>
    `)}
    ${hasVisibleSeries ? _mpControlSection('Smoothing', `
      <div class="mp-slider-row">
        <input type="range" min="0" max="0.95" step="0.05"
               value="${_mpState.smoothing}"
               oninput="_mpPreviewSmoothing(this.value)"
               onchange="_mpSetSmoothing(this.value)">
        <b>${_mpState.smoothing.toFixed(2)}</b>
      </div>
      <label class="mp-checkbox">
        <input type="checkbox" ${_mpState.showRaw ? 'checked' : ''} onchange="_mpSetShowRaw(this.checked)">
        Show original (raw) line
      </label>
    `) : ''}
    ${hasVisibleSeries ? _mpControlSection('Outliers', `
      <label class="mp-checkbox">
        <input type="checkbox" ${_mpState.ignoreOutliers ? 'checked' : ''} onchange="_mpSetIgnoreOutliers(this.checked)">
        Ignore outliers (IQR filter)
      </label>
    `) : ''}
    ${_mpControlSection('Highlight Mode', `
      <label class="mp-radio${_mpState.highlightMode === 'off' ? ' active' : ''}">
        <input type="radio" name="mp-hl" ${_mpState.highlightMode === 'off' ? 'checked' : ''} onchange="_mpSetHighlightMode('off')"> Off
      </label>
      <label class="mp-radio${_mpState.highlightMode === 'metric' ? ' active' : ''}">
        <input type="radio" name="mp-hl" ${_mpState.highlightMode === 'metric' ? 'checked' : ''} onchange="_mpSetHighlightMode('metric')"> Metric on hover
      </label>
      <label class="mp-radio${_mpState.highlightMode === 'run' ? ' active' : ''}">
        <input type="radio" name="mp-hl" ${_mpState.highlightMode === 'run' ? 'checked' : ''} onchange="_mpSetHighlightMode('run')"> Run on hover
      </label>
    `)}
    ${hasVisibleSeries ? _mpControlSection('Zoom', `
      <div class="mp-zoom-state">
        x: <b>${_mpState.xRange ? `${_formatMetricValue(_mpState.xRange[0])} – ${_formatMetricValue(_mpState.xRange[1])}` : 'auto'}</b>
        y: <b>${_mpState.yRange ? `${_formatMetricValue(_mpState.yRange[0])} – ${_formatMetricValue(_mpState.yRange[1])}` : 'auto'}</b>
      </div>
      <div class="mp-zoom-hint">Drag to zoom · double-click to reset</div>
      <button class="mp-btn ghost" onclick="_mpResetZoom()">⟲ reset zoom</button>
    `) : ''}
    ${_mpControlSection('Export', `
      <button class="mp-btn ghost" onclick="_mpExportAll()">⤓ download all charts as PNG</button>
    `)}
  </div>`;
}

function _mpVisibleNumericRecords(matchingRecords) {
  const selected = new Set(_mpState.selectedMetrics || []);
  return (matchingRecords || []).filter(r => r.numeric && selected.has(r.key));
}

function _mpHasVisibleSeries(matchingRecords) {
  return _mpVisibleNumericRecords(matchingRecords).some(r => r.kind === 'series');
}

function _mpControlSection(title, body) {
  return `<div class="mp-control-section">
    <div class="mp-control-title">${_escHtml(title)}</div>
    <div class="mp-control-body">${body}</div>
  </div>`;
}

function _mpSetAlign(v) { _mpState.align = v; _mpState.xRange = null; _mpReplaceUrl(); _mpRender(); }
function _mpSetYScale(v) { _mpState.yScale = v; _mpState.yRange = null; _mpReplaceUrl(); _mpRender(); }
function _mpPreviewSmoothing(v) {
  const el = document.querySelector('.mp-slider-row b');
  if (el) el.textContent = (parseFloat(v) || 0).toFixed(2);
}
function _mpSetSmoothing(v) { _mpState.smoothing = Math.max(0, Math.min(0.95, parseFloat(v) || 0)); _mpReplaceUrl(); _mpRender(); }
function _mpSetShowRaw(v) { _mpState.showRaw = !!v; _mpReplaceUrl(); _mpRender(); }
function _mpSetIgnoreOutliers(v) { _mpState.ignoreOutliers = !!v; _mpReplaceUrl(); _mpRender(); }
function _mpSetHighlightMode(v) {
  _mpState.highlightMode = ['off', 'metric', 'run'].includes(v) ? v : 'metric';
  _mpReplaceUrl(); _mpRender();
}

// ─── 11. Context table + splitter ────────────────────────────────────

// Build a pivot table from the currently-matching records:
//  - rows = unique (cluster, run_hash)
//  - cols = unique (metric_key, context_signature)
//  - cells = latest numeric value for that row/col
// Per-column max is precomputed so the renderer can bold it.
function _mpBuildPivotTable(matchingRecords, selectedMetrics) {
  const selected = new Set(selectedMetrics || []);
  const records = matchingRecords.filter(r => !selected.size || selected.has(r.key));
  const runs = new Map();
  const cols = new Map();
  records.forEach(r => {
    const v = r.stats && r.stats.latestNum;
    if (v == null || !Number.isFinite(v)) return;
    const runKey = `${r.cluster}/${r.runHash}`;
    if (!runs.has(runKey)) {
      runs.set(runKey, {
        runKey, runHash: r.runHash, cluster: r.cluster,
        runName: r.runName || r.runHash,
        project: r.project,
      });
    }
    const colKey = `${r.key}::${r.contextSig}`;
    if (!cols.has(colKey)) {
      cols.set(colKey, {
        key: colKey,
        metric: r.key,
        contextSig: r.contextSig,
        contextLabel: _mpContextLabel(r.context),
        values: {},
      });
    }
    cols.get(colKey).values[runKey] = v;
  });
  // Per-column max + the run keys that share it (could be multiple ties).
  cols.forEach(col => {
    const vals = Object.values(col.values).filter(Number.isFinite);
    if (!vals.length) { col.max = null; col.maxRunKeys = new Set(); return; }
    col.max = Math.max(...vals);
    col.maxRunKeys = new Set();
    Object.entries(col.values).forEach(([k, v]) => { if (v === col.max) col.maxRunKeys.add(k); });
  });
  const colList = Array.from(cols.values()).sort((a, b) =>
    a.metric.localeCompare(b.metric) || a.contextLabel.localeCompare(b.contextLabel)
  );
  let runList = Array.from(runs.values()).sort((a, b) =>
    (a.runName || '').localeCompare(b.runName || '')
  );
  // Apply user-selected sort if any.
  const sort = _mpState.tableViewSort || {};
  if (sort.col) {
    const col = cols.get(sort.col);
    if (col) {
      const dir = sort.dir === 'asc' ? 1 : -1;
      runList = runList.slice().sort((a, b) => {
        const va = col.values[a.runKey];
        const vb = col.values[b.runKey];
        const aOk = Number.isFinite(va);
        const bOk = Number.isFinite(vb);
        if (!aOk && !bOk) return 0;
        if (!aOk) return 1;
        if (!bOk) return -1;
        return (va - vb) * dir;
      });
    }
  }
  return { runs: runList, cols: colList };
}

function _mpTableViewHtml(matchingRecords) {
  const pivot = _mpBuildPivotTable(matchingRecords, _mpState.selectedMetrics);
  if (!pivot.runs.length || !pivot.cols.length) {
    return `<div class="mp-tv-overlay" onclick="_mpCloseTableView(event)">
      <div class="mp-tv-modal" onclick="event.stopPropagation()">
        <div class="mp-tv-head">
          <div class="mp-tv-title">Table view</div>
          <div class="mp-tv-tools">
            <button class="mp-icon-btn" onclick="_mpCloseTableView()" title="Close">×</button>
          </div>
        </div>
        <div class="mp-tv-empty">No matching numeric values to pivot.</div>
      </div>
    </div>`;
  }
  const sort = _mpState.tableViewSort || {};
  const head = `<tr>
    <th class="mp-tv-th mp-tv-run-col">Run</th>
    <th class="mp-tv-th mp-tv-meta-col">Cluster</th>
    <th class="mp-tv-th mp-tv-meta-col">Project</th>
    ${pivot.cols.map(col => {
      const sub = col.contextLabel ? `<div class="mp-tv-ctx">${_escHtml(col.contextLabel)}</div>` : '';
      const isSorted = sort.col === col.key;
      const arrow = isSorted ? (sort.dir === 'asc' ? ' ▲' : ' ▼') : '';
      return `<th class="mp-tv-th mp-tv-num-col${isSorted ? ' mp-tv-sorted' : ''}"
                  onclick="_mpTableViewSort('${escAttr(col.key)}')"
                  title="Sort by ${escAttr(col.metric)}${col.contextLabel ? ' · ' + col.contextLabel : ''}">
        <div class="mp-tv-metric">${_escHtml(col.metric)}<span class="mp-tv-arrow">${arrow}</span></div>
        ${sub}
      </th>`;
    }).join('')}
  </tr>`;
  const body = pivot.runs.map(run => {
    const cells = pivot.cols.map(col => {
      const v = col.values[run.runKey];
      if (v == null || !Number.isFinite(v)) return '<td class="mp-tv-td mp-tv-num-col mp-tv-empty">—</td>';
      const isMax = col.maxRunKeys && col.maxRunKeys.has(run.runKey);
      return `<td class="mp-tv-td mp-tv-num-col${isMax ? ' mp-tv-max' : ''}" title="${escAttr(_formatMetricValue(v))}">
        ${_escHtml(_mpFormatBarValue(v))}
      </td>`;
    }).join('');
    return `<tr>
      <td class="mp-tv-td mp-tv-run-col">
        <div class="mp-tv-run-name" title="${escAttr(run.runName)}">${_escHtml(run.runName)}</div>
        <div class="mp-tv-run-sub">${_escHtml(run.runHash.slice(0, 8))}</div>
      </td>
      <td class="mp-tv-td mp-tv-meta-col">${_escHtml(run.cluster)}</td>
      <td class="mp-tv-td mp-tv-meta-col">${_escHtml(run.project || '—')}</td>
      ${cells}
    </tr>`;
  }).join('');
  return `<div class="mp-tv-overlay" onclick="_mpCloseTableView(event)">
    <div class="mp-tv-modal" onclick="event.stopPropagation()">
      <div class="mp-tv-head">
        <div class="mp-tv-title">
          Table view
          <span class="mp-tv-sub">${pivot.runs.length} runs · ${pivot.cols.length} columns · highest per column bolded · click header to sort</span>
        </div>
        <div class="mp-tv-tools">
          <button class="mp-btn" onclick="_mpExportTableViewCsv()" title="Download as CSV">⤓ csv</button>
          <button class="mp-btn" onclick="_mpCopyTableViewTsv()" title="Copy as TSV for spreadsheets">⧉ copy</button>
          <button class="mp-icon-btn" onclick="_mpCloseTableView()" title="Close (Esc)">×</button>
        </div>
      </div>
      <div class="mp-tv-scroll">
        <table class="mp-tv-table">
          <thead>${head}</thead>
          <tbody>${body}</tbody>
        </table>
      </div>
    </div>
  </div>`;
}

function _mpOpenTableView() {
  _mpState.tableViewOpen = true;
  _mpState.tableViewSort = { col: null, dir: 'desc' };
  _mpRender();
}

function _mpCloseTableView(e) {
  if (e && e.target && !e.target.classList.contains('mp-tv-overlay') && e.target.tagName !== 'BUTTON' && !e.target.closest('.mp-icon-btn')) return;
  _mpState.tableViewOpen = false;
  _mpRender();
}

function _mpTableViewSort(colKey) {
  const cur = _mpState.tableViewSort || {};
  if (cur.col === colKey) {
    _mpState.tableViewSort = { col: colKey, dir: cur.dir === 'desc' ? 'asc' : 'desc' };
  } else {
    _mpState.tableViewSort = { col: colKey, dir: 'desc' };
  }
  _mpRender();
}

function _mpExportTableViewCsv() {
  const records = _mpFilteredMatchingRecordsForExport();
  const pivot = _mpBuildPivotTable(records, _mpState.selectedMetrics);
  const head = ['run', 'run_hash', 'cluster', 'project',
                ...pivot.cols.map(c => c.contextLabel ? `${c.metric} (${c.contextLabel})` : c.metric)];
  const rows = pivot.runs.map(run => [
    run.runName, run.runHash, run.cluster, run.project || '',
    ...pivot.cols.map(c => {
      const v = c.values[run.runKey];
      return v == null || !Number.isFinite(v) ? '' : String(v);
    }),
  ]);
  const escapeCsv = (s) => {
    const str = String(s == null ? '' : s);
    return /[",\n]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
  };
  const csv = [head, ...rows].map(r => r.map(escapeCsv).join(',')).join('\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `clausius-metrics-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-')}.csv`;
  document.body.appendChild(a); a.click();
  setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 100);
}

async function _mpCopyTableViewTsv() {
  const records = _mpFilteredMatchingRecordsForExport();
  const pivot = _mpBuildPivotTable(records, _mpState.selectedMetrics);
  const head = ['run', 'run_hash', 'cluster', 'project',
                ...pivot.cols.map(c => c.contextLabel ? `${c.metric} (${c.contextLabel})` : c.metric)];
  const rows = pivot.runs.map(run => [
    run.runName, run.runHash, run.cluster, run.project || '',
    ...pivot.cols.map(c => {
      const v = c.values[run.runKey];
      return v == null || !Number.isFinite(v) ? '' : String(v);
    }),
  ]);
  const tsv = [head, ...rows].map(r => r.join('\t')).join('\n');
  try {
    await navigator.clipboard.writeText(tsv);
    toast('Table copied to clipboard (TSV)');
  } catch (_) {
    toast('Clipboard not available', 'error');
  }
}

// Resolve the same set of records the table-view modal renders. Encapsulated
// here so CSV / copy share with the renderer.
function _mpFilteredMatchingRecordsForExport() {
  const { ast } = _mpCompileQuery(_mpState.query);
  return _mpState.records.filter(r => _mpRecordMatches(r, ast));
}

function _mpContextTableHtml(matchingRecords) {
  const selected = new Set(_mpState.selectedMetrics);
  const records = matchingRecords.filter(r => selected.has(r.key));
  if (!records.length) {
    return '<div class="mp-empty">Select metrics to populate the context table.</div>';
  }
  const rows = records.map(r => {
    const traceId = `${r.cluster}/${r.runHash}/${r.key}/${r.contextSig}`;
    const rowId = _mpTableRowId(traceId);
    const hidden = _mpState.hiddenTraces[traceId];
    // Use the same enumerated color assignment as the chart and the runs
    // legend so the table swatch always matches the bar / line color.
    const color = METRICS_PALETTE[_mpRecordColorIdx(r)];
    const ctxText = _mpContextLabel(r.context) || '∅';
    const stats = r.stats;
    const runDisplay = r.runName || r.runHash;
    const eyeTitle = hidden ? 'Show this trace on the chart' : 'Hide this trace from the chart';
    return `<tr id="${escAttr(rowId)}" class="${hidden ? 'mp-row-hidden' : ''}" data-trace-id="${escAttr(traceId)}">
      <td><span class="mp-table-color" style="background:${color}"></span></td>
      <td class="mp-table-run-cell">
        <a class="mp-table-run-link" href="javascript:void(0)"
           onclick="_mpOpenRunFromTable('${escAttr(r.cluster)}','${escAttr(r.runHash)}','${escAttr(runDisplay)}')"
           title="Open run popup for ${escAttr(runDisplay)}">
          <div class="mp-table-run-name">${_escHtml(runDisplay)}</div>
          <div class="mp-table-run-meta">${_escHtml(r.runHash)} · ${_escHtml(r.cluster)}</div>
        </a>
        <button class="mp-table-eye" onclick="_mpToggleTrace('${escAttr(traceId)}')"
                title="${escAttr(eyeTitle)}" aria-label="${escAttr(eyeTitle)}">
          ${hidden
            ? '<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M2.5 2.5l11 11M3.5 8.2c.7-1 1.6-1.8 2.6-2.4M9.5 5.4c.4-.1.8-.1 1.2-.1 3 0 5 2.7 5 2.7s-.7 1-1.7 2M6.6 9.1a2 2 0 002.8 2.8" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/></svg>'
            : '<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M1.5 8s2.4-4.5 6.5-4.5S14.5 8 14.5 8 12.1 12.5 8 12.5 1.5 8 1.5 8z" fill="none" stroke="currentColor" stroke-width="1.4"/><circle cx="8" cy="8" r="2" fill="currentColor"/></svg>'}
        </button>
      </td>
      <td>${_escHtml(r.project || '—')}</td>
      <td><span class="mp-table-metric">${_escHtml(r.key)}</span> <span class="mp-table-kind">${_escHtml(r.kind === 'scalars' ? 'scalar' : 'series')}</span></td>
      <td class="mp-table-ctx">${_escHtml(ctxText)}</td>
      <td>${stats.latestNum != null ? _escHtml(_formatMetricValue(stats.latestNum)) : '—'}</td>
      <td>${stats.min != null ? _escHtml(_formatMetricValue(stats.min)) : '—'}</td>
      <td>${stats.max != null ? _escHtml(_formatMetricValue(stats.max)) : '—'}</td>
      <td>${stats.numericCount}</td>
    </tr>`;
  }).join('');
  return `<div class="mp-table-wrap">
    <div class="mp-table-head-row">
      <div class="mp-table-head-title">Context Table</div>
      <div class="mp-table-head-meta">${records.length} traces · double-click a bar/line to jump to its row · click run name to open run page · eye toggles visibility</div>
      <button class="mp-btn mp-btn-sm" onclick="_mpOpenTableView()"
              title="Open a pivot table: rows = runs, columns = metric+context, highest per column bolded">
        ⊞ table view
      </button>
    </div>
    <div class="mp-table-scroll">
      <table class="mp-table">
        <thead>
          <tr>
            <th></th><th>Run</th><th>Project</th><th>Metric</th><th>Context</th>
            <th>Last</th><th>Min</th><th>Max</th><th>Pts</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  </div>`;
}

// Stable, querySelector-safe DOM id for each table row, derived from the
// trace id (which contains "/" and ".").
function _mpTableRowId(traceId) {
  return 'mp-row-' + String(traceId || '').replace(/[^A-Za-z0-9_-]/g, '_');
}

// Scroll to and flash the context-table row matching the given trace id.
// If the table is collapsed, open it first and wait one frame for the new
// row to land in the DOM.
function _mpScrollToTableRow(traceId) {
  const doScroll = () => {
    const row = document.getElementById(_mpTableRowId(traceId));
    if (!row) return;
    row.scrollIntoView({ behavior: 'smooth', block: 'center' });
    row.classList.add('mp-row-flash');
    setTimeout(() => row.classList.remove('mp-row-flash'), 1600);
  };
  if (!_mpState.tableOpen) {
    _mpState.tableOpen = true;
    _mpReplaceUrl();
    _mpRender();
    requestAnimationFrame(() => requestAnimationFrame(doScroll));
  } else {
    doScroll();
  }
}

// Open the run popup (overlay modal) for a given run. Used by the context
// table and the right-rail runs legend. Falls back to the full run page
// when the popup helper isn't loaded for some reason.
function _mpOpenRunPopup(cluster, runHash, runName) {
  if (typeof openRunInfoByHash === 'function') {
    openRunInfoByHash(cluster, runHash, runName || '');
  } else if (typeof openRunPage === 'function') {
    openRunPage(cluster, runHash, true);
  }
}

function _mpOpenRunFromTable(cluster, runHash, runName) {
  _mpOpenRunPopup(cluster, runHash, runName);
}

function _mpToggleTrace(traceId) {
  if (_mpState.hiddenTraces[traceId]) delete _mpState.hiddenTraces[traceId];
  else _mpState.hiddenTraces[traceId] = true;
  _mpRender();
}

function _mpAttachSplitterHandlers() {
  const splitter = document.getElementById('mp-splitter');
  if (!splitter) return;
  splitter.addEventListener('mousedown', (e) => {
    e.preventDefault();
    const main = document.querySelector('.mp-main');
    if (!main) return;
    const rect = main.getBoundingClientRect();
    _mpSplitDrag = { rect };
    document.body.style.cursor = 'row-resize';
    const onMove = (mv) => {
      if (!_mpSplitDrag) return;
      const y = Math.max(_mpSplitDrag.rect.top + 120, Math.min(_mpSplitDrag.rect.bottom - 120, mv.clientY));
      const ratio = (y - _mpSplitDrag.rect.top) / _mpSplitDrag.rect.height;
      _mpState.splitRatio = Math.max(0.2, Math.min(0.9, ratio));
      const chartPane = document.getElementById('mp-chart-pane');
      const tablePane = main.querySelector('.mp-table-pane');
      if (chartPane) chartPane.style.flex = `${_mpState.splitRatio} 1 0`;
      if (tablePane) tablePane.style.flex = `${1 - _mpState.splitRatio} 1 0`;
      _mpState.charts.forEach(c => { try { c.resize(); } catch (_) {} });
    };
    const onUp = () => {
      _mpSplitDrag = null;
      document.body.style.cursor = '';
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
      _mpReplaceUrl();
    };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  });
}

// ─── 12. Saved views API ──────────────────────────────────────────────

async function _mpLoadSavedViews() {
  try {
    const res = await fetch('/api/metrics_views');
    const data = await res.json();
    if (data.status === 'ok') {
      _mpState.savedViews = data.views || [];
      _mpRender();
    }
  } catch (_) {}
}

function _mpSerializeState() {
  return {
    runs: _mpState.runs,
    selectedMetrics: _mpState.selectedMetrics,
    query: _mpState.query,
    grouping: _mpState.grouping,
    traceLabelFields: _mpState.traceLabelFields,
    align: _mpState.align,
    yScale: _mpState.yScale,
    xRange: _mpState.xRange,
    yRange: _mpState.yRange,
    smoothing: _mpState.smoothing,
    showRaw: _mpState.showRaw,
    ignoreOutliers: _mpState.ignoreOutliers,
    highlightMode: _mpState.highlightMode,
    splitRatio: _mpState.splitRatio,
    tableOpen: _mpState.tableOpen,
    rightRailWidth: _mpState.rightRailWidth,
    chartOrder: _mpState.chartOrder,
    hiddenTraces: _mpState.hiddenTraces,
  };
}

function _mpApplySerialized(state) {
  const s = state || {};
  _mpState.runs = Array.isArray(s.runs) ? s.runs : [];
  _mpState.selectedMetrics = Array.isArray(s.selectedMetrics) ? s.selectedMetrics : [];
  _mpState.query = s.query || '';
  _mpState.grouping = {
    color: Array.isArray(s.grouping?.color) ? s.grouping.color : ['run.hash'],
    chart: Array.isArray(s.grouping?.chart) ? s.grouping.chart : ['metric.name'],
    pattern: Array.isArray(s.grouping?.pattern) ? s.grouping.pattern : [],
  };
  _mpState.traceLabelFields = Array.isArray(s.traceLabelFields) ? s.traceLabelFields : ['run.name'];
  _mpState.align = s.align || 'step';
  _mpState.yScale = s.yScale || 'linear';
  _mpState.xRange = Array.isArray(s.xRange) ? s.xRange : null;
  _mpState.yRange = Array.isArray(s.yRange) ? s.yRange : null;
  _mpState.smoothing = Math.max(0, Math.min(0.95, parseFloat(s.smoothing ?? 0.25) || 0));
  _mpState.showRaw = !!s.showRaw;
  _mpState.ignoreOutliers = !!s.ignoreOutliers;
  _mpState.highlightMode = ['off', 'metric', 'run'].includes(s.highlightMode) ? s.highlightMode : 'metric';
  _mpState.splitRatio = Math.max(0.2, Math.min(0.9, parseFloat(s.splitRatio ?? 0.65) || 0.65));
  _mpState.tableOpen = s.tableOpen !== false;
  _mpState.rightRailWidth = Number.isFinite(s.rightRailWidth)
    ? Math.max(220, Math.min(640, s.rightRailWidth))
    : 300;
  _mpState.chartOrder = Array.isArray(s.chartOrder)
    ? s.chartOrder.filter(k => typeof k === 'string')
    : [];
  _mpState.hiddenTraces = s.hiddenTraces && typeof s.hiddenTraces === 'object' ? s.hiddenTraces : {};
}

// Always creates a new saved view, leaving any currently-active view
// untouched. The new view becomes the active one.
async function _mpSaveAsNewView() {
  const title = prompt('Name this metrics view:', _mpSuggestedTitle());
  if (!title) return;
  try {
    const res = await fetch('/api/metrics_views', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: String(title).trim(), state: _mpSerializeState() }),
    });
    const data = await res.json();
    if (data.status === 'ok') {
      _mpState.activeViewId = data.view.id;
      _mpState.viewsMenuOpen = false;
      toast(`Saved as "${data.view.title}"`);
      _mpReplaceUrl();
      await _mpLoadSavedViews();
    } else {
      toast(data.error || 'Failed to save metrics view', 'error');
    }
  } catch (_) {
    toast('Network error while saving view', 'error');
  }
}

// Updates the currently-active view in place. No-op when no view is active.
async function _mpUpdateCurrentView() {
  const id = _mpState.activeViewId;
  if (!id) { toast('No active view to update — use "Save as new view" instead.', 'error'); return; }
  try {
    const res = await fetch(`/api/metrics_views/${id}`, {
      method: 'PATCH', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ state: _mpSerializeState() }),
    });
    const data = await res.json();
    if (data.status === 'ok') {
      _mpState.viewsMenuOpen = false;
      toast(`Updated "${data.view.title}"`);
      await _mpLoadSavedViews();
    } else {
      toast(data.error || 'Failed to update view', 'error');
    }
  } catch (_) {
    toast('Network error while updating view', 'error');
  }
}

function _mpSuggestedTitle() {
  const names = _mpState.runs.map(r => {
    const p = _mpState.runData[_mpRunKey(r)];
    return (p && p.info && (p.info.run_name || p.info.name)) || r.runHash;
  }).slice(0, 2).join(' vs ');
  return names || 'Metrics view';
}

async function _mpOpenSavedView(id) {
  try {
    const res = await fetch(`/api/metrics_views/${id}`);
    const data = await res.json();
    if (data.status !== 'ok') { toast(data.error || 'Failed to load view', 'error'); return; }
    _mpState.activeViewId = id;
    _mpState.viewsMenuOpen = false;
    _mpState.renamingViewId = null;
    _mpApplySerialized(data.view.state || {});
    _mpReplaceUrl();
    _mpRender();
    if (_mpState.runs.length) _mpLoadRuns();
  } catch (_) { toast('Network error while loading view', 'error'); }
}

async function _mpDeleteSavedView(id) {
  if (!confirm('Delete this saved metrics view?')) return;
  try {
    const res = await fetch(`/api/metrics_views/${id}`, { method: 'DELETE' });
    const data = await res.json();
    if (data.status === 'ok') {
      if (_mpState.activeViewId === id) _mpState.activeViewId = null;
      _mpLoadSavedViews();
    } else {
      toast(data.error || 'Failed to delete view', 'error');
    }
  } catch (_) { toast('Network error while deleting view', 'error'); }
}

// ─── Window-level exports (no-op references for test runners) ────────

if (typeof window !== 'undefined') {
  window._mpState = _mpState;
  window._mpQLParse = _mpQLParse;
  window._mpQLEval = _mpQLEval;
  window._mpRecordMatches = _mpRecordMatches;
  window._mpQLBuildContext = _mpQLBuildContext;
  window._mpGroupKey = _mpGroupKey;
  window._mpColorIndexForKey = _mpColorIndexForKey;
  window.METRICS_PALETTE = METRICS_PALETTE;
}
