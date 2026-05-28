let _runPageState = {
  cluster: '',
  runHash: '',
  run: null,
  metrics: { metadata: {}, series: {}, latest: {}, scalars: {}, scalar_latest: {} },
  customMetrics: null,
  selectedSeries: [],
  selectedScalars: [],
  metricQuery: '',
  catalogTab: 'series',
  pageTab: 'overview',
  xAxis: 'step',
  groupByContext: '',
  smoothing: 0.25,
  showRaw: false,
  yScale: 'linear',
  ignoreOutliers: false,
  tooltipMode: 'all',
  hiddenTraces: {},
  charts: [],
  loadSeq: 0,
  noteTimer: null,
};
let _runPageChartRetryTimer = null;

const RUN_PAGE_COLORS = ['#22c55e', '#3b82f6', '#f59e0b', '#a855f7', '#ef4444', '#14b8a6', '#84cc16', '#06b6d4'];

function csColorMix(hex, otherHex, weight) {
  const parse = (value) => {
    const s = String(value || '').replace('#', '').trim();
    if (s.length !== 6) return null;
    const n = parseInt(s, 16);
    if (!Number.isFinite(n)) return null;
    return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
  };
  const a = parse(hex);
  const b = parse(otherHex);
  if (!a || !b) return hex;
  const w = Math.max(0, Math.min(1, Number(weight) || 0));
  const mixed = a.map((v, i) => Math.round(v * (1 - w) + b[i] * w));
  return `#${mixed.map(v => v.toString(16).padStart(2, '0')).join('')}`;
}

async function openRunPage(cluster, runHash, fromTab = false) {
  const c = String(cluster || '');
  const h = String(runHash || '');
  if (!c || !h) return;

  _runPageState = {
    ..._runPageState,
    cluster: c,
    runHash: h,
    run: null,
    customMetrics: null,
  };
  _runPageReadUrlState();

  const label = `Run ${h}`;
  if (!fromTab && typeof _appTabs !== 'undefined') {
    const existing = _appTabs.find(t => t.type === 'run' && t.runCluster === c && t.runHash === h);
    if (existing) {
      _activeTabId = existing.id;
      existing.label = existing.label || label;
    } else {
      _appTabs.push({
        id: _nextTabId++,
        type: 'run',
        label,
        project: null,
        runCluster: c,
        runHash: h,
      });
      _activeTabId = _appTabs[_appTabs.length - 1].id;
    }
  }

  if (typeof _activateView === 'function') _activateView('run');
  if (typeof _renderAppTabs === 'function') _renderAppTabs();
  if (typeof _persistTabs === 'function') _persistTabs();
  if (!fromTab && typeof _setHash === 'function') {
    _setHash(_runPageBaseHash(c, h) + _runPageCurrentQuery());
  }

  _runPageDestroyCharts();
  const seq = ++_runPageState.loadSeq;
  _runPageRenderLoading(c, h);
  if (document.hidden) {
    _runPageRenderMessage('Run metrics are paused while the tab is hidden.');
    return;
  }

  try {
    const [infoRes, metricRes] = await Promise.all([
      fetch(`/api/run_info_by_hash/${encodeURIComponent(c)}/${encodeURIComponent(h)}`),
      fetch(`/api/run_metrics_by_hash/${encodeURIComponent(c)}/${encodeURIComponent(h)}`),
    ]);
    const info = await infoRes.json();
    const metrics = await metricRes.json();
    if (seq !== _runPageState.loadSeq) return;
    if (info.status !== 'ok' || !info.run) throw new Error(info.error || 'Run not found');
    if (metrics.status !== 'ok') throw new Error(metrics.error || 'Metrics not found');

    _runPageState.run = info.run;
    if (info.run.run_hash) _runPageState.runHash = info.run.run_hash;
    _runPageState.metrics = _runPageNormalizeMetrics(metrics);
    _runPageApplyDefaultSelections();
    _runPageSyncTabLabel(info.run);
    _runPageRender();
    _runPageLoadCustomMetrics();
  } catch (e) {
    if (seq !== _runPageState.loadSeq) return;
    _runPageRenderMessage(`Could not load run page: ${e.message || e}`);
  }
}

function _runPageBaseHash(cluster, runHash) {
  return `/run/${encodeURIComponent(cluster)}/${encodeURIComponent(runHash)}`;
}

function _runPageReadUrlState() {
  const raw = location.hash.startsWith('#/')
    ? (location.hash.split('?')[1] || '')
    : location.search.replace(/^\?/, '');
  const params = new URLSearchParams(raw);
  const series = params.get('series') || params.get('metrics') || '';
  const scalars = params.get('scalars') || '';
  _runPageState.selectedSeries = series ? series.split(',').map(decodeURIComponent).filter(Boolean) : [];
  _runPageState.selectedScalars = scalars ? scalars.split(',').map(decodeURIComponent).filter(Boolean) : [];
  _runPageState.metricQuery = params.get('q') || '';
  _runPageState.xAxis = params.get('x') || 'step';
  _runPageState.groupByContext = params.get('group') || '';
  _runPageState.yScale = params.get('yscale') || 'linear';
  _runPageState.smoothing = Math.max(0, Math.min(0.95, parseFloat(params.get('smooth') || '0.25') || 0));
  _runPageState.showRaw = params.get('raw') === '1';
  _runPageState.ignoreOutliers = params.get('outliers') === '0';
  _runPageState.tooltipMode = params.get('hover') === 'nearest' ? 'nearest' : 'all';
  _runPageState.pageTab = params.get('tab') || 'overview';
  _runPageState.catalogTab = params.get('kind') || 'series';
}

function _runPageCurrentQuery() {
  const params = new URLSearchParams();
  if (_runPageState.pageTab && _runPageState.pageTab !== 'overview') params.set('tab', _runPageState.pageTab);
  if (_runPageState.selectedSeries.length) params.set('series', _runPageState.selectedSeries.map(encodeURIComponent).join(','));
  if (_runPageState.selectedScalars.length) params.set('scalars', _runPageState.selectedScalars.map(encodeURIComponent).join(','));
  if (_runPageState.metricQuery) params.set('q', _runPageState.metricQuery);
  if (_runPageState.catalogTab !== 'series') params.set('kind', _runPageState.catalogTab);
  if (_runPageState.xAxis !== 'step') params.set('x', _runPageState.xAxis);
  if (_runPageState.groupByContext) params.set('group', _runPageState.groupByContext);
  if (_runPageState.yScale !== 'linear') params.set('yscale', _runPageState.yScale);
  if (_runPageState.smoothing !== 0.25) params.set('smooth', String(_runPageState.smoothing));
  if (_runPageState.showRaw) params.set('raw', '1');
  if (_runPageState.ignoreOutliers) params.set('outliers', '0');
  if (_runPageState.tooltipMode !== 'all') params.set('hover', _runPageState.tooltipMode);
  const text = params.toString();
  return text ? `?${text}` : '';
}

function _runPageReplaceUrlState() {
  if (!_runPageState.cluster || !_runPageState.runHash) return;
  const next = _runPageBaseHash(_runPageState.cluster, _runPageState.runHash) + _runPageCurrentQuery();
  if (`${location.pathname}${location.search}` !== next) history.replaceState(null, '', next);
}

function _runPageSyncTabLabel(run) {
  if (typeof _appTabs === 'undefined') return;
  const t = _appTabs.find(tab => tab.type === 'run' && tab.runCluster === _runPageState.cluster && tab.runHash === _runPageState.runHash);
  if (!t) return;
  t.label = run.run_name || run.name || `Run ${_runPageState.runHash}`;
  if (typeof _renderAppTabs === 'function') _renderAppTabs();
  if (typeof _persistTabs === 'function') _persistTabs();
}

function _runPageNormalizeMetrics(payload) {
  return {
    metadata: payload.metadata && typeof payload.metadata === 'object' ? payload.metadata : {},
    series: payload.series && typeof payload.series === 'object' ? payload.series : {},
    latest: payload.latest && typeof payload.latest === 'object' ? payload.latest : {},
    scalars: payload.scalars && typeof payload.scalars === 'object' ? payload.scalars : {},
    scalar_latest: payload.scalar_latest && typeof payload.scalar_latest === 'object' ? payload.scalar_latest : {},
  };
}

function _runPageApplyDefaultSelections() {
  const numericSeries = _runPageNumericSeriesKeys();
  const numericScalars = _runPageNumericScalarKeys();
  _runPageState.selectedSeries = _runPageState.selectedSeries.filter(k => numericSeries.includes(k));
  _runPageState.selectedScalars = _runPageState.selectedScalars.filter(k => numericScalars.includes(k));
  if (!_runPageState.selectedSeries.length) _runPageState.selectedSeries = numericSeries.slice(0, 4);
  if (!_runPageState.selectedScalars.length) _runPageState.selectedScalars = numericScalars.slice(0, 10);
}

function _runPageNumericSeriesKeys() {
  const series = _runPageState.metrics.series || {};
  return Object.keys(series).filter(k => (series[k] || []).filter(p => Number.isFinite(p.value_num)).length >= 2).sort();
}

function _runPageNumericScalarKeys() {
  const scalars = _runPageState.metrics.scalars || {};
  return Object.keys(scalars).filter(k => {
    const pts = scalars[k] || [];
    const p = pts[pts.length - 1] || {};
    return Number.isFinite(p.value_num);
  }).sort();
}

function _runPageMetricRecords(kind) {
  const records = [];
  const add = (metricKind, source) => {
    Object.keys(source || {}).forEach((key) => {
      const points = source[key] || [];
      const stats = _runPageStats(points, metricKind === 'series');
      const latestPoint = points[points.length - 1] || {};
      const numeric = metricKind === 'series'
        ? stats.numericCount >= 2
        : Number.isFinite(latestPoint.value_num);
      records.push({
        key,
        kind: metricKind,
        points,
        stats,
        numeric,
        context: _runPageMergedContext(points),
        contexts: _runPageMetricContexts(points),
        metadata: _runPageState.metrics.metadata || {},
      });
    });
  };
  if (!kind || kind === 'series') add('series', _runPageState.metrics.series);
  if (!kind || kind === 'scalars') add('scalars', _runPageState.metrics.scalars);
  return records.sort((a, b) => a.key.localeCompare(b.key));
}

function _runPageFilteredMetricRecords(kind) {
  const query = _runPageState.metricQuery || '';
  return _runPageMetricRecords(kind).filter(record => _runPageMatchesMetricQuery(record, query));
}

function _runPageMergedContext(points) {
  const merged = {};
  for (const p of points || []) {
    const ctx = p.context || {};
    Object.entries(ctx).forEach(([k, v]) => {
      if (merged[k] == null) merged[k] = v;
      else if (merged[k] !== v) {
        if (!Array.isArray(merged[k])) merged[k] = [merged[k]];
        if (!merged[k].includes(v)) merged[k].push(v);
      }
    });
  }
  return merged;
}

function _runPageMatchesMetricQuery(record, query) {
  const q = String(query || '').trim();
  if (!q) return true;
  const orParts = _runPageSplitQuery(q, 'or');
  return orParts.some(part => {
    const andParts = _runPageSplitQuery(part, 'and');
    return andParts.every(term => _runPageEvalQueryTerm(record, term));
  });
}

function _runPageSplitQuery(query, op) {
  const parts = [];
  let buf = '';
  let quote = '';
  let depth = 0;
  const words = query.split(/(\s+)/);
  for (let i = 0; i < words.length; i++) {
    const token = words[i];
    for (const ch of token) {
      if (quote) {
        if (ch === quote) quote = '';
      } else if (ch === '"' || ch === "'") quote = ch;
      else if (ch === '(' || ch === '[') depth++;
      else if (ch === ')' || ch === ']') depth = Math.max(0, depth - 1);
    }
    if (!quote && depth === 0 && token.trim().toLowerCase() === op) {
      if (buf.trim()) parts.push(buf.trim());
      buf = '';
    } else {
      buf += token;
    }
  }
  if (buf.trim()) parts.push(buf.trim());
  return parts.length ? parts : [query];
}

function _runPageEvalQueryTerm(record, rawTerm) {
  let term = String(rawTerm || '').trim();
  if (!term) return true;
  let negate = false;
  if (/^not\s+/i.test(term)) {
    negate = true;
    term = term.replace(/^not\s+/i, '').trim();
  }
  if (term.startsWith('(') && term.endsWith(')')) term = term.slice(1, -1).trim();

  let result = _runPageEvalPositiveQueryTerm(record, term);
  return negate ? !result : result;
}

function _runPageEvalPositiveQueryTerm(record, term) {
  let m = term.match(/^(.+?)\.(contains|startswith|endswith)\((.+)\)$/i);
  if (m) {
    const value = String(_runPageResolveMetricField(record, m[1]) ?? '').toLowerCase();
    const arg = String(_runPageParseQueryValue(m[3]) ?? '').toLowerCase();
    if (m[2].toLowerCase() === 'contains') return value.includes(arg);
    if (m[2].toLowerCase() === 'startswith') return value.startsWith(arg);
    return value.endsWith(arg);
  }

  m = term.match(/^(.+?)\s+in\s+\[(.*)\]$/i);
  if (m) {
    const value = _runPageResolveMetricField(record, m[1]);
    const values = _runPageParseQueryList(m[2]);
    return values.some(v => String(v) === String(value));
  }

  m = term.match(/^(.+?)\s*(==|!=|>=|<=|>|<)\s*(.+)$/);
  if (m) {
    const left = _runPageResolveMetricField(record, m[1]);
    const right = _runPageParseQueryValue(m[3]);
    const op = m[2];
    if (op === '==') return String(left) === String(right);
    if (op === '!=') return String(left) !== String(right);
    const lnum = Number(left);
    const rnum = Number(right);
    if (!Number.isFinite(lnum) || !Number.isFinite(rnum)) return false;
    if (op === '>=') return lnum >= rnum;
    if (op === '<=') return lnum <= rnum;
    if (op === '>') return lnum > rnum;
    if (op === '<') return lnum < rnum;
  }

  const needle = _runPageStripQuotes(term).toLowerCase();
  const hay = [
    record.key,
    record.kind,
    ...record.contexts,
    ...Object.entries(record.metadata || {}).map(([k, v]) => `${k}:${_formatMetricValue(v)}`),
  ].join(' ').toLowerCase();
  return hay.includes(needle);
}

function _runPageResolveMetricField(record, rawField) {
  const field = String(rawField || '').trim();
  if (field === 'metric' || field === 'metric.name' || field === 'metric.key' || field === 'name' || field === 'key') return record.key;
  if (field === 'metric.kind' || field === 'kind') return record.kind;
  if (field === 'metric.count' || field === 'count' || field === 'points') return (record.points || []).length;
  if (field === 'metric.latest' || field === 'latest') return record.stats.latest;
  if (field === 'metric.min' || field === 'min') return record.stats.min;
  if (field === 'metric.max' || field === 'max') return record.stats.max;
  let m = field.match(/^context(?:\.([A-Za-z0-9_-]+)|\[['"]([^'"]+)['"]\])$/);
  if (m) return record.context[m[1] || m[2]];
  m = field.match(/^metadata(?:\.([A-Za-z0-9_-]+)|\[['"]([^'"]+)['"]\])$/);
  if (m) return record.metadata[m[1] || m[2]];
  return undefined;
}

function _runPageParseQueryList(text) {
  const values = [];
  let buf = '';
  let quote = '';
  for (const ch of String(text || '')) {
    if (quote) {
      buf += ch;
      if (ch === quote) quote = '';
    } else if (ch === '"' || ch === "'") {
      quote = ch;
      buf += ch;
    } else if (ch === ',') {
      if (buf.trim()) values.push(_runPageParseQueryValue(buf));
      buf = '';
    } else {
      buf += ch;
    }
  }
  if (buf.trim()) values.push(_runPageParseQueryValue(buf));
  return values;
}

function _runPageParseQueryValue(raw) {
  const text = _runPageStripQuotes(String(raw || '').trim());
  if (/^-?\d+(?:\.\d+)?$/.test(text)) return Number(text);
  if (text === 'true') return true;
  if (text === 'false') return false;
  return text;
}

function _runPageStripQuotes(text) {
  const s = String(text || '').trim();
  if ((s.startsWith('"') && s.endsWith('"')) || (s.startsWith("'") && s.endsWith("'"))) {
    return s.slice(1, -1);
  }
  return s;
}

function _runPageContextSummary() {
  const counts = {};
  for (const record of _runPageFilteredMetricRecords()) {
    for (const point of record.points || []) {
      Object.entries(point.context || {}).forEach(([key, value]) => {
        const id = `${key}=${String(value)}`;
        if (!counts[id]) counts[id] = { key, value, count: 0 };
        counts[id].count += 1;
      });
    }
  }
  return Object.values(counts).sort((a, b) => b.count - a.count || a.key.localeCompare(b.key)).slice(0, 24);
}

function _runPageContextKeys() {
  const keys = new Set();
  for (const record of _runPageMetricRecords()) {
    for (const point of record.points || []) {
      Object.keys(point.context || {}).forEach(k => keys.add(k));
    }
  }
  return Array.from(keys).sort();
}

function _runPageRenderLoading(cluster, runHash) {
  const el = document.getElementById('run-page');
  if (!el) return;
  el.innerHTML = `<div class="run-page-empty">Loading ${_escHtml(cluster)} / ${_escHtml(runHash)}…</div>`;
}

function _runPageRenderMessage(message) {
  const el = document.getElementById('run-page');
  if (!el) return;
  el.innerHTML = `<div class="run-page-empty">${_escHtml(message)}</div>`;
}

function _runPageRender() {
  const el = document.getElementById('run-page');
  const run = _runPageState.run;
  if (!el || !run) return;
  const title = run.run_name || run.name || `Run ${_runPageState.runHash}`;
  const jobs = run.jobs || [];
  const logJob = typeof _runPrimaryLogJob === 'function'
    ? _runPrimaryLogJob(run)
    : { jobId: run.root_job_id || (jobs[0] && (jobs[0].job_id || jobs[0].jobid)) || '', name: title };
  const tabs = [
    ['overview', 'Overview'],
    ['metadata', 'Metadata'],
    ['metrics', 'Metrics'],
    ['jobs', `Jobs${jobs.length ? ` (${jobs.length})` : ''}`],
  ];
  const tabButtons = tabs.map(([id, label]) => `
    <button type="button" class="run-page-tab${_runPageState.pageTab === id ? ' active' : ''}" onclick="_runPageSetTab('${id}')">${label}</button>
  `).join('');

  el.innerHTML = `
    <div class="run-page-head">
      <div class="run-page-title-wrap">
        <div class="run-page-kicker">${_escHtml(_runPageState.cluster)} · run ${_escHtml(_runPageState.runHash)}</div>
        <div class="run-page-title">${_escHtml(title)}</div>
      </div>
      <div class="run-page-head-actions">
        <button class="btn" onclick="showTab('history')">Runs</button>
        ${logJob.jobId ? `<button class="btn" onclick="_openRunLog(${_jsArg(_runPageState.cluster)},${_jsArg(logJob.jobId)},${_jsArg(logJob.name || title)})">Log</button>` : ''}
        <button class="btn" onclick="openRunInfoByHash('${escAttr(_runPageState.cluster)}','${escAttr(_runPageState.runHash)}','${escAttr(title)}')">quick peek</button>
        <button class="btn" onclick="openRunPage('${escAttr(_runPageState.cluster)}','${escAttr(_runPageState.runHash)}', true)">↻ refresh</button>
        ${run.id ? `<button class="btn run-delete-btn" onclick="_runPageDelete()" title="Permanently delete this run and all its metrics/metadata">Delete run</button>` : ''}
      </div>
    </div>
    <div class="run-page-tabs">${tabButtons}</div>
    <div class="run-page-panel" id="run-page-panel"></div>
  `;
  _runPageRenderPanel();
}

function _runPageRenderPanel() {
  const el = document.getElementById('run-page-panel');
  if (!el) return;
  _runPageDestroyCharts();
  if (_runPageState.pageTab === 'overview') el.innerHTML = _runPageOverviewHtml();
  else if (_runPageState.pageTab === 'metadata' || _runPageState.pageTab === 'provenance') {
    el.innerHTML = _runPageMetadataHtml();
  }
  else if (_runPageState.pageTab === 'jobs') el.innerHTML = _runPageJobsHtml();
  else {
    el.innerHTML = _runPageMetricsHtml();
    _runPageRenderCharts();
  }
}

function _runPageSetTab(tab) {
  _runPageState.pageTab = tab || 'overview';
  _runPageReplaceUrlState();
  _runPageRender();
}

function _runPageOverviewHtml() {
  const run = _runPageState.run || {};
  const jobs = run.jobs || [];
  const earliest = _earliestTime(jobs, 'started');
  const latest = _latestTime(jobs, 'ended_at');
  const duration = earliest && latest ? _formatDuration(earliest, latest) : '—';
  const gpusPerNode = run.gpus_per_node;
  const _jobStates = _computeJobStateSummary(jobs, gpusPerNode);
  const _durationRing = _runDurationRing(earliest, latest);
  const _runLabel = run.run_name || run.name || '';
  const _cancelableRunIds = jobs
    .filter(j => _isActivelyCancelableState((j.state || '').toUpperCase()))
    .map(j => String(j.job_id || j.jobid));
  const cluster = _runPageState.cluster;
  const _cancelRunBtn = _cancelableRunIds.length > 0
    ? `<button class="action-btn cancel-run-btn" onclick="_cancelRun('${escAttr(cluster)}',${escAttr(JSON.stringify(_cancelableRunIds))},'${escAttr(_runLabel)}')">cancel run</button>`
    : '';
  const _resubmitBtn = run.can_resubmit
    ? `<button class="action-btn resubmit-run-btn" onclick="_resubmitRun('${escAttr(cluster)}','${escAttr(run.run_hash || _runPageState.runHash || '')}','${escAttr(_runLabel)}')" title="Re-run the original submission command locally">resubmit</button>`
    : '';
  const sdkExtra = (run.source === 'sdk' || run.source === 'sdk+legacy') ? `
    <div class="run-timing-item">
      <span class="run-timing-label">Source</span>
      <span class="run-timing-value" style="color:var(--accent);font-weight:600">SDK</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Git</span>
      <span class="run-timing-value">${_escHtml(run.git_commit || '—')}</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Launcher</span>
      <span class="run-timing-value">${_escHtml(run.launcher_hostname || '—')}</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Working dir</span>
      <span class="run-timing-value" style="word-break:break-all">${_escHtml(run.submit_cwd || '—')}</span>
    </div>` : '';

  const scalarLatest = _runPageLatestScalarBlock();
  const facts = _runPageKeyValueBlock('Run Facts', {
    cluster: _runPageState.cluster,
    run_hash: _runPageState.runHash,
    root_job_id: run.root_job_id || '',
    source: run.source || '',
    git_commit: run.git_commit || '',
    launcher: run.launcher_hostname || '',
    working_dir: run.submit_cwd || '',
    output_dir: run.primary_output_dir || '',
  });

  return `<div class="run-page-overview-grid">
    <div>
      <div class="run-page-card">
        <div class="run-page-card-title">Notes</div>
        <textarea class="run-page-notes" placeholder="Add notes about this run…" oninput="_runPageOnNoteInput()" onblur="_runPageSaveNotes()">${_escHtml(run.notes || '')}</textarea>
        <div class="run-page-save-state" id="run-page-notes-saved">saved</div>
      </div>
      <div class="run-page-card run-page-malfunction-card">
        <div class="run-page-card-title">Run quality</div>
        <label class="run-page-malfunction-label">
          <input type="checkbox" class="run-page-malfunction-cb" ${run.malfunctioned ? 'checked' : ''}
                 onchange="_runPageToggleMalfunctioned(this.checked)">
          <span>Malfunctioned — treat metrics as low-trust; experiment should be redone</span>
        </label>
      </div>
      <div class="run-page-card run-page-overview-summary">
        <div class="run-page-card-title">Timeline & jobs</div>
        <div class="run-timing">
          <div class="run-timing-item">
            <span class="run-timing-label">Started</span>
            <span class="run-timing-value">${_fmtRunTime(earliest)}</span>
          </div>
          <div class="run-timing-item">
            <span class="run-timing-label">Ended</span>
            <span class="run-timing-value">${_fmtRunTime(latest)}</span>
          </div>
          <div class="run-timing-item">
            <span class="run-timing-label">Duration</span>
            <span class="run-timing-value" style="display:inline-flex;align-items:center;gap:5px">${_durationRing}${duration}</span>
          </div>
          <div class="run-timing-item">
            <span class="run-timing-label">Project</span>
            <span class="run-timing-value">${_escHtml(run.project || '—')}</span>
          </div>
          ${sdkExtra}
        </div>
        <div class="run-resource-bar run-page-resource-bar">
          <span class="job-count-text">${_jobStates}</span>
          ${_cancelRunBtn}
          ${_resubmitBtn}
        </div>
      </div>
      ${facts}
    </div>
    <div>
      ${scalarLatest}
      <div id="run-page-custom-metrics">${_runPageCustomMetricsHtml()}</div>
    </div>
  </div>`;
}

function _runPageMetadataHtml() {
  const run = _runPageState.run || {};
  const paramsHtml = _renderRunParams(run.params, run.root_job_id);
  const _suffix = String(run.root_job_id || run.id || '0').replace(/\W/g, '_');
  const metaHtml = _renderSdkMetadataTree(run.metadata || {}, `page-db-${_suffix}`);
  const metricsMetaHtml = _renderSdkMetadataTree(_runPageState.metrics.metadata || {}, `page-api-${_suffix}`);

  const sections = [];
  if (run.submit_command) sections.push(_renderToggleSection('run-page-submit-cmd', 'Submit Command', `<pre>${_escHtml(run.submit_command)}</pre>`, true));
  if (run.batch_script) sections.push(_renderToggleSection('run-page-batch-script', 'Batch Script', `<pre>${_escHtml(run.batch_script)}</pre>`, true));
  if (run.scontrol_raw) sections.push(_renderToggleSection('run-page-scontrol', 'Slurm Configuration', `<pre>${_escHtml(run.scontrol_raw)}</pre>`, true));
  if (run.env_vars) sections.push(_renderToggleSection('run-page-env-vars', 'Environment Variables', _renderEnvTable(run.env_vars), true));
  if (run.conda_state) sections.push(_renderToggleSection('run-page-conda', 'Conda / Pip State', `<pre>${_escHtml(run.conda_state)}</pre>`, true));

  const hasPrelude = !!(metaHtml || metricsMetaHtml);
  const accordionInner = `${paramsHtml}${sections.join('')}`;
  const hasAccordions = !!(paramsHtml || sections.length);
  let inner = '';
  if (!hasPrelude && !hasAccordions) {
    inner = '<div class="run-empty-state">No infrastructure metadata captured yet.</div>';
  } else {
    inner = `${metaHtml}${metricsMetaHtml}`;
    if (hasAccordions) {
      inner += `<div class="run-page-meta-accordions">${accordionInner}</div>`;
    }
  }
  const provClass = hasAccordions ? 'run-page-provenance run-page-provenance--stack' : 'run-page-provenance';
  return `<div class="${provClass}">${inner}</div>`;
}

function _runPageMetricsHtml() {
  const seriesCount = Object.keys(_runPageState.metrics.series || {}).length;
  const scalarCount = Object.keys(_runPageState.metrics.scalars || {}).length;
  if (!seriesCount && !scalarCount) {
    return `<div class="run-page-empty">No tracked SDK metrics yet.</div>`;
  }
  return `<div class="run-page-metrics-layout">
    <section class="run-page-chart-stack">
      ${_runPageSelectorHtml(seriesCount, scalarCount)}
      <div class="run-page-card">
        <div class="run-page-card-title-row">
          <div>
            <div class="run-page-card-title">Charts Explorer</div>
            <div class="run-page-card-sub">${_runPageState.selectedSeries.length} selected · ${_runPageState.xAxis.replace('_', ' ')} axis${_runPageState.groupByContext ? ` · grouped by ${_escHtml(_runPageState.groupByContext)}` : ''}</div>
          </div>
        </div>
        <div class="run-page-chart"><canvas id="run-page-series-chart"></canvas></div>
      </div>
      <div class="run-page-card">
        <div class="run-page-card-title-row">
          <div>
            <div class="run-page-card-title">Scalar Stats</div>
            <div class="run-page-card-sub">${_runPageState.selectedScalars.length} selected</div>
          </div>
        </div>
        <div class="run-page-scalar-wrap">
          <div class="run-page-scalar-chart"><canvas id="run-page-scalar-chart"></canvas></div>
          <div class="run-page-stat-grid">${_runPageSelectedStatCardsHtml()}</div>
        </div>
      </div>
      ${_runPageContextTableHtml()}
    </section>
    <aside class="run-page-inspector">
      ${_runPageInspectorHtml()}
    </aside>
  </div>`;
}

function _runPageSelectorHtml(seriesCount, scalarCount) {
  const filteredSeries = _runPageFilteredMetricRecords('series').filter(r => r.numeric);
  const filteredScalars = _runPageFilteredMetricRecords('scalars').filter(r => r.numeric);
  const contextChips = _runPageContextSummary().map(item => `
    <button type="button" class="run-page-context-chip" onclick="_runPageApplyContextFilter('${escAttr(item.key)}','${escAttr(String(item.value))}')">
      <span>${_escHtml(item.key)}=${_escHtml(String(item.value))}</span>
      <b>${item.count}</b>
    </button>
  `).join('');
  return `<div class="run-page-card run-page-selector-card">
    <div class="run-page-card-title-row">
      <div>
        <div class="run-page-card-title">Metrics Select</div>
        <div class="run-page-card-sub">Python-like filter over metric, context, and metadata. ${filteredSeries.length} / ${seriesCount} series, ${filteredScalars.length} / ${scalarCount} scalars match.</div>
      </div>
      <div class="run-page-selector-actions">
        <button class="btn" onclick="_runPageSelectQueryMatches()">select matches</button>
        <button class="btn" onclick="_runPageClearMetricQuery()">clear query</button>
      </div>
    </div>
    <input class="run-page-query-input" type="text" value="${escAttr(_runPageState.metricQuery)}"
           placeholder='metric.name.contains("loss") and context.split == "eval"'
           onchange="_runPageSetMetricQuery(this.value)"
           onkeydown="if(event.key==='Enter')_runPageSetMetricQuery(this.value)">
    <div class="run-page-query-hint">
      Examples: <code>metric.kind == "series"</code>, <code>context.split == "eval"</code>, <code>metadata.model.contains("synthetic")</code>, <code>metric.name.startswith("system/")</code>
    </div>
    <div class="run-page-dropdown-row">
      <label>Series
        <select class="run-page-metric-select" onchange="_runPageAddSeries(this.value); this.value=''">
          ${_runPageMetricSelectOptions('series')}
        </select>
      </label>
      <label>Scalars
        <select class="run-page-metric-select" onchange="_runPageAddScalar(this.value); this.value=''">
          ${_runPageMetricSelectOptions('scalars')}
        </select>
      </label>
    </div>
    <div class="run-page-selected-section">
      <div class="run-page-mini-label">Selected series</div>
      <div class="run-page-selected-chips">${_runPageSelectedMetricChips('series')}</div>
      <div class="run-page-mini-label">Selected scalars</div>
      <div class="run-page-selected-chips">${_runPageSelectedMetricChips('scalars')}</div>
    </div>
    <div class="run-page-context-section">
      <div class="run-page-mini-label">Metric context</div>
      <div class="run-page-context-cloud">${contextChips || '<span class="run-page-muted">No context on matching metrics.</span>'}</div>
    </div>
  </div>`;
}

function _runPageMetricSelectOptions(kind) {
  const selected = new Set(kind === 'series' ? _runPageState.selectedSeries : _runPageState.selectedScalars);
  const label = kind === 'series' ? 'Add series metric…' : 'Add scalar metric…';
  const records = _runPageFilteredMetricRecords(kind).filter(r => r.numeric && !selected.has(r.key));
  const options = records.map(r => {
    const latest = r.stats.latest == null ? '—' : _formatMetricValue(r.stats.latest);
    return `<option value="${escAttr(r.key)}">${_escHtml(r.key)} · latest ${_escHtml(latest)}</option>`;
  }).join('');
  return `<option value="">${label}</option>${options || '<option value="" disabled>No matching metrics</option>'}`;
}

function _runPageSelectedMetricChips(kind) {
  const selected = kind === 'series' ? _runPageState.selectedSeries : _runPageState.selectedScalars;
  if (!selected.length) return '<span class="run-page-muted">None selected.</span>';
  const fn = kind === 'series' ? '_runPageRemoveSeries' : '_runPageRemoveScalar';
  return selected.map(key => `
    <button type="button" class="run-page-selected-chip" onclick="${fn}('${escAttr(key)}')">
      ${_escHtml(key)} <span>×</span>
    </button>
  `).join('');
}

function _runPageSetMetricQuery(value) {
  _runPageState.metricQuery = value || '';
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageSetCatalogTab(tab) {
  _runPageState.catalogTab = tab || 'series';
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageSetXAxis(value) {
  _runPageState.xAxis = value || 'step';
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageSetGroupByContext(value) {
  _runPageState.groupByContext = value || '';
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageSetYScale(value) {
  _runPageState.yScale = value || 'linear';
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageSetTooltipMode(value) {
  _runPageState.tooltipMode = value === 'nearest' ? 'nearest' : 'all';
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageSetShowRaw(value) {
  _runPageState.showRaw = !!value;
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageSetIgnoreOutliers(value) {
  _runPageState.ignoreOutliers = !!value;
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageSetSmoothing(value) {
  _runPageState.smoothing = Math.max(0, Math.min(0.95, parseFloat(value) || 0));
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPagePreviewSmoothingValue(value) {
  const el = document.getElementById('run-page-smoothing-value');
  if (el) el.textContent = _runPageSmoothingLabel(value);
}

function _runPageSmoothingLabel(value) {
  const n = Math.max(0, Math.min(0.95, parseFloat(value) || 0));
  return n.toFixed(2);
}

function _runPageMetricContexts(points) {
  const seen = new Set();
  for (const p of points || []) {
    const ctx = p.context || {};
    Object.entries(ctx).forEach(([k, v]) => seen.add(`${k}:${v}`));
  }
  return Array.from(seen);
}

function _runPageAddSeries(key) {
  if (!key || _runPageState.selectedSeries.includes(key)) return;
  _runPageState.selectedSeries = [..._runPageState.selectedSeries, key];
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageAddScalar(key) {
  if (!key || _runPageState.selectedScalars.includes(key)) return;
  _runPageState.selectedScalars = [..._runPageState.selectedScalars, key];
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageRemoveSeries(key) {
  _runPageState.selectedSeries = _runPageState.selectedSeries.filter(k => k !== key);
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageRemoveScalar(key) {
  _runPageState.selectedScalars = _runPageState.selectedScalars.filter(k => k !== key);
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageSelectQueryMatches() {
  _runPageState.selectedSeries = _runPageFilteredMetricRecords('series').filter(r => r.numeric).map(r => r.key).slice(0, 8);
  _runPageState.selectedScalars = _runPageFilteredMetricRecords('scalars').filter(r => r.numeric).map(r => r.key).slice(0, 16);
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageClearMetricQuery() {
  _runPageState.metricQuery = '';
  _runPageReplaceUrlState();
  _runPageRenderPanel();
}

function _runPageApplyContextFilter(key, value) {
  _runPageState.metricQuery = `context.${key} == "${String(value).replace(/"/g, '\\"')}"`;
  _runPageSelectQueryMatches();
}

function _runPageRenderCharts() {
  const seriesCanvas = document.getElementById('run-page-series-chart');
  const scalarCanvas = document.getElementById('run-page-scalar-chart');
  const seriesDatasets = _runPageBuildSeriesDatasets();
  const scalarInfo = _runPageBuildScalarDatasetInfo();
  if (typeof Chart === 'undefined') {
    if (_runPageChartRetryTimer) clearTimeout(_runPageChartRetryTimer);
    _runPageChartRetryTimer = setTimeout(() => {
      _runPageChartRetryTimer = null;
      if (_runPageState.pageTab === 'metrics' && document.getElementById('run-page-series-chart')) {
        _runPageRenderCharts();
      }
    }, 80);
    return;
  }
  if (_runPageChartRetryTimer) {
    clearTimeout(_runPageChartRetryTimer);
    _runPageChartRetryTimer = null;
  }
  if (seriesCanvas) {
    if (seriesDatasets.length) {
      _runPageState.charts.push(new Chart(seriesCanvas, {
        type: 'line',
        data: { datasets: seriesDatasets },
        options: _runPageChartOptions(_runPageXAxisLabel(), _runPageState.yScale),
      }));
    }
  }
  if (scalarCanvas) {
    if (scalarInfo.labels.length) {
      _runPageState.charts.push(new Chart(scalarCanvas, {
        type: 'bar',
        data: {
          labels: scalarInfo.labels,
          datasets: [{ label: 'latest scalar', data: scalarInfo.values, borderColor: scalarInfo.colors, backgroundColor: scalarInfo.colors.map(c => c + '66'), borderWidth: 1 }],
        },
        options: _runPageChartOptions('', 'linear', true),
      }));
    }
  }
  requestAnimationFrame(() => {
    (_runPageState.charts || []).forEach(chart => {
      try { chart.resize(); chart.update('none'); } catch (_) {}
    });
  });
}

function _runPageBuildScalarDatasetInfo() {
  const labels = [];
  const values = [];
  const colors = [];
  const legendItems = [];
  _runPageState.selectedScalars.forEach((key, idx) => {
    const pts = _runPageState.metrics.scalars[key] || [];
    const p = pts[pts.length - 1] || {};
    const color = RUN_PAGE_COLORS[idx % RUN_PAGE_COLORS.length];
    const id = `scalar:${key}`;
    const hidden = !!_runPageState.hiddenTraces[id];
    if (Number.isFinite(p.value_num)) {
      if (!hidden) {
        labels.push(key);
        values.push(p.value_num);
        colors.push(color);
      }
      legendItems.push({
        id,
        label: key,
        color,
        value: _formatMetricValue(p.value),
        meta: _runPageMetricContexts(pts).slice(0, 2).join(' · ') || 'scalar',
        hidden,
      });
    }
  });
  return { labels, values, colors, legendItems };
}

function _runPageLegendItemsFromDatasets(datasets) {
  return (datasets || []).map(ds => ({
    id: ds._legendId || ds.label,
    label: ds.label,
    color: ds.borderColor || ds._legendColor || '#22c55e',
    value: ds._legendValue || '',
    meta: ds._legendMeta || '',
    muted: !!ds._legendMuted,
    hidden: !!ds.hidden,
  }));
}

function _runPageLegendHtml(title, items, compact = false) {
  if (!items || !items.length) {
    return `<div class="run-page-card run-page-legend-card">
      <div class="run-page-card-title">${_escHtml(title)}</div>
      <div class="run-page-legend-empty">No visible traces.</div>
    </div>`;
  }
  return `<div class="run-page-card run-page-legend-card">
    <div class="run-page-card-title">${_escHtml(title)}</div>
    <div class="run-page-aim-legend sidebar${compact ? ' compact' : ''}">
      ${items.map(item => `
    <button type="button" class="run-page-legend-item${item.muted ? ' muted' : ''}${item.hidden ? ' hidden-trace' : ''}" style="--legend-color:${escAttr(item.color)}" onclick="_runPageToggleTrace('${escAttr(item.id)}')">
      <span class="run-page-legend-eye">${item.hidden ? '○' : '●'}</span>
      <span class="run-page-legend-line"></span>
      <span class="run-page-legend-name" title="${escAttr(item.label)}">${_escHtml(item.label)}</span>
      ${item.value ? `<span class="run-page-legend-value">${_escHtml(item.value)}</span>` : ''}
      ${item.meta ? `<span class="run-page-legend-meta">${_escHtml(item.meta)}</span>` : ''}
    </button>
  `).join('')}
    </div>
  </div>`;
}

function _runPageToggleTrace(id) {
  if (!id) return;
  _runPageState.hiddenTraces[id] = !_runPageState.hiddenTraces[id];
  _runPageRenderPanel();
}

function _runPageIsTraceHidden(id) {
  return !!_runPageState.hiddenTraces[id];
}

function _runPageBuildSeriesDatasets() {
  const datasets = [];
  let colorIdx = 0;
  _runPageState.selectedSeries.forEach((key, idx) => {
    const rawAll = (_runPageState.metrics.series[key] || []).filter(p => Number.isFinite(p.value_num));
    const groups = _runPageGroupSeriesPoints(rawAll);
    for (const [groupName, rawGroup] of groups) {
      const raw = _runPageState.ignoreOutliers ? _runPageFilterOutlierPoints(rawGroup) : rawGroup;
      const chartPoints = _runPageDownsample(_runPagePointsForAxis(raw), 2200);
      const visiblePoints = _runPageState.yScale === 'logarithmic'
        ? chartPoints.filter(p => p.y > 0)
        : chartPoints;
      const color = RUN_PAGE_COLORS[colorIdx % RUN_PAGE_COLORS.length];
      colorIdx++;
      const label = groupName ? `${key} · ${groupName}` : key;
      const legendId = `series:${label}`;
      const hidden = !!_runPageState.hiddenTraces[legendId];
      const latest = raw[raw.length - 1] || {};
      const contextMeta = _runPageMetricContexts(raw).slice(0, 2).join(' · ');
      if (_runPageState.showRaw && _runPageState.smoothing > 0 && visiblePoints.length) {
        const rawLegendId = `${legendId}:raw`;
        datasets.push({
          label: `${label} raw`,
          data: visiblePoints,
          borderColor: color + '55',
          backgroundColor: color + '11',
          borderWidth: 1,
          pointRadius: 0,
          tension: 0,
          hidden: !!_runPageState.hiddenTraces[rawLegendId],
          _legendId: rawLegendId,
          _legendValue: latest.value == null ? '' : _formatMetricValue(latest.value),
          _legendMeta: contextMeta || 'original',
          _legendMuted: true,
        });
      }
      const smoothed = _runPageSmoothPoints(visiblePoints, _runPageState.smoothing);
      if (smoothed.length) {
        datasets.push({
          label,
          data: smoothed,
          borderColor: color,
          backgroundColor: color + '22',
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 6,
          pointHitRadius: 10,
          pointHoverBorderWidth: 2,
          pointHoverBorderColor: color,
          pointHoverBackgroundColor: csColorMix(color, '#ffffff', 0.35),
          tension: 0,
          hidden,
          _legendId: legendId,
          _legendValue: latest.value == null ? '' : _formatMetricValue(latest.value),
          _legendMeta: contextMeta || 'series',
        });
      }
    }
  });
  return datasets;
}

function _runPageGroupSeriesPoints(points) {
  const key = _runPageState.groupByContext;
  if (!key) return [['', points || []]];
  const groups = {};
  for (const p of points || []) {
    const value = p.context && p.context[key] != null ? String(p.context[key]) : 'None';
    if (!groups[value]) groups[value] = [];
    groups[value].push(p);
  }
  return Object.entries(groups).sort((a, b) => a[0].localeCompare(b[0]));
}

function _runPageFilterOutlierPoints(points) {
  const nums = (points || []).map(p => p.value_num).filter(Number.isFinite).sort((a, b) => a - b);
  if (nums.length < 8) return points || [];
  const q1 = nums[Math.floor(nums.length * 0.25)];
  const q3 = nums[Math.floor(nums.length * 0.75)];
  const iqr = q3 - q1;
  const lo = q1 - 1.5 * iqr;
  const hi = q3 + 1.5 * iqr;
  return (points || []).filter(p => p.value_num >= lo && p.value_num <= hi);
}

function _runPagePointsForAxis(points) {
  const firstTs = points.find(p => Number.isFinite(p.ts))?.ts || 0;
  return points.map((p, idx) => {
    let x = idx + 1;
    if (_runPageState.xAxis === 'step') x = p.step == null ? idx + 1 : p.step;
    else if (_runPageState.xAxis === 'wall_time') x = Number.isFinite(p.ts) && firstTs ? (p.ts - firstTs) / 60 : idx + 1;
    return { x, y: p.value_num, raw: p };
  });
}

function _runPageXAxisLabel() {
  if (_runPageState.xAxis === 'wall_time') return 'minutes since first point';
  if (_runPageState.xAxis === 'index') return 'point index';
  return 'step';
}

function _runPageDownsample(points, maxPoints = 2000) {
  if (!Array.isArray(points) || points.length <= maxPoints) return points || [];
  const bucketSize = Math.ceil(points.length / maxPoints);
  const out = [];
  for (let i = 0; i < points.length; i += bucketSize) {
    const bucket = points.slice(i, i + bucketSize);
    const picks = [bucket[0], bucket[bucket.length - 1]];
    let min = bucket[0], max = bucket[0];
    for (const p of bucket) {
      if (p.y < min.y) min = p;
      if (p.y > max.y) max = p;
    }
    picks.push(min, max);
    for (const p of picks.sort((a, b) => a.x - b.x)) {
      if (!out.length || out[out.length - 1] !== p) out.push(p);
    }
  }
  return out;
}

function _runPageSmoothPoints(points, amount) {
  if (!Array.isArray(points) || !points.length || amount <= 0) return points || [];
  const weight = Math.min(Math.sqrt(amount), 0.999);
  let last = 0;
  let debias = 0;
  return points.map((p) => {
    last = last * weight + p.y;
    debias = debias * weight + 1;
    return { ...p, y: last / debias };
  });
}

function _runPageChartOptions(xTitle, yScale, isBar = false) {
  const cs = getComputedStyle(document.documentElement);
  const textColor = cs.getPropertyValue('--text').trim();
  const mutedColor = cs.getPropertyValue('--muted').trim();
  const gridColor = cs.getPropertyValue('--border').trim();
  const surfaceColor = cs.getPropertyValue('--surface').trim();
  const hoverMode = isBar ? 'nearest' : (_runPageState.tooltipMode === 'nearest' ? 'nearest' : 'index');
  const hoverIntersect = !isBar && _runPageState.tooltipMode === 'nearest';
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    normalized: true,
    parsing: false,
    plugins: {
      legend: { display: false },
      tooltip: {
        mode: hoverMode,
        intersect: hoverIntersect,
        displayColors: false,
        backgroundColor: surfaceColor,
        titleColor: textColor,
        bodyColor: textColor,
        footerColor: mutedColor,
        borderColor: gridColor,
        borderWidth: 1,
        cornerRadius: 8,
        caretSize: 5,
        padding: 10,
        titleMarginBottom: 6,
        bodySpacing: 4,
        footerMarginTop: 6,
        titleFont: { family: 'JetBrains Mono', size: 11, weight: '700' },
        bodyFont: { family: 'JetBrains Mono', size: 11, weight: '500' },
        footerFont: { family: 'JetBrains Mono', size: 10, weight: '400' },
        callbacks: {
          title(items) {
            const first = items && items[0];
            if (!first) return '';
            const x = first.parsed && first.parsed.x != null ? first.parsed.x : first.label;
            return xTitle ? `${xTitle}: ${_formatMetricValue(x)}` : String(x);
          },
          label(ctx) {
            const y = ctx.parsed && ctx.parsed.y != null ? _formatMetricValue(ctx.parsed.y) : '';
            return `${ctx.dataset.label}: ${y}`;
          },
          footer(items) {
            const n = (items || []).length;
            return n > 1 && _runPageState.tooltipMode === 'all' ? `${n} visible traces` : '';
          },
        },
      },
    },
    transitions: {
      active: { animation: { duration: 0 } },
      resize: { animation: { duration: 0 } },
      show: { animation: { duration: 0 } },
      hide: { animation: { duration: 0 } },
    },
    elements: {
      line: { tension: 0, borderJoinStyle: 'round' },
      point: { radius: 0, hoverRadius: 6, hitRadius: 10, hoverBorderWidth: 2 },
      bar: { borderRadius: 2 },
    },
    scales: {
      x: {
        type: isBar ? 'category' : 'linear',
        title: { display: !!xTitle, text: xTitle, color: mutedColor },
        ticks: { color: mutedColor, maxTicksLimit: isBar ? 8 : 9, autoSkip: true },
        grid: { color: gridColor, drawTicks: false },
        border: { color: gridColor },
      },
      y: {
        type: yScale || 'linear',
        ticks: { color: mutedColor, maxTicksLimit: 7 },
        grid: { color: gridColor, drawTicks: false },
        border: { color: gridColor },
      },
    },
    interaction: { mode: hoverMode, intersect: hoverIntersect },
    color: textColor,
  };
}

function _runPageDestroyCharts() {
  if (_runPageChartRetryTimer) {
    clearTimeout(_runPageChartRetryTimer);
    _runPageChartRetryTimer = null;
  }
  (_runPageState.charts || []).forEach(c => {
    try { c.destroy(); } catch (_) {}
  });
  _runPageState.charts = [];
}

function _runPageInspectorHtml() {
  const selected = _runPageState.selectedSeries;
  const detail = selected.length === 1 ? _runPageMetricDetailHtml(selected[0]) : '';
  const seriesLegend = _runPageLegendHtml('Legends', _runPageLegendItemsFromDatasets(_runPageBuildSeriesDatasets()));
  const scalarLegend = _runPageLegendHtml('Scalar Legends', _runPageBuildScalarDatasetInfo().legendItems, true);
  return `${seriesLegend}
  ${scalarLegend}
  ${_runPageModifiersHtml()}
  <div class="run-page-card">
    <div class="run-page-card-title">Inspector</div>
    <div class="run-page-selected-list">${selected.length ? selected.map(k => `<span>${_escHtml(k)}</span>`).join('') : '<em>No series selected</em>'}</div>
    ${detail}
  </div>
  ${_runPageSummarySidebarHtml()}`;
}

function _runPageSummarySidebarHtml() {
  const run = _runPageState.run || {};
  const jobs = run.jobs || [];
  const earliest = _earliestTime(jobs, 'started');
  const latest = _latestTime(jobs, 'ended_at');
  const duration = earliest && latest ? _formatDuration(earliest, latest) : '—';
  const stateSummary = _computeJobStateSummary(jobs, run.gpus_per_node) || 'no jobs';
  const rows = [
    ['Started', _fmtRunTime(earliest)],
    ['Duration', duration],
    ['Project', run.project || '—'],
    ['Jobs', String(jobs.length)],
    ['Status', stateSummary],
  ];
  return `<div class="run-page-card run-page-sidebar-summary">
    <div class="run-page-card-title">Run Summary</div>
    ${rows.map(([label, value]) => `
      <div class="run-page-sidebar-summary-row">
        <span>${_escHtml(label)}</span>
        <b>${_escHtml(value)}</b>
      </div>
    `).join('')}
  </div>`;
}

function _runPageModifiersHtml() {
  const contextOptions = _runPageContextKeys().map(key => `<option value="${escAttr(key)}"${_runPageState.groupByContext === key ? ' selected' : ''}>${_escHtml(key)}</option>`).join('');
  return `<div class="run-page-card run-page-modifiers-card">
    <div class="run-page-card-title">Metrics Modifiers</div>
    <label>Group by
      <select onchange="_runPageSetGroupByContext(this.value)">
        <option value="">None</option>
        ${contextOptions}
      </select>
    </label>
    <label>X-axis
      <select onchange="_runPageSetXAxis(this.value)">
        <option value="step"${_runPageState.xAxis === 'step' ? ' selected' : ''}>step</option>
        <option value="wall_time"${_runPageState.xAxis === 'wall_time' ? ' selected' : ''}>wall time</option>
        <option value="index"${_runPageState.xAxis === 'index' ? ' selected' : ''}>index</option>
      </select>
    </label>
    <label>Y scale
      <select onchange="_runPageSetYScale(this.value)">
        <option value="linear"${_runPageState.yScale === 'linear' ? ' selected' : ''}>linear</option>
        <option value="logarithmic"${_runPageState.yScale === 'logarithmic' ? ' selected' : ''}>log</option>
      </select>
    </label>
    <label>Hover
      <select onchange="_runPageSetTooltipMode(this.value)">
        <option value="all"${_runPageState.tooltipMode === 'all' ? ' selected' : ''}>all lines at step</option>
        <option value="nearest"${_runPageState.tooltipMode === 'nearest' ? ' selected' : ''}>exact line only</option>
      </select>
    </label>
    <label class="run-page-smoothing-control">
      <span>Smoothing <b id="run-page-smoothing-value">${_runPageSmoothingLabel(_runPageState.smoothing)}</b></span>
      <input type="range" min="0" max="0.95" step="0.05" value="${_runPageState.smoothing}"
             oninput="_runPagePreviewSmoothingValue(this.value)"
             onchange="_runPageSetSmoothing(this.value)"
             onpointerup="_runPageSetSmoothing(this.value)"
             onkeyup="if(event.key==='Enter'||event.key===' ')_runPageSetSmoothing(this.value)">
    </label>
    <label class="run-page-switch"><input type="checkbox" ${_runPageState.showRaw ? 'checked' : ''} onchange="_runPageSetShowRaw(this.checked)"> Show original</label>
    <label class="run-page-switch"><input type="checkbox" ${_runPageState.ignoreOutliers ? 'checked' : ''} onchange="_runPageSetIgnoreOutliers(this.checked)"> Ignore outliers</label>
  </div>`;
}

function _runPageMetricDetailHtml(key) {
  const pts = _runPageState.metrics.series[key] || [];
  const stats = _runPageStats(pts, true);
  const contexts = _runPageMetricContexts(pts).slice(0, 12).map(c => `<span>${_escHtml(c)}</span>`).join('');
  return `<div class="run-page-detail">
    <div><span>latest</span><b>${_escHtml(_formatMetricValue(stats.latest))}</b></div>
    <div><span>min</span><b>${_escHtml(_formatMetricValue(stats.min))}</b></div>
    <div><span>max</span><b>${_escHtml(_formatMetricValue(stats.max))}</b></div>
    <div><span>points</span><b>${pts.length}</b></div>
    ${contexts ? `<div class="run-page-contexts">${contexts}</div>` : ''}
  </div>`;
}

function _runPageStats(points, stepped) {
  const nums = (points || []).map(p => p.value_num).filter(Number.isFinite);
  const last = (points || [])[Math.max(0, (points || []).length - 1)] || {};
  return {
    latest: last.value,
    numericCount: nums.length,
    min: nums.length ? Math.min(...nums) : null,
    max: nums.length ? Math.max(...nums) : null,
    avg: nums.length ? nums.reduce((a, b) => a + b, 0) / nums.length : null,
    lastStep: stepped ? last.step : null,
  };
}

function _runPageSelectedStatCardsHtml() {
  const cards = [];
  for (const key of _runPageState.selectedSeries) {
    const stats = _runPageStats(_runPageState.metrics.series[key] || [], true);
    cards.push(_runPageStatCard(key, stats, 'series'));
  }
  for (const key of _runPageState.selectedScalars) {
    const stats = _runPageStats(_runPageState.metrics.scalars[key] || [], false);
    cards.push(_runPageStatCard(key, stats, 'scalar'));
  }
  return cards.length ? cards.join('') : '<div class="run-page-empty-mini">Select metrics to see summary cards.</div>';
}

function _runPageContextTableHtml() {
  const selected = new Set([..._runPageState.selectedSeries, ..._runPageState.selectedScalars]);
  const records = _runPageMetricRecords().filter(record => selected.has(record.key));
  const rows = [];
  for (const record of records) {
    const counts = {};
    for (const point of record.points || []) {
      Object.entries(point.context || {}).forEach(([key, value]) => {
        const id = `${key}\u0000${String(value)}`;
        if (!counts[id]) counts[id] = { key, value, count: 0 };
        counts[id].count += 1;
      });
    }
    const entries = Object.values(counts);
    if (!entries.length) {
      rows.push(`<tr><td>${_escHtml(record.key)}</td><td>${_escHtml(record.kind)}</td><td colspan="3" class="dim">No context</td></tr>`);
    } else {
      entries.sort((a, b) => a.key.localeCompare(b.key) || String(a.value).localeCompare(String(b.value))).forEach((item) => {
        rows.push(`<tr>
          <td>${_escHtml(record.key)}</td>
          <td>${_escHtml(record.kind)}</td>
          <td>${_escHtml(item.key)}</td>
          <td>${_escHtml(String(item.value))}</td>
          <td class="dim">${item.count}</td>
        </tr>`);
      });
    }
  }
  return `<div class="run-page-card">
    <div class="run-page-card-title-row">
      <div>
        <div class="run-page-card-title">Context Table</div>
        <div class="run-page-card-sub">Aim-style context breakdown for selected metrics.</div>
      </div>
    </div>
    ${rows.length ? `<table class="run-page-context-table">
      <thead><tr><th>Metric</th><th>Type</th><th>Context key</th><th>Context value</th><th>Points</th></tr></thead>
      <tbody>${rows.join('')}</tbody>
    </table>` : '<div class="run-page-empty-mini">Select metrics to inspect context.</div>'}
  </div>`;
}

function _runPageStatCard(key, stats, kind) {
  const latest = stats.latest == null ? '—' : _formatMetricValue(stats.latest);
  const range = stats.min == null ? '' : `<span>${_escHtml(_formatMetricValue(stats.min))} → ${_escHtml(_formatMetricValue(stats.max))}</span>`;
  return `<div class="run-page-stat-card">
    <span>${_escHtml(kind)}</span>
    <b title="${escAttr(key)}">${_escHtml(key)}</b>
    <strong>${_escHtml(latest)}</strong>
    ${range}
  </div>`;
}

// Build a single "Latest Scalars" widget that fans out by (name × context).
// The server's `scalar_latest` map is keyed by metric name only (last write
// wins), so a metric logged under many contexts (e.g. `accuracy` across 72
// benchmark/eval_mode/scorer tuples) is reduced to one arbitrary bucket —
// which is how `accuracy = 0.0` could appear here while ~71 other accuracy
// buckets had meaningful non-zero values. We rebuild the per-bucket view
// from the full `scalars` payload so every distinct context shows up.
function _runPageLatestScalarBlock() {
  const scalars = _runPageState.metrics.scalars || {};
  const buckets = [];

  Object.entries(scalars).forEach(([name, pts]) => {
    if (!Array.isArray(pts) || !pts.length) return;
    const byCtx = new Map();
    pts.forEach(p => {
      if (!p || typeof p !== 'object') return;
      const ctx = (p.context && typeof p.context === 'object') ? p.context : {};
      const ck = _runPageCtxKey(ctx);
      const ts = Number(p.ts || 0);
      const prev = byCtx.get(ck);
      if (!prev || ts >= prev._ts) byCtx.set(ck, { value: p.value, context: ctx, _ts: ts });
    });
    byCtx.forEach((p, ck) => buckets.push({ name, ctx: p.context, ctxKey: ck, value: p.value }));
  });

  // Fallback for older runs that have scalar_latest but no scalars[] payload.
  if (!buckets.length) {
    const latest = _runPageState.metrics.scalar_latest || {};
    Object.entries(latest).forEach(([name, p]) => {
      if (!p || typeof p !== 'object') return;
      const ctx = (p.context && typeof p.context === 'object') ? p.context : {};
      buckets.push({ name, ctx, ctxKey: _runPageCtxKey(ctx), value: p.value });
    });
  }
  if (!buckets.length) return '';

  buckets.sort((a, b) => a.name.localeCompare(b.name) || a.ctxKey.localeCompare(b.ctxKey));

  const LIMIT = 120;
  const shown = buckets.slice(0, LIMIT);
  const extra = Math.max(0, buckets.length - LIMIT);

  const rows = shown.map(b => `
    <div class="run-page-scalar-row">
      <div class="run-page-scalar-key">
        <span class="run-page-scalar-name">${_escHtml(b.name)}</span>
        ${_runPageCtxChipHtml(b.ctx)}
      </div>
      <div class="run-page-scalar-val">${_escHtml(_formatMetricValue(b.value))}</div>
    </div>`).join('');

  const tail = extra ? `<div class="run-page-muted">+ ${extra} more</div>` : '';

  return `<div class="run-params">
    <div class="run-params-head">Latest Scalars (${buckets.length})</div>
    <div class="run-page-scalar-list">${rows}</div>
    ${tail}
  </div>`;
}

function _runPageCtxKey(ctx) {
  const keys = Object.keys(ctx || {}).sort();
  return keys.map(k => `${k}=${_runPageCtxStringify(ctx[k])}`).join('|');
}

function _runPageCtxStringify(v) {
  if (v == null) return '';
  if (typeof v === 'string') return v;
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  try { return JSON.stringify(v); } catch (_) { return String(v); }
}

function _runPageCtxChipHtml(ctx) {
  const keys = Object.keys(ctx || {}).sort();
  if (!keys.length) return '';
  const MAX = 4;
  const parts = keys.slice(0, MAX).map(k => {
    const val = _runPageCtxStringify(ctx[k]);
    return `<span class="run-page-ctx-kv"><span class="run-page-ctx-k">${_escHtml(k)}</span>=<span class="run-page-ctx-v">${_escHtml(val)}</span></span>`;
  });
  const more = keys.length > MAX ? `<span class="run-page-ctx-more">+${keys.length - MAX}</span>` : '';
  return `<span class="run-page-ctx-chip" title="${escAttr(keys.map(k => `${k}=${_runPageCtxStringify(ctx[k])}`).join(', '))}">${parts.join('')}${more}</span>`;
}

function _runPageKeyValueBlock(title, obj, limit) {
  const entries = Object.entries(obj || {}).sort((a, b) => a[0].localeCompare(b[0]));
  const shown = limit ? entries.slice(0, limit) : entries;
  if (!shown.length) return '';
  const rows = shown.map(([k, v]) => `
    <div class="run-params-row">
      <span class="run-params-label">${_escHtml(k)}</span>
      <span class="run-params-value">${_escHtml(_formatMetricValue(v))}</span>
    </div>`).join('');
  const extra = limit && entries.length > limit ? `<div class="run-page-muted">+ ${entries.length - limit} more</div>` : '';
  return `<div class="run-params">
    <div class="run-params-head">${_escHtml(title)}</div>
    <div class="run-params-grid">${rows}</div>
    ${extra}
  </div>`;
}

function _runPageJobsHtml() {
  const jobs = (_runPageState.run && _runPageState.run.jobs) || [];
  if (!jobs.length) return '<div class="run-page-empty">No jobs attached to this run.</div>';
  const rows = jobs.map(j => {
    const name = j.job_name || j.name || '';
    const safeName = name.replace(/'/g, "\\'");
    const state = (j.state || '').toUpperCase();
    return `<tr>
      <td class="dim">${_escHtml(j.job_id || j.jobid || '—')}</td>
      <td class="bold">${_escHtml(name || '—')}</td>
      <td><span class="state-chip ${stateClass(state, j.reason)}">${_escHtml(j.state || '—')}</span></td>
      <td class="dim">${fmtTime(j.started_local || j.started || j.submitted)}</td>
      <td class="dim">${fmtTime(j.ended_local || j.ended_at)}</td>
      <td class="dim">${_escHtml(j.elapsed || '—')}</td>
      <td><button class="action-btn log-btn" onclick="openLog('${escAttr(_runPageState.cluster)}','${escAttr(j.job_id || j.jobid)}','${escAttr(safeName)}')">log</button> <button class="action-btn log-btn" onclick="openStats('${escAttr(_runPageState.cluster)}','${escAttr(j.job_id || j.jobid)}','${escAttr(safeName)}')">stats</button></td>
    </tr>`;
  }).join('');
  return `<div class="run-page-card">
    <div class="run-page-card-title">Jobs in Run</div>
    <table class="run-jobs-table">
      <thead><tr><th>ID</th><th>Name</th><th>State</th><th>Start</th><th>End</th><th>Elapsed</th><th>Actions</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}


async function _runPageLoadCustomMetrics() {
  const run = _runPageState.run;
  if (!run || !run.root_job_id || document.hidden) return;
  if (typeof isCustomMetricsEnabled === 'function' && !isCustomMetricsEnabled()) return;
  try {
    const res = await fetch(`/api/custom_metrics_run/${encodeURIComponent(_runPageState.cluster)}/${encodeURIComponent(run.root_job_id)}`);
    const d = await res.json();
    if (d.status === 'ok') {
      _runPageState.customMetrics = d;
      const el = document.getElementById('run-page-custom-metrics');
      if (el) el.innerHTML = _runPageCustomMetricsHtml();
    }
  } catch (e) {
    console.error('Failed to load run page custom metrics', e);
  }
}

function _runPageCustomMetricsHtml() {
  if (typeof isCustomMetricsEnabled === 'function' && !isCustomMetricsEnabled()) return '';
  const d = _runPageState.customMetrics;
  if (!d || !d.aggregates || !d.aggregates.length) return '';
  const rows = d.aggregates.map(agg => `
    <div class="run-params-row">
      <span class="run-params-label">${_escHtml(agg.name)}</span>
      <span class="run-params-value">avg ${_escHtml(String(agg.avg))} · min ${_escHtml(String(agg.min))} · max ${_escHtml(String(agg.max))}</span>
    </div>`).join('');
  return `<div class="run-params">
    <div class="run-params-head">Custom Metrics</div>
    <div class="run-params-grid">${rows}</div>
  </div>`;
}

function _runPageOnNoteInput() {
  if (_runPageState.noteTimer) clearTimeout(_runPageState.noteTimer);
  _runPageState.noteTimer = setTimeout(_runPageSaveNotes, 1500);
}

async function _runPageDelete() {
  const run = _runPageState.run;
  if (!run || !run.id) return;
  const label = run.run_name || run.name || _runPageState.runHash || `run ${run.id}`;
  const msg = `Permanently delete "${label}"?\n\nThis erases:\n  • the run row (params, metadata, batch script, env, conda)\n  • every SDK metric, scalar, event, and alias for this run\n\nLinked Slurm jobs stay in history (just unlinked).\nThis cannot be undone.`;
  if (!window.confirm(msg)) return;
  try {
    const res = await fetch(`/api/run/${run.id}`, { method: 'DELETE' });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.status === 'error') {
      throw new Error(data.error || `HTTP ${res.status}`);
    }
    const c = data.counts || {};
    if (typeof toast === 'function') {
      toast(`Deleted run · metrics:${c.run_metrics || 0} scalars:${c.run_scalars || 0} events:${c.sdk_events || 0}`, 'ok');
    }
    if (typeof showTab === 'function') showTab('history');
    if (typeof loadHistory === 'function') loadHistory();
  } catch (e) {
    if (typeof toast === 'function') toast(`Delete failed: ${e.message}`, 'error');
    else alert(`Delete failed: ${e.message}`);
  }
}

async function _runPageToggleMalfunctioned(checked) {
  const run = _runPageState.run;
  const cb = document.querySelector('.run-page-malfunction-cb');
  if (!run || !run.id) return;
  try {
    const res = await fetch(`/api/run/${run.id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ malfunctioned: !!checked }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.status === 'error') {
      throw new Error(data.error || 'failed');
    }
    run.malfunctioned = !!checked;
  } catch (_) {
    if (cb) cb.checked = !checked;
  }
}

async function _runPageSaveNotes() {
  if (_runPageState.noteTimer) {
    clearTimeout(_runPageState.noteTimer);
    _runPageState.noteTimer = null;
  }
  const run = _runPageState.run;
  const ta = document.querySelector('.run-page-notes');
  if (!run || !run.id || !ta) return;
  try {
    await fetch(`/api/run/${run.id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ notes: ta.value }),
    });
    run.notes = ta.value;
    const badge = document.getElementById('run-page-notes-saved');
    if (badge) {
      badge.classList.add('show');
      setTimeout(() => badge.classList.remove('show'), 1200);
    }
  } catch (_) {}
}
