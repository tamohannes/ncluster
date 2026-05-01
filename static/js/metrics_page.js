let _metricsPageState = {
  runs: [],
  runData: {},
  records: [],
  selectedMetrics: [],
  chartsConfig: [],
  query: '',
  runInput: '',
  xAxis: 'step',
  groupByContext: '',
  smoothing: 0.25,
  showRaw: false,
  yScale: 'linear',
  ignoreOutliers: false,
  tooltipMode: 'all',
  hiddenTraces: {},
  savedViews: [],
  activeViewId: null,
  suggestions: [],
  suggestionTarget: '',
  suggestionIndex: 0,
  charts: [],
  loadSeq: 0,
};
let _metricsPageChartRetryTimer = null;

function openMetricsPage(fromTab = false) {
  if (typeof _activateView === 'function') _activateView('metrics');
  _metricsPageReadUrlState();
  if (!fromTab && typeof _appTabs !== 'undefined') {
    const at = _appTabs.find(t => t.id === _activeTabId);
    if (at) {
      at.type = 'metrics';
      at.label = 'Metrics';
      at.project = null;
    }
    if (typeof _renderAppTabs === 'function') _renderAppTabs();
    if (typeof _persistTabs === 'function') _persistTabs();
    if (typeof _setHash === 'function') _setHash(`#/metrics${_metricsPageCurrentQuery()}`);
  }
  _metricsPageRenderShell();
  _metricsPageLoadSavedViews();
  if (_metricsPageState.activeViewId) {
    _metricsPageOpenSavedView(_metricsPageState.activeViewId);
    return;
  }
  if (_metricsPageState.runs.length) _metricsPageLoadRuns();
}

function _metricsPageCurrentQuery() {
  const params = new URLSearchParams();
  if (_metricsPageState.runs.length) params.set('runs', _metricsPageState.runs.map(r => `${r.cluster}/${r.runHash}`).join(','));
  if (_metricsPageState.selectedMetrics.length) params.set('metrics', _metricsPageState.selectedMetrics.map(encodeURIComponent).join(','));
  if (_metricsPageState.query) params.set('q', _metricsPageState.query);
  if (_metricsPageState.xAxis !== 'step') params.set('x', _metricsPageState.xAxis);
  if (_metricsPageState.groupByContext) params.set('group', _metricsPageState.groupByContext);
  if (_metricsPageState.yScale !== 'linear') params.set('yscale', _metricsPageState.yScale);
  if (_metricsPageState.smoothing !== 0.25) params.set('smooth', String(_metricsPageState.smoothing));
  if (_metricsPageState.showRaw) params.set('raw', '1');
  if (_metricsPageState.ignoreOutliers) params.set('outliers', '0');
  if (_metricsPageState.tooltipMode !== 'all') params.set('hover', _metricsPageState.tooltipMode);
  if (_metricsPageState.chartsConfig.length) params.set('charts', btoa(unescape(encodeURIComponent(JSON.stringify(_metricsPageState.chartsConfig)))));
  if (_metricsPageState.activeViewId) params.set('view', String(_metricsPageState.activeViewId));
  const text = params.toString();
  return text ? `?${text}` : '';
}

function _metricsPageReplaceUrlState() {
  const next = `#/metrics${_metricsPageCurrentQuery()}`;
  if (location.hash !== next) history.replaceState(null, '', next);
}

function _metricsPageReadUrlState() {
  const raw = location.hash.split('?')[1] || '';
  const params = new URLSearchParams(raw);
  _metricsPageState.runs = parseMetricsRunRefs(params.get('runs') || '');
  _metricsPageState.selectedMetrics = (params.get('metrics') || '')
    .split(',')
    .map(decodeURIComponent)
    .map(s => s.trim())
    .filter(Boolean);
  _metricsPageState.query = params.get('q') || '';
  _metricsPageState.xAxis = params.get('x') || 'step';
  _metricsPageState.groupByContext = params.get('group') || '';
  _metricsPageState.yScale = params.get('yscale') || 'linear';
  _metricsPageState.smoothing = Math.max(0, Math.min(0.95, parseFloat(params.get('smooth') || '0.25') || 0));
  _metricsPageState.showRaw = params.get('raw') === '1';
  _metricsPageState.ignoreOutliers = params.get('outliers') === '0';
  _metricsPageState.tooltipMode = params.get('hover') === 'nearest' ? 'nearest' : 'all';
  _metricsPageState.activeViewId = params.get('view') ? parseInt(params.get('view'), 10) : null;
  if (params.get('charts')) {
    try {
      const parsed = JSON.parse(decodeURIComponent(escape(atob(params.get('charts')))));
      _metricsPageState.chartsConfig = Array.isArray(parsed) ? parsed : [];
    } catch (_) {
      _metricsPageState.chartsConfig = [];
    }
  } else {
    _metricsPageState.chartsConfig = [];
  }
}

function parseMetricsRunRefs(text) {
  const seen = new Set();
  return String(text || '')
    .split(/[,\n\s]+/)
    .map(s => s.trim())
    .filter(Boolean)
    .map((ref) => {
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

function _metricsPageRunKey(run) {
  return `${run.cluster}/${run.runHash}`;
}

function _metricsPageRunPayload(runRef) {
  const key = typeof runRef === 'string' ? runRef : _metricsPageRunKey(runRef);
  return _metricsPageState.runData[key] || null;
}

function _metricsPageRunName(runRef) {
  const payload = _metricsPageRunPayload(runRef);
  if (payload && payload.info) return payload.info.run_name || payload.info.name || payload.run.runHash;
  return typeof runRef === 'string' ? runRef.split('/').pop() : runRef.runHash;
}

function _metricsPageRunIdentityHtml(runRef, opts = {}) {
  const key = typeof runRef === 'string' ? runRef : _metricsPageRunKey(runRef);
  const [cluster, runHash] = key.split('/');
  const name = _metricsPageRunName(runRef);
  const removable = opts.remove ? `<button type="button" onclick="${opts.remove}('${escAttr(key)}')" title="Remove">×</button>` : '';
  return `<span class="metrics-run-ident">
    <span class="metrics-run-name" title="${escAttr(name)}">${_escHtml(name)}</span>
    <span class="metrics-run-meta">${_escHtml(runHash || key)}${cluster ? ` · ${_escHtml(cluster)}` : ''}</span>
    ${removable}
  </span>`;
}

function _metricsPageRenderShell() {
  const el = document.getElementById('metrics-page');
  if (!el) return;
  const runChips = _metricsPageState.runs.length
    ? _metricsPageState.runs.map(run => `
      ${_metricsPageRunIdentityHtml(run, { remove: '_metricsPageRemoveRun' })}`).join('')
    : '<span class="metrics-page-muted">No runs selected.</span>';
  el.innerHTML = `<div class="metrics-page-head">
    <div>
      <div class="metrics-page-kicker">multi-run metrics explorer</div>
      <div class="metrics-page-title">Metrics</div>
    </div>
    <div class="metrics-page-head-actions">
      <button class="btn" onclick="_metricsPageAddChart()">+ chart</button>
      <button class="btn" onclick="_metricsPageSaveView()">save view</button>
      <button class="btn" onclick="_metricsPageLoadRuns()">↻ refresh</button>
    </div>
  </div>
  <div class="metrics-page-run-select metrics-page-card">
    <div class="metrics-page-card-title-row">
      <div>
        <div class="metrics-page-card-title">Runs Select</div>
        <div class="metrics-page-card-sub">Enter refs as <code>cluster/run_hash</code>, separated by commas or new lines.</div>
      </div>
      <button class="btn" onclick="_metricsPageAddRunsFromInput()">add runs</button>
    </div>
    <div class="metrics-page-input-wrap">
      <textarea class="metrics-page-run-input" id="metrics-page-run-input" placeholder="aws-cmh/86398daa"
        oninput="_metricsPageOnRunInput(this.value)"
        onkeydown="_metricsPageSuggestionKey(event, 'run')">${_escHtml(_metricsPageState.runInput || '')}</textarea>
      <div class="metrics-page-suggest" id="metrics-page-run-suggest"></div>
    </div>
    <div class="metrics-page-run-chips">${runChips}</div>
  </div>
  ${_metricsPageSavedViewsHtml()}
  <div class="metrics-page-body" id="metrics-page-body">
    ${_metricsPageState.runs.length ? '<div class="metrics-page-empty">Loading selected runs…</div>' : '<div class="metrics-page-empty">Add at least one run to compare metrics.</div>'}
  </div>`;
}

function _metricsPageSavedViewsHtml() {
  const views = _metricsPageState.savedViews || [];
  if (!views.length) return '';
  return `<div class="metrics-page-saved metrics-page-card">
    <div class="metrics-page-card-title-row">
      <div>
        <div class="metrics-page-card-title">Saved Views</div>
        <div class="metrics-page-card-sub">Click a bookmark to restore its exact workspace.</div>
      </div>
    </div>
    <div class="metrics-page-saved-list">
      ${views.map(view => `
        <button type="button" class="metrics-page-saved-item${_metricsPageState.activeViewId === view.id ? ' active' : ''}" onclick="_metricsPageOpenSavedView(${view.id})">
          <b>${_escHtml(view.title || 'Untitled metrics view')}</b>
          <span>${_escHtml((view.updated_at || '').replace('T', ' ').slice(0, 16))}</span>
        </button>
        <button type="button" class="metrics-page-saved-delete" onclick="event.stopPropagation();_metricsPageDeleteSavedView(${view.id})">delete</button>
      `).join('')}
    </div>
  </div>`;
}

async function _metricsPageLoadSavedViews() {
  try {
    const res = await fetch('/api/metrics_views');
    const data = await res.json();
    if (data.status === 'ok') {
      _metricsPageState.savedViews = data.views || [];
      if (document.querySelector('.metrics-page-layout')) _metricsPageRenderExplorer();
      else _metricsPageRenderShell();
    }
  } catch (_) {}
}

function _metricsPageRecentRunSuggestions(query = '') {
  const q = String(query || '').replace(/^@/, '').toLowerCase();
  const counts = {};
  const add = (run, source, weight = 1) => {
    if (!run || !run.runHash) return;
    const key = `${run.cluster || ''}/${run.runHash}`;
    const payload = _metricsPageRunPayload(run);
    const runName = run.runName || (payload && payload.info && (payload.info.run_name || payload.info.name)) || run.runHash;
    const hay = `${runName} ${run.runHash} ${run.cluster || ''}`.toLowerCase();
    if (q && !hay.includes(q)) return;
    if (!counts[key]) counts[key] = { ...run, runName, score: 0, sources: new Set() };
    counts[key].score += weight;
    counts[key].sources.add(source);
  };
  _metricsPageState.runs.forEach(run => add(run, 'current', 20));
  Object.values(_metricsPageState.runData || {}).forEach(payload => add({ ...payload.run, runName: payload.info && payload.info.run_name }, 'loaded', 12));
  (_metricsPageState.savedViews || []).forEach(view => {
    const state = view.state || {};
    (state.runs || []).forEach(run => add(run, view.title || 'saved', 3 + (view.pinned ? 4 : 0)));
  });
  return Object.values(counts)
    .sort((a, b) => b.score - a.score || (a.runName || '').localeCompare(b.runName || ''))
    .slice(0, 8);
}

async function _metricsPageFetchRunSuggestions(query) {
  const local = _metricsPageRecentRunSuggestions(query);
  try {
    if (!query || query.length < 2) return local;
    const res = await fetch(`/api/spotlight?q=${encodeURIComponent(query.replace(/^@/, ''))}`);
    const data = await res.json();
    const seen = new Set(local.map(r => `${r.cluster || ''}/${r.runHash}`.toLowerCase()));
    const remote = (data.runs || []).map(r => ({
      cluster: r.cluster,
      runHash: r.run_hash,
      runName: r.run_name || `Run ${r.run_hash}`,
      score: 1,
      sources: new Set(['search']),
    })).filter(r => {
      const key = `${r.cluster || ''}/${r.runHash}`.toLowerCase();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
    return [...local, ...remote].slice(0, 10);
  } catch (_) {
    return local;
  }
}

function _metricsPageRenderSuggestions(target, runs) {
  const el = document.getElementById(target === 'query' ? 'metrics-page-query-suggest' : 'metrics-page-run-suggest');
  if (!el) return;
  _metricsPageState.suggestions = runs || [];
  _metricsPageState.suggestionTarget = target;
  _metricsPageState.suggestionIndex = 0;
  if (!runs || !runs.length) {
    el.classList.remove('open');
    el.innerHTML = '';
    return;
  }
  el.classList.add('open');
  el.innerHTML = runs.map((run, idx) => {
    const sources = Array.from(run.sources || []).join(', ');
    return `<button type="button" class="metrics-page-suggest-item${idx === 0 ? ' active' : ''}" onclick="_metricsPageApplySuggestion(${idx})">
      <span class="metrics-page-suggest-name">${_escHtml(run.runName || run.runHash)}</span>
      <span class="metrics-page-suggest-meta">${_escHtml(run.runHash)}${run.cluster ? ` · ${_escHtml(run.cluster)}` : ''}${sources ? ` · ${_escHtml(sources)}` : ''}</span>
    </button>`;
  }).join('');
}

function _metricsPageUpdateSuggestionActive() {
  document.querySelectorAll('.metrics-page-suggest-item').forEach((item, idx) => {
    item.classList.toggle('active', idx === _metricsPageState.suggestionIndex);
  });
}

function _metricsPageCloseSuggestions() {
  document.querySelectorAll('.metrics-page-suggest').forEach(el => {
    el.classList.remove('open');
    el.innerHTML = '';
  });
  _metricsPageState.suggestions = [];
  _metricsPageState.suggestionTarget = '';
}

async function _metricsPageOnRunInput(value) {
  _metricsPageState.runInput = value || '';
  const token = _metricsPageLastToken(value);
  const suggestions = await _metricsPageFetchRunSuggestions(token);
  _metricsPageRenderSuggestions('run', suggestions);
}

async function _metricsPageOnQueryInput(value) {
  _metricsPageState.query = value || '';
  const token = _metricsPageAtToken(value);
  if (!token) {
    _metricsPageRenderSuggestions('query', []);
    return;
  }
  const suggestions = await _metricsPageFetchRunSuggestions(token);
  _metricsPageRenderSuggestions('query', suggestions);
}

function _metricsPageLastToken(value) {
  const parts = String(value || '').split(/[,\n\s]+/).filter(Boolean);
  return parts[parts.length - 1] || '';
}

function _metricsPageAtToken(value) {
  const before = String(value || '');
  const m = before.match(/@([A-Za-z0-9_-]*)$/);
  return m ? `@${m[1]}` : '';
}

function _metricsPageSuggestionKey(event, target) {
  const suggestions = _metricsPageState.suggestions || [];
  if (!suggestions.length) {
    if (event.key === 'Enter' && target === 'run' && (event.metaKey || event.ctrlKey)) {
      event.preventDefault();
      _metricsPageAddRunsFromInput();
    }
    return;
  }
  if (event.key === 'ArrowDown') {
    event.preventDefault();
    _metricsPageState.suggestionIndex = Math.min(suggestions.length - 1, _metricsPageState.suggestionIndex + 1);
    _metricsPageUpdateSuggestionActive();
  } else if (event.key === 'ArrowUp') {
    event.preventDefault();
    _metricsPageState.suggestionIndex = Math.max(0, _metricsPageState.suggestionIndex - 1);
    _metricsPageUpdateSuggestionActive();
  } else if (event.key === 'Enter' || event.key === 'Tab') {
    event.preventDefault();
    _metricsPageApplySuggestion(_metricsPageState.suggestionIndex);
  } else if (event.key === 'Escape') {
    _metricsPageCloseSuggestions();
  }
}

function _metricsPageQueryKey(event, value) {
  if ((event.key === 'Enter') && !(_metricsPageState.suggestions || []).length) {
    _metricsPageSetQuery(value);
    return;
  }
  _metricsPageSuggestionKey(event, 'query');
}

function _metricsPageApplySuggestion(idx) {
  const run = (_metricsPageState.suggestions || [])[idx];
  if (!run) return;
  if (_metricsPageState.suggestionTarget === 'query') {
    const input = document.querySelector('.metrics-page-query-input');
    const value = input ? input.value : _metricsPageState.query;
    const next = String(value || '').replace(/@([A-Za-z0-9_-]*)$/, `@${run.runHash}`);
    _metricsPageState.query = next;
    if (input) input.value = next;
    _metricsPageCloseSuggestions();
  } else {
    const input = document.getElementById('metrics-page-run-input');
    const value = input ? input.value : _metricsPageState.runInput;
    const ref = run.cluster ? `${run.cluster}/${run.runHash}` : run.runHash;
    const parts = String(value || '').split(/([,\n\s]+)/);
    let replaced = false;
    for (let i = parts.length - 1; i >= 0; i--) {
      if (parts[i].trim()) {
        parts[i] = ref;
        replaced = true;
        break;
      }
    }
    const next = replaced ? parts.join('') : ref;
    _metricsPageState.runInput = next;
    if (input) input.value = next;
    _metricsPageCloseSuggestions();
  }
}

function _metricsPageSerializeState() {
  return {
    runs: _metricsPageState.runs,
    selectedMetrics: _metricsPageState.selectedMetrics,
    query: _metricsPageState.query,
    xAxis: _metricsPageState.xAxis,
    groupByContext: _metricsPageState.groupByContext,
    smoothing: _metricsPageState.smoothing,
    showRaw: _metricsPageState.showRaw,
    yScale: _metricsPageState.yScale,
    ignoreOutliers: _metricsPageState.ignoreOutliers,
    tooltipMode: _metricsPageState.tooltipMode,
    hiddenTraces: _metricsPageState.hiddenTraces,
    chartsConfig: _metricsPageState.chartsConfig,
  };
}

function _metricsPageApplySerializedState(state) {
  const s = state || {};
  _metricsPageState.runs = Array.isArray(s.runs) ? s.runs : [];
  _metricsPageState.selectedMetrics = Array.isArray(s.selectedMetrics) ? s.selectedMetrics : [];
  _metricsPageState.query = s.query || '';
  _metricsPageState.xAxis = s.xAxis || 'step';
  _metricsPageState.groupByContext = s.groupByContext || '';
  _metricsPageState.smoothing = Math.max(0, Math.min(0.95, parseFloat(s.smoothing || '0.25') || 0));
  _metricsPageState.showRaw = !!s.showRaw;
  _metricsPageState.yScale = s.yScale || 'linear';
  _metricsPageState.ignoreOutliers = !!s.ignoreOutliers;
  _metricsPageState.tooltipMode = s.tooltipMode === 'nearest' ? 'nearest' : 'all';
  _metricsPageState.hiddenTraces = s.hiddenTraces || {};
  _metricsPageState.chartsConfig = Array.isArray(s.chartsConfig) ? s.chartsConfig : [];
}

async function _metricsPageSaveView() {
  const title = prompt('Save metrics view as:', _metricsPageSuggestedViewTitle());
  if (!title) return;
  const body = { title, state: _metricsPageSerializeState() };
  const url = _metricsPageState.activeViewId ? `/api/metrics_views/${_metricsPageState.activeViewId}` : '/api/metrics_views';
  const method = _metricsPageState.activeViewId ? 'PATCH' : 'POST';
  const res = await fetch(url, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (data.status === 'ok') {
    _metricsPageState.activeViewId = data.view.id;
    toast('Metrics view saved');
    await _metricsPageLoadSavedViews();
  } else {
    toast(data.error || 'Failed to save metrics view', 'error');
  }
}

function _metricsPageSuggestedViewTitle() {
  const names = _metricsPageState.runs.map(r => _metricsPageRunName(r)).slice(0, 2).join(' vs ');
  return names || 'Metrics view';
}

async function _metricsPageOpenSavedView(viewId) {
  const res = await fetch(`/api/metrics_views/${viewId}`);
  const data = await res.json();
  if (data.status !== 'ok') {
    toast(data.error || 'Failed to load metrics view', 'error');
    return;
  }
  _metricsPageState.activeViewId = viewId;
  _metricsPageApplySerializedState(data.view.state || {});
  _metricsPageReplaceUrlState();
  _metricsPageRenderShell();
  if (_metricsPageState.runs.length) _metricsPageLoadRuns();
}

async function _metricsPageDeleteSavedView(viewId) {
  if (!confirm('Delete this saved metrics view?')) return;
  const res = await fetch(`/api/metrics_views/${viewId}`, { method: 'DELETE' });
  const data = await res.json();
  if (data.status === 'ok') {
    if (_metricsPageState.activeViewId === viewId) _metricsPageState.activeViewId = null;
    await _metricsPageLoadSavedViews();
    _metricsPageRenderShell();
  } else {
    toast(data.error || 'Failed to delete metrics view', 'error');
  }
}

function _metricsPageAddRunsFromInput() {
  const input = document.getElementById('metrics-page-run-input');
  const refs = parseMetricsRunRefs(input ? input.value : '');
  const byKey = {};
  _metricsPageState.runs.forEach(run => { byKey[_metricsPageRunKey(run).toLowerCase()] = run; });
  refs.forEach(run => { byKey[_metricsPageRunKey(run).toLowerCase()] = run; });
  _metricsPageState.runs = Object.values(byKey);
  _metricsPageState.runInput = '';
  _metricsPageReplaceUrlState();
  _metricsPageRenderShell();
  _metricsPageLoadRuns();
}

function _metricsPageRemoveRun(runKey) {
  _metricsPageState.runs = _metricsPageState.runs.filter(run => _metricsPageRunKey(run) !== runKey);
  delete _metricsPageState.runData[runKey];
  _metricsPageNormalizeRecords();
  _metricsPageReplaceUrlState();
  _metricsPageRenderShell();
  if (_metricsPageState.runs.length) _metricsPageRenderExplorer();
}

async function _metricsPageLoadRuns() {
  const body = document.getElementById('metrics-page-body');
  if (!body || !_metricsPageState.runs.length || document.hidden) return;
  const seq = ++_metricsPageState.loadSeq;
  body.innerHTML = '<div class="metrics-page-empty">Loading selected runs…</div>';
  try {
    await _metricsPageResolveRuns();
    const payloads = await Promise.all(_metricsPageState.runs.map(async (run) => {
      const [infoRes, metricsRes] = await Promise.all([
        fetch(`/api/run_info_by_hash/${encodeURIComponent(run.cluster)}/${encodeURIComponent(run.runHash)}`),
        fetch(`/api/run_metrics_by_hash/${encodeURIComponent(run.cluster)}/${encodeURIComponent(run.runHash)}`),
      ]);
      const info = await infoRes.json();
      const metrics = await metricsRes.json();
      if (info.status !== 'ok' || !info.run) throw new Error(`${_metricsPageRunKey(run)}: ${info.error || 'run not found'}`);
      if (metrics.status !== 'ok') throw new Error(`${_metricsPageRunKey(run)}: ${metrics.error || 'metrics not found'}`);
      return { run: { cluster: run.cluster, runHash: info.run.run_hash || run.runHash }, info: info.run, metrics: _metricsPageNormalizeMetrics(metrics) };
    }));
    if (seq !== _metricsPageState.loadSeq) return;
    _metricsPageState.runData = {};
    _metricsPageState.runs = payloads.map(payload => payload.run);
    payloads.forEach(payload => { _metricsPageState.runData[_metricsPageRunKey(payload.run)] = payload; });
    _metricsPageNormalizeRecords();
    _metricsPageApplyDefaultSelection();
    _metricsPageEnsureChartPanels();
    _metricsPageRenderExplorer();
  } catch (e) {
    if (seq !== _metricsPageState.loadSeq) return;
    body.innerHTML = `<div class="metrics-page-empty">${_escHtml(e.message || e)}</div>`;
  }
}

async function _metricsPageResolveRuns() {
  const resolved = [];
  for (const run of _metricsPageState.runs) {
    if (run.cluster) {
      resolved.push(run);
      continue;
    }
    const res = await fetch(`/api/resolve_run_hash/${encodeURIComponent(run.runHash)}`);
    const data = await res.json();
    if (data.status !== 'ok') throw new Error(data.error || `Could not resolve ${run.runHash}`);
    resolved.push({ cluster: data.cluster, runHash: data.run_hash || run.runHash });
  }
  const seen = new Set();
  _metricsPageState.runs = resolved.filter(run => {
    const key = _metricsPageRunKey(run).toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  _metricsPageReplaceUrlState();
}

function _metricsPageNormalizeMetrics(payload) {
  return {
    metadata: payload.metadata && typeof payload.metadata === 'object' ? payload.metadata : {},
    series: payload.series && typeof payload.series === 'object' ? payload.series : {},
    scalars: payload.scalars && typeof payload.scalars === 'object' ? payload.scalars : {},
    scalar_latest: payload.scalar_latest && typeof payload.scalar_latest === 'object' ? payload.scalar_latest : {},
  };
}

function _metricsPageNormalizeRecords() {
  const records = [];
  Object.values(_metricsPageState.runData).forEach((payload) => {
    const runHash = payload.run.runHash;
    const cluster = payload.run.cluster;
    const runName = payload.info.run_name || payload.info.name || `Run ${runHash}`;
    const metadata = payload.metrics.metadata || {};
    Object.entries(payload.metrics.series || {}).forEach(([key, points]) => {
      records.push(_metricsPageRecord({ cluster, runHash, runName, metadata, key, kind: 'series', points: points || [] }));
    });
    Object.entries(payload.metrics.scalars || {}).forEach(([key, points]) => {
      records.push(_metricsPageRecord({ cluster, runHash, runName, metadata, key, kind: 'scalars', points: points || [] }));
    });
  });
  _metricsPageState.records = records;
}

function _metricsPageRecord({ cluster, runHash, runName, metadata, key, kind, points }) {
  const stats = _metricsPageStats(points, kind === 'series');
  return {
    cluster,
    runHash,
    runName,
    key,
    kind,
    points,
    metadata,
    stats,
    context: _metricsPageMergedContext(points),
    contexts: _metricsPageMetricContexts(points),
    numeric: kind === 'series' ? stats.numericCount >= 2 : Number.isFinite((points[points.length - 1] || {}).value_num),
  };
}

function _metricsPageStats(points, stepped) {
  const nums = (points || []).map(p => p.value_num).filter(Number.isFinite);
  const last = (points || [])[Math.max(0, (points || []).length - 1)] || {};
  return {
    latest: last.value,
    numericCount: nums.length,
    min: nums.length ? Math.min(...nums) : null,
    max: nums.length ? Math.max(...nums) : null,
    lastStep: stepped ? last.step : null,
  };
}

function _metricsPageMergedContext(points) {
  const merged = {};
  (points || []).forEach(point => {
    Object.entries(point.context || {}).forEach(([key, value]) => {
      if (merged[key] == null) merged[key] = value;
      else if (merged[key] !== value) {
        if (!Array.isArray(merged[key])) merged[key] = [merged[key]];
        if (!merged[key].includes(value)) merged[key].push(value);
      }
    });
  });
  return merged;
}

function _metricsPageMetricContexts(points) {
  const seen = new Set();
  (points || []).forEach(point => {
    Object.entries(point.context || {}).forEach(([key, value]) => seen.add(`${key}:${value}`));
  });
  return Array.from(seen);
}

function _metricsPageApplyDefaultSelection() {
  const numericMetrics = _metricsPageFilteredRecords().filter(r => r.numeric).map(r => r.key);
  _metricsPageState.selectedMetrics = _metricsPageState.selectedMetrics.filter(key => numericMetrics.includes(key));
  if (!_metricsPageState.selectedMetrics.length) {
    _metricsPageState.selectedMetrics = Array.from(new Set(numericMetrics)).slice(0, 4);
  }
}

function _metricsPageEnsureChartPanels() {
  if (!_metricsPageState.chartsConfig.length) {
    _metricsPageState.chartsConfig = [_metricsPageNewChartConfig(_metricsPageState.selectedMetrics)];
    return;
  }
  const available = new Set(_metricsPageFilteredRecords().filter(r => r.numeric).map(r => r.key));
  _metricsPageState.chartsConfig = _metricsPageState.chartsConfig
    .map(chart => ({ ...chart, metricKeys: (chart.metricKeys || []).filter(key => available.has(key)) }))
    .filter(chart => (chart.metricKeys || []).length);
  if (!_metricsPageState.chartsConfig.length) {
    _metricsPageState.chartsConfig = [_metricsPageNewChartConfig(_metricsPageState.selectedMetrics)];
  }
}

function _metricsPageNewChartConfig(metricKeys) {
  const id = `chart-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
  return {
    id,
    title: 'Comparison',
    metricKeys: Array.from(new Set(metricKeys || [])).slice(0, 8),
    query: _metricsPageState.query,
    groupByContext: _metricsPageState.groupByContext,
    xAxis: _metricsPageState.xAxis,
    yScale: _metricsPageState.yScale,
    smoothing: _metricsPageState.smoothing,
    showRaw: _metricsPageState.showRaw,
    ignoreOutliers: _metricsPageState.ignoreOutliers,
    tooltipMode: _metricsPageState.tooltipMode,
    hiddenTraces: {},
  };
}

function _metricsPageFilteredRecords(kind) {
  return _metricsPageState.records.filter(record => {
    if (kind && record.kind !== kind) return false;
    return _metricsPageMatchesQuery(record, _metricsPageState.query);
  });
}

function _metricsPageMatchesQuery(record, query) {
  const q = String(query || '').trim();
  if (!q) return true;
  if (q.includes('@')) return _metricsPageMatchesAtQuery(record, q);
  if (_metricsPageMatchesRunFieldQuery(record, q)) return true;
  if (typeof _runPageMatchesMetricQuery === 'function') {
    const proxy = { ...record, kind: record.kind, key: record.key, context: record.context, metadata: record.metadata, stats: record.stats, contexts: record.contexts, points: record.points };
    if (_runPageMatchesMetricQuery(proxy, q)) return true;
  }
  const hay = [
    record.runHash, record.cluster, record.runName, record.key, record.kind,
    ...record.contexts,
    ...Object.entries(record.metadata || {}).map(([k, v]) => `${k}:${_formatMetricValue(v)}`),
  ].join(' ').toLowerCase();
  return hay.includes(q.toLowerCase());
}

function _metricsPageMatchesAtQuery(record, query) {
  const orParts = query.split(/\s+or\s+/i).map(s => s.trim()).filter(Boolean);
  return (orParts.length ? orParts : [query]).some(part => {
    const andParts = part.split(/\s+and\s+/i).map(s => s.trim()).filter(Boolean);
    return andParts.every(term => {
      const m = term.match(/^@([A-Za-z0-9]+)/);
      if (m) return String(record.runHash || '').toLowerCase().startsWith(m[1].toLowerCase());
      return _metricsPageMatchesQuery(record, term);
    });
  });
}

function _metricsPageMatchesRunFieldQuery(record, query) {
  const m = String(query || '').trim().match(/^run\.(hash|cluster|name)\s*(==|!=)\s*(.+)$/);
  if (!m) return false;
  const field = m[1] === 'hash' ? record.runHash : (m[1] === 'cluster' ? record.cluster : record.runName);
  const value = _runPageStripQuotes ? _runPageStripQuotes(m[3]) : String(m[3] || '').replace(/^['"]|['"]$/g, '');
  return m[2] === '==' ? String(field) === String(value) : String(field) !== String(value);
}

function _metricsPageRenderExplorer() {
  const body = document.getElementById('metrics-page-body');
  if (!body) return;
  _metricsPageDestroyCharts();
  const seriesRecords = _metricsPageFilteredRecords('series').filter(r => r.numeric);
  const scalarRecords = _metricsPageFilteredRecords('scalars').filter(r => r.numeric);
  _metricsPageEnsureChartPanels();
  body.innerHTML = `<div class="metrics-page-layout">
    <section class="metrics-page-main">
      ${_metricsPageSelectHtml(seriesRecords, scalarRecords)}
      <div class="metrics-page-chart-grid">
        ${_metricsPageState.chartsConfig.map((chart, idx) => _metricsPageChartPanelHtml(chart, idx)).join('')}
      </div>
      ${_metricsPageContextTableHtml()}
    </section>
    <aside class="metrics-page-sidebar">
      ${_metricsPageSavedViewsHtml()}
      ${_metricsPageRunsSummaryHtml()}
    </aside>
  </div>`;
  _metricsPageRenderCharts();
}

function _metricsPageChartPanelHtml(chart, idx) {
  const metricChips = (chart.metricKeys || []).map(key => `
    <button type="button" class="metrics-page-selected-chip" onclick="_metricsPageRemoveMetricFromChart('${escAttr(chart.id)}','${escAttr(key)}')">
      ${_escHtml(key)} <span>×</span>
    </button>`).join('');
  const metricOptions = Array.from(new Set(_metricsPageFilteredRecords().filter(r => r.numeric).map(r => r.key)))
    .filter(key => !(chart.metricKeys || []).includes(key))
    .sort()
    .map(key => `<option value="${escAttr(key)}">${_escHtml(key)}</option>`).join('');
  return `<div class="metrics-page-card metrics-page-chart-card" data-chart-id="${escAttr(chart.id)}">
    <div class="metrics-page-card-title-row">
      <div>
        <div class="metrics-page-card-title"><span class="metrics-page-chart-num">#${idx + 1}</span> ${_escHtml(chart.title || 'Comparison')}</div>
        <div class="metrics-page-card-sub">${(chart.metricKeys || []).length} metrics · ${_metricsPageState.runs.length} runs</div>
      </div>
      <div class="metrics-page-chart-actions">
        <button class="btn" onclick="_metricsPageExportChart('${escAttr(chart.id)}', ${idx + 1})">export</button>
        <button class="btn" onclick="_metricsPageDuplicateChart('${escAttr(chart.id)}')">duplicate</button>
        <button class="btn" onclick="_metricsPageRemoveChart('${escAttr(chart.id)}')">remove</button>
      </div>
    </div>
    <div class="metrics-page-chart-local-select">
      <select onchange="_metricsPageAddMetricToChart('${escAttr(chart.id)}', this.value); this.value=''">
        <option value="">Add metric to chart…</option>${metricOptions || '<option disabled>No more metrics</option>'}
      </select>
      <div class="metrics-page-selected-chips">${metricChips || '<span class="metrics-page-muted">No metrics selected.</span>'}</div>
    </div>
    <div class="metrics-page-chart"><canvas id="metrics-chart-${escAttr(chart.id)}"></canvas></div>
    ${_metricsPageLegendHtml('Legend', _metricsPageLegendItemsFromDatasets(_metricsPageBuildChartDatasets(chart)), true, chart.id)}
    ${_metricsPageModifiersHtml(chart)}
  </div>`;
}

function _metricsPageSelectHtml(seriesRecords, scalarRecords) {
  const selected = new Set(_metricsPageState.selectedMetrics);
  const metricNames = Array.from(new Set(seriesRecords.map(r => r.key))).sort();
  const scalarNames = Array.from(new Set(scalarRecords.map(r => r.key))).sort();
  const metricOptions = metricNames.filter(k => !selected.has(k)).map(key => `<option value="${escAttr(key)}">${_escHtml(key)}</option>`).join('');
  const scalarOptions = scalarNames.filter(k => !selected.has(k)).map(key => `<option value="${escAttr(key)}">${_escHtml(key)}</option>`).join('');
  const selectedChips = _metricsPageState.selectedMetrics.length
    ? _metricsPageState.selectedMetrics.map(key => `<button type="button" class="metrics-page-selected-chip" onclick="_metricsPageRemoveMetric('${escAttr(key)}')">${_escHtml(key)} <span>×</span></button>`).join('')
    : '<span class="metrics-page-muted">No metrics selected.</span>';
  const contextChips = _metricsPageContextSummary().map(item => `
    <button type="button" class="metrics-page-context-chip" onclick="_metricsPageApplyContextFilter('${escAttr(item.key)}','${escAttr(String(item.value))}')">
      <span>${_escHtml(item.key)}=${_escHtml(String(item.value))}</span><b>${item.count}</b>
    </button>`).join('');
  return `<div class="metrics-page-card metrics-page-select-card">
    <div class="metrics-page-card-title-row">
      <div>
        <div class="metrics-page-card-title">Metrics Select</div>
        <div class="metrics-page-card-sub">Python-like filter across run, metric, context, and metadata. ${seriesRecords.length} series records, ${scalarRecords.length} scalar records match.</div>
      </div>
      <div class="metrics-page-actions">
        <button class="btn" onclick="_metricsPageSelectMatches()">select matches</button>
        <button class="btn" onclick="_metricsPageClearQuery()">clear query</button>
      </div>
    </div>
    <div class="metrics-page-input-wrap">
    <input class="metrics-page-query-input" value="${escAttr(_metricsPageState.query)}"
           placeholder='run.hash == "86398daa" and metric.name.contains("accuracy")'
           oninput="_metricsPageOnQueryInput(this.value)"
           onchange="_metricsPageSetQuery(this.value)"
           onkeydown="_metricsPageQueryKey(event, this.value)">
      <div class="metrics-page-suggest" id="metrics-page-query-suggest"></div>
    </div>
    <div class="metrics-page-query-hint">
      Examples: <code>run.cluster == "aws-cmh"</code>, <code>metric.name.startswith("system/")</code>, <code>context.split == "eval"</code>, <code>metadata.model.contains("synthetic")</code>
    </div>
    <div class="metrics-page-dropdown-row">
      <label>Series
        <select onchange="_metricsPageAddMetric(this.value); this.value=''">
          <option value="">Add series metric…</option>${metricOptions || '<option disabled>No matching metrics</option>'}
        </select>
      </label>
      <label>Scalars
        <select onchange="_metricsPageAddMetric(this.value); this.value=''">
          <option value="">Add scalar metric…</option>${scalarOptions || '<option disabled>No matching scalars</option>'}
        </select>
      </label>
    </div>
    <div class="metrics-page-mini-label">Selected metrics</div>
    <div class="metrics-page-selected-chips">${selectedChips}</div>
    <div class="metrics-page-mini-label">Metric context</div>
    <div class="metrics-page-context-cloud">${contextChips || '<span class="metrics-page-muted">No context on matching metrics.</span>'}</div>
  </div>`;
}

function _metricsPageSetQuery(value) {
  _metricsPageState.query = value || '';
  _metricsPageReplaceUrlState();
  _metricsPageApplyDefaultSelection();
  _metricsPageRenderExplorer();
}

function _metricsPageClearQuery() {
  _metricsPageState.query = '';
  _metricsPageReplaceUrlState();
  _metricsPageRenderExplorer();
}

function _metricsPageAddMetric(key) {
  if (!key || _metricsPageState.selectedMetrics.includes(key)) return;
  _metricsPageState.selectedMetrics = [..._metricsPageState.selectedMetrics, key];
  if (_metricsPageState.chartsConfig.length) {
    const chart = _metricsPageState.chartsConfig[0];
    chart.metricKeys = Array.from(new Set([...(chart.metricKeys || []), key]));
  }
  _metricsPageReplaceUrlState();
  _metricsPageRenderExplorer();
}

function _metricsPageRemoveMetric(key) {
  _metricsPageState.selectedMetrics = _metricsPageState.selectedMetrics.filter(k => k !== key);
  _metricsPageState.chartsConfig.forEach(chart => {
    chart.metricKeys = (chart.metricKeys || []).filter(k => k !== key);
  });
  _metricsPageReplaceUrlState();
  _metricsPageRenderExplorer();
}

function _metricsPageSelectMatches() {
  _metricsPageState.selectedMetrics = Array.from(new Set(_metricsPageFilteredRecords('series').filter(r => r.numeric).map(r => r.key))).slice(0, 8);
  if (_metricsPageState.chartsConfig.length) {
    _metricsPageState.chartsConfig[0].metricKeys = [..._metricsPageState.selectedMetrics];
  }
  _metricsPageReplaceUrlState();
  _metricsPageRenderExplorer();
}

function _metricsPageApplyContextFilter(key, value) {
  _metricsPageState.query = `context.${key} == "${String(value).replace(/"/g, '\\"')}"`;
  _metricsPageSelectMatches();
}

function _metricsPageAddChart() {
  _metricsPageState.chartsConfig.push(_metricsPageNewChartConfig(_metricsPageState.selectedMetrics));
  _metricsPageReplaceUrlState();
  _metricsPageRenderExplorer();
}

function _metricsPageRemoveChart(chartId) {
  if (_metricsPageState.chartsConfig.length <= 1) {
    toast('Keep at least one chart in the workspace', 'error');
    return;
  }
  _metricsPageState.chartsConfig = _metricsPageState.chartsConfig.filter(c => c.id !== chartId);
  _metricsPageReplaceUrlState();
  _metricsPageRenderExplorer();
}

function _metricsPageDuplicateChart(chartId) {
  const chart = _metricsPageState.chartsConfig.find(c => c.id === chartId);
  if (!chart) return;
  _metricsPageState.chartsConfig.push({ ...chart, id: _metricsPageNewChartConfig([]).id, title: `${chart.title || 'Comparison'} copy`, hiddenTraces: { ...(chart.hiddenTraces || {}) } });
  _metricsPageReplaceUrlState();
  _metricsPageRenderExplorer();
}

function _metricsPageAddMetricToChart(chartId, key) {
  const chart = _metricsPageState.chartsConfig.find(c => c.id === chartId);
  if (!chart || !key || (chart.metricKeys || []).includes(key)) return;
  chart.metricKeys = [...(chart.metricKeys || []), key];
  _metricsPageState.selectedMetrics = Array.from(new Set([..._metricsPageState.selectedMetrics, key]));
  _metricsPageReplaceUrlState();
  _metricsPageRenderExplorer();
}

function _metricsPageRemoveMetricFromChart(chartId, key) {
  const chart = _metricsPageState.chartsConfig.find(c => c.id === chartId);
  if (!chart) return;
  chart.metricKeys = (chart.metricKeys || []).filter(k => k !== key);
  _metricsPageReplaceUrlState();
  _metricsPageRenderExplorer();
}

function _metricsPageExportChart(chartId, idx) {
  const canvas = document.getElementById(`metrics-chart-${chartId}`);
  if (!canvas) return;
  const a = document.createElement('a');
  a.download = `clausius-metrics-chart-${idx}.png`;
  a.href = canvas.toDataURL('image/png');
  a.click();
}

function _metricsPageContextSummary() {
  const counts = {};
  _metricsPageFilteredRecords().forEach(record => {
    (record.points || []).forEach(point => {
      Object.entries(point.context || {}).forEach(([key, value]) => {
        const id = `${key}=${String(value)}`;
        if (!counts[id]) counts[id] = { key, value, count: 0 };
        counts[id].count += 1;
      });
    });
  });
  return Object.values(counts).sort((a, b) => b.count - a.count || a.key.localeCompare(b.key)).slice(0, 24);
}

function _metricsPageContextKeys() {
  const keys = new Set();
  _metricsPageState.records.forEach(record => {
    (record.points || []).forEach(point => Object.keys(point.context || {}).forEach(k => keys.add(k)));
  });
  return Array.from(keys).sort();
}

function _metricsPageRenderCharts() {
  if (typeof Chart === 'undefined') {
    if (_metricsPageChartRetryTimer) clearTimeout(_metricsPageChartRetryTimer);
    _metricsPageChartRetryTimer = setTimeout(() => {
      _metricsPageChartRetryTimer = null;
      if (document.querySelector('[id^="metrics-chart-"]')) _metricsPageRenderCharts();
    }, 80);
    return;
  }
  _metricsPageState.chartsConfig.forEach(chart => {
    const canvas = document.getElementById(`metrics-chart-${chart.id}`);
    const datasets = _metricsPageBuildChartDatasets(chart);
    if (canvas && datasets.length) {
      _metricsPageState.charts.push(new Chart(canvas, {
        type: 'line',
        data: { datasets },
        options: _metricsPageChartOptions(_metricsPageXAxisLabel(chart), chart.yScale || 'linear', false, chart),
      }));
    }
  });
  requestAnimationFrame(() => {
    _metricsPageState.charts.forEach(chart => {
      try { chart.resize(); chart.update('none'); } catch (_) {}
    });
  });
}

function _metricsPageBuildChartDatasets(chart) {
  const datasets = [];
  let colorIdx = 0;
  const selected = new Set(chart.metricKeys || []);
  _metricsPageFilteredRecords().filter(r => r.numeric && selected.has(r.key)).forEach(record => {
    if (record.kind === 'scalars') {
      const p = (record.points || [])[Math.max(0, (record.points || []).length - 1)] || {};
      if (!Number.isFinite(p.value_num)) return;
      const color = RUN_PAGE_COLORS[colorIdx % RUN_PAGE_COLORS.length];
      colorIdx++;
      const label = _metricsPageTraceLabel(record, record.key);
      const id = `scalar:${chart.id}:${record.cluster}/${record.runHash}/${record.key}`;
      const scalarIndex = colorIdx;
      datasets.push({
        type: 'scatter',
        label,
        data: [{ x: scalarIndex, y: p.value_num }],
        borderColor: color,
        backgroundColor: color + 'aa',
        pointRadius: 4,
        pointHoverRadius: 7,
        showLine: false,
        hidden: !!(chart.hiddenTraces || {})[id],
        _legendId: id,
        _legendValue: _formatMetricValue(p.value),
        _legendMeta: `${record.runHash} · scalar`,
        _metricKind: 'scalar',
      });
      return;
    }
    const rawGroups = _metricsPageGroupPoints(record.points || []);
    rawGroups.forEach(([groupName, groupPoints]) => {
      const raw = chart.ignoreOutliers ? _metricsPageFilterOutliers(groupPoints) : groupPoints;
      const color = RUN_PAGE_COLORS[colorIdx % RUN_PAGE_COLORS.length];
      colorIdx++;
      const label = _metricsPageTraceLabel(record, `${record.key}${groupName ? ` · ${groupName}` : ''}`);
      const id = `series:${record.cluster}/${label}`;
      const visiblePoints = _metricsPageDownsample(_metricsPagePointsForAxis(raw, chart), 2200);
      const latest = raw[raw.length - 1] || {};
      const meta = `${record.runHash} · ${record.cluster}`;
      if (chart.showRaw && (chart.smoothing || 0) > 0) {
        const rawId = `${id}:raw`;
        datasets.push({
          label: `${label} raw`,
          data: visiblePoints,
          borderColor: color + '55',
          backgroundColor: color + '11',
          borderWidth: 1,
          pointRadius: 0,
          tension: 0,
          hidden: !!(chart.hiddenTraces || {})[rawId],
          _legendId: rawId,
          _legendValue: latest.value == null ? '' : _formatMetricValue(latest.value),
          _legendMeta: `${meta} · original`,
          _legendMuted: true,
          _metricKind: 'series',
        });
      }
      datasets.push({
        label,
        data: _metricsPageSmoothPoints(visiblePoints, chart.smoothing || 0),
        borderColor: color,
        backgroundColor: color + '22',
        borderWidth: 2,
        pointRadius: 0,
        pointHoverRadius: 6,
        pointHitRadius: 10,
        tension: 0,
        hidden: !!(chart.hiddenTraces || {})[id],
        _legendId: id,
        _legendValue: latest.value == null ? '' : _formatMetricValue(latest.value),
        _legendMeta: meta,
        _metricKind: 'series',
      });
    });
  });
  return datasets;
}

function _metricsPageTraceLabel(record, metricLabel) {
  const name = record.runName || record.runHash;
  return `${name} / ${metricLabel}`;
}

function _metricsPageGroupPoints(points) {
  const key = _metricsPageState.groupByContext;
  if (!key) return [['', points || []]];
  const groups = {};
  (points || []).forEach(point => {
    const value = point.context && point.context[key] != null ? String(point.context[key]) : 'None';
    if (!groups[value]) groups[value] = [];
    groups[value].push(point);
  });
  return Object.entries(groups).sort((a, b) => a[0].localeCompare(b[0]));
}

function _metricsPagePointsForAxis(points, chart = _metricsPageState) {
  const firstTs = (points || []).find(p => Number.isFinite(p.ts))?.ts || 0;
  return (points || []).filter(p => Number.isFinite(p.value_num)).map((p, idx) => {
    let x = idx + 1;
    if ((chart.xAxis || 'step') === 'step') x = p.step == null ? idx + 1 : p.step;
    else if (chart.xAxis === 'wall_time') x = Number.isFinite(p.ts) && firstTs ? (p.ts - firstTs) / 60 : idx + 1;
    return { x, y: p.value_num, raw: p };
  }).filter(p => (chart.yScale || 'linear') !== 'logarithmic' || p.y > 0);
}

function _metricsPageXAxisLabel(chart = _metricsPageState) {
  if (chart.xAxis === 'wall_time') return 'minutes since first point';
  if (chart.xAxis === 'index') return 'point index';
  return 'step';
}

function _metricsPageDownsample(points, maxPoints = 2000) {
  if (typeof _runPageDownsample === 'function') return _runPageDownsample(points, maxPoints);
  return points || [];
}

function _metricsPageSmoothPoints(points, amount) {
  if (typeof _runPageSmoothPoints === 'function') return _runPageSmoothPoints(points, amount);
  return points || [];
}

function _metricsPageFilterOutliers(points) {
  if (typeof _runPageFilterOutlierPoints === 'function') return _runPageFilterOutlierPoints(points);
  return points || [];
}

function _metricsPageBuildScalarInfo(records) {
  const labels = [];
  const values = [];
  const colors = [];
  const legendItems = [];
  let idx = 0;
  const selected = new Set(_metricsPageState.selectedMetrics);
  records.filter(r => selected.has(r.key)).forEach(record => {
    const pts = record.points || [];
    const p = pts[pts.length - 1] || {};
    const color = RUN_PAGE_COLORS[idx % RUN_PAGE_COLORS.length];
    idx++;
    const label = `${record.runHash} · ${record.key}`;
    const id = `scalar:${record.cluster}/${label}`;
    const hidden = !!_metricsPageState.hiddenTraces[id];
    if (Number.isFinite(p.value_num)) {
      if (!hidden) {
        labels.push(label);
        values.push(p.value_num);
        colors.push(color);
      }
      legendItems.push({ id, label, color, value: _formatMetricValue(p.value), meta: `${record.cluster} · ${record.runName}`, hidden });
    }
  });
  return { labels, values, colors, legendItems };
}

function _metricsPageLegendItemsFromDatasets(datasets) {
  return (datasets || []).map(ds => ({
    id: ds._legendId || ds.label,
    label: ds.label,
    color: ds.borderColor || '#22c55e',
    value: ds._legendValue || '',
    meta: ds._legendMeta || '',
    muted: !!ds._legendMuted,
    hidden: !!ds.hidden,
    kind: ds._metricKind || '',
  }));
}

function _metricsPageLegendHtml(title, items, compact = false, chartId = '') {
  if (!items || !items.length) {
    return `<div class="metrics-page-card metrics-page-legend-card"><div class="metrics-page-card-title">${_escHtml(title)}</div><div class="metrics-page-muted">No visible traces.</div></div>`;
  }
  return `<div class="metrics-page-card metrics-page-legend-card">
    <div class="metrics-page-card-title">${_escHtml(title)}</div>
    <div class="metrics-page-legend-list${compact ? ' compact' : ''}">
      ${items.map(item => `
        <button type="button" data-kind="${escAttr(item.kind || '')}" class="metrics-page-legend-item${item.hidden ? ' hidden-trace' : ''}${item.muted ? ' muted' : ''}" style="--legend-color:${escAttr(item.color)}" onclick="_metricsPageToggleTrace('${escAttr(item.id)}','${escAttr(chartId)}')">
          <span class="metrics-page-legend-eye">${item.hidden ? '○' : '●'}</span>
          <span class="metrics-page-legend-line"></span>
          <span class="metrics-page-legend-name" title="${escAttr(item.label)}">${_escHtml(item.label)}</span>
          ${item.value ? `<span class="metrics-page-legend-value">${_escHtml(item.value)}</span>` : ''}
          ${item.meta ? `<span class="metrics-page-legend-meta">${_escHtml(item.meta)}</span>` : ''}
        </button>`).join('')}
    </div>
  </div>`;
}

function _metricsPageToggleTrace(id, chartId = '') {
  const chart = _metricsPageState.chartsConfig.find(c => c.id === chartId);
  if (chart) {
    chart.hiddenTraces = chart.hiddenTraces || {};
    chart.hiddenTraces[id] = !chart.hiddenTraces[id];
  } else {
    _metricsPageState.hiddenTraces[id] = !_metricsPageState.hiddenTraces[id];
  }
  _metricsPageRenderExplorer();
}

function _metricsPageModifiersHtml(chart = _metricsPageState) {
  const contextOptions = _metricsPageContextKeys().map(key => `<option value="${escAttr(key)}"${(chart.groupByContext || '') === key ? ' selected' : ''}>${_escHtml(key)}</option>`).join('');
  const chartId = chart.id || '';
  return `<div class="metrics-page-card metrics-page-modifiers-card">
    <div class="metrics-page-card-title">Metrics Modifiers</div>
    <label>Group by<select onchange="_metricsPageSetChartOption('${escAttr(chartId)}','groupByContext',this.value)"><option value="">None</option>${contextOptions}</select></label>
    <label>X-axis<select onchange="_metricsPageSetChartOption('${escAttr(chartId)}','xAxis',this.value)"><option value="step"${(chart.xAxis || 'step') === 'step' ? ' selected' : ''}>step</option><option value="wall_time"${chart.xAxis === 'wall_time' ? ' selected' : ''}>wall time</option><option value="index"${chart.xAxis === 'index' ? ' selected' : ''}>index</option></select></label>
    <label>Y scale<select onchange="_metricsPageSetChartOption('${escAttr(chartId)}','yScale',this.value)"><option value="linear"${(chart.yScale || 'linear') === 'linear' ? ' selected' : ''}>linear</option><option value="logarithmic"${chart.yScale === 'logarithmic' ? ' selected' : ''}>log</option></select></label>
    <label>Hover<select onchange="_metricsPageSetChartOption('${escAttr(chartId)}','tooltipMode',this.value)"><option value="all"${(chart.tooltipMode || 'all') === 'all' ? ' selected' : ''}>all lines at step</option><option value="nearest"${chart.tooltipMode === 'nearest' ? ' selected' : ''}>exact line only</option></select></label>
    <label class="metrics-page-smoothing">Smoothing <b>${_metricsPageSmoothingLabel(chart.smoothing ?? 0.25)}</b><input type="range" min="0" max="0.95" step="0.05" value="${chart.smoothing ?? 0.25}" oninput="_metricsPagePreviewSmoothing(this.value)" onchange="_metricsPageSetChartOption('${escAttr(chartId)}','smoothing',this.value)" onpointerup="_metricsPageSetChartOption('${escAttr(chartId)}','smoothing',this.value)"></label>
    <label class="metrics-page-switch"><input type="checkbox" ${chart.showRaw ? 'checked' : ''} onchange="_metricsPageSetChartOption('${escAttr(chartId)}','showRaw',this.checked)"> Show original</label>
    <label class="metrics-page-switch"><input type="checkbox" ${chart.ignoreOutliers ? 'checked' : ''} onchange="_metricsPageSetChartOption('${escAttr(chartId)}','ignoreOutliers',this.checked)"> Ignore outliers</label>
  </div>`;
}

function _metricsPageSetChartOption(chartId, key, value) {
  const chart = _metricsPageState.chartsConfig.find(c => c.id === chartId);
  if (!chart) return;
  if (key === 'smoothing') value = Math.max(0, Math.min(0.95, parseFloat(value) || 0));
  if (key === 'showRaw' || key === 'ignoreOutliers') value = !!value;
  chart[key] = value;
  _metricsPageReplaceUrlState();
  _metricsPageRenderExplorer();
}

function _metricsPageSetGroupBy(value) { _metricsPageState.groupByContext = value || ''; _metricsPageReplaceUrlState(); _metricsPageRenderExplorer(); }
function _metricsPageSetXAxis(value) { _metricsPageState.xAxis = value || 'step'; _metricsPageReplaceUrlState(); _metricsPageRenderExplorer(); }
function _metricsPageSetYScale(value) { _metricsPageState.yScale = value || 'linear'; _metricsPageReplaceUrlState(); _metricsPageRenderExplorer(); }
function _metricsPageSetTooltipMode(value) { _metricsPageState.tooltipMode = value === 'nearest' ? 'nearest' : 'all'; _metricsPageReplaceUrlState(); _metricsPageRenderExplorer(); }
function _metricsPageSetShowRaw(value) { _metricsPageState.showRaw = !!value; _metricsPageReplaceUrlState(); _metricsPageRenderExplorer(); }
function _metricsPageSetIgnoreOutliers(value) { _metricsPageState.ignoreOutliers = !!value; _metricsPageReplaceUrlState(); _metricsPageRenderExplorer(); }
function _metricsPageSmoothingLabel(value) { return (Math.max(0, Math.min(0.95, parseFloat(value) || 0))).toFixed(2); }
function _metricsPagePreviewSmoothing(value) {
  const el = document.querySelector('.metrics-page-smoothing b');
  if (el) el.textContent = _metricsPageSmoothingLabel(value);
}
function _metricsPageSetSmoothing(value) { _metricsPageState.smoothing = Math.max(0, Math.min(0.95, parseFloat(value) || 0)); _metricsPageReplaceUrlState(); _metricsPageRenderExplorer(); }

function _metricsPageChartOptions(xTitle, yScale, isBar = false, chart = _metricsPageState) {
  const prevMode = _runPageState && _runPageState.tooltipMode;
  if (typeof _runPageState !== 'undefined') _runPageState.tooltipMode = chart.tooltipMode || _metricsPageState.tooltipMode;
  const options = typeof _runPageChartOptions === 'function'
    ? _runPageChartOptions(xTitle, yScale, isBar)
    : { responsive: true, maintainAspectRatio: false };
  if (typeof _runPageState !== 'undefined') _runPageState.tooltipMode = prevMode;
  return options;
}

function _metricsPageScalarCardsHtml(records) {
  const selected = new Set(_metricsPageState.selectedMetrics);
  const cards = records.filter(r => selected.has(r.key)).slice(0, 24).map(record => {
    const p = (record.points || [])[Math.max(0, (record.points || []).length - 1)] || {};
    return `<div class="metrics-page-stat-card">
      <span>${_escHtml(record.runHash)} · scalar</span>
      <b>${_escHtml(record.key)}</b>
      <strong>${_escHtml(_formatMetricValue(p.value))}</strong>
      <span>${_escHtml(record.cluster)}</span>
    </div>`;
  });
  return cards.length ? cards.join('') : '<div class="metrics-page-muted">Select scalar metrics to see values.</div>';
}

function _metricsPageContextTableHtml() {
  const selected = new Set(_metricsPageState.selectedMetrics);
  const rows = [];
  _metricsPageState.records.filter(r => selected.has(r.key)).forEach(record => {
    const counts = {};
    (record.points || []).forEach(point => {
      Object.entries(point.context || {}).forEach(([key, value]) => {
        const id = `${key}\u0000${String(value)}`;
        if (!counts[id]) counts[id] = { key, value, count: 0 };
        counts[id].count += 1;
      });
    });
    const entries = Object.values(counts);
    if (!entries.length) rows.push(`<tr><td>${_escHtml(record.runHash)}</td><td>${_escHtml(record.key)}</td><td colspan="3" class="dim">No context</td></tr>`);
    entries.forEach(item => rows.push(`<tr><td>${_escHtml(record.runHash)}</td><td>${_escHtml(record.key)}</td><td>${_escHtml(item.key)}</td><td>${_escHtml(String(item.value))}</td><td class="dim">${item.count}</td></tr>`));
  });
  return `<div class="metrics-page-card">
    <div class="metrics-page-card-title">Context Table</div>
    ${rows.length ? `<table class="metrics-page-context-table"><thead><tr><th>Run</th><th>Metric</th><th>Context key</th><th>Context value</th><th>Points</th></tr></thead><tbody>${rows.join('')}</tbody></table>` : '<div class="metrics-page-muted">Select metrics to inspect context.</div>'}
  </div>`;
}

function _metricsPageRunsSummaryHtml() {
  return `<div class="metrics-page-card metrics-page-runs-card">
    <div class="metrics-page-card-title">Runs</div>
    ${_metricsPageState.runs.map(run => {
      const payload = _metricsPageState.runData[_metricsPageRunKey(run)];
      const name = payload && payload.info ? (payload.info.run_name || payload.info.name || run.runHash) : run.runHash;
      return `<div class="metrics-page-run-row"><span>${_escHtml(run.cluster)}</span><b>${_escHtml(run.runHash)}</b><em>${_escHtml(name)}</em></div>`;
    }).join('')}
  </div>`;
}

function _metricsPageDestroyCharts() {
  if (_metricsPageChartRetryTimer) {
    clearTimeout(_metricsPageChartRetryTimer);
    _metricsPageChartRetryTimer = null;
  }
  _metricsPageState.charts.forEach(chart => {
    try { chart.destroy(); } catch (_) {}
  });
  _metricsPageState.charts = [];
}

function _metricsPageSetRecordsForTest(records) {
  _metricsPageState.records = Array.isArray(records) ? records : [];
}

function _metricsPageFirstChartForTest() {
  return _metricsPageState.chartsConfig[0];
}
