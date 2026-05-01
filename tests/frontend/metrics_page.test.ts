/**
 * Unit tests for the multi-run Metrics Explorer page.
 *
 * Run with: npx vitest run tests/frontend/metrics_page.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import { loadBrowserScripts } from './helpers';

declare const parseMetricsRunRefs: (text: string) => Array<{ cluster: string; runHash: string }>;
declare const _metricsPageNormalizeMetrics: (payload: any) => any;
declare const _metricsPageRecord: (payload: any) => any;
declare const _metricsPageMatchesQuery: (record: any, query: string) => boolean;
declare const _metricsPageCurrentQuery: () => string;
declare const _metricsPageReadUrlState: () => void;
declare const _metricsPageApplySerializedState: (state: any) => void;
declare const _metricsPageSerializeState: () => any;
declare const _metricsPageNewChartConfig: (metricKeys: string[]) => any;
declare const _metricsPageBuildChartDatasets: (chart: any) => any[];
declare const _metricsPageSetRecordsForTest: (records: any[]) => void;
declare const _metricsPageFirstChartForTest: () => any;
declare const _metricsPageSmoothingLabel: (value: string | number) => string;
declare const _metricsPageChartOptions: (xTitle: string, yScale: string, isBar?: boolean) => any;
declare const showTab: (tab: string) => void;
declare const _metricsPageRecentRunSuggestions: (query?: string) => any[];
declare const _metricsPageApplySuggestion: (idx: number) => void;
declare const _metricsPageRenderSuggestions: (target: string, runs: any[]) => void;

beforeAll(() => {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"aws-cmh":{"host":"login","gpu_type":"gb300"}}</script>
    <script id="username-data" type="text/plain">tester</script>
    <script id="team-data" type="text/plain">team</script>
  `;
  loadBrowserScripts(['utils.js', 'jobs.js', 'runs.js', 'run_page.js', 'metrics_page.js']);
});

beforeEach(() => {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"aws-cmh":{"host":"login","gpu_type":"gb300"}}</script>
    <script id="username-data" type="text/plain">tester</script>
    <script id="team-data" type="text/plain">team</script>
    <div id="live-view"></div>
    <div id="history-view"></div>
    <div id="metrics-view"><div id="metrics-page"></div></div>
    <div id="project-view"></div>
    <div id="run-page-view"></div>
    <div id="logbook-view"></div>
    <div id="clusters-view"></div>
    <div id="explorer-page"></div>
    <button id="tab-live"></button>
    <button id="tab-history"></button>
    <button id="tab-metrics"></button>
    <button id="tab-logbook"></button>
    <button id="tab-clusters"></button>
    <div id="topbar-tabs"></div>
  `;
  localStorage.clear();
  history.replaceState(null, '', '#/metrics');
});

describe('parseMetricsRunRefs', () => {
  it('accepts comma, whitespace, and newline separated cluster/hash refs', () => {
    const refs = parseMetricsRunRefs('aws-cmh/abc12345, eos/deadbeef\nabc999999999 aws-cmh/abc12345');

    expect(refs).toEqual([
      { cluster: 'aws-cmh', runHash: 'abc12345' },
      { cluster: 'eos', runHash: 'deadbeef' },
      { cluster: '', runHash: 'abc999999999' },
    ]);
  });
});

describe('metrics page activation', () => {
  it('renders the run input when opened from the sidebar tab', () => {
    showTab('metrics');

    expect(document.querySelector('.metrics-page-run-input')).toBeTruthy();
    expect(document.getElementById('metrics-view')?.classList.contains('active')).toBe(true);
  });
});

describe('metrics page normalization and query', () => {
  it('normalizes missing payload sections', () => {
    const normalized = _metricsPageNormalizeMetrics({ series: { loss: [] } });

    expect(normalized.series).toEqual({ loss: [] });
    expect(normalized.scalars).toEqual({});
    expect(normalized.metadata).toEqual({});
  });

  it('matches run, metric, context, and metadata query fields', () => {
    const record = _metricsPageRecord({
      cluster: 'aws-cmh',
      runHash: 'abc12345',
      runName: 'demo_run',
      metadata: { model: 'synthetic/model' },
      key: 'eval/accuracy',
      kind: 'series',
      points: [{ value_num: 0.8, value: 0.8, step: 1, context: { split: 'eval' } }],
    });

    expect(_metricsPageMatchesQuery(record, 'aws-cmh')).toBe(true);
    expect(_metricsPageMatchesQuery(record, 'metric.name.contains("accuracy")')).toBe(true);
    expect(_metricsPageMatchesQuery(record, 'context.split == "eval"')).toBe(true);
    expect(_metricsPageMatchesQuery(record, 'metadata.model.contains("synthetic")')).toBe(true);
    expect(_metricsPageMatchesQuery(record, 'run.hash == "abc12345"')).toBe(true);
    expect(_metricsPageMatchesQuery(record, 'run.cluster == "aws-cmh"')).toBe(true);
    expect(_metricsPageMatchesQuery(record, '@abc123')).toBe(true);
    expect(_metricsPageMatchesQuery(record, 'run.hash == "missing"')).toBe(false);
  });
});

describe('metrics page URL and chart settings', () => {
  it('serializes selected runs and metrics into URL state', () => {
    history.replaceState(null, '', '#/metrics?runs=aws-cmh/abc12345&metrics=eval%2Faccuracy&q=accuracy');
    _metricsPageReadUrlState();

    const query = _metricsPageCurrentQuery();

    expect(query).toContain('runs=aws-cmh%2Fabc12345');
    expect(query).toContain('metrics=eval%252Faccuracy');
    expect(query).toContain('q=accuracy');
  });

  it('serializes and restores multi-chart workspace state', () => {
    const chart = _metricsPageNewChartConfig(['eval/accuracy']);
    _metricsPageApplySerializedState({
      runs: [{ cluster: 'aws-cmh', runHash: 'abc12345' }],
      selectedMetrics: ['eval/accuracy'],
      query: 'accuracy',
      chartsConfig: [{ ...chart, title: 'Accuracy panel' }],
    });

    const state = _metricsPageSerializeState();

    expect(state.chartsConfig).toHaveLength(1);
    expect(state.chartsConfig[0].title).toBe('Accuracy panel');
    expect(state.runs[0].runHash).toBe('abc12345');
  });

  it('prioritizes current runs in run recommendations', () => {
    _metricsPageApplySerializedState({
      runs: [{ cluster: 'aws-cmh', runHash: 'abc12345' }],
    });

    const suggestions = _metricsPageRecentRunSuggestions('abc');

    expect(suggestions[0].runHash).toBe('abc12345');
    expect(Array.from(suggestions[0].sources)).toContain('current');
  });

  it('inserts @run references from query suggestions', () => {
    document.body.innerHTML += `
      <input class="metrics-page-query-input" value="@ab">
      <div id="metrics-page-query-suggest"></div>
    `;
    _metricsPageRenderSuggestions('query', [{ cluster: 'aws-cmh', runHash: 'abc12345', runName: 'demo', sources: new Set(['current']) }]);

    _metricsPageApplySuggestion(0);

    expect((document.querySelector('.metrics-page-query-input') as HTMLInputElement).value).toBe('@abc12345');
  });

  it('uses calm chart defaults via shared chart options', () => {
    const options = _metricsPageChartOptions('step', 'linear');

    expect(options.animation).toBe(false);
    expect(options.plugins.legend.display).toBe(false);
    expect(options.elements.line.tension).toBe(0);
    expect(_metricsPageSmoothingLabel('0.5')).toBe('0.50');
  });

  it('encodes scalar metrics as chartable point traces', () => {
    _metricsPageApplySerializedState({
      selectedMetrics: ['final_accuracy'],
      chartsConfig: [{ id: 'c1', metricKeys: ['final_accuracy'], hiddenTraces: {} }],
    });
    _metricsPageSetRecordsForTest([
      _metricsPageRecord({
        cluster: 'aws-cmh',
        runHash: 'abc12345',
        runName: 'demo_run',
        metadata: {},
        key: 'final_accuracy',
        kind: 'scalars',
        points: [{ value_num: 0.8, value: 0.8, context: {} }],
      }),
    ]);

    const datasets = _metricsPageBuildChartDatasets(_metricsPageFirstChartForTest());

    expect(datasets[0].type).toBe('scatter');
    expect(datasets[0]._metricKind).toBe('scalar');
  });
});
