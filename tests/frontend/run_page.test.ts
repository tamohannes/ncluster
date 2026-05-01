/**
 * Unit tests for the dedicated run metrics page helpers and routing.
 *
 * Run with: npx vitest run tests/frontend/run_page.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest';
import { loadBrowserScripts } from './helpers';

declare const _hashForView: (type: string, extra?: any) => string;
declare const _onHashChange: () => void;
declare const _runPageDownsample: (points: Array<{ x: number; y: number }>, maxPoints?: number) => Array<{ x: number; y: number }>;
declare const _runPageSmoothPoints: (points: Array<{ x: number; y: number }>, amount: number) => Array<{ x: number; y: number }>;
declare const _runPageNormalizeMetrics: (payload: any) => any;
declare const _runPageMatchesMetricQuery: (record: any, query: string) => boolean;
declare const _runPageFilterOutlierPoints: (points: Array<{ value_num: number }>) => Array<{ value_num: number }>;
declare const _runPageMergedContext: (points: Array<{ context?: any }>) => Record<string, any>;
declare const _runPageChartOptions: (xTitle: string, yScale: string, isBar?: boolean) => any;
declare const _runPageSmoothingLabel: (value: number | string) => string;
declare const _runPagePreviewSmoothingValue: (value: number | string) => void;
declare const _runPageLegendHtml: (title: string, items: any[], compact?: boolean) => string;
declare const _runPageToggleTrace: (id: string) => void;
declare const _runPageIsTraceHidden: (id: string) => boolean;
declare const _runPageSetTooltipMode: (mode: string) => void;
declare const runPageUrl: (cluster: string, runHash: string) => string;
declare const openRunPageFromRun: (cluster: string, runHash: string) => void;

beforeAll(() => {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"eos":{"host":"login","gpu_type":"h100"}}</script>
    <script id="username-data" type="text/plain">tester</script>
    <script id="team-data" type="text/plain">team</script>
  `;
  loadBrowserScripts(['utils.js', 'jobs.js', 'runs.js', 'run_page.js']);
});

beforeEach(() => {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"eos":{"host":"login","gpu_type":"h100"}}</script>
    <script id="username-data" type="text/plain">tester</script>
    <script id="team-data" type="text/plain">team</script>
    <div id="live-view"></div>
    <div id="history-view"></div>
    <div id="project-view"></div>
    <div id="run-page-view"><div id="run-page"></div></div>
    <div id="logbook-view"></div>
    <div id="clusters-view"></div>
    <div id="explorer-page"></div>
    <button id="tab-live"></button>
    <button id="tab-history"></button>
    <button id="tab-logbook"></button>
    <button id="tab-clusters"></button>
    <div id="topbar-tabs"></div>
  `;
  localStorage.clear();
  history.replaceState(null, '', '#/live');
});

describe('run page routing', () => {
  it('builds stable hash URLs for run tabs', () => {
    expect(_hashForView('run', { cluster: 'eos', runHash: 'deadbeef' })).toBe('#/run/eos/deadbeef');
    expect(runPageUrl('eos', 'deadbeef')).toBe('#/run/eos/deadbeef');
  });

  it('routes #/run/<cluster>/<run_hash> to openRunPage', () => {
    const spy = vi.fn();
    (globalThis as any).openRunPage = spy;
    history.replaceState(null, '', '#/run/eos/deadbeef?series=loss');

    _onHashChange();

    expect(spy).toHaveBeenCalledWith('eos', 'deadbeef');
  });

  it('modal helper opens the run page by cluster and hash', () => {
    const spy = vi.fn();
    (globalThis as any).openRunPage = spy;

    openRunPageFromRun('eos', 'cafebabe');

    expect(spy).toHaveBeenCalledWith('eos', 'cafebabe');
  });
});

describe('run page metrics helpers', () => {
  it('normalizes missing metrics payload sections', () => {
    const normalized = _runPageNormalizeMetrics({ metadata: { model: 'x' }, series: { loss: [] } });

    expect(normalized.metadata).toEqual({ model: 'x' });
    expect(normalized.series).toEqual({ loss: [] });
    expect(normalized.scalars).toEqual({});
    expect(normalized.scalar_latest).toEqual({});
  });

  it('downsamples dense series while preserving extrema', () => {
    const points = Array.from({ length: 100 }, (_, i) => ({ x: i, y: i }));
    points[44] = { x: 44, y: -1000 };
    points[45] = { x: 45, y: 1000 };

    const sampled = _runPageDownsample(points, 10);

    expect(sampled.length).toBeLessThan(points.length);
    expect(sampled.some(p => p.y === -1000)).toBe(true);
    expect(sampled.some(p => p.y === 1000)).toBe(true);
  });

  it('smooths values without changing point count or x positions', () => {
    const points = [
      { x: 1, y: 0 },
      { x: 2, y: 10 },
      { x: 3, y: 0 },
    ];

    const smoothed = _runPageSmoothPoints(points, 0.5);

    expect(smoothed).toHaveLength(points.length);
    expect(smoothed.map(p => p.x)).toEqual([1, 2, 3]);
    expect(smoothed[1].y).toBeGreaterThan(0);
    expect(smoothed[1].y).toBeLessThan(10);
  });

  it('matches Aim-style metric queries over metric, context, and metadata', () => {
    const record = {
      key: 'eval/accuracy',
      kind: 'series',
      points: [{ context: { split: 'eval', seed: 1 } }],
      stats: { latest: 0.84, min: 0.2, max: 0.84 },
      context: { split: 'eval', seed: 1 },
      contexts: ['split:eval', 'seed:1'],
      metadata: { model: 'synthetic/metrics-page-demo' },
    };

    expect(_runPageMatchesMetricQuery(record, 'metric.name.contains("accuracy")')).toBe(true);
    expect(_runPageMatchesMetricQuery(record, 'metric.kind == "series" and context.split == "eval"')).toBe(true);
    expect(_runPageMatchesMetricQuery(record, 'metadata.model.contains("synthetic")')).toBe(true);
    expect(_runPageMatchesMetricQuery(record, 'context.split == "train"')).toBe(false);
  });

  it('merges metric contexts and preserves distinct values', () => {
    const context = _runPageMergedContext([
      { context: { split: 'eval', seed: 1 } },
      { context: { split: 'eval', seed: 2 } },
    ]);

    expect(context.split).toBe('eval');
    expect(context.seed).toEqual([1, 2]);
  });

  it('filters obvious outliers for modifier toggles', () => {
    const points = [1, 2, 2, 3, 3, 3, 4, 4, 100].map(value_num => ({ value_num }));

    const filtered = _runPageFilterOutlierPoints(points);

    expect(filtered.some(p => p.value_num === 100)).toBe(false);
    expect(filtered.length).toBe(8);
  });

  it('uses calm Aim-like chart defaults', () => {
    const options = _runPageChartOptions('step', 'linear');

    expect(options.animation).toBe(false);
    expect(options.plugins.legend.display).toBe(false);
    expect(options.elements.line.tension).toBe(0);
    expect(options.interaction.mode).toBe('index');

    _runPageSetTooltipMode('nearest');
    const nearestOptions = _runPageChartOptions('step', 'linear');
    expect(nearestOptions.interaction.mode).toBe('nearest');
    expect(nearestOptions.interaction.intersect).toBe(true);
  });

  it('previews smoothing value without requiring a panel re-render', () => {
    document.body.innerHTML += '<span id="run-page-smoothing-value"></span>';

    _runPagePreviewSmoothingValue(0.5);

    expect(document.getElementById('run-page-smoothing-value')?.textContent).toBe('0.50');
    expect(_runPageSmoothingLabel('0.95')).toBe('0.95');
  });

  it('renders Aim-style sidebar legend rows and toggles hidden traces', () => {
    const html = _runPageLegendHtml('Legends', [{
      id: 'series:loss',
      label: 'train/loss',
      color: '#22c55e',
      value: '0.42',
      meta: 'split:train',
      hidden: false,
    }]);

    expect(html).toContain('run-page-legend-item');
    expect(html).toContain('train/loss');
    expect(html).toContain('0.42');

    _runPageToggleTrace('series:loss');
    expect(_runPageIsTraceHidden('series:loss')).toBe(true);
  });
});
