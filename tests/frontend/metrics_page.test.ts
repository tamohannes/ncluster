/**
 * Unit tests for static/js/metrics_page.js helper rendering.
 *
 * Run with: npx vitest run tests/frontend/metrics_page.test.ts
 */

import { beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { loadBrowserScripts } from './helpers';

declare const _mpControlsHtml: (matchingRecords: any[]) => string;
declare const _mpChartCardHtml: (group: any, idx: number) => string;
declare const _mpQLParse: (query: string) => any;
declare const _mpRecordMatches: (record: any, ast: any) => boolean;

function scalarRecord() {
  return {
    cluster: 'aws-cmh',
    runHash: 'abc12345',
    runName: 'scalar-run',
    key: 'accuracy',
    kind: 'scalars',
    numeric: true,
    points: [{ value_num: 0.91 }],
    context: {},
    contextSig: '{}',
    stats: { latestNum: 0.91 },
    metadata: {},
    params: {},
    tags: [],
  };
}

function seriesRecord() {
  return {
    cluster: 'aws-cmh',
    runHash: 'abc12345',
    runName: 'series-run',
    key: 'loss',
    kind: 'series',
    numeric: true,
    points: [{ step: 1, value_num: 0.5 }, { step: 2, value_num: 0.3 }],
    context: {},
    contextSig: '{}',
    stats: { latestNum: 0.3, firstStep: 1, lastStep: 2 },
    metadata: {},
    params: {},
    tags: [],
  };
}

function installState(records: any[], selectedMetrics: string[]) {
  (globalThis as any).eval(`
    Object.assign(_mpState, {
      records: ${JSON.stringify(records)},
      selectedMetrics: ${JSON.stringify(selectedMetrics)},
      runs: [],
      runData: {},
      hiddenRuns: {},
      hiddenTraces: {},
      grouping: { color: ['run.hash'], chart: ['metric.name'], pattern: [] },
      traceLabelFields: ['run.name'],
      metricsOpen: false,
      metricsFilter: '',
      align: 'step',
      yScale: 'linear',
      smoothing: 0.25,
      showRaw: false,
      ignoreOutliers: false,
      highlightMode: 'metric',
      xRange: null,
      yRange: null
    });
  `);
}

function renderDom() {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"aws-cmh":{"host":"login","gpu_type":"GB300"}}</script>
    <script id="username-data" type="text/plain">tester</script>
    <script id="team-data" type="text/plain">team</script>
    <div id="metrics-page"></div>
  `;
}

beforeAll(() => {
  renderDom();
  loadBrowserScripts(['utils.js', 'jobs.js', 'runs.js', 'metrics_page.js']);
});

beforeEach(() => {
  renderDom();
});

describe('metrics explorer controls', () => {
  it('hides series-only controls for scalar-only selections', () => {
    const rec = scalarRecord();
    installState([rec], ['accuracy']);

    const html = _mpControlsHtml([rec]);

    expect(html).not.toContain('Axes Alignment');
    expect(html).not.toContain('Smoothing');
    expect(html).not.toContain('Show original');
    expect(html).not.toContain('Ignore outliers');
    expect(html).not.toContain('Drag to zoom');
    expect(html).toContain('Axes Scale');
    expect(html).toContain('Highlight Mode');
  });

  it('shows series-only controls when a selected series is visible', () => {
    const rec = seriesRecord();
    installState([rec], ['loss']);

    const html = _mpControlsHtml([rec]);

    expect(html).toContain('Axes Alignment');
    expect(html).toContain('Smoothing');
    expect(html).toContain('Show original');
    expect(html).toContain('Ignore outliers');
    expect(html).toContain('Drag to zoom');
  });

  it('hides per-chart zoom reset for scalar-only chart cards', () => {
    const rec = scalarRecord();
    const html = _mpChartCardHtml({
      key: 'metric.name=accuracy',
      records: [rec],
      titleParts: [{ id: 'metric.name', value: 'accuracy' }],
    }, 0);

    expect(html).not.toContain('_mpResetZoom');
    expect(html).toContain('Export PNG');
  });

  it('matches run tags as array membership in AimQL', () => {
    const rec = { ...scalarRecord(), tags: ['smoke', 'malfunctioning'] };

    expect(_mpRecordMatches(rec, _mpQLParse('run.tags == "smoke"'))).toBe(true);
    expect(_mpRecordMatches(rec, _mpQLParse('"malfunctioning" in run.tags'))).toBe(true);
    expect(_mpRecordMatches(rec, _mpQLParse('run.tags == "production"'))).toBe(false);
  });
});
