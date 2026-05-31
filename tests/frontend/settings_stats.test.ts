/**
 * Unit tests for static/js/settings.js stats modal behavior.
 *
 * Run with: npx vitest run tests/frontend/settings_stats.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach, afterEach, vi } from 'vitest';
import { loadBrowserScripts } from './helpers';

const chartInstances: any[] = [];

function renderDom() {
  document.body.innerHTML = `
    <div id="favicon"></div>
    <div id="stats-overlay"></div>
    <div id="stats-title"></div>
    <div id="stats-sub"></div>
    <div id="stats-body"></div>
    <div><span id="cd"></span></div>
    <input id="set-autorefresh" type="checkbox" />
    <input id="set-refresh-interval" value="30" />
    <input id="set-hist-pagesize" value="50" />
    <input id="set-jsonl-limit" value="50" />
    <select id="set-jsonl-mode"><option value="first">first</option></select>
  `;
  document.documentElement.style.setProperty('--text', '#111');
  document.documentElement.style.setProperty('--muted', '#777');
  document.documentElement.style.setProperty('--border', '#ddd');
  document.documentElement.style.setProperty('--amber', '#f59e0b');
}

beforeAll(() => {
  renderDom();
  (globalThis as any).allData = {};
  (globalThis as any)._customMetricsEnabled = false;
  (globalThis as any).isCustomMetricsEnabled = () => false;
  (globalThis as any).setupTreeResizer = () => {};
  (globalThis as any).setupSidebarResizer = () => {};
  (globalThis as any).applySidebarState = () => {};
  (globalThis as any).fetchAll = () => {};
  (globalThis as any).loadProjectButtons = () => {};
  (globalThis as any)._restoreTabArray = () => {};
  (globalThis as any)._renderAppTabs = () => {};
  (globalThis as any)._onHashChange = () => {};
  (globalThis as any)._restoreTabs = () => true;
  (globalThis as any).showTab = () => {};
  (globalThis as any).HIST_GROUPS_PER_PAGE = 50;
  (globalThis as any).refreshIntervalSec = 0;
  (globalThis as any).cdTimer = null;
  (globalThis as any).countdown = 0;
  (globalThis as any).Chart = vi.fn().mockImplementation(function ChartStub(this: any, _ctx: any, config: any) {
    this.config = config;
    this.destroy = vi.fn();
    chartInstances.push(this);
  });
  loadBrowserScripts(['settings.js']);
});

beforeEach(() => {
  renderDom();
  chartInstances.length = 0;
  vi.restoreAllMocks();
  vi.useFakeTimers();
  (globalThis as any).Chart = vi.fn().mockImplementation(function ChartStub(this: any, _ctx: any, config: any) {
    this.config = config;
    this.destroy = vi.fn();
    chartInstances.push(this);
  });
  (globalThis as any).fetch = vi.fn();
});

afterEach(() => {
  closeStatsDirect();
  vi.useRealTimers();
});

declare const openStats: (cluster: string, jobId: string, jobName?: string) => Promise<void>;
declare const openRunStats: (cluster: string, runRef: string, runName?: string) => Promise<void>;
declare const closeStatsDirect: () => void;

function latestGpuUtilChart() {
  return chartInstances
    .filter(c => c.config?.options?.plugins?.title?.text === 'GPU Utilization')
    .at(-1);
}

function latestRunGpuUtilChart() {
  return chartInstances
    .filter(c => c.config?.options?.plugins?.title?.text === 'Run GPU Utilization (3-min avg/job)')
    .at(-1);
}

describe('stats modal live charting', () => {
  it('appends live GPU samples while the stats modal is open', async () => {
    (globalThis as any).fetch = vi.fn()
      // Phase 1: instant cached paint.
      .mockResolvedValueOnce({ json: async () => ({
        status: 'ok',
        state: 'RUNNING',
        snapshots: [],
        gpus: [{ index: '0', util: '50%', mem: '100/200 MiB' }],
      }) })
      // Phase 2: immediate background refresh with the live probe.
      .mockResolvedValueOnce({ json: async () => ({
        status: 'ok',
        state: 'RUNNING',
        snapshots: [],
        gpus: [{ index: '0', util: '80%', mem: '150/200 MiB' }],
      }) })
      // Periodic live tick.
      .mockResolvedValueOnce({ json: async () => ({
        status: 'ok',
        state: 'RUNNING',
        snapshots: [],
        gpus: [{ index: '0', util: '90%', mem: '160/200 MiB' }],
      }) });

    await openStats('eos', '42', 'job');
    // Cached-first: a ?cached=1 paint followed by the background refresh.
    expect(String((globalThis as any).fetch.mock.calls[0][0])).toContain('cached=1');
    expect((globalThis as any).fetch).toHaveBeenCalledTimes(2);
    expect(latestGpuUtilChart().config.data.datasets[0].data).toEqual([80]);

    await vi.advanceTimersByTimeAsync(179000);
    await Promise.resolve();
    expect((globalThis as any).fetch).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(1000);
    await Promise.resolve();

    expect((globalThis as any).fetch).toHaveBeenCalledTimes(3);
    expect(latestGpuUtilChart().config.data.datasets[0].data).toEqual([80, 90]);
  });

  it('does not duplicate a fresh persisted snapshot with an immediate live sample', async () => {
    const ts = new Date().toISOString();
    (globalThis as any).fetch = vi.fn()
      .mockResolvedValue({ json: async () => ({
        status: 'ok',
        state: 'RUNNING',
        snapshots: [{ ts, per_gpu: [{ index: '0', util: '75%', mem: '100/200 MiB' }] }],
        gpus: [{ index: '0', util: '75%', mem: '100/200 MiB' }],
      }) });

    await openStats('eos', '42', 'job');

    expect(latestGpuUtilChart().config.data.datasets[0].data).toEqual([75]);
  });

  it('stops polling when the stats modal closes', async () => {
    (globalThis as any).fetch = vi.fn()
      .mockResolvedValue({ json: async () => ({
        status: 'ok',
        state: 'RUNNING',
        snapshots: [],
        gpus: [{ index: '0', util: '50%', mem: '100/200 MiB' }],
      }) });

    await openStats('eos', '42', 'job');
    // Cached paint + background refresh = two fetches before any live tick.
    expect((globalThis as any).fetch).toHaveBeenCalledTimes(2);
    closeStatsDirect();
    await vi.advanceTimersByTimeAsync(180000);

    expect((globalThis as any).fetch).toHaveBeenCalledTimes(2);
  });

  it('uses endpoint-safe chart options and renders paired GPU charts', async () => {
    (globalThis as any).fetch = vi.fn()
      .mockResolvedValue({ json: async () => ({
        status: 'ok',
        state: 'RUNNING',
        snapshots: [],
        gpus: [
          { index: '0', util: '100%', mem: '180/200 MiB' },
          { index: '1', util: '98%', mem: '170/200 MiB' },
        ],
      }) });

    await openStats('eos', '42', 'job');

    const chartsGrid = document.querySelector('.stats-charts');
    expect(chartsGrid).not.toBeNull();
    expect(document.querySelectorAll('.stats-chart-wrap')).toHaveLength(2);

    const chart = latestGpuUtilChart();
    expect(chart.config.options.layout.padding.right).toBeGreaterThanOrEqual(18);
    expect(chart.config.options.scales.x.offset).toBe(true);
    expect(chart.config.options.scales.y.max).toBe(100);
    expect(chart.config.options.scales.y.ticks.includeBounds).toBe(true);
    expect(chart.config.data.datasets[0].clip).toBe(false);
    expect(chart.config.data.datasets[0].pointRadius).toBeGreaterThan(0);
  });

  it('renders run-level GPU utilization with one colored line per job', async () => {
    (globalThis as any).fetch = vi.fn()
      .mockResolvedValue({ json: async () => ({
        status: 'ok',
        run: { root_job_id: 'root42', run_name: 'run-name' },
        jobs: [
          {
            job_id: '100',
            name: 'run-name-server',
            state: 'RUNNING',
            avg_gpu_util: 80,
            latest_gpu_util: 90,
            sample_count: 2,
            snapshots: [
              { ts: '2026-05-29T18:00:00', gpu_util: 70, gpu_mem_used: 1000, per_gpu: [] },
              { ts: '2026-05-29T18:01:00', gpu_util: 90, gpu_mem_used: 1200, per_gpu: [] },
              { ts: '2026-05-29T18:04:00', gpu_util: 96, gpu_mem_used: 1400, per_gpu: [] },
            ],
          },
          {
            job_id: '101',
            name: 'run-name-client',
            state: 'RUNNING',
            avg_gpu_util: 40,
            latest_gpu_util: 45,
            sample_count: 2,
            snapshots: [
              { ts: '2026-05-29T18:00:00', gpu_util: 35, gpu_mem_used: 800, per_gpu: [] },
              { ts: '2026-05-29T18:02:00', gpu_util: 45, gpu_mem_used: 900, per_gpu: [] },
              { ts: '2026-05-29T18:04:00', gpu_util: 55, gpu_mem_used: 1000, per_gpu: [] },
            ],
          },
        ],
      }) });

    await openRunStats('eos', 'root42', 'run-name');

    const chart = latestRunGpuUtilChart();
    expect(chart.config.data.datasets).toHaveLength(2);
    expect(chart.config.data.datasets[0].label).toContain('100');
    expect(chart.config.data.labels).toHaveLength(2);
    expect(chart.config.data.datasets[0].data).toEqual([80, 96]);
    expect(chart.config.data.datasets[1].data).toEqual([40, 55]);
    expect(chart.config.options.scales.y.max).toBe(100);
    expect(chart.config.data.datasets[0].borderColor)
      .not.toBe(chart.config.data.datasets[1].borderColor);
    const swatches = Array.from(document.querySelectorAll('.run-stats-color-swatch')) as HTMLElement[];
    expect(swatches).toHaveLength(2);
    expect(swatches[0].style.getPropertyValue('--run-stats-color'))
      .toBe(chart.config.data.datasets[0].borderColor);
    expect(swatches[1].style.getPropertyValue('--run-stats-color'))
      .toBe(chart.config.data.datasets[1].borderColor);
    expect(document.getElementById('stats-body')?.textContent).toContain('Run avg GPU');
  });
});
