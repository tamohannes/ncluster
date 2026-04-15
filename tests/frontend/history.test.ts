/**
 * Unit tests for static/js/history.js grouping logic.
 *
 * Run with: npx vitest run tests/frontend/history.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest';
import { loadBrowserScripts } from './helpers';

function renderDom() {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>
    <div id="live-view"></div>
    <div id="history-view"></div>
    <div id="tab-live"></div>
    <div id="tab-history"></div>
    <select id="hist-cluster"><option value="all">All</option></select>
    <select id="hist-days"><option value="all">All time</option></select>
    <select id="hist-project"><option value="">All projects</option></select>
    <select id="hist-campaign"><option value="">All campaigns</option></select>
    <select id="hist-partition"><option value="">All partitions</option></select>
    <select id="hist-account"><option value="">All accounts</option></select>
    <input id="hist-search" value="" />
    <div id="hist-state-filters">
      <button class="hist-state-btn active" data-state="COMPLETED"></button>
      <button class="hist-state-btn active" data-state="FAILED"></button>
      <button class="hist-state-btn active" data-state="CANCELLED"></button>
      <button class="hist-state-btn active" data-state="TIMEOUT"></button>
      <button class="hist-state-btn active" data-state="RUNNING"></button>
      <button class="hist-state-btn active" data-state="PENDING"></button>
    </div>
    <table><tbody id="hist-body"></tbody></table>
    <div id="hist-pagination"></div>
    <div id="grid"></div>
    <div id="cd">30</div>
    <div id="s-running"></div>
    <div id="s-pending"></div>
    <div id="s-failed"></div>
    <div id="s-completed"></div>
    <div id="s-gpus"></div>
    <div id="s-clusters"></div>
    <div id="s-mounted"></div>
    <div id="mount-panel"></div>
    <div id="nav-toggle"></div>
    <div id="side-nav"></div>
    <div id="nav-splitter"></div>
    <div id="modal-overlay"></div>
    <div id="modal-title"></div>
    <div id="modal-subtitle"></div>
    <div id="content-path"></div>
    <div id="content-source"></div>
    <div id="modal-content"></div>
    <div id="tree-pane"></div>
    <div id="tree-splitter"></div>
    <div id="toasts"></div>
    <div id="stats-overlay"></div>
    <div id="stats-title"></div>
    <div id="stats-sub"></div>
    <div id="stats-body"></div>
    <div id="explorer-page"></div>
    <div id="exp-content"></div>
    <div id="exp-filename"></div>
    <div id="exp-cluster"></div>
    <div id="exp-source"></div>
    <div id="exp-pagination"></div>
    <div id="settings-overlay"></div>
    <div id="set-autorefresh"></div>
    <div id="set-refresh-interval"></div>
    <div id="set-hist-pagesize"></div>
    <div id="set-jsonl-limit"></div>
    <div id="set-jsonl-mode"></div>
    <div id="exp-tab-formatted"></div>
    <div id="exp-tab-raw"></div>
    <input id="cleanup-days" value="30" />
  `;
}

beforeAll(() => {
  renderDom();
  loadBrowserScripts(['utils.js', 'history.js']);
});

beforeEach(() => {
  renderDom();
  vi.restoreAllMocks();
});

declare const historyGroupKey: (r: any) => string;
declare const loadHistory: () => Promise<void>;

describe('historyGroupKey', () => {
  it('groups eval prefix with cluster', () => {
    const key = historyGroupKey({ cluster: 'cluster-a', job_name: 'eval-math_100' });
    expect(key).toBe('cluster-a:eval-math_100');
  });

  it('strips judge suffix', () => {
    const key = historyGroupKey({ cluster: 'test-cluster', job_name: 'eval-code-judge-rs0' });
    expect(key).toBe('test-cluster:eval-code');
  });

  it('returns misc for empty name', () => {
    const key = historyGroupKey({ cluster: 'c1', job_name: '' });
    expect(key).toBe('c1:misc');
  });
});

describe('history search rendering', () => {
  it('shows only run rows when search is active', async () => {
    (document.getElementById('hist-search') as HTMLInputElement).value = 'text-qwen35-no-tool-r7';
    (globalThis as any).fetch = vi.fn().mockResolvedValue({
      json: async () => ([
        {
          cluster: 'h100',
          job_id: '101',
          job_name: 'hle_text-qwen35-no-tool-r7',
          state: 'COMPLETED',
          depends_on: [],
          dep_details: [],
        },
        {
          cluster: 'h100',
          job_id: '102',
          job_name: 'hle_text-qwen35-no-tool-r7-judge-rs0',
          state: 'COMPLETED',
          depends_on: ['101'],
          dep_details: [],
        },
      ]),
    });

    await loadHistory();

    expect(document.querySelectorAll('#hist-body tr.group-head-row').length).toBe(1);
    expect(document.querySelectorAll('#hist-body tr.hist-compact').length).toBe(0);
    expect(document.getElementById('hist-body')?.textContent).toContain('2 jobs');
  });
});
