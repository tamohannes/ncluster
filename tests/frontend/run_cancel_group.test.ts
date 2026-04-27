import { beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { loadBrowserScripts } from './helpers';

declare const _renderRunBody: (run: any, cluster: string) => void;
declare const openRunInfo: (cluster: string, rootJobId: string, runName: string, cancelKey?: string) => Promise<void>;

beforeAll(() => {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"dfw":{"host":"x","gpu_type":"H100"}}</script>
    <div id="run-overlay"></div>
    <div id="run-title"></div>
    <div id="run-subtitle"></div>
    <div id="run-body"></div>
    <div id="run-mark-slot"></div>
  `;
  loadBrowserScripts(['utils.js', 'jobs.js', 'runs.js']);
});

beforeEach(() => {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"dfw":{"host":"x","gpu_type":"H100"}}</script>
    <div id="run-overlay"></div>
    <div id="run-title"></div>
    <div id="run-subtitle"></div>
    <div id="run-body"></div>
    <div id="run-mark-slot"></div>
  `;
  // Minimal stubs used by the cancel button rendering path.
  (globalThis as any).toast = () => {};
  (globalThis as any).refreshCluster = () => {};
  (globalThis as any)._doCancelGroup = () => {};
  (window as any)._runGroupJobIds = {};
});

describe('run cancel button uses merged board group ids', () => {
  it('prefers explicit board-group cancel ids over backend subset jobs', async () => {
    (window as any)._runGroupJobIds['dfw:root123'] = ['100', '101', '102', '103'];
    (globalThis as any).fetch = async () => ({
      async json() {
        return {
          status: 'ok',
          run: {
            id: 1,
            root_job_id: 'root123',
            run_name: 'hle_text_qwen35-v9-no-tool-r1-hle',
            jobs: [
              { job_id: '100', state: 'CANCELLED' },
              { job_id: '101', state: 'FAILED' },
            ],
          },
        };
      },
    });

    await openRunInfo('dfw', 'root123', 'hle_text_qwen35-v9-no-tool-r1-hle', 'dfw:root123');

    const btn = document.querySelector('.cancel-run-btn') as HTMLButtonElement | null;
    expect(btn).toBeTruthy();
    const onclick = btn!.getAttribute('onclick') || '';
    expect(onclick).toContain('100');
    expect(onclick).toContain('101');
    expect(onclick).toContain('102');
    expect(onclick).toContain('103');
  });
});
