import { beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { loadBrowserScripts } from './helpers';

declare const _renderRunBody: (run: any, cluster: string) => void;
declare const _renderRunTagsEditor: (runId: number, tags: string[], context?: string) => string;
declare const _removeRunTag: (runId: number, context: string, tag: string) => Promise<void>;
declare const setRunTagDefinitions: (defs: any[]) => any;
declare const openRunInfo: (cluster: string, rootJobId: string, runName: string, cancelKey?: string) => Promise<void>;

beforeAll(() => {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"dfw":{"host":"x","gpu_type":"H100"}}</script>
    <div id="run-overlay"></div>
    <div id="run-title"></div>
    <div id="run-subtitle"></div>
    <div id="run-body"></div>
    <div id="run-mark-slot"></div>
    <div id="run-page-action-slot"></div>
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
    <div id="run-page-action-slot"></div>
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

describe('read-only job-history run popups', () => {
  it('hide run edit actions when there is no persistent run id', () => {
    (globalThis as any).fetch = async () => ({
      async json() {
        return { status: 'error', error: 'Run not found' };
      },
    });

    _renderRunBody({
      id: null,
      read_only: true,
      root_job_id: '4148623',
      run_name: 'mpsf_v15_v1-5-hle-phy-nem120b-turn1-test5-r3',
      source: 'job_history',
      params: {},
      metadata: {},
      jobs: [
        {
          job_id: '4148623',
          job_name: 'mpsf_v15_v1-5-hle-phy-nem120b-turn1-test5-r3-path_server',
          state: 'RUNNING',
          nodes: '1',
          gres: 'gpu:4',
        },
      ],
    }, 'dfw');

    expect(document.querySelector('#run-mark-btn')).toBeNull();
    expect(document.querySelector('#run-notes-textarea')).toBeNull();
    expect(document.querySelector('.run-tags-editor')).toBeNull();
    expect(document.querySelector('.run-delete-btn')).toBeNull();
    const actionLabels = Array.from(document.querySelectorAll('#run-page-action-slot .run-page-action-btn'))
      .map((el) => el.textContent);
    expect(actionLabels).toEqual(['Log']);
    expect(document.getElementById('run-body')!.textContent).toContain('No SDK metadata is attached');
  });
});

describe('run popup settings tab', () => {
  it('moves tag and delete controls out of the overview/header', () => {
    _renderRunBody({
      id: 7,
      root_job_id: 'root123',
      run_hash: 'abc12345',
      run_name: 'editable-run',
      params: {},
      metadata: {},
      jobs: [
        {
          job_id: '100',
          state: 'COMPLETED',
        },
      ],
      malfunctioned: true,
    }, 'dfw');

    expect(document.getElementById('run-tab-btn-settings')?.textContent).toBe('Settings');
    expect(document.querySelector('#run-tab-overview .run-tags-editor')).toBeNull();
    const tagEditor = document.querySelector('#run-tab-settings .run-tags-editor');
    expect(tagEditor).toBeTruthy();
    expect(tagEditor?.textContent).toContain('malfunctioning');
    expect(document.querySelector('#run-page-action-slot .run-delete-btn')).toBeNull();
    const actionLabels = Array.from(document.querySelectorAll('#run-page-action-slot .run-page-action-btn'))
      .map((el) => el.textContent);
    expect(actionLabels).toEqual(['Log', 'Run page']);
    expect(document.querySelector('#run-tab-settings .run-delete-btn')?.textContent).toContain('Delete run');
  });

  it('removes tags through the rendered chip control', async () => {
    let patchedTags: string[] = [];
    (globalThis as any).fetch = async (_url: string, init: any) => {
      patchedTags = JSON.parse(init.body).tags;
      return {
        ok: true,
        async json() {
          return { status: 'ok' };
        },
      };
    };

    document.body.innerHTML = _renderRunTagsEditor(7, ['smoke', 'malfunctioning'], 'popup');

    const removeSmoke = document.querySelector('.run-tag-remove') as HTMLButtonElement | null;
    expect(removeSmoke?.getAttribute('onclick')).toBe("_removeRunTag(7,'popup','smoke')");

    await _removeRunTag(7, 'popup', 'smoke');

    expect(patchedTags).toEqual(['malfunctioning']);
    expect(document.getElementById('run-tags-list-popup-7')?.textContent).not.toContain('smoke');
    expect(document.getElementById('run-tags-list-popup-7')?.textContent).toContain('malfunctioning');
  });

  it('uses shared tag colors on rendered chips', () => {
    setRunTagDefinitions([{ tag: 'smoke', color: '#123abc' }]);

    document.body.innerHTML = _renderRunTagsEditor(8, ['smoke'], 'popup');

    const chip = document.querySelector('[data-run-tag="smoke"]') as HTMLElement | null;
    expect(chip?.getAttribute('style')).toContain('--run-tag-color:#123abc');
  });
});
