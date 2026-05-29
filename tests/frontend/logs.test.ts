/**
 * Unit tests for static/js/logs.js rendering functions.
 *
 * Run with: npx vitest run tests/frontend/logs.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach, afterEach, vi } from 'vitest';
import { loadBrowserScripts } from './helpers';

function renderDom() {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>
    <div id="modal-overlay"></div>
    <div id="modal-title"></div>
    <div id="modal-subtitle"></div>
    <div id="content-path"></div>
    <div id="content-source"></div>
    <button id="live-toggle"></button>
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
  `;
}

beforeAll(() => {
  // Provide DOM elements that logs.js and its dependencies need
  renderDom();
  loadBrowserScripts(['utils.js', 'crash_detect.js', 'logs.js']);
});

beforeEach(() => {
  renderDom();
  vi.restoreAllMocks();
});

afterEach(() => {
  vi.useRealTimers();
});

declare const escapeHtml: (s: string) => string;
declare const renderLogWithHighlights: (raw: string) => string;
declare const renderJsonWithSyntax: (raw: string) => string;
declare const jsonlRecordSummary: (obj: any) => string;
declare const guessIcon: (name: string) => string;
declare const markdownToHtml: (raw: string) => string;
declare const renderFileContentByType: (path: string, raw: string) => { cls: string; html: string };
declare const _popupShouldLoadFully: (path: string) => boolean;
declare const renderJsonlLazyViewer: (
  data: any,
  filePath: string,
  opts?: { containerId?: string; cluster?: string; jobId?: string }
) => string;
declare const openLog: (cluster: string, jobId: string, jobName?: string, force?: boolean) => Promise<void>;
declare const openDir: (cluster: string, dirPath: string, label?: string) => Promise<void>;
declare const toggleLive: () => void;

describe('escapeHtml', () => {
  it('escapes angle brackets', () => {
    expect(escapeHtml('<script>')).toBe('&lt;script&gt;');
  });
  it('escapes ampersand', () => {
    expect(escapeHtml('a & b')).toBe('a &amp; b');
  });
  it('handles null/undefined', () => {
    expect(escapeHtml(null as any)).toBe('');
  });
});

describe('renderLogWithHighlights', () => {
  it('wraps lines in log-line divs', () => {
    const result = renderLogWithHighlights('line1\nline2');
    expect(result).toContain('log-line');
    expect(result).toContain('line1');
  });
  it('adds error class for typed exception lines', () => {
    const result = renderLogWithHighlights('ValueError: something broke');
    expect(result).toContain('error');
  });
  it('adds error class for traceback header', () => {
    const result = renderLogWithHighlights('Traceback (most recent call last):');
    expect(result).toContain('error');
  });
  it('adds error class for CUDA error', () => {
    const result = renderLogWithHighlights('CUDA out of memory');
    expect(result).toContain('error');
  });
  it('adds error class for srun error', () => {
    const result = renderLogWithHighlights('srun: error: task failed');
    expect(result).toContain('error');
  });
  it('does NOT add error class for bare word "error" in text', () => {
    const result = renderLogWithHighlights('Several errors have been corrected');
    expect(result).not.toContain('class="log-line error"');
    expect(result).not.toContain('class="log-line trace error"');
  });
  it('does NOT add error class for "error" in prose context', () => {
    const result = renderLogWithHighlights('the mathematical notation has been updated. Several errors have been corrected and the tables have been recomputed.');
    expect(result).not.toContain('class="log-line error"');
    expect(result).not.toContain('class="log-line trace error"');
  });
  it('adds warn class for warning lines', () => {
    const result = renderLogWithHighlights('Warning: heads up');
    expect(result).toContain('warn');
  });
});

describe('renderJsonWithSyntax', () => {
  it('highlights keys', () => {
    const result = renderJsonWithSyntax('{"key": "value"}');
    expect(result).toContain('json-key');
  });
  it('highlights strings', () => {
    const result = renderJsonWithSyntax('{"k": "hello"}');
    expect(result).toContain('json-string');
  });
  it('highlights numbers', () => {
    const result = renderJsonWithSyntax('{"k": 42}');
    expect(result).toContain('json-number');
  });
  it('highlights booleans', () => {
    const result = renderJsonWithSyntax('{"k": true}');
    expect(result).toContain('json-boolean');
  });
});

describe('jsonlRecordSummary', () => {
  it('picks preferred keys', () => {
    const result = jsonlRecordSummary({ id: '1', task: 'math', extra: 'x' });
    expect(result).toContain('id=1');
    expect(result).toContain('task=math');
  });
  it('handles non-object', () => {
    expect(jsonlRecordSummary([1, 2, 3])).toContain('non-object');
  });
  it('falls back to first keys', () => {
    const result = jsonlRecordSummary({ foo: 'bar', baz: 'qux' });
    expect(result).toContain('foo=bar');
  });
});

describe('guessIcon', () => {
  it('log files get clipboard icon', () => expect(guessIcon('output.log')).toBe('📋'));
  it('json files get braces', () => expect(guessIcon('data.json')).toBe('{}'));
  it('md files get M', () => expect(guessIcon('README.md')).toBe('Ⓜ'));
  it('sh files get gear', () => expect(guessIcon('run.sh')).toBe('⚙'));
  it('unknown gets page', () => expect(guessIcon('data.bin')).toBe('📄'));
});

describe('markdownToHtml', () => {
  it('converts headings', () => {
    const result = markdownToHtml('# Hello');
    expect(result).toContain('<h1>');
  });
  it('converts list items', () => {
    const result = markdownToHtml('- item 1\n- item 2');
    expect(result).toContain('<li>');
  });
  it('converts code blocks', () => {
    const result = markdownToHtml('```\ncode\n```');
    expect(result).toContain('<pre><code>');
  });
  it('renders markdown and LaTeX bold inside tables', () => {
    const result = markdownToHtml('| Metric | Value |\n|---|---:|\n| A | **52.2** |\n| B | \\textbf{54.0} |');
    expect(result).toContain('<strong>52.2</strong>');
    expect(result).toContain('<strong>54.0</strong>');
    expect(result).not.toContain('\\textbf');
  });
  it('renders LaTeX small caps inside tables', () => {
    const result = markdownToHtml('| Setup |\n|---|\n| All \\textsc{Artsiv} findings |');
    expect(result).toContain('<span class="md-small-caps">Artsiv</span>');
    expect(result).not.toContain('\\textsc');
  });
  it('keeps pipes inside inline code spans from splitting cells', () => {
    const src = '| Action | Format |\n|---|---|\n| Python tool call | `<\\|start\\|>assistant<\\|channel\\|>final` |';
    const result = markdownToHtml(src);
    expect(result).toContain('<code class="md-inline-code">&lt;|start|&gt;assistant&lt;|channel|&gt;final</code>');
    expect((result.match(/<td/g) || []).length).toBe(2);
  });
  it('treats \\| as a literal pipe outside code spans', () => {
    const result = markdownToHtml('| A | B |\n|---|---|\n| foo \\| bar | baz |');
    expect(result).toContain('<td>foo | bar</td>');
    expect(result).toContain('<td>baz</td>');
  });
});

describe('_popupShouldLoadFully', () => {
  it('matches metrics.json at any depth', () => {
    expect(_popupShouldLoadFully('metrics.json')).toBe(true);
    expect(_popupShouldLoadFully('/a/b/metrics.json')).toBe(true);
    expect(_popupShouldLoadFully('/eval-results/gpqa/metrics.json')).toBe(true);
  });
  it('is case-insensitive on the filename', () => {
    expect(_popupShouldLoadFully('/x/Metrics.JSON')).toBe(true);
  });
  it('does not match other json files', () => {
    expect(_popupShouldLoadFully('/x/output.json')).toBe(false);
    expect(_popupShouldLoadFully('/x/config.json')).toBe(false);
  });
  it('does not match metrics-like names that are not metrics.json', () => {
    expect(_popupShouldLoadFully('/x/metrics.jsonl')).toBe(false);
    expect(_popupShouldLoadFully('/x/my_metrics.json')).toBe(false);
    expect(_popupShouldLoadFully('/x/metrics.json.bak')).toBe(false);
  });
  it('handles empty/missing path', () => {
    expect(_popupShouldLoadFully('')).toBe(false);
    expect(_popupShouldLoadFully(undefined as any)).toBe(false);
  });
});

describe('renderFileContentByType', () => {
  it('renders json files with syntax highlighting', () => {
    const { cls, html } = renderFileContentByType('data.json', '{"key": 1}');
    expect(cls).toContain('json-view');
    expect(html).toContain('json-key');
  });
  it('renders md files as markdown', () => {
    const { cls } = renderFileContentByType('README.md', '# Title');
    expect(cls).toContain('md-view');
  });
  it('renders log files with highlights', () => {
    const { html } = renderFileContentByType('output.log', 'some log line');
    expect(html).toContain('log-line');
  });
});

describe('live log viewer defaults', () => {
  it('keeps live off by default until the user enables it', async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        json: async () => ({
          files: [{ path: '/remote/output.log', label: 'main log' }],
          dirs: [],
          first_content: 'hello from mount',
          first_source: 'mount',
          first_resolved_path: '/mnt/output.log',
          first_hash: 'abc123',
        }),
      })
      .mockResolvedValue({
        json: async () => ({
          status: 'ok',
          content: 'updated content',
          source: 'mount',
          resolved_path: '/mnt/output.log',
          hash: 'def456',
        }),
      });

    (globalThis as any).fetchWithTimeout = fetchMock;

    await openLog('h100', '123', 'demo job');
    expect(document.getElementById('live-toggle')?.classList.contains('active')).toBe(false);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(2500);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    toggleLive();
    expect(document.getElementById('live-toggle')?.classList.contains('active')).toBe(true);

    await vi.advanceTimersByTimeAsync(2500);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(String(fetchMock.mock.calls[1][0])).toContain('/api/log/h100/123');
    expect(String(fetchMock.mock.calls[1][0])).toContain('force=1');
  });

  it('shows a browse hint when discovery finds directories but no direct logs', async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        json: async () => ({
          files: [],
          dirs: [{ label: 'nemo-run', path: '/remote/nemo-run/demo' }],
        }),
      })
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          entries: [
            { name: 'nemo-run_sbatch.sh', path: '/remote/nemo-run/demo/nemo-run_sbatch.sh', is_dir: false, size: 10 },
          ],
        }),
      });

    (globalThis as any).fetchWithTimeout = fetchMock;

    await openLog('h100', '357999', 'demo job');

    expect(document.getElementById('modal-content')?.textContent).toContain('No direct log file found');
    expect(document.getElementById('content-path')?.textContent).toBe('/remote/nemo-run/demo');
    expect(document.getElementById('tree-pane')?.textContent).toContain('nemo-run_sbatch.sh');
    expect(String(fetchMock.mock.calls[1][0])).toContain('/api/ls/h100');
  });

  it('uses the shared run directory as the only tree root when direct logs exist', async () => {
    const rootDir = '/remote/nemo-run/demo';
    const leafDir = `${rootDir}/demo_1`;
    const logDir = `${leafDir}/nemo-run`;
    const firstLog = `${logDir}/main_366675_srun.log`;
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        json: async () => ({
          files: [
            { label: 'main output', path: firstLog },
          ],
          dirs: [
            { label: 'demo', path: rootDir },
            { label: 'experiment output', path: '/remote/experiments/demo' },
          ],
          first_content: 'hello log',
          first_source: 'ssh',
          first_resolved_path: firstLog,
        }),
      })
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          entries: [
            { name: 'demo_1', path: leafDir, is_dir: true, size: 10 },
          ],
        }),
      })
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          entries: [
            { name: 'nemo-run', path: logDir, is_dir: true, size: 10 },
          ],
        }),
      })
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          entries: [
            { name: 'main_366675_srun.log', path: firstLog, is_dir: false, size: 100 },
          ],
        }),
      });

    (globalThis as any).fetchWithTimeout = fetchMock;

    await openLog('h100', '366675', 'demo job');

    const treeText = document.getElementById('tree-pane')?.textContent || '';
    expect(treeText).toContain('demo_1');
    expect(treeText).toContain('main_366675_srun.log');
    expect(treeText).not.toContain('logs');
    expect(treeText).not.toContain('main output');
    expect(treeText).not.toContain('experiment output');
    expect(document.getElementById('content-path')?.textContent).toBe(firstLog);
    expect(document.querySelector('.tree-item.active')?.textContent).toContain('main_366675_srun.log');
    expect(String(fetchMock.mock.calls[1][0])).toContain(encodeURIComponent(rootDir));
  });

  it('descends from a NeMo launch wrapper directory into useful logs', async () => {
    const launchDir = '/remote/nemo-run/demo/demo_1';
    const logDir = `${launchDir}/nemo-run`;
    const mainLog = `${logDir}/main_demo_123_srun.log`;
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          source: 'ssh',
          entries: [
            { name: '__main__.py', path: `${launchDir}/__main__.py`, is_dir: false, size: 11000 },
            { name: '_CONFIG', path: `${launchDir}/_CONFIG`, is_dir: false, size: 1000 },
            { name: '_TASKS', path: `${launchDir}/_TASKS`, is_dir: false, size: 9000 },
            { name: '_VERSION', path: `${launchDir}/_VERSION`, is_dir: false, size: 14 },
            { name: 'nemo-run_sbatch.sh', path: `${launchDir}/nemo-run_sbatch.sh`, is_dir: false, size: 5000 },
          ],
        }),
      })
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          source: 'ssh',
          entries: [
            { name: 'code', path: `${logDir}/code`, is_dir: true, size: 0 },
            { name: 'main_demo_123_srun.log', path: mainLog, is_dir: false, size: 8540 },
            { name: 'server_demo_123_srun.log', path: `${logDir}/server_demo_123_srun.log`, is_dir: false, size: 62447 },
            { name: 'demo_123_sbatch.log', path: `${logDir}/demo_123_sbatch.log`, is_dir: false, size: 15801 },
          ],
        }),
      })
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          content: 'real log content',
          source: 'ssh',
          resolved_path: mainLog,
          total_pages: 1,
          page: 0,
        }),
      });

    (globalThis as any).fetchWithTimeout = fetchMock;

    await openDir('h100', launchDir, 'demo job');

    const treeText = document.getElementById('tree-pane')?.textContent || '';
    expect(treeText).toContain('main_demo_123_srun.log');
    expect(treeText).not.toContain('__main__.py');
    expect(treeText).not.toContain('_CONFIG');
    expect(document.getElementById('content-path')?.textContent).toBe(mainLog);
    expect(document.querySelector('.tree-item.active')?.textContent).toContain('main_demo_123_srun.log');
    expect(document.getElementById('modal-content')?.textContent).toContain('real log content');
    expect(String(fetchMock.mock.calls[1][0])).toContain(encodeURIComponent(logDir));
    expect(String(fetchMock.mock.calls[2][0])).toContain('/api/log_full/h100/__dir__');
  });

  it('shows an empty log directory instead of launch wrapper internals', async () => {
    const launchDir = '/remote/nemo-run/demo/demo_1';
    const logDir = `${launchDir}/nemo-run`;
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          source: 'ssh',
          entries: [
            { name: '__main__.py', path: `${launchDir}/__main__.py`, is_dir: false, size: 0 },
            { name: '_CONFIG', path: `${launchDir}/_CONFIG`, is_dir: false, size: 1000 },
            { name: '_TASKS', path: `${launchDir}/_TASKS`, is_dir: false, size: 9000 },
            { name: '_VERSION', path: `${launchDir}/_VERSION`, is_dir: false, size: 14 },
            { name: 'nemo-run_sbatch.sh', path: `${launchDir}/nemo-run_sbatch.sh`, is_dir: false, size: 5000 },
          ],
        }),
      })
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          source: 'ssh',
          entries: [],
        }),
      });

    (globalThis as any).fetchWithTimeout = fetchMock;

    await openDir('h100', launchDir, 'demo job');

    const treeText = document.getElementById('tree-pane')?.textContent || '';
    expect(treeText).toContain('(empty directory)');
    expect(treeText).not.toContain('__main__.py');
    expect(treeText).not.toContain('_CONFIG');
    expect(treeText).not.toContain('nemo-run_sbatch.sh');
    expect(document.getElementById('content-path')?.textContent).toBe(logDir);
    expect(document.getElementById('modal-content')?.textContent).toContain('Select a file');
    expect(document.getElementById('modal-content')?.textContent).not.toContain('(empty file)');
    expect(String(fetchMock.mock.calls[1][0])).toContain(encodeURIComponent(logDir));
  });

  it('normalizes an initially opened job directory before rendering tree entries', async () => {
    const launchDir = '/remote/nemo-run/demo/demo_1';
    const logDir = `${launchDir}/nemo-run`;
    const fetchMock = vi.fn()
      .mockResolvedValueOnce({
        json: async () => ({
          files: [],
          dirs: [{ label: 'demo_1', path: launchDir }],
        }),
      })
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          source: 'ssh',
          entries: [
            { name: '__main__.py', path: `${launchDir}/__main__.py`, is_dir: false, size: 0 },
            { name: '_CONFIG', path: `${launchDir}/_CONFIG`, is_dir: false, size: 1000 },
            { name: '_TASKS', path: `${launchDir}/_TASKS`, is_dir: false, size: 9000 },
            { name: '_VERSION', path: `${launchDir}/_VERSION`, is_dir: false, size: 14 },
            { name: 'nemo-run_sbatch.sh', path: `${launchDir}/nemo-run_sbatch.sh`, is_dir: false, size: 5000 },
          ],
        }),
      })
      .mockResolvedValueOnce({
        json: async () => ({
          status: 'ok',
          source: 'ssh',
          entries: [],
        }),
      });

    (globalThis as any).fetchWithTimeout = fetchMock;

    await openLog('h100', '357999', 'demo job');

    const treeText = document.getElementById('tree-pane')?.textContent || '';
    expect(treeText).toContain('(empty directory)');
    expect(treeText).not.toContain('__main__.py');
    expect(treeText).not.toContain('_CONFIG');
    expect(treeText).not.toContain('nemo-run_sbatch.sh');
    expect(document.getElementById('modal-content')?.textContent).toContain('No direct log file found');
    expect(document.getElementById('modal-content')?.textContent).not.toContain('(empty file)');
    expect(String(fetchMock.mock.calls[2][0])).toContain(encodeURIComponent(logDir));
  });
});

describe('jsonl lazy record loading', () => {
  it('loads a record when rendered in explorer content', async () => {
    vi.useFakeTimers();
    (globalThis as any).fetchWithTimeout = vi.fn().mockResolvedValue({
      json: async () => ({
        status: 'ok',
        line: 0,
        content: '{"id":"abc","value":1}',
        source: 'mount',
      }),
    });

    const exp = document.getElementById('exp-content')!;
    exp.innerHTML = renderJsonlLazyViewer({
      status: 'ok',
      total: 1,
      count: 1,
      mode: 'all',
      limit: 0,
      records: [{ line: 0, preview: '{"id":"abc"}', valid: true, size: 16 }],
    }, '/remote/data.jsonl', {
      containerId: 'exp-content',
      cluster: 'h100',
      jobId: '123',
    });

    await vi.advanceTimersByTimeAsync(0);

    const details = exp.querySelector('[data-jsonl-rec]') as HTMLDetailsElement;
    const body = exp.querySelector('[data-lazy]') as HTMLElement;
    details.open = true;
    details.dispatchEvent(new Event('toggle', { bubbles: false }));
    await Promise.resolve();
    await Promise.resolve();

    expect((globalThis as any).fetchWithTimeout).toHaveBeenCalledTimes(1);
    expect(String((globalThis as any).fetchWithTimeout.mock.calls[0][0])).toContain('/api/jsonl_record/h100/123');
    expect(body.textContent).toContain('"id"');
    expect(body.textContent).toContain('"abc"');
  });
});
