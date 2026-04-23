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
  it('adds error class for error lines', () => {
    const result = renderLogWithHighlights('Error: something broke');
    expect(result).toContain('error');
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
