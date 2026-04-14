/**
 * Unit tests for static/js/logs.js rendering functions.
 *
 * Run with: npx vitest run tests/frontend/logs.test.ts
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { loadBrowserScripts } from './helpers';

beforeAll(() => {
  // Provide DOM elements that logs.js and its dependencies need
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>
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
  `;

  loadBrowserScripts(['utils.js', 'crash_detect.js', 'logs.js']);
});

declare const escapeHtml: (s: string) => string;
declare const renderLogWithHighlights: (raw: string) => string;
declare const renderJsonWithSyntax: (raw: string) => string;
declare const jsonlRecordSummary: (obj: any) => string;
declare const guessIcon: (name: string) => string;
declare const markdownToHtml: (raw: string) => string;
declare const renderFileContentByType: (path: string, raw: string) => { cls: string; html: string };

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
