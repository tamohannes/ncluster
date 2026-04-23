/**
 * Unit tests for the Run Parameters block in the run-info modal.
 *
 * Run with: npx vitest run tests/frontend/run_params.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import { loadBrowserScripts } from './helpers';

declare const _renderRunParams: (params: any, rootJobId?: string | number) => string;
declare const _fmtBenchmarks: (raw: any) => string;
declare const _fmtNumSamples: (v: any) => string;

beforeAll(() => {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>
  `;
  loadBrowserScripts(['utils.js', 'jobs.js', 'runs.js']);
});

beforeEach(() => {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>
    <div id="sandbox"></div>
  `;
});

function render(params: any): HTMLElement {
  const host = document.getElementById('sandbox') as HTMLElement;
  host.innerHTML = _renderRunParams(params, '12345');
  return host;
}

describe('_renderRunParams', () => {
  it('returns empty string when params is missing or empty', () => {
    expect(_renderRunParams(null)).toBe('');
    expect(_renderRunParams(undefined)).toBe('');
    expect(_renderRunParams({})).toBe('');
  });

  it('returns empty string when no known keys are present', () => {
    expect(_renderRunParams({ unrelated: 'x' })).toBe('');
  });

  it('renders the model row with a copy button', () => {
    const host = render({ model: 'meta/llama-3.3-70b' });
    expect(host.querySelector('.run-params')).toBeTruthy();
    const labels = Array.from(host.querySelectorAll('.run-params-label')).map((el) => el.textContent);
    expect(labels).toEqual(['Model']);
    const copy = host.querySelector('.run-params-copy') as HTMLElement;
    expect(copy).toBeTruthy();
    expect(copy.getAttribute('data-copy')).toBe('meta/llama-3.3-70b');
  });

  it('combines server_type/gpus/nodes into a single Server row', () => {
    const host = render({ server_type: 'sglang', server_gpus: 8, server_nodes: 2 });
    const row = _valueOf(host, 'Server');
    expect(row).toContain('sglang');
    expect(row).toContain('8');
    expect(row).toContain('2');
  });

  it('omits node count from Server when exactly 1 node', () => {
    const host = render({ server_type: 'sglang', server_gpus: 8, server_nodes: 1 });
    const row = _valueOf(host, 'Server');
    expect(row).toContain('sglang');
    expect(row).toContain('8');
    expect(row).not.toMatch(/nodes?/i);
  });

  it('renders benchmarks as chips with ×N seed badges', () => {
    const host = render({ benchmarks: 'hle:3,gpqa_diamond:5' });
    const chips = Array.from(host.querySelectorAll('.run-params-chip')).map((el) => el.textContent || '');
    expect(chips.length).toBe(2);
    expect(chips[0]).toContain('hle');
    expect(chips[0]).toContain('×3');
    expect(chips[1]).toContain('gpqa_diamond');
    expect(chips[1]).toContain('×5');
  });

  it('renders benchmark chip without ×N when seeds is 1 or missing', () => {
    const host = render({ benchmarks: 'hle:1,math' });
    const chips = Array.from(host.querySelectorAll('.run-params-chip')).map((el) => el.textContent || '');
    expect(chips[0]).toContain('hle');
    expect(chips[0]).not.toContain('×');
    expect(chips[1]).toContain('math');
    expect(chips[1]).not.toContain('×');
  });

  it('accepts benchmarks as an array of strings or {name, seeds} dicts', () => {
    const sandbox = document.getElementById('sandbox') as HTMLElement;

    sandbox.innerHTML = _fmtBenchmarks(['hle:3', 'math:2']);
    let chips = Array.from(sandbox.querySelectorAll('.run-params-chip')).map((el) => (el.textContent || '').trim());
    expect(chips).toContain('hle ×3');
    expect(chips).toContain('math ×2');

    sandbox.innerHTML = _fmtBenchmarks([
      { name: 'hle', seeds: 3 },
      { benchmark: 'gpqa_diamond', num_seeds: 5 },
    ]);
    chips = Array.from(sandbox.querySelectorAll('.run-params-chip')).map((el) => (el.textContent || '').trim());
    expect(chips).toContain('hle ×3');
    expect(chips).toContain('gpqa_diamond ×5');
  });

  it('formats num_samples: 0/null as full, positive as "first N"', () => {
    expect(_fmtNumSamples(0)).toBe('full dataset');
    expect(_fmtNumSamples(null)).toBe('');
    expect(_fmtNumSamples(100)).toMatch(/first\s+100/);
    expect(_fmtNumSamples(1000)).toMatch(/first\s+1,000/);
  });

  it('renders a Samples row for positive num_samples', () => {
    const host = render({ num_samples: 100 });
    expect(_valueOf(host, 'Samples')).toMatch(/first\s+100/);
  });

  it('renders Samples=full dataset when num_samples=0', () => {
    const host = render({ num_samples: 0 });
    expect(_valueOf(host, 'Samples')).toContain('full');
  });

  it('only shows Chunks when > 1', () => {
    const host1 = render({ num_chunks: 1 });
    expect(_labels(host1)).not.toContain('Chunks');

    const host2 = render({ num_chunks: 4 });
    expect(_valueOf(host2, 'Chunks')).toBe('4');
  });

  it('only shows Sandbox when exactly true', () => {
    expect(_labels(render({ with_sandbox: false }))).not.toContain('Sandbox');
    expect(_labels(render({ with_sandbox: 'yes' as any }))).not.toContain('Sandbox');
    expect(_valueOf(render({ with_sandbox: true }), 'Sandbox')).toBe('yes');
  });

  it('renders Judge with extras and a copy button', () => {
    const host = render({
      judge_model: 'gpt-oss-120b',
      judge_server_type: 'openai',
      judge_server_gpus: 0,
    });
    const judgeValue = _valueOf(host, 'Judge');
    expect(judgeValue).toContain('gpt-oss-120b');
    expect(judgeValue).toContain('openai');
    const copies = host.querySelectorAll('.run-params-copy');
    const judgeCopy = Array.from(copies).find((c) => c.getAttribute('data-copy') === 'gpt-oss-120b');
    expect(judgeCopy).toBeTruthy();
  });

  it('combines prompt_config/template/format into one Prompt row', () => {
    const host = render({
      prompt_config: 'mcp_scipython_full',
      prompt_template: 'chat',
    });
    const row = _valueOf(host, 'Prompt');
    expect(row).toContain('mcp_scipython_full');
    expect(row).toContain('chat');
  });

  it('truncates very long model paths with middle ellipsis', () => {
    const longPath = '/shared/storage/models/' +
      'a-very-deeply-nested-model-checkpoint-name-that-is-too-long';
    const host = render({ model: longPath });
    const row = _valueOf(host, 'Model');
    expect(row).toContain('…');
    expect(row.length).toBeLessThan(longPath.length);
    const copy = host.querySelector('.run-params-copy') as HTMLElement;
    expect(copy.getAttribute('data-copy')).toBe(longPath);
  });

  it('renders multiple rows in the right order for a full payload', () => {
    const host = render({
      model: 'kimi/K2.5',
      server_type: 'sglang',
      server_gpus: 8,
      server_nodes: 1,
      benchmarks: 'hle:3',
      split: 'test',
      num_samples: 100,
      num_chunks: 4,
      with_sandbox: true,
      judge_model: 'gpt-oss-120b',
    });
    const labels = _labels(host);
    expect(labels).toEqual([
      'Model',
      'Server',
      'Benchmarks',
      'Split',
      'Samples',
      'Chunks',
      'Sandbox',
      'Judge',
    ]);
  });

  it('stores the root job id as a data attribute for targeting', () => {
    const host = render({ model: 'x' });
    const block = host.querySelector('.run-params') as HTMLElement;
    expect(block.getAttribute('data-run-root')).toBe('12345');
  });
});

// Helpers ---------------------------------------------------------------------

function _labels(host: HTMLElement): string[] {
  return Array.from(host.querySelectorAll('.run-params-label')).map((el) => (el.textContent || '').trim());
}

function _valueOf(host: HTMLElement, label: string): string {
  const labels = Array.from(host.querySelectorAll('.run-params-label'));
  const target = labels.find((el) => (el.textContent || '').trim() === label);
  if (!target) return '';
  const value = target.nextElementSibling as HTMLElement | null;
  return value ? (value.textContent || '').trim() : '';
}
