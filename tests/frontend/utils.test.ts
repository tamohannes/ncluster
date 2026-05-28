/**
 * Unit tests for static/js/utils.js pure functions.
 *
 * Run with: npx vitest run tests/frontend/utils.test.ts
 * Requires: npm install (installs vitest + jsdom)
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { loadBrowserScripts } from './helpers';

// Load utils.js into the jsdom global scope
beforeAll(() => {
  // Provide the minimal DOM elements utils.js expects
  document.body.innerHTML = '<script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>';
  loadBrowserScripts(['utils.js', 'jobs.js']);
});

// Access globals that utils.js defines
declare const stateClass: (s: string) => string;
declare const stateChip: (s: string, progress?: number | null) => string;
declare const isFailedLikeState: (s: string) => boolean;
declare const isCompletedState: (s: string) => boolean;
declare const fmtTime: (s: string) => string;
declare const parseGpus: (nodes: string, gres: string) => string | null;
declare const jobGpuCount: (nodes: string, gres: string) => number;
declare const groupKeyForJob: (name: string) => string;
declare const groupJobsByDependency: (jobs: any[]) => [string, any[]][];
declare const topoSortJobs: (jobs: any[]) => any[];
declare const depthInGroup: (job: any, byId: any, idSet: Set<string>, memo: any) => number;
declare const computeRefreshIntervalSec: (data: any) => number;

describe('stateClass', () => {
  it('maps RUNNING', () => expect(stateClass('RUNNING')).toBe('s-RUNNING'));
  it('maps PENDING', () => expect(stateClass('PENDING')).toBe('s-PENDING'));
  it('maps FAILED', () => expect(stateClass('FAILED')).toBe('s-FAILED'));
  it('maps CANCELLED', () => expect(stateClass('CANCELLED')).toBe('s-CANCELLED'));
  it('maps COMPLETED', () => expect(stateClass('COMPLETED')).toBe('s-COMPLETED'));
  it('maps COMPLETING', () => expect(stateClass('COMPLETING')).toBe('s-COMPLETING'));
  it('handles unknown', () => expect(stateClass('WEIRD')).toBe('s-OTHER'));
  it('handles empty', () => expect(stateClass('')).toBe('s-OTHER'));
  it('handles compound state', () => expect(stateClass('FAILED by xyz')).toBe('s-FAILED'));
});

describe('isFailedLikeState', () => {
  it('detects FAILED', () => expect(isFailedLikeState('FAILED')).toBe(true));
  it('detects CANCELLED', () => expect(isFailedLikeState('CANCELLED')).toBe(true));
  it('detects TIMEOUT', () => expect(isFailedLikeState('TIMEOUT')).toBe(true));
  it('detects OUT_OF_MEMORY', () => expect(isFailedLikeState('OUT_OF_MEMORY')).toBe(true));
  it('rejects RUNNING', () => expect(isFailedLikeState('RUNNING')).toBe(false));
  it('rejects COMPLETED', () => expect(isFailedLikeState('COMPLETED')).toBe(false));
});

describe('isCompletedState', () => {
  it('detects COMPLETED', () => expect(isCompletedState('COMPLETED')).toBe(true));
  it('rejects COMPLETING', () => expect(isCompletedState('COMPLETING')).toBe(false));
  it('rejects FAILED', () => expect(isCompletedState('FAILED')).toBe(false));
});

describe('fmtTime', () => {
  it('returns dash for empty', () => expect(fmtTime('')).toBe('—'));
  it('returns dash for N/A', () => expect(fmtTime('N/A')).toBe('—'));
  it('returns dash for Unknown', () => expect(fmtTime('Unknown')).toBe('—'));
  it('handles ISO format', () => {
    const result = fmtTime('2026-03-09T13:22:55');
    expect(result).not.toBe('—');
    expect(typeof result).toBe('string');
  });
});

describe('parseGpus', () => {
  it('parses gpu:8', () => expect(parseGpus('1', 'gpu:8')).toBe('8 GPUs'));
  it('parses gpu:h100:8', () => expect(parseGpus('1', 'gpu:h100:8')).toBe('8 GPUs'));
  it('multi-node', () => expect(parseGpus('2', 'gpu:8')).toBe('16 GPUs (2×8)'));
  it('returns null for cpu', () => expect(parseGpus('1', 'cpu')).toBe(null));
  it('returns null for (null)', () => expect(parseGpus('1', '(null)')).toBe(null));
  it('single gpu', () => expect(parseGpus('1', 'gpu:1')).toBe('1 GPU'));
});

describe('jobGpuCount', () => {
  it('matches parseGpus totals', () => {
    expect(jobGpuCount('1', 'gpu:8')).toBe(8);
    expect(jobGpuCount('2', 'gpu:8')).toBe(16);
    expect(jobGpuCount('1', 'gpu:h100:8')).toBe(8);
  });
  it('returns 0 for cpu or missing gres', () => {
    expect(jobGpuCount('1', 'cpu')).toBe(0);
    expect(jobGpuCount('1', '(null)')).toBe(0);
  });
});

describe('groupKeyForJob', () => {
  it('keeps eval suffix segments', () => expect(groupKeyForJob('eval-math_100')).toBe('eval-math_100'));
  it('strips judge suffix', () => expect(groupKeyForJob('eval-math-judge')).toBe('eval-math'));
  it('strips rs suffix', () => expect(groupKeyForJob('eval-math-rs0')).toBe('eval-math'));
  it('empty returns misc', () => expect(groupKeyForJob('')).toBe('misc'));
});

describe('groupJobsByDependency', () => {
  it('keeps same-wave MPSF stages together even with per-stage output dirs', () => {
    const base = 'mpsf_v15_v1-5-hle-phy-nem120b-turn1-test5-r3';
    const jobs = [
      {
        jobid: '4148623',
        name: `${base}-path_server`,
        depends_on: [],
        dependents: [],
        submitted: '2026-05-26T17:29:43',
        output_dir: `/nemo-run/${base}-path_server/${base}-path_server_1`,
      },
      {
        jobid: '4148628',
        name: `${base}-path-analytical`,
        depends_on: [],
        dependents: ['4148638'],
        submitted: '2026-05-26T17:29:57',
        output_dir: `/nemo-run/${base}-path-analytical/${base}-path-analytical_1`,
      },
      {
        jobid: '4148629',
        name: `${base}-path-computational`,
        depends_on: [],
        dependents: ['4148638'],
        submitted: '2026-05-26T17:30:13',
        output_dir: `/nemo-run/${base}-path-computational/${base}-path-computational_1`,
      },
      {
        jobid: '4148636',
        name: `${base}-path-knowledge`,
        depends_on: [],
        dependents: ['4148638'],
        submitted: '2026-05-26T17:30:27',
        output_dir: `/nemo-run/${base}-path-knowledge/${base}-path-knowledge_1`,
      },
      {
        jobid: '4148638',
        name: `${base}-gate`,
        depends_on: ['4148628', '4148629', '4148636'],
        dependents: [],
        submitted: '2026-05-26T17:30:48',
      },
    ];

    const groups = groupJobsByDependency(jobs);
    expect(groups).toHaveLength(1);
    expect(groups[0][0]).toBe(base);
    expect(groups[0][1].map(j => j.jobid).sort()).toEqual(['4148623', '4148628', '4148629', '4148636', '4148638']);
  });

  it('does not merge distinct authoritative run ids just because names match', () => {
    const groups = groupJobsByDependency([
      { jobid: '1', name: 'hle_eval_math', run_id: '101', depends_on: [], submitted: '2026-05-26T17:00:00' },
      { jobid: '2', name: 'hle_eval_math', run_id: '102', depends_on: [], submitted: '2026-05-26T17:00:10' },
    ]);

    expect(groups).toHaveLength(2);
  });
});

describe('topoSortJobs', () => {
  it('single job unchanged', () => {
    const jobs = [{ jobid: '1', depends_on: [], dependents: [], state: 'RUNNING' }];
    expect(topoSortJobs(jobs)).toEqual(jobs);
  });

  it('parent before child', () => {
    const jobs = [
      { jobid: '2', depends_on: ['1'], dependents: [], state: 'PENDING' },
      { jobid: '1', depends_on: [], dependents: ['2'], state: 'RUNNING' },
    ];
    const sorted = topoSortJobs(jobs);
    expect(sorted[0].jobid).toBe('1');
    expect(sorted[1].jobid).toBe('2');
  });
});

describe('computeRefreshIntervalSec', () => {
  it('30s when running jobs exist', () => {
    const data = { c1: { jobs: [{ state: 'RUNNING' }] } };
    expect(computeRefreshIntervalSec(data)).toBe(30);
  });

  it('60s when all idle', () => {
    const data = { c1: { jobs: [] } };
    expect(computeRefreshIntervalSec(data)).toBe(60);
  });
});
