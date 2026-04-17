/**
 * Unit tests for the in-flight fetch registry and the server-reconnect
 * state machine that drops stuck connections when every cluster fails.
 *
 * Run with: npx vitest run tests/frontend/reconnect.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest';
import { loadBrowserScripts } from './helpers';

declare const dropAllInFlight: (reason?: string) => number;
declare const fetchWithTimeout: (url: string, opts?: any, ms?: number) => Promise<Response>;

function renderDom() {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>
    <div id="error-banner" class="error-banner hidden">
      <span id="error-banner-msg"></span>
    </div>
    <div id="grid"></div>
  `;
}

beforeAll(() => {
  renderDom();
  loadBrowserScripts(['utils.js', 'jobs.js']);
});

beforeEach(() => {
  renderDom();
  (window as any)._clausiusReconnect.reset();
  vi.useRealTimers();
});

function installNativeFetch(fn: (input: any, init?: any) => Promise<Response>) {
  (window as any)._clausiusNativeFetch = fn;
}

describe('dropAllInFlight', () => {
  it('returns 0 when no fetches are in flight', () => {
    expect((globalThis as any).dropAllInFlight('test')).toBe(0);
  });

  it('aborts every tracked fetch and clears the registry', async () => {
    const hanging = vi.fn((_input: any, init?: any) =>
      new Promise<Response>((_resolve, reject) => {
        if (init?.signal) {
          init.signal.addEventListener('abort', () => reject(init.signal.reason));
        }
      })
    );
    installNativeFetch(hanging);

    const p1 = fetchWithTimeout('/api/jobs', {}, 60000).catch((e) => e);
    const p2 = fetchWithTimeout('/api/partition_summary', {}, 60000).catch((e) => e);
    const p3 = fetchWithTimeout('/api/storage_quota/eos', {}, 60000).catch((e) => e);

    // Let microtasks settle so the fetch wrapper registers each controller.
    await new Promise((r) => setTimeout(r, 0));

    expect((window as any)._inFlightCount()).toBe(3);

    const dropped = dropAllInFlight('manual');
    expect(dropped).toBe(3);
    expect((window as any)._inFlightCount()).toBe(0);

    const results = await Promise.all([p1, p2, p3]);
    for (const r of results) {
      expect(String((r as any)?.name || '')).toBe('AbortError');
    }
  });

  it('unregisters a controller when the fetch resolves normally', async () => {
    installNativeFetch(() =>
      Promise.resolve(new Response('{}', { status: 200 }))
    );

    await fetchWithTimeout('/api/health');
    expect((window as any)._inFlightCount()).toBe(0);
  });

  it('aborts with TimeoutError when the timeout fires', async () => {
    installNativeFetch((_input: any, init?: any) =>
      new Promise<Response>((_resolve, reject) => {
        if (init?.signal) {
          init.signal.addEventListener('abort', () => reject(init.signal.reason));
        }
      })
    );

    const err = await fetchWithTimeout('/api/slow', {}, 10).catch((e) => e);
    expect(String((err as any)?.name || '')).toBe('TimeoutError');
    expect((window as any)._inFlightCount()).toBe(0);
  });
});

describe('_triggerServerReconnect', () => {
  it('is a no-op while already probing', async () => {
    (window as any)._clausiusReconnect.state = 'probing';
    const healthy = vi.fn(() =>
      Promise.resolve(new Response('{}', { status: 200 }))
    );
    installNativeFetch(healthy);

    await (window as any)._clausiusReconnect.trigger('test');
    expect(healthy).not.toHaveBeenCalled();
  });

  it('is debounced — second call within 15s is ignored', async () => {
    (window as any)._clausiusReconnect.state = 'idle';
    (window as any)._clausiusReconnect.lastAt = Date.now();

    const healthy = vi.fn(() =>
      Promise.resolve(new Response('{}', { status: 200 }))
    );
    installNativeFetch(healthy);

    await (window as any)._clausiusReconnect.trigger('debounce');
    expect(healthy).not.toHaveBeenCalled();
  });

  it('probes /api/health, drops stuck fetches, then clears state on 200', async () => {
    // Stub out fetchAll so reconnect's "resume" doesn't pull in unrelated
    // polling endpoints while we're asserting on fetch history.
    const origFetchAll = (globalThis as any).fetchAll;
    (globalThis as any).fetchAll = vi.fn().mockResolvedValue(undefined);

    const calls: string[] = [];
    const stuck = vi.fn((input: any, init?: any) => {
      const url = typeof input === 'string' ? input : input?.url || '';
      calls.push(url);
      if (url.includes('/api/health')) {
        return Promise.resolve(new Response('{"status":"ok"}', { status: 200 }));
      }
      return new Promise<Response>((_resolve, reject) => {
        if (init?.signal) {
          init.signal.addEventListener('abort', () => reject(init.signal.reason));
        }
      });
    });
    installNativeFetch(stuck);

    const stuck1 = fetchWithTimeout('/api/jobs/eos', {}, 60000).catch((e) => e);
    const stuck2 = fetchWithTimeout('/api/jobs/hsg', {}, 60000).catch((e) => e);
    await new Promise((r) => setTimeout(r, 0));
    expect((window as any)._inFlightCount()).toBe(2);

    try {
      await (window as any)._clausiusReconnect.trigger('all clusters failed');
    } finally {
      (globalThis as any).fetchAll = origFetchAll;
    }

    expect((window as any)._clausiusReconnect.state).toBe('idle');
    expect(calls.some((u) => u.includes('/api/health'))).toBe(true);

    const results = await Promise.all([stuck1, stuck2]);
    for (const r of results) {
      expect(['AbortError', 'TimeoutError']).toContain(String((r as any)?.name || ''));
    }
  });
});
