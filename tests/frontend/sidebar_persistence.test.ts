/**
 * Unit tests for sidebar width persistence.
 *
 * Run with: npx vitest run tests/frontend/sidebar_persistence.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import { readFileSync } from 'fs';
import { join } from 'path';

function renderDom() {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>
    <div id="nav-toggle"></div>
    <div id="side-nav"></div>
    <div id="nav-splitter"></div>
    <div class="proj-layout" id="proj-layout">
      <div class="proj-main"></div>
      <div class="logbook-panel" id="logbook-panel" style="display:none">
        <div class="logbook-resizer" id="logbook-resizer"></div>
      </div>
    </div>
  `;

  const layout = document.getElementById('proj-layout') as HTMLDivElement;
  layout.getBoundingClientRect = () => ({
    x: 0,
    y: 0,
    left: 0,
    top: 0,
    right: 1200,
    bottom: 900,
    width: 1200,
    height: 900,
    toJSON() { return {}; },
  } as DOMRect);
}

beforeAll(() => {
  renderDom();
  const files = ['utils.js', 'jobs.js', 'logbooks.js'];
  for (const file of files) {
    const code = readFileSync(join(__dirname, '../../static/js', file), 'utf-8');
    new Function(code).call(globalThis);
  }
});

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  renderDom();
});

declare const applySidebarState: () => void;
declare const _restoreLogbookState: () => void;

describe('sidebar width persistence', () => {
  it('restores the left sidebar width from localStorage', () => {
    localStorage.setItem('clausius.navWidth', '410');

    applySidebarState();

    expect(document.getElementById('side-nav')!.style.width).toBe('410px');
  });

  it('restores the right sidebar width from localStorage', () => {
    localStorage.setItem('clausius.logbookWidth', '560');
    sessionStorage.setItem('clausius.logbookOpen', '1');

    _restoreLogbookState();

    const panel = document.getElementById('logbook-panel')!;
    expect(panel.style.display).toBe('');
    expect(panel.style.width).toBe('560px');
  });

  it('saves the right sidebar width while dragging', () => {
    const resizer = document.getElementById('logbook-resizer')!;
    const panel = document.getElementById('logbook-panel')!;

    resizer.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, clientX: 900 }));
    document.dispatchEvent(new MouseEvent('mousemove', { bubbles: true, clientX: 760 }));
    document.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));

    expect(panel.style.width).toBe('440px');
    expect(localStorage.getItem('clausius.logbookWidth')).toBe('440');
  });
});
