/**
 * Unit tests for sidebar width persistence.
 *
 * Run with: npx vitest run tests/frontend/sidebar_persistence.test.ts
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import { loadBrowserScripts } from './helpers';

function renderDom() {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>
    <div id="nav-toggle"></div>
    <div id="side-nav"></div>
    <div id="nav-splitter"></div>
    <div class="lb-page" id="lb-page">
      <div class="lb-sidebar"></div>
      <div class="lb-splitter" id="lb-splitter"></div>
      <div class="lb-main" id="lb-main"></div>
    </div>
  `;

  const page = document.getElementById('lb-page') as HTMLDivElement;
  page.getBoundingClientRect = () => ({
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
  loadBrowserScripts(['utils.js', 'jobs.js', 'logbooks.js']);
});

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  renderDom();
});

declare const applySidebarState: () => void;
declare const _restoreLogbookSidebarState: () => void;

describe('sidebar width persistence', () => {
  it('restores the left sidebar width from localStorage', () => {
    localStorage.setItem('clausius.navWidth', '410');

    applySidebarState();

    expect(document.getElementById('side-nav')!.style.width).toBe('410px');
  });

  it('restores the right sidebar width from localStorage', () => {
    localStorage.setItem('clausius.lbSidebarWidth', '560');

    _restoreLogbookSidebarState();

    const sidebar = document.querySelector('.lb-sidebar') as HTMLDivElement;
    expect(sidebar.style.width).toBe('560px');
  });

  it('saves the right sidebar width while dragging', () => {
    const splitter = document.getElementById('lb-splitter')!;
    const sidebar = document.querySelector('.lb-sidebar') as HTMLDivElement;

    splitter.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, clientX: 440 }));
    document.dispatchEvent(new MouseEvent('mousemove', { bubbles: true, clientX: 440 }));
    document.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));

    expect(sidebar.style.width).toBe('440px');
    expect(localStorage.getItem('clausius.lbSidebarWidth')).toBe('440');
  });
});
