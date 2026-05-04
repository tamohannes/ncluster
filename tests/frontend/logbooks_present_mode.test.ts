import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { loadBrowserScripts } from './helpers';

function renderDom() {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>
    <div class="logbook-view active" id="logbook-view">
      <div class="lb-page">
        <div class="lb-sidebar">
          <select id="lb-project-select">
            <option value="hle">hle</option>
            <option value="artsiv">artsiv</option>
          </select>
          <input id="lb-search" value="">
          <div id="lb-campaign-filters"></div>
          <div id="lb-sidebar-list"></div>
        </div>
        <div class="lb-splitter" id="lb-splitter"></div>
        <div class="lb-main" id="lb-main"></div>
      </div>
    </div>
  `;

  const root = document.getElementById('logbook-view') as HTMLDivElement & {
    requestFullscreen: ReturnType<typeof vi.fn>;
  };
  root.requestFullscreen = vi.fn(async () => {
    (document as Document & { fullscreenElement: Element | null }).fullscreenElement = root;
  });
  return root;
}

let exitFullscreenMock: ReturnType<typeof vi.fn>;

beforeAll(() => {
  Object.defineProperty(document, 'fullscreenElement', {
    configurable: true,
    writable: true,
    value: null,
  });

  exitFullscreenMock = vi.fn(async () => {
    (document as Document & { fullscreenElement: Element | null }).fullscreenElement = null;
  });
  Object.defineProperty(document, 'exitFullscreen', {
    configurable: true,
    writable: true,
    value: exitFullscreenMock,
  });

  renderDom();
  loadBrowserScripts(['utils.js', 'jobs.js', 'logbooks.js']);
});

beforeEach(async () => {
  renderDom();
  exitFullscreenMock.mockClear();
  (document as Document & { fullscreenElement: Element | null }).fullscreenElement = null;
  await toggleLogbookPresentMode(false);
});

declare const toggleLogbookPresentMode: (force?: boolean) => Promise<void>;
declare const _showMainEmpty: () => void;
declare const _captureLogbookTabState: () => any;
declare const _restoreLogbookTabState: (state: any, tab?: any) => boolean;

describe('logbook present mode', () => {
  it('enables fullscreen reader mode on the logbook view', async () => {
    const root = document.getElementById('logbook-view') as HTMLDivElement & {
      requestFullscreen: ReturnType<typeof vi.fn>;
    };

    await toggleLogbookPresentMode(true);

    expect(root.classList.contains('lb-present-mode')).toBe(true);
    expect(root.requestFullscreen).toHaveBeenCalledTimes(1);
    expect(document.fullscreenElement).toBe(root);
  });

  it('disables fullscreen reader mode and exits fullscreen', async () => {
    const root = document.getElementById('logbook-view') as HTMLDivElement & {
      requestFullscreen: ReturnType<typeof vi.fn>;
    };

    await toggleLogbookPresentMode(true);
    root.requestFullscreen.mockClear();
    exitFullscreenMock.mockClear();

    await toggleLogbookPresentMode(false);

    expect(root.classList.contains('lb-present-mode')).toBe(false);
    expect(root.requestFullscreen).not.toHaveBeenCalled();
    expect(exitFullscreenMock).toHaveBeenCalledTimes(1);
    expect(document.fullscreenElement).toBe(null);
  });

  it('drops out of present mode when leaving the entry detail', async () => {
    const root = document.getElementById('logbook-view') as HTMLDivElement;

    await toggleLogbookPresentMode(true);
    _showMainEmpty();
    await Promise.resolve();

    expect(root.classList.contains('lb-present-mode')).toBe(false);
  });

  it('syncs out of present mode when fullscreen exits externally', async () => {
    const root = document.getElementById('logbook-view') as HTMLDivElement;

    await toggleLogbookPresentMode(true);
    (document as Document & { fullscreenElement: Element | null }).fullscreenElement = null;
    document.dispatchEvent(new Event('fullscreenchange'));

    expect(root.classList.contains('lb-present-mode')).toBe(false);
  });

  it('captures and restores independent logbook tab snapshots', () => {
    const aState = {
      project: 'hle',
      entryId: 101,
      entryTitle: 'HLE entry',
      currentEntry: { id: 101, title: 'HLE entry' },
      editingId: null,
      typeFilter: 'note',
      campaignFilter: 'eval',
      history: [{ type: 'entry', entryId: 101, project: 'hle', anchor: null }],
      mainHtml: '<div class="lb-detail">HLE content</div>',
      mainPlan: false,
      mainScrollTop: 12,
      sidebarHtml: '<div class="lb-sidebar-item" data-id="101">HLE item</div>',
      sidebarScrollTop: 5,
      campaignHtml: '<button class="lb-campaign-chip active">eval</button>',
      searchValue: 'reasoning',
      selectValue: 'hle',
    };
    const bState = {
      ...aState,
      project: 'artsiv',
      entryId: 202,
      entryTitle: 'Artsiv entry',
      currentEntry: { id: 202, title: 'Artsiv entry' },
      history: [{ type: 'entry', entryId: 202, project: 'artsiv', anchor: null }],
      mainHtml: '<div class="lb-detail">Artsiv content</div>',
      sidebarHtml: '<div class="lb-sidebar-item" data-id="202">Artsiv item</div>',
      campaignHtml: '<button class="lb-campaign-chip active">bugs</button>',
      searchValue: 'localization',
      selectValue: 'artsiv',
    };

    expect(_restoreLogbookTabState(aState)).toBe(true);
    const capturedA = _captureLogbookTabState();
    expect(capturedA.lbProject).toBe('hle');
    expect(capturedA.lbEntryId).toBe(101);

    expect(_restoreLogbookTabState(bState)).toBe(true);
    expect(document.getElementById('lb-main')!.innerHTML).toContain('Artsiv content');

    expect(_restoreLogbookTabState(capturedA.lbState)).toBe(true);
    expect((document.getElementById('lb-project-select') as HTMLSelectElement).value).toBe('hle');
    expect((document.getElementById('lb-search') as HTMLInputElement).value).toBe('reasoning');
    expect(document.getElementById('lb-main')!.innerHTML).toContain('HLE content');
    expect(document.getElementById('lb-sidebar-list')!.innerHTML).toContain('HLE item');
  });
});
