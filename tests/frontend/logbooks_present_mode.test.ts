import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { loadBrowserScripts } from './helpers';

function renderDom() {
  document.body.innerHTML = `
    <script id="cluster-data" type="application/json">{"local":{"host":null,"gpu_type":"local"}}</script>
    <div class="logbook-view active" id="logbook-view">
      <div class="lb-page">
        <div class="lb-sidebar"></div>
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
});
