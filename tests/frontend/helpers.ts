import { readFileSync } from 'fs';
import { join } from 'path';

function makeMatchMediaResult(query: string) {
  return {
    matches: false,
    media: query,
    onchange: null,
    addListener() {},
    removeListener() {},
    addEventListener() {},
    removeEventListener() {},
    dispatchEvent() { return false; },
  };
}

export function installBrowserStubs() {
  if (typeof window.matchMedia !== 'function') {
    Object.defineProperty(window, 'matchMedia', {
      configurable: true,
      writable: true,
      value: (query: string) => makeMatchMediaResult(query),
    });
  }
  if (typeof globalThis.matchMedia !== 'function') {
    Object.defineProperty(globalThis, 'matchMedia', {
      configurable: true,
      writable: true,
      value: window.matchMedia.bind(window),
    });
  }
}

export function loadBrowserScript(file: string) {
  loadBrowserScripts([file]);
}

export function loadBrowserScripts(files: string[]) {
  installBrowserStubs();
  const bundle = files.map((file) => {
    const code = readFileSync(join(__dirname, '../../static/js', file), 'utf-8');
    return `\n// ---- ${file} ----\n${code}\n`;
  }).join('\n');
  globalThis.eval(bundle);
}
