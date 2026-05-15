// Mind-map renderer for per-campaign DAG (logbook entry_type === 'mind_map').
//
// Reads structured graph_json {version, nodes, edges} and lays it out via
// dagre (top-down DAG by default), draws static SVG with D3, attaches
// hover tooltip + click popover behavior. No drag-and-drop. Editing happens
// over MCP / API; this file is read-only render code.
//
// Public surface:
//   renderMindMap(container, entry) — draws into `container`
//   _mmClosePopover() — global close handler bound from the popover backdrop
//
// Status palette is keyed off CSS variables (--mm-status-*) declared in
// static/css/style.css so themes can override without touching this file.

const _MM_STATUS_LABEL = {
  planned: 'Planned',
  active: 'Active',
  blocked: 'Blocked',
  done: 'Done',
  failed: 'Failed',
  abandoned: 'Abandoned',
};

const _MM_EDGE_KIND_LABEL = {
  default: '',
  success: 'on success',
  failure: 'on failure',
  branch: 'branch',
  blocker: 'blocks',
  verification: 'verifies',
};

// Edge kinds that should render with a distinct end-cap instead of the
// default arrowhead. `blocker` gets a small stop bar (visually "stops
// downstream until source clears"); `verification` keeps the arrowhead
// but the stroke style (double line via CSS) signals validation.
const _MM_EDGE_END_MARKER = {
  blocker: 'mm-stop',
};

const _MM_NODE_WIDTH = 240;
const _MM_NODE_HEIGHT = 64;
const _MM_NODE_PAD_X = 14;
const _MM_RANK_SEP = 60;
const _MM_NODE_SEP = 28;

// Cache the active entry's graph on the renderer so the popover handler can
// resolve clicked node ids back to their full data (description, summary).
let _mmActiveEntry = null;
let _mmActiveContainer = null;
let _mmNodesById = {};

function renderMindMap(container, entry) {
  if (!container) return;
  _mmActiveEntry = entry || null;
  _mmActiveContainer = container;
  _mmNodesById = {};
  let graph = {};
  try {
    if (entry && entry.graph_json) {
      graph = typeof entry.graph_json === 'string'
        ? JSON.parse(entry.graph_json)
        : entry.graph_json;
    }
  } catch (_) {
    graph = {};
  }
  const nodes = Array.isArray(graph && graph.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph && graph.edges) ? graph.edges : [];
  nodes.forEach(n => { if (n && n.id) _mmNodesById[n.id] = n; });

  if (!nodes.length) {
    container.innerHTML = `<div class="mm-empty">
      <div class="mm-empty-title">This mind map is empty.</div>
      <div class="mm-empty-sub">Add the first node with <code>patch_mind_map</code>
        (op: <code>add_node</code>) after discussing the change with the user.</div>
    </div>`;
    return;
  }

  if (typeof dagre === 'undefined' || !dagre.graphlib) {
    container.innerHTML = `<div class="mm-empty mm-empty-error">
      <div class="mm-empty-title">Mind map renderer unavailable.</div>
      <div class="mm-empty-sub">dagre.min.js failed to load — check the network tab.</div>
    </div>`;
    return;
  }

  const g = new dagre.graphlib.Graph();
  g.setGraph({
    rankdir: 'TB',
    nodesep: _MM_NODE_SEP,
    ranksep: _MM_RANK_SEP,
    marginx: 18,
    marginy: 18,
  });
  g.setDefaultEdgeLabel(() => ({}));
  nodes.forEach(n => {
    g.setNode(n.id, { width: _MM_NODE_WIDTH, height: _MM_NODE_HEIGHT });
  });
  edges.forEach(e => {
    if (e && e.from && e.to && _mmNodesById[e.from] && _mmNodesById[e.to]) {
      g.setEdge(e.from, e.to, { kind: e.kind || 'default', label: e.label || '', id: e.id || '' });
    }
  });

  try {
    dagre.layout(g);
  } catch (err) {
    console.error('mind map layout failed', err);
    container.innerHTML = `<div class="mm-empty mm-empty-error">
      <div class="mm-empty-title">Mind map layout failed.</div>
      <div class="mm-empty-sub">${_escHtml(String(err && err.message || err))}</div>
    </div>`;
    return;
  }

  const gw = g.graph().width || 100;
  const gh = g.graph().height || 100;

  container.innerHTML = `
    <button class="mm-fullscreen-toggle" type="button"
            onclick="event.stopPropagation();toggleMindMapFullscreen(this)"
            title="Open mind map in a fullscreen popup (Esc to close)">fullscreen</button>
    <div class="mm-shell" data-mm-shell="1">
      <div class="mm-legend">
        ${Object.keys(_MM_STATUS_LABEL).map(s => `
          <span class="mm-legend-chip mm-status-${s}">
            <span class="mm-legend-dot"></span>${_MM_STATUS_LABEL[s]}
          </span>`).join('')}
      </div>
      <div class="mm-canvas-wrap">
        <svg class="mm-canvas" viewBox="0 0 ${gw} ${gh}"
             preserveAspectRatio="xMidYMid meet"
             role="img" aria-label="Campaign mind map">
          <defs>
            <marker id="mm-arrow" viewBox="0 0 10 10" refX="9" refY="5"
                    markerWidth="7" markerHeight="7" orient="auto-start-reverse">
              <path d="M0,0 L10,5 L0,10 z" class="mm-arrow-head"></path>
            </marker>
            <marker id="mm-stop" viewBox="0 0 10 10" refX="5" refY="5"
                    markerWidth="9" markerHeight="9" orient="auto">
              <rect x="3.5" y="0.5" width="3" height="9" class="mm-stop-bar"></rect>
            </marker>
          </defs>
        </svg>
      </div>
    </div>
    <div class="mm-popover-backdrop" id="mm-popover-backdrop"
         onclick="_mmClosePopover(event)"></div>`;

  const svg = container.querySelector('.mm-canvas');
  if (!svg) return;
  const svgNS = 'http://www.w3.org/2000/svg';

  // Edges first so nodes sit on top.
  g.edges().forEach(eo => {
    const meta = g.edge(eo);
    const points = (meta.points || []).map(p => `${p.x},${p.y}`).join(' ');
    const kind = (meta.kind || 'default').toLowerCase();
    const polyline = document.createElementNS(svgNS, 'polyline');
    polyline.setAttribute('points', points);
    polyline.setAttribute('class', `mm-edge mm-edge-${kind}`);
    polyline.setAttribute('fill', 'none');
    const endMarker = _MM_EDGE_END_MARKER[kind] || 'mm-arrow';
    polyline.setAttribute('marker-end', `url(#${endMarker})`);
    svg.appendChild(polyline);
    const labelText = meta.label || _MM_EDGE_KIND_LABEL[kind] || '';
    if (labelText) {
      const mid = meta.points && meta.points.length
        ? meta.points[Math.floor(meta.points.length / 2)]
        : null;
      if (mid) {
        const label = document.createElementNS(svgNS, 'text');
        label.setAttribute('x', mid.x);
        label.setAttribute('y', mid.y - 6);
        label.setAttribute('class', `mm-edge-label mm-edge-label-${kind}`);
        label.setAttribute('text-anchor', 'middle');
        label.textContent = labelText;
        svg.appendChild(label);
      }
    }
  });

  g.nodes().forEach(id => {
    const meta = g.node(id);
    const node = _mmNodesById[id];
    if (!node) return;
    const x = meta.x - meta.width / 2;
    const y = meta.y - meta.height / 2;
    const status = (node.status || 'planned').toLowerCase();
    const group = document.createElementNS(svgNS, 'g');
    group.setAttribute('class', `mm-node mm-status-${status}`);
    group.setAttribute('transform', `translate(${x}, ${y})`);
    group.setAttribute('tabindex', '0');
    group.setAttribute('role', 'button');
    group.setAttribute('aria-label', `${_MM_STATUS_LABEL[status] || status}: ${node.title || id}`);
    group.dataset.nodeId = id;

    const rect = document.createElementNS(svgNS, 'rect');
    rect.setAttribute('width', meta.width);
    rect.setAttribute('height', meta.height);
    rect.setAttribute('rx', 10);
    rect.setAttribute('ry', 10);
    rect.setAttribute('class', 'mm-node-rect');
    group.appendChild(rect);

    const statusDot = document.createElementNS(svgNS, 'circle');
    statusDot.setAttribute('cx', 14);
    statusDot.setAttribute('cy', 14);
    statusDot.setAttribute('r', 5);
    statusDot.setAttribute('class', 'mm-node-status-dot');
    group.appendChild(statusDot);

    const title = document.createElementNS(svgNS, 'text');
    title.setAttribute('x', _MM_NODE_PAD_X);
    title.setAttribute('y', 30);
    title.setAttribute('class', 'mm-node-title');
    title.textContent = _mmTruncate(node.title || id, 30);
    group.appendChild(title);

    const summaryText = (node.summary || '').trim();
    if (summaryText) {
      const sub = document.createElementNS(svgNS, 'text');
      sub.setAttribute('x', _MM_NODE_PAD_X);
      sub.setAttribute('y', 48);
      sub.setAttribute('class', 'mm-node-summary');
      sub.textContent = _mmTruncate(summaryText, 38);
      group.appendChild(sub);
    } else {
      const statusLabel = document.createElementNS(svgNS, 'text');
      statusLabel.setAttribute('x', _MM_NODE_PAD_X);
      statusLabel.setAttribute('y', 48);
      statusLabel.setAttribute('class', 'mm-node-summary mm-node-summary-muted');
      statusLabel.textContent = _MM_STATUS_LABEL[status] || status;
      group.appendChild(statusLabel);
    }

    const native = document.createElementNS(svgNS, 'title');
    native.textContent = summaryText
      ? `${node.title || id}\n[${_MM_STATUS_LABEL[status] || status}]\n${summaryText}`
      : `${node.title || id}\n[${_MM_STATUS_LABEL[status] || status}]`;
    group.appendChild(native);

    group.addEventListener('click', () => _mmOpenPopover(id));
    group.addEventListener('keydown', evt => {
      if (evt.key === 'Enter' || evt.key === ' ') {
        evt.preventDefault();
        _mmOpenPopover(id);
      }
    });
    svg.appendChild(group);
  });
}

function _mmTruncate(s, max) {
  s = String(s || '');
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function _mmOpenPopover(nodeId) {
  const node = _mmNodesById[nodeId];
  if (!node || !_mmActiveContainer) return;
  const backdrop = document.getElementById('mm-popover-backdrop');
  if (!backdrop) return;

  const status = (node.status || 'planned').toLowerCase();
  const statusLabel = _MM_STATUS_LABEL[status] || status;
  const description = (node.description || '').trim();
  const summary = (node.summary || '').trim();
  const descHtml = description ? renderRichText(description) : '';
  const summaryHtml = summary
    ? `<div class="mm-popover-summary lb-detail-body">${renderRefs(_escHtml(summary))}</div>`
    : '';

  backdrop.innerHTML = `
    <div class="mm-popover" role="dialog" aria-modal="true"
         onclick="event.stopPropagation()">
      <div class="mm-popover-head">
        <span class="mm-popover-status mm-status-${status}">
          <span class="mm-legend-dot"></span>${statusLabel}
        </span>
        <button class="mm-popover-close" type="button"
                onclick="_mmClosePopover()" aria-label="Close">×</button>
      </div>
      <h3 class="mm-popover-title">${_escHtml(node.title || nodeId)}</h3>
      <div class="mm-popover-id">id: <code>${_escHtml(nodeId)}</code></div>
      ${summaryHtml}
      ${descHtml ? `<div class="mm-popover-body lb-detail-body">${descHtml}</div>` : ''}
    </div>`;
  backdrop.classList.add('visible');
  hydrateRefs(backdrop);
  _mmDecorateAimqlBlocks(backdrop);
}

function _mmClosePopover(evt) {
  if (evt && evt.target && evt.target.id !== 'mm-popover-backdrop'
      && !evt.target.classList.contains('mm-popover-close')) {
    return;
  }
  const backdrop = document.getElementById('mm-popover-backdrop');
  if (!backdrop) return;
  backdrop.classList.remove('visible');
  backdrop.innerHTML = '';
}

// Find every aimql code block inside the popover and inject a small
// "Open in Metrics ↗" anchor that deep-links to the Metrics Explorer with
// the block text pre-populated as the AimQL query.
function _mmDecorateAimqlBlocks(root) {
  if (!root) return;
  const blocks = root.querySelectorAll('pre > code.language-aimql, pre > code.aimql, pre > code[class*="lang-aimql"]');
  blocks.forEach(code => {
    const pre = code.parentElement;
    if (!pre || pre.dataset.mmAimqlDecorated === '1') return;
    pre.dataset.mmAimqlDecorated = '1';
    const text = (code.textContent || '').trim();
    if (!text) return;
    const a = document.createElement('a');
    a.className = 'mm-aimql-open';
    a.target = '_blank';
    a.rel = 'noopener';
    a.href = buildMetricsUrlFromAimql(text);
    a.textContent = 'Open in Metrics ↗';
    a.title = 'Open this AimQL query in the Metrics Explorer';
    pre.insertAdjacentElement('afterend', a);
  });
}

function buildMetricsUrlFromAimql(aimqlText) {
  const trimmed = (aimqlText || '').trim();
  if (!trimmed) return '/metrics';
  const p = new URLSearchParams();
  p.set('q', trimmed);
  return `/metrics?${p.toString()}`;
}

// Fullscreen popup mode: toggles `.mm-fullscreen-modal` on the outer
// `.lb-mind-map-canvas` so the SVG (with its viewBox + preserveAspectRatio)
// scales up to fill a centered overlay. The node-click popover keeps working
// because `#mm-popover-backdrop` is still the same element in the DOM and its
// z-index sits above the fullscreen modal.
function toggleMindMapFullscreen(btn) {
  const canvas = btn && btn.closest ? btn.closest('.lb-mind-map-canvas') : null;
  if (!canvas) return;
  if (canvas.classList.contains('mm-fullscreen-modal')) {
    exitMindMapFullscreen();
  } else {
    enterMindMapFullscreen(canvas, btn);
  }
}

function enterMindMapFullscreen(canvas, btn) {
  if (!canvas) return;
  canvas.classList.add('mm-fullscreen-modal');
  document.body.classList.add('mm-fullscreen-active');
  let veil = document.getElementById('mm-fullscreen-veil');
  if (!veil) {
    veil = document.createElement('div');
    veil.id = 'mm-fullscreen-veil';
    veil.className = 'mm-fullscreen-veil';
    veil.addEventListener('click', exitMindMapFullscreen);
    document.body.appendChild(veil);
  }
  if (btn) {
    btn.textContent = 'exit';
    btn.title = 'Exit fullscreen (Esc)';
  }
}

function exitMindMapFullscreen() {
  const canvas = document.querySelector('.lb-mind-map-canvas.mm-fullscreen-modal');
  if (canvas) canvas.classList.remove('mm-fullscreen-modal');
  document.body.classList.remove('mm-fullscreen-active');
  const veil = document.getElementById('mm-fullscreen-veil');
  if (veil) veil.remove();
  const btn = document.querySelector('.lb-mind-map-canvas .mm-fullscreen-toggle');
  if (btn) {
    btn.textContent = 'fullscreen';
    btn.title = 'Open mind map in a fullscreen popup (Esc to close)';
  }
}

if (typeof document !== 'undefined') {
  document.addEventListener('keydown', evt => {
    if (evt.key !== 'Escape') return;
    // Popover takes priority over fullscreen so users can drill down into a
    // node and pop back out one level at a time.
    const backdrop = document.getElementById('mm-popover-backdrop');
    if (backdrop && backdrop.classList.contains('visible')) {
      _mmClosePopover();
      return;
    }
    if (document.body.classList.contains('mm-fullscreen-active')) {
      exitMindMapFullscreen();
    }
  });
}
