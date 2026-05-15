// Universal Clausius reference renderer.
//
// Single source of truth for rendering @run-name and #N references inside
// any rich-text surface (logbook bodies, mind map popovers, campaign goals,
// future popups, etc). Other modules should NEVER reimplement the regexes or
// onclick wiring — call `renderRefs` / `renderRichText` and hydrate with
// `hydrateRefs` instead.
//
// References supported:
//   @run-name              -> opens the run popup (via /api/runs_by_name)
//   #N                     -> opens logbook entry N (cross-project safe)
//   #N:fig-M / #N:tbl-M    -> opens entry N and scrolls to the figure/table
//
// Public surface:
//   renderRefs(html)                       — rewrite @ and # tokens in HTML.
//   renderRichText(text)                   — markdownToHtml(text) + renderRefs.
//   hydrateRefs(rootElement?)              — resolve .entry-ref placeholders.
//   openRunRef(name)                       — open the most relevant run.
//   openEntryRef(id, project?, anchor?)    — open an entry by id.
//
// Backward-compat aliases (DO NOT use in new code, kept so inline onclick
// strings already in the DOM keep working):
//   _renderLogbookMarkdown, _resolveEntryRefs, openLogByName, _openEntryRef
//
// Load order: this file MUST come AFTER logs.js (for markdownToHtml + escapeHtml)
// and runs.js (for openRunInfoByHash) and BEFORE logbooks.js / mind_map.js.

function renderRefs(html) {
  if (typeof html !== 'string' || !html) return html || '';
  let out = html;
  // The `(?<!\w)` lookbehind is load-bearing: it excludes ML metric notation
  // like `pass@k`, `pass@1`, `accuracy@5`, `recall@10`, `BLEU@4`, where the `@`
  // is preceded by a word char. Real run refs always come after start-of-string,
  // whitespace, or punctuation. Do NOT relax this without a metric-safety test.
  out = out.replace(/(?<!\w)@([\w-]+)/g, (match, name) =>
    `<span class="run-ref" onclick="openRunRef('${name}')" title="Open run @${name}">${match}</span>`
  );
  const anchorRefs = [];
  out = out.replace(/(?<!\w)#(\d+):(fig|tbl)-(\d+)/g, (match, id, kind, num) => {
    const anchor = `${kind}-${num}`;
    const label = kind === 'fig' ? `Figure ${num}` : `Table ${num}`;
    const placeholder = `\x00ANCHOR${anchorRefs.length}\x00`;
    anchorRefs.push(`<span class="anchor-ref" onclick="openEntryRef(${id},'','${anchor}')" title="${label} in entry #${id}">${match}</span>`);
    return placeholder;
  });
  out = out.replace(/(?<!\w)#(\d+)/g, (match, id) =>
    `<span class="entry-ref" data-entry-ref="${id}" onclick="openEntryRef(${id})" title="Open entry #${id}">${match}</span>`
  );
  anchorRefs.forEach((span, i) => {
    out = out.replace(`\x00ANCHOR${i}\x00`, span);
  });
  return out;
}

function renderRichText(text) {
  const raw = text == null ? '' : String(text);
  if (typeof markdownToHtml === 'function') return renderRefs(markdownToHtml(raw));
  const escaped = typeof escapeHtml === 'function' ? escapeHtml(raw) : raw;
  return renderRefs(escaped);
}

function hydrateRefs(rootElement) {
  const root = rootElement || document;
  const refs = root.querySelectorAll('.entry-ref[data-entry-ref]:not(.resolved)');
  if (!refs.length) return;
  const ids = new Set();
  refs.forEach(el => ids.add(el.dataset.entryRef));
  fetch(`/api/logbook/resolve_refs?ids=${Array.from(ids).join(',')}`)
    .then(r => r.json())
    .then(entries => {
      const currentProject = (typeof _lbProject !== 'undefined') ? _lbProject : '';
      for (const entry of entries) {
        root.querySelectorAll(`.entry-ref[data-entry-ref="${entry.id}"]`).forEach(el => {
          const escaped = (entry.title || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
          const crossProject = entry.project && entry.project !== currentProject;
          const projectBadge = crossProject
            ? `<span class="entry-ref-project">${entry.project}</span>`
            : '';
          el.innerHTML = `<span class="entry-ref-id">#${entry.id}</span>${projectBadge}<span class="entry-ref-title">${escaped}</span>`;
          el.title = entry.title;
          el.dataset.entryProject = entry.project || '';
          el.classList.add('resolved');
          if (crossProject) el.classList.add('cross-project');
          el.setAttribute('onclick', `openEntryRef(${entry.id},'${(entry.project || '').replace(/'/g, "\\'")}')`);
        });
      }
    })
    .catch(() => {});
}

async function openRunRef(name) {
  if (!name) return;
  const trimmed = String(name).trim();
  if (!trimmed) return;
  try {
    let runs = await _refsLookupRuns(trimmed, 'equals');
    if (!runs.length) runs = await _refsLookupRuns(trimmed, 'contains');
    if (runs.length && typeof openRunInfoByHash === 'function') {
      const r = runs[0];
      openRunInfoByHash(r.cluster, r.run_hash, r.run_name || trimmed);
      return;
    }
    const project = (typeof _lbProject !== 'undefined') ? _lbProject : '';
    if (project) {
      try {
        const res = await fetch(`/api/history?project=${encodeURIComponent(project)}&limit=500`);
        const rows = await res.json();
        const match = (rows || []).find(r => (r.job_name || '').includes(trimmed));
        if (match && typeof openLog === 'function') {
          openLog(match.cluster, match.job_id, match.job_name);
          return;
        }
      } catch (_) {}
    }
    if (typeof toast === 'function') toast(`No run found matching "@${trimmed}"`, 'error');
  } catch (_) {
    if (typeof toast === 'function') toast('Failed to look up run', 'error');
  }
}

async function _refsLookupRuns(name, mode) {
  try {
    const res = await fetch(`/api/runs_by_name?q=${encodeURIComponent(name)}&mode=${mode}&limit=1`);
    const data = await res.json();
    if (data && data.status === 'ok' && Array.isArray(data.runs)) return data.runs;
  } catch (_) {}
  return [];
}

async function openEntryRef(entryId, project, anchor) {
  let proj = project || '';
  if (!proj) {
    try {
      const res = await fetch(`/api/logbook/resolve_refs?ids=${entryId}`);
      const entries = await res.json();
      if (entries.length) proj = entries[0].project || '';
    } catch (_) {}
  }
  const currentProject = (typeof _lbProject !== 'undefined') ? _lbProject : '';
  if (proj && proj !== currentProject) {
    if (typeof _lbProject !== 'undefined') _lbProject = proj;
    const sel = document.getElementById('lb-project-select');
    if (sel) sel.value = proj;
    if (typeof _loadEntries === 'function') _loadEntries(proj);
    if (typeof _loadRunNames === 'function') _loadRunNames(proj);
  }
  if (typeof openLogbookEntry === 'function') {
    openLogbookEntry(entryId, anchor ? { anchor } : {});
  }
}

const _renderLogbookMarkdown = renderRichText;
const _resolveEntryRefs = (root) => hydrateRefs(root);
const openLogByName = openRunRef;
const _openEntryRef = openEntryRef;
