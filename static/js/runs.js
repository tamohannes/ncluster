let _runOverlayOpen = false;

async function openRunInfo(cluster, rootJobId, runName) {
  const overlay = document.getElementById('run-overlay');
  const title = document.getElementById('run-title');
  const subtitle = document.getElementById('run-subtitle');
  const body = document.getElementById('run-body');
  const markSlot = document.getElementById('run-mark-slot');
  if (markSlot) markSlot.innerHTML = '';

  title.textContent = runName || 'Run Info';
  subtitle.textContent = `${cluster} · job ${rootJobId}`;
  body.innerHTML = '<div class="log-loading">Loading run info…</div>';
  overlay.classList.add('open');
  _runOverlayOpen = true;

  try {
    const res = await fetch(`/api/run_info/${encodeURIComponent(cluster)}/${encodeURIComponent(rootJobId)}`);
    const data = await res.json();
    if (data.status !== 'ok' || !data.run) {
      if (markSlot) markSlot.innerHTML = '';
      body.innerHTML = `<div class="err-msg">Could not load run info: ${data.error || 'unknown error'}</div>`;
      return;
    }
    _renderRunBody(data.run, cluster);
  } catch (e) {
    if (markSlot) markSlot.innerHTML = '';
    body.innerHTML = `<div class="err-msg">Failed to fetch run info: ${e.message}</div>`;
  }
}

function closeRunInfo(event) {
  if (event && event.target !== event.currentTarget) return;
  closeRunInfoDirect();
}

function closeRunInfoDirect() {
  const markSlot = document.getElementById('run-mark-slot');
  if (markSlot) markSlot.innerHTML = '';
  document.getElementById('run-overlay').classList.remove('open');
  _runOverlayOpen = false;
}

let _runNoteTimer = null;

function _renderRunBody(run, cluster) {
  const body = document.getElementById('run-body');
  const jobs = run.jobs || [];

  const earliest = _earliestTime(jobs, 'started');
  const latest = _latestTime(jobs, 'ended_at');
  const duration = earliest && latest ? _formatDuration(earliest, latest) : '—';
  const totalGpus = run.total_gpus;
  const uniqueNodes = run.unique_nodes;
  const gpusPerNode = run.gpus_per_node;

  const starred = run.starred ? 1 : 0;
  const notes = run.notes || '';
  const runId = run.id;

  const markSlot = document.getElementById('run-mark-slot');
  if (markSlot) {
    const dc = escAttr(cluster);
    const dr = escAttr(String(run.root_job_id));
    markSlot.innerHTML = `<button type="button" class="run-mark-btn${starred ? ' active' : ''}" id="run-mark-btn"
            data-run-cluster="${dc}" data-run-root="${dr}"
            onclick="_toggleRunMark(${runId})" title="${starred ? 'Unmark this run' : 'Mark this run'}">
      ${starred ? 'Marked' : 'Mark'}
    </button>`;
  }

  let html = '';

  html += `<div class="run-notes-block">
    <div class="run-notes-wrap">
      <textarea class="run-notes-textarea" id="run-notes-textarea"
                placeholder="Add notes about this run…"
                oninput="_onRunNoteInput(${runId})"
                onblur="_saveRunNotes(${runId})">${_escHtml(notes)}</textarea>
      <span class="run-notes-saved" id="run-notes-saved">saved</span>
    </div>
  </div>`;

  const _jobStates = _computeJobStateSummary(jobs, gpusPerNode);
  const _durationRing = _runDurationRing(earliest, latest);

  html += `<div class="run-timing">
    <div class="run-timing-item">
      <span class="run-timing-label">Started</span>
      <span class="run-timing-value">${_fmtRunTime(earliest)}</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Ended</span>
      <span class="run-timing-value">${_fmtRunTime(latest)}</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Duration</span>
      <span class="run-timing-value" style="display:inline-flex;align-items:center;gap:5px">${_durationRing}${duration}</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Project</span>
      <span class="run-timing-value">${run.project || '—'}</span>
    </div>
    ${run.source === 'sdk' ? `
    <div class="run-timing-item">
      <span class="run-timing-label">Source</span>
      <span class="run-timing-value" style="color:var(--accent);font-weight:600">SDK</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Git</span>
      <span class="run-timing-value">${_escHtml(run.git_commit || '—')}</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Launcher</span>
      <span class="run-timing-value">${_escHtml(run.launcher_hostname || '—')}</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Working dir</span>
      <span class="run-timing-value" style="word-break:break-all">${_escHtml(run.submit_cwd || '—')}</span>
    </div>` : ''}
  </div>`;

  html += `<div class="run-resource-bar">
    <span class="job-count-text">${_jobStates}</span>
  </div>`;

  const paramsHtml = _renderRunParams(run.params, run.root_job_id);
  if (paramsHtml) html += paramsHtml;

  if (run.submit_command) {
    const cmdId = 'submit-cmd-pre-' + (run.root_job_id || '0');
    const cmdContent = `<div style="position:relative">
      <pre id="${cmdId}" style="white-space:pre-wrap;word-break:break-all;padding-right:36px">${_escHtml(run.submit_command)}</pre>
      <button onclick="navigator.clipboard.writeText(document.getElementById('${cmdId}').textContent).then(()=>{this.textContent='✓';setTimeout(()=>this.textContent='⧉',1200)})" style="position:absolute;top:6px;right:6px;background:var(--surface);border:1px solid var(--border);color:var(--muted);border-radius:4px;padding:2px 6px;cursor:pointer;font-size:12px;line-height:1">⧉</button>
    </div>`;
    html += _renderToggleSection('submit-cmd', 'Submit Command', cmdContent, false);
  }

  if (run.batch_script) {
    html += _renderToggleSection('batch-script', 'Batch Script', `<pre>${_escHtml(run.batch_script)}</pre>`, true);
  }

  if (run.scontrol_raw) {
    html += _renderToggleSection('scontrol', 'Slurm Configuration', `<pre>${_escHtml(run.scontrol_raw)}</pre>`, true);
  }

  if (run.env_vars) {
    const envHtml = _renderEnvTable(run.env_vars);
    html += _renderToggleSection('env-vars', 'Environment Variables', envHtml, true);
  }

  if (run.conda_state) {
    html += _renderToggleSection('conda', 'Conda / Pip State', `<pre>${_escHtml(run.conda_state)}</pre>`, true);
  }

  if (!run.submit_command && !run.batch_script && !run.scontrol_raw && !run.env_vars && !run.conda_state) {
    html += '<div style="font-family:var(--mono);font-size:11px;color:var(--muted);padding:12px 0">';
    html += 'No metadata captured yet. ';
    html += '<a href="#" onclick="retryMetadata(\'' + _escHtml(cluster) + '\',\'' + _escHtml(String(run.root_job_id)) + '\');return false" style="color:var(--accent)">Retry</a>';
    html += '</div>';
  }

  if (jobs.length > 0) {
    html += `<div class="run-section" style="margin-top:14px">
      <div class="run-section-head" onclick="toggleRunSection('run-jobs-sec')">
        <span>Jobs in this run (${jobs.length})</span>
        <span class="run-section-chevron collapsed" id="run-jobs-sec-chevron">▼</span>
      </div>
      <div class="run-section-body hidden" id="run-jobs-sec">
        <table class="run-jobs-table">
          <thead><tr><th>ID</th><th>Name</th><th>State</th><th>Start</th><th>End</th><th>Elapsed</th><th>GPUs</th><th>Nodes</th></tr></thead>
          <tbody>${(() => {
            const _runNames = jobs.map(j => j.job_name || j.name).filter(Boolean);
            const _runHL = computeNameHighlight(_runNames);
            return jobs.map(j => {
              const st = (j.state || '').toUpperCase();
              const reason = j.reason || '';
              const cls = stateClass(st, reason);
              const label = isSoftFail(st, reason) ? 'SOFT FAIL' : (j.state || '—');
              const start = fmtTime(j.started_local || j.started || j.submitted);
              const end = fmtTime(j.ended_local || j.ended_at);
              const rawName = j.job_name || j.name || '';
              const dispName = rawName ? highlightJobName(rawName, _runHL.prefix, _runHL.suffix) : '—';
              const gm = (j.gres || '').match(/gpu[^:]*:(?:[a-zA-Z]\w*:)?(\d+)/);
              const part = (j.partition || '').toLowerCase();
              const isCpuPart = part.startsWith('cpu') || part === 'defq' || part === 'fake';
              const gpusPerNodeJob = gm ? parseInt(gm[1], 10) : ((!isCpuPart && (gpusPerNode || 0) > 0) ? gpusPerNode : 0);
              const nodes = parseInt(j.nodes, 10) || 0;
              const jobGpus = nodes * gpusPerNodeJob;
              const hasGpuSignal = !!gm || isCpuPart || ((!isCpuPart && (gpusPerNode || 0) > 0));
              const gpuCell = hasGpuSignal ? jobGpus : '—';
              return `<tr>
                <td style="color:var(--muted)">${j.job_id || j.jobid || '—'}</td>
                <td class="bold" title="${rawName}">${dispName}</td>
                <td><span class="state-chip ${cls}">${label}</span></td>
                <td style="color:var(--muted)">${start}</td>
                <td style="color:var(--muted)">${end}</td>
                <td style="color:var(--muted)">${j.elapsed || '—'}</td>
                <td style="color:var(--muted)">${gpuCell}</td>
                <td style="color:var(--muted)">${nodes > 0 ? nodes : '—'}</td>
              </tr>`;
            }).join('');
          })()}</tbody>
        </table>
      </div>
    </div>`;
  }

  body.innerHTML = html;
}

// ── Run Parameters block ──────────────────────────────────────────────────
// Renders a compact key/value grid of pipeline kwargs captured by the SDK
// hook (model, benchmarks, num_samples, judge_model, …). Only rows whose
// source value is present are rendered; a fully empty params dict yields
// an empty string so the caller can skip inserting the block entirely.
function _renderRunParams(params, rootJobId) {
  if (!params || typeof params !== 'object') return '';

  const rows = [];

  const push = (label, valueHtml, opts) => {
    if (valueHtml == null || valueHtml === '') return;
    const copy = opts && opts.copy;
    const title = opts && opts.title ? ` title="${escAttr(opts.title)}"` : '';
    const copyBtn = copy
      ? ` <button class="run-params-copy" data-copy="${escAttr(String(copy))}" onclick="_copyText(this)" title="Copy">⧉</button>`
      : '';
    rows.push(`<div class="run-params-row">
        <span class="run-params-label">${label}</span>
        <span class="run-params-value"${title}>${valueHtml}${copyBtn}</span>
      </div>`);
  };

  const modelStr = _paramStr(params.model);
  if (modelStr) push('Model', _truncateMid(modelStr, 60), { copy: modelStr, title: modelStr });

  const serverParts = [];
  if (params.server_type) serverParts.push(_escHtml(String(params.server_type)));
  const serverGpus = _paramInt(params.server_gpus);
  const serverNodes = _paramInt(params.server_nodes);
  if (serverGpus != null) serverParts.push(`${serverGpus}&thinsp;GPU${serverGpus === 1 ? '' : 's'}`);
  if (serverNodes != null && serverNodes !== 1) serverParts.push(`&times; ${serverNodes}&thinsp;node${serverNodes === 1 ? '' : 's'}`);
  if (serverParts.length) push('Server', serverParts.join(' · '));

  const benchHtml = _fmtBenchmarks(params.benchmarks);
  if (benchHtml) push('Benchmarks', benchHtml);

  const splitStr = _paramStr(params.split);
  if (splitStr) push('Split', _escHtml(splitStr));

  const samplesHtml = _fmtNumSamples(params.num_samples);
  if (samplesHtml) push('Samples', samplesHtml);

  const chunks = _paramInt(params.num_chunks);
  if (chunks != null && chunks > 1) push('Chunks', String(chunks));

  if (params.with_sandbox === true) push('Sandbox', 'yes');

  const judgeStr = _paramStr(params.judge_model);
  if (judgeStr) {
    const judgeExtras = [];
    if (params.judge_server_type) judgeExtras.push(_escHtml(String(params.judge_server_type)));
    const judgeGpus = _paramInt(params.judge_server_gpus);
    if (judgeGpus != null && judgeGpus > 0) judgeExtras.push(`${judgeGpus}&thinsp;GPU${judgeGpus === 1 ? '' : 's'}`);
    const judgeValue = _escHtml(judgeStr) + (judgeExtras.length ? ` <span class="run-params-muted">· ${judgeExtras.join(' · ')}</span>` : '');
    push('Judge', judgeValue, { copy: judgeStr, title: judgeStr });
  }

  const promptParts = [];
  if (params.prompt_config) promptParts.push(`config=${_escHtml(String(params.prompt_config))}`);
  if (params.prompt_template) promptParts.push(`template=${_escHtml(String(params.prompt_template))}`);
  if (params.prompt_format) promptParts.push(`format=${_escHtml(String(params.prompt_format))}`);
  if (promptParts.length) push('Prompt', promptParts.join(' · '));

  const datasetStr = _paramStr(params.dataset);
  if (datasetStr) push('Dataset', _escHtml(datasetStr));

  const inputStr = _paramStr(params.input_file) || _paramStr(params.input_dir);
  if (inputStr) push('Input', _truncateMid(inputStr, 60), { copy: inputStr, title: inputStr });

  const depJobs = _paramInt(params.dependent_jobs);
  if (depJobs != null && depJobs > 0) push('Dependent jobs', String(depJobs));

  if (!rows.length) return '';
  return `<div class="run-params" data-run-root="${escAttr(String(rootJobId || ''))}">
    <div class="run-params-head">Run Parameters</div>
    <div class="run-params-grid">${rows.join('')}</div>
  </div>`;
}

function _paramStr(v) {
  if (v == null) return '';
  const s = String(v).trim();
  return s && s.toLowerCase() !== 'none' ? s : '';
}

function _paramInt(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function _truncateMid(s, max) {
  if (!s) return '';
  const str = String(s);
  if (str.length <= max) return _escHtml(str);
  const head = Math.ceil((max - 1) / 2);
  const tail = Math.floor((max - 1) / 2);
  return _escHtml(str.slice(0, head)) + '…' + _escHtml(str.slice(-tail));
}

// Benchmarks may arrive as a string ("hle:3,gpqa_diamond:5"), a list of
// such strings, or a list of {name, seeds} dicts. Render each as a chip
// "<name> ×<seeds>" (seeds omitted when 1 or absent). Degrades to escaped
// raw text for anything we don't recognise so the user still sees something.
function _fmtBenchmarks(raw) {
  if (raw == null || raw === '') return '';
  const entries = [];
  const addSpec = (spec) => {
    const s = String(spec).trim();
    if (!s) return;
    const m = s.match(/^([^:\s]+)(?::(\d+))?/);
    if (m) {
      const name = m[1];
      const seeds = m[2] ? parseInt(m[2], 10) : null;
      entries.push({ name, seeds });
    } else {
      entries.push({ name: s, seeds: null });
    }
  };
  if (typeof raw === 'string') {
    for (const part of raw.split(',')) addSpec(part);
  } else if (Array.isArray(raw)) {
    for (const item of raw) {
      if (typeof item === 'string') addSpec(item);
      else if (item && typeof item === 'object') {
        const name = item.name || item.benchmark || '';
        const seeds = item.seeds || item.num_seeds || null;
        if (name) entries.push({ name: String(name), seeds: seeds == null ? null : parseInt(seeds, 10) });
      }
    }
  } else {
    return _escHtml(String(raw));
  }
  if (!entries.length) return '';
  return entries.map(({ name, seeds }) => {
    const seedPart = seeds && seeds > 1 ? ` <span class="run-params-muted">&times;${seeds}</span>` : '';
    return `<span class="run-params-chip">${_escHtml(name)}${seedPart}</span>`;
  }).join(' ');
}

function _fmtNumSamples(v) {
  if (v == null || v === '') return '';
  const n = _paramInt(v);
  if (n == null) return _escHtml(String(v));
  if (n <= 0) return 'full dataset';
  return `first ${n.toLocaleString()} per benchmark`;
}

function _copyText(btn) {
  const text = btn && btn.getAttribute && btn.getAttribute('data-copy');
  if (!text) return;
  try {
    navigator.clipboard.writeText(String(text));
    const prev = btn.textContent;
    btn.textContent = '✓';
    setTimeout(() => { btn.textContent = prev; }, 1200);
  } catch (_) {}
}

function _runDurationRing(start, end) {
  if (!start) return '';
  const MAX_MS = 4 * 3600 * 1000;
  const now = end || new Date();
  const elapsed = now - start;
  const pct = Math.min(100, Math.max(0, (elapsed / MAX_MS) * 100));
  const r = 7, c = 2 * Math.PI * r;
  const dash = (pct / 100) * c;
  const isOver = elapsed >= MAX_MS;
  const isLive = !end;
  const colorCls = isOver ? 'ring-over' : isLive ? 'ring-live' : '';
  return `<svg class="progress-ring run-dur-ring ${colorCls}" width="18" height="18" viewBox="0 0 18 18" role="img">
    <title>${Math.round(pct)}% of 4h</title>
    <circle class="ring-bg" cx="9" cy="9" r="${r}" fill="none" stroke-width="2"/>
    <circle class="ring-fg" cx="9" cy="9" r="${r}" fill="none" stroke-width="2"
      stroke-dasharray="${dash.toFixed(1)} ${c.toFixed(1)}" transform="rotate(-90 9 9)"/>
  </svg>`;
}

function _computeJobStateSummary(jobs, gpusPerNode) {
  const byId = {};
  for (const j of jobs) { if (j.job_id || j.jobid) byId[j.job_id || j.jobid] = j; }

  function _jobGpus(j) {
    const gm = (j.gres || '').match(/gpu[^:]*:(?:[a-zA-Z]\w*:)?(\d+)/);
    const part = (j.partition || '').toLowerCase();
    const isCpu = part.startsWith('cpu') || part === 'defq' || part === 'fake';
    const gpn = gm ? parseInt(gm[1], 10) : ((!isCpu && (gpusPerNode || 0) > 0) ? gpusPerNode : 0);
    return (parseInt(j.nodes, 10) || 0) * gpn;
  }

  let runC = 0, runG = 0, pendC = 0, pendG = 0, depC = 0, depG = 0, bkpC = 0, bkpG = 0;
  let compC = 0, compG = 0, doneC = 0, failC = 0, failG = 0, toC = 0, toG = 0, cancC = 0;
  const otherCounts = {};
  const otherGpus = {};

  for (const j of jobs) {
    const st = (j.state || 'UNKNOWN').toUpperCase();
    const g = _jobGpus(j);
    if (st === 'RUNNING' || st === 'COMPLETING') { runC++; runG += g; }
    else if (st === 'PENDING' || st === 'SUBMITTING') {
      if (typeof _isBackupDep === 'function' && _isBackupDep(j, byId)) { bkpC++; bkpG += g; }
      else if (typeof _isDependentJob === 'function' && _isDependentJob(j)) { depC++; depG += g; }
      else { pendC++; pendG += g; }
    }
    else if (st === 'COMPLETED') { doneC++; }
    else if (st.includes('FAIL')) { failC++; failG += g; }
    else if (st === 'TIMEOUT') { toC++; toG += g; }
    else if (st.startsWith('CANCEL')) { cancC++; }
    else { otherCounts[st] = (otherCounts[st] || 0) + 1; otherGpus[st] = (otherGpus[st] || 0) + g; }
  }

  const parts = [];
  if (runC)  parts.push(`<span class="ss-run">${runC} running (<span class="gpu-num">${runG}</span>&thinsp;GPU)</span>`);
  if (pendC) parts.push(`<span class="ss-pend">${pendC} pending (<span class="gpu-num">${pendG}</span>&thinsp;GPU)</span>`);
  if (depC)  parts.push(`<span class="ss-dep">${depC} dep (<span class="gpu-num">${depG}</span>)</span>`);
  if (bkpC)  parts.push(`<span class="ss-bkp">${bkpC} backup (<span class="gpu-num">${bkpG}</span>)</span>`);
  if (doneC) parts.push(`<span class="ss-done">${doneC} done</span>`);
  if (failC) parts.push(`<span class="ss-fail">${failC} failed (<span class="gpu-num">${failG}</span>&thinsp;GPU)</span>`);
  if (toC)   parts.push(`<span class="ss-fail">${toC} timeout (<span class="gpu-num">${toG}</span>&thinsp;GPU)</span>`);
  if (cancC) parts.push(`<span class="ss-canc">${cancC} cancelled</span>`);
  for (const st of Object.keys(otherCounts)) {
    parts.push(`<span class="ss-canc">${otherCounts[st]} ${st.toLowerCase()} (<span class="gpu-num">${otherGpus[st]}</span>)</span>`);
  }

  return parts.join(' · ');
}

function _renderToggleSection(id, title, contentHtml, collapsed) {
  const chevronCls = collapsed ? 'collapsed' : '';
  const bodyCls = collapsed ? 'hidden' : '';
  return `<div class="run-section">
    <div class="run-section-head" onclick="toggleRunSection('${id}')">
      <span>${title}</span>
      <span class="run-section-chevron ${chevronCls}" id="${id}-chevron">▼</span>
    </div>
    <div class="run-section-body ${bodyCls}" id="${id}">${contentHtml}</div>
  </div>`;
}

function toggleRunSection(sectionId) {
  const body = document.getElementById(sectionId);
  const chevron = document.getElementById(sectionId + '-chevron');
  if (!body) return;
  body.classList.toggle('hidden');
  if (chevron) chevron.classList.toggle('collapsed');
}

function _renderEnvTable(envStr) {
  let pairs = [];
  const trimmed = envStr.trim();
  if (trimmed.startsWith('{')) {
    try {
      const obj = JSON.parse(trimmed);
      pairs = Object.entries(obj).sort((a, b) => a[0].localeCompare(b[0]));
    } catch { /* fall through to line-based parsing */ }
  }
  if (!pairs.length) {
    const lines = trimmed.split('\n').filter(l => l.trim());
    if (!lines.length) return `<pre>${_escHtml(envStr)}</pre>`;
    pairs = lines.map(line => {
      const eq = line.indexOf('=');
      return eq < 0 ? [line, ''] : [line.slice(0, eq), line.slice(eq + 1)];
    });
  }
  const filterId = 'env-filter-' + Math.random().toString(36).slice(2, 8);
  const tableId = 'env-tbl-' + filterId;
  const filterInput = pairs.length > 8
    ? `<input id="${filterId}" type="text" placeholder="Filter variables…" oninput="(function(v){var rows=document.getElementById('${tableId}').querySelectorAll('tr');for(var i=0;i<rows.length;i++){rows[i].style.display=rows[i].textContent.toLowerCase().includes(v)?'':'none'}})(this.value.toLowerCase())" style="width:100%;padding:4px 8px;margin-bottom:6px;font-family:var(--mono);font-size:11px;background:var(--surface);border:1px solid var(--border);border-radius:4px;color:var(--text);outline:none">`
    : '';
  const rows = pairs.map(([k, v]) =>
    `<tr><td>${_escHtml(k)}</td><td style="word-break:break-all">${_escHtml(String(v))}</td></tr>`
  ).join('');
  return `${filterInput}<table class="env-table" id="${tableId}">${rows}</table>`;
}

function _escHtml(s) {
  if (!s) return '';
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function _earliestTime(jobs, field) {
  let best = null;
  for (const j of jobs) {
    const v = j[field] || j['started_local'] || j['started'] || j['submitted'];
    if (v && v !== 'Unknown' && v !== 'N/A' && v !== '—') {
      const d = new Date(v.replace('T', ' '));
      if (!isNaN(d) && (!best || d < best)) best = d;
    }
  }
  return best;
}

function _latestTime(jobs, field) {
  let best = null;
  for (const j of jobs) {
    const v = j[field] || j['ended_local'] || j['ended_at'];
    if (v && v !== 'Unknown' && v !== 'N/A' && v !== '—') {
      const d = new Date(v.replace('T', ' '));
      if (!isNaN(d) && (!best || d > best)) best = d;
    }
  }
  return best;
}

function _fmtRunTime(d) {
  if (!d) return '—';
  const now = new Date();
  const sameYear = d.getFullYear() === now.getFullYear();
  const date = d.toLocaleDateString([], {month: 'short', day: 'numeric', ...(sameYear ? {} : {year: 'numeric'})});
  const time = d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', hour12: false});
  return `${date}, ${time}`;
}

function _formatDuration(start, end) {
  const ms = end - start;
  if (ms < 0) return '—';
  const totalSec = Math.floor(ms / 1000);
  const days = Math.floor(totalSec / 86400);
  const hours = Math.floor((totalSec % 86400) / 3600);
  const mins = Math.floor((totalSec % 3600) / 60);
  const secs = totalSec % 60;
  if (days > 0) return `${days}d ${hours}h ${mins}m`;
  if (hours > 0) return `${hours}h ${mins}m ${secs}s`;
  if (mins > 0) return `${mins}m ${secs}s`;
  return `${secs}s`;
}

async function _toggleRunMark(runId) {
  const btn = document.getElementById('run-mark-btn');
  if (!btn) return;
  const cluster = btn.getAttribute('data-run-cluster');
  const rootJobId = btn.getAttribute('data-run-root');
  const wasMarked = btn.classList.contains('active');
  const newVal = wasMarked ? 0 : 1;
  btn.classList.toggle('active', !!newVal);
  btn.textContent = newVal ? 'Marked' : 'Mark';
  btn.title = newVal ? 'Unmark this run' : 'Mark this run';
  if (typeof syncRunMarkedBorders === 'function') {
    syncRunMarkedBorders(cluster, rootJobId, !!newVal);
  }
  try {
    const res = await fetch(`/api/run/${runId}`, {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({starred: newVal}),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.status === 'error') {
      throw new Error(data.error || 'request failed');
    }
  } catch (_) {
    btn.classList.toggle('active', !!wasMarked);
    btn.textContent = wasMarked ? 'Marked' : 'Mark';
    btn.title = wasMarked ? 'Unmark this run' : 'Mark this run';
    if (typeof syncRunMarkedBorders === 'function') {
      syncRunMarkedBorders(cluster, rootJobId, !!wasMarked);
    }
  }
}

function _onRunNoteInput(runId) {
  if (_runNoteTimer) clearTimeout(_runNoteTimer);
  _runNoteTimer = setTimeout(() => _saveRunNotes(runId), 1500);
}

async function _saveRunNotes(runId) {
  if (_runNoteTimer) { clearTimeout(_runNoteTimer); _runNoteTimer = null; }
  const ta = document.getElementById('run-notes-textarea');
  if (!ta) return;
  const notes = ta.value;
  try {
    await fetch(`/api/run/${runId}`, {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({notes}),
    });
    const badge = document.getElementById('run-notes-saved');
    if (badge) {
      badge.classList.add('show');
      setTimeout(() => badge.classList.remove('show'), 1200);
    }
  } catch (_) {}
}

async function retryMetadata(cluster, rootJobId) {
  const body = document.getElementById('run-body');
  const retryLink = body && body.querySelector('a[onclick*="retryMetadata"]');
  const container = retryLink && retryLink.parentElement;
  if (container) {
    container.innerHTML = '<span style="color:var(--accent)">Fetching metadata…</span>';
  }
  try {
    const res = await fetch(`/api/run_info/${encodeURIComponent(cluster)}/${encodeURIComponent(rootJobId)}/retry_meta`, { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok' && data.run) {
      _renderRunBody(data.run, cluster);
    } else {
      if (container) container.innerHTML = 'Retry failed: ' + (data.error || 'unknown error');
    }
  } catch (e) {
    if (container) container.innerHTML = 'Retry failed: ' + e.message;
  }
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && _runOverlayOpen) {
    closeRunInfoDirect();
  }
});
