let _runOverlayOpen = false;

async function openRunInfo(cluster, rootJobId, runName) {
  const overlay = document.getElementById('run-overlay');
  const title = document.getElementById('run-title');
  const subtitle = document.getElementById('run-subtitle');
  const body = document.getElementById('run-body');

  title.textContent = runName || 'Run Info';
  subtitle.textContent = `${cluster} · job ${rootJobId}`;
  body.innerHTML = '<div class="log-loading">Loading run info…</div>';
  overlay.classList.add('open');
  _runOverlayOpen = true;

  try {
    const res = await fetch(`/api/run_info/${encodeURIComponent(cluster)}/${encodeURIComponent(rootJobId)}`);
    const data = await res.json();
    if (data.status !== 'ok' || !data.run) {
      body.innerHTML = `<div class="err-msg">Could not load run info: ${data.error || 'unknown error'}</div>`;
      return;
    }
    _renderRunBody(data.run, cluster);
  } catch (e) {
    body.innerHTML = `<div class="err-msg">Failed to fetch run info: ${e.message}</div>`;
  }
}

function closeRunInfo(event) {
  if (event && event.target !== event.currentTarget) return;
  closeRunInfoDirect();
}

function closeRunInfoDirect() {
  document.getElementById('run-overlay').classList.remove('open');
  _runOverlayOpen = false;
}

function _renderRunBody(run, cluster) {
  const body = document.getElementById('run-body');
  const jobs = run.jobs || [];

  const earliest = _earliestTime(jobs, 'started');
  const latest = _latestTime(jobs, 'ended_at');
  const duration = earliest && latest ? _formatDuration(earliest, latest) : '—';

  let html = '';

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
      <span class="run-timing-value">${duration}</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Jobs</span>
      <span class="run-timing-value">${jobs.length}</span>
    </div>
    <div class="run-timing-item">
      <span class="run-timing-label">Project</span>
      <span class="run-timing-value">${run.project || '—'}</span>
    </div>
  </div>`;

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

  if (!run.batch_script && !run.scontrol_raw && !run.env_vars && !run.conda_state) {
    html += '<div style="font-family:var(--mono);font-size:11px;color:var(--muted);padding:12px 0">';
    html += 'No metadata captured yet. ';
    html += '<a href="#" onclick="retryMetadata(\'' + _escHtml(cluster) + '\',\'' + _escHtml(String(run.root_job_id)) + '\');return false" style="color:var(--accent)">Retry</a>';
    html += '</div>';
  }

  if (jobs.length > 0) {
    html += `<div class="run-section" style="margin-top:14px">
      <div class="run-section-head" onclick="toggleRunSection('run-jobs-sec')">
        <span>Jobs in this run</span>
        <span class="run-section-chevron" id="run-jobs-sec-chevron">▼</span>
      </div>
      <div class="run-section-body" id="run-jobs-sec">
        <table class="run-jobs-table">
          <thead><tr><th>ID</th><th>Name</th><th>State</th><th>Start</th><th>End</th><th>Elapsed</th></tr></thead>
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
              return `<tr>
                <td style="color:var(--muted)">${j.job_id || j.jobid || '—'}</td>
                <td style="font-weight:500" title="${rawName}">${dispName}</td>
                <td><span class="state-chip ${cls}">${label}</span></td>
                <td style="color:var(--muted)">${start}</td>
                <td style="color:var(--muted)">${end}</td>
                <td style="color:var(--muted)">${j.elapsed || '—'}</td>
              </tr>`;
            }).join('');
          })()}</tbody>
        </table>
      </div>
    </div>`;
  }

  body.innerHTML = html;
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
  const lines = envStr.split('\n').filter(l => l.trim());
  if (!lines.length) return `<pre>${_escHtml(envStr)}</pre>`;
  const rows = lines.map(line => {
    const eq = line.indexOf('=');
    if (eq < 0) return `<tr><td colspan="2">${_escHtml(line)}</td></tr>`;
    const key = line.slice(0, eq);
    const val = line.slice(eq + 1);
    return `<tr><td>${_escHtml(key)}</td><td>${_escHtml(val)}</td></tr>`;
  }).join('');
  return `<table class="env-table">${rows}</table>`;
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
  return d.toLocaleDateString([], {month: 'short', day: 'numeric', year: 'numeric'})
    + ' ' + d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', second: '2-digit'});
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
