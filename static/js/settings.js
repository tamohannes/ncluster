// ── Stats popup ──
async function openStats(cluster, jobId, jobName) {
  document.getElementById('stats-overlay').classList.add('open');
  document.getElementById('stats-title').textContent = jobName || `job ${jobId}`;
  document.getElementById('stats-sub').textContent = `${cluster} · ${jobId}`;
  document.getElementById('stats-body').innerHTML = '<div class="log-loading">Loading stats…</div>';
  try {
    const res = await fetch(`/api/stats/${cluster}/${jobId}`);
    const d = await res.json();
    if (d.status !== 'ok') {
      document.getElementById('stats-body').innerHTML = `<div class="log-loading" style="color:var(--red)">` +
        `${d.error || 'Could not load stats.'}</div>`;
      return;
    }
    const gRows = (d.gpus || []).map(g => `<tr><td>${g.index}</td><td>${g.name}</td><td>${g.util}</td><td>${g.mem}</td></tr>`).join('');
    const gpuTable = gRows
      ? `<div class="gpu-table"><table><thead><tr><th>GPU</th><th>Model</th><th>Util</th><th>Memory</th></tr></thead><tbody>${gRows}</tbody></table></div>`
      : `<div class="stats-kv" style="margin-top:12px">
          <div class="stats-k">GPU Metrics</div>
          <div class="stats-v">${d.gpu_summary || 'Not available (job may be pending/finished, or direct probe is restricted).'}</div>
          ${d.gpu_probe_error ? `<div class="stats-v" style="color:var(--muted);font-size:10px;margin-top:4px">probe detail: ${d.gpu_probe_error}</div>` : ''}
        </div>`;
    document.getElementById('stats-body').innerHTML = `
      <div class="stats-grid">
        <div class="stats-kv"><div class="stats-k">State</div><div class="stats-v">${d.state || '—'}</div></div>
        <div class="stats-kv"><div class="stats-k">Elapsed</div><div class="stats-v">${d.elapsed || '—'}</div></div>
        <div class="stats-kv"><div class="stats-k">Nodes</div><div class="stats-v">${d.nodes || '—'}</div></div>
        <div class="stats-kv"><div class="stats-k">Node List</div><div class="stats-v">${d.node_list || '—'}</div></div>
        <div class="stats-kv"><div class="stats-k">Allocated CPU</div><div class="stats-v">${d.cpus || '—'}</div></div>
        <div class="stats-kv"><div class="stats-k">Allocated GPU</div><div class="stats-v">${d.gres || '—'}</div></div>
        <div class="stats-kv"><div class="stats-k">Ave GPU util (TRES)</div><div class="stats-v">${d.gpuutil_ave || '—'}</div></div>
        <div class="stats-kv"><div class="stats-k">Ave GPU mem (TRES)</div><div class="stats-v">${d.gpumem_ave || '—'}</div></div>
        <div class="stats-kv"><div class="stats-k">Ave CPU (sstat)</div><div class="stats-v">${d.ave_cpu || '—'}</div></div>
        <div class="stats-kv"><div class="stats-k">Ave RSS / Max RSS</div><div class="stats-v">${d.ave_rss || '—'} / ${d.max_rss || '—'}</div></div>
      </div>
      ${gpuTable}
    `;
  } catch (e) {
    document.getElementById('stats-body').innerHTML = `<div class="log-loading" style="color:var(--red)">Failed to load stats.</div>`;
  }
}

function closeStats(e) {
  if (e.target === document.getElementById('stats-overlay')) closeStatsDirect();
}
function closeStatsDirect() {
  document.getElementById('stats-overlay').classList.remove('open');
}

// ── Failed pins ──
async function dismissFailed(cluster, jobId) {
  await fetch(`/api/clear_failed_job/${cluster}/${jobId}`, { method: 'POST' });
  await refreshCluster(cluster);
}

async function clearFailed(cluster) {
  await fetch(`/api/clear_failed/${cluster}`, { method: 'POST' });
  await refreshCluster(cluster);
  toast(`Cleared failed jobs on ${cluster}`);
}

async function clearCompleted(cluster) {
  await fetch(`/api/clear_completed/${cluster}`, { method: 'POST' });
  await refreshCluster(cluster);
  toast(`Cleared completed jobs on ${cluster}`);
}

// ── Cancel ──
async function cancelJob(cluster, jobId) {
  if (!confirm(`Cancel job ${jobId} on ${cluster}?`)) return;
  try {
    const res = await fetch(`/api/cancel/${cluster}/${jobId}`, { method: 'POST' });
    const d = await res.json();
    if (d.status === 'ok') { toast(`Cancelled ${jobId}`); refreshCluster(cluster); }
    else toast(d.error, 'error');
  } catch { toast('Cancel failed', 'error'); }
}

async function cancelAll(cluster) {
  if (!confirm(`Cancel ALL your jobs on ${cluster}?`)) return;
  try {
    const res = await fetch(`/api/cancel_all/${cluster}`, { method: 'POST' });
    const d = await res.json();
    if (d.status === 'ok') { toast(`Cancelled all on ${cluster}`); refreshCluster(cluster); }
    else toast(d.error, 'error');
  } catch { toast('Cancel failed', 'error'); }
}

// ── Mount controls ──
async function mountCluster(cluster) {
  try {
    const res = await fetch(`/api/mount/mount/${cluster}`, { method: 'POST' });
    const d = await res.json();
    if (d.status === 'ok') {
      toast(`Mounted ${cluster}`);
      await refreshCluster(cluster);
    } else {
      toast(d.error || `Mount failed on ${cluster}`, 'error');
    }
  } catch {
    toast(`Mount failed on ${cluster}`, 'error');
  }
}

async function unmountCluster(cluster) {
  try {
    const res = await fetch(`/api/mount/unmount/${cluster}`, { method: 'POST' });
    const d = await res.json();
    if (d.status === 'ok') {
      toast(`Unmounted ${cluster}`);
      await refreshCluster(cluster);
    } else {
      toast(d.error || `Unmount failed on ${cluster}`, 'error');
    }
  } catch {
    toast(`Unmount failed on ${cluster}`, 'error');
  }
}

async function mountAll() {
  try {
    const res = await fetch('/api/mount/mount', { method: 'POST' });
    const d = await res.json();
    if (d.status === 'ok') {
      toast('Mounted all clusters');
      await fetchAll();
    } else {
      toast(d.error || 'Mount all failed', 'error');
    }
  } catch {
    toast('Mount all failed', 'error');
  }
}

async function unmountAll() {
  try {
    const res = await fetch('/api/mount/unmount', { method: 'POST' });
    const d = await res.json();
    if (d.status === 'ok') {
      toast('Unmounted all clusters');
      await fetchAll();
    } else {
      toast(d.error || 'Unmount all failed', 'error');
    }
  } catch {
    toast('Unmount all failed', 'error');
  }
}

async function checkMountStatus(cluster) {
  try {
    const res = await fetch(`/api/mounts?cluster=${cluster}`);
    const d = await res.json();
    if (d.status !== 'ok') {
      toast(d.error || `Status check failed for ${cluster}`, 'error');
      return;
    }
    const item = (d.mounts || {})[cluster] || {};
    allData[cluster] = allData[cluster] || { status: 'error', jobs: [] };
    allData[cluster].mount = item;
    renderMountPanel(allData);
    toast(`${cluster}: ${item.mounted ? 'mounted' : 'ssh-only'}`);
  } catch {
    toast(`Status check failed for ${cluster}`, 'error');
  }
}

// ── Toast ──
function toast(msg, type='ok') {
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = msg;
  document.getElementById('toasts').appendChild(el);
  setTimeout(() => el.remove(), 3000);
}

// ── Countdown ──
function refreshNow() {
  clearInterval(cdTimer);
  fetchAll();
  if (refreshIntervalSec > 0) {
    countdown = refreshIntervalSec;
    startCountdown();
  }
}

function startCountdown() {
  document.getElementById('cd').textContent = countdown;
  cdTimer = setInterval(() => {
    countdown--;
    document.getElementById('cd').textContent = countdown;
    if (countdown <= 0) { countdown = refreshIntervalSec; fetchAll(); }
  }, 1000);
}

// ── Settings modal ──

function openSettingsModal() {
  document.getElementById('settings-overlay').classList.add('open');
  loadSettingsPanel();
  renderMountPanel(allData);
}

function closeSettingsModal() {
  document.getElementById('settings-overlay').classList.remove('open');
}

function showSettingsSection(el) {
  document.querySelectorAll('.settings-nav-item').forEach(n => n.classList.remove('active'));
  document.querySelectorAll('.settings-section').forEach(s => s.classList.remove('active'));
  el.classList.add('active');
  document.getElementById(el.dataset.section).classList.add('active');
}

async function loadSettingsPanel() {
  try {
    const res = await fetch('/api/settings');
    const cfg = await res.json();
    document.getElementById('set-ssh-timeout').value = cfg.ssh_timeout || 8;
    document.getElementById('set-cache-fresh').value = cfg.cache_fresh_sec || 30;

    const inc = (cfg.local_process_filters || {}).include || [];
    const exc = (cfg.local_process_filters || {}).exclude || [];
    document.getElementById('set-proc-include').value = inc.join(', ');
    document.getElementById('set-proc-exclude').value = exc.join(', ');

    renderClusterEditor(cfg.clusters || {});
    renderProjectEditor(cfg.projects || {});
  } catch (e) {
    toast('Failed to load settings', 'error');
  }
}

function renderClusterEditor(clusters) {
  const el = document.getElementById('cluster-editor');
  el.innerHTML = Object.entries(clusters).map(([name, c]) => `
    <div class="cluster-edit-card" data-cluster="${name}">
      <div class="ce-head">
        <span class="ce-name">${name}</span>
        <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove()" title="remove">✕</button>
      </div>
      <div class="ce-fields">
        <div class="ce-field"><span>Name</span><input data-f="name" value="${name}"></div>
        <div class="ce-field"><span>Host</span><input data-f="host" value="${c.host || ''}"></div>
        <div class="ce-field"><span>Port</span><input data-f="port" type="number" value="${c.port || 22}"></div>
        <div class="ce-field"><span>GPU Type</span><input data-f="gpu_type" value="${c.gpu_type || ''}"></div>
        <div class="ce-field" style="grid-column:1/-1"><span>Remote Root</span><input data-f="remote_root" value="${c.remote_root || '/'}"></div>
      </div>
    </div>
  `).join('');
}

function addClusterRow() {
  const el = document.getElementById('cluster-editor');
  const div = document.createElement('div');
  div.className = 'cluster-edit-card';
  div.innerHTML = `
    <div class="ce-head">
      <span class="ce-name">new cluster</span>
      <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove()" title="remove">✕</button>
    </div>
    <div class="ce-fields">
      <div class="ce-field"><span>Name</span><input data-f="name" value="" placeholder="cluster-name"></div>
      <div class="ce-field"><span>Host</span><input data-f="host" value="" placeholder="login-node.example.com"></div>
      <div class="ce-field"><span>Port</span><input data-f="port" type="number" value="22"></div>
      <div class="ce-field"><span>GPU Type</span><input data-f="gpu_type" value="" placeholder="H100"></div>
      <div class="ce-field" style="grid-column:1/-1"><span>Remote Root</span><input data-f="remote_root" value="/lustre"></div>
    </div>
  `;
  el.appendChild(div);
}

async function saveClusters() {
  const cards = document.querySelectorAll('#cluster-editor .cluster-edit-card');
  const clusters = {};
  for (const card of cards) {
    const name = (card.querySelector('[data-f="name"]').value || '').trim();
    if (!name) continue;
    clusters[name] = {
      host: card.querySelector('[data-f="host"]').value.trim(),
      port: parseInt(card.querySelector('[data-f="port"]').value) || 22,
      gpu_type: card.querySelector('[data-f="gpu_type"]').value.trim(),
      remote_root: card.querySelector('[data-f="remote_root"]').value.trim() || '/',
    };
  }
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clusters }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      toast('Clusters saved');
      fetchAll();
    } else {
      toast(d.error || 'Save failed', 'error');
    }
  } catch (e) {
    toast('Save failed', 'error');
  }
}

function renderProjectEditor(projects) {
  const el = document.getElementById('project-editor');
  el.innerHTML = Object.entries(projects).map(([name, p]) => `
    <div class="cluster-edit-card" data-project="${name}">
      <div class="ce-head">
        <span class="ce-name" style="display:flex;align-items:center;gap:6px">
          <span style="font-size:16px">${p.emoji || '📁'}</span>
          <span class="project-color-dot" style="background:${p.color || '#ddd'}"></span>${name}
        </span>
        <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove()" title="remove">✕</button>
      </div>
      <div class="ce-fields">
        <div class="ce-field"><span>Name</span><input data-f="name" value="${name}"></div>
        <div class="ce-field"><span>Prefix</span><input data-f="prefix" value="${p.prefix || ''}" placeholder="name_"></div>
        <div class="ce-field"><span>Emoji</span><input data-f="emoji" value="${p.emoji || ''}" placeholder="🔬" style="width:40px;text-align:center"></div>
        <div class="ce-field"><span>Color</span><input data-f="color" type="color" value="${p.color || '#e8f4fd'}" style="width:40px;height:28px;padding:0;border:none;cursor:pointer"></div>
      </div>
    </div>
  `).join('');
}

function addProjectRow() {
  const el = document.getElementById('project-editor');
  const div = document.createElement('div');
  div.className = 'cluster-edit-card';
  div.innerHTML = `
    <div class="ce-head">
      <span class="ce-name">new project</span>
      <button class="ce-remove" onclick="this.closest('.cluster-edit-card').remove()" title="remove">✕</button>
    </div>
    <div class="ce-fields">
      <div class="ce-field"><span>Name</span><input data-f="name" value="" placeholder="my-project"></div>
      <div class="ce-field"><span>Prefix</span><input data-f="prefix" value="" placeholder="my-project_"></div>
      <div class="ce-field"><span>Emoji</span><input data-f="emoji" value="" placeholder="🔬" style="width:40px;text-align:center"></div>
      <div class="ce-field"><span>Color</span><input data-f="color" type="color" value="#e8f4fd" style="width:40px;height:28px;padding:0;border:none;cursor:pointer"></div>
    </div>
  `;
  el.appendChild(div);
}

async function saveProjects() {
  const cards = document.querySelectorAll('#project-editor .cluster-edit-card');
  const projects = {};
  for (const card of cards) {
    const name = (card.querySelector('[data-f="name"]').value || '').trim();
    if (!name) continue;
    projects[name] = {
      prefix: card.querySelector('[data-f="prefix"]').value.trim(),
      emoji: card.querySelector('[data-f="emoji"]').value.trim(),
      color: card.querySelector('[data-f="color"]').value.trim(),
    };
  }
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ projects }),
    });
    const d = await res.json();
    if (d.status === 'ok') {
      toast('Projects saved');
      fetchAll();
    } else {
      toast(d.error || 'Save failed', 'error');
    }
  } catch (e) {
    toast('Save failed', 'error');
  }
}

async function saveAdvancedSettings() {
  const sshTimeout = parseInt(document.getElementById('set-ssh-timeout').value) || 8;
  const cacheFresh = parseInt(document.getElementById('set-cache-fresh').value) || 30;
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ssh_timeout: sshTimeout, cache_fresh_sec: cacheFresh }),
    });
    const d = await res.json();
    if (d.status === 'ok') toast('Advanced settings saved');
    else toast(d.error || 'Save failed', 'error');
  } catch (e) {
    toast('Save failed', 'error');
  }
}

async function saveProcessFilters() {
  const inc = document.getElementById('set-proc-include').value.split(',').map(s => s.trim()).filter(Boolean);
  const exc = document.getElementById('set-proc-exclude').value.split(',').map(s => s.trim()).filter(Boolean);
  try {
    const res = await fetch('/api/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ local_process_filters: { include: inc, exclude: exc } }),
    });
    const d = await res.json();
    if (d.status === 'ok') toast('Process filters saved');
    else toast(d.error || 'Save failed', 'error');
  } catch (e) {
    toast('Save failed', 'error');
  }
}

// ── Local settings (localStorage) ──
let jsonlLimit = 50;
let jsonlMode = 'first';

function loadLocalSettings() {
  try {
    const autoRefresh = localStorage.getItem('ncluster.autoRefresh') === '1';
    const interval = parseInt(localStorage.getItem('ncluster.refreshInterval') || '30');
    const pageSize = parseInt(localStorage.getItem('ncluster.histPageSize') || '50');
    jsonlLimit = parseInt(localStorage.getItem('ncluster.jsonlLimit') || '50');
    jsonlMode = localStorage.getItem('ncluster.jsonlMode') || 'first';
    document.getElementById('set-autorefresh').checked = autoRefresh;
    document.getElementById('set-refresh-interval').value = interval;
    document.getElementById('set-hist-pagesize').value = pageSize;
    document.getElementById('set-jsonl-limit').value = jsonlLimit;
    document.getElementById('set-jsonl-mode').value = jsonlMode;
    return { autoRefresh, interval, pageSize };
  } catch (_) {
    return { autoRefresh: false, interval: 30, pageSize: 50 };
  }
}

function saveLocalSettings() {
  const autoRefresh = document.getElementById('set-autorefresh').checked;
  const interval = parseInt(document.getElementById('set-refresh-interval').value) || 30;
  const pageSize = parseInt(document.getElementById('set-hist-pagesize').value) || 50;
  jsonlLimit = parseInt(document.getElementById('set-jsonl-limit').value) || 100;
  jsonlMode = document.getElementById('set-jsonl-mode').value || 'last';
  try {
    localStorage.setItem('ncluster.autoRefresh', autoRefresh ? '1' : '0');
    localStorage.setItem('ncluster.refreshInterval', String(interval));
    localStorage.setItem('ncluster.histPageSize', String(pageSize));
    localStorage.setItem('ncluster.jsonlLimit', String(jsonlLimit));
    localStorage.setItem('ncluster.jsonlMode', jsonlMode);
  } catch (_) {}
  applyLocalSettings();
}

function applyLocalSettings() {
  const s = loadLocalSettings();
  HIST_GROUPS_PER_PAGE = s.pageSize;
  refreshIntervalSec = s.autoRefresh ? Math.max(5, s.interval) : 0;
  clearInterval(cdTimer);
  if (refreshIntervalSec > 0) {
    countdown = refreshIntervalSec;
    startCountdown();
    document.getElementById('cd').parentElement.style.display = '';
  } else {
    document.getElementById('cd').parentElement.style.display = 'none';
  }
}

// Init
showTab('live');
setupTreeResizer();
setupSidebarResizer();
applySidebarState();
loadLocalSettings();
applyLocalSettings();
fetchAll();
loadProjectButtons();
if (refreshIntervalSec > 0) startCountdown();
